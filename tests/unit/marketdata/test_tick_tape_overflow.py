"""Invariant tests for tier C same-night tick-tape overflow planning and run behavior."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib

import polars as pl
import pytest
from pydantic import ValidationError

from src.core.config import TickTapeOverflowSettings
from src.core.paths import DataPaths
from src.marketdata import tick_tape_backfill as backfill_mod
from src.marketdata.tick_tape_backfill import (
    TickTapeTarget,
    abandon_stale_tier_c,
    plan_tick_tape_targets,
    read_ledger_abandoned,
    read_ledger_done,
    run_tick_tape_backfill,
)
from src.universe.membership import MembershipRow, append_membership
from tests.unit.marketdata.test_tick_tape_backfill import (
    _EVENING,
    _KST,
    _ChartResp,
    _creds,
    _deadline,
    _mute_sleep,
    _prev,
    _Session,
    _settings,
    _universe,
    _walk_day_pages,
    _audit,
    _manifest,
)

_D = dt.date(2026, 10, 21)
_ON = TickTapeOverflowSettings(enabled=True, max_symbols=60)


def _row(symbol: str, tier: str, *, source: str = "research", trade: int | None = None) -> MembershipRow:
    return MembershipRow(
        symbol=symbol,
        tier=tier,
        source=source,
        decided_at_ns=1,
        direction=None,
        warning_level=None,
        ls_shard=None,
        kis_slot=None,
        trade_value_krw=trade,
        reason=None,
    )


def _membership(paths: DataPaths, day: dt.date, rows: list[MembershipRow]) -> None:
    append_membership(paths.membership_path(day), rows)


def _seed_repair_and_gap(paths: DataPaths) -> None:
    _universe(paths, _prev(_D), ["R", "G"])
    _manifest(paths, _D, ["G"])
    _audit(paths, _D, {"G": 1})


def test_disabled_overflow_matches_legacy_plan(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _seed_repair_and_gap(paths)
    _membership(paths, _D, [_row("X", "C", trade=900), _row("Y", "C", trade=500)])
    repairs = frozenset({("R", _D)})
    legacy = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset(), repair_symbol_days=repairs)
    disabled = plan_tick_tape_targets(
        sessions=[_D],
        paths=paths,
        ledger_done=frozenset(),
        repair_symbol_days=repairs,
        overflow=TickTapeOverflowSettings(enabled=False, max_symbols=60),
    )
    assert disabled == legacy
    assert [(item.symbol, item.tier) for item in legacy] == [("R", "repair"), ("G", "gap")]


def test_tier_c_ordered_after_repairs_and_gaps(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _seed_repair_and_gap(paths)
    _membership(paths, _D, [_row("C1", "C", trade=500), _row("C2", "C", trade=900)])
    targets = plan_tick_tape_targets(
        sessions=[_D],
        paths=paths,
        ledger_done=frozenset(),
        repair_symbol_days=frozenset({("R", _D)}),
        overflow=_ON,
    )
    assert [(item.symbol, item.tier) for item in targets] == [
        ("R", "repair"),
        ("G", "gap"),
        ("C2", "C"),
        ("C1", "C"),
    ]
    assert all(item.days == (_D,) for item in targets if item.tier == "C")


def test_promoted_symbol_is_b_dynamic_not_c(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), [])
    _membership(
        paths,
        _D,
        [_row("P", "C", trade=100), _row("P", "B", source="dynamic", trade=100), _row("Q", "C", trade=50)],
    )
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset(), overflow=_ON)
    assert [(item.symbol, item.tier) for item in targets] == [("P", "B_dynamic"), ("Q", "C")]


def test_tier_c_capped_by_max_symbols(tmp_path: pathlib.Path, caplog) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), [])
    _membership(paths, _D, [_row(f"S{idx}", "C", trade=idx) for idx in range(5)])
    with caplog.at_level(logging.INFO, logger=backfill_mod.logger.name):
        targets = plan_tick_tape_targets(
            sessions=[_D],
            paths=paths,
            ledger_done=frozenset(),
            overflow=TickTapeOverflowSettings(enabled=True, max_symbols=2),
        )
    assert [item.symbol for item in targets] == ["S4", "S3"]
    truncated = [r for r in caplog.records if "status=TRUNCATED" in r.getMessage()]
    assert len(truncated) == 1
    assert "kept=2 dropped=3" in truncated[0].getMessage()


def test_only_latest_session_tier_c_and_stale_rows_abandoned_once(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(backfill_mod, "_prev_business_day", lambda day, _paths: _prev(day))
    paths = DataPaths(tmp_path)
    older = _prev(_D)
    _membership(paths, older, [_row("OLD", "C", trade=10)])
    _membership(paths, _D, [_row("NEW", "C", trade=10)])
    targets = plan_tick_tape_targets(sessions=[older, _D], paths=paths, ledger_done=frozenset(), overflow=_ON)
    assert [(item.symbol, item.days, item.tier) for item in targets] == [("NEW", (_D,), "C")]

    assert abandon_stale_tier_c(sessions=[older, _D], paths=paths, ledger_done=frozenset(), overflow=_ON) == 1
    assert read_ledger_abandoned(paths.tick_tape_ledger) == frozenset({("OLD", older)})
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["status"] == "abandoned"
    assert rows[0]["reason"] == "same_night_only"
    assert rows[0]["tier"] == "C"
    assert abandon_stale_tier_c(sessions=[older, _D], paths=paths, ledger_done=frozenset(), overflow=_ON) == 0


def test_abandon_stale_tier_c_inert_when_disabled(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backfill_mod, "_prev_business_day", lambda day, _paths: _prev(day))
    paths = DataPaths(tmp_path)
    older = _prev(_D)
    _membership(paths, older, [_row("OLD", "C", trade=10)])
    assert abandon_stale_tier_c(
        sessions=[older, _D], paths=paths, ledger_done=frozenset(), overflow=TickTapeOverflowSettings()
    ) == 0
    assert not paths.tick_tape_ledger.exists()


def test_unreadable_membership_skips_tier_c_and_keeps_repairs(tmp_path: pathlib.Path, caplog) -> None:
    paths = DataPaths(tmp_path)
    _seed_repair_and_gap(paths)
    paths.membership_path(_D).parent.mkdir(parents=True, exist_ok=True)
    paths.membership_path(_D).write_text("not json\n", encoding="utf-8")
    with caplog.at_level(logging.ERROR, logger=backfill_mod.logger.name):
        targets = plan_tick_tape_targets(
            sessions=[_D],
            paths=paths,
            ledger_done=frozenset(),
            repair_symbol_days=frozenset({("R", _D)}),
            overflow=_ON,
        )
    assert [(item.symbol, item.tier) for item in targets] == [("R", "repair"), ("G", "gap")]
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any(
        "stage=tick_tape_plan tier=C status=SKIP reason=membership_unreadable" in r.getMessage() for r in errors
    )


def test_deadline_abandons_remaining_tier_c_without_partial_files(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    prev_day = _prev(_D)
    session = _Session(
        pages={
            "R": _walk_day_pages(_D, prev_day),
            "C1": _walk_day_pages(_D, prev_day),
            "C2": _walk_day_pages(_D, prev_day),
        }
    )

    def clock() -> dt.datetime:
        if any(symbol == "C1" for symbol, _ in session.chart_calls):
            return dt.datetime(2026, 10, 22, 4, 31, tzinfo=_KST)
        return _EVENING

    targets = [
        TickTapeTarget(symbol="R", days=(_D,), cost_hint=1.0, tier="repair"),
        TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C"),
        TickTapeTarget(symbol="C2", days=(_D,), cost_hint=1.0, tier="C"),
    ]
    summary = run_tick_tape_backfill(
        targets=targets,
        paths=paths,
        settings=_settings(),
        credentials=_creds(),
        deadline=_deadline(),
        session=session,
        now=clock,
        run_id="runoverflow1",
        overflow=TickTapeOverflowSettings(enabled=True, max_symbols=60, deadline=dt.time(4, 30)),
    )
    assert summary.status == "OK"
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    abandoned = {(row["symbol"], row["reason"], row["tier"]) for row in rows if row["status"] == "abandoned"}
    assert abandoned == {("C1", "deadline", "C"), ("C2", "deadline", "C")}
    assert ("R", _D) in read_ledger_done(paths.tick_tape_ledger)
    frame = pl.read_parquet(paths.tick_tape_partition(_D, "runoverflow1"))
    assert frame.get_column("symbol").unique().to_list() == ["R"]
    assert not list(paths.work_root.rglob("C1.parquet"))
    assert not list(paths.work_root.rglob("C2.parquet"))


def test_overflow_deadline_must_precede_window_end() -> None:
    assert TickTapeOverflowSettings(deadline=dt.time(4, 30)).deadline == dt.time(4, 30)
    with pytest.raises(ValidationError):
        TickTapeOverflowSettings(deadline=dt.time(5, 30))
    with pytest.raises(ValidationError):
        TickTapeOverflowSettings(deadline=dt.time(6, 0))


def test_nightly_env_carries_overflow_settings_to_child(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.orchestration.daemon import NightlyJob, _nightly_job_env

    cfg = TickTapeOverflowSettings(enabled=True, max_symbols=7, deadline=dt.time(4, 15))
    env = _nightly_job_env(NightlyJob.TICK_TAPE, cfg)
    assert _nightly_job_env(NightlyJob.REPAIR, cfg) == {}
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert TickTapeOverflowSettings() == cfg


def _late() -> dt.datetime:
    return dt.datetime(2026, 10, 22, 4, 31, tzinfo=_KST)


def test_tier_c_skips_legacy_planned_and_settled_symbols(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _seed_repair_and_gap(paths)
    _membership(paths, _D, [_row("G", "C", trade=900), _row("S", "C", trade=800), _row("T", "C", trade=700)])
    targets = plan_tick_tape_targets(
        sessions=[_D], paths=paths, ledger_done=frozenset({("T", _D)}), overflow=_ON
    )
    assert [(item.symbol, item.tier) for item in targets] == [("G", "gap"), ("R", "gap"), ("S", "C")]


def test_stale_abandon_needs_an_older_session(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _membership(paths, _D, [_row("X", "C")])
    assert abandon_stale_tier_c(sessions=[_D], paths=paths, ledger_done=frozenset(), overflow=_ON) == 0
    assert not paths.tick_tape_ledger.exists()


def test_stale_abandon_degrades_on_unreadable_older_membership(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    monkeypatch.setattr(backfill_mod, "_prev_business_day", lambda day, _paths: _prev(day))
    paths = DataPaths(tmp_path)
    older = _prev(_D)
    paths.membership_path(older).parent.mkdir(parents=True, exist_ok=True)
    paths.membership_path(older).write_text("garbage\n", encoding="utf-8")
    with caplog.at_level(logging.ERROR, logger=backfill_mod.logger.name):
        assert abandon_stale_tier_c(sessions=[older, _D], paths=paths, ledger_done=frozenset(), overflow=_ON) == 0
    assert any("reason=membership_unreadable" in r.getMessage() for r in caplog.records)


def test_tier_c_retry_pass_respects_deadline(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"C1": [_ChartResp([], status=500, cont="N")]})

    def clock() -> dt.datetime:
        return _late() if session.chart_calls else _EVENING

    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C")],
        paths=paths,
        settings=_settings(transient_retries=0),
        credentials=_creds(),
        deadline=_deadline(),
        session=session,
        now=clock,
        run_id="runretry1",
        sleep=lambda seconds: None,
        overflow=_ON,
    )
    assert summary.status == "OK"
    assert summary.failed == 0
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    assert [(row["status"], row["reason"], row["tier"]) for row in rows] == [("abandoned", "deadline", "C")]
    assert not paths.tick_tape_partition(_D, "runretry1").exists()


def test_tier_c_completes_with_tier_label(tmp_path: pathlib.Path, monkeypatch, caplog) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"C1": _walk_day_pages(_D, _prev(_D))})
    with caplog.at_level(logging.INFO, logger=backfill_mod.logger.name):
        summary = run_tick_tape_backfill(
            targets=[TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C")],
            paths=paths,
            settings=_settings(),
            credentials=_creds(),
            deadline=_deadline(),
            session=session,
            now=lambda: _EVENING,
            run_id="runc1",
            overflow=_ON,
        )
    assert summary.status == "OK"
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    assert {(row["status"], row["tier"]) for row in rows} == {("complete", "C")}
    assert any("tier_c_done=1 tier_c_abandoned=0" in r.getMessage() for r in caplog.records)


def test_tier_c_target_without_overflow_settings_is_not_deadline_gated(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"C1": _walk_day_pages(_D, _prev(_D))})
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C")],
        paths=paths,
        settings=_settings(),
        credentials=_creds(),
        deadline=_deadline(),
        session=session,
        now=_late,
        run_id="runc2",
    )
    assert summary.status == "OK"
    assert paths.tick_tape_partition(_D, "runc2").exists()


def test_stale_abandon_failure_degrades_without_failing_run(tmp_path: pathlib.Path, monkeypatch, caplog) -> None:
    import src.cli.backfill_tick_tape as cli
    from src.marketdata.tick_tape_backfill import TickTapeRunSummary

    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A"])
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    monkeypatch.setenv("KRX_ALPHA_TICK_TAPE_ENABLED", "1")
    monkeypatch.setenv("KRX_ALPHA_TICK_TAPE_C_ENABLED", "1")
    monkeypatch.setattr(cli, "resolve_backfill_sessions", lambda *args: (_D,))
    monkeypatch.setattr(cli, "_now_kst", lambda: dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST))

    def _boom(**kwargs):
        raise OSError("disk")

    monkeypatch.setattr(cli, "abandon_stale_tier_c", _boom)
    monkeypatch.setattr(
        cli,
        "run_tick_tape_backfill",
        lambda **kwargs: TickTapeRunSummary("OK", None, "x", 1, 0, 0, 0, 0),
    )
    args = type(
        "Args",
        (),
        {"dry_run": False, "data_root": tmp_path, "max_pages": None, "deadline": None, "only_symbols": None},
    )()
    with caplog.at_level(logging.WARNING, logger=cli.logger.name):
        code = cli.run(args)
    assert code == 0
    assert any("reason=stale_abandon" in r.getMessage() for r in caplog.records)


def test_tier_c_retry_pass_mid_walk_deadline_abandons(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(
        pages={"C1": [_ChartResp([], status=500, cont="N"), *_walk_day_pages(_D, _prev(_D))]}
    )

    def clock() -> dt.datetime:
        return _late() if len(session.chart_calls) >= 2 else _EVENING

    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C")],
        paths=paths,
        settings=_settings(transient_retries=0),
        credentials=_creds(),
        deadline=_deadline(),
        session=session,
        now=clock,
        run_id="runretry2",
        sleep=lambda seconds: None,
        overflow=_ON,
    )
    assert summary.status == "OK"
    assert summary.failed == 0
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    assert [(row["status"], row["reason"], row["tier"]) for row in rows] == [("abandoned", "deadline", "C")]
    assert not paths.tick_tape_partition(_D, "runretry2").exists()
    assert not list(paths.work_root.rglob("C1.parquet"))


def test_tier_c_retry_success_completes(tmp_path: pathlib.Path, monkeypatch, caplog) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(
        pages={"C1": [_ChartResp([], status=500, cont="N"), *_walk_day_pages(_D, _prev(_D))]}
    )
    with caplog.at_level(logging.INFO, logger=backfill_mod.logger.name):
        summary = run_tick_tape_backfill(
            targets=[TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C")],
            paths=paths,
            settings=_settings(transient_retries=0),
            credentials=_creds(),
            deadline=_deadline(),
            session=session,
            now=lambda: _EVENING,
            run_id="runretry3",
            sleep=lambda seconds: None,
            overflow=_ON,
        )
    assert summary.status == "OK"
    assert summary.failed == 0
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    assert {(row["status"], row["tier"]) for row in rows} == {("complete", "C")}
    assert any("tier_c_done=1 tier_c_abandoned=0" in r.getMessage() for r in caplog.records)


def test_tier_c_terminal_failure_abandons_with_exception_name(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"C1": [_ChartResp({"return_code": 99, "return_msg": "bad"}, cont="N")]})
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="C1", days=(_D,), cost_hint=1.0, tier="C")],
        paths=paths,
        settings=_settings(retry_passes=0),
        credentials=_creds(),
        deadline=_deadline(),
        session=session,
        now=lambda: _EVENING,
        run_id="runfail1",
        overflow=_ON,
    )
    assert summary.status == "OK"
    assert summary.failed == 1
    rows = [json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines()]
    assert [(row["status"], row["tier"]) for row in rows] == [("abandoned", "C")]
    assert rows[0]["reason"] == "KiwoomTickTapeError"
