"""Invariant tests for Kiwoom tick-tape backfill planning and one-shot run."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib
import time

import polars as pl
import pytest

from src.core.config import KiwoomCredentials, TickTapeBackfillSettings
from src.core.paths import DataPaths
from src.marketdata import tick_tape_backfill as backfill_mod
from src.marketdata.kiwoom_ticks import TapeTick, TickPage
from src.marketdata.tick_tape_backfill import (
    TickTapeTarget,
    plan_tick_tape_targets,
    read_ledger_done,
    run_tick_tape_backfill,
)

_D = dt.date(2026, 10, 21)


def _universe(paths: DataPaths, decision: dt.date, symbols: list[str]) -> None:
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    day = decision + dt.timedelta(days=1)
    while day.weekday() >= 5:
        day += dt.timedelta(days=1)
    save_trading_day_cache(paths.calendar_cache, TradingDay(day, True, decision, day + dt.timedelta(days=1)))
    path = paths.universe_out(decision)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}, schema={"symbol": pl.String}).write_parquet(path)


def _manifest(paths: DataPaths, day: dt.date, accepted: list[str]) -> None:
    path = paths.manifest_path(day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "session_date": day.isoformat(),
                "clock_offset_ns": 0,
                "started_at_ns": 0,
                "subscription_acks": [
                    {"symbol": symbol, "accepted": True} for symbol in accepted
                ],
            }
        ),
        encoding="utf-8",
    )


def _audit(paths: DataPaths, day: dt.date, gaps: dict[str, int]) -> None:
    path = paths.session_audit_path(day, "ls")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "symbols": [
                    {"symbol": symbol, "confirmed_missing": missing} for symbol, missing in gaps.items()
                ]
            }
        ),
        encoding="utf-8",
    )


def _bars(paths: DataPaths, rows: list[tuple[dt.date, str, float]]) -> None:
    frame = pl.DataFrame(
        {"date": [row[0] for row in rows], "symbol": [row[1] for row in rows], "trade_value_100m": [row[2] for row in rows]},
        schema={"date": pl.Date, "symbol": pl.String, "trade_value_100m": pl.Float64},
    )
    out = paths.bars_daily_dir / f"{rows[0][0]:%Y-%m}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out)


def _prev(day: dt.date) -> dt.date:
    prev = day - dt.timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= dt.timedelta(days=1)
    return prev


def test_unsubscribed_universe_names_are_targets(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A", "B", "C"])
    _manifest(paths, _D, ["A"])
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert [(item.symbol, item.days) for item in targets] == [("B", (_D,)), ("C", (_D,))]


def test_audit_gap_makes_subscribed_name_a_target(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A"])
    _manifest(paths, _D, ["A"])
    _audit(paths, _D, {"A": 4})
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert [(item.symbol, item.days) for item in targets] == [("A", (_D,))]


def test_ledger_settles_complete_and_vendor_empty_only(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A", "B", "C"])
    ledger = paths.tick_tape_ledger
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(
        "\n".join(
            json.dumps({"date": _D.isoformat(), "symbol": symbol, "status": status, "rows": 1})
            for symbol, status in (("A", "complete"), ("B", "vendor_empty"), ("C", "edge_partial"))
        )
        + "\n",
        encoding="utf-8",
    )
    done = read_ledger_done(ledger)
    assert done == frozenset({("A", _D), ("B", _D)})
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=done)
    assert [item.symbol for item in targets] == ["C"]


def test_ledger_tolerates_torn_final_line(tmp_path: pathlib.Path) -> None:
    ledger = tmp_path / "kiwoom_ledger.jsonl"
    ledger.write_text(
        json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "complete", "rows": 2}) + "\n{truncated",
        encoding="utf-8",
    )
    assert read_ledger_done(ledger) == frozenset({("A", _D)})


def test_universe_file_mapping_uses_previous_business_day(tmp_path: pathlib.Path) -> None:
    monday = dt.date(2026, 10, 19)
    friday = dt.date(2026, 10, 16)
    assert monday.weekday() == 0
    assert friday.weekday() == 4
    paths = DataPaths(tmp_path)
    _universe(paths, friday, ["A"])
    targets = plan_tick_tape_targets(sessions=[monday], paths=paths, ledger_done=frozenset())
    assert [item.symbol for item in targets] == ["A"]


def test_missing_universe_logs_skip_without_targets(tmp_path: pathlib.Path, caplog) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), [])
    paths.universe_out(_prev(_D)).unlink()
    with caplog.at_level("WARNING"):
        targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert targets == ()
    assert any("stage=tick_tape_plan" in record.getMessage() and "no_universe" in record.getMessage() for record in caplog.records)


def test_cheap_names_first_deterministic(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["B", "A"])
    _bars(paths, [(_D, "A", 900.0), (_D, "B", 100.0)])
    first = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    _universe(paths, _prev(_D), ["A", "B"])
    second = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert [item.symbol for item in first] == ["B", "A"]
    assert first == second


def test_expiring_days_first_then_cheap(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = DataPaths(tmp_path)
    monkeypatch.setattr(backfill_mod, "_prev_business_day", lambda day, _paths: _prev(day))
    older = _prev(_D)
    _universe(paths, _prev(older), ["HEAVY_OLD"])
    _universe(paths, _prev(_D), ["CHEAP_NEW", "MID_NEW"])
    _bars(
        paths,
        [(older, "HEAVY_OLD", 9000.0), (_D, "HEAVY_OLD", 9000.0), (_D, "CHEAP_NEW", 1.0), (_D, "MID_NEW", 50.0)],
    )
    targets = plan_tick_tape_targets(sessions=[older, _D], paths=paths, ledger_done=frozenset())
    assert [item.symbol for item in targets] == ["HEAVY_OLD", "CHEAP_NEW", "MID_NEW"]
    assert targets[0].days[0] == older


class _ChartResp:
    def __init__(self, rows, *, status=200, cont=None, next_key=None):
        self.status_code = status
        self.headers = {}
        if cont is not None:
            self.headers["cont-yn"] = cont
        if next_key is not None:
            self.headers["next-key"] = next_key
        self._rows = rows
        self._status = status

    def json(self):
        if self._status == 429:
            return {"return_code": 5, "return_msg": "over limit"}
        if isinstance(self._rows, dict):
            return self._rows
        return {"return_code": 0, "return_msg": "ok", "stk_tic_chart_qry": self._rows}


class _Session:
    def __init__(self, token_behavior="ok", pages=None):
        self.token_behavior = token_behavior
        self.pages = dict(pages or {})
        self.token_calls = 0
        self.chart_calls: list[tuple[str, str | None]] = []

    def post(self, url, **kwargs):
        if url.endswith("/oauth2/token"):
            self.token_calls += 1
            if self.token_behavior == "deny":
                return _ChartResp({"return_code": 1, "return_msg": "bad"})
            token = f"tok-{self.token_calls}"
            return _ChartResp({"return_code": 0, "return_msg": "ok", "token": token}, cont="N")
        body = kwargs.get("json", {})
        symbol = str(body.get("stk_cd", ""))
        headers = kwargs.get("headers", {})
        key = headers.get("next-key")
        self.chart_calls.append((symbol, key))
        queue = self.pages.get(symbol, [])
        if not queue:
            return _ChartResp([], cont="N")
        return queue.pop(0)


def _rows(day: dt.date, count: int, base: int = 90000) -> list[dict]:
    return [
        {"cntr_tm": f"{day:%Y%m%d}{base + idx:06d}", "cur_prc": "100", "trde_qty": "2"}
        for idx in reversed(range(count))
    ]


def _settings(**overrides) -> TickTapeBackfillSettings:
    return TickTapeBackfillSettings(enabled=True, **overrides)


def _creds() -> KiwoomCredentials:
    return KiwoomCredentials(kiwoom_app_key="k", kiwoom_secret_key="s")


def _deadline() -> dt.datetime:
    return dt.datetime(2026, 10, 22, 5, 30, tzinfo=dt.timezone(dt.timedelta(hours=9)))


def _walk_day_pages(day: dt.date, prev_day: dt.date) -> list:
    return [
        _ChartResp(_rows(day, 2, 150000), cont="Y", next_key="k1"),
        _ChartResp(_rows(prev_day, 1, 150000), cont="N"),
    ]


_KST = dt.timezone(dt.timedelta(hours=9))

_EVENING = dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST)


def _mute_sleep(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda seconds: None)


def _evening_now() -> dt.datetime:
    return _EVENING


def test_partition_before_ledger_and_immutable(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    session = _Session(pages={"A": _walk_day_pages(day, prev_day)})
    target = TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)
    now = _evening_now
    original_append = backfill_mod._append_ledger_lines
    calls = {"count": 0}

    def _flaky(path, lines):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("disk full")
        return original_append(path, lines)

    _mute_sleep(monkeypatch)
    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", _flaky)
    summary = run_tick_tape_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=now, run_id="run1retry",
    )
    assert summary.status == "FAIL"
    partition = paths.tick_tape_partition(day, "run1retry")
    assert partition.exists()
    assert not paths.tick_tape_ledger.exists()
    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", original_append)
    session.pages = {"A": _walk_day_pages(day, prev_day)}
    summary = run_tick_tape_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=now, run_id="run1",
    )
    assert summary.status == "OK"
    assert len(session.chart_calls) == 4
    first_bytes = partition.read_bytes()
    session.pages = {"A": _walk_day_pages(day, prev_day)}
    rerun = run_tick_tape_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=now, run_id="run2",
    )
    assert rerun.status == "OK"
    assert partition.read_bytes() == first_bytes
    assert paths.tick_tape_partition(day, "run2").exists()
    ledger_rows = [
        json.loads(line) for line in paths.tick_tape_ledger.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    assert all(
        paths.tick_tape_partition(dt.date.fromisoformat(row["date"]), row["run_id"]).exists()
        for row in ledger_rows
        if row["status"] == "complete"
    )


def test_second_auth_rejection_aborts(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    denied = _ChartResp({"return_code": 3, "return_msg": "expired"}, cont="N")
    session = _Session(pages={"A": [denied, denied]})
    now = _evening_now
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=now, run_id="run9",
    )
    assert summary.status == "FAIL"
    assert list(paths.tick_tape_root.rglob("*.parquet")) == []
    assert not paths.tick_tape_ledger.exists()


def test_throttle_isolates_symbol(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    throttled = [_ChartResp([], status=429, cont="N") for _ in range(5)]
    session = _Session(pages={"A": throttled, "B": _walk_day_pages(day, prev_day)})
    now = _evening_now
    summary = run_tick_tape_backfill(
        targets=[
            TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0),
            TickTapeTarget(symbol="B", days=(day,), cost_hint=2.0),
        ],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=now, run_id="run7",
    )
    assert summary.status == "OK"
    assert summary.failed == 1
    assert paths.tick_tape_partition(day, "run7").exists()
    frame = pl.read_parquet(paths.tick_tape_partition(day, "run7"))
    assert set(frame.get_column("symbol").to_list()) == {"B"}


def test_deadline_stops_new_symbols(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    session = _Session(pages={"A": _walk_day_pages(day, prev_day), "B": _walk_day_pages(day, prev_day)})
    kst = dt.timezone(dt.timedelta(hours=9))
    def clock():
        return dt.datetime(2026, 10, 22, 6, 0, tzinfo=kst) if len(session.chart_calls) >= 2 else _EVENING
    summary = run_tick_tape_backfill(
        targets=[
            TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0),
            TickTapeTarget(symbol="B", days=(day,), cost_hint=2.0),
        ],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=clock, run_id="run5",
    )
    assert summary.symbols == 1
    assert [call[0] for call in session.chart_calls] == ["A", "A"]


def test_shared_pacer_path_matches_kca_naming(tmp_path: pathlib.Path) -> None:
    from src.brokers.kis.rate import host_state_path

    digest = hashlib.sha256(b"k").hexdigest()[:12]
    assert host_state_path(tmp_path, "kiwoom", "k", "ka10079").name == f"admission_kiwoom_{digest}_ka10079.state"


def test_dry_run_makes_no_vendor_call(tmp_path: pathlib.Path, monkeypatch, capsys) -> None:
    import src.cli.backfill_tick_tape as cli

    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A"])
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    monkeypatch.setattr(cli, "resolve_backfill_sessions", lambda *args: (_D,))
    args = type("Args", (), {"dry_run": True, "data_root": tmp_path, "max_pages": None, "deadline": None})()
    code = cli.run(args)
    assert code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["symbols"] == 1
    assert out["symbol_days"] == 1


def test_walk_tick_shape_flows_to_partition_schema(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    assert TickPage(ticks=(TapeTick(day=day, ts_hms=90001, price=100, volume=2),), next_key=None).ticks
    session = _Session(pages={"A": _walk_day_pages(day, prev_day)})
    now = _evening_now
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0)],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=now, run_id="runschema",
    )
    assert summary.status == "OK"
    frame = pl.read_parquet(paths.tick_tape_partition(day, "runschema"))
    assert frame.columns == ["date", "symbol", "tape_seq", "ts_hms", "price", "volume", "vendor", "run_id"]
    assert frame.get_column("tape_seq").to_list() == list(range(frame.height))


def test_tick_tape_settings_window_and_bounds() -> None:
    import pydantic

    assert TickTapeBackfillSettings().rate_per_s == 2.0
    with pytest.raises(pydantic.ValidationError):
        TickTapeBackfillSettings(window_start_kst=dt.time(5, 30), window_end_kst=dt.time(5, 30))
    with pytest.raises(pydantic.ValidationError):
        TickTapeBackfillSettings(rate_per_s=6.0)


def test_corrupt_universe_manifest_audit_plan_without_crash(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), [])
    universe_path = paths.universe_out(_prev(_D))
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"code": ["A"]}, schema={"code": pl.String}).write_parquet(universe_path)
    assert plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset()) == ()


def test_malformed_manifest_audit_fall_back_to_unsubscribed(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A", "B"])
    manifest_path = paths.manifest_path(_D)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"subscription_acks": {}}), encoding="utf-8")
    audit_path = paths.session_audit_path(_D, "ls")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps({"symbols": [42, {"symbol": "X", "confirmed_missing": "bad"}, {"confirmed_missing": 3}]}),
        encoding="utf-8",
    )
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert {item.symbol for item in targets} == {"A", "B"}
    assert all(item.cost_hint == 0.0 for item in targets)


def test_empty_bars_filter_counts_zero(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A", "B"])
    _bars(paths, [(_D, "B", 50.0)])
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    hint = {item.symbol: item.cost_hint for item in targets}
    assert hint["A"] == 0.0
    assert hint["B"] == 50.0


def test_bars_unreadable_counts_zero(tmp_path: pathlib.Path, monkeypatch) -> None:
    import src.marketdata.tick_tape_backfill as module

    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A"])
    monkeypatch.setattr(module, "scan_month_partitions", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("down")))
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert [item.symbol for item in targets] == ["A"]
    assert targets[0].cost_hint == 0.0


def test_empty_sessions_and_empty_universe(tmp_path: pathlib.Path) -> None:
    from src.marketdata.tick_tape_backfill import _read_audit_gap_symbols

    paths = DataPaths(tmp_path)
    assert plan_tick_tape_targets(sessions=[], paths=paths, ledger_done=frozenset()) == ()
    _universe(paths, _prev(_D), [])
    universe_path = paths.universe_out(_prev(_D))
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": []}, schema={"symbol": pl.String}).write_parquet(universe_path)
    assert plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset()) == ()
    audit_path = tmp_path / "audit.json"
    audit_path.write_text(json.dumps({"symbols": {"A": 1}}), encoding="utf-8")
    assert _read_audit_gap_symbols(audit_path) == frozenset()


def test_ledger_edge_records(tmp_path: pathlib.Path) -> None:
    ledger = tmp_path / "edge.jsonl"
    assert read_ledger_done(tmp_path / "absent.jsonl") == frozenset()
    ledger.write_text(
        "\n".join(
            [
                "",
                json.dumps([1, 2]),
                json.dumps({"date": "not-a-date", "symbol": "A", "status": "complete", "rows": 1}),
                json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "edge_partial", "rows": 0}),
                json.dumps({"date": _D.isoformat(), "symbol": "B", "status": "complete", "rows": 2}),
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert read_ledger_done(ledger) == frozenset({("B", _D)})


def test_empty_tape_symbol_settles_nothing(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"A": [_ChartResp([], cont="N")]})
    summary = run_tick_tape_backfill(
        targets=[TickTapeTarget(symbol="A", days=(_D,), cost_hint=1.0)],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runempty",
    )
    assert summary.status == "OK"
    assert summary.symbol_days == 0
    assert list(paths.tick_tape_root.rglob("*.parquet")) == []


def test_mid_run_flush_failure_isolated(tmp_path: pathlib.Path, monkeypatch) -> None:
    _mute_sleep(monkeypatch)
    monkeypatch.setattr(backfill_mod, "_FLUSH_ROW_LIMIT", 1)
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    prev_day = dt.date(2026, 10, 17)
    session = _Session(pages={"A": _walk_day_pages(day, prev_day), "B": _walk_day_pages(day, prev_day)})
    original_append = backfill_mod._append_ledger_lines
    calls = {"count": 0}

    def _flaky(path, lines):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("disk full")
        return original_append(path, lines)

    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", _flaky)
    summary = run_tick_tape_backfill(
        targets=[
            TickTapeTarget(symbol="A", days=(day,), cost_hint=1.0),
            TickTapeTarget(symbol="B", days=(day,), cost_hint=2.0),
        ],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runflush",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "persistence"
    assert summary.failed == 1
    assert summary.symbols == 1
    assert summary.symbol_days == 0
    assert paths.tick_tape_partition(day, "runflush").exists()
    assert not paths.tick_tape_ledger.exists()
    frame = pl.read_parquet(paths.tick_tape_partition(day, "runflush"))
    assert frame.group_by("symbol").len().sort("symbol").rows() == [("A", 2)]


def _replay(paths: DataPaths, decision: dt.date, symbols: list[str]) -> None:
    path = paths.universe_replay_out(decision)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}, schema={"symbol": pl.String}).write_parquet(path)


def test_replayed_tiered_universe_joins_live_targets(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), ["A", "EVENT"])
    _replay(paths, _prev(_D), ["A", "CORE"])
    _manifest(paths, _D, ["A", "EVENT"])
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert [(item.symbol, item.days) for item in targets] == [("CORE", (_D,))]


def test_replay_alone_is_a_universe_source(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _universe(paths, _prev(_D), [])
    paths.universe_out(_prev(_D)).unlink()
    _replay(paths, _prev(_D), ["CORE"])
    targets = plan_tick_tape_targets(sessions=[_D], paths=paths, ledger_done=frozenset())
    assert [item.symbol for item in targets] == ["CORE"]
