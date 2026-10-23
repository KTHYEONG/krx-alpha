"""Losslessness, request-boundary and calendar regression checks for tick tapes."""

from __future__ import annotations

import datetime as dt
import json

import polars as pl
import pytest

from src.core.paths import DataPaths
from src.marketdata import tick_tape_backfill as module
from src.marketdata.kiwoom_ticks import KiwoomTickTapeError, TapeTick, TickPage, fetch_tick_page, walk_symbol_tape
from tests.unit.marketdata.test_tick_tape_backfill import (
    _ChartResp, _Session, _creds, _deadline, _evening_now, _mute_sleep, _rows, _settings, _walk_day_pages,
)

DAY = dt.date(2026, 10, 20)
OLDER = DAY - dt.timedelta(days=1)


def _run(paths, session, **kwargs):
    defaults = {
        "targets": [module.TickTapeTarget("A", (DAY,), 1.0)],
        "paths": paths, "settings": _settings(), "credentials": _creds(),
        "deadline": _deadline(), "session": session, "now": _evening_now, "run_id": "regression",
    }
    defaults.update(kwargs)
    return module.run_tick_tape_backfill(**defaults)


def test_threshold_flush_preserves_every_symbol_and_ledger_count(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    monkeypatch.setattr(module, "_FLUSH_ROW_LIMIT", 1)
    paths = DataPaths(tmp_path)
    session = _Session(pages={s: _walk_day_pages(DAY, OLDER) for s in ("A", "B", "C")})
    result = _run(paths, session, targets=[module.TickTapeTarget(s, (DAY,), 1.0) for s in ("A", "B", "C")])
    labels = ["regression", "regression-1", "regression-2"]
    parts = [paths.tick_tape_partition(DAY, label) for label in labels]
    assert all(part.exists() for part in parts)
    assert not paths.tick_tape_partition(DAY, "regression-3").exists()
    frame = pl.concat([pl.read_parquet(part) for part in parts])
    ledger = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert result.symbol_days == 3
    assert frame.height == 6
    assert frame.group_by("symbol").len().sort("symbol").rows() == [("A", 2), ("B", 2), ("C", 2)]
    assert {row["symbol"]: row["rows"] for row in ledger} == {"A": 2, "B": 2, "C": 2}
    assert all(paths.tick_tape_partition(DAY, row["run_id"]).exists() for row in ledger)
    original = [part.read_bytes() for part in parts]
    with pytest.raises(FileExistsError):
        _run(paths, session)
    assert [part.read_bytes() for part in parts] == original


def test_budget_counts_failed_http_attempts(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    session = _Session(pages={"A": [_ChartResp([], status=429)] * 5, "B": _walk_day_pages(DAY, OLDER)})
    result = _run(DataPaths(tmp_path), session, settings=_settings(max_pages_per_run=2),
                  targets=[module.TickTapeTarget(s, (DAY,), 1.0) for s in ("A", "B")])
    assert result.pages == 2
    assert session.chart_calls == [("A", None), ("A", None)]
    assert result.symbol_days == 0


def test_inflight_deadline_persists_only_already_crossed_days(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    older_target = DAY - dt.timedelta(days=3)
    session = _Session(pages={"A": [
        _ChartResp(_rows(DAY, 2, 150000), cont="Y", next_key="1"),
        _ChartResp(_rows(OLDER, 1), cont="Y", next_key="2"),
        _ChartResp(_rows(older_target, 1), cont="N"),
    ]})

    def clock():
        return _deadline() if len(session.chart_calls) == 2 else _evening_now()

    result = _run(paths, session, now=clock, targets=[module.TickTapeTarget("A", (older_target, DAY), 1.0)])
    assert result.pages == 2
    assert module.read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", DAY)})
    assert not paths.tick_tape_partition(older_target, "regression").exists()


def test_pacer_wait_rechecks_deadline_before_http(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    expired = False

    def acquire(self):
        nonlocal expired
        expired = True

    monkeypatch.setattr(module.HostPacedRateLimiter, "acquire", acquire)
    session = _Session(pages={"A": _walk_day_pages(DAY, OLDER)})
    result = _run(DataPaths(tmp_path), session, now=lambda: _deadline() if expired else _evening_now())
    assert result.pages == 0
    assert session.chart_calls == []


def test_token_issuance_failure_aborts_instead_of_retrying_each_symbol(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    session = _Session(token_behavior="deny")
    result = _run(DataPaths(tmp_path), session, targets=[module.TickTapeTarget(s, (DAY,), 1.0) for s in ("A", "B")])
    assert result.status == "FAIL"
    assert result.reason == "auth"
    assert session.token_calls == 1


def test_disabled_and_outside_window_never_issue_tokens(tmp_path):
    from src.core.config import TickTapeBackfillSettings

    session = _Session()
    assert _run(DataPaths(tmp_path), session, settings=TickTapeBackfillSettings()).reason == "disabled"
    result = _run(DataPaths(tmp_path), session, now=lambda: _evening_now().replace(hour=12))
    assert result.pages == 0
    assert session.token_calls == 0


def test_torn_utf8_ledger_tail_is_repaired_before_append(tmp_path):
    ledger = tmp_path / "ledger.jsonl"
    record = {"symbol": "A", "date": DAY.isoformat(), "status": "complete", "rows": 2}
    ledger.write_bytes(json.dumps(record).encode() + b'\n{"return_msg":"\xed')
    assert module.read_ledger_done(ledger) == frozenset({("A", DAY)})
    record["symbol"] = "B"
    module._append_ledger_lines(ledger, [record])
    assert module.read_ledger_done(ledger) == frozenset({("A", DAY), ("B", DAY)})
    assert len([json.loads(line) for line in ledger.read_text().splitlines()]) == 2


def test_publish_failure_never_creates_a_settled_ledger(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    original = module._publish_partition

    def fail_final(tmp, target):
        if target.is_relative_to(paths.tick_tape_root):
            raise OSError("fsync failure")
        original(tmp, target)

    monkeypatch.setattr(module, "_publish_partition", fail_final)
    result = _run(paths, _Session(pages={"A": _walk_day_pages(DAY, OLDER)}))
    assert result.status == "FAIL"
    assert result.symbol_days == 0
    assert not paths.tick_tape_ledger.exists()


def test_holiday_universe_uses_last_actual_session(tmp_path):
    from src.cli.backfill_tick_tape import resolve_backfill_sessions
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    paths = DataPaths(tmp_path)
    sessions = [dt.date(2026, 9, 30), dt.date(2026, 10, 1), dt.date(2026, 10, 2), dt.date(2026, 10, 6)]
    paths.bars_daily_dir.mkdir(parents=True)
    for month in ("2026-09", "2026-10"):
        dates = [d for d in sessions if d.strftime("%Y-%m") == month]
        pl.DataFrame({"date": dates, "symbol": ["A"] * len(dates), "trade_value_100m": [1.0] * len(dates)}).write_parquet(paths.bars_daily_dir / f"{month}.parquet")
    today = dt.date(2026, 10, 7)
    save_trading_day_cache(paths.calendar_cache, TradingDay(today, True, sessions[-1], today + dt.timedelta(days=1)))
    paths.universe_dir.mkdir()
    pl.DataFrame({"symbol": ["A"]}).write_parquet(paths.universe_out(dt.date(2026, 10, 2)))
    assert resolve_backfill_sessions(paths, 2, today) == (sessions[-2], sessions[-1])
    targets = module.plan_tick_tape_targets(sessions=[sessions[-1]], paths=paths, ledger_done=frozenset())
    assert targets == (module.TickTapeTarget("A", (sessions[-1],), 1.0),)


@pytest.mark.parametrize(("body", "status"), [({"return_code": 0}, 200), ({"return_code": 0, "stk_tic_chart_qry": []}, 500)])
def test_malformed_or_http_failure_never_means_empty_tape(body, status):
    session = _Session(pages={"A": [_ChartResp(body, status=status)]})
    with pytest.raises(KiwoomTickTapeError):
        fetch_tick_page("A", access_token="t", app_key="k", next_key=None, session=session, timeout_s=1)


def test_invalid_timestamp_and_reordered_cursor_fail_closed():
    session = _Session(pages={"A": [_ChartResp([{"cntr_tm": "20261020256061", "cur_prc": "100", "trde_qty": "1"}])]})
    with pytest.raises(KiwoomTickTapeError, match="invalid time"):
        fetch_tick_page("A", access_token="t", app_key="k", next_key=None, session=session, timeout_s=1)
    pages = iter([TickPage((TapeTick(DAY, 90000, 1, 1),), "1"), TickPage((TapeTick(DAY, 100000, 1, 1),), None)])
    with pytest.raises(KiwoomTickTapeError, match="newest-first"):
        walk_symbol_tape("A", wanted_days=frozenset({DAY}), fetch_page=lambda key: next(pages), max_pages=5)


def test_same_second_vendor_order_reverses_without_deduplication():
    ticks = (TapeTick(DAY, 90000, 3, 1), TapeTick(DAY, 90000, 2, 1), TapeTick(DAY, 90000, 2, 1), TapeTick(OLDER, 150000, 1, 1))
    walk = walk_symbol_tape("A", wanted_days=frozenset({DAY}), fetch_page=lambda key: TickPage(ticks, None), max_pages=1)
    assert [tick.price for tick in walk.complete[DAY]] == [2, 2, 3]


def test_missing_calendar_and_corrupt_ledger_fail_closed(tmp_path):
    paths = DataPaths(tmp_path)
    assert module.tape_calendar_sessions(paths, DAY) == ()
    with pytest.raises(ValueError, match="calendar unavailable"):
        module.plan_tick_tape_targets(sessions=[DAY], paths=paths, ledger_done=frozenset())
    ledger = tmp_path / "bad.jsonl"
    ledger.write_bytes(b'{broken}\n')
    with pytest.raises(ValueError, match="corrupt tick-tape ledger"):
        module.read_ledger_done(ledger)


def test_append_preserves_valid_final_record_without_newline(tmp_path):
    ledger = tmp_path / "valid.jsonl"
    record = {"symbol": "A", "date": DAY.isoformat(), "status": "complete", "rows": 1}
    ledger.write_text(json.dumps(record))
    module._append_ledger_lines(ledger, [dict(record, symbol="B")])
    assert module.read_ledger_done(ledger) == frozenset({("A", DAY), ("B", DAY)})


def test_invalid_run_identifiers_and_naive_deadlines_rejected(tmp_path):
    with pytest.raises(ValueError, match="run_id"):
        _run(DataPaths(tmp_path), _Session(), run_id="../../unsafe")
    with pytest.raises(ValueError, match="timezone-aware"):
        _run(DataPaths(tmp_path), _Session(), deadline=_deadline().replace(tzinfo=None))


def test_staging_io_failure_aborts_without_settling_any_day(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    monkeypatch.setattr(module, "_FLUSH_ROW_LIMIT", 1)

    def fail_stage(path, symbol, day, ticks, label):
        raise OSError("disk full")

    monkeypatch.setattr(module, "_write_day_chunk", fail_stage)
    paths = DataPaths(tmp_path)
    session = _Session(pages={s: _walk_day_pages(DAY, OLDER) for s in ("A", "B")})
    result = _run(paths, session, targets=[module.TickTapeTarget(s, (DAY,), 1.0) for s in ("A", "B")])
    assert result.reason == "persistence"
    assert result.symbol_days == 0
    assert not paths.tick_tape_ledger.exists()
    assert [symbol for symbol, _key in session.chart_calls] == ["A", "A"]


def test_repeated_cursor_cannot_certify_a_day():
    page = TickPage((TapeTick(DAY, 90000, 1, 1),), "same")
    with pytest.raises(KiwoomTickTapeError, match="no progress"):
        walk_symbol_tape("A", wanted_days=frozenset({DAY}), fetch_page=lambda key: page, max_pages=5)


def test_token_http_error_with_success_envelope_rejected():
    from src.marketdata.kiwoom_ticks import issue_kiwoom_token

    class Session:
        def post(self, *args, **kwargs):
            return _ChartResp({"return_code": 0, "token": "must-not-be-used"}, status=500)

    with pytest.raises(KiwoomTickTapeError, match="HTTP failure"):
        issue_kiwoom_token(app_key="k", secret_key="s", session=Session(), timeout_s=1)


def test_streamed_days_survive_a_later_symbol_crash_via_checkpoint(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    monkeypatch.setattr(module, "_FLUSH_ROW_LIMIT", 1)
    paths = DataPaths(tmp_path)

    class _Crash(BaseException):  # noqa: N818 - simulates a hard kill that bypasses `except Exception`
        pass

    session = _Session(pages={"A": _walk_day_pages(DAY, OLDER), "B": _walk_day_pages(DAY, OLDER)})
    original_post = session.post

    def post(url, **kwargs):
        if kwargs.get("json", {}).get("stk_cd") == "B":
            raise _Crash
        return original_post(url, **kwargs)

    session.post = post
    with pytest.raises(_Crash):
        _run(paths, session, targets=[module.TickTapeTarget(s, (DAY,), 1.0) for s in ("A", "B")])
    assert module.read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", DAY)})
    assert pl.read_parquet(paths.tick_tape_partition(DAY, "regression"))["symbol"].unique().to_list() == ["A"]


def test_symbol_failure_discards_its_staged_days(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)

    def walk_then_fail(*args, on_day_complete, **kwargs):
        on_day_complete(DAY, (TapeTick(DAY, 90000, 1, 1),))
        raise KiwoomTickTapeError("tape is not newest-first")

    monkeypatch.setattr(module, "walk_symbol_tape", walk_then_fail)
    result = _run(paths, _Session(), targets=[module.TickTapeTarget("A", (DAY,), 1.0)])
    assert result.failed == 1
    assert result.symbol_days == 0
    assert not paths.tick_tape_ledger.exists()
    assert list((paths.work_root / "tick_tape").rglob("*.parquet")) == []


def test_walk_emits_certified_days_and_frees_them():
    emitted = []
    pages = iter(
        [
            TickPage((TapeTick(DAY, 90001, 2, 1), TapeTick(DAY, 90000, 1, 1)), "1"),
            TickPage((TapeTick(OLDER, 150000, 3, 1),), None),
        ]
    )
    walk = walk_symbol_tape(
        "A", wanted_days=frozenset({DAY, OLDER}), fetch_page=lambda key: next(pages), max_pages=5,
        on_day_complete=lambda day, ticks: emitted.append((day, [t.price for t in ticks])),
    )
    assert emitted == [(DAY, [1, 2])]
    assert walk.emitted == frozenset({DAY})
    assert DAY not in walk.complete
    assert walk.edge_partial == frozenset({OLDER})
    assert DAY not in walk.vendor_empty


def _big_rows(day: dt.date, count: int) -> list[dict]:
    return [
        {"cntr_tm": f"{day:%Y%m%d}{90000 + count - 1 - idx:06d}", "cur_prc": "100", "trde_qty": "2"}
        for idx in range(count)
    ]


def test_oversize_day_is_retried_not_settled(tmp_path, monkeypatch, caplog):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    older = DAY - dt.timedelta(days=1)
    floor = older - dt.timedelta(days=1)
    session = _Session(pages={
        "A": [
            _ChartResp(_rows(DAY, 2), cont="Y", next_key="k1"),
            _ChartResp(_rows(older, 2), cont="Y", next_key="k2"),
            _ChartResp(_big_rows(older, 1), cont="Y", next_key="k3"),
            _ChartResp(_rows(floor, 1), cont="N"),
        ],
        "B": _walk_day_pages(DAY, older),
    })
    settings = _settings(max_ticks_per_day=2)
    with caplog.at_level("WARNING"):
        result = _run(paths, session, settings=settings,
                      targets=[module.TickTapeTarget("A", (floor, older, DAY), 1.0),
                               module.TickTapeTarget("B", (DAY,), 1.0)])
    assert result.oversize == 1
    assert result.failed == 0
    assert module.read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", DAY), ("B", DAY)})
    records = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert all(row["date"] == DAY.isoformat() for row in records)
    assert pl.read_parquet(paths.tick_tape_partition(DAY, "regression")).height == 4
    assert session.chart_calls == [("A", None), ("A", "k1"), ("A", "k2"), ("B", None), ("B", "k1")]
    assert any("status=OVERSIZE" in r.getMessage() for r in caplog.records)


def test_stop_request_ends_through_final_checkpoint(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"A": _walk_day_pages(DAY, OLDER), "B": _walk_day_pages(DAY, OLDER)})
    stop_after_a = lambda: len(session.chart_calls) >= 2  # noqa: E731
    result = _run(paths, session, should_stop=stop_after_a,
                  targets=[module.TickTapeTarget(s, (DAY,), 1.0) for s in ("A", "B")])
    assert result.status == "OK"
    assert result.reason == "stop_requested"
    assert module.read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", DAY)})
    assert paths.tick_tape_partition(DAY, "regression").exists()
    assert [call[0] for call in session.chart_calls] == ["A", "A"]


def test_stop_before_first_request(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"A": _walk_day_pages(DAY, OLDER)})
    result = _run(paths, session, should_stop=lambda: True)
    assert result.status == "OK"
    assert result.reason == "stop_requested"
    assert session.chart_calls == []
    assert session.token_calls == 0
    assert not paths.tick_tape_ledger.exists()


def test_heartbeat_cadence(tmp_path, monkeypatch, caplog):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    pages = [
        _ChartResp(
            [{"cntr_tm": f"{DAY:%Y%m%d}{90003 - idx:06d}", "cur_prc": "712345", "trde_qty": "654321"}],
            cont="Y", next_key=f"k{idx}")
        for idx in range(4)
    ]
    pages.append(_ChartResp(_rows(OLDER, 1), cont="N"))
    session = _Session(pages={"A": pages})
    settings = _settings(progress_every_pages=2)
    with caplog.at_level("INFO"):
        _run(paths, session, settings=settings)
    progress = [r for r in caplog.records if "stage=tick_tape_progress" in r.getMessage()]
    assert len(progress) == 2
    for record in progress:
        message = record.getMessage()
        assert "712345" not in message
        assert "654321" not in message


def test_default_stop_is_inert(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"A": _walk_day_pages(DAY, OLDER)})
    result = _run(paths, session)
    assert result.reason is None
    assert result.status == "OK"
    assert module.read_ledger_done(paths.tick_tape_ledger) == frozenset({("A", DAY)})


def test_inflight_stop_is_latched_and_preserves_certified_days(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    floor = OLDER - dt.timedelta(days=1)
    session = _Session(pages={"A": [
        _ChartResp(_rows(DAY, 2), cont="Y", next_key="1"),
        _ChartResp(_rows(OLDER, 1), cont="Y", next_key="2"),
        _ChartResp(_rows(floor, 1), cont="N"),
    ]})
    observed = False

    def should_stop():
        nonlocal observed
        if len(session.chart_calls) == 2 and not observed:
            observed = True
            return True
        return False

    result = _run(paths, session, should_stop=should_stop,
                  targets=[module.TickTapeTarget("A", (floor, OLDER, DAY), 1.0)])
    assert (result.status, result.reason, result.failed) == ("OK", "stop_requested", 0)
    assert len(session.chart_calls) == 2
    ledger = [json.loads(line) for line in paths.tick_tape_ledger.read_text().splitlines()]
    assert [(row["symbol"], row["date"], row["rows"]) for row in ledger] == [("A", DAY.isoformat(), 2)]
    assert pl.read_parquet(paths.tick_tape_partition(DAY, "regression")).height == 2


def test_stop_is_not_polled_during_or_after_final_checkpoint(tmp_path, monkeypatch):
    _mute_sleep(monkeypatch)
    paths = DataPaths(tmp_path)
    session = _Session(pages={"A": _walk_day_pages(DAY, OLDER)})
    checkpoint_started = False
    original = module._publish_partition

    def publish(tmp, target):
        nonlocal checkpoint_started
        checkpoint_started = True
        original(tmp, target)

    def should_stop():
        assert not checkpoint_started
        return False

    monkeypatch.setattr(module, "_publish_partition", publish)
    result = _run(paths, session, should_stop=should_stop)
    assert checkpoint_started
    assert (result.status, result.reason, result.symbol_days) == ("OK", None, 1)


@pytest.mark.parametrize("field", ["max_ticks_per_day", "progress_every_pages"])
def test_hardening_settings_are_env_overridable_and_positive(monkeypatch, field):
    from pydantic import ValidationError

    monkeypatch.setenv(f"KRX_ALPHA_TICK_TAPE_{field.upper()}", "7")
    assert getattr(_settings(), field) == 7
    with pytest.raises(ValidationError):
        _settings(**{field: 0})
