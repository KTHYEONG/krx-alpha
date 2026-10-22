"""Invariant tests for Toss 1-minute bar backfill planning and one-shot run."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib
import time
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.core.config import MinuteBackfillSettings, TossCredentials
from src.core.paths import DataPaths
from src.marketdata import minute_backfill as backfill_mod
from src.marketdata.minute_backfill import (
    MinuteTarget,
    plan_minute_targets,
    read_minute_ledger_done,
    run_minute_backfill,
)
from src.marketdata.toss_candles import MinuteCandle

_KST = ZoneInfo("Asia/Seoul")
_KST_TZ = dt.timezone(dt.timedelta(hours=9))
_D = dt.date(2026, 10, 20)
_PREV = dt.date(2026, 10, 17)
_EVENING = dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST_TZ)


@pytest.fixture(autouse=True)
def _planning_clock(monkeypatch) -> None:
    assert backfill_mod._now_kst().tzinfo is not None
    monkeypatch.setattr(backfill_mod, "_now_kst", lambda: _EVENING)


def _bars(paths: DataPaths, days: list[dt.date]) -> None:
    frame = pl.DataFrame(
        {"date": days, "symbol": ["000000"] * len(days)},
        schema={"date": pl.Date, "symbol": pl.String},
    )
    out = paths.bars_daily_dir / f"{days[0]:%Y-%m}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out)


def _universe_file(path: pathlib.Path, symbols: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}, schema={"symbol": pl.String}).write_parquet(path)


def _candle(day: dt.date, h: int, m: int) -> MinuteCandle:
    return MinuteCandle(
        ts=dt.datetime(day.year, day.month, day.day, h, m, tzinfo=_KST),
        open=100,
        high=101,
        low=99,
        close=100,
        volume=10,
    )


class _TokenResp:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"access_token": "tok"}


class _CandleResp:
    def __init__(self, candles: list[dict], calls: list[dict]):
        self._candles = candles
        self._calls = calls
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"result": {"candles": self._candles}}


class _Session:
    def __init__(self, pages: dict[str, list[list[dict]]]):
        self._pages = {symbol: list(queue) for symbol, queue in pages.items()}
        self.candle_calls: list[str] = []
        self.candle_params: list[dict] = []
        self.token_calls = 0

    def post(self, url, **kwargs):
        self.token_calls += 1
        return _TokenResp()

    def get(self, url, **kwargs):
        if "candles" not in url:
            raise AssertionError(f"unexpected url {url}")
        params = kwargs.get("params", {})
        symbol = str(params.get("symbol", ""))
        self.candle_calls.append(symbol)
        self.candle_params.append(dict(params))
        queue = self._pages.get(symbol, [])
        rows = queue.pop(0) if queue else []
        if isinstance(rows, Exception):
            raise rows
        return _CandleResp(rows, self.candle_params)


def _row(day: dt.date, h: int, m: int) -> dict:
    return {
        "timestamp": dt.datetime(day.year, day.month, day.day, h, m, tzinfo=_KST).isoformat(),
        "openPrice": 100,
        "highPrice": 101,
        "lowPrice": 99,
        "closePrice": 100,
        "volume": 10,
    }


def _settings(**overrides) -> MinuteBackfillSettings:
    return MinuteBackfillSettings(enabled=True, **overrides)


def _creds() -> TossCredentials:
    return TossCredentials(toss_app_key="k", toss_app_secret="s")


def _deadline() -> dt.datetime:
    return dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST_TZ)


def _evening_now() -> dt.datetime:
    return _EVENING


def _mute_sleep(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda seconds: None)


def _isolate(monkeypatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "tokens"))

    class _NoopLimiter:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def acquire(self) -> None:
            return None

    monkeypatch.setattr(backfill_mod, "HostPacedRateLimiter", _NoopLimiter)
    _mute_sleep(monkeypatch)


def test_live_log_preferred_over_replay(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    _universe_file(paths.universe_out(_PREV), ["LIVE_A"])
    _universe_file(paths.universe_replay_out(_PREV), ["REPLAY_B"])
    targets = plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=frozenset())
    assert [(item.symbol, dict(item.universe_sources)) for item in targets] == [
        ("LIVE_A", {_D: "live"})
    ]


def test_replay_used_when_live_missing(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    _universe_file(paths.universe_replay_out(_PREV), ["R_A"])
    targets = plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=frozenset())
    assert [(item.symbol, dict(item.universe_sources)) for item in targets] == [
        ("R_A", {_D: "replay"})
    ]


def test_ledger_settles_pairs(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    _universe_file(paths.universe_out(_PREV), ["A", "B"])
    ledger = paths.minute_history_ledger
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(
        json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "complete", "rows": 2}) + "\n",
        encoding="utf-8",
    )
    done = read_minute_ledger_done(ledger)
    assert done == frozenset({("A", _D)})
    targets = plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=done)
    assert [item.symbol for item in targets] == ["B"]


def test_partition_before_ledger_and_immutable(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 1), _row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [day_rows, prev_rows]})
    target = MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})
    original_append = backfill_mod._append_ledger_lines
    calls = {"count": 0}

    def _flaky(path, lines):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("disk full")
        return original_append(path, lines)

    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", _flaky)
    summary = run_minute_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="run1retry",
    )
    assert summary.status == "FAIL"
    assert paths.minute_history_partition(_D, "run1retry").exists()
    assert not paths.minute_history_ledger.exists()
    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", original_append)
    session = _Session({"A": [day_rows, prev_rows]})
    summary = run_minute_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="run1",
    )
    assert summary.status == "OK"
    first_bytes = paths.minute_history_partition(_D, "run1").read_bytes()
    frame = pl.read_parquet(paths.minute_history_partition(_D, "run1"))
    assert frame.columns == [
        "date", "symbol", "ts_hms", "open", "high", "low", "close", "volume",
        "vendor", "universe_source", "run_id",
    ]
    assert frame.get_column("ts_hms").to_list() == [90000, 90100]
    assert set(frame.get_column("vendor").to_list()) == {"toss"}
    session = _Session({"A": [day_rows, prev_rows]})
    rerun = run_minute_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="run2",
    )
    assert rerun.status == "OK"
    assert paths.minute_history_partition(_D, "run1").read_bytes() == first_bytes


def test_shared_toss_pacer_scope(tmp_path: pathlib.Path) -> None:
    from src.brokers.kis.rate import host_state_path

    digest = hashlib.sha256(b"k").hexdigest()[:12]
    path = host_state_path(tmp_path, "toss", "k", "MARKET_DATA_CHART")
    assert path.parent == tmp_path
    assert path.name == f"admission_toss_{digest}_MARKET_DATA_CHART.state"


def test_budget_and_deadline_stop_new_symbols(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [day_rows, prev_rows], "B": [day_rows, prev_rows]})
    targets = [
        MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"}),
        MinuteTarget(symbol="B", days=(_D,), universe_sources={_D: "live"}),
    ]
    summary = run_minute_backfill(
        targets=targets, paths=paths, settings=_settings(max_calls_per_run=2), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runbudget",
    )
    assert summary.symbols == 1
    assert session.candle_calls
    assert set(session.candle_calls) == {"A"}
    session = _Session({"A": [day_rows, prev_rows], "B": [day_rows, prev_rows]})

    def _past_deadline() -> dt.datetime:
        return _deadline()

    summary = run_minute_backfill(
        targets=targets, paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_past_deadline, run_id="rundeadline",
    )
    assert summary.symbols == 0
    assert session.candle_calls == []


def test_minute_settings_bounds() -> None:
    import pydantic

    assert MinuteBackfillSettings().rate_per_s == 8.0
    assert MinuteBackfillSettings().history_start == dt.date(2023, 3, 2)
    with pytest.raises(pydantic.ValidationError):
        MinuteBackfillSettings(rate_per_s=21.0)
    with pytest.raises(pydantic.ValidationError):
        MinuteBackfillSettings(max_calls_per_run=0)


def test_plan_without_bars_returns_empty(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    assert plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=frozenset()) == ()


def test_plan_end_before_start_returns_empty(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    assert plan_minute_targets(paths=paths, start=_D, end=_PREV, ledger_done=frozenset()) == ()


def test_plan_first_session_has_no_decision(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_D])
    _universe_file(paths.universe_out(_PREV), ["A"])
    assert plan_minute_targets(paths=paths, start=_D, end=_D, ledger_done=frozenset()) == ()


def test_plan_without_universe_files_returns_empty(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    assert plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=frozenset()) == ()


def test_plan_corrupt_live_does_not_substitute_replay(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    live = paths.universe_out(_PREV)
    live.parent.mkdir(parents=True, exist_ok=True)
    live.write_bytes(b"not a parquet file")
    _universe_file(paths.universe_replay_out(_PREV), ["R_A"])
    targets = plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=frozenset())
    assert targets == ()


def test_plan_wrong_column_universe_skipped(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    live = paths.universe_out(_PREV)
    live.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"code": ["A"]}, schema={"code": pl.String}).write_parquet(live)
    assert plan_minute_targets(paths=paths, start=_PREV, end=_D, ledger_done=frozenset()) == ()


def test_ledger_absent_returns_empty(tmp_path: pathlib.Path) -> None:
    assert read_minute_ledger_done(tmp_path / "absent.jsonl") == frozenset()


def test_ledger_edge_records(tmp_path: pathlib.Path) -> None:
    ledger = tmp_path / "edge.jsonl"
    ledger.write_text(
        "\n".join(
            [
                "",
                json.dumps([1, 2]),
                json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "edge_partial", "rows": 0}),
                json.dumps({"date": "not-a-date", "symbol": "A", "status": "complete", "rows": 1}),
                json.dumps({"date": _D.isoformat(), "symbol": "B", "status": "complete", "rows": 2}),
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert read_minute_ledger_done(ledger) == frozenset({("B", _D)})


def test_ledger_torn_final_line_tolerated(tmp_path: pathlib.Path) -> None:
    ledger = tmp_path / "torn.jsonl"
    ledger.write_text(
        json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "complete", "rows": 2}) + "\n{truncated",
        encoding="utf-8",
    )
    assert read_minute_ledger_done(ledger) == frozenset({("A", _D)})


def test_ledger_corrupt_middle_line_raises(tmp_path: pathlib.Path) -> None:
    ledger = tmp_path / "corrupt.jsonl"
    ledger.write_text("{truncated\n" + json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "complete"}) + "\n")
    with pytest.raises(ValueError, match="corrupt minute-history ledger"):
        read_minute_ledger_done(ledger)


def test_append_ledger_repairs_missing_newline(tmp_path: pathlib.Path) -> None:
    from src.marketdata.minute_backfill import _append_ledger_lines

    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "complete"}), encoding="utf-8")
    _append_ledger_lines(ledger, [{"date": _D.isoformat(), "symbol": "B", "status": "complete"}])
    rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [row["symbol"] for row in rows] == ["A", "B"]


def test_append_ledger_truncates_torn_tail(tmp_path: pathlib.Path) -> None:
    from src.marketdata.minute_backfill import _append_ledger_lines

    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        json.dumps({"date": _D.isoformat(), "symbol": "A", "status": "complete"}) + "\n{truncated",
        encoding="utf-8",
    )
    _append_ledger_lines(ledger, [{"date": _D.isoformat(), "symbol": "B", "status": "complete"}])
    rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [row["symbol"] for row in rows] == ["A", "B"]


def test_invalid_run_id_raises(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    with pytest.raises(ValueError, match="invalid minute-history run_id"):
        run_minute_backfill(
            targets=[], paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
            deadline=_deadline(), session=_Session({}), now=_evening_now, run_id="bad id!",
        )


def test_naive_deadline_raises(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    with pytest.raises(ValueError, match="timezone-aware"):
        run_minute_backfill(
            targets=[], paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
            deadline=dt.datetime(2026, 10, 22, 5, 30), session=_Session({}),
            now=_evening_now, run_id="run1",
        )


def test_disabled_settings_short_circuit(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=DataPaths(tmp_path), settings=MinuteBackfillSettings(),
        credentials=_creds(), deadline=_deadline(), session=_Session({}),
        now=_evening_now, run_id="run1",
    )
    assert summary.status == "OK"
    assert summary.reason == "disabled"
    assert summary.symbols == 0


def test_token_issuance_failure_aborts(tmp_path: pathlib.Path, monkeypatch) -> None:
    from src.marketdata.toss_calendar import TossCalendarError

    _isolate(monkeypatch, tmp_path)

    class _DeniedSession(_Session):
        def post(self, url, **kwargs):
            raise TossCalendarError("denied")

    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=_DeniedSession({}), now=_evening_now, run_id="runauth",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "auth"


def _http_error(status: int):
    import requests

    exc = requests.HTTPError(f"http {status}")
    exc.response = type("R", (), {"status_code": status})()
    return exc


def test_auth_rotation_recovers(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 1), _row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [_http_error(401), day_rows, prev_rows]})
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runrot",
    )
    assert summary.status == "OK"
    assert summary.symbol_days == 1
    assert session.token_calls == 2
    assert paths.minute_history_partition(_D, "runrot").exists()


def test_double_auth_rejection_aborts(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    session = _Session({"A": [_http_error(401), _http_error(401)]})
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="run401",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "auth"
    assert list(paths.minute_history_root.rglob("*.parquet")) == []
    assert not paths.minute_history_ledger.exists()


def test_auth_rejection_after_rotation_aborts(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [_http_error(401), day_rows, prev_rows], "B": [_http_error(401), _http_error(401)]})
    summary = run_minute_backfill(
        targets=[
            MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"}),
            MinuteTarget(symbol="B", days=(_D,), universe_sources={_D: "live"}),
        ],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runrot2",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "auth"


def test_symbol_failure_isolated(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [RuntimeError("boom")], "B": [day_rows, prev_rows]})
    summary = run_minute_backfill(
        targets=[
            MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"}),
            MinuteTarget(symbol="B", days=(_D,), universe_sources={_D: "live"}),
        ],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runiso",
    )
    assert summary.status == "OK"
    assert summary.failed == 1
    assert summary.symbol_days == 1
    frame = pl.read_parquet(paths.minute_history_partition(_D, "runiso"))
    assert set(frame.get_column("symbol").to_list()) == {"B"}


def test_boundary_mid_walk_breaks(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [day_rows, prev_rows]})
    calls = {"n": 0}

    def _clock() -> dt.datetime:
        calls["n"] += 1
        if calls["n"] > 4:
            return _deadline()
        return _EVENING

    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_clock, run_id="runbnd",
    )
    assert summary.status == "OK"
    assert summary.symbols == 1
    assert summary.calls == len(session.candle_calls)


def test_vendor_empty_run_settles_ledger(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    session = _Session({"A": [[]]})
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "replay"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runempty",
    )
    assert summary.status == "OK"
    assert summary.vendor_empty == 1
    assert summary.symbol_days == 0
    rows = [json.loads(line) for line in paths.minute_history_ledger.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["status"] == "vendor_empty"
    assert rows[0]["reason"] == "before_vendor_history"
    assert list(paths.minute_history_root.rglob("*.parquet")) == []


def test_mid_run_flush_failure_isolated(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(backfill_mod, "_FLUSH_ROW_LIMIT", 1)
    monkeypatch.setattr(
        backfill_mod, "_write_partition",
        lambda path, rows: (_ for _ in ()).throw(OSError("disk full")),
    )
    paths = DataPaths(tmp_path)
    day_rows = [_row(_D, 9, 1), _row(_D, 9, 0)]
    prev_rows = [_row(_PREV, 15, 30)]
    session = _Session({"A": [day_rows, prev_rows]})
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="runflush",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "persistence"
    assert not paths.minute_history_ledger.exists()


def test_start_day_uses_decision_outside_requested_range(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _bars(paths, [_PREV, _D])
    _universe_file(paths.universe_out(_PREV), ["A"])
    targets = plan_minute_targets(paths=paths, start=_D, end=_D, ledger_done=frozenset())
    assert [(t.symbol, t.days) for t in targets] == [("A", (_D,))]


def test_plan_excludes_future_and_unfinished_sessions(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    today = _EVENING.date()
    tomorrow = today + dt.timedelta(days=1)
    _bars(paths, [_PREV, _D, today, tomorrow])
    for day in (_PREV, _D, today):
        _universe_file(paths.universe_out(day), ["A"])
    monkeypatch.setattr(backfill_mod, "_now_kst", lambda: _EVENING.replace(hour=10))
    targets = plan_minute_targets(paths=paths, start=_D, end=tomorrow, ledger_done=frozenset())
    assert targets[0].days == (_D,)
    monkeypatch.setattr(backfill_mod, "_now_kst", lambda: _EVENING)
    targets = plan_minute_targets(paths=paths, start=_D, end=tomorrow, ledger_done=frozenset())
    assert targets[0].days == (_D, today)


def test_failed_attempt_consumes_budget_before_next_symbol(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    session = _Session({"A": [_http_error(500)], "B": [[]]})
    summary = run_minute_backfill(
        targets=[MinuteTarget(s, (_D,), {_D: "live"}) for s in ("A", "B")],
        paths=DataPaths(tmp_path), settings=_settings(max_calls_per_run=1), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="failed_budget",
    )
    assert summary.calls == 1
    assert summary.failed == 1
    assert session.candle_calls == ["A"]


def test_auth_retry_is_paced_and_counted(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    admissions = []

    class Limiter:
        def __init__(self, path, rate):
            assert path.name.endswith("_MARKET_DATA_CHART.state")
            assert rate == 8.0

        def acquire(self):
            admissions.append("slot")

    monkeypatch.setattr(backfill_mod, "HostPacedRateLimiter", Limiter)
    session = _Session({"A": [_http_error(401), [_row(_D, 9, 0)], [_row(_PREV, 15, 30)]]})
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_D,), {_D: "live"})],
        paths=DataPaths(tmp_path), settings=_settings(max_calls_per_run=3), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="paced_retry",
    )
    assert summary.calls == 3
    assert len(admissions) == 3
    assert summary.symbol_days == 1


def test_auth_at_budget_does_not_issue_another_token(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    session = _Session({"A": [_http_error(401), []]})
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_D,), {_D: "live"})],
        paths=DataPaths(tmp_path), settings=_settings(max_calls_per_run=1), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="auth_budget",
    )
    assert summary.calls == 1
    assert session.token_calls == 1
    assert session.candle_calls == ["A"]


def test_deadline_keeps_already_certified_days(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    paths = DataPaths(tmp_path)
    session = _Session({"A": [[_row(_D, 9, 0)], [_row(_PREV, 15, 30)]]})

    def clock():
        return _deadline() if len(session.candle_calls) == 2 else _EVENING

    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_PREV, _D), {_PREV: "replay", _D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=clock, run_id="partial_walk",
    )
    assert summary.calls == 2
    assert read_minute_ledger_done(paths.minute_history_ledger) == frozenset({("A", _D)})
    assert paths.minute_history_partition(_D, "partial_walk").exists()
    assert not paths.minute_history_partition(_PREV, "partial_walk").exists()


def test_market_freeze_admits_no_requests(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    session = _Session({"A": [[]]})
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_D,), {_D: "live"})],
        paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=lambda: _EVENING.replace(hour=10), run_id="freeze",
    )
    assert summary.calls == 0
    assert session.token_calls == 0


def test_rotation_failure_aborts_remaining_symbols(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)

    class Session(_Session):
        def post(self, url, **kwargs):
            if self.token_calls:
                raise RuntimeError("issuer unavailable")
            return super().post(url, **kwargs)

    session = Session({"A": [_http_error(401)], "B": [[]]})
    summary = run_minute_backfill(
        targets=[MinuteTarget(s, (_D,), {_D: "live"}) for s in ("A", "B")],
        paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="rotation_failure",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "auth"
    assert session.candle_calls == ["A"]


def test_successful_chunk_flush_preserves_all_symbols(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    monkeypatch.setattr(backfill_mod, "_FLUSH_ROW_LIMIT", 1)
    paths = DataPaths(tmp_path)
    session = _Session({s: [[_row(_D, 9, 1), _row(_D, 9, 0)], [_row(_PREV, 15, 30)]] for s in ("A", "B")})
    summary = run_minute_backfill(
        targets=[MinuteTarget(s, (_D,), {_D: "live"}) for s in ("A", "B")],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=_evening_now, run_id="chunks",
    )
    assert summary.symbol_days == 2
    assert pl.read_parquet(paths.minute_history_partition(_D, "chunks")).height == 4
    assert read_minute_ledger_done(paths.minute_history_ledger) == frozenset({("A", _D), ("B", _D)})


def test_deadline_between_admission_and_transport_is_fail_closed(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, tmp_path)
    after_admission = []

    class Limiter:
        def __init__(self, *args):
            return None

        def acquire(self):
            after_admission.extend([_EVENING, _deadline()])

    def clock():
        return after_admission.pop(0) if after_admission else _EVENING

    monkeypatch.setattr(backfill_mod, "HostPacedRateLimiter", Limiter)
    session = _Session({"A": [[]]})
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_D,), {_D: "live"})],
        paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=session, now=clock, run_id="admission_deadline",
    )
    assert summary.calls == 0
    assert session.candle_calls == []


def test_summary_info_log_includes_offhours_count(tmp_path: pathlib.Path, monkeypatch, caplog) -> None:
    _isolate(monkeypatch, tmp_path)
    session = _Session({"A": [[_row(_D, 9, 0), _row(_D, 8, 30)], [_row(_PREV, 15, 30)]]})
    with caplog.at_level("INFO", logger=backfill_mod.__name__):
        summary = run_minute_backfill(
            targets=[MinuteTarget("A", (_D,), {_D: "live"})],
            paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
            deadline=_deadline(), session=session, now=_evening_now, run_id="log_summary",
        )
    assert summary.discarded_offhours == 1
    assert "symbols=1 symbol_days=1 calls=2 vendor_empty=0 failed=0 discarded_offhours=1" in caplog.text
