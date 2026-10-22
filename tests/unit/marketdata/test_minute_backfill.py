"""Invariant tests for KIS 1-minute bar backfill planning and one-shot run."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.core.config import MinuteBackfillSettings
from src.core.errors import MissingCredentialsError
from src.core.paths import DataPaths
from src.marketdata import minute_backfill as backfill_mod
from src.marketdata.minute_backfill import (
    MinuteTarget,
    plan_minute_targets,
    read_minute_ledger_done,
    run_minute_backfill,
)
from src.brokers.kis.data import KisMinuteBar
from src.realtime.kis_sharding import KisDataCredential

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


def _bar(day: dt.date, h: int, m: int) -> KisMinuteBar:
    return KisMinuteBar(
        day=day, ts_hms=h * 10000 + m * 100, open=100, high=101, low=99, close=100, volume=10
    )


def _minute_range(day: dt.date, count: int) -> list[KisMinuteBar]:
    base = dt.datetime(day.year, day.month, day.day, 9, 0)
    out: list[KisMinuteBar] = []
    for idx in range(count):
        moment = base + dt.timedelta(minutes=idx)
        out.append(
            KisMinuteBar(
                day=day,
                ts_hms=moment.hour * 10000 + moment.minute * 100,
                open=100, high=101, low=99, close=100, volume=10,
            )
        )
    return out


class _FakeKisData:
    def __init__(self, pages: dict[tuple[str, dt.date], list]) -> None:
        self._pages = {key: list(queue) for key, queue in pages.items()}
        self.calls: list[tuple[str, dt.date, int]] = []

    def get_historical_minute_bars(self, symbol: str, *, day: dt.date, before_hms: int):
        self.calls.append((symbol, day, before_hms))
        queue = self._pages.get((symbol, day), [])
        page = queue.pop(0) if queue else ()
        if isinstance(page, Exception):
            raise page
        return tuple(bar for bar in page if bar.ts_hms <= before_hms)

    def get(self, url, *, symbol, day, before_hms, timeout):
        assert timeout > 0
        return self.get_historical_minute_bars(symbol, day=day, before_hms=before_hms)


def _settings(**overrides) -> MinuteBackfillSettings:
    return MinuteBackfillSettings(enabled=True, **overrides)


def _creds(slot: str = "5") -> tuple[KisDataCredential, ...]:
    return (KisDataCredential(slot=slot, app_key="k", app_secret="s", hts_id="h", key_id="id"),)


def _deadline() -> dt.datetime:
    return dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST_TZ)


def _evening_now() -> dt.datetime:
    return _EVENING


def _isolate(monkeypatch, fake: _FakeKisData) -> None:
    def _builder(**kwargs):
        def fetch(symbol, *, day, before_hms):
            return kwargs["session"].get("minute", symbol=symbol, day=day, before_hms=before_hms)

        return SimpleNamespace(data=SimpleNamespace(get_historical_minute_bars=fetch))

    monkeypatch.setattr(backfill_mod, "build_kis_rest_stack", _builder)


def _day_pages(day: dt.date) -> list:
    bars = sorted(_minute_range(day, 391), key=lambda b: b.ts_hms, reverse=True)
    return [bars[i : i + 120] for i in range(0, len(bars), 120)]


def test_no_toss_reference() -> None:
    source = pathlib.Path(backfill_mod.__file__).read_text(encoding="utf-8")
    assert "toss_candles" not in source
    assert "toss_token_store" not in source
    assert backfill_mod.MINUTE_VENDOR == "kis"


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
    paths = DataPaths(tmp_path)
    page = [_bar(_D, 9, 1), _bar(_D, 9, 0)]
    fake = _FakeKisData({("A", _D): [list(reversed(page))]})
    _isolate(monkeypatch, fake)
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
        deadline=_deadline(), session=fake, now=_evening_now, run_id="run1retry",
    )
    assert summary.status == "FAIL"
    assert paths.minute_history_partition(_D, "run1retry").exists()
    assert not paths.minute_history_ledger.exists()
    monkeypatch.setattr(backfill_mod, "_append_ledger_lines", original_append)
    fake2 = _FakeKisData({("A", _D): [list(reversed(page))]})
    _isolate(monkeypatch, fake2)
    summary = run_minute_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake2, now=_evening_now, run_id="run1",
    )
    assert summary.status == "OK"
    first_bytes = paths.minute_history_partition(_D, "run1").read_bytes()
    frame = pl.read_parquet(paths.minute_history_partition(_D, "run1"))
    assert frame.columns == [
        "date", "symbol", "ts_hms", "open", "high", "low", "close", "volume",
        "vendor", "venue", "universe_source", "run_id",
    ]
    assert frame.get_column("ts_hms").to_list() == [90000, 90100]
    assert set(frame.get_column("vendor").to_list()) == {"kis"}
    assert set(frame.get_column("venue").to_list()) == {"KRX"}
    assert str(paths.minute_history_partition(_D, "run1")).replace("\\", "/").count("history/minute/kis") == 1
    assert paths.minute_history_ledger.name == "kis_ledger.jsonl"
    fake3 = _FakeKisData({("A", _D): [list(reversed(page))]})
    _isolate(monkeypatch, fake3)
    rerun = run_minute_backfill(
        targets=[target], paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake3, now=_evening_now, run_id="run2",
    )
    assert rerun.status == "OK"
    assert paths.minute_history_partition(_D, "run1").read_bytes() == first_bytes


def test_budget_and_deadline_stop_new_symbols(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({
        ("A", _D): [[_bar(_D, 9, 0)]],
        ("B", _D): [[_bar(_D, 9, 0)]],
    })
    _isolate(monkeypatch, fake)
    targets = [
        MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"}),
        MinuteTarget(symbol="B", days=(_D,), universe_sources={_D: "live"}),
    ]
    summary = run_minute_backfill(
        targets=targets, paths=paths, settings=_settings(max_calls_per_run=1), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="runbudget",
    )
    assert summary.symbols == 1
    assert [call[0] for call in fake.calls] == ["A"]

    def _past_deadline() -> dt.datetime:
        return _deadline()

    fake2 = _FakeKisData({("A", _D): [[_bar(_D, 9, 0)]]})
    _isolate(monkeypatch, fake2)
    summary = run_minute_backfill(
        targets=targets, paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake2, now=_past_deadline, run_id="rundeadline",
    )
    assert summary.symbols == 0
    assert fake2.calls == []


def test_minute_settings_bounds() -> None:
    import pydantic

    assert MinuteBackfillSettings().rate_per_s == 8.0
    assert MinuteBackfillSettings().history_start == dt.date(2025, 10, 27)
    assert MinuteBackfillSettings().kis_data_slot == 5
    with pytest.raises(pydantic.ValidationError):
        MinuteBackfillSettings(rate_per_s=15.5)
    with pytest.raises(pydantic.ValidationError):
        MinuteBackfillSettings(max_calls_per_run=0)
    with pytest.raises(pydantic.ValidationError):
        MinuteBackfillSettings(kis_data_slot=6)


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
    _isolate(monkeypatch, _FakeKisData({}))
    with pytest.raises(ValueError, match="invalid minute-history run_id"):
        run_minute_backfill(
            targets=[], paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
            deadline=_deadline(), session=object(), now=_evening_now, run_id="bad id!",
        )


def test_naive_deadline_raises(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, _FakeKisData({}))
    with pytest.raises(ValueError, match="timezone-aware"):
        run_minute_backfill(
            targets=[], paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
            deadline=dt.datetime(2026, 10, 22, 5, 30), session=object(),
            now=_evening_now, run_id="run1",
        )


def test_disabled_settings_short_circuit(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, _FakeKisData({}))
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=DataPaths(tmp_path), settings=MinuteBackfillSettings(),
        credentials=_creds(), deadline=_deadline(), session=object(),
        now=_evening_now, run_id="run1",
    )
    assert summary.status == "OK"
    assert summary.reason == "disabled"
    assert summary.symbols == 0


def test_missing_slot_credential_raises(tmp_path: pathlib.Path, monkeypatch) -> None:
    _isolate(monkeypatch, _FakeKisData({}))
    with pytest.raises(MissingCredentialsError):
        run_minute_backfill(
            targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
            paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(slot="1"),
            deadline=_deadline(), session=object(), now=_evening_now, run_id="run1",
        )


def test_symbol_failure_isolated(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({
        ("A", _D): [RuntimeError("boom")],
        ("B", _D): [[_bar(_D, 9, 0)]],
    })
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[
            MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"}),
            MinuteTarget(symbol="B", days=(_D,), universe_sources={_D: "live"}),
        ],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="runiso",
    )
    assert summary.status == "OK"
    assert summary.failed == 1
    assert summary.symbol_days == 1
    frame = pl.read_parquet(paths.minute_history_partition(_D, "runiso"))
    assert set(frame.get_column("symbol").to_list()) == {"B"}


def test_vendor_empty_run_settles_ledger(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({("A", _D): [[]]})
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "replay"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="runempty",
    )
    assert summary.status == "OK"
    assert summary.vendor_empty == 1
    assert summary.symbol_days == 0
    rows = [json.loads(line) for line in paths.minute_history_ledger.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["status"] == "vendor_empty"
    assert rows[0]["reason"] == "no_bars"
    assert list(paths.minute_history_root.rglob("*.parquet")) == []


def test_budget_exhaustion_is_incomplete_and_unwritten(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({("A", _D): _day_pages(_D)})
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=paths, settings=_settings(max_calls_per_run=2), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="runincomplete",
    )
    assert summary.status == "OK"
    assert summary.symbol_days == 0
    assert summary.vendor_empty == 0
    assert list(paths.minute_history_root.rglob("*.parquet")) == []
    if paths.minute_history_ledger.exists():
        assert paths.minute_history_ledger.read_text(encoding="utf-8").strip() == ""


def test_mid_run_flush_failure_isolated(tmp_path: pathlib.Path, monkeypatch) -> None:
    fake = _FakeKisData({("A", _D): [[_bar(_D, 9, 1), _bar(_D, 9, 0)]]})
    _isolate(monkeypatch, fake)
    monkeypatch.setattr(backfill_mod, "_FLUSH_ROW_LIMIT", 1)
    monkeypatch.setattr(
        backfill_mod, "_write_partition",
        lambda path, rows: (_ for _ in ()).throw(OSError("disk full")),
    )
    paths = DataPaths(tmp_path)
    summary = run_minute_backfill(
        targets=[MinuteTarget(symbol="A", days=(_D,), universe_sources={_D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="runflush",
    )
    assert summary.status == "FAIL"
    assert summary.reason == "persistence"
    assert not paths.minute_history_ledger.exists()


def test_market_freeze_admits_no_requests(tmp_path: pathlib.Path, monkeypatch) -> None:
    fake = _FakeKisData({("A", _D): [[]]})
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_D,), {_D: "live"})],
        paths=DataPaths(tmp_path), settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake, now=lambda: _EVENING.replace(hour=10), run_id="freeze",
    )
    assert summary.calls == 0
    assert fake.calls == []


def test_successful_chunk_flush_preserves_all_symbols(tmp_path: pathlib.Path, monkeypatch) -> None:
    monkeypatch.setattr(backfill_mod, "_FLUSH_ROW_LIMIT", 1)
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({
        ("A", _D): [[_bar(_D, 9, 1), _bar(_D, 9, 0)]],
        ("B", _D): [[_bar(_D, 9, 1), _bar(_D, 9, 0)]],
    })
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[MinuteTarget(s, (_D,), {_D: "live"}) for s in ("A", "B")],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="chunks",
    )
    assert summary.symbol_days == 2
    assert pl.read_parquet(paths.minute_history_partition(_D, "chunks")).height == 4
    assert read_minute_ledger_done(paths.minute_history_ledger) == frozenset({("A", _D), ("B", _D)})


def test_deadline_keeps_already_certified_days(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({
        ("A", _PREV): [[_bar(_PREV, 9, 0)]],
        ("A", _D): [[_bar(_D, 9, 0)]],
    })
    _isolate(monkeypatch, fake)

    def clock() -> dt.datetime:
        return _deadline() if fake.calls else _EVENING

    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_PREV, _D), {_PREV: "replay", _D: "live"})],
        paths=paths, settings=_settings(), credentials=_creds(),
        deadline=_deadline(), session=fake, now=clock, run_id="partial_walk",
    )
    assert summary.calls == 1
    assert read_minute_ledger_done(paths.minute_history_ledger) == frozenset({("A", _PREV)})
    assert paths.minute_history_partition(_PREV, "partial_walk").exists()
    assert not paths.minute_history_partition(_D, "partial_walk").exists()


def test_exhausted_budget_within_target_skips_remaining_days(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    fake = _FakeKisData({
        ("A", _PREV): [[_bar(_PREV, 9, 0)]],
        ("A", _D): [[_bar(_D, 9, 0)]],
    })
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_PREV, _D), {_PREV: "replay", _D: "live"})],
        paths=paths, settings=_settings(max_calls_per_run=1), credentials=_creds(),
        deadline=_deadline(), session=fake, now=_evening_now, run_id="daybudget",
    )
    assert summary.calls == 1
    assert summary.symbol_days == 1
    assert read_minute_ledger_done(paths.minute_history_ledger) == frozenset({("A", _PREV)})
    assert not paths.minute_history_partition(_D, "daybudget").exists()


@pytest.fixture
def wire_run(tmp_path, monkeypatch):
    from src.brokers.kis import stack as stack_mod
    from src.brokers.kis.auth import kis_token_cache_path

    cache = tmp_path / "tokens"
    cache.mkdir()
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(cache))
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_ALLOW_ISSUE", "true")
    kis_token_cache_path(cache, "k").write_text(json.dumps({
        "app_key": "k", "access_token": "cached",
        "expired_at": (_EVENING + dt.timedelta(days=1)).isoformat(),
    }))
    clock = {"now": _EVENING}
    admissions = []

    class Limiter:
        def __init__(self, path, rate, **kwargs):
            assert path.name == "tps_8254c329a928.state"
            assert rate == 8.0

        def acquire(self):
            admissions.append(1)

    monkeypatch.setattr(stack_mod, "HostPacedRateLimiter", Limiter)

    def run(session, *, budget=10, cached=True, now=None):
        if not cached:
            kis_token_cache_path(cache, "k").unlink()
        return run_minute_backfill(
            targets=[MinuteTarget(s, (_D,), {_D: "live"}) for s in ("A", "B")],
            paths=DataPaths(tmp_path), settings=_settings(max_calls_per_run=budget),
            credentials=_creds(), deadline=_deadline(), session=session,
            now=now or (lambda: clock["now"]), run_id="wire",
        )

    return run, clock, admissions


class _WireResponse:
    def __init__(self, body):
        self.headers = {}
        self.body = body

    def json(self):
        return self.body


class _WireSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []
        self.posts = []

    def get(self, url, **kwargs):
        self.calls.append(kwargs)
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return _WireResponse(page)

    def post(self, url, **kwargs):
        self.posts.append(kwargs)
        return _WireResponse({
            "access_token": "issued",
            "access_token_token_expired": "2026-10-23 23:45:00",
        })


def _wire_complete():
    return {"rt_cd": "0", "output2": [{
        "stck_bsop_date": "20261020", "stck_cntg_hour": "090000",
        "stck_oprc": "100", "stck_hgpr": "101", "stck_lwpr": "99",
        "stck_prpr": "100", "cntg_vol": "10",
    }]}


def test_wire_retries_are_paced_counted_and_budgeted(wire_run):
    run, _, admissions = wire_run
    session = _WireSession([{"rt_cd": "1", "msg_cd": "EGW00201"}, _wire_complete()])
    summary = run(session, budget=2)
    assert summary.calls == len(session.calls) == len(admissions) == 2
    assert summary.symbol_days == 1


def test_wire_failed_attempt_consumes_budget(wire_run):
    import requests

    run, _, _ = wire_run
    session = _WireSession([requests.ConnectionError("offline")])
    summary = run(session, budget=1)
    assert summary.calls == 1
    assert summary.failed == 1
    assert summary.symbols == 1


def test_wire_retry_cannot_overrun_budget(wire_run, tmp_path):
    run, _, _ = wire_run
    session = _WireSession([{"rt_cd": "1", "msg_cd": "EGW00201"}])
    summary = run(session, budget=1)
    assert summary.calls == len(session.calls) == 1
    assert summary.symbol_days == 0
    assert not DataPaths(tmp_path).minute_history_ledger.exists()


def test_wire_expired_token_at_budget_does_not_issue(wire_run):
    run, _, _ = wire_run
    session = _WireSession([{"rt_cd": "1", "msg_cd": "EGW00123"}])
    summary = run(session, budget=1)
    assert summary.calls == 1
    assert session.posts == []


def test_wire_timeout_clamped_to_remaining_deadline(wire_run):
    run, clock, _ = wire_run
    clock["now"] = _deadline() - dt.timedelta(seconds=0.25)
    session = _WireSession([_wire_complete()])
    summary = run(session, budget=1)
    assert summary.symbol_days == 1
    assert session.calls[0]["timeout"] == 0.25


def test_wire_admission_crossing_deadline_sends_nothing(wire_run, monkeypatch):
    from src.brokers.kis import stack as stack_mod

    run, clock, _ = wire_run

    class Limiter:
        def __init__(self, *args, **kwargs):
            return None

        def acquire(self):
            clock["now"] = _deadline()

    monkeypatch.setattr(stack_mod, "HostPacedRateLimiter", Limiter)
    session = _WireSession([])
    summary = run(session)
    assert summary.calls == 0
    assert session.calls == session.posts == []


def test_wire_token_issuance_respects_settings(wire_run, monkeypatch):
    run, _, _ = wire_run
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_ALLOW_ISSUE", "false")
    session = _WireSession([])
    summary = run(session, cached=False)
    assert summary.status == "FAIL"
    assert summary.reason == "auth"
    assert session.posts == []


def test_wire_token_issue_and_transport_failure(wire_run):
    run, _, _ = wire_run

    class Session(_WireSession):
        def post(self, url, **kwargs):
            raise RuntimeError("token endpoint offline")

    session = Session([])
    summary = run(session, cached=False)
    assert summary.status == "FAIL"
    assert summary.reason == "auth"
    assert summary.symbols == 1


def test_wire_token_issuance_and_auth_retry(wire_run):
    run, _, admissions = wire_run
    session = _WireSession([_wire_complete()])
    summary = run(session, budget=1, cached=False)
    assert summary.calls == 1
    assert len(session.posts) == 1
    assert len(admissions) == 2


def test_wire_auth_failure_preserves_certified_days(wire_run, tmp_path):
    run, _, _ = wire_run
    rejected = {"rt_cd": "1", "msg_cd": "EGW00123"}
    session = _WireSession([_wire_complete(), rejected, rejected])
    summary = run(session, budget=3)
    assert summary.status == "FAIL"
    assert summary.reason == "auth"
    assert summary.calls == 3
    assert len(session.posts) == 1
    assert summary.symbol_days == 1
    assert read_minute_ledger_done(DataPaths(tmp_path).minute_history_ledger) == frozenset({("A", _D)})


def test_discarded_offhours_reported(tmp_path, monkeypatch):
    fake = _FakeKisData({("A", _D): [[_bar(_D, 8, 59), _bar(_D, 9, 0)]]})
    _isolate(monkeypatch, fake)
    summary = run_minute_backfill(
        targets=[MinuteTarget("A", (_D,), {_D: "live"})], paths=DataPaths(tmp_path),
        settings=_settings(), credentials=_creds(), deadline=_deadline(),
        session=fake, now=_evening_now, run_id="offhours",
    )
    assert summary.discarded_offhours == 1
