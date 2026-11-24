"""Full-market sweep plan, symbol source, rotation and runner invariants."""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import pathlib
import threading
from zoneinfo import ZoneInfo

import pytest

from src.core.config import DataPaths, MarketSweepSettings, SnapshotSettings
from src.core.session_anchors import AnchorSource, SessionAnchors, standard_session_anchors
from src.execution.contracts import KisApiError
from src.marketdata.market_sweep import (
    MarketMapError,
    build_market_sweep_jobs,
    idle_until_run_end,
    load_market_symbols,
    pass_positions,
    rotate,
    rotation_offset,
    run_market_sweep_job,
    run_market_sweep_session,
    shift_market_sweep_settings,
    sweep_covered_investor_symbols,
)
from src.marketdata.snapshot_plan import SnapshotJob, SnapshotJobKind
from src.marketdata.snapshot_schema import SNAPSHOT_SCHEMAS, SnapshotDataset
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 11, 24)
AUCTION_TR = "FHKST01010200"
INVESTOR_TR = "HHPTJ04160200"


def _t(hour: int, minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(DAY.year, DAY.month, DAY.day, hour, minute, second, tzinfo=KST)


def _auction_row(symbol: str) -> dict[str, object]:
    return {
        "source_tr": AUCTION_TR,
        "market_div_code": "J",
        "symbol": symbol,
        "book_time": "085900",
        "auction_code": "1",
        "expected_price": 70100,
        "expected_volume": 1234,
        "expected_change_pct": 1.45,
        "vi_code": "",
        "last_price": 70000,
        "base_price": 69500,
        "ask_prices": [0] * 10,
        "ask_sizes": [0] * 10,
        "bid_prices": [0] * 10,
        "bid_sizes": [0] * 10,
        "total_ask_size": 300,
        "total_bid_size": 150,
    }


def _investor_rows(symbol: str) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "source_tr": INVESTOR_TR,
            "market_div_code": "",
            "symbol": symbol,
            "bucket": bucket,
            "foreign_net_qty": -100,
            "institution_net_qty": 50,
            "total_net_qty": -50,
        }
        for bucket in (1, 2)
    )


class _Clock:
    def __init__(self, start: dt.datetime) -> None:
        self.now = start
        self.ns = 0

    def now_fn(self) -> dt.datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        assert seconds > 0
        self.now += dt.timedelta(seconds=seconds)

    def wall_ns(self) -> int:
        self.ns += 1
        return self.ns


class _FakeSweepSource:
    """Exposes only the two sweep endpoints; each call advances the fake clock by ``step_s``."""

    def __init__(self, clock: _Clock, *, step_s: float = 0.0, fail: frozenset[str] = frozenset()) -> None:
        self.clock = clock
        self.step_s = step_s
        self.fail = fail
        self.calls: list[tuple[str, str, dt.datetime]] = []

    def _call(self, name: str, symbol: str) -> None:
        self.calls.append((name, symbol, self.clock.now))
        self.clock.now += dt.timedelta(seconds=self.step_s)
        if symbol in self.fail:
            raise KisApiError("EGW00201", "rate")

    def get_auction_book(self, symbol: str) -> dict[str, object]:
        self._call("auction_book", symbol)
        return _auction_row(symbol)

    def get_investor_estimate(self, symbol: str) -> tuple[dict[str, object], ...]:
        self._call("investor_estimate", symbol)
        return _investor_rows(symbol)


def _symbols(count: int) -> tuple[str, ...]:
    return tuple(f"{index:06d}" for index in range(count))


def _store(tmp_path) -> SnapshotStore:
    return SnapshotStore(paths=DataPaths(tmp_path), session_date=DAY)


def test_plan_has_only_sweep_kinds_with_configured_times_and_deadlines() -> None:
    jobs = build_market_sweep_jobs(MarketSweepSettings(), DAY)

    windows = {job.job_id: (job.due_at.time(), job.not_after.time()) for job in jobs}
    assert {job.kind for job in jobs} == {
        SnapshotJobKind.AUCTION_OPEN,
        SnapshotJobKind.AUCTION_CLOSE,
        SnapshotJobKind.INVESTOR_ESTIMATE,
    }
    assert windows == {
        "auction_open@084000": (dt.time(8, 40), dt.time(8, 43)),
        "auction_open@084300": (dt.time(8, 43), dt.time(8, 46)),
        "auction_open@084600": (dt.time(8, 46), dt.time(8, 49)),
        "auction_open@084900": (dt.time(8, 49), dt.time(8, 52)),
        "auction_open@085200": (dt.time(8, 52), dt.time(8, 55)),
        "auction_open@085500": (dt.time(8, 55), dt.time(9, 0)),
        "investor_estimate@093500": (dt.time(9, 35), dt.time(10, 5)),
        "investor_estimate@100500": (dt.time(10, 5), dt.time(11, 25)),
        "investor_estimate@112500": (dt.time(11, 25), dt.time(13, 25)),
        "investor_estimate@132500": (dt.time(13, 25), dt.time(14, 35)),
        "investor_estimate@143500": (dt.time(14, 35), dt.time(15, 20, 30)),
        "auction_close@152030": (dt.time(15, 20, 30), dt.time(15, 23)),
        "auction_close@152300": (dt.time(15, 23), dt.time(15, 25, 30)),
        "auction_close@152530": (dt.time(15, 25, 30), dt.time(15, 28)),
        "auction_close@152800": (dt.time(15, 28), dt.time(15, 30)),
        "investor_estimate@153500": (dt.time(15, 35), dt.time(15, 39)),
    }
    assert [job.due_at for job in jobs] == sorted(job.due_at for job in jobs)
    assert pass_positions(jobs)["auction_open@085500"] == (5, 6)
    assert pass_positions(jobs)["investor_estimate@153500"] == (5, 6)


def test_shift_follows_csat_open_and_close_and_is_identity_on_standard_days() -> None:
    settings = MarketSweepSettings()
    csat = SessionAnchors(
        date=DAY,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    shifted = shift_market_sweep_settings(settings, csat)

    assert shift_market_sweep_settings(settings, standard_session_anchors(DAY)) == settings
    assert shifted.auction_open_times[0] == dt.time(9, 40)
    assert shifted.auction_open_deadline == dt.time(10, 0)
    assert shifted.investor_estimate_times[0] == dt.time(10, 35)
    assert shifted.auction_close_times[0] == dt.time(16, 20, 30)
    assert shifted.investor_estimate_close_time == dt.time(16, 35)
    assert shifted.run_end == dt.time(16, 39)


def test_shift_rejects_order_violation_against_shifted_close() -> None:
    late_open = SessionAnchors(
        date=DAY,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(15, 20),
        regular_close=dt.time(15, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    with pytest.raises(ValueError, match="market sweep order"):
        shift_market_sweep_settings(MarketSweepSettings(), late_open)


def test_load_market_symbols_filters_markets_and_codes_sorted(tmp_path) -> None:
    path = tmp_path / "market_map.json"
    path.write_text(
        json.dumps({"035720": "KOSPI", "005930": "KOSPI", "0001A0": "KOSDAQ", "BAD": "KOSPI", "900110": "KONEX"}),
        encoding="utf-8",
    )

    assert load_market_symbols(path, markets=("KOSPI", "KOSDAQ")) == ("0001A0", "005930", "035720")


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        (None, "market_map_unreadable:FileNotFoundError"),
        ("{broken", "market_map_unreadable:JSONDecodeError"),
        ("[]", "market_map_not_object"),
        ('{"900110": "KONEX"}', "market_map_empty"),
    ],
)
def test_load_market_symbols_fails_closed(tmp_path, content, reason) -> None:
    path = tmp_path / "market_map.json"
    if content is not None:
        path.write_text(content, encoding="utf-8")

    with pytest.raises(MarketMapError, match=reason):
        load_market_symbols(path, markets=("KOSPI", "KOSDAQ"))


@pytest.mark.parametrize("passes", [4, 6])
@pytest.mark.parametrize("size", [1, 7, 100, 2500, 2503])
def test_rotation_covers_every_symbol_when_each_pass_truncates_at_60_percent(passes, size) -> None:
    symbols = _symbols(size)
    reach = math.ceil(size * 0.6)
    covered: set[str] = set()
    for index in range(passes):
        covered.update(rotate(symbols, rotation_offset(index, passes, size))[:reach])

    assert covered == set(symbols)
    assert rotation_offset(0, passes, size) == 0
    assert rotation_offset(1, 0, size) == 0
    assert rotation_offset(1, passes, 0) == 0


def test_session_writes_only_market_datasets_via_two_endpoints(tmp_path) -> None:
    clock = _Clock(_t(8, 39))
    source = _FakeSweepSource(clock)
    store = _store(tmp_path)
    symbols = _symbols(3)

    result = run_market_sweep_session(
        settings=MarketSweepSettings(concurrency=1),
        session_date=DAY,
        source=source,
        store=store,
        symbols=symbols,
        now_fn=clock.now_fn,
        sleep_fn=clock.sleep,
        wall_ns=clock.wall_ns,
    )

    assert {name for name, _, _ in source.calls} == {"auction_book", "investor_estimate"}
    assert (result.executed, result.expired, result.failed_jobs) == (16, 0, 0)
    assert result.rows_added == {"auction_book_market": 30, "investor_estimate_market": 36}
    archive = DataPaths(tmp_path).archive_root / "snapshot"
    assert sorted(path.name for path in archive.iterdir()) == ["auction_book_market", "investor_estimate_market"]
    auction = store.frame(SnapshotDataset.AUCTION_BOOK_MARKET)
    assert set(auction["source_tr"].to_list()) == {AUCTION_TR}
    assert auction.filter(auction["phase"] == "open").height == 18
    assert auction.filter(auction["phase"] == "close").height == 12
    assert set(store.frame(SnapshotDataset.INVESTOR_ESTIMATE_MARKET)["source_tr"].to_list()) == {INVESTOR_TR}
    assert store.frame(SnapshotDataset.AUCTION_BOOK).height == 0
    assert store.frame(SnapshotDataset.INVESTOR_ESTIMATE).height == 0
    assert SNAPSHOT_SCHEMAS[SnapshotDataset.AUCTION_BOOK_MARKET] == SNAPSHOT_SCHEMAS[SnapshotDataset.AUCTION_BOOK]


def test_truncated_passes_rotate_to_cover_whole_window(tmp_path, caplog) -> None:
    clock = _Clock(_t(8, 39))
    source = _FakeSweepSource(clock, step_s=3.0)
    symbols = _symbols(100)

    with caplog.at_level(logging.INFO, logger="src.marketdata.market_sweep"):
        run_market_sweep_session(
            settings=MarketSweepSettings(concurrency=1),
            session_date=DAY,
            source=source,
            store=_store(tmp_path),
            symbols=symbols,
            now_fn=clock.now_fn,
            sleep_fn=clock.sleep,
            wall_ns=clock.wall_ns,
        )

    open_calls = [(symbol, at) for name, symbol, at in source.calls if name == "auction_book" and at < _t(9, 0)]
    first_pass = [symbol for symbol, at in open_calls if at < _t(8, 43)]
    assert len(first_pass) == 60
    assert {symbol for symbol, _ in open_calls} == set(symbols)
    assert all(at < _t(9, 0) for _, at in open_calls)
    close_calls = {symbol for name, symbol, at in source.calls if name == "auction_book" and at >= _t(15, 20)}
    assert close_calls == set(symbols)
    truncated = [r for r in caplog.records if "stage=market_sweep_job" in r.getMessage() and "truncated=True" in r.getMessage()]
    assert truncated
    assert all(r.levelno == logging.WARNING for r in truncated)
    assert "job=auction_open@084300 symbols=100 offset=16" in caplog.text


def test_late_start_expires_past_jobs_without_calls(tmp_path, caplog) -> None:
    clock = _Clock(_t(15, 24))
    source = _FakeSweepSource(clock)

    with caplog.at_level(logging.INFO, logger="src.marketdata.market_sweep"):
        result = run_market_sweep_session(
            settings=MarketSweepSettings(concurrency=1),
            session_date=DAY,
            source=source,
            store=_store(tmp_path),
            symbols=_symbols(2),
            now_fn=clock.now_fn,
            sleep_fn=clock.sleep,
            wall_ns=clock.wall_ns,
        )

    assert result.expired == 12
    assert result.executed == 4
    assert all(at >= _t(15, 24) for _, _, at in source.calls)
    assert "stage=market_sweep_expired count=12" in caplog.text
    assert "stage=market_sweep_summary symbols=2 executed=4 expired=12" in caplog.text


def test_session_past_run_end_makes_no_calls(tmp_path) -> None:
    clock = _Clock(_t(15, 39))
    source = _FakeSweepSource(clock)

    result = run_market_sweep_session(
        settings=MarketSweepSettings(concurrency=1),
        session_date=DAY,
        source=source,
        store=_store(tmp_path),
        symbols=_symbols(2),
        now_fn=clock.now_fn,
        sleep_fn=clock.sleep,
        max_iterations=3,
    )

    assert source.calls == []
    assert result.executed == 0


def test_all_failed_pass_counts_as_failed_job(tmp_path, caplog) -> None:
    clock = _Clock(_t(8, 40))
    source = _FakeSweepSource(clock, fail=frozenset({"000000"}))

    with caplog.at_level(logging.WARNING, logger="src.marketdata.market_sweep"):
        result = run_market_sweep_session(
            settings=MarketSweepSettings(concurrency=1),
            session_date=DAY,
            source=source,
            store=_store(tmp_path),
            symbols=_symbols(1),
            now_fn=clock.now_fn,
            sleep_fn=clock.sleep,
            max_iterations=1,
        )

    assert result.failed_jobs == 1
    assert "job=auction_open@084000 symbols=1 offset=0 attempted=1 failed=1 rows=0" in caplog.text
    assert "EGW00201" in caplog.text


def test_store_failure_is_logged_and_session_continues(tmp_path, caplog) -> None:
    clock = _Clock(_t(8, 40))

    class _BrokenStore:
        session_date = DAY

        def append(self, dataset, rows) -> int:
            raise SnapshotStoreError("disk")

    with caplog.at_level(logging.ERROR, logger="src.marketdata.market_sweep"):
        result = run_market_sweep_session(
            settings=MarketSweepSettings(concurrency=1),
            session_date=DAY,
            source=_FakeSweepSource(clock),
            store=_BrokenStore(),  # type: ignore[arg-type]
            symbols=_symbols(1),
            now_fn=clock.now_fn,
            sleep_fn=clock.sleep,
            max_iterations=1,
        )

    assert result.failed_jobs == 1
    assert "status=STORE_FAIL reason=disk" in caplog.text


def test_job_rejects_non_sweep_kind(tmp_path) -> None:
    clock = _Clock(_t(9, 0))
    job = SnapshotJob(job_id="ranking@090000", kind=SnapshotJobKind.RANKING, due_at=_t(9, 0), not_after=_t(9, 1))

    with pytest.raises(ValueError, match="not a market sweep job kind: ranking"):
        run_market_sweep_job(
            job, offset=0, source=_FakeSweepSource(clock), store=_store(tmp_path), symbols=_symbols(1), now_fn=clock.now_fn
        )


def test_market_auction_dataset_applies_candidate_row_validation(tmp_path) -> None:
    store = _store(tmp_path)
    bad = {**_auction_row("000001"), "expected_price": -1, "phase": "open", "session_date": DAY, "observed_at_ns": 1}
    good = {**_auction_row("000002"), "phase": "open", "session_date": DAY, "observed_at_ns": 2}

    assert store.append(SnapshotDataset.AUCTION_BOOK_MARKET, [bad, good]) == 1
    assert store.frame(SnapshotDataset.AUCTION_BOOK_MARKET)["symbol"].to_list() == ["000002"]


def test_idle_until_run_end_sleeps_without_calls() -> None:
    clock = _Clock(_t(8, 0))

    idle_until_run_end(MarketSweepSettings(), DAY, now_fn=clock.now_fn, sleep_fn=clock.sleep)

    assert clock.now == _t(15, 39)
    idle_until_run_end(MarketSweepSettings(), DAY, now_fn=clock.now_fn, sleep_fn=clock.sleep, max_iterations=0)


class _QuietSpy:
    """Records how many sweep calls had already happened each time the idle quiet check ran."""

    def __init__(self) -> None:
        self.seen: list[int] = []

    def run(self, source: _FakeSweepSource) -> int:
        self.seen.append(len(source.calls))
        return 0


def test_quiet_check_only_when_idle(tmp_path) -> None:
    clock = _Clock(_t(8, 40))
    source = _FakeSweepSource(clock)
    spy = _QuietSpy()

    result = run_market_sweep_session(
        settings=MarketSweepSettings(concurrency=1),
        session_date=DAY,
        source=source,
        store=_store(tmp_path),
        symbols=_symbols(3),
        now_fn=clock.now_fn,
        sleep_fn=clock.sleep,
        wall_ns=clock.wall_ns,
        max_iterations=2,
        quiet_check=spy,
    )

    assert result.executed > 0
    assert spy.seen == [len(source.calls)]


class _LockedSweepSource(_FakeSweepSource):
    def __init__(self, clock: _Clock) -> None:
        super().__init__(clock)
        self._lock = threading.Lock()

    def _call(self, name: str, symbol: str) -> None:
        with self._lock:
            super()._call(name, symbol)


def test_session_passes_concurrency(tmp_path) -> None:
    clock = _Clock(_t(8, 40))
    source = _LockedSweepSource(clock)
    store = _store(tmp_path)
    symbols = _symbols(12)

    run_market_sweep_session(
        settings=MarketSweepSettings(concurrency=3),
        session_date=DAY,
        source=source,
        store=store,
        symbols=symbols,
        now_fn=clock.now_fn,
        sleep_fn=clock.sleep,
        wall_ns=clock.wall_ns,
        max_iterations=1,
    )

    open_calls = [symbol for name, symbol, _ in source.calls if name == "auction_book"][:12]
    assert sorted(open_calls) == sorted(symbols)
    auction = store.frame(SnapshotDataset.AUCTION_BOOK_MARKET)
    assert auction.filter(auction["phase"] == "open").height == 12


def _covered_map(tmp_path, codes: tuple[str, ...]) -> pathlib.Path:
    path = tmp_path / "market_map.json"
    path.write_text(json.dumps(dict.fromkeys(codes, "KOSPI")), encoding="utf-8")
    return path


def test_dedupe_skips_covered_symbols_in_order(tmp_path) -> None:
    path = _covered_map(tmp_path, ("005930", "000660"))

    result = sweep_covered_investor_symbols(
        ("005930", "035720", "000660"),
        snapshot=SnapshotSettings(),
        sweep=MarketSweepSettings(enabled=True, kis_data_slot="6", markets=("KOSPI",)),
        market_map_path=path,
    )

    assert result == (("035720",), "sweep_covered")


def test_dedupe_off_when_sweep_disabled(tmp_path) -> None:
    path = _covered_map(tmp_path, ("005930",))

    result = sweep_covered_investor_symbols(
        ("005930",), snapshot=SnapshotSettings(), sweep=MarketSweepSettings(enabled=False), market_map_path=path
    )

    assert result == (None, "sweep_disabled")


def test_dedupe_off_when_no_market_map_path() -> None:
    result = sweep_covered_investor_symbols(
        ("005930",), snapshot=SnapshotSettings(), sweep=MarketSweepSettings(enabled=True, kis_data_slot="6"), market_map_path=None
    )

    assert result == (None, "sweep_disabled")


def test_dedupe_off_when_times_differ(tmp_path) -> None:
    path = _covered_map(tmp_path, ("005930",))
    snapshot = SnapshotSettings(investor_estimate_times=(dt.time(9, 35), dt.time(12, 0)))

    result = sweep_covered_investor_symbols(
        ("005930",), snapshot=snapshot, sweep=MarketSweepSettings(enabled=True, kis_data_slot="6"), market_map_path=path
    )

    assert result == (None, "times_differ")


def test_dedupe_off_on_bad_map(tmp_path) -> None:
    result = sweep_covered_investor_symbols(
        ("005930",),
        snapshot=SnapshotSettings(),
        sweep=MarketSweepSettings(enabled=True, kis_data_slot="6"),
        market_map_path=tmp_path / "missing.json",
    )

    assert result == (None, "market_map_unreadable")

