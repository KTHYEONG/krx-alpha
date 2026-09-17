import datetime as dt
from zoneinfo import ZoneInfo

from src.core.config import SnapshotSettings
from src.execution.contracts import KisApiError
from src.execution.kis_client import TR_FLUCTUATION, TR_TRADE_AMOUNT, KisRankingRow
from src.marketdata.snapshot_contracts import SnapshotDataset, SnapshotJob, SnapshotJobKind
from src.marketdata.snapshot_service import run_snapshot_job, run_snapshot_session
from src.storage.snapshot_store import SnapshotStoreError

KST = ZoneInfo("Asia/Seoul")
SESSION_DATE = dt.date(2026, 9, 17)


def _t(hour: int, minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(2026, 9, 17, hour, minute, second, tzinfo=KST)


def _ns(hour: int, minute: int, second: int = 0) -> int:
    utc = dt.datetime(2026, 9, 17, hour, minute, second, tzinfo=KST).astimezone(dt.UTC)
    epoch = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    return int((utc - epoch).total_seconds()) * 1_000_000_000


def _job(kind: SnapshotJobKind, due: dt.datetime, not_after: dt.datetime) -> SnapshotJob:
    return SnapshotJob(job_id=f"{kind.value}@{due:%H%M%S}", kind=kind, due_at=due, not_after=not_after)


def _auction_row(symbol: str) -> dict:
    return {
        "source_tr": "FHKST01010200",
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


def _investor_rows(symbol: str) -> tuple:
    return tuple(
        {
            "source_tr": "HHPTJ04160200",
            "market_div_code": "",
            "symbol": symbol,
            "bucket": bucket,
            "foreign_net_qty": -100,
            "institution_net_qty": 50,
            "total_net_qty": -50,
        }
        for bucket in (1, 2)
    )


def _program_row(symbol: str) -> dict:
    return {
        "source_tr": "FHPPG04650101",
        "market_div_code": "J",
        "symbol": symbol,
        "trade_time": "150000",
        "cum_volume": 5000,
        "sell_qty": 100,
        "buy_qty": 150,
        "net_qty": 50,
        "sell_value_krw": 1000,
        "buy_value_krw": 1500,
        "net_value_krw": 500,
    }


def _index_row(code: str) -> dict:
    return {
        "source_tr": "FHPUP02100000",
        "market_div_code": "U",
        "index_code": code,
        "index_value": 822.18,
        "change_pct": 0.87,
        "cum_value_mil_krw": 123456,
        "advancers": 945,
        "decliners": 123,
    }


def _bar_row(symbol: str, bar_time: str = "093000") -> dict:
    return {
        "source_tr": "FHKST03010200",
        "market_div_code": "J",
        "symbol": symbol,
        "bar_time": bar_time,
        "open": 70000,
        "high": 70100,
        "low": 69900,
        "close": 70050,
        "volume": 10,
    }


def _news_row(news_id: str, hour: int = 10, minute: int = 0, second: int = 0) -> dict:
    return {
        "source_tr": "FHKST01011800",
        "market_div_code": "",
        "news_id": news_id,
        "published_at_ns": _ns(hour, minute, second),
        "provider": "p",
        "provider_code": "1",
        "category_code": "A",
        "title": "t",
        "symbols": [],
    }


def _ranking_row(symbol: str, rank: int, observed: int, change: float = 1.0, value=None) -> dict:
    return {
        "session_date": SESSION_DATE,
        "observed_at_ns": observed,
        "source_tr": "FHPST01710000",
        "market_div_code": "J",
        "list_kind": "trade_amount",
        "rank": rank,
        "symbol": symbol,
        "change_pct": change,
        "trade_value_krw": value,
    }


class _FakeSource:
    def __init__(self) -> None:
        self.calls: list = []
        self.auction: dict = {}
        self.investor: dict = {}
        self.program: dict = {}
        self.index: dict = {}
        self.bars: dict = {}
        self.news: dict = {}
        self.trade_rows: tuple = ()
        self.fluct_rows: tuple = ()

    def _run(self, name: str, key, mapping: dict):
        self.calls.append((name, key))
        behavior = mapping.get(key, mapping.get("*"))
        if isinstance(behavior, BaseException):
            raise behavior
        return behavior() if callable(behavior) else behavior

    def get_auction_book(self, symbol: str) -> dict:
        return self._run("auction", symbol, self.auction)

    def get_investor_estimate(self, symbol: str) -> tuple:
        return self._run("investor", symbol, self.investor)

    def get_program_trade_latest(self, symbol: str):
        return self._run("program", symbol, self.program)

    def get_index_snapshot(self, index_code: str) -> dict:
        return self._run("index", index_code, self.index)

    def get_stock_minute_bars(self, symbol: str, **kwargs) -> tuple:
        self.calls.append(("bars", symbol, kwargs))
        behavior = self.bars.get(symbol, self.bars.get("*"))
        if isinstance(behavior, BaseException):
            raise behavior
        return behavior() if callable(behavior) else behavior

    def get_news_titles(self, *, before=None) -> tuple:
        self.calls.append(("news", before))
        behavior = self.news.get(before, self.news.get("*"))
        if isinstance(behavior, BaseException):
            raise behavior
        return behavior() if callable(behavior) else behavior

    def get_trade_amount_ranking(self):
        self.calls.append(("trade_amount", None))
        return self.trade_rows

    def get_fluctuation_ranking(self):
        self.calls.append(("fluctuation", None))
        return self.fluct_rows


def _counter(start: int = 100):
    state = [start]

    def _next() -> int:
        value = state[0]
        state[0] += 1
        return value

    return _next


def test_auction_job_stamps_phase_and_survives_symbol_failure(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    symbols = ("A1", "B2", "C3")
    source = _FakeSource()
    source.auction = {"A1": _auction_row("A1"), "B2": KisApiError("EGW00000", "bad"), "C3": _auction_row("C3")}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.AUCTION_OPEN, _t(8, 40), _t(8, 50))

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=symbols,
        news_seen=set(), now_fn=lambda: _t(8, 40, 5), wall_ns=_counter(101),
    )

    assert (result.attempted, result.succeeded, result.failed) == (3, 2, 1)
    assert result.rows_added == 2
    assert result.truncated is False
    frame = store.frame(SnapshotDataset.AUCTION_BOOK)
    assert frame.height == 2
    assert frame["phase"].to_list() == ["open", "open"]
    assert frame["observed_at_ns"].to_list() == [101, 102]


def test_job_stops_calling_after_window_closes(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.auction = {"*": lambda: _auction_row("x")}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.AUCTION_CLOSE, _t(15, 21), _t(15, 25))
    ticks = [_t(15, 21, 5), _t(15, 21, 6), _t(15, 25, 1)]

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=("A", "B", "C"),
        news_seen=set(), now_fn=lambda: ticks.pop(0), wall_ns=_counter(),
    )

    assert [call[1] for call in source.calls] == ["A", "B"]
    assert result.truncated is True
    assert result.rows_added == 2
    assert store.frame(SnapshotDataset.AUCTION_BOOK).height == 2


def test_ranking_job_writes_both_lists_with_null_fluctuation_value(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.trade_rows = (KisRankingRow("005930", 1, 1.5, 100), KisRankingRow("000660", 2, 0.5, 50))
    source.fluct_rows = (KisRankingRow("000660", 1, 29.9, 0),)
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.RANKING, _t(9, 0), _t(9, 1))

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=set(), now_fn=lambda: _t(9, 0, 5), wall_ns=_counter(),
    )

    assert result.rows_added == 3
    frame = store.frame(SnapshotDataset.RANKING)
    assert frame.height == 3
    fluct = frame.filter(frame["list_kind"] == "fluctuation")
    assert fluct["trade_value_krw"].to_list() == [None]
    assert fluct["source_tr"].to_list() == [TR_FLUCTUATION]
    assert set(frame["source_tr"].to_list()) == {TR_TRADE_AMOUNT, TR_FLUCTUATION}


def test_program_trade_none_counts_as_success_without_rows(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.program = {"*": None}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.PROGRAM_TRADE, _t(9, 30), _t(10, 0))

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=("A",),
        news_seen=set(), now_fn=lambda: _t(9, 30, 5), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed, result.rows_added) == (1, 1, 0, 0)


def test_news_job_pages_inclusive_cursor_until_overlap(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.news = {
        None: (_news_row("n4", 10, 1), _news_row("n3", 10, 0)),
        ("20260917", "100000"): (_news_row("n3", 10, 0), _news_row("n2", 9, 59), _news_row("n1", 9, 58)),
    }
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.NEWS_TITLE, _t(10, 0), _t(10, 0, 30))
    news_seen = {"n1"}

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=news_seen, now_fn=lambda: _t(10, 0, 5), wall_ns=_counter(),
    )

    assert [call[1] for call in source.calls] == [None, ("20260917", "100000")]
    assert result.truncated is False
    frame = store.frame(SnapshotDataset.NEWS_TITLE)
    assert set(frame["news_id"].to_list()) == {"n4", "n3", "n2"}
    assert news_seen == {"n1", "n4", "n3", "n2"}


def test_news_job_marks_truncated_at_page_cap(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.news = {
        None: (_news_row("a", 10, 2), _news_row("b", 10, 1)),
        ("20260917", "100100"): (_news_row("c", 10, 0, 30), _news_row("d", 10, 0)),
    }
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.NEWS_TITLE, _t(10, 2), _t(10, 2, 30))

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(news_max_pages=2), symbols=(),
        news_seen=set(), now_fn=lambda: _t(10, 2, 5), wall_ns=_counter(),
    )

    assert len(source.calls) == 2
    assert result.truncated is True
    assert result.rows_added == 4


def test_news_job_stops_on_failed_page_and_keeps_first_page(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.news = {
        None: (_news_row("a", 10, 1),),
        ("20260917", "100100"): KisApiError("EGW00000", "bad"),
    }
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.NEWS_TITLE, _t(10, 1), _t(10, 1, 30))
    news_seen: set = set()

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=news_seen, now_fn=lambda: _t(10, 1, 5), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed) == (2, 1, 1)
    assert result.truncated is False
    assert store.frame(SnapshotDataset.NEWS_TITLE)["news_id"].to_list() == ["a"]
    assert news_seen == {"a"}


def _seed_ranking(store, rows: list) -> None:
    store.append(SnapshotDataset.RANKING, rows)


def test_eod_minute_bars_selects_first_seen_non_universe_symbols(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    _seed_ranking(store, [
        _ranking_row("000001", 3, 100),
        _ranking_row("000002", 5, 50),
        _ranking_row("000003", 1, 40),
        _ranking_row("000004", 2, 30),
    ])
    store.append(SnapshotDataset.STOCK_MINUTE_BAR, [{
        "session_date": SESSION_DATE, "observed_at_ns": 31, "source_tr": "FHKST03010200",
        "market_div_code": "J", "symbol": "000004", "bar_time": "093000",
        "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1,
    }])
    source = _FakeSource()
    source.bars = {"*": lambda: (_bar_row("x"),)}
    job = _job(SnapshotJobKind.EOD_MINUTE_BARS, _t(15, 35), _t(15, 39))
    settings = SnapshotSettings(stock_minute_max_symbols=1)

    result = run_snapshot_job(
        job, source=source, store=store, settings=settings, symbols=("000003",),
        news_seen=set(), now_fn=lambda: _t(15, 35, 5), wall_ns=_counter(),
    )

    requested = [call[1] for call in source.calls if call[0] == "bars"]
    assert requested == ["000002"]
    kwargs = source.calls[0][2]
    assert kwargs["session_open"] == settings.intraday_start
    assert kwargs["session_close"] == settings.intraday_end
    assert result.rows_added == 1


def test_eod_minute_bars_skips_empty_results_and_empty_ranking(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.EOD_MINUTE_BARS, _t(15, 35), _t(15, 39))

    empty = run_snapshot_job(
        job, source=_FakeSource(), store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=set(), now_fn=lambda: _t(15, 35, 5), wall_ns=_counter(),
    )
    assert (empty.attempted, empty.rows_added) == (0, 0)

    _seed_ranking(store, [_ranking_row("000001", 1, 50)])
    source = _FakeSource()
    source.bars = {"*": ()}
    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=set(), now_fn=lambda: _t(15, 35, 5), wall_ns=_counter(),
    )
    assert (result.attempted, result.succeeded, result.rows_added) == (1, 1, 0)


def test_investor_job_skips_failed_symbols_and_truncates(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.investor = {
        "A": _investor_rows("A"),
        "B": KisApiError("EGW00000", "bad"),
        "C": _investor_rows("C"),
    }
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.INVESTOR_ESTIMATE, _t(9, 35), _t(10, 5))
    ticks = [_t(9, 35, 5), _t(9, 35, 6), _t(10, 5, 1)]

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=("A", "B", "C"),
        news_seen=set(), now_fn=lambda: ticks.pop(0), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed) == (2, 1, 1)
    assert result.truncated is True
    assert result.rows_added == 2
    assert [call[1] for call in source.calls] == ["A", "B"]


def test_program_job_skips_failed_symbol_then_truncates(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.program = {"A": KisApiError("EGW00000", "bad"), "B": _program_row("B")}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.PROGRAM_TRADE, _t(9, 30), _t(10, 0))
    ticks = [_t(9, 30, 5), _t(10, 0, 1)]

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=("A", "B"),
        news_seen=set(), now_fn=lambda: ticks.pop(0), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed) == (1, 0, 1)
    assert result.truncated is True
    assert result.rows_added == 0


def test_index_job_skips_failed_code_then_truncates(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.index = {"0001": _index_row("0001"), "1001": KisApiError("EGW00000", "bad")}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.INDEX_SNAPSHOT, _t(9, 0), _t(9, 1))
    ticks = [_t(9, 0, 5), _t(9, 0, 6), _t(9, 1, 1)]

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=set(), now_fn=lambda: ticks.pop(0), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed) == (2, 1, 1)
    assert result.truncated is True
    assert result.rows_added == 1


def test_eod_job_skips_failed_symbol_then_truncates(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    _seed_ranking(store, [_ranking_row("000001", 1, 50), _ranking_row("000002", 2, 60)])
    source = _FakeSource()
    source.bars = {"000001": KisApiError("EGW00000", "bad"), "000002": (_bar_row("000002"),)}
    job = _job(SnapshotJobKind.EOD_MINUTE_BARS, _t(15, 35), _t(15, 39))
    ticks = [_t(15, 35, 5), _t(15, 39, 1)]

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=(),
        news_seen=set(), now_fn=lambda: ticks.pop(0), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed) == (1, 0, 1)
    assert result.truncated is True
    assert result.rows_added == 0


def test_all_failing_job_counts_as_failed_without_rows(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.auction = {"*": KisApiError("EGW00000", "bad")}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    job = _job(SnapshotJobKind.AUCTION_OPEN, _t(8, 40), _t(8, 50))

    result = run_snapshot_job(
        job, source=source, store=store, settings=SnapshotSettings(), symbols=("A", "B"),
        news_seen=set(), now_fn=lambda: _t(8, 40, 5), wall_ns=_counter(),
    )

    assert (result.attempted, result.succeeded, result.failed, result.rows_added) == (2, 0, 2, 0)
    assert store.frame(SnapshotDataset.AUCTION_BOOK).height == 0


def _session_source() -> _FakeSource:
    source = _FakeSource()
    source.auction = {"*": lambda: _auction_row("s")}
    source.investor = {"*": lambda: _investor_rows("s")}
    source.program = {"*": lambda: _program_row("s")}
    source.index = {"*": lambda: _index_row("c")}
    source.bars = {"*": lambda: (_bar_row("s"),)}
    source.news = {None: (_news_row("n1", 10, 0),), "*": ()}
    source.trade_rows = (KisRankingRow("005930", 1, 1.5, 100),)
    source.fluct_rows = (KisRankingRow("000660", 1, 29.9, 0),)
    return source


def test_session_expires_backlog_without_vendor_calls_after_late_start(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.marketdata.snapshot_contracts import build_session_jobs, partition_due_jobs
    from src.storage.snapshot_store import SnapshotStore

    settings = SnapshotSettings()
    now = _t(10, 0, 5)
    jobs = build_session_jobs(settings, SESSION_DATE)
    due, expired = partition_due_jobs(jobs, completed=set(), now=now)
    source = _session_source()
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)

    result = run_snapshot_session(
        settings=settings, session_date=SESSION_DATE, source=source, store=store,
        symbols=("005930",), now_fn=lambda: now, sleep_fn=lambda s: None,
        wall_ns=_counter(), max_iterations=1,
    )

    assert result.expired == len(expired)
    assert result.executed == len(due)
    assert result.expired > 0
    assert not [call for call in source.calls if call[0] == "auction"]
    assert ("trade_amount", None) in source.calls
    assert ("fluctuation", None) in source.calls


def test_session_exits_at_run_end(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _session_source()
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)

    result = run_snapshot_session(
        settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=store,
        symbols=("005930",), now_fn=lambda: _t(15, 39), sleep_fn=lambda s: None,
        wall_ns=_counter(), max_iterations=10,
    )

    assert source.calls == []
    assert (result.executed, result.expired, result.failed_jobs) == (0, 0, 0)


def test_session_stops_before_job_when_run_end_passes(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _session_source()
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    ticks = [_t(9, 0, 10), _t(15, 39, 1)]

    result = run_snapshot_session(
        settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=store,
        symbols=("005930",), now_fn=lambda: ticks.pop(0) if len(ticks) > 1 else ticks[0],
        sleep_fn=lambda s: None, wall_ns=_counter(), max_iterations=1,
    )

    assert result.executed == 0
    assert result.expired > 0


def test_session_isolates_store_error_to_single_job(tmp_path, caplog) -> None:
    import logging

    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    real = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)

    class _FlakyStore:
        def __init__(self) -> None:
            self.fail_next = True

        @property
        def session_date(self):
            return real.session_date

        def frame(self, dataset):
            return real.frame(dataset)

        def append(self, dataset, rows):
            if self.fail_next:
                self.fail_next = False
                raise SnapshotStoreError("boom")
            return real.append(dataset, rows)

    source = _session_source()
    with caplog.at_level(logging.ERROR, logger="src.marketdata.snapshot_service"):
        result = run_snapshot_session(
            settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=_FlakyStore(),
            symbols=("005930",), now_fn=lambda: _t(9, 0, 10), sleep_fn=lambda s: None,
            wall_ns=_counter(), max_iterations=1,
        )

    assert result.failed_jobs == 1
    assert real.frame(SnapshotDataset.INDEX_SNAPSHOT).height == 3
    assert sum("status=STORE_FAIL" in rec.message for rec in caplog.records) == 1


def test_session_counts_all_failing_jobs_with_warning(tmp_path, caplog) -> None:
    import logging

    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _FakeSource()
    source.auction = {"*": KisApiError("EGW00000", "bad")}
    source.news = {"*": KisApiError("EGW00000", "bad")}
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    with caplog.at_level(logging.WARNING, logger="src.marketdata.snapshot_service"):
        result = run_snapshot_session(
            settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=store,
            symbols=("005930",), now_fn=lambda: _t(8, 40, 5), sleep_fn=lambda s: None,
            wall_ns=_counter(), max_iterations=1,
        )

    assert result.failed_jobs == 2
    assert result.executed == 2
    assert sum("status=NO_SUCCESS" in rec.message for rec in caplog.records) == 2


def test_session_sleeps_capped_delay_when_nothing_due(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _session_source()
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    slept: list = []

    result = run_snapshot_session(
        settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=store,
        symbols=("005930",), now_fn=lambda: _t(15, 38, 45), sleep_fn=slept.append,
        wall_ns=_counter(), max_iterations=3,
    )

    assert slept == [5.0, 5.0]
    assert result.executed > 0


def test_session_sleeps_until_first_job_when_starting_early(tmp_path) -> None:
    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _session_source()
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    slept: list = []

    result = run_snapshot_session(
        settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=store,
        symbols=("005930",), now_fn=lambda: _t(7, 0), sleep_fn=slept.append,
        wall_ns=_counter(), max_iterations=2,
    )

    assert slept == [5.0, 5.0]
    assert (result.executed, result.expired) == (0, 0)
    assert source.calls == []


def test_session_emits_heartbeat_after_interval(tmp_path, caplog) -> None:
    import logging

    from src.core.config import DataPaths
    from src.storage.snapshot_store import SnapshotStore

    source = _session_source()
    store = SnapshotStore(paths=DataPaths(tmp_path), session_date=SESSION_DATE)
    ticks = [_t(9, 0, 10), _t(9, 10, 11)]

    with caplog.at_level(logging.INFO, logger="src.marketdata.snapshot_service"):
        run_snapshot_session(
            settings=SnapshotSettings(), session_date=SESSION_DATE, source=source, store=store,
            symbols=("005930",), now_fn=lambda: ticks.pop(0) if len(ticks) > 1 else ticks[0],
            sleep_fn=lambda s: None, wall_ns=_counter(), max_iterations=2,
        )

    assert sum("stage=snapshot_heartbeat" in rec.message for rec in caplog.records) == 2
