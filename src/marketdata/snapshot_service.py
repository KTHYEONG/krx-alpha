"""Session runner that executes the REST snapshot job plan against a quotation source."""

from __future__ import annotations

import datetime as dt
import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

import polars as pl

from src.brokers.kis.data import (
    TR_FLUCTUATION,
    TR_TRADE_AMOUNT,
    KisRankingRow,
)
from src.core.config import SnapshotSettings
from src.core.symbols import KRX_SHORT_CODE_PATTERN
from src.execution.contracts import KisApiError
from src.marketdata.snapshot_plan import (
    SnapshotJob,
    SnapshotJobKind,
    build_session_jobs,
    partition_due_jobs,
)
from src.marketdata.snapshot_schema import SnapshotDataset
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")

SNAPSHOT_HEARTBEAT_S: float = 600.0


class SnapshotSource(Protocol):
    """Read-only quotation endpoints consumed by the snapshot runner."""

    def get_auction_book(self, symbol: str) -> dict[str, object]: ...
    def get_investor_estimate(self, symbol: str) -> tuple[dict[str, object], ...]: ...
    def get_program_trade_latest(self, symbol: str) -> dict[str, object] | None: ...
    def get_index_snapshot(self, index_code: str) -> dict[str, object]: ...
    def get_index_minute_bars(self, index_code: str, *, session_date: dt.date) -> tuple[dict[str, object], ...]: ...
    def get_stock_minute_bars(
        self, symbol: str, *, session_date: dt.date, session_open: dt.time, session_close: dt.time
    ) -> tuple[dict[str, object], ...]: ...
    def get_news_titles(self, *, before: tuple[str, str] | None = None) -> tuple[dict[str, object], ...]: ...
    def get_trade_amount_ranking(self) -> tuple[KisRankingRow, ...]: ...
    def get_fluctuation_ranking(self) -> tuple[KisRankingRow, ...]: ...


@dataclass(frozen=True)
class SnapshotJobResult:
    job_id: str
    kind: SnapshotJobKind
    attempted: int
    succeeded: int
    failed: int
    rows_added: int
    truncated: bool


@dataclass(frozen=True)
class SnapshotSessionResult:
    executed: int
    expired: int
    failed_jobs: int
    rows_added: dict[str, int]


_KIND_DATASET: dict[SnapshotJobKind, SnapshotDataset] = {
    SnapshotJobKind.AUCTION_OPEN: SnapshotDataset.AUCTION_BOOK,
    SnapshotJobKind.AUCTION_CLOSE: SnapshotDataset.AUCTION_BOOK,
    SnapshotJobKind.INVESTOR_ESTIMATE: SnapshotDataset.INVESTOR_ESTIMATE,
    SnapshotJobKind.PROGRAM_TRADE: SnapshotDataset.PROGRAM_TRADE,
    SnapshotJobKind.RANKING: SnapshotDataset.RANKING,
    SnapshotJobKind.INDEX_SNAPSHOT: SnapshotDataset.INDEX_SNAPSHOT,
    SnapshotJobKind.INDEX_MINUTE_BAR: SnapshotDataset.INDEX_MINUTE_BAR,
    SnapshotJobKind.NEWS_TITLE: SnapshotDataset.NEWS_TITLE,
    SnapshotJobKind.EOD_MINUTE_BARS: SnapshotDataset.STOCK_MINUTE_BAR,
}

_SKIP: Any = object()


def _ns_to_kst_cursor(published_at_ns: int) -> tuple[str, str]:
    moment = dt.datetime.fromtimestamp(published_at_ns // 1_000_000_000, tz=_KST)
    return moment.strftime("%Y%m%d"), moment.strftime("%H%M%S")


def _eod_targets(
    store: SnapshotStore, settings: SnapshotSettings, symbols: tuple[str, ...]
) -> tuple[str, ...]:
    ranked = store.frame(SnapshotDataset.RANKING)
    if ranked.height == 0:
        return ()
    barred = store.frame(SnapshotDataset.STOCK_MINUTE_BAR)
    have = set(barred["symbol"].to_list()) if barred.height else set()
    cands = (
        ranked.filter(
            ~pl.col("symbol").is_in(list(symbols))
            & ~pl.col("symbol").is_in(sorted(have))
            & pl.col("symbol").str.contains(KRX_SHORT_CODE_PATTERN)
        )
        .group_by("symbol")
        .agg(
            pl.col("observed_at_ns").min().alias("_first_ns"),
            pl.col("rank").min().alias("_min_rank"),
        )
        .sort(["_first_ns", "_min_rank", "symbol"])
        .head(settings.stock_minute_max_symbols)
    )
    return tuple(cands["symbol"].to_list())


def run_snapshot_job(
    job: SnapshotJob,
    *,
    source: SnapshotSource,
    store: SnapshotStore,
    settings: SnapshotSettings,
    symbols: tuple[str, ...],
    news_seen: set[str],
    now_fn: Callable[[], dt.datetime],
    wall_ns: Callable[[], int] = time.time_ns,
) -> SnapshotJobResult:
    """Collect one bounded scheduled job and persist only validated rows."""
    session_date = store.session_date
    attempted = 0
    succeeded = 0
    failed = 0
    rows_added = 0
    truncated = False

    def _guarded_call(call: Callable[[], Any]) -> Any:
        nonlocal attempted, succeeded, failed, truncated
        if now_fn() >= job.not_after:
            truncated = True
            return _SKIP
        attempted += 1
        try:
            result = call()
        except KisApiError:
            failed += 1
            return _SKIP
        succeeded += 1
        return (result, wall_ns())

    def _take(outcome: Any) -> tuple[Any, int] | None:
        if outcome is _SKIP:
            return None
        return (outcome[0], outcome[1])

    def _handle_auction_security() -> int:
        if job.kind in (SnapshotJobKind.AUCTION_OPEN, SnapshotJobKind.AUCTION_CLOSE):
            phase = "open" if job.kind is SnapshotJobKind.AUCTION_OPEN else "close"
            auction_rows: list[dict[str, object]] = []
            for symbol in symbols:
                outcome = _guarded_call(partial(source.get_auction_book, symbol))
                taken = _take(outcome)
                if taken is None:
                    if truncated:
                        break
                    continue
                result, observed = taken
                auction_rows.append({**result, "phase": phase, "session_date": session_date, "observed_at_ns": observed})
            if auction_rows:
                return store.append(SnapshotDataset.AUCTION_BOOK, auction_rows)
            return 0
        if job.kind is SnapshotJobKind.INVESTOR_ESTIMATE:
            investor_rows: list[dict[str, object]] = []
            for symbol in symbols:
                outcome = _guarded_call(partial(source.get_investor_estimate, symbol))
                taken = _take(outcome)
                if taken is None:
                    if truncated:
                        break
                    continue
                result, observed = taken
                investor_rows.extend(
                    {**row, "session_date": session_date, "observed_at_ns": observed} for row in result
                )
            if investor_rows:
                return store.append(SnapshotDataset.INVESTOR_ESTIMATE, investor_rows)
            return 0
        program_rows: list[dict[str, object]] = []
        for symbol in symbols:
            outcome = _guarded_call(partial(source.get_program_trade_latest, symbol))
            taken = _take(outcome)
            if taken is None:
                if truncated:
                    break
                continue
            result, observed = taken
            if result is not None:
                program_rows.append({**result, "session_date": session_date, "observed_at_ns": observed})
        if program_rows:
            return store.append(SnapshotDataset.PROGRAM_TRADE, program_rows)
        return 0

    def _handle_ranking_index() -> int:
        if job.kind is SnapshotJobKind.RANKING:
            ranking_rows: list[dict[str, object]] = []
            outcome = _guarded_call(source.get_trade_amount_ranking)
            taken = _take(outcome)
            if taken is not None:
                result, observed = taken
                ranking_rows.extend(
                    {
                        "session_date": session_date,
                        "observed_at_ns": observed,
                        "source_tr": TR_TRADE_AMOUNT,
                        "market_div_code": "J",
                        "list_kind": "trade_amount",
                        "rank": row.rank,
                        "symbol": row.symbol,
                        "change_pct": row.change_pct,
                        "trade_value_krw": row.trade_value_krw,
                    }
                    for row in cast("tuple[KisRankingRow, ...]", result)
                )
            if not truncated:
                outcome = _guarded_call(source.get_fluctuation_ranking)
                taken = _take(outcome)
                if taken is not None:
                    result, observed = taken
                    ranking_rows.extend(
                        {
                            "session_date": session_date,
                            "observed_at_ns": observed,
                            "source_tr": TR_FLUCTUATION,
                            "market_div_code": "J",
                            "list_kind": "fluctuation",
                            "rank": row.rank,
                            "symbol": row.symbol,
                            "change_pct": row.change_pct,
                            "trade_value_krw": None,
                        }
                        for row in cast("tuple[KisRankingRow, ...]", result)
                    )
            if ranking_rows:
                return store.append(SnapshotDataset.RANKING, ranking_rows)
            return 0
        index_rows: list[dict[str, object]] = []
        for index_code in settings.index_codes:
            outcome = _guarded_call(partial(source.get_index_snapshot, index_code))
            taken = _take(outcome)
            if taken is None:
                if truncated:
                    break
                continue
            result, observed = taken
            index_rows.append({**result, "session_date": session_date, "observed_at_ns": observed})
        if index_rows:
            return store.append(SnapshotDataset.INDEX_SNAPSHOT, index_rows)
        return 0

    def _handle_news() -> int:
        nonlocal attempted, succeeded, failed, truncated
        news_rows: list[dict[str, object]] = []
        new_ids: list[str] = []
        claimed = set(news_seen)
        before: tuple[str, str] | None = None
        pages = 0
        while True:
            if now_fn() >= job.not_after:
                truncated = True
                break
            attempted += 1
            try:
                page = source.get_news_titles(before=before)
            except KisApiError:
                failed += 1
                break
            succeeded += 1
            pages += 1
            observed = wall_ns()
            fresh = [row for row in page if cast(str, row["news_id"]) not in claimed]
            for row in fresh:
                claimed.add(cast(str, row["news_id"]))
            news_rows.extend(
                {**row, "session_date": session_date, "observed_at_ns": observed} for row in fresh
            )
            new_ids.extend(cast(str, row["news_id"]) for row in fresh)
            if not page or any(cast(str, row["news_id"]) in news_seen for row in page) or not fresh:
                break
            if pages >= settings.news_max_pages:
                truncated = True
                break
            last = page[-1]
            before = _ns_to_kst_cursor(cast(int, last["published_at_ns"]))
        if news_rows:
            added = store.append(SnapshotDataset.NEWS_TITLE, news_rows)
            news_seen.update(new_ids)
            return added
        return 0

    def _handle_minute_bars() -> int:
        if job.kind is SnapshotJobKind.INDEX_MINUTE_BAR:
            bar_rows: list[dict[str, object]] = []
            for index_code in settings.index_codes:
                outcome = _guarded_call(
                    partial(source.get_index_minute_bars, index_code, session_date=session_date)
                )
                taken = _take(outcome)
                if taken is None:
                    if truncated:
                        break
                    continue
                result, observed = taken
                bar_rows.extend(
                    {**row, "session_date": session_date, "observed_at_ns": observed} for row in result
                )
            if bar_rows:
                return store.append(SnapshotDataset.INDEX_MINUTE_BAR, bar_rows)
            return 0
        added = 0
        for symbol in _eod_targets(store, settings, symbols):
            outcome = _guarded_call(
                partial(
                    source.get_stock_minute_bars,
                    symbol,
                    session_date=session_date,
                    session_open=settings.intraday_start,
                    session_close=settings.intraday_end,
                )
            )
            taken = _take(outcome)
            if taken is None:
                if truncated:
                    break
                continue
            result, observed = taken
            stamped = [
                {**row, "session_date": session_date, "observed_at_ns": observed}
                for row in cast("Sequence[dict[str, object]]", result)
            ]
            if stamped:
                added += store.append(SnapshotDataset.STOCK_MINUTE_BAR, stamped)
        return added

    if job.kind in (
        SnapshotJobKind.AUCTION_OPEN,
        SnapshotJobKind.AUCTION_CLOSE,
        SnapshotJobKind.INVESTOR_ESTIMATE,
        SnapshotJobKind.PROGRAM_TRADE,
    ):
        rows_added += _handle_auction_security()
    elif job.kind in (SnapshotJobKind.RANKING, SnapshotJobKind.INDEX_SNAPSHOT):
        rows_added += _handle_ranking_index()
    elif job.kind is SnapshotJobKind.NEWS_TITLE:
        rows_added += _handle_news()
    else:
        rows_added += _handle_minute_bars()
    return SnapshotJobResult(
        job_id=job.job_id,
        kind=job.kind,
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
        rows_added=rows_added,
        truncated=truncated,
    )


def run_snapshot_session(
    *,
    settings: SnapshotSettings,
    session_date: dt.date,
    source: SnapshotSource,
    store: SnapshotStore,
    symbols: tuple[str, ...],
    now_fn: Callable[[], dt.datetime],
    sleep_fn: Callable[[float], None],
    wall_ns: Callable[[], int] = time.time_ns,
    max_iterations: int | None = None,
) -> SnapshotSessionResult:
    """Run the session job plan until ``run_end`` and return aggregate counts.

    Jobs whose window already closed are expired without vendor calls, so a
    late start or restart never replays stale polls.
    """
    run_end = dt.datetime.combine(session_date, settings.run_end, tzinfo=_KST)
    jobs = build_session_jobs(settings, session_date)
    news_frame = store.frame(SnapshotDataset.NEWS_TITLE)
    news_seen = set(news_frame["news_id"].to_list()) if news_frame.height else set()
    completed: set[str] = set()
    executed = 0
    expired = 0
    failed_jobs = 0
    rows_added: dict[str, int] = {}
    last_heartbeat: dt.datetime | None = None
    iterations = 0
    while True:
        if max_iterations is not None and iterations >= max_iterations:
            break
        iterations += 1
        now = now_fn()
        if now >= run_end:
            break
        _, expired_now = partition_due_jobs(jobs, completed=completed, now=now)
        if expired_now:
            for job in expired_now:
                completed.add(job.job_id)
            expired += len(expired_now)
            logger.info("[DATA] stage=snapshot_expired count=%d", len(expired_now))
        due, _ = partition_due_jobs(jobs, completed=completed, now=now)
        stop = False
        for job in due:
            if now_fn() >= run_end:
                stop = True
                break
            try:
                result = run_snapshot_job(
                    job,
                    source=source,
                    store=store,
                    settings=settings,
                    symbols=symbols,
                    news_seen=news_seen,
                    now_fn=now_fn,
                    wall_ns=wall_ns,
                )
            except SnapshotStoreError as exc:
                logger.error("[DATA] stage=snapshot_job job=%s status=STORE_FAIL reason=%s", job.job_id, str(exc))
                completed.add(job.job_id)
                executed += 1
                failed_jobs += 1
                continue
            completed.add(job.job_id)
            executed += 1
            dataset = _KIND_DATASET[job.kind].value
            rows_added[dataset] = rows_added.get(dataset, 0) + result.rows_added
            if result.attempted > 0 and result.succeeded == 0:
                failed_jobs += 1
                logger.warning(
                    "[DATA] stage=snapshot_job job=%s attempted=%d failed=%d rows=%d truncated=%s status=NO_SUCCESS",
                    result.job_id,
                    result.attempted,
                    result.failed,
                    result.rows_added,
                    result.truncated,
                )
            elif job.kind in (SnapshotJobKind.RANKING, SnapshotJobKind.INDEX_SNAPSHOT, SnapshotJobKind.NEWS_TITLE):
                logger.debug(
                    "[DATA] stage=snapshot_job job=%s attempted=%d failed=%d rows=%d truncated=%s",
                    result.job_id,
                    result.attempted,
                    result.failed,
                    result.rows_added,
                    result.truncated,
                )
            else:
                logger.info(
                    "[DATA] stage=snapshot_job job=%s attempted=%d failed=%d rows=%d truncated=%s",
                    result.job_id,
                    result.attempted,
                    result.failed,
                    result.rows_added,
                    result.truncated,
                )
        if stop:
            break
        if not due:
            future = [j.due_at for j in jobs if j.job_id not in completed and j.due_at > now]
            wait_options = [settings.idle_sleep_cap_s, (run_end - now).total_seconds()]
            if future:
                wait_options.append((min(future) - now).total_seconds())
            delay = min(wait_options)
            if delay > 0:
                sleep_fn(delay)
        if last_heartbeat is None or (now - last_heartbeat).total_seconds() >= SNAPSHOT_HEARTBEAT_S:
            logger.info(
                "[DATA] stage=snapshot_heartbeat executed=%d expired=%d rows=%s",
                executed,
                expired,
                rows_added,
            )
            last_heartbeat = now
    logger.info(
        "[DATA] stage=snapshot_summary executed=%d expired=%d failed_jobs=%d rows=%s",
        executed,
        expired,
        failed_jobs,
        rows_added,
    )
    return SnapshotSessionResult(
        executed=executed, expired=expired, failed_jobs=failed_jobs, rows_added=rows_added
    )
