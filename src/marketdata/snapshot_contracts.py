"""Intraday REST snapshot dataset schemas and deterministic session job plan."""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.core.config import SnapshotSettings

_KST = ZoneInfo("Asia/Seoul")


class SnapshotDataset(StrEnum):
    """REST snapshot dataset identifiers."""

    SECURITY_STATUS = "security_status"
    AUCTION_BOOK = "auction_book"
    INVESTOR_ESTIMATE = "investor_estimate"
    PROGRAM_TRADE = "program_trade"
    RANKING = "ranking"
    INDEX_SNAPSHOT = "index_snapshot"
    INDEX_MINUTE_BAR = "index_minute_bar"
    NEWS_TITLE = "news_title"
    STOCK_MINUTE_BAR = "stock_minute_bar"


COMMON_SNAPSHOT_COLUMNS: dict[str, Any] = {
    "session_date": pl.Date,
    "observed_at_ns": pl.Int64,
    "source_tr": pl.String,
    "market_div_code": pl.String,
}

SNAPSHOT_SCHEMAS: dict[SnapshotDataset, dict[str, Any]] = {
    SnapshotDataset.SECURITY_STATUS: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "status_code": pl.String,
        "managed": pl.Boolean,
        "market_warning_code": pl.String,
        "short_overheated": pl.Boolean,
        "investment_caution": pl.Boolean,
        "liquidation_trading": pl.Boolean,
        "trading_halted": pl.Boolean,
        "vi_code": pl.String,
        "overtime_vi_code": pl.String,
        "credit_available": pl.Boolean,
        "last_price": pl.Int64,
        "base_price": pl.Int64,
        "upper_limit": pl.Int64,
        "lower_limit": pl.Int64,
    },
    SnapshotDataset.AUCTION_BOOK: {
        **COMMON_SNAPSHOT_COLUMNS,
        "phase": pl.String,
        "symbol": pl.String,
        "book_time": pl.String,
        "auction_code": pl.String,
        "expected_price": pl.Int64,
        "expected_volume": pl.Int64,
        "expected_change_pct": pl.Float64,
        "vi_code": pl.String,
        "last_price": pl.Int64,
        "base_price": pl.Int64,
        "ask_prices": pl.List(pl.Int64),
        "ask_sizes": pl.List(pl.Int64),
        "bid_prices": pl.List(pl.Int64),
        "bid_sizes": pl.List(pl.Int64),
        "total_ask_size": pl.Int64,
        "total_bid_size": pl.Int64,
    },
    SnapshotDataset.INVESTOR_ESTIMATE: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "bucket": pl.Int64,
        "foreign_net_qty": pl.Int64,
        "institution_net_qty": pl.Int64,
        "total_net_qty": pl.Int64,
    },
    SnapshotDataset.PROGRAM_TRADE: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "trade_time": pl.String,
        "cum_volume": pl.Int64,
        "sell_qty": pl.Int64,
        "buy_qty": pl.Int64,
        "net_qty": pl.Int64,
        "sell_value_krw": pl.Int64,
        "buy_value_krw": pl.Int64,
        "net_value_krw": pl.Int64,
    },
    SnapshotDataset.RANKING: {
        **COMMON_SNAPSHOT_COLUMNS,
        "list_kind": pl.String,
        "rank": pl.Int64,
        "symbol": pl.String,
        "change_pct": pl.Float64,
        "trade_value_krw": pl.Int64,
    },
    SnapshotDataset.INDEX_SNAPSHOT: {
        **COMMON_SNAPSHOT_COLUMNS,
        "index_code": pl.String,
        "index_value": pl.Float64,
        "change_pct": pl.Float64,
        "cum_value_mil_krw": pl.Int64,
        "advancers": pl.Int64,
        "decliners": pl.Int64,
    },
    SnapshotDataset.INDEX_MINUTE_BAR: {
        **COMMON_SNAPSHOT_COLUMNS,
        "index_code": pl.String,
        "bar_time": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
        "cum_value_mil_krw": pl.Int64,
    },
    SnapshotDataset.NEWS_TITLE: {
        **COMMON_SNAPSHOT_COLUMNS,
        "news_id": pl.String,
        "published_at_ns": pl.Int64,
        "provider": pl.String,
        "provider_code": pl.String,
        "category_code": pl.String,
        "title": pl.String,
        "symbols": pl.List(pl.String),
    },
    SnapshotDataset.STOCK_MINUTE_BAR: {
        **COMMON_SNAPSHOT_COLUMNS,
        "symbol": pl.String,
        "bar_time": pl.String,
        "open": pl.Int64,
        "high": pl.Int64,
        "low": pl.Int64,
        "close": pl.Int64,
        "volume": pl.Int64,
    },
}

SNAPSHOT_DEDUP_KEYS: dict[SnapshotDataset, tuple[str, ...]] = {
    SnapshotDataset.SECURITY_STATUS: ("symbol", "observed_at_ns"),
    SnapshotDataset.AUCTION_BOOK: ("symbol", "observed_at_ns"),
    SnapshotDataset.PROGRAM_TRADE: ("symbol", "observed_at_ns"),
    SnapshotDataset.INVESTOR_ESTIMATE: ("symbol", "bucket", "observed_at_ns"),
    SnapshotDataset.RANKING: ("list_kind", "rank", "observed_at_ns"),
    SnapshotDataset.INDEX_SNAPSHOT: ("index_code", "observed_at_ns"),
    SnapshotDataset.INDEX_MINUTE_BAR: ("index_code", "bar_time"),
    SnapshotDataset.NEWS_TITLE: ("news_id",),
    SnapshotDataset.STOCK_MINUTE_BAR: ("symbol", "bar_time"),
}


class SnapshotJobKind(StrEnum):
    """Scheduled snapshot job kinds in same-instant execution priority order."""

    AUCTION_OPEN = "auction_open"
    AUCTION_CLOSE = "auction_close"
    INVESTOR_ESTIMATE = "investor_estimate"
    PROGRAM_TRADE = "program_trade"
    RANKING = "ranking"
    INDEX_SNAPSHOT = "index_snapshot"
    INDEX_MINUTE_BAR = "index_minute_bar"
    NEWS_TITLE = "news_title"
    EOD_MINUTE_BARS = "eod_minute_bars"


@dataclass(frozen=True)
class SnapshotJob:
    """One scheduled snapshot execution window.

    ``not_after`` bounds staleness: a job that could not start before it is
    skipped rather than executed late, so a restarted collector never floods
    the vendor with backlogged polls whose results would be mislabeled.
    """

    job_id: str
    kind: SnapshotJobKind
    due_at: dt.datetime
    not_after: dt.datetime


def _aware(session_date: dt.date, t: dt.time) -> dt.datetime:
    return dt.datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        t.hour,
        t.minute,
        t.second,
        t.microsecond,
        tzinfo=_KST,
    )


def _add_seconds(base: dt.datetime, seconds: int) -> dt.datetime:
    return base + dt.timedelta(seconds=seconds)


def _interval_series(start: dt.datetime, interval_s: int, end_exclusive: dt.datetime) -> list[dt.datetime]:
    out: list[dt.datetime] = []
    cur = start
    while cur < end_exclusive:
        out.append(cur)
        cur = _add_seconds(cur, interval_s)
    return out


def _interval_series_inclusive(start: dt.datetime, interval_s: int, end_inclusive: dt.datetime) -> list[dt.datetime]:
    out: list[dt.datetime] = []
    cur = start
    while cur <= end_inclusive:
        out.append(cur)
        cur = _add_seconds(cur, interval_s)
    return out


def _coverage_series(start_exclusive: dt.datetime, interval_s: int, end_inclusive: dt.datetime) -> list[dt.datetime]:
    """Build due times that guarantee a vendor 100-unit lookback window never gaps.

    Each due time after the first is reachable from the previous one within
    ``interval_s``, and the series always ends exactly at ``end_inclusive`` so
    the closing bars are never missed even when the interval does not evenly
    divide the session length.
    """
    out: list[dt.datetime] = []
    cur = _add_seconds(start_exclusive, interval_s)
    while cur < end_inclusive:
        out.append(cur)
        cur = _add_seconds(cur, interval_s)
    if not out or out[-1] != end_inclusive:
        out.append(end_inclusive)
    return out


def build_session_jobs(settings: SnapshotSettings, session_date: dt.date) -> tuple[SnapshotJob, ...]:
    """Expand snapshot settings into the ordered, KST-aware job plan of one session.

    Args:
        settings: Validated snapshot settings.
        session_date: KST trading date the jobs run on.

    Returns:
        Jobs sorted by ``(due_at, kind declaration order)``.
    """
    kind_order = {kind: idx for idx, kind in enumerate(SnapshotJobKind)}
    intraday_start = _aware(session_date, settings.intraday_start)
    intraday_end = _aware(session_date, settings.intraday_end)
    run_end = _aware(session_date, settings.run_end)
    eod_collect = _aware(session_date, settings.eod_collect_time)

    per_kind: dict[SnapshotJobKind, list[dt.datetime]] = {
        SnapshotJobKind.AUCTION_OPEN: [_aware(session_date, t) for t in settings.auction_open_times],
        SnapshotJobKind.AUCTION_CLOSE: [_aware(session_date, t) for t in settings.auction_close_times],
        SnapshotJobKind.INVESTOR_ESTIMATE: [_aware(session_date, t) for t in (*settings.investor_estimate_times, settings.eod_collect_time)],
        SnapshotJobKind.RANKING: _interval_series(intraday_start, settings.ranking_interval_s, intraday_end),
        SnapshotJobKind.INDEX_SNAPSHOT: _interval_series(intraday_start, settings.index_interval_s, intraday_end),
        SnapshotJobKind.INDEX_MINUTE_BAR: _coverage_series(intraday_start, settings.index_minute_interval_s, intraday_end),
        SnapshotJobKind.NEWS_TITLE: _interval_series(
            _aware(session_date, settings.news_start), settings.news_interval_s, run_end
        ),
        SnapshotJobKind.EOD_MINUTE_BARS: [eod_collect],
    }
    program_start = _add_seconds(intraday_start, settings.program_trade_interval_s)
    per_kind[SnapshotJobKind.PROGRAM_TRADE] = [
        *_interval_series_inclusive(program_start, settings.program_trade_interval_s, intraday_end),
        eod_collect,
    ]

    deadlines = {
        SnapshotJobKind.AUCTION_OPEN: _aware(session_date, settings.auction_open_deadline),
        SnapshotJobKind.AUCTION_CLOSE: _aware(session_date, settings.auction_close_deadline),
    }

    jobs: list[SnapshotJob] = []
    seen_ids: set[str] = set()
    for kind in SnapshotJobKind:
        dues = sorted(per_kind[kind])
        deadline = deadlines.get(kind, run_end)
        for idx, due in enumerate(dues):
            not_after = dues[idx + 1] if idx + 1 < len(dues) else deadline
            if not due < not_after:
                raise ValueError(f"job window must satisfy due_at < not_after: {kind.value}@{due}")
            job_id = f"{kind.value}@{due:%H%M%S}"
            if job_id in seen_ids:
                raise ValueError(f"duplicate job_id: {job_id}")
            seen_ids.add(job_id)
            jobs.append(SnapshotJob(job_id=job_id, kind=kind, due_at=due, not_after=not_after))
    jobs.sort(key=lambda j: (j.due_at, kind_order[j.kind]))
    return tuple(jobs)


def partition_due_jobs(
    jobs: Sequence[SnapshotJob], *, completed: AbstractSet[str], now: dt.datetime
) -> tuple[tuple[SnapshotJob, ...], tuple[SnapshotJob, ...]]:
    """Split pending jobs into runnable and expired sets at ``now``.

    Returns:
        ``(due, expired)`` preserving plan order. ``due`` holds jobs with
        ``due_at <= now < not_after``; ``expired`` holds jobs with
        ``now >= not_after``. Completed job ids and future jobs appear in neither.

    Raises:
        ValueError: If ``now`` is naive.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be tz-aware")
    due: list[SnapshotJob] = []
    expired: list[SnapshotJob] = []
    for job in jobs:
        if job.job_id in completed:
            continue
        if job.due_at <= now < job.not_after:
            due.append(job)
        elif now >= job.not_after:
            expired.append(job)
    return tuple(due), tuple(expired)
