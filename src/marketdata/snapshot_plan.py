"""Deterministic KST session job plan for REST snapshots."""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import StrEnum
from zoneinfo import ZoneInfo

from src.core.calendar import SessionSchedule, schedule_for
from src.core.config import SnapshotSettings, validate_snapshot_order
from src.core.session_anchors import SessionAnchors

_KST = ZoneInfo("Asia/Seoul")
_BASE_DATE: dt.date = dt.date(2000, 1, 1)


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


def _shift_time(value: dt.time, delta: dt.timedelta) -> dt.time:
    return (dt.datetime.combine(_BASE_DATE, value) + delta).time()


def shift_snapshot_settings(settings: SnapshotSettings, anchors: SessionAnchors) -> SnapshotSettings:
    """Move every snapshot time with the session it samples.

    Opening-auction samples follow the open shift; closing-auction, EOD minute
    bars, run end and intraday end follow the close shift; intraday starts,
    investor-estimate times and news start follow the open shift.

    Raises:
        ValueError: If the shifted times violate the session order against the
            day's shifted close transition.
    """
    open_shift = anchors.open_shift
    close_shift = anchors.close_shift
    shifted = settings.model_copy(
        update={
            "auction_open_times": tuple(_shift_time(t, open_shift) for t in settings.auction_open_times),
            "auction_open_deadline": _shift_time(settings.auction_open_deadline, open_shift),
            "auction_close_times": tuple(_shift_time(t, close_shift) for t in settings.auction_close_times),
            "auction_close_deadline": _shift_time(settings.auction_close_deadline, close_shift),
            "intraday_start": _shift_time(settings.intraday_start, open_shift),
            "intraday_end": _shift_time(settings.intraday_end, close_shift),
            "news_start": _shift_time(settings.news_start, open_shift),
            "investor_estimate_times": tuple(_shift_time(t, open_shift) for t in settings.investor_estimate_times),
            "eod_collect_time": _shift_time(settings.eod_collect_time, close_shift),
            "run_end": _shift_time(settings.run_end, close_shift),
        }
    )
    validate_snapshot_order(shifted, market_close=schedule_for(anchors, SessionSchedule()).market_close)
    return shifted


def build_session_jobs(
    settings: SnapshotSettings, session_date: dt.date,
) -> tuple[SnapshotJob, ...]:
    """Build deterministic KST jobs from one validated snapshot schedule."""
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
    jobs: Sequence[SnapshotJob], *, completed: AbstractSet[str],
    now: dt.datetime,
) -> tuple[tuple[SnapshotJob, ...], tuple[SnapshotJob, ...]]:
    """Separate due and expired jobs without replaying closed windows."""
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
