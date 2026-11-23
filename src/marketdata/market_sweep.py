"""Full-market auction book and investor-estimate REST sweep over the prior day's listing."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Final, Protocol
from zoneinfo import ZoneInfo

from src.core.calendar import SessionSchedule, schedule_for
from src.core.config import MarketSweepSettings, validate_market_sweep_order
from src.core.errors import KrxAlphaError
from src.core.session_anchors import SessionAnchors
from src.core.symbols import is_krx_short_code
from src.marketdata.snapshot_plan import SnapshotJob, SnapshotJobKind, partition_due_jobs
from src.marketdata.snapshot_schema import SnapshotDataset
from src.marketdata.snapshot_service import SnapshotJobResult, SnapshotSessionResult, _JobBudget
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_BASE_DATE: Final[dt.date] = dt.date(2000, 1, 1)
_EXPIRED_JOBS_LOGGED: Final[int] = 8

MARKET_SWEEP_HEARTBEAT_S: Final[float] = 600.0

MARKET_SWEEP_DATASETS: Final[Mapping[SnapshotJobKind, SnapshotDataset]] = {
    SnapshotJobKind.AUCTION_OPEN: SnapshotDataset.AUCTION_BOOK_MARKET,
    SnapshotJobKind.AUCTION_CLOSE: SnapshotDataset.AUCTION_BOOK_MARKET,
    SnapshotJobKind.INVESTOR_ESTIMATE: SnapshotDataset.INVESTOR_ESTIMATE_MARKET,
}


class MarketSweepSource(Protocol):
    """The only two read-only quotation endpoints the sweep may call."""

    def get_auction_book(self, symbol: str) -> dict[str, object]: ...
    def get_investor_estimate(self, symbol: str) -> tuple[dict[str, object], ...]: ...


class QuietCheckRunner(Protocol):
    """Idle-iteration hook that may confirm quiet suspects through the sweep's read-only source."""

    def run(self, source: MarketSweepSource) -> int: ...


class MarketMapError(KrxAlphaError):
    """The full-market symbol source is missing, unreadable or holds no eligible symbol."""


def load_market_symbols(path: pathlib.Path, *, markets: Sequence[str]) -> tuple[str, ...]:
    """Return sorted unique KRX short codes listed on ``markets`` in a ``market_map.json`` file.

    Raises:
        MarketMapError: The file is missing, unreadable, not a symbol-to-market object,
            or yields no eligible symbol.
    """
    try:
        raw = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise MarketMapError(f"market_map_unreadable:{type(exc).__name__}") from exc
    if not isinstance(raw, dict):
        raise MarketMapError("market_map_not_object")
    wanted = frozenset(markets)
    symbols = sorted({code for code, market in raw.items() if market in wanted and is_krx_short_code(code)})
    if not symbols:
        raise MarketMapError("market_map_empty")
    return tuple(symbols)


def _shift_time(value: dt.time, delta: dt.timedelta) -> dt.time:
    return (dt.datetime.combine(_BASE_DATE, value) + delta).time()


def shift_market_sweep_settings(settings: MarketSweepSettings, anchors: SessionAnchors) -> MarketSweepSettings:
    """Move sweep times with the session they sample: open-side times follow the open shift, the rest the close shift.

    Raises:
        ValueError: If the shifted times violate the sweep order against the day's shifted close transition.
    """
    open_shift = anchors.open_shift
    close_shift = anchors.close_shift
    shifted = settings.model_copy(
        update={
            "auction_open_times": tuple(_shift_time(t, open_shift) for t in settings.auction_open_times),
            "auction_open_deadline": _shift_time(settings.auction_open_deadline, open_shift),
            "investor_estimate_times": tuple(_shift_time(t, open_shift) for t in settings.investor_estimate_times),
            "auction_close_times": tuple(_shift_time(t, close_shift) for t in settings.auction_close_times),
            "auction_close_deadline": _shift_time(settings.auction_close_deadline, close_shift),
            "investor_estimate_close_time": _shift_time(settings.investor_estimate_close_time, close_shift),
            "run_end": _shift_time(settings.run_end, close_shift),
        }
    )
    validate_market_sweep_order(shifted, market_close=schedule_for(anchors, SessionSchedule()).market_close)
    return shifted


def _aware(session_date: dt.date, t: dt.time) -> dt.datetime:
    return dt.datetime.combine(session_date, t, tzinfo=_KST)


def build_market_sweep_jobs(settings: MarketSweepSettings, session_date: dt.date) -> tuple[SnapshotJob, ...]:
    """Build the sweep's KST jobs; each job closes at its kind's next due time, deadline, or the next other-kind job.

    Bounding by the next other-kind job keeps a late or slow pass from consuming an auction window.
    """
    dues: dict[SnapshotJobKind, list[dt.datetime]] = {
        SnapshotJobKind.AUCTION_OPEN: [_aware(session_date, t) for t in settings.auction_open_times],
        SnapshotJobKind.AUCTION_CLOSE: [_aware(session_date, t) for t in settings.auction_close_times],
        SnapshotJobKind.INVESTOR_ESTIMATE: [
            _aware(session_date, t) for t in (*settings.investor_estimate_times, settings.investor_estimate_close_time)
        ],
    }
    deadlines = {
        SnapshotJobKind.AUCTION_OPEN: _aware(session_date, settings.auction_open_deadline),
        SnapshotJobKind.AUCTION_CLOSE: _aware(session_date, settings.auction_close_deadline),
        SnapshotJobKind.INVESTOR_ESTIMATE: _aware(session_date, settings.run_end),
    }
    jobs: list[SnapshotJob] = []
    for kind, kind_dues in dues.items():
        others = sorted(due for other, other_dues in dues.items() if other is not kind for due in other_dues)
        for idx, due in enumerate(kind_dues):
            bounds = [kind_dues[idx + 1] if idx + 1 < len(kind_dues) else deadlines[kind]]
            later_other = next((other for other in others if other > due), None)
            if later_other is not None:
                bounds.append(later_other)
            jobs.append(
                SnapshotJob(job_id=f"{kind.value}@{due:%H%M%S}", kind=kind, due_at=due, not_after=min(bounds))
            )
    order = {kind: idx for idx, kind in enumerate(SnapshotJobKind)}
    jobs.sort(key=lambda job: (job.due_at, order[job.kind]))
    return tuple(jobs)


def pass_positions(jobs: Sequence[SnapshotJob]) -> dict[str, tuple[int, int]]:
    """Map each job id to ``(pass_index, passes_of_kind)`` in due order within its kind."""
    by_kind: dict[SnapshotJobKind, list[SnapshotJob]] = {}
    for job in sorted(jobs, key=lambda j: j.due_at):
        by_kind.setdefault(job.kind, []).append(job)
    return {
        job.job_id: (idx, len(kind_jobs)) for kind_jobs in by_kind.values() for idx, job in enumerate(kind_jobs)
    }


def rotation_offset(pass_index: int, passes: int, size: int) -> int:
    """Start offset that spreads passes evenly over the list, so truncated passes miss different tails."""
    if passes <= 0 or size <= 0:
        return 0
    return (pass_index * size) // passes


def rotate(symbols: tuple[str, ...], offset: int) -> tuple[str, ...]:
    """Return ``symbols`` starting at ``offset`` and wrapping around."""
    return symbols[offset:] + symbols[:offset]


def run_market_sweep_job(
    job: SnapshotJob,
    *,
    offset: int,
    source: MarketSweepSource,
    store: SnapshotStore,
    symbols: tuple[str, ...],
    now_fn: Callable[[], dt.datetime],
    wall_ns: Callable[[], int] = time.time_ns,
) -> SnapshotJobResult:
    """Run one bounded sweep pass from ``offset`` and persist rows to the job's ``*_market`` dataset.

    Raises:
        ValueError: ``job.kind`` is not a sweep job kind.
        SnapshotStoreError: The store rejects the rows.
    """
    session_date = store.session_date
    budget = _JobBudget(not_after=job.not_after, now_fn=now_fn, wall_ns=wall_ns)
    ordered = rotate(symbols, offset)
    rows: list[dict[str, object]]
    match job.kind:
        case SnapshotJobKind.AUCTION_OPEN | SnapshotJobKind.AUCTION_CLOSE:
            phase = "open" if job.kind is SnapshotJobKind.AUCTION_OPEN else "close"
            rows = [
                {**result, "phase": phase, "session_date": session_date, "observed_at_ns": observed}
                for result, observed in budget.each(ordered, source.get_auction_book)
            ]
        case SnapshotJobKind.INVESTOR_ESTIMATE:
            rows = [
                {**row, "session_date": session_date, "observed_at_ns": observed}
                for result, observed in budget.each(ordered, source.get_investor_estimate)
                for row in result
            ]
        case _:
            raise ValueError(f"not a market sweep job kind: {job.kind.value}")
    rows_added = store.append(MARKET_SWEEP_DATASETS[job.kind], rows) if rows else 0
    return SnapshotJobResult(
        job_id=job.job_id,
        kind=job.kind,
        attempted=budget.attempted,
        succeeded=budget.succeeded,
        failed=budget.failed,
        rows_added=rows_added,
        truncated=budget.truncated,
        first_error=budget.first_error,
        failed_keys=tuple(budget.failed_keys),
    )


def _log_job(result: SnapshotJobResult, *, symbols: int, offset: int) -> None:
    degraded = result.truncated or result.failed > 0
    logger.log(
        logging.WARNING if degraded else logging.INFO,
        "[DATA] stage=market_sweep_job job=%s symbols=%d offset=%d attempted=%d failed=%d rows=%d truncated=%s"
        " reason=%s",
        result.job_id,
        symbols,
        offset,
        result.attempted,
        result.failed,
        result.rows_added,
        result.truncated,
        result.first_error,
    )


def run_market_sweep_session(
    *,
    settings: MarketSweepSettings,
    session_date: dt.date,
    source: MarketSweepSource,
    store: SnapshotStore,
    symbols: tuple[str, ...],
    now_fn: Callable[[], dt.datetime],
    sleep_fn: Callable[[float], None],
    wall_ns: Callable[[], int] = time.time_ns,
    max_iterations: int | None = None,
    quiet_check: QuietCheckRunner | None = None,
) -> SnapshotSessionResult:
    """Run the sweep plan until ``run_end``; jobs whose window already closed expire without vendor calls."""
    run_end = _aware(session_date, settings.run_end)
    jobs = build_market_sweep_jobs(settings, session_date)
    positions = pass_positions(jobs)
    completed: set[str] = set()
    executed = 0
    expired = 0
    failed_jobs = 0
    rows_added: dict[str, int] = {}
    last_heartbeat: dt.datetime | None = None
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        now = now_fn()
        if now >= run_end:
            break
        due, expired_now = partition_due_jobs(jobs, completed=completed, now=now)
        if expired_now:
            completed.update(job.job_id for job in expired_now)
            expired += len(expired_now)
            logger.info(
                "[DATA] stage=market_sweep_expired count=%d jobs=%s",
                len(expired_now),
                ",".join(job.job_id for job in expired_now[:_EXPIRED_JOBS_LOGGED]),
            )
        for job in due:
            pass_index, passes = positions[job.job_id]
            offset = rotation_offset(pass_index, passes, len(symbols))
            completed.add(job.job_id)
            executed += 1
            try:
                result = run_market_sweep_job(
                    job, offset=offset, source=source, store=store, symbols=symbols, now_fn=now_fn, wall_ns=wall_ns
                )
            except SnapshotStoreError as exc:
                logger.error("[DATA] stage=market_sweep_job job=%s status=STORE_FAIL reason=%s", job.job_id, str(exc))
                failed_jobs += 1
                continue
            dataset = MARKET_SWEEP_DATASETS[job.kind].value
            rows_added[dataset] = rows_added.get(dataset, 0) + result.rows_added
            if result.attempted > 0 and result.succeeded == 0:
                failed_jobs += 1
            _log_job(result, symbols=len(symbols), offset=offset)
        if not due:
            if quiet_check is not None:
                quiet_check.run(source)
                now = now_fn()
            waits = [settings.idle_sleep_cap_s, (run_end - now).total_seconds()]
            waits.extend(
                (job.due_at - now).total_seconds() for job in jobs if job.job_id not in completed and job.due_at > now
            )
            sleep_fn(min(waits))
        if last_heartbeat is None or (now - last_heartbeat).total_seconds() >= MARKET_SWEEP_HEARTBEAT_S:
            logger.info(
                "[DATA] stage=market_sweep_heartbeat executed=%d expired=%d rows=%s", executed, expired, rows_added
            )
            last_heartbeat = now
    logger.info(
        "[DATA] stage=market_sweep_summary symbols=%d executed=%d expired=%d failed_jobs=%d rows=%s",
        len(symbols),
        executed,
        expired,
        failed_jobs,
        rows_added,
    )
    return SnapshotSessionResult(executed=executed, expired=expired, failed_jobs=failed_jobs, rows_added=rows_added)


def idle_until_run_end(
    settings: MarketSweepSettings,
    session_date: dt.date,
    *,
    now_fn: Callable[[], dt.datetime],
    sleep_fn: Callable[[float], None],
    max_iterations: int | None = None,
) -> None:
    """Sleep without vendor calls until ``run_end`` so a degraded child is not restarted in a loop."""
    run_end = _aware(session_date, settings.run_end)
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        iterations += 1
        remaining = (run_end - now_fn()).total_seconds()
        if remaining <= 0:
            return
        sleep_fn(min(MARKET_SWEEP_HEARTBEAT_S, remaining))


__all__ = [
    "MARKET_SWEEP_DATASETS",
    "MarketMapError",
    "MarketSweepSource",
    "build_market_sweep_jobs",
    "idle_until_run_end",
    "load_market_symbols",
    "pass_positions",
    "rotate",
    "rotation_offset",
    "run_market_sweep_job",
    "run_market_sweep_session",
    "shift_market_sweep_settings",
]
