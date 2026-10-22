"""Per-shard staleness verdicts for aftermarket/premarket KIS collectors."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from zoneinfo import ZoneInfo

from src.core.session_anchors import SessionAnchors
from src.orchestration.premarket import premarket_window
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.streamer import aftermarket_open_time
from src.storage.layout import (
    l0_journal_file_glob,
    l0_partition_key,
    l0_partition_relpath,
)

_KST = ZoneInfo("Asia/Seoul")


class ShardVerdict(StrEnum):
    OK = "OK"
    STALE = "STALE"
    VENUE_WIDE_SILENCE = "VENUE_WIDE_SILENCE"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    OUTSIDE_WINDOW = "OUTSIDE_WINDOW"
    NOT_RUNNING = "NOT_RUNNING"


@dataclass(frozen=True)
class ShardObservation:
    key: str
    venue: str
    running: bool
    started_at_wall: float | None = None
    last_journal_wall: float | None = None


@dataclass(frozen=True)
class ShardDecision:
    key: str
    verdict: ShardVerdict
    silent_s: float | None
    limit_s: float
    restart: bool


class ShardWatchdog:
    """Pure per-shard staleness verdicts for one daemon cycle."""

    def __init__(self, *, limit_s: float, max_restarts_per_day: int) -> None:
        if limit_s <= 0:
            raise ValueError(f"limit_s must be positive, got {limit_s}")
        if max_restarts_per_day < 0:
            raise ValueError(f"max_restarts_per_day must be >= 0, got {max_restarts_per_day}")
        self._limit_s = limit_s
        self._max_restarts = max_restarts_per_day
        self._counts: dict[tuple[str, dt.date], int] = {}

    def _used(self, key: str, day: dt.date) -> int:
        return self._counts.get((key, day), 0)

    def record_restart(self, key: str, *, day: dt.date) -> int:
        """Count one stale restart for `key` on `day`; returns the day's total."""
        total = self._used(key, day) + 1
        self._counts[(key, day)] = total
        return total

    def evaluate(
        self,
        observations: Sequence[ShardObservation],
        *,
        now_wall: float,
        window_open_wall: float,
        in_window: bool,
        day: dt.date,
    ) -> tuple[ShardDecision, ...]:
        """Pure per-shard staleness verdicts for one daemon cycle."""
        if not in_window:
            return tuple(
                ShardDecision(key=o.key, verdict=ShardVerdict.OUTSIDE_WINDOW, silent_s=None, limit_s=self._limit_s, restart=False)
                for o in observations
            )
        silent: dict[str, float | None] = {}
        stale_keys: set[str] = set()
        for o in observations:
            if not o.running:
                continue
            baseline = window_open_wall
            if o.last_journal_wall is not None and o.last_journal_wall > baseline:
                baseline = o.last_journal_wall
            if o.started_at_wall is not None and o.started_at_wall > baseline:
                baseline = o.started_at_wall
            age = now_wall - baseline
            if age < 0:
                age = 0.0
            silent[o.key] = age
            if age > self._limit_s:
                stale_keys.add(o.key)
        venue_running: dict[str, list[str]] = {}
        venue_stale: dict[str, list[str]] = {}
        for o in observations:
            if not o.running:
                continue
            venue_running.setdefault(o.venue, []).append(o.key)
            if o.key in stale_keys:
                venue_stale.setdefault(o.venue, []).append(o.key)
        venue_wide: set[str] = set()
        for venue, keys in venue_running.items():
            if len(keys) >= 2 and len(venue_stale.get(venue, [])) == len(keys):
                venue_wide.update(keys)
        decisions: list[ShardDecision] = []
        for o in observations:
            if not o.running:
                decisions.append(
                    ShardDecision(key=o.key, verdict=ShardVerdict.NOT_RUNNING, silent_s=None, limit_s=self._limit_s, restart=False)
                )
            elif o.key in venue_wide:
                decisions.append(
                    ShardDecision(key=o.key, verdict=ShardVerdict.VENUE_WIDE_SILENCE, silent_s=silent[o.key], limit_s=self._limit_s, restart=False)
                )
            elif o.key in stale_keys:
                if self._used(o.key, day) >= self._max_restarts:
                    decisions.append(
                        ShardDecision(key=o.key, verdict=ShardVerdict.BUDGET_EXHAUSTED, silent_s=silent[o.key], limit_s=self._limit_s, restart=False)
                    )
                else:
                    decisions.append(
                        ShardDecision(key=o.key, verdict=ShardVerdict.STALE, silent_s=silent[o.key], limit_s=self._limit_s, restart=True)
                    )
            else:
                decisions.append(
                    ShardDecision(key=o.key, verdict=ShardVerdict.OK, silent_s=silent[o.key], limit_s=self._limit_s, restart=False)
                )
        return tuple(decisions)


def shard_window(
    venue: MarketVenue, session: MarketSession, *, day: dt.date, anchors: SessionAnchors
) -> tuple[dt.datetime, dt.datetime]:
    """KST `[open, end)` of the shard's active window."""
    if session is MarketSession.NXT_PRE:
        if venue is not MarketVenue.NXT:
            raise ValueError(f"no premarket window for venue {venue!r}")
        start, end = premarket_window(anchors)
    elif session is MarketSession.NXT_AFTER:
        if venue is not MarketVenue.NXT:
            raise ValueError(f"no nxt_after window for venue {venue!r}")
        start, end = aftermarket_open_time(venue, anchors), anchors.after_market_end
    elif session is MarketSession.KRX_AFTER:
        if venue is not MarketVenue.KRX:
            raise ValueError(f"no krx_after window for venue {venue!r}")
        start, end = aftermarket_open_time(venue, anchors), anchors.after_market_end
    elif session is MarketSession.REGULAR:
        if venue is not MarketVenue.KRX:
            raise ValueError(f"no regular window for venue {venue!r}")
        start, end = anchors.regular_open, anchors.closing_auction_start
    else:
        raise ValueError(f"no shard window for venue {venue!r} session {session!r}")
    return (
        dt.datetime.combine(day, start, tzinfo=_KST),
        dt.datetime.combine(day, end, tzinfo=_KST),
    )


def shard_last_journal_wall(
    journal_root: pathlib.Path,
    *,
    vendor: str,
    venue: str,
    session: str,
    streams: Sequence[str],
    day: dt.date,
    shard_index: int,
) -> float | None:
    """Newest mtime over the shard's own tagged journal files for `day`, or None."""
    latest: float | None = None
    root = pathlib.Path(journal_root)
    tag = f"s{shard_index}"
    for stream in streams:
        key = l0_partition_key(vendor, venue, session, stream, day)
        if key is None:
            continue
        part = root / l0_partition_relpath(key)
        try:
            files = list(part.glob(l0_journal_file_glob(tag)))
        except OSError:
            continue
        for path in files:
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if latest is None or mtime > latest:
                latest = mtime
    return latest


__all__ = [
    "ShardDecision",
    "ShardObservation",
    "ShardVerdict",
    "ShardWatchdog",
    "shard_last_journal_wall",
    "shard_window",
]
