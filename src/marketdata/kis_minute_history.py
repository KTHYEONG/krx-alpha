"""KIS historical 1m day walk over direct-seek pages."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass

from src.brokers.kis.data import KisMinuteBar
from src.execution.contracts import KisApiError

_SESSION_LO = 90000
_SESSION_HI = 153000
_OPEN_BAR = 90000
_COMPLETE_EARLY = 90100


class MinuteBackfillBoundaryError(KisApiError):
    """The caller stopped admission at its deadline or request budget."""


@dataclass(frozen=True)
class KisMinuteDay:
    status: str
    reason: str
    bars: tuple[KisMinuteBar, ...]
    calls: int
    discarded_offhours: int = 0


def _minus_one_second(hms: int) -> int:
    moment = dt.datetime.strptime(f"{hms:06d}", "%H%M%S") - dt.timedelta(seconds=1)
    return moment.hour * 10000 + moment.minute * 100 + moment.second


def walk_kis_minute_day(
    symbol: str,
    day: dt.date,
    *,
    fetch: Callable[[int], tuple[KisMinuteBar, ...]],
    max_calls: int,
) -> KisMinuteDay:
    """Page one symbol-day backward from 153000 until 090000 is covered or the vendor returns no bars; return status and bars ascending.

    A vendor that repeats its already-seen earliest page below the first traded minute marks the day floor (``vendor_floor``).
    """
    if max_calls < 1:
        raise ValueError("max_calls must be >= 1")
    cursor = _SESSION_HI
    seen: set[int] = set()
    kept: dict[int, KisMinuteBar] = {}
    calls = 0
    discarded = 0
    while calls < max_calls:
        try:
            page = fetch(cursor)
        except MinuteBackfillBoundaryError:
            break
        calls += 1
        if not page:
            if calls == 1:
                return KisMinuteDay(status="vendor_empty", reason="no_bars", bars=(), calls=calls)
            earliest = min(kept) if kept else None
            if earliest is not None and earliest <= _COMPLETE_EARLY:
                return KisMinuteDay("complete", "", tuple(kept[k] for k in sorted(kept)), calls, discarded)
            return KisMinuteDay(status="incomplete", reason="exhausted", bars=(), calls=calls)
        earliest_raw = min(bar.ts_hms for bar in page)
        if earliest_raw > cursor:
            if kept and all(bar.ts_hms in seen for bar in page):
                ordered = tuple(kept[key] for key in sorted(kept))
                return KisMinuteDay("complete", "vendor_floor", ordered, calls, discarded)
            raise KisApiError("PAGINATION", f"minute page made no progress for {symbol}")
        for bar in page:
            if bar.day != day:
                raise KisApiError("SCHEMA", "minute page contains another date")
            if bar.ts_hms < _SESSION_LO or bar.ts_hms > _SESSION_HI:
                discarded += 1
                continue
            if bar.ts_hms in seen:
                continue
            seen.add(bar.ts_hms)
            kept[bar.ts_hms] = bar
        if _OPEN_BAR in kept:
            ordered = tuple(kept[key] for key in sorted(kept))
            return KisMinuteDay(status="complete", reason="", bars=ordered, calls=calls, discarded_offhours=discarded)
        nxt = _minus_one_second(earliest_raw)
        cursor = nxt
    return KisMinuteDay(status="incomplete", reason="exhausted", bars=(), calls=calls, discarded_offhours=discarded)
