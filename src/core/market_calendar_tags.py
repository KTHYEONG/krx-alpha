"""Derivatives expiry, quad-witching and KOSPI200 rebalance tags derived from a caller-supplied trading-day set."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Final

from src.core.errors import KrxAlphaError

QUAD_WITCHING_MONTHS: Final[frozenset[int]] = frozenset({3, 6, 9, 12})
KOSPI200_REBALANCE_MONTHS: Final[frozenset[int]] = frozenset({6, 12})

_THURSDAY: Final[int] = 3
_SECOND: Final[int] = 2


@dataclass(frozen=True)
class CalendarTags:
    is_derivatives_expiry: bool
    is_quad_witching: bool
    is_kospi200_rebalance_effective: bool


def derivatives_expiry_day(year: int, month: int, trading_days: frozenset[dt.date]) -> dt.date:
    """Return the month's derivatives expiry; raises KrxAlphaError when the calendar cannot place it."""
    month_days = [d for d in trading_days if d.year == year and d.month == month]
    if not month_days:
        raise KrxAlphaError(f"no trading days loaded for {year:04d}-{month:02d}")

    first = dt.date(year, month, 1)
    first_thursday = first + dt.timedelta(days=(_THURSDAY - first.weekday()) % 7)
    expiry = first_thursday + dt.timedelta(weeks=_SECOND - 1)

    candidate = expiry
    while candidate.month == month:
        if candidate in trading_days:
            return candidate
        candidate -= dt.timedelta(days=1)
    raise KrxAlphaError(f"no trading day on or before expiry in {year:04d}-{month:02d}")


def calendar_tags(day: dt.date, trading_days: frozenset[dt.date]) -> CalendarTags:
    """Tag a trading day; non-trading days are all false."""
    if day not in trading_days:
        return CalendarTags(False, False, False)

    expiry = derivatives_expiry_day(day.year, day.month, trading_days)
    is_expiry = day == expiry
    return CalendarTags(
        is_derivatives_expiry=is_expiry,
        is_quad_witching=is_expiry and day.month in QUAD_WITCHING_MONTHS,
        is_kospi200_rebalance_effective=_follows_rebalance_expiry(day, trading_days),
    )


def _follows_rebalance_expiry(day: dt.date, trading_days: frozenset[dt.date]) -> bool:
    # The effective day follows a 2nd-Thursday expiry, so it always falls inside a rebalance month.
    if day.month not in KOSPI200_REBALANCE_MONTHS:
        return False
    previous = max((d for d in trading_days if d < day), default=None)
    if previous is None:
        raise KrxAlphaError(f"no trading day before {day.isoformat()} loaded; cannot place rebalance")
    if previous.month not in KOSPI200_REBALANCE_MONTHS:
        return False
    return derivatives_expiry_day(previous.year, previous.month, trading_days) == previous
