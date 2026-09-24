"""Per-date trading-day status shared by every daemon state."""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from src.marketdata.toss_calendar import TradingDay

UNKNOWN_RETRY_INTERVAL: dt.timedelta = dt.timedelta(minutes=30)


class TradingDayStatus(StrEnum):
    """Resolved trading-day status for a KST date."""

    BUSINESS = "business"
    HOLIDAY = "holiday"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class TradingDayView:
    """Immutable per-date status snapshot returned by the gate."""

    date: dt.date
    status: TradingDayStatus
    trading_day: TradingDay | None


class TradingDayGate:
    """Per-date trading-day status shared by every daemon state.

    The collector must know whether a weekday is a KRX business day in every
    session state, including right after a restart, not only at the pre-market
    orchestration step. A resolved status (business or holiday) is final for
    its date; an unresolved status is retried at most once per
    ``retry_interval`` so a vendor outage neither hammers the calendar API
    nor leaves the daemon permanently blind.

    Args:
        resolver: Returns the ``TradingDay`` for a date, or ``None`` when no
            source (vendor or cache) can decide it. Must not raise.
        retry_interval: Minimum spacing between resolver calls while the
            status for the current date is unknown.
    """

    def __init__(
        self,
        *,
        resolver: Callable[[dt.date], TradingDay | None],
        retry_interval: dt.timedelta = UNKNOWN_RETRY_INTERVAL,
    ) -> None:
        self._resolver = resolver
        self._retry_interval = retry_interval
        self._date: dt.date | None = None
        self._view: TradingDayView | None = None
        self._last_attempt: dt.datetime | None = None

    def view(self, today: dt.date, now: dt.datetime) -> TradingDayView:
        """Return the status for ``today``, resolving it when needed.

        Args:
            today: KST calendar date being evaluated.
            now: Timezone-aware current time; paces unknown-status retries.

        Returns:
            ``BUSINESS`` or ``HOLIDAY`` from ``TradingDay.is_business_day``
            with the resolved ``TradingDay``, or ``UNKNOWN`` with ``None``.

        Raises:
            ValueError: If ``now`` is naive.
        """
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if self._date != today:
            return self._resolve(today, now)
        assert self._view is not None
        assert self._last_attempt is not None
        if self._view.status is not TradingDayStatus.UNKNOWN:
            return self._view
        if now - self._last_attempt >= self._retry_interval:
            return self._resolve(today, now)
        return self._view

    def _resolve(self, today: dt.date, now: dt.datetime) -> TradingDayView:
        day = self._resolver(today)
        self._date = today
        self._last_attempt = now
        if day is None or day.date != today:
            self._view = TradingDayView(date=today, status=TradingDayStatus.UNKNOWN, trading_day=None)
        elif day.is_business_day:
            self._view = TradingDayView(date=today, status=TradingDayStatus.BUSINESS, trading_day=day)
        else:
            self._view = TradingDayView(date=today, status=TradingDayStatus.HOLIDAY, trading_day=day)
        return self._view
