"""TradingDayGate invariant guards."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src.marketdata.toss_calendar import TradingDay
from src.orchestration.trading_day_gate import (
    UNKNOWN_RETRY_INTERVAL,
    TradingDayGate,
    TradingDayStatus,
)

_KST = ZoneInfo("Asia/Seoul")


def _business(day: dt.date) -> TradingDay:
    return TradingDay(
        date=day,
        is_business_day=True,
        previous_business_day=day - dt.timedelta(days=1),
        next_business_day=day + dt.timedelta(days=1),
    )


def _holiday(day: dt.date) -> TradingDay:
    return TradingDay(
        date=day,
        is_business_day=False,
        previous_business_day=day - dt.timedelta(days=3),
        next_business_day=day + dt.timedelta(days=1),
    )


def test_trading_day_gate_resolves_business_day_once() -> None:
    calls: list[dt.date] = []

    def _resolver(day: dt.date) -> TradingDay | None:
        calls.append(day)
        return _business(day)

    gate = TradingDayGate(resolver=_resolver)
    today = dt.date(2026, 9, 14)
    now = dt.datetime(2026, 9, 14, 8, 30, tzinfo=_KST)
    views = [gate.view(today, now) for _ in range(3)]
    assert all(v.status is TradingDayStatus.BUSINESS for v in views)
    assert views[0].trading_day is not None
    assert calls == [today]


def test_trading_day_gate_resolves_holiday_once() -> None:
    calls: list[dt.date] = []

    def _resolver(day: dt.date) -> TradingDay | None:
        calls.append(day)
        return _holiday(day)

    gate = TradingDayGate(resolver=_resolver)
    today = dt.date(2026, 9, 24)
    now = dt.datetime(2026, 9, 24, 19, 31, tzinfo=_KST)
    first = gate.view(today, now)
    second = gate.view(today, now)
    assert first.status is TradingDayStatus.HOLIDAY
    assert first.trading_day is not None
    assert first.trading_day.is_business_day is False
    assert second == first
    assert calls == [today]


def test_trading_day_gate_retries_unknown_only_after_interval() -> None:
    calls: list[dt.date] = []
    gate = TradingDayGate(resolver=lambda d: calls.append(d) or None)
    today = dt.date(2026, 9, 14)
    t0 = dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST)
    assert gate.view(today, t0).status is TradingDayStatus.UNKNOWN
    assert gate.view(today, t0 + dt.timedelta(minutes=10)).status is TradingDayStatus.UNKNOWN
    assert gate.view(today, t0 + dt.timedelta(minutes=31)).status is TradingDayStatus.UNKNOWN
    assert calls == [today, today]
    assert dt.timedelta(minutes=30) == UNKNOWN_RETRY_INTERVAL


def test_trading_day_gate_upgrades_unknown_to_resolved() -> None:
    today = dt.date(2026, 9, 24)
    outcomes: list[TradingDay | None] = [None, _holiday(today)]
    calls: list[dt.date] = []

    def _resolver(day: dt.date) -> TradingDay | None:
        calls.append(day)
        return outcomes[min(len(calls) - 1, 1)]

    gate = TradingDayGate(resolver=_resolver)
    t0 = dt.datetime(2026, 9, 24, 8, 30, tzinfo=_KST)
    assert gate.view(today, t0).status is TradingDayStatus.UNKNOWN
    upgraded = gate.view(today, t0 + dt.timedelta(minutes=31))
    assert upgraded.status is TradingDayStatus.HOLIDAY
    assert upgraded.trading_day is not None
    later = gate.view(today, t0 + dt.timedelta(minutes=60))
    assert later.status is TradingDayStatus.HOLIDAY
    assert calls == [today, today]


def test_trading_day_gate_resets_state_on_date_rollover() -> None:
    seen: list[dt.date] = []

    def _resolver(day: dt.date) -> TradingDay | None:
        seen.append(day)
        return _holiday(day)

    gate = TradingDayGate(resolver=_resolver)
    first = dt.date(2026, 9, 24)
    second = dt.date(2026, 9, 25)
    gate.view(first, dt.datetime(2026, 9, 24, 20, 5, tzinfo=_KST))
    view = gate.view(second, dt.datetime(2026, 9, 25, 8, 30, tzinfo=_KST))
    assert view.date == second
    assert seen == [first, second]


def test_trading_day_gate_treats_mismatched_vendor_date_as_unknown() -> None:
    def _resolver(day: dt.date) -> TradingDay | None:
        return _business(day + dt.timedelta(days=1))

    gate = TradingDayGate(resolver=_resolver)
    view = gate.view(dt.date(2026, 9, 14), dt.datetime(2026, 9, 14, 8, 30, tzinfo=_KST))
    assert view.status is TradingDayStatus.UNKNOWN
    assert view.trading_day is None


def test_trading_day_gate_rejects_naive_now() -> None:
    gate = TradingDayGate(resolver=lambda d: None)
    with pytest.raises(ValueError, match="timezone-aware"):
        gate.view(dt.date(2026, 9, 14), dt.datetime(2026, 9, 14, 8, 30))
