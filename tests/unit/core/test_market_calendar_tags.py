from __future__ import annotations

import datetime as dt

import pytest

from src.core.errors import KrxAlphaError
from src.core.market_calendar_tags import calendar_tags, derivatives_expiry_day


def _weekdays(start: dt.date, end: dt.date, exclude: frozenset[dt.date] = frozenset()) -> frozenset[dt.date]:
    days = []
    day = start
    while day <= end:
        if day.weekday() < 5 and day not in exclude:
            days.append(day)
        day += dt.timedelta(days=1)
    return frozenset(days)


def test_expiry_second_thursday() -> None:
    # Given: 2026-11 with every weekday trading
    trading = _weekdays(dt.date(2026, 10, 1), dt.date(2026, 12, 31))

    # When / Then: 2nd Thursday of November 2026
    assert derivatives_expiry_day(2026, 11, trading) == dt.date(2026, 11, 12)


def test_expiry_holiday_rolls_back() -> None:
    # Given: the 2nd Thursday is not a trading day
    trading = _weekdays(dt.date(2026, 10, 1), dt.date(2026, 12, 31), exclude=frozenset({dt.date(2026, 11, 12)}))

    # When / Then: the nearest earlier trading day in the same month is used
    assert derivatives_expiry_day(2026, 11, trading) == dt.date(2026, 11, 11)


def test_expiry_no_trading_days_in_month_raises() -> None:
    # Given: the calendar holds no day of November 2026
    trading = _weekdays(dt.date(2026, 10, 1), dt.date(2026, 10, 31))

    # When / Then: the expiry cannot be placed and the call fails closed
    with pytest.raises(KrxAlphaError):
        derivatives_expiry_day(2026, 11, trading)


def test_quad_witching_only_quarter_months() -> None:
    # Given: March 2026 expiry (Mar 12) and April 2026 expiry (Apr 9)
    trading = _weekdays(dt.date(2026, 3, 1), dt.date(2026, 4, 30))

    # When / Then: quad witching only in quarter-end months
    assert calendar_tags(dt.date(2026, 3, 12), trading).is_quad_witching is True
    assert calendar_tags(dt.date(2026, 4, 9), trading).is_quad_witching is False
    assert calendar_tags(dt.date(2026, 4, 9), trading).is_derivatives_expiry is True


def test_kospi200_rebalance_day_after_june_dec() -> None:
    # Given: June 2026 expiry is Thursday Jun 11; September 2026 expiry is Thursday Sep 10
    trading = _weekdays(dt.date(2026, 6, 1), dt.date(2026, 9, 30))

    # When / Then: the first trading day after the June expiry is effective; the expiry itself is not
    assert calendar_tags(dt.date(2026, 6, 12), trading).is_kospi200_rebalance_effective is True
    assert calendar_tags(dt.date(2026, 6, 11), trading).is_kospi200_rebalance_effective is False
    assert calendar_tags(dt.date(2026, 9, 11), trading).is_kospi200_rebalance_effective is False


def test_non_trading_day_all_false() -> None:
    # Given: a Saturday outside the trading set
    trading = _weekdays(dt.date(2026, 11, 1), dt.date(2026, 11, 30))

    # When
    tags = calendar_tags(dt.date(2026, 11, 14), trading)

    # Then
    assert tags.is_derivatives_expiry is False
    assert tags.is_quad_witching is False
    assert tags.is_kospi200_rebalance_effective is False


def test_expiry_no_trading_day_on_or_before_expiry_raises() -> None:
    # Given: November 2026 trades only from the 13th onward, so no candidate on or before the 12th exists
    trading = _weekdays(dt.date(2026, 11, 13), dt.date(2026, 11, 30))

    # When / Then: no trading day on or before the expiry exists in the month
    with pytest.raises(KrxAlphaError):
        derivatives_expiry_day(2026, 11, trading)


def test_rebalance_tag_without_prior_trading_day_raises() -> None:
    # Given: the loaded calendar starts on the expiry day itself
    trading = frozenset({dt.date(2026, 6, 11)})

    # When / Then: the preceding trading day needed for the rebalance tag is unknown
    with pytest.raises(KrxAlphaError):
        calendar_tags(dt.date(2026, 6, 11), trading)


def test_first_loaded_day_outside_rebalance_month_does_not_raise() -> None:
    # Given: the loaded calendar starts on a February day (no rebalance possible in February)
    trading = _weekdays(dt.date(2026, 2, 2), dt.date(2026, 2, 27))

    # When
    tags = calendar_tags(dt.date(2026, 2, 2), trading)

    # Then: no rebalance tag and no fail-closed error
    assert tags.is_kospi200_rebalance_effective is False


def test_first_june_trading_day_after_may_is_not_rebalance() -> None:
    # Given: the previous trading day falls in May
    trading = _weekdays(dt.date(2026, 5, 25), dt.date(2026, 6, 30))

    # When / Then
    assert calendar_tags(dt.date(2026, 6, 1), trading).is_kospi200_rebalance_effective is False
