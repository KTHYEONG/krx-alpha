from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from src.core.errors import KrxAlphaError
from src.core.sell_tax import (
    SELL_TAX_SCHEDULE,
    SELL_TAX_SCHEDULE_START,
    SellTaxRate,
    TaxMarket,
    sell_tax_bps,
    settlement_day,
    validate_sell_tax_schedule,
)


def _weekdays(start: dt.date, end: dt.date, exclude: frozenset[dt.date] = frozenset()) -> frozenset[dt.date]:
    days = []
    day = start
    while day <= end:
        if day.weekday() < 5 and day not in exclude:
            days.append(day)
        day += dt.timedelta(days=1)
    return frozenset(days)


def test_rates_by_year_and_market() -> None:
    # Given: mid-year trade days in 2024 (KOSPI), 2025 (KOSDAQ), 2026 (KOSPI, ETF)
    trading = _weekdays(dt.date(2023, 12, 1), dt.date(2026, 12, 31))

    # When / Then: rates resolved by settlement year
    assert sell_tax_bps(dt.date(2024, 6, 12), TaxMarket.KOSPI, trading) == Decimal("18")
    assert sell_tax_bps(dt.date(2025, 6, 12), TaxMarket.KOSDAQ, trading) == Decimal("15")
    assert sell_tax_bps(dt.date(2026, 6, 12), TaxMarket.KOSPI, trading) == Decimal("20")
    assert sell_tax_bps(dt.date(2026, 6, 12), TaxMarket.ETF, trading) == Decimal("0")


def test_settlement_t_plus_2_skips_closed_days() -> None:
    # Given: Friday trade with Monday closed
    trading = _weekdays(dt.date(2026, 11, 9), dt.date(2026, 11, 30), exclude=frozenset({dt.date(2026, 11, 16)}))

    # When / Then: settlement is the second trading day after the trade
    assert settlement_day(dt.date(2026, 11, 13), trading) == dt.date(2026, 11, 18)


def test_year_boundary_uses_settlement_date() -> None:
    # Given: 2024-12-31 closed; KOSDAQ rates change on 2025-01-01
    trading = frozenset(
        {
            dt.date(2024, 12, 26),
            dt.date(2024, 12, 27),
            dt.date(2024, 12, 30),
            dt.date(2025, 1, 2),
            dt.date(2025, 1, 3),
        }
    )

    # When / Then: the rate follows the settlement date, not the trade date
    assert sell_tax_bps(dt.date(2024, 12, 26), TaxMarket.KOSDAQ, trading) == Decimal("18")
    assert sell_tax_bps(dt.date(2024, 12, 27), TaxMarket.KOSDAQ, trading) == Decimal("15")


def test_kospi_total_is_tx_plus_rural() -> None:
    # Given: every schedule row

    # When / Then: KOSPI total equals the sum; other markets carry no rural tax
    for row in SELL_TAX_SCHEDULE:
        assert row.total_bps == row.transaction_bps + row.rural_bps
        if row.market != TaxMarket.KOSPI:
            assert row.rural_bps == Decimal("0")


def test_before_schedule_start_raises() -> None:
    # Given: a trade whose settlement (2022-12-29) precedes the schedule start
    trading = _weekdays(dt.date(2022, 12, 20), dt.date(2023, 1, 10))

    # When / Then: no extrapolation before the schedule start
    assert dt.date(2022, 12, 29) < SELL_TAX_SCHEDULE_START
    with pytest.raises(KrxAlphaError):
        sell_tax_bps(dt.date(2022, 12, 27), TaxMarket.KOSPI, trading)


def test_trading_days_insufficient_raises() -> None:
    # Given: a trade day missing from the set
    trading = _weekdays(dt.date(2026, 11, 2), dt.date(2026, 11, 30))

    # When / Then: unknown trade day fails closed
    with pytest.raises(KrxAlphaError):
        settlement_day(dt.date(2026, 11, 14), trading)

    # Given: a set that ends the day after the trade
    short = frozenset({dt.date(2026, 11, 13), dt.date(2026, 11, 16)})

    # When / Then: settlement two trading days out is not provable
    with pytest.raises(KrxAlphaError):
        settlement_day(dt.date(2026, 11, 13), short)


def test_decimal_not_float() -> None:
    # Given: a lookup
    trading = _weekdays(dt.date(2026, 11, 2), dt.date(2026, 11, 30))

    # When
    result = sell_tax_bps(dt.date(2026, 11, 13), TaxMarket.KOSPI, trading)

    # Then
    assert isinstance(result, Decimal)


def test_validator_rejects_bad_schedule() -> None:
    # Given: a valid two-row KOSPI and KOSDAQ and ETF base
    base = (
        SellTaxRate(dt.date(2023, 1, 1), TaxMarket.KOSPI, Decimal("5"), Decimal("15")),
        SellTaxRate(dt.date(2023, 1, 1), TaxMarket.KOSDAQ, Decimal("20"), Decimal("0")),
        SellTaxRate(dt.date(2023, 1, 1), TaxMarket.ETF, Decimal("0"), Decimal("0")),
    )
    validate_sell_tax_schedule(base, SELL_TAX_SCHEDULE_START)

    # When / Then: a decreasing date in one market
    decreasing = (
        *base,
        SellTaxRate(dt.date(2025, 1, 1), TaxMarket.KOSPI, Decimal("0"), Decimal("15")),
        SellTaxRate(dt.date(2024, 1, 1), TaxMarket.KOSPI, Decimal("3"), Decimal("15")),
    )
    with pytest.raises(KrxAlphaError):
        validate_sell_tax_schedule(decreasing, SELL_TAX_SCHEDULE_START)

    # When / Then: a market missing at the schedule start
    missing = base[:2]
    with pytest.raises(KrxAlphaError):
        validate_sell_tax_schedule(missing, SELL_TAX_SCHEDULE_START)

    # When / Then: a negative bps value
    negative = (
        SellTaxRate(dt.date(2023, 1, 1), TaxMarket.KOSPI, Decimal("-5"), Decimal("15")),
        *base[1:],
    )
    with pytest.raises(KrxAlphaError):
        validate_sell_tax_schedule(negative, SELL_TAX_SCHEDULE_START)
