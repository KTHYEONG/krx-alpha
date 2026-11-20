"""Dated statutory sell-side tax schedule keyed by settlement (transfer) date."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from itertools import pairwise
from typing import Final

from src.core.errors import KrxAlphaError

SETTLEMENT_LAG_TRADING_DAYS: Final[int] = 2


class TaxMarket(StrEnum):
    KOSPI = "KOSPI"
    KOSDAQ = "KOSDAQ"
    ETF = "ETF"


@dataclass(frozen=True)
class SellTaxRate:
    effective_from: dt.date  # first settlement (transfer) date the rate applies to
    market: TaxMarket
    transaction_bps: Decimal  # securities transaction tax
    rural_bps: Decimal  # special rural development tax (KOSPI only)

    @property
    def total_bps(self) -> Decimal:
        return self.transaction_bps + self.rural_bps


SELL_TAX_SCHEDULE_START: Final[dt.date] = dt.date(2023, 1, 1)

# Sources: 증권거래세법 시행령 §5 (KOSPI/KOSDAQ 2023-2025 steps); ekn.kr 2025-04-30 and KB증권 2024-12-23 notice
# (2025 rate applies to trades from 2024-12-27, i.e. settlement on/after 2025-01-02); 머니투데이 2025-12-01 and
# 2025-12-31 (2026 restoration: KOSPI 0.05% + rural 0.15%, KOSDAQ 0.20%); taxnet.co.kr ETF tax guide (ETF sale exempt).
SELL_TAX_SCHEDULE: Final[tuple[SellTaxRate, ...]] = (
    SellTaxRate(dt.date(2023, 1, 1), TaxMarket.KOSPI, Decimal("5"), Decimal("15")),
    SellTaxRate(dt.date(2023, 1, 1), TaxMarket.KOSDAQ, Decimal("20"), Decimal("0")),
    SellTaxRate(dt.date(2023, 1, 1), TaxMarket.ETF, Decimal("0"), Decimal("0")),
    SellTaxRate(dt.date(2024, 1, 1), TaxMarket.KOSPI, Decimal("3"), Decimal("15")),
    SellTaxRate(dt.date(2024, 1, 1), TaxMarket.KOSDAQ, Decimal("18"), Decimal("0")),
    SellTaxRate(dt.date(2024, 1, 1), TaxMarket.ETF, Decimal("0"), Decimal("0")),
    SellTaxRate(dt.date(2025, 1, 1), TaxMarket.KOSPI, Decimal("0"), Decimal("15")),
    SellTaxRate(dt.date(2025, 1, 1), TaxMarket.KOSDAQ, Decimal("15"), Decimal("0")),
    SellTaxRate(dt.date(2025, 1, 1), TaxMarket.ETF, Decimal("0"), Decimal("0")),
    SellTaxRate(dt.date(2026, 1, 1), TaxMarket.KOSPI, Decimal("5"), Decimal("15")),
    SellTaxRate(dt.date(2026, 1, 1), TaxMarket.KOSDAQ, Decimal("20"), Decimal("0")),
    SellTaxRate(dt.date(2026, 1, 1), TaxMarket.ETF, Decimal("0"), Decimal("0")),
)


def validate_sell_tax_schedule(schedule: tuple[SellTaxRate, ...], start: dt.date) -> None:
    """Raise KrxAlphaError unless each market has ordered, non-negative rows and covers ``start``."""
    for market in TaxMarket:
        rows = [row for row in schedule if row.market == market]
        dates = [row.effective_from for row in rows]
        if not dates or min(dates) > start:
            raise KrxAlphaError(f"sell tax schedule has no {market} row at or before {start.isoformat()}")
        if any(later <= earlier for earlier, later in pairwise(dates)):
            raise KrxAlphaError(f"sell tax schedule {market} effective_from is not strictly increasing")
    for row in schedule:
        if row.transaction_bps < 0 or row.rural_bps < 0:
            raise KrxAlphaError(f"sell tax schedule has negative bps for {row.market} {row.effective_from}")


validate_sell_tax_schedule(SELL_TAX_SCHEDULE, SELL_TAX_SCHEDULE_START)


def settlement_day(trade_day: dt.date, trading_days: frozenset[dt.date]) -> dt.date:
    """Return the T+SETTLEMENT_LAG_TRADING_DAYS trading day; raises KrxAlphaError if unknowable."""
    if trade_day not in trading_days:
        raise KrxAlphaError(f"trade day {trade_day.isoformat()} is not a trading day in the loaded calendar")
    later = sorted(d for d in trading_days if d > trade_day)
    if len(later) < SETTLEMENT_LAG_TRADING_DAYS:
        raise KrxAlphaError(f"trading calendar ends before settlement of {trade_day.isoformat()}")
    return later[SETTLEMENT_LAG_TRADING_DAYS - 1]


def sell_tax_bps(trade_day: dt.date, market: TaxMarket, trading_days: frozenset[dt.date]) -> Decimal:
    """Return the total sell tax in bps for a trade, keyed by its settlement date."""
    settled = settlement_day(trade_day, trading_days)
    if settled < SELL_TAX_SCHEDULE_START:
        raise KrxAlphaError(f"settlement {settled.isoformat()} precedes sell tax schedule start")
    applicable = (row for row in SELL_TAX_SCHEDULE if row.market == market and row.effective_from <= settled)
    return max(applicable, key=lambda row: row.effective_from).total_bps
