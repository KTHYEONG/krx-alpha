"""bars-refresh 유스케이스 서비스 (CLI/오케스트레이션 공용 진입점)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from dataclasses import dataclass
from typing import Any

from src.marketdata.krx_bars import (
    append_daily_bars,
    backfill_bars,
    derive_market_map,
    latest_trading_day,
    write_market_map,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BarsRefreshResult:
    trading_day: dt.date
    appended_rows: int
    backfilled_days: int


def refresh_bars(
    *,
    store_path: pathlib.Path,
    market_map_path: pathlib.Path,
    ref_date: dt.date,
    window_days: int,
    auth_key: str,
    session: Any | None = None,
) -> BarsRefreshResult:
    """bars store 누적 + market-map 기록을 수행한다 (KRX 실패 시 산출물 보존)."""
    store = pathlib.Path(store_path)
    backfilled_days = 0
    if not store.exists():
        backfill = backfill_bars(
            store, auth_key=auth_key, end_date=ref_date - dt.timedelta(days=1), window_days=window_days, session=session
        )
        backfilled_days = backfill["trading_days"]
        logger.info(
            "[DATA] stage=bars_backfill trading_days=%d appended=%d status=OK",
            backfill["trading_days"],
            backfill["appended_rows"],
        )
    day, bars = latest_trading_day(ref_date, auth_key=auth_key, session=session)
    appended = append_daily_bars(store, bars)
    write_market_map(pathlib.Path(market_map_path), derive_market_map(bars))
    logger.info("[DATA] stage=bars_refresh date=%s appended=%d status=OK", day.isoformat(), appended)
    return BarsRefreshResult(trading_day=day, appended_rows=appended, backfilled_days=backfilled_days)
