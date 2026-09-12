"""bars-refresh 유스케이스 서비스 (CLI/오케스트레이션 공용 진입점)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
from dataclasses import dataclass
from typing import Any

import polars as pl

from src.core.errors import KrxAlphaError
from src.execution.contracts import KisApiError
from src.execution.kis_client import KisRestClient
from src.marketdata.krx_bars import (
    append_daily_bars,
    backfill_bars,
    derive_market_map,
    latest_trading_day,
    write_market_map,
)
from src.marketdata.schema import BAR_SCHEMA

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BarsRefreshResult:
    trading_day: dt.date
    appended_rows: int
    backfilled_days: int


class KisFallbackError(KrxAlphaError):
    """KIS 일봉 폴백 fail-closed 신호 (market_map 부재/전량 실패)."""


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


def refresh_bars_via_kis_fallback(
    *,
    store_path: pathlib.Path,
    market_map_path: pathlib.Path,
    target_date: dt.date,
    kis_client: KisRestClient,
) -> BarsRefreshResult:
    """직전 성공 market_map.json 유니버스를 KIS 일봉으로 적재한다 (KRX 장애시 1회성 폴백)."""
    try:
        market_map = json.loads(pathlib.Path(market_map_path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise KisFallbackError(f"no market_map for kis fallback: {exc}") from exc
    if not market_map:
        raise KisFallbackError("empty market_map for kis fallback")
    rows: list[dict[str, object]] = []
    for symbol, market in market_map.items():
        try:
            row = kis_client.get_daily_bar(symbol, target_date)
        except KisApiError as exc:
            logger.warning("[DATA] stage=kis_fallback status=SKIP symbol=%s reason=%s", symbol, str(exc))
            continue
        if row is None:
            continue
        close = float(row["stck_clpr"])
        vrss = float(row.get("prdy_vrss", "0"))
        prev_close = close - vrss
        change_pct = vrss / prev_close * 100.0 if prev_close != 0 else 0.0
        rows.append(
            {
                "date": target_date,
                "symbol": symbol,
                "close": close,
                "volume": int(row["acml_vol"]),
                "trade_value_100m": float(row["acml_tr_pbmn"]) / 1e8,
                "daily_change_pct": change_pct,
                "market": market,
            }
        )
    if not rows:
        raise KisFallbackError(f"kis fallback produced 0 rows for {target_date}")
    bars = pl.DataFrame(rows, schema=BAR_SCHEMA)
    appended = append_daily_bars(pathlib.Path(store_path), bars)
    logger.info("[DATA] stage=kis_fallback date=%s appended=%d status=OK", target_date.isoformat(), appended)
    return BarsRefreshResult(trading_day=target_date, appended_rows=appended, backfilled_days=0)
