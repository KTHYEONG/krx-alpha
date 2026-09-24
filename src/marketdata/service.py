"""bars-refresh 유스케이스 서비스 (CLI/오케스트레이션 공용 진입점)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import polars as pl

from src.core.errors import KrxAlphaError
from src.core.symbols import is_krx_short_code
from src.execution.contracts import KisApiError
from src.execution.kis_client import KisRestClient, RateLimiter
from src.marketdata.krx_bars import (
    append_daily_bars,
    backfill_bars,
    derive_market_map,
    latest_trading_day,
    write_market_map,
)
from src.marketdata.schema import BAR_SCHEMA
from src.marketdata.toss_calendar import TossCalendarError, issue_access_token
from src.marketdata.toss_program_trades import (
    TossProgramTradesError,
    append_program_trades,
    backfill_program_trades_history,
    symbols_needing_backfill,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BarsRefreshResult:
    trading_day: dt.date
    appended_rows: int
    backfilled_days: int


class KisFallbackError(KrxAlphaError):
    """KIS 일봉 폴백 fail-closed 신호 (market_map 부재/전량 실패)."""


@dataclass(frozen=True)
class ProgramTradesBackfillResult:
    symbols_ok: int
    symbols_failed: int
    appended_rows: int


def backfill_program_trades(
    *,
    store_path: pathlib.Path,
    symbols: Sequence[str],
    min_date: dt.date,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    session: Any | None = None,
) -> ProgramTradesBackfillResult:
    """Backfill and persist Toss program-trade history for a fixed symbol set.

    One symbol's fetch failure does not abort the run: over-skipping a
    problem symbol can be corrected by re-running the tool, but losing every
    already-fetched symbol's history to one bad response would waste the
    Raises:
        ValueError: If ``symbols`` is empty or contains a malformed KRX short code.
        TossProgramTradesError: If the initial token issuance fails.
    """
    unique = list(dict.fromkeys(symbols))
    if not unique or any(not is_krx_short_code(code) for code in unique):
        raise ValueError(f"symbols must be non-empty KRX short codes: {list(symbols)!r}")
    try:
        token = issue_access_token(app_key=app_key, app_secret=app_secret, session=session)
    except TossCalendarError as exc:
        raise TossProgramTradesError(f"toss program-trades token issuance failed: {exc}") from exc
    limiter = RateLimiter(rate_per_s)
    symbols_ok = 0
    symbols_failed = 0
    appended_rows = 0
    for code in unique:
        try:
            rows = backfill_program_trades_history(
                code, access_token=token, min_date=min_date, session=session, throttle=limiter.acquire
            )
        except TossProgramTradesError as exc:
            logger.warning("[DATA] stage=toss_program_backfill status=SKIP symbol=%s reason=%s", code, str(exc))
            symbols_failed += 1
            continue
        appended_rows += append_program_trades(store_path, rows)
        symbols_ok += 1
    logger.info(
        "[DATA] stage=toss_program_backfill status=OK symbols_ok=%d symbols_failed=%d appended_rows=%d",
        symbols_ok,
        symbols_failed,
        appended_rows,
    )
    return ProgramTradesBackfillResult(symbols_ok=symbols_ok, symbols_failed=symbols_failed, appended_rows=appended_rows)

def backfill_universe_program_trades(
    *,
    store_path: pathlib.Path,
    symbols: Sequence[str],
    lookback_days: int,
    reference_date: dt.date,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    session: Any | None = None,
) -> ProgramTradesBackfillResult:
    """Backfill only universe symbols whose stored history misses the lookback window.

    Runs once per session as part of pre-market orchestration so a symbol
    newly entering the universe carries enough program-trade history for
    rolling-window features from its first session, without re-fetching
    symbols a prior day's run already covered.

    Raises:
        TossProgramTradesError: If the coverage check cannot read an existing
            but corrupted store, or if token issuance for the backfill fails.
    """
    if not symbols:
        return ProgramTradesBackfillResult(symbols_ok=0, symbols_failed=0, appended_rows=0)
    clean_symbols = tuple(code for code in symbols if is_krx_short_code(code))
    skipped = set(symbols) - set(clean_symbols)
    if skipped:
        logger.warning(
            "[DATA] stage=toss_program_backfill status=SKIP_INVALID_CODE symbols=%s", sorted(skipped)
        )
    if not clean_symbols:
        return ProgramTradesBackfillResult(symbols_ok=0, symbols_failed=0, appended_rows=0)
    min_date = reference_date - dt.timedelta(days=lookback_days)
    targets = symbols_needing_backfill(store_path, clean_symbols, min_date)
    if not targets:
        return ProgramTradesBackfillResult(symbols_ok=0, symbols_failed=0, appended_rows=0)
    return backfill_program_trades(
        store_path=store_path,
        symbols=targets,
        min_date=min_date,
        app_key=app_key,
        app_secret=app_secret,
        rate_per_s=rate_per_s,
        session=session,
    )


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
    """직전 성공 market_map.json 유니버스를 KIS 일봉으로 적재한다 (KRX 장애시 1회성 폴백).

    Bars whose session date differs from ``target_date`` are never stored.
    """
    try:
        market_map = json.loads(pathlib.Path(market_map_path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise KisFallbackError(f"no market_map for kis fallback: {exc}") from exc
    if not market_map:
        raise KisFallbackError("empty market_map for kis fallback")
    want = target_date.strftime("%Y%m%d")
    rows: list[dict[str, object]] = []
    for symbol, market in market_map.items():
        try:
            row = kis_client.get_daily_bar(symbol, target_date)
        except KisApiError as exc:
            logger.warning("[DATA] stage=kis_fallback status=SKIP symbol=%s reason=%s", symbol, str(exc))
            continue
        if row is None:
            continue
        got = str(row.get("stck_bsop_date", ""))
        if got != want:
            logger.warning(
                "[DATA] stage=kis_fallback status=SKIP symbol=%s reason=date_mismatch got=%s want=%s",
                symbol,
                got,
                want,
            )
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
                "open": float(row["stck_oprc"]),
                "high": float(row["stck_hgpr"]),
                "low": float(row["stck_lwpr"]),
                "base_price": prev_close,
            }
        )
    if not rows:
        raise KisFallbackError(f"kis fallback produced 0 rows for {target_date}")
    bars = pl.DataFrame(rows, schema=BAR_SCHEMA)
    appended = append_daily_bars(pathlib.Path(store_path), bars)
    logger.info("[DATA] stage=kis_fallback date=%s appended=%d status=OK", target_date.isoformat(), appended)
    return BarsRefreshResult(trading_day=target_date, appended_rows=appended, backfilled_days=0)
