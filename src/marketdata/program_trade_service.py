"""Toss program-trade history backfill orchestration (rate-limited, per-symbol isolated)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from src.brokers.kis.rate import RateLimiter
from src.core.symbols import is_krx_short_code
from src.marketdata.toss_calendar import TossCalendarError, issue_access_token
from src.marketdata.toss_program_trades import (
    TossProgramTradesError,
    append_program_trades,
    backfill_program_trades_history,
    symbols_needing_backfill,
)

logger = logging.getLogger(__name__)


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
    """Fetch a fixed symbol set under the Toss rate limit and persist successes."""
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
    """Backfill only universe symbols lacking the requested historical window."""
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
