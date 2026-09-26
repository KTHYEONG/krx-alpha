"""Nightly program-trades forward sync plus candidate history backfill (one-shot child)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
from collections.abc import Sequence
from typing import cast

from src.core.config import (
    CollectorSettings,
    TossCredentials,
    TossProgramTradesSettings,
    load_credentials,
)
from src.core.errors import MissingCredentialsError
from src.core.observability import configure_logging
from src.marketdata.program_trade_service import sync_program_trades_forward
from src.marketdata.toss_program_trades import TossProgramTradesError
from src.universe.ipc import CandidateFileError, read_candidates

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the nightly sync argument parser."""
    parser = argparse.ArgumentParser(prog="toss-program-trades-sync")
    parser.add_argument("--session-date", required=True, help="business day just closed (KST, YYYY-MM-DD)")
    parser.add_argument("--complete-through", required=True, help="previous business day every tracked symbol must reach")
    return parser


def _active_symbols(market_map_path: pathlib.Path) -> frozenset[str] | None:
    # market_map 은 최신 일봉 거래일의 상장 종목이다: 상장폐지 종목을 누락 비율에서 빼는 기준으로 쓴다.
    try:
        raw = json.loads(pathlib.Path(market_map_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("[DATA] stage=program_trades_sync status=DEGRADED reason=market_map_unreadable error=%s", type(exc).__name__)
        return None
    if not isinstance(raw, dict) or not raw:
        logger.warning("[DATA] stage=program_trades_sync status=DEGRADED reason=market_map_empty")
        return None
    return frozenset(str(code) for code in raw)


def main(argv: Sequence[str] | None = None) -> int:
    """Nightly forward sync and candidate history backfill for program trades.

    Args (CLI):
        --session-date YYYY-MM-DD: business day just closed (KST).
        --complete-through YYYY-MM-DD: previous business day; every tracked
            symbol is expected to have rows through this date after the run.

    Exit codes:
        0: sync finished and staleness is within tolerance.
        2: sync finished but staleness exceeded tolerance (CRITICAL logged).
        1: fatal error (credentials, token, unreadable store).
    """
    args = build_parser().parse_args(argv)
    configure_logging("toss-program-trades-sync")
    try:
        session_date = dt.date.fromisoformat(str(args.session_date))
        complete_through = dt.date.fromisoformat(str(args.complete_through))
        settings = TossProgramTradesSettings()
        paths = CollectorSettings().paths
        creds = load_credentials(TossCredentials)
        data = read_candidates(paths.candidates)
        rows = cast("list[dict[str, object]] | None", data.get("candidates") if data else None)
        candidate_symbols = tuple(str(row["symbol"]) for row in rows) if rows else ()
        result = sync_program_trades_forward(
            store_root=paths.program_trades_dir,
            session_date=session_date,
            complete_through=complete_through,
            candidate_symbols=candidate_symbols,
            lookback_days=settings.auto_backfill_lookback_days,
            app_key=creds.toss_app_key,
            app_secret=creds.toss_app_secret,
            rate_per_s=settings.rate_per_s,
            active_symbols=_active_symbols(paths.market_map),
        )
    except MissingCredentialsError as exc:
        logger.error("[DATA] stage=program_trades_sync status=FAIL reason=missing_credentials:%s", str(exc))
        return 1
    except (TossProgramTradesError, CandidateFileError, ValueError) as exc:
        logger.error("[DATA] stage=program_trades_sync status=FAIL reason=%s", str(exc))
        return 1
    stale = len(result.stale_symbols)
    tracked = len(result.tracked)
    ratio = (stale / tracked) if tracked else 0.0
    sample = ",".join(result.stale_symbols[:10])
    if ratio > settings.max_stale_ratio:
        logger.critical(
            "[DATA] stage=program_trades_sync status=STALE stale=%d tracked=%d ratio=%.4f complete_through=%s stale_sample=%s",
            stale,
            tracked,
            ratio,
            complete_through.isoformat(),
            sample,
        )
        return 2
    logger.info(
        "[DATA] stage=program_trades_sync status=OK fetched_symbols=%d appended_rows=%d stale=%d tracked=%d ratio=%.4f stale_sample=%s",
        result.symbols_ok,
        result.appended_rows,
        stale,
        tracked,
        ratio,
        sample,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
