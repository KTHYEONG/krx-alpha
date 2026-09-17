"""toss-program-trades-backfill CLI subcommand (operator-triggered historical depth backfill)."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib

from src.core.config import TossCredentials, TossProgramTradesSettings, load_credentials
from src.core.errors import MissingCredentialsError
from src.marketdata.service import ProgramTradesBackfillResult as ProgramTradesBackfillResult
from src.marketdata.service import backfill_program_trades
from src.marketdata.toss_program_trades import TossProgramTradesError

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the 'toss-program-trades-backfill' subcommand."""
    parser = subparsers.add_parser("toss-program-trades-backfill")
    parser.add_argument("--store-path", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--min-date", required=True)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Parse args, load Toss credentials, delegate to the service, and map exit codes."""
    store = pathlib.Path(str(args.store_path))
    symbols = tuple(part.strip() for part in str(args.symbols).split(",") if part.strip())
    min_date = dt.date.fromisoformat(str(args.min_date))
    try:
        creds = load_credentials(TossCredentials)
    except MissingCredentialsError as exc:
        logger.error("[DATA] stage=toss_program_backfill status=FAIL reason=missing_credentials:%s", str(exc))
        return 4
    try:
        result = backfill_program_trades(
            store_path=store,
            symbols=symbols,
            min_date=min_date,
            app_key=creds.toss_app_key,
            app_secret=creds.toss_app_secret,
            rate_per_s=TossProgramTradesSettings().rate_per_s,
        )
    except (ValueError, TossProgramTradesError) as exc:
        logger.error("[DATA] stage=toss_program_backfill status=FAIL reason=%s", str(exc))
        return 4
    logger.info(
        "[DATA] stage=toss_program_backfill status=OK symbols_ok=%d symbols_failed=%d appended_rows=%d",
        result.symbols_ok,
        result.symbols_failed,
        result.appended_rows,
    )
    return 0
