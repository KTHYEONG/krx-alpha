"""bars-refresh CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib

from src.core.config import KrxCredentials, load_credentials
from src.core.errors import MissingCredentialsError
from src.marketdata.krx_bars import KrxBarsError
from src.marketdata.service import BarsRefreshResult as BarsRefreshResult
from src.marketdata.service import refresh_bars

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'bars-refresh' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("bars-refresh")
    parser.add_argument("--store-path", required=True)
    parser.add_argument("--market-map-path", required=True)
    parser.add_argument("--ref-date", required=True)
    parser.add_argument("--window-days", type=int, default=90)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """인자 파싱 + 자격증명 로드 + service 위임 + 종료코드 매핑만 수행한다."""
    ref = dt.date.fromisoformat(str(args.ref_date))
    store = pathlib.Path(str(args.store_path))
    market_map_path = pathlib.Path(str(args.market_map_path))
    window_days = int(args.window_days)
    try:
        creds = load_credentials(KrxCredentials)
    except MissingCredentialsError as exc:
        logger.error("[DATA] stage=bars_refresh status=FAIL reason=missing_credentials:%s", str(exc))
        return 4
    try:
        result = refresh_bars(
            store_path=store,
            market_map_path=market_map_path,
            ref_date=ref,
            window_days=window_days,
            auth_key=creds.krx_openapi_key,
        )
    except KrxBarsError as exc:
        logger.error("[DATA] stage=bars_refresh status=FAIL reason=%s", str(exc))
        return 4
    logger.info("[DATA] stage=bars_refresh date=%s appended=%d status=OK", result.trading_day.isoformat(), result.appended_rows)
    return 0
