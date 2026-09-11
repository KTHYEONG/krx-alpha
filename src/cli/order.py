"""order CLI 서브커맨드: 주문 의도 조립과 매니저 위임."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from zoneinfo import ZoneInfo

import requests

from src.core.config import ExecutionSettings, KisCredentials, load_credentials
from src.core.errors import LiveNotArmedError, MissingCredentialsError
from src.execution.contracts import AccountCheckError, KisApiError, OrderIntent, OrderStatus, OrderType, Side
from src.execution.service import build_order_manager

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'order' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("order")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=["buy", "sell"])
    parser.add_argument("--qty", required=True, type=int)
    parser.add_argument("--type", default="limit", choices=["limit", "market"])
    parser.add_argument("--price", type=int, default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """자격증명 로드 후 주문 제출과 대사를 수행한다."""
    try:
        settings = load_credentials(ExecutionSettings)
        creds = load_credentials(KisCredentials)
    except MissingCredentialsError as exc:
        logger.error("[EXEC] stage=order status=FAIL reason=missing_config:%s", str(exc))
        return 4
    except LiveNotArmedError:
        logger.error("[EXEC] stage=order status=FAIL reason=live_not_armed")
        return 4
    try:
        manager = build_order_manager(
            settings=settings,
            creds=creds,
            session=requests.Session(),
            now=lambda: dt.datetime.now(_KST),
        )
    except (AccountCheckError, KisApiError) as exc:
        logger.error("[EXEC] stage=order status=FAIL reason=account_check:%s", str(exc))
        return 4
    intent = OrderIntent(
        symbol=str(args.symbol),
        side=Side(str(args.side)),
        qty=int(args.qty),
        order_type=OrderType(str(args.type)),
        limit_price=None if args.price is None else int(args.price),
    )
    try:
        order = manager.submit(intent)
    except KisApiError as exc:
        logger.error("[EXEC] stage=order status=FAIL reason=submit:%s", str(exc))
        return 4
    if order.status is not OrderStatus.REJECTED:
        try:
            manager.reconcile()
        except KisApiError as exc:
            logger.warning("[EXEC] stage=order_reconcile status=WARN reason=%s", str(exc))
    logger.info(
        "[EXEC] stage=order mode=%s client_id=%s symbol=%s side=%s qty=%d status=%s filled=%d code=%s",
        settings.mode.value,
        order.client_id,
        order.intent.symbol,
        order.intent.side.value,
        order.intent.qty,
        order.status.value,
        order.filled_qty,
        order.reject_code or "",
    )
    if order.status is OrderStatus.REJECTED:
        return 3
    if order.status is OrderStatus.UNKNOWN:
        return 5
    return 0
