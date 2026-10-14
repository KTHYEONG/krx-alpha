"""order CLI 서브커맨드: 주문 의도 조립과 매니저 위임."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from zoneinfo import ZoneInfo

import requests

from src.core.config import ExecutionSettings, load_credentials, load_venue_credentials
from src.core.errors import LiveNotArmedError, MissingCredentialsError, VenueCredentialError
from src.execution.contracts import (
    TERMINAL_STATUSES,
    AccountCheckError,
    ExecutionError,
    KisApiError,
    Order,
    OrderIntent,
    OrderStatus,
    OrderType,
    SessionLockError,
    Side,
)
from src.execution.oms import OrderManager
from src.execution.service import build_order_manager

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def _assemble(stage: str) -> tuple[ExecutionSettings, OrderManager] | None:
    """Build the session-locked manager; logs the failure reason and returns None when it cannot."""
    try:
        settings = load_credentials(ExecutionSettings)
    except MissingCredentialsError as exc:
        logger.error("[EXEC] stage=%s status=FAIL reason=missing_config:%s", stage, str(exc))
        return None
    except LiveNotArmedError:
        logger.error("[EXEC] stage=%s status=FAIL reason=live_not_armed", stage)
        return None
    try:
        creds = load_venue_credentials(settings.venue)
    except MissingCredentialsError as exc:
        logger.error("[EXEC] stage=%s status=FAIL reason=missing_config:%s", stage, str(exc))
        return None
    except VenueCredentialError as exc:
        logger.error("[EXEC] stage=%s status=FAIL reason=venue_credentials:%s", stage, str(exc))
        return None
    try:
        manager = build_order_manager(
            settings=settings,
            creds=creds,
            session=requests.Session(),
            now=lambda: dt.datetime.now(_KST),
        )
    except SessionLockError as exc:
        logger.error("[EXEC] stage=%s status=FAIL reason=session_locked:%s", stage, str(exc))
        return None
    except (AccountCheckError, KisApiError) as exc:
        logger.error("[EXEC] stage=%s status=FAIL reason=account_check:%s", stage, str(exc))
        return None
    except ExecutionError:
        logger.exception("[EXEC] stage=%s status=FAIL reason=session_restore", stage)
        return None
    return settings, manager


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register 'order', 'order-cancel', and 'order-status'."""
    parser = subparsers.add_parser("order")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--side", required=True, choices=["buy", "sell"])
    parser.add_argument("--qty", required=True, type=int)
    parser.add_argument("--type", default="limit", choices=["limit", "market"])
    parser.add_argument("--price", type=int, default=None)
    parser.add_argument("--wait-s", type=float, default=0.0)
    parser.add_argument("--cancel-on-timeout", action="store_true", default=False)
    parser.set_defaults(handler=run)
    cancel = subparsers.add_parser("order-cancel")
    cancel.add_argument("--client-id", required=True)
    cancel.set_defaults(handler=run_cancel)
    resolve = subparsers.add_parser("order-resolve")
    resolve.add_argument("--client-id", required=True)
    resolve.add_argument("--reason", required=True)
    resolve.add_argument("--confirm-not-at-broker", action="store_true", default=False)
    resolve.set_defaults(handler=run_resolve)
    status = subparsers.add_parser("order-status")
    status.set_defaults(handler=run_status)


def _summarize_order(stage: str, settings: ExecutionSettings, order: Order) -> None:
    logger.info(
        "[EXEC] stage=%s mode=%s client_id=%s symbol=%s side=%s qty=%d status=%s filled=%d code=%s",
        stage,
        settings.mode.value,
        order.client_id,
        order.intent.symbol,
        order.intent.side.value,
        order.intent.qty,
        order.status.value,
        order.filled_qty,
        order.reject_code or "",
    )


def _submit_exit_code(order: Order) -> int:
    if order.status is OrderStatus.REJECTED:
        return 3
    if order.status is OrderStatus.UNKNOWN:
        return 5
    return 0


# Past assembly, only KisApiError is handled: a BrokerIntegrityError or IllegalTransitionError
# must surface as a traceback, never as code 4, which tells the operator nothing reached the broker.
def run(args: argparse.Namespace) -> int:
    """Submit one order and optionally wait for its fill."""
    wait_s = float(getattr(args, "wait_s", 0.0) or 0.0)
    cancel_on_timeout = bool(getattr(args, "cancel_on_timeout", False))
    if cancel_on_timeout and wait_s <= 0:
        logger.error("[EXEC] stage=order status=FAIL reason=invalid_args:cancel_requires_wait")
        return 4
    assembled = _assemble("order")
    if assembled is None:
        return 4
    settings, manager = assembled
    try:
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
            logger.error(
                "[EXEC] stage=order mode=%s status=FAIL reason=submit:%s", settings.mode.value, str(exc)
            )
            return 4
        if order.status is OrderStatus.REJECTED:
            _summarize_order("order", settings, order)
            return 3
        if order.status is OrderStatus.UNKNOWN or wait_s <= 0:
            try:
                manager.reconcile()
            except KisApiError as exc:
                logger.warning("[EXEC] stage=order_reconcile status=WARN reason=%s", str(exc))
            _summarize_order("order", settings, order)
            return _submit_exit_code(order)
        order = manager.await_terminal(
            order.client_id, timeout_s=wait_s, interval_s=settings.fill_poll_interval_s
        )
        if order.status is not OrderStatus.FILLED and cancel_on_timeout:
            try:
                manager.cancel(order.client_id)
            except KisApiError as exc:
                logger.warning("[EXEC] stage=order_cancel status=WARN reason=%s", str(exc))
            order = manager.await_terminal(
                order.client_id,
                timeout_s=settings.cancel_confirm_wait_s,
                interval_s=settings.fill_poll_interval_s,
            )
        _summarize_order("order", settings, order)
        return 0 if order.status is OrderStatus.FILLED else 6
    finally:
        manager.close()


def run_cancel(args: argparse.Namespace) -> int:
    """Cancel one same-day order by client id and confirm the result."""
    assembled = _assemble("order_cancel")
    if assembled is None:
        return 4
    settings, manager = assembled
    try:
        client_id = str(args.client_id)
        order = manager.orders.get(client_id)
        if order is None:
            logger.error(
                "[EXEC] stage=order_cancel mode=%s client_id=%s status=FAIL reason=unknown_order",
                settings.mode.value,
                client_id,
            )
            return 4
        if order.status not in TERMINAL_STATUSES:
            if order.broker_order_no is None:
                _summarize_order("order_cancel", settings, order)
                return 5
            try:
                manager.cancel(client_id)
            except KisApiError as exc:
                logger.warning("[EXEC] stage=order_cancel status=WARN reason=%s", str(exc))
            order = manager.await_terminal(
                client_id,
                timeout_s=settings.cancel_confirm_wait_s,
                interval_s=settings.fill_poll_interval_s,
            )
        _summarize_order("order_cancel", settings, order)
        return 0 if order.status is OrderStatus.CANCELLED else 6
    finally:
        manager.close()


def run_status(args: argparse.Namespace) -> int:
    """Reconcile and report same-day open orders and session guards."""
    assembled = _assemble("order_status")
    if assembled is None:
        return 4
    settings, manager = assembled
    try:
        try:
            manager.reconcile()
        except KisApiError as exc:
            # Fail closed: an all-clear from unreconciled local state would pass the rehearsal gate.
            logger.error(
                "[EXEC] stage=order_status mode=%s status=FAIL reason=reconcile:%s",
                settings.mode.value,
                str(exc),
            )
            return 4
        non_terminal = [o for o in manager.orders.values() if o.status not in TERMINAL_STATUSES]
        for o in non_terminal:
            logger.info(
                "[EXEC] stage=order_open client_id=%s symbol=%s side=%s qty=%d filled=%d status=%s odno=%s",
                o.client_id,
                o.intent.symbol,
                o.intent.side.value,
                o.intent.qty,
                o.filled_qty,
                o.status.value,
                o.broker_order_no or "",
            )
        unknown = sum(1 for o in non_terminal if o.status is OrderStatus.UNKNOWN)
        blocked = manager.submit_block_reason is not None
        logger.info(
            "[EXEC] stage=order_status mode=%s open=%d unknown=%d blocked=%s realized_pnl_krw=%d",
            settings.mode.value,
            len(non_terminal),
            unknown,
            blocked,
            manager.ledger.realized_pnl_krw,
        )
        if blocked:
            return 7
        if unknown:
            return 5
        return 0
    finally:
        manager.close()


def run_resolve(args: argparse.Namespace) -> int:
    """Retire an UNKNOWN order the operator verified is absent at the broker."""
    if not bool(getattr(args, "confirm_not_at_broker", False)):
        logger.error("[EXEC] stage=order_resolve status=FAIL reason=invalid_args:confirmation_required")
        return 4
    assembled = _assemble("order_resolve")
    if assembled is None:
        return 4
    settings, manager = assembled
    try:
        try:
            order = manager.resolve_unknown(str(args.client_id), reason=str(args.reason))
        except KisApiError as exc:
            logger.error("[EXEC] stage=order_resolve status=FAIL reason=reconcile:%s", str(exc))
            return 4
        except ExecutionError as exc:
            logger.error("[EXEC] stage=order_resolve status=FAIL reason=%s", str(exc))
            return 4
        _summarize_order("order_resolve", settings, order)
        return 0
    finally:
        manager.close()
