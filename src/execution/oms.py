"""단일 OMS: 사전 리스크 게이트 + 전송 + 브로커 누적체결 대사."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable
from zoneinfo import ZoneInfo

from src.core.config import ExecutionMode
from src.execution.contracts import (
    TERMINAL_STATUSES,
    BrokerIntegrityError,
    ExecutionError,
    Order,
    OrderGateway,
    OrderIntent,
    OrderStatus,
    OrderType,
    OutcomeKind,
    QuoteSource,
    RiskRejectedError,
    Side,
    derive_status,
    transition,
)
from src.execution.journal import OrderJournal
from src.execution.ledger import CostModel, Ledger
from src.execution.risk import RiskContext, RiskLimits, check_intent, reference_price

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_OPEN_STATUSES = frozenset(
    {OrderStatus.PENDING_SUBMIT, OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.UNKNOWN}
)


class OrderManager:
    """주문 상태머신 소유자 (접수·취소·대사)."""

    _UNKNOWN_MATCH_SKEW = dt.timedelta(seconds=5)

    def __init__(
        self,
        *,
        gateway: OrderGateway,
        quotes: QuoteSource,
        ledger: Ledger,
        limits: RiskLimits,
        costs: CostModel,
        journal: OrderJournal,
        now: Callable[[], dt.datetime],
        kill_switch: Callable[[], bool],
    ) -> None:
        self.gateway = gateway
        self.ledger = ledger
        self.orders: dict[str, Order] = {}
        self._quotes = quotes
        self._limits = limits
        self._costs = costs
        self._journal = journal
        self._now = now
        self._kill_switch = kill_switch
        self._seq = 0
        self._submit_times: list[dt.datetime] = []

    @property
    def _mode(self) -> ExecutionMode:
        return self.gateway.mode

    def submit(self, intent: OrderIntent) -> Order:
        """의도를 검증·전송하고 주문을 등록한다."""
        quote = self._quotes.get_quote(intent.symbol)
        self._seq += 1
        client_id = f"{self._mode.value}-{self._now():%Y%m%d}-{self._seq:06d}"
        try:
            ref = reference_price(intent, quote)
            orderable = (
                self.gateway.orderable_cash(intent.symbol, ref, intent.order_type)
                if intent.side is Side.BUY
                else 0
            )
            now = self._now()
            window_start = now - dt.timedelta(seconds=60)
            open_orders = [o for o in self.orders.values() if o.status in _OPEN_STATUSES]
            same_key_unknown = any(
                o.status is OrderStatus.UNKNOWN
                and o.intent.symbol == intent.symbol
                and o.intent.side is intent.side
                for o in open_orders
            )
            same_symbol_open = [o for o in open_orders if o.intent.symbol == intent.symbol]
            ctx = RiskContext(
                kill_switch=self._kill_switch(),
                unknown_pending=same_key_unknown,
                orders_last_minute=len([t for t in self._submit_times if t > window_start]),
                realized_pnl_krw=self.ledger.realized_pnl_krw,
                position_qty=self.ledger.qty(intent.symbol),
                open_buy_qty=sum(
                    o.remaining_qty for o in same_symbol_open if o.intent.side is Side.BUY
                ),
                open_sell_qty=sum(
                    o.remaining_qty for o in same_symbol_open if o.intent.side is Side.SELL
                ),
                orderable_cash_krw=orderable,
                est_commission_krw=self._costs.commission(intent.qty * ref),
            )
            check_intent(intent, quote, limits=self._limits, ctx=ctx)
        except RiskRejectedError as exc:
            order = Order(
                client_id=client_id,
                intent=intent,
                status=OrderStatus.REJECTED,
                created_at=self._now(),
                reject_code=exc.code.value,
                reject_message=exc.detail,
            )
            self.orders[client_id] = order
            self._journal.append(
                "risk_reject",
                client_id=client_id,
                intent=intent,
                code=exc.code.value,
                detail=exc.detail,
                quote=quote,
            )
            logger.info("[EXEC] stage=risk_reject client_id=%s code=%s", client_id, exc.code.value)
            return order
        order = Order(
            client_id=client_id, intent=intent, status=OrderStatus.PENDING_SUBMIT, created_at=self._now()
        )
        self.orders[client_id] = order
        self._journal.append(
            "intent", client_id=client_id, intent=intent, quote=quote, reference_price=ref
        )
        self._submit_times.append(self._now())
        outcome = self.gateway.submit(order, quote)
        if outcome.kind is OutcomeKind.ACCEPTED:
            order.status = transition(order.status, OrderStatus.ACCEPTED)
            order.broker_order_no = outcome.broker_order_no
            order.broker_org_no = outcome.broker_org_no
            order.broker_order_time = outcome.order_time
        elif outcome.kind is OutcomeKind.REJECTED:
            order.status = transition(order.status, OrderStatus.REJECTED)
            order.reject_code = outcome.code
            order.reject_message = outcome.message
        else:
            order.status = transition(order.status, OrderStatus.UNKNOWN)
        self._journal.append(
            "submit_result", client_id=client_id, outcome=outcome, status=order.status.value
        )
        logger.info(
            "[EXEC] stage=submit mode=%s client_id=%s symbol=%s side=%s qty=%d status=%s code=%s",
            self._mode.value,
            client_id,
            intent.symbol,
            intent.side.value,
            intent.qty,
            order.status.value,
            outcome.code,
        )
        return order

    def cancel(self, client_id: str) -> Order:
        """취소를 요청한다 (상태 변경은 reconcile 만 수행)."""
        order = self.orders.get(client_id)
        if order is None:
            raise ExecutionError(f"unknown client_id: {client_id}")
        if order.status in TERMINAL_STATUSES:
            return order
        if order.broker_order_no is None:
            raise ExecutionError("cannot cancel without broker order number; reconcile first")
        outcome = self.gateway.cancel(order)
        self._journal.append("cancel_result", client_id=client_id, outcome=outcome)
        logger.info("[EXEC] stage=cancel client_id=%s code=%s", client_id, outcome.code)
        return order

    def reconcile(self) -> list[Order]:
        """브로커 누적체결을 대사로 반영한다."""
        statuses = self.gateway.fetch_statuses()
        by_no = {s.broker_order_no: s for s in statuses}
        child_parents = {
            s.original_order_no
            for s in statuses
            if s.original_order_no and s.original_order_no != s.broker_order_no
        }
        changed: dict[str, Order] = {}
        known = {o.broker_order_no for o in self.orders.values() if o.broker_order_no}
        for order in self.orders.values():
            if order.status is not OrderStatus.UNKNOWN:
                continue
            threshold = (order.created_at - self._UNKNOWN_MATCH_SKEW).astimezone(_KST).strftime(
                "%H%M%S"
            )
            expected_price = (
                order.intent.limit_price if order.intent.order_type is OrderType.LIMIT else 0
            )
            candidates = [
                s
                for s in statuses
                if s.broker_order_no not in known
                and (not s.original_order_no or s.original_order_no == s.broker_order_no)
                and s.symbol == order.intent.symbol
                and s.side is order.intent.side
                and s.ordered_qty == order.intent.qty
                and s.price == expected_price
                and s.order_time >= threshold
            ]
            if len(candidates) == 1:
                match = candidates[0]
                order.broker_order_no = match.broker_order_no
                order.broker_org_no = match.org_no
                order.broker_order_time = match.order_time
                known.add(match.broker_order_no)
                self._journal.append(
                    "unknown_adopted",
                    client_id=order.client_id,
                    broker_order_no=match.broker_order_no,
                )
                changed[order.client_id] = order
                logger.info("[EXEC] stage=unknown_adopted client_id=%s", order.client_id)
            elif len(candidates) > 1:
                self._journal.append(
                    "unknown_ambiguous",
                    client_id=order.client_id,
                    candidates=[s.broker_order_no for s in candidates],
                )
                logger.critical(
                    "[EXEC] stage=unknown_ambiguous client_id=%s candidates=%s",
                    order.client_id,
                    ",".join(s.broker_order_no for s in candidates),
                )
        for order in self.orders.values():
            if order.status in TERMINAL_STATUSES:
                continue
            if order.broker_order_no is None or order.broker_order_no not in by_no:
                continue
            status = by_no[order.broker_order_no]
            if status.filled_qty < order.filled_qty or status.filled_qty > order.intent.qty:
                self._journal.append(
                    "integrity_violation",
                    client_id=order.client_id,
                    broker_filled_qty=status.filled_qty,
                    local_filled_qty=order.filled_qty,
                )
                logger.critical(
                    "[EXEC] stage=integrity_violation client_id=%s broker=%d local=%d",
                    order.client_id,
                    status.filled_qty,
                    order.filled_qty,
                )
                raise BrokerIntegrityError(
                    f"cumulative fill moved backwards or exceeded: {status.filled_qty}"
                )
            new_status = derive_status(
                status, has_cancel_child=order.broker_order_no in child_parents
            )
            delta_qty = status.filled_qty - order.filled_qty
            delta_amount = status.filled_amount_krw - order.filled_amount_krw
            if delta_qty > 0:
                self.ledger.apply_fill(
                    order.intent.symbol, order.intent.side, delta_qty, delta_amount
                )
                self._journal.append(
                    "fill",
                    client_id=order.client_id,
                    delta_qty=delta_qty,
                    delta_amount_krw=delta_amount,
                    cumulative_qty=status.filled_qty,
                )
                logger.info(
                    "[EXEC] stage=fill client_id=%s delta_qty=%d", order.client_id, delta_qty
                )
            order.filled_qty = status.filled_qty
            order.filled_amount_krw = status.filled_amount_krw
            old_status = order.status
            order.status = transition(old_status, new_status)
            if order.status is not old_status or delta_qty > 0:
                self._journal.append(
                    "status",
                    client_id=order.client_id,
                    status=order.status.value,
                    filled_qty=order.filled_qty,
                )
                changed[order.client_id] = order
        return list(changed.values())
