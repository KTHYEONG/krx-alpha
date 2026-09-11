"""주문 전송기: paper 섀도(기록+모의체결)와 live 실전 POST 분기."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from src.core.config import ExecutionMode, KisCredentials
from src.execution.contracts import (
    BrokerOrderStatus,
    BrokerOutcome,
    ExecutionError,
    Order,
    OrderIntent,
    OrderType,
    OutcomeKind,
    Quote,
    Side,
)
from src.execution.journal import OrderJournal
from src.execution.kis_client import TR_CANCEL, build_cancel_body, build_order_body, order_tr_id
from src.execution.ledger import Ledger
from src.execution.risk import reference_price

logger = logging.getLogger(__name__)


class LiveOrderClient(Protocol):
    """LiveGateway 가 사용하는 클라이언트 경계 (실전 KisRestClient)."""

    def post_order(self, tr_id: str, body: dict[str, str]) -> BrokerOutcome: ...
    def get_daily_orders(self, day: dt.date) -> list[BrokerOrderStatus]: ...
    def get_orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int: ...


def simulate_paper_fills(intent: OrderIntent, quote: Quote) -> tuple[int, int]:
    """제출 시점 10단계 호가 소진 모의체결 (paper_l10_sweep_v1)."""
    levels = quote.asks if intent.side is Side.BUY else quote.bids
    limit = intent.limit_price if intent.order_type is OrderType.LIMIT else None
    filled = 0
    amount = 0
    remaining = intent.qty
    for price, size in levels:
        if limit is not None:
            if intent.side is Side.BUY and price > limit:
                break
            if intent.side is Side.SELL and price < limit:
                break
        take = min(remaining, size)
        filled += take
        amount += take * price
        remaining -= take
        if remaining == 0:
            break
    return filled, amount


@dataclass
class _PaperEntry:
    intent: OrderIntent
    filled_qty: int
    filled_amount_krw: int
    reserve_price: int
    cancelled: bool = False


class PaperGateway:
    """실계좌 섀도 전송기: 주문 POST 없이 실전 TR/바디 기록과 모의체결만 수행한다."""

    mode = ExecutionMode.PAPER

    def __init__(self, *, creds: KisCredentials, journal: OrderJournal, ledger: Ledger) -> None:
        self._creds = creds
        self._journal = journal
        self._ledger = ledger
        self._seq = 0
        self._book: dict[str, _PaperEntry] = {}

    def submit(self, order: Order, quote: Quote) -> BrokerOutcome:
        self._seq += 1
        odno = f"P{self._seq:08d}"
        filled, amount = simulate_paper_fills(order.intent, quote)
        self._journal.append(
            "paper_would_send",
            client_id=order.client_id,
            broker_order_no=odno,
            tr_id=order_tr_id(order.intent.side),
            body=build_order_body(self._creds, order),
            quote=quote,
            simulated_fill_qty=filled,
            simulated_fill_amount_krw=amount,
            fill_model="paper_l10_sweep_v1",
        )
        self._book[odno] = _PaperEntry(
            intent=order.intent,
            filled_qty=filled,
            filled_amount_krw=amount,
            reserve_price=reference_price(order.intent, quote),
        )
        logger.info("[EXEC] stage=paper_would_send client_id=%s odno=%s", order.client_id, odno)
        return BrokerOutcome(
            kind=OutcomeKind.ACCEPTED, broker_order_no=odno, broker_org_no="PAPER", order_time=""
        )

    def cancel(self, order: Order) -> BrokerOutcome:
        entry = self._book.get(order.broker_order_no or "")
        if entry is None:
            raise ExecutionError("unknown paper order")
        if entry.filled_qty == entry.intent.qty:
            return BrokerOutcome(
                kind=OutcomeKind.REJECTED,
                broker_order_no=order.broker_order_no,
                code="ALREADY_FILLED",
                message="nothing to cancel",
            )
        entry.cancelled = True
        self._journal.append(
            "paper_would_cancel",
            client_id=order.client_id,
            tr_id=TR_CANCEL,
            body=build_cancel_body(self._creds, order),
        )
        logger.info("[EXEC] stage=paper_would_cancel client_id=%s", order.client_id)
        return BrokerOutcome(
            kind=OutcomeKind.ACCEPTED, broker_order_no=order.broker_order_no, broker_org_no="PAPER"
        )

    def fetch_statuses(self) -> list[BrokerOrderStatus]:
        statuses: list[BrokerOrderStatus] = []
        for odno, entry in self._book.items():
            remaining = 0 if entry.cancelled else entry.intent.qty - entry.filled_qty
            statuses.append(
                BrokerOrderStatus(
                    broker_order_no=odno,
                    original_order_no="",
                    org_no="PAPER",
                    symbol=entry.intent.symbol,
                    side=entry.intent.side,
                    ordered_qty=entry.intent.qty,
                    price=entry.intent.limit_price or 0,
                    filled_qty=entry.filled_qty,
                    filled_amount_krw=entry.filled_amount_krw,
                    remaining_qty=remaining,
                    rejected_qty=0,
                    cancelled=entry.cancelled,
                    order_time="",
                )
            )
        return statuses

    def orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int:
        reserved = sum(
            (entry.intent.qty - entry.filled_qty) * entry.reserve_price
            for entry in self._book.values()
            if not entry.cancelled and entry.intent.side is Side.BUY
        )
        return self._ledger.cash_krw - reserved


class LiveGateway:
    """실전 전송기: 전송 직전 기록 후 클라이언트에 위임한다."""

    mode = ExecutionMode.LIVE

    def __init__(
        self,
        *,
        client: LiveOrderClient,
        creds: KisCredentials,
        journal: OrderJournal,
        today: Callable[[], dt.date],
    ) -> None:
        self._client = client
        self._creds = creds
        self._journal = journal
        self._today = today

    def submit(self, order: Order, quote: Quote) -> BrokerOutcome:
        tr_id = order_tr_id(order.intent.side)
        body = build_order_body(self._creds, order)
        self._journal.append("live_send", client_id=order.client_id, tr_id=tr_id, body=body)
        logger.info("[EXEC] stage=live_send client_id=%s tr_id=%s", order.client_id, tr_id)
        return self._client.post_order(tr_id, body)

    def cancel(self, order: Order) -> BrokerOutcome:
        body = build_cancel_body(self._creds, order)
        self._journal.append("live_send", client_id=order.client_id, tr_id=TR_CANCEL, body=body)
        logger.info("[EXEC] stage=live_send_cancel client_id=%s", order.client_id)
        return self._client.post_order(TR_CANCEL, body)

    def fetch_statuses(self) -> list[BrokerOrderStatus]:
        return self._client.get_daily_orders(self._today())

    def orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int:
        return self._client.get_orderable_cash(symbol, price, order_type)
