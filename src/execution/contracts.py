"""주문집행 도메인 계약: 상태·이벤트·게이트웨이 경계."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from src.core.config import ExecutionMode
from src.core.errors import KrxAlphaError


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(StrEnum):
    PENDING_SUBMIT = "pending_submit"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class OutcomeKind(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class RejectCode(StrEnum):
    KILL_SWITCH = "kill_switch"
    UNKNOWN_PENDING = "unknown_pending"
    SYMBOL_NOT_ALLOWED = "symbol_not_allowed"
    QTY_INVALID = "qty_invalid"
    PRICE_INVALID = "price_invalid"
    TRADING_HALTED = "trading_halted"
    TICK_TABLE_MISMATCH = "tick_table_mismatch"
    PRICE_OFF_TICK = "price_off_tick"
    PRICE_OUT_OF_BAND = "price_out_of_band"
    ORDER_RATE_LIMIT = "order_rate_limit"
    DAILY_LOSS_LIMIT = "daily_loss_limit"
    NOTIONAL_LIMIT = "notional_limit"
    POSITION_LIMIT = "position_limit"
    INSUFFICIENT_CASH = "insufficient_cash"
    INSUFFICIENT_POSITION = "insufficient_position"


class ExecutionError(KrxAlphaError):
    """주문집행 계층 공용 예외 루트."""


class RiskRejectedError(ExecutionError):
    """사전 리스크 게이트 거부 신호."""

    def __init__(self, code: RejectCode, detail: str) -> None:
        super().__init__(f"{code.value}: {detail}")
        self.code = code
        self.detail = detail


class IllegalTransitionError(ExecutionError):
    """허용되지 않은 주문 상태 전이 신호."""


class BrokerIntegrityError(ExecutionError):
    """브로커 누적체결 불변식 위반 신호."""


class KisApiError(ExecutionError):
    """KIS API 오류 신호 (msg_cd 보존)."""

    def __init__(self, msg_cd: str, message: str) -> None:
        super().__init__(f"{msg_cd}: {message}")
        self.msg_cd = msg_cd
        self.message = message


class AccountCheckError(ExecutionError):
    """계좌 자기검증 실패 fail-closed 신호."""


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: Side
    qty: int
    order_type: OrderType
    limit_price: int | None = None
    tag: str = ""


@dataclass(frozen=True)
class Quote:
    symbol: str
    last: int
    upper_limit: int
    lower_limit: int
    tick: int
    halted: bool
    asks: tuple[tuple[int, int], ...]
    bids: tuple[tuple[int, int], ...]


@dataclass
class Order:
    client_id: str
    intent: OrderIntent
    status: OrderStatus
    created_at: dt.datetime
    broker_order_no: str | None = None
    broker_org_no: str | None = None
    broker_order_time: str | None = None
    filled_qty: int = 0
    filled_amount_krw: int = 0
    reject_code: str | None = None
    reject_message: str | None = None

    @property
    def remaining_qty(self) -> int:
        return self.intent.qty - self.filled_qty


@dataclass(frozen=True)
class BrokerOrderStatus:
    broker_order_no: str
    original_order_no: str
    org_no: str
    symbol: str
    side: Side
    ordered_qty: int
    price: int
    filled_qty: int
    filled_amount_krw: int
    remaining_qty: int
    rejected_qty: int
    cancelled: bool
    order_time: str


@dataclass(frozen=True)
class BrokerOutcome:
    kind: OutcomeKind
    broker_order_no: str | None = None
    broker_org_no: str | None = None
    order_time: str | None = None
    code: str = ""
    message: str = ""


@dataclass(frozen=True)
class Holding:
    symbol: str
    qty: int
    cost_krw: int


class QuoteSource(Protocol):
    def get_quote(self, symbol: str) -> Quote: ...


class OrderGateway(Protocol):
    mode: ExecutionMode

    def submit(self, order: Order, quote: Quote) -> BrokerOutcome: ...
    def cancel(self, order: Order) -> BrokerOutcome: ...
    def fetch_statuses(self) -> list[BrokerOrderStatus]: ...
    def orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int: ...


TERMINAL_STATUSES: frozenset[OrderStatus] = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
)

ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.PENDING_SUBMIT: frozenset({OrderStatus.ACCEPTED, OrderStatus.REJECTED, OrderStatus.UNKNOWN}),
    OrderStatus.UNKNOWN: frozenset(
        {
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
        }
    ),
    OrderStatus.ACCEPTED: frozenset(
        {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset({OrderStatus.FILLED, OrderStatus.CANCELLED}),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
}


def transition(current: OrderStatus, new: OrderStatus) -> OrderStatus:
    """허용 전이를 적용하고 금지 전이는 fail-closed 로 거부한다."""
    if new == current:
        return current
    if new in ALLOWED_TRANSITIONS[current]:
        return new
    raise IllegalTransitionError(f"{current.value}->{new.value}")


def derive_status(status: BrokerOrderStatus, *, has_cancel_child: bool) -> OrderStatus:
    """브로커 누적체결 행을 주문 상태로 결정적 매핑한다."""
    if status.filled_qty < 0 or status.filled_qty > status.ordered_qty:
        raise BrokerIntegrityError(f"invalid cumulative fill: {status.filled_qty}/{status.ordered_qty}")
    if status.filled_qty == status.ordered_qty:
        return OrderStatus.FILLED
    cancel_evidence = status.cancelled or has_cancel_child
    if cancel_evidence or status.rejected_qty > 0 or status.remaining_qty == 0:
        if status.filled_qty == 0 and status.rejected_qty > 0 and not cancel_evidence:
            return OrderStatus.REJECTED
        return OrderStatus.CANCELLED
    if status.filled_qty > 0:
        return OrderStatus.PARTIALLY_FILLED
    return OrderStatus.ACCEPTED
