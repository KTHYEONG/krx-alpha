"""사전 리스크 게이트: 주문 전송 전 fail-closed 검증."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.execution.contracts import OrderIntent, OrderType, Quote, RejectCode, RiskRejectedError, Side
from src.execution.ticks import is_tick_aligned, tick_of

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RiskLimits:
    max_order_notional_krw: int
    max_position_notional_krw: int
    max_orders_per_minute: int
    max_daily_loss_krw: int
    symbol_whitelist: frozenset[str] = frozenset()


@dataclass(frozen=True)
class RiskContext:
    kill_switch: bool
    unknown_pending: bool
    orders_last_minute: int
    realized_pnl_krw: int
    position_qty: int
    open_buy_qty: int
    open_sell_qty: int
    orderable_cash_krw: int
    est_commission_krw: int


def reference_price(intent: OrderIntent, quote: Quote) -> int:
    """리스크 산정 기준가를 결정한다."""
    if intent.order_type is OrderType.LIMIT:
        if intent.limit_price is None:
            raise RiskRejectedError(RejectCode.PRICE_INVALID, "limit order requires limit_price")
        return intent.limit_price
    if intent.side is Side.BUY:
        return quote.upper_limit
    return quote.last


def check_intent(intent: OrderIntent, quote: Quote, *, limits: RiskLimits, ctx: RiskContext) -> None:
    """위반 코드를 순서대로 판정하고 통과 시 None 을 반환한다."""
    if ctx.kill_switch:
        raise RiskRejectedError(RejectCode.KILL_SWITCH, "kill switch engaged")
    if ctx.unknown_pending:
        raise RiskRejectedError(RejectCode.UNKNOWN_PENDING, "unknown order pending for symbol side")
    if limits.symbol_whitelist and intent.symbol not in limits.symbol_whitelist:
        raise RiskRejectedError(RejectCode.SYMBOL_NOT_ALLOWED, f"symbol not allowed: {intent.symbol}")
    if intent.qty < 1:
        raise RiskRejectedError(RejectCode.QTY_INVALID, f"qty must be >= 1: {intent.qty}")
    if (intent.order_type is OrderType.LIMIT and intent.limit_price is None) or (
        intent.order_type is OrderType.MARKET and intent.limit_price is not None
    ):
        raise RiskRejectedError(RejectCode.PRICE_INVALID, "limit/market price combination invalid")
    if quote.halted:
        raise RiskRejectedError(RejectCode.TRADING_HALTED, "trading halted")
    if quote.last >= 1 and tick_of(quote.last) != quote.tick:
        raise RiskRejectedError(RejectCode.TICK_TABLE_MISMATCH, "broker tick unit mismatch")
    if intent.order_type is OrderType.LIMIT:
        limit_price = intent.limit_price
        if limit_price is not None:
            if not is_tick_aligned(limit_price):
                raise RiskRejectedError(RejectCode.PRICE_OFF_TICK, f"price off tick: {limit_price}")
            if not quote.lower_limit <= limit_price <= quote.upper_limit:
                raise RiskRejectedError(RejectCode.PRICE_OUT_OF_BAND, f"price out of band: {limit_price}")
    if ctx.orders_last_minute >= limits.max_orders_per_minute:
        raise RiskRejectedError(RejectCode.ORDER_RATE_LIMIT, "order rate limit exceeded")
    ref = reference_price(intent, quote)
    notional = intent.qty * ref
    if intent.side is Side.BUY and ctx.realized_pnl_krw <= -limits.max_daily_loss_krw:
        raise RiskRejectedError(RejectCode.DAILY_LOSS_LIMIT, "daily loss limit reached")
    if notional > limits.max_order_notional_krw:
        raise RiskRejectedError(RejectCode.NOTIONAL_LIMIT, f"notional too large: {notional}")
    if intent.side is Side.BUY and (ctx.position_qty + ctx.open_buy_qty + intent.qty) * ref > limits.max_position_notional_krw:
        raise RiskRejectedError(RejectCode.POSITION_LIMIT, "position limit exceeded")
    if intent.side is Side.BUY and notional + ctx.est_commission_krw > ctx.orderable_cash_krw:
        raise RiskRejectedError(RejectCode.INSUFFICIENT_CASH, "insufficient cash")
    if intent.side is Side.SELL and intent.qty > ctx.position_qty - ctx.open_sell_qty:
        raise RiskRejectedError(RejectCode.INSUFFICIENT_POSITION, "insufficient position")
    logger.debug("[EXEC] stage=risk_check symbol=%s side=%s qty=%d ref=%d", intent.symbol, intent.side.value, intent.qty, ref)
    return None
