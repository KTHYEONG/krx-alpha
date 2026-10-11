"""Same-day order session restore from the append-only audit journal."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from src.core.config import ExecutionMode
from src.execution.contracts import (
    ExecutionError,
    Holding,
    Order,
    OrderIntent,
    OrderStatus,
    OrderType,
    Side,
)
from src.execution.ledger import CostModel, Ledger, Position

_ORDER_EVENTS = frozenset({"submit_result", "unknown_adopted", "fill", "status"})


@dataclass(frozen=True)
class RecoveredSession:
    orders: dict[str, Order]
    next_seq: int
    submit_times: tuple[dt.datetime, ...]
    ledger: Ledger | None


def _parse_ts(raw: Any) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        raise ExecutionError(f"invalid journal ts: {raw!r}") from None
    if parsed.utcoffset() is None:
        raise ExecutionError("journal timestamp must be timezone-aware")
    return parsed


def _seq_of(client_id: Any) -> int:
    try:
        suffix = str(client_id).rsplit("-", 1)[1]
    except IndexError:
        raise ExecutionError(f"invalid client_id: {client_id!r}") from None
    if not suffix.isascii() or not suffix.isdecimal():
        raise ExecutionError(f"non-numeric client_id suffix: {client_id!r}")
    return int(suffix)


def _intent_of(raw: Any) -> OrderIntent:
    if not isinstance(raw, Mapping):
        raise ExecutionError("invalid intent record")
    try:
        return OrderIntent(
            symbol=str(raw["symbol"]),
            side=Side(str(raw["side"])),
            qty=_integer(raw["qty"]),
            order_type=OrderType(str(raw["order_type"])),
            limit_price=None if raw.get("limit_price") is None else _integer(raw["limit_price"]),
            tag=str(raw.get("tag", "")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ExecutionError(f"invalid intent record: {exc}") from None


def _integer(raw: Any) -> int:
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        raise ExecutionError(f"journal value must be an integer: {raw!r}")
    try:
        return int(raw)
    except ValueError:
        raise ExecutionError(f"invalid journal integer: {raw!r}") from None


def restore_session(
    records: Sequence[Mapping[str, Any]], *, mode: ExecutionMode, costs: CostModel
) -> RecoveredSession:
    """Rebuild same-day order state from journal records."""
    next_seq = 0
    submit_times: list[dt.datetime] = []
    for rec in records:
        event = rec.get("event")
        if event in ("intent", "risk_reject"):
            client_id = rec.get("client_id")
            next_seq = max(next_seq, _seq_of(client_id))
        if event == "intent":
            submit_times.append(_parse_ts(rec.get("ts")))
    if mode is ExecutionMode.PAPER:
        return RecoveredSession(orders={}, next_seq=next_seq, submit_times=tuple(submit_times), ledger=None)
    orders: dict[str, Order] = {}
    for rec in records:
        event = rec.get("event")
        if event == "intent":
            client_id = str(rec.get("client_id"))
            if client_id in orders:
                raise ExecutionError(f"duplicate intent: {client_id}")
            _seq_of(client_id)
            intent = _intent_of(rec.get("intent"))
            orders[client_id] = Order(
                client_id=client_id,
                intent=intent,
                status=OrderStatus.PENDING_SUBMIT,
                created_at=_parse_ts(rec.get("ts")),
            )
        elif event in _ORDER_EVENTS or ("client_id" in rec and event != "risk_reject"):
            client_id = rec.get("client_id")
            _seq_of(client_id)
            order = orders.get(str(client_id))
            if order is None:
                raise ExecutionError(f"order event without intent: {client_id!r}")
            if event == "submit_result":
                try:
                    order.status = OrderStatus(str(rec.get("status")))
                except ValueError:
                    raise ExecutionError(f"invalid status: {rec.get('status')!r}") from None
                outcome = rec.get("outcome")
                if not isinstance(outcome, Mapping):
                    raise ExecutionError("invalid submit_result outcome")
                order.broker_order_no = outcome.get("broker_order_no")
                order.broker_org_no = outcome.get("broker_org_no")
                order.broker_order_time = outcome.get("order_time")
                if order.status is OrderStatus.REJECTED:
                    order.reject_code = str(outcome.get("code") or "") or None
                    order.reject_message = str(outcome.get("message") or "") or None
                else:
                    order.reject_code = None
                    order.reject_message = None
            elif event == "unknown_adopted":
                order.broker_order_no = rec.get("broker_order_no")
                order.broker_org_no = rec.get("broker_org_no")
                order.broker_order_time = rec.get("order_time")
            elif event == "fill":
                if "cumulative_amount_krw" not in rec:
                    raise ExecutionError("fill without cumulative_amount_krw")
                try:
                    qty = _integer(rec["cumulative_qty"])
                    amount = _integer(rec["cumulative_amount_krw"])
                    delta_qty = _integer(rec["delta_qty"])
                    delta_amount = _integer(rec["delta_amount_krw"])
                except (KeyError, TypeError, ValueError):
                    raise ExecutionError("invalid fill record") from None
                if (
                    not order.filled_qty < qty <= order.intent.qty
                    or amount < order.filled_amount_krw
                    or delta_qty != qty - order.filled_qty
                    or delta_amount != amount - order.filled_amount_krw
                ):
                    raise ExecutionError("inconsistent cumulative fill")
                order.filled_qty = qty
                order.filled_amount_krw = amount
            elif event == "status":
                try:
                    order.status = OrderStatus(str(rec.get("status")))
                except ValueError:
                    raise ExecutionError(f"invalid status: {rec.get('status')!r}") from None
                if "filled_qty" in rec:
                    status_qty = _integer(rec["filled_qty"])
                    if status_qty != order.filled_qty:
                        raise ExecutionError("status quantity without matching fill")
    for order in orders.values():
        if order.status is OrderStatus.PENDING_SUBMIT:
            order.status = OrderStatus.UNKNOWN
    ledger = _replay_ledger(records, orders, costs)
    return RecoveredSession(orders=orders, next_seq=next_seq, submit_times=tuple(submit_times), ledger=ledger)


def _replay_ledger(
    records: Sequence[Mapping[str, Any]], orders: dict[str, Order], costs: CostModel
) -> Ledger | None:
    starts = [r for r in records if r.get("event") == "session_start"]
    if not starts:
        return None
    first = starts[0]
    if "holdings" not in first or not isinstance(first["holdings"], list):
        raise ExecutionError("earliest session_start lacks holdings")
    positions: dict[str, Position] = {}
    for row in first["holdings"]:
        if not isinstance(row, Mapping):
            raise ExecutionError("invalid holdings snapshot")
        try:
            symbol = str(row["symbol"])
            qty = _integer(row["qty"])
            cost = _integer(row["cost_krw"])
        except (KeyError, TypeError, ValueError):
            raise ExecutionError("invalid holdings snapshot") from None
        if qty < 0 or cost < 0 or symbol in positions:
            raise ExecutionError("inconsistent holdings snapshot")
        positions[symbol] = Position(qty=qty, cost_krw=cost)
    ledger = Ledger(cash_krw=0, costs=costs, positions=positions)
    for rec in records:
        if rec.get("event") != "fill":
            continue
        order = orders[str(rec.get("client_id"))]
        delta_qty = int(rec["delta_qty"])
        delta_amount = int(rec["delta_amount_krw"])
        ledger.apply_fill(order.intent.symbol, order.intent.side, delta_qty, delta_amount)
    return ledger


def position_diffs(ledger: Ledger, holdings: Sequence[Holding]) -> list[tuple[str, int, int]]:
    """Return (symbol, ledger_qty, broker_qty) for every symbol whose quantities differ."""
    broker: dict[str, int] = {}
    for holding in holdings:
        broker[holding.symbol] = holding.qty
    diffs: list[tuple[str, int, int]] = []
    for symbol in sorted(set(ledger.positions) | set(broker)):
        held = ledger.positions.get(symbol)
        left = held.qty if held is not None else 0
        right = broker.get(symbol, 0)
        if left != right:
            diffs.append((symbol, left, right))
    return diffs
