"""포지션 원장: 현금·보유·실현손익의 정수(원) 회계."""

from __future__ import annotations

from dataclasses import dataclass

from src.execution.contracts import BrokerIntegrityError, Side


@dataclass(frozen=True)
class CostModel:
    commission_bps: float
    sell_tax_bps: float

    def commission(self, amount_krw: int) -> int:
        return int(amount_krw * self.commission_bps // 10_000)

    def sell_tax(self, amount_krw: int) -> int:
        return int(amount_krw * self.sell_tax_bps // 10_000)


@dataclass
class Position:
    qty: int = 0
    cost_krw: int = 0


class Ledger:
    """현금·포지션·실현손익 원장."""

    def __init__(self, *, cash_krw: int, costs: CostModel, positions: dict[str, Position] | None = None) -> None:
        self.cash_krw = cash_krw
        self.costs = costs
        self.realized_pnl_krw = 0
        self.positions = dict(positions) if positions is not None else {}

    def qty(self, symbol: str) -> int:
        return self.positions.get(symbol, Position()).qty

    def apply_fill(self, symbol: str, side: Side, qty: int, amount_krw: int) -> int:
        """누적체결 델타를 원장에 반영하고 실현손익(매도) 을 반환한다."""
        if qty <= 0 or amount_krw < 0:
            raise BrokerIntegrityError(f"invalid fill: qty={qty} amount={amount_krw}")
        if side is Side.BUY:
            fee = self.costs.commission(amount_krw)
            self.cash_krw -= amount_krw + fee
            pos = self.positions.get(symbol)
            if pos is None:
                pos = Position()
                self.positions[symbol] = pos
            pos.qty += qty
            pos.cost_krw += amount_krw + fee
            return 0
        pos = self.positions.get(symbol)
        if pos is None or qty > pos.qty:
            raise BrokerIntegrityError(f"oversell without position: {symbol} qty={qty}")
        fee = self.costs.commission(amount_krw)
        tax = self.costs.sell_tax(amount_krw)
        removed = pos.cost_krw * qty // pos.qty
        pos.cost_krw -= removed
        pos.qty -= qty
        proceeds = amount_krw - fee - tax
        self.cash_krw += proceeds
        pnl = proceeds - removed
        self.realized_pnl_krw += pnl
        if pos.qty == 0:
            del self.positions[symbol]
        return pnl
