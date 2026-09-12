"""벤더 추상화 + 구독 플래너 (실시간 수집기 v1)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.core.errors import KrxAlphaError, SlotBudgetExceededError

__all__ = [
    "L0Frame",
    "SubscriptionPlanner",
    "VendorAck",
    "VendorAdapter",
    "VendorCapacity",
    "VendorDisconnected",
]


@dataclass(frozen=True)
class L0Frame:
    vendor: str
    stream: str
    symbol: str
    raw: str
    recv_mono_ns: int
    recv_wall_ns: int
    conn_seq: int
    conn_id: str


@dataclass(frozen=True)
class VendorAck:
    symbol: str
    stream: str
    accepted: bool
    code: str


@dataclass(frozen=True)
class VendorCapacity:
    vendor: str
    capacity_pairs: int


class VendorDisconnected(KrxAlphaError):  # noqa: N818 - contract-pinned signal name
    """WS 절단 신호."""


class VendorAdapter(Protocol):
    name: str
    capacity_pairs: int

    async def connect(self) -> None: ...
    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]: ...
    async def recv(self) -> L0Frame: ...
    async def aclose(self) -> None: ...


class SubscriptionPlanner:
    def __init__(self, *, streams: tuple[str, ...]) -> None:
        self._streams = streams

    def symbol_budget(self, capacities: list[VendorCapacity]) -> int:
        return sum(cap.capacity_pairs // len(self._streams) for cap in capacities)

    def plan(
        self,
        symbols: list[str],
        primary: list[VendorCapacity],
        crosscheck: list[tuple[VendorCapacity, int]] | None = None,
    ) -> dict[str, list[tuple[str, str]]]:
        n_streams = len(self._streams)
        per_vendor = [cap.capacity_pairs // n_streams for cap in primary]
        if len(symbols) > sum(per_vendor):
            raise SlotBudgetExceededError(
                f"want {len(symbols)} symbols exceeds primary capacity {sum(per_vendor)}"
            )
        out: dict[str, list[tuple[str, str]]] = {}
        rest = list(symbols)
        for cap, budget in zip(primary, per_vendor, strict=True):
            take = rest[:budget]
            rest = rest[budget:]
            pairs = sorted((sym, stream) for sym in take for stream in self._streams)
            if pairs:
                out[cap.vendor] = pairs
        for cap, top_n in crosscheck or []:
            limit = min(top_n, len(symbols), cap.capacity_pairs // n_streams)
            pairs = sorted((sym, stream) for sym in symbols[:limit] for stream in self._streams)
            if pairs:
                out[cap.vendor] = pairs
        return out
