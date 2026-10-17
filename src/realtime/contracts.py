"""벤더 추상화 (실시간 수집기 v1)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from src.core.errors import KrxAlphaError

__all__ = [
    "HeartbeatProbe",
    "L0Frame",
    "MarketSession",
    "MarketVenue",
    "VendorAck",
    "VendorAdapter",
    "VendorAuthRejected",
    "VendorDisconnected",
]


class MarketVenue(StrEnum):
    KRX = "krx"
    NXT = "nxt"
    UNKNOWN = "unknown"


class MarketSession(StrEnum):
    REGULAR = "regular"
    KRX_AFTER = "krx_after"
    NXT_AFTER = "nxt_after"
    NXT_PRE = "nxt_pre"


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
    venue: MarketVenue = MarketVenue.UNKNOWN
    session: MarketSession = MarketSession.REGULAR
    exchange_event_time: str = ""


@dataclass(frozen=True)
class VendorAck:
    symbol: str
    stream: str
    accepted: bool
    code: str


class VendorDisconnected(KrxAlphaError):  # noqa: N818 - contract-pinned signal name
    """WS 절단 신호."""


class VendorAuthRejected(VendorDisconnected):  # noqa: N818
    """벤더 인증 거부 신호 (키 폐기·만료, 자동 복구 불가)."""


class VendorAdapter(Protocol):
    name: str
    capacity_pairs: int

    async def connect(self) -> None: ...
    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]: ...
    async def recv(self) -> L0Frame: ...
    async def aclose(self) -> None: ...


@runtime_checkable
class HeartbeatProbe(Protocol):
    """Optional adapter telemetry for vendor application-level heartbeats.

    Diagnostic only: heartbeats prove the control channel is alive, never that
    market data flows, so the streamer must not treat them as liveness.
    Counters are per connection and reset on ``connect``.
    """

    pingpong_count: int
    last_pingpong_wall_ns: int | None
