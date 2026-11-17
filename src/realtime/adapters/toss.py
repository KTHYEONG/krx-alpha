"""Toss Securities realtime adapter (unified KRX+NXT trades and unified book over one declarative websocket)."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final

import aiohttp

from src.marketdata.toss_auth import is_invalid_token_envelope
from src.marketdata.toss_token_store import IssuedToken, TokenStoreLockTimeout, TossTokenStore
from src.realtime.contracts import (
    L0Frame,
    MarketSession,
    MarketVenue,
    VendorAck,
    VendorAuthRejected,
    VendorDisconnected,
)

logger = logging.getLogger(__name__)

TOSS_WS_URL: Final[str] = "wss://openapi-ws.tossinvest.com/ws/v1"
# Read-only market-data topics; account and order topics are unreachable because only these two are mapped.
TOSS_TOPIC_TYPE: Mapping[str, str] = {"H0STCNT0": "trade:kr", "H0STASP0": "orderbook:kr"}

_REV_TOPIC_TYPE: dict[str, str] = {v: k for k, v in TOSS_TOPIC_TYPE.items()}
_SYMBOL_RE = re.compile(r"[0-9A-Z]{6}")
_KST = dt.timezone(dt.timedelta(hours=9))
_UNPARSABLE_LOG = "[DATA] stage=toss_frame status=UNPARSEABLE conn_id=%s frame_len=%d"
_PING: Final[str] = "PING"
_PONG: Final[str] = "PONG"
_MISSED_PONG_INTERVALS: Final[int] = 2
_MIN_WAIT_S: Final[float] = 0.001


def _topic(pair: tuple[str, str]) -> str:
    symbol, stream = pair
    return f"{TOSS_TOPIC_TYPE[stream]}:{symbol}"


def _ack_topic(item: object) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict) and isinstance(item.get("topic"), str):
        return str(item["topic"])
    return json.dumps(item, sort_keys=True)


def _event_hhmmss(stamp: object) -> str:
    if not isinstance(stamp, str):
        return ""
    try:
        moment = dt.datetime.fromisoformat(stamp)
    except ValueError:
        return ""
    if moment.tzinfo is None:
        return ""
    return moment.astimezone(_KST).strftime("%H%M%S")


async def _refuse_issue() -> IssuedToken:
    """Issue callback handed to the shared store: this adapter only consumes tokens that kca (the sole issuer) published."""
    raise VendorDisconnected("token_unavailable:no_shared_token")


class TossRealtimeAdapter:
    """VendorAdapter over the Toss declarative websocket: state-replacing declarations, text PING/PONG, unified venue."""

    name: str = "toss"
    capacity_pairs: int
    pingpong_count: int
    last_pingpong_wall_ns: int | None
    unparsable_frames: int

    def __init__(
        self,
        *,
        http: Any,
        token_store: TossTokenStore,
        ws_url: str = TOSS_WS_URL,
        capacity_pairs: int = 100,
        ack_timeout_s: float = 10.0,
        declare_spacing_s: float = 0.25,
        heartbeat_s: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Bind one Toss websocket session to the host-shared token store; no I/O."""
        if capacity_pairs < 1:
            raise ValueError(f"capacity_pairs must be >= 1, got {capacity_pairs}")
        if ack_timeout_s <= 0 or declare_spacing_s <= 0 or heartbeat_s <= 0:
            raise ValueError("ack_timeout_s, declare_spacing_s and heartbeat_s must be positive")
        self.capacity_pairs = capacity_pairs
        self._http = http
        self._token_store = token_store
        self._ws_url = ws_url
        self._ack_timeout_s = ack_timeout_s
        self._declare_spacing_s = declare_spacing_s
        self._heartbeat_s = heartbeat_s
        self._clock = clock
        self._sleep = sleep
        self._ws: Any = None
        self._token: str | None = None
        self._seq = 0
        self._conn_id = ""
        self._pending: list[L0Frame] = []
        self._desired: list[tuple[str, str]] = []
        self._subscribed: list[tuple[str, str]] = []
        self._ack_waiter: asyncio.Future[tuple[set[str], set[str]]] | None = None
        self._late_acks = 0
        self._last_declared_at: float | None = None
        self._next_ping_at = 0.0
        self._last_pong_at = 0.0
        self._unparsable_warned = False
        self._lock = asyncio.Lock()
        self.pingpong_count = 0
        self.last_pingpong_wall_ns = None
        self.unparsable_frames = 0

    async def connect(self) -> None:
        """Take the shared token (never issuing one) and open the socket; raises VendorDisconnected / VendorAuthRejected."""
        try:
            token = await self._token_store.aget_or_issue(_refuse_issue)
        except VendorDisconnected:
            raise
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc
        try:
            self._ws = await self._http.ws_connect(
                self._ws_url, headers={"Authorization": f"Bearer {token}"}, heartbeat=None, max_msg_size=0
            )
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in (401, 403):
                await self._replace_rejected(token)
                raise VendorAuthRejected(f"auth_rejected:{exc.status}") from exc
            raise VendorDisconnected(f"handshake_failed:{exc.status}") from exc
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._token = token
        self._seq = 0
        self._conn_id = f"toss-{time.time_ns()}"
        self._pending = []
        self._desired = []
        self._subscribed = []
        self._ack_waiter = None
        self._late_acks = 0
        self._last_declared_at = None
        now = self._clock()
        self._next_ping_at = now + self._heartbeat_s
        self._last_pong_at = now
        self._unparsable_warned = False
        self.pingpong_count = 0
        self.last_pingpong_wall_ns = None
        self.unparsable_frames = 0

    async def _replace_rejected(self, token: str) -> None:
        try:
            await self._token_store.areplace_rejected(token, _refuse_issue)
        except VendorDisconnected:
            return
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc

    def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
        """Pairs the server acknowledged on the current connection (reset on connect)."""
        return tuple(self._subscribed)

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        """Declare pairs and read the socket until the ack; data frames met meanwhile are buffered for recv()."""
        return await self._declare_pairs(pairs, inline=True)

    async def subscribe_live(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        """Declare pairs while recv() runs; the ack is routed through recv() and the socket is never read here."""
        return await self._declare_pairs(pairs, inline=False)

    async def _declare_pairs(self, pairs: list[tuple[str, str]], *, inline: bool) -> list[VendorAck]:
        for symbol, stream in pairs:
            if stream not in TOSS_TOPIC_TYPE:
                raise ValueError(f"unsupported stream: {stream}")
            if _SYMBOL_RE.fullmatch(symbol) is None:
                raise ValueError(f"invalid symbol: {symbol!r}")
        async with self._lock:
            known = set(self._desired)
            candidate = [*self._desired, *dict.fromkeys(p for p in pairs if p not in known)]
            if len(candidate) > self.capacity_pairs:
                raise VendorDisconnected(f"capacity_exceeded:{len(candidate)}>{self.capacity_pairs}")
            waiter: asyncio.Future[tuple[set[str], set[str]]] = asyncio.get_running_loop().create_future()
            self._ack_waiter = waiter
            try:
                await self._send_declaration(candidate)
                result = await (self._pump_until(waiter) if inline else self._wait_routed(waiter))
            finally:
                self._ack_waiter = None
            if result is None:
                self._late_acks += 1
                logger.warning(
                    "[DATA] stage=toss_declare status=TIMEOUT conn_id=%s topics=%d", self._conn_id, len(candidate)
                )
                self._desired = [p for p in candidate if p in known]
                return [VendorAck(symbol, stream, False, "timeout") for symbol, stream in pairs]
            subscribed, rejected = result
            self._desired = [p for p in candidate if _topic(p) in subscribed]
            self._subscribed = list(self._desired)
            return [self._ack_for(pair, subscribed, rejected) for pair in pairs]

    @staticmethod
    def _ack_for(pair: tuple[str, str], subscribed: set[str], rejected: set[str]) -> VendorAck:
        topic = _topic(pair)
        if topic in subscribed:
            return VendorAck(pair[0], pair[1], True, "subscribed")
        if topic in rejected:
            return VendorAck(pair[0], pair[1], False, "rejected")
        return VendorAck(pair[0], pair[1], False, "missing_ack")

    async def _send_declaration(self, pairs: list[tuple[str, str]]) -> None:
        if self._last_declared_at is not None:
            wait = self._last_declared_at + self._declare_spacing_s - self._clock()
            if wait > 0:
                await self._sleep(wait)
        self._last_declared_at = self._clock()
        grouped: dict[str, list[str]] = {}
        for symbol, stream in pairs:
            grouped.setdefault(TOSS_TOPIC_TYPE[stream], []).append(symbol)
        body = [{"type": kind, "codes": codes} for kind, codes in grouped.items()]
        await self._send(json.dumps(body, separators=(",", ":")))

    async def _send(self, text: str) -> None:
        try:
            await self._ws.send_str(text)
        except (aiohttp.ClientError, OSError) as exc:
            raise VendorDisconnected(f"send_failed:{type(exc).__name__}") from exc

    async def _wait_routed(self, waiter: asyncio.Future[tuple[set[str], set[str]]]) -> tuple[set[str], set[str]] | None:
        try:
            return await asyncio.wait_for(waiter, timeout=self._ack_timeout_s)
        except TimeoutError:
            return None

    async def _pump_until(self, waiter: asyncio.Future[tuple[set[str], set[str]]]) -> tuple[set[str], set[str]] | None:
        deadline = self._clock() + self._ack_timeout_s
        while not waiter.done():
            remaining = deadline - self._clock()
            if remaining <= 0:
                return None
            received = await self._receive_text(remaining)
            if received is None:
                continue
            frame = await self._dispatch(*received)
            if frame is not None:
                self._pending.append(frame)
        return waiter.result()

    async def _receive_text(self, wait_s: float) -> tuple[str, int, int] | None:
        try:
            msg = await self._ws.receive(timeout=wait_s)
        except TimeoutError:
            return None
        except (aiohttp.ClientError, OSError) as exc:
            raise VendorDisconnected(f"recv_failed:{type(exc).__name__}") from exc
        mono, wall = time.monotonic_ns(), time.time_ns()
        if msg.type == aiohttp.WSMsgType.TEXT:
            return str(msg.data), mono, wall
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
            code = msg.data if msg.type == aiohttp.WSMsgType.CLOSE else msg.type.name
            raise VendorDisconnected(f"close:{code}")
        if msg.type == aiohttp.WSMsgType.ERROR:
            raise VendorDisconnected("ws_error")
        return None

    async def _heartbeat_tick(self) -> None:
        now = self._clock()
        if now - self._last_pong_at >= self._heartbeat_s * _MISSED_PONG_INTERVALS:
            raise VendorDisconnected("pong_timeout")
        if now >= self._next_ping_at:
            self._next_ping_at = now + self._heartbeat_s
            await self._send(_PING)

    async def recv(self) -> L0Frame:
        """Return the next data frame (buffered frames first); raises VendorDisconnected on close, silence or auth loss."""
        if self._pending:
            return self._pending.pop(0)
        while True:
            await self._heartbeat_tick()
            received = await self._receive_text(max(_MIN_WAIT_S, self._next_ping_at - self._clock()))
            if received is None:
                continue
            frame = await self._dispatch(*received)
            if frame is not None:
                return frame

    async def _dispatch(self, text: str, recv_mono_ns: int, recv_wall_ns: int) -> L0Frame | None:
        if text == _PONG:
            self._last_pong_at = self._clock()
            self.pingpong_count += 1
            self.last_pingpong_wall_ns = recv_wall_ns
            return None
        if text == _PING:
            await self._send(_PONG)
            return None
        try:
            obj = json.loads(text)
        except ValueError:
            self._note_unparsable(len(text))
            return None
        if not isinstance(obj, dict):
            self._note_unparsable(len(text))
            return None
        if is_invalid_token_envelope(obj):
            if self._token is not None:
                await self._replace_rejected(self._token)
            raise VendorAuthRejected("auth_rejected:invalid_token")
        kind = obj.get("type")
        if kind == "subscriptions":
            self._route_ack(obj)
            return None
        if kind == "message":
            frame = self._data_frame(obj, text, recv_mono_ns, recv_wall_ns)
            if frame is None:
                self._note_unparsable(len(text))
            return frame
        self._note_unparsable(len(text))
        return None

    def _route_ack(self, obj: dict[str, Any]) -> None:
        waiter = self._ack_waiter
        if waiter is not None and not waiter.done():
            subscribed = {_ack_topic(item) for item in obj.get("subscribed") or []}
            rejected = {_ack_topic(item) for item in obj.get("rejected") or []}
            waiter.set_result((subscribed, rejected))
        elif self._late_acks > 0:
            self._late_acks -= 1
            logger.info("[DATA] stage=toss_declare status=LATE_ACK_IGNORED conn_id=%s", self._conn_id)

    def _data_frame(self, obj: dict[str, Any], raw: str, recv_mono_ns: int, recv_wall_ns: int) -> L0Frame | None:
        topic, data = obj.get("topic"), obj.get("data")
        if not isinstance(topic, str) or not isinstance(data, dict):
            return None
        parts = topic.split(":")
        if len(parts) != 3:
            return None
        stream = _REV_TOPIC_TYPE.get(f"{parts[0]}:{parts[1]}")
        if stream is None or _SYMBOL_RE.fullmatch(parts[2]) is None:
            return None
        self._seq += 1
        return L0Frame(
            vendor="toss",
            stream=stream,
            symbol=parts[2],
            raw=raw,
            recv_mono_ns=recv_mono_ns,
            recv_wall_ns=recv_wall_ns,
            conn_seq=self._seq,
            conn_id=self._conn_id,
            venue=MarketVenue.UNIFIED,
            session=MarketSession.REGULAR,
            exchange_event_time=_event_hhmmss(data.get("timestamp")),
        )

    def _note_unparsable(self, frame_len: int) -> None:
        self.unparsable_frames += 1
        if not self._unparsable_warned:
            self._unparsable_warned = True
            logger.warning(_UNPARSABLE_LOG, self._conn_id, frame_len)

    async def aclose(self) -> None:
        """Close the socket idempotently, suppressing transport errors."""
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            await ws.close()
        except (aiohttp.ClientError, OSError) as exc:
            logger.debug("[DATA] stage=toss_close status=SUPPRESSED error_type=%s", type(exc).__name__)
