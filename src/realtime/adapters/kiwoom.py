"""Kiwoom REST WebSocket realtime adapter (KRX 0B trades, 0D book)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final

import aiohttp

from src.realtime.contracts import (
    L0Frame,
    MarketSession,
    MarketVenue,
    VendorAck,
    VendorAuthRejected,
    VendorDisconnected,
)

logger = logging.getLogger(__name__)

KIWOOM_WS_URL: Final[str] = "wss://api.kiwoom.com:10000/api/dostk/websocket"
KIWOOM_STREAM_TYPE: Mapping[str, str] = {"H0STCNT0": "0B", "H0STASP0": "0D"}

_REV_STREAM_TYPE: dict[str, str] = {v: k for k, v in KIWOOM_STREAM_TYPE.items()}

_UNPARSABLE_LOG = "[DATA] stage=kiwoom_frame status=UNPARSEABLE conn_id=%s frame_len=%d"


class KiwoomRealtimeAdapter:
    """VendorAdapter over Kiwoom REST WebSocket: LOGIN, batched REG, PING echo, one L0Frame per REAL entry."""

    name: str
    capacity_pairs: int
    pingpong_count: int
    last_pingpong_wall_ns: int | None

    def __init__(
        self,
        *,
        app_key: str,
        secret_key: str,
        http: aiohttp.ClientSession,
        streams: tuple[str, ...],
        capacity_pairs: int,
        ws_url: str = KIWOOM_WS_URL,
        user_agent: str,
        ack_timeout_s: float,
        issue_token: Callable[[], Awaitable[str]],
    ) -> None:
        unknown = [stream for stream in streams if stream not in KIWOOM_STREAM_TYPE]
        if unknown:
            raise ValueError(f"unsupported streams: {unknown}")
        if ack_timeout_s <= 0:
            raise ValueError(f"ack_timeout_s must be positive, got {ack_timeout_s}")
        if capacity_pairs < 1:
            raise ValueError(f"capacity_pairs must be >= 1, got {capacity_pairs}")
        self.name = "kiwoom"
        self.capacity_pairs = capacity_pairs
        self._app_key = app_key
        self._secret_key = secret_key
        self._http = http
        self._streams = tuple(streams)
        self._ws_url = ws_url
        self._user_agent = user_agent
        self._ack_timeout_s = ack_timeout_s
        self._issue_token = issue_token
        self._ws: Any = None
        self._seq = 0
        self._conn_id = ""
        self._pending: list[L0Frame] = []
        self.pingpong_count = 0
        self.last_pingpong_wall_ns = None
        self.unparsable_frames = 0

    def _note_unparsable(self, frame_len: int) -> None:
        self.unparsable_frames += 1
        logger.warning(_UNPARSABLE_LOG, self._conn_id, frame_len)

    async def _send(self, payload: dict[str, Any]) -> None:
        try:
            await self._ws.send_str(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        except (aiohttp.ClientError, OSError) as exc:
            raise VendorDisconnected(f"send_failed:{type(exc).__name__}") from exc

    async def _await_reply(self, trnm: str) -> dict[str, Any]:
        deadline = time.monotonic() + self._ack_timeout_s
        while True:
            try:
                raw = await asyncio.wait_for(self._ws.receive_str(), timeout=max(0.0, deadline - time.monotonic()))
            except TimeoutError as exc:
                raise VendorDisconnected(f"ack_timeout:{trnm}") from exc
            except TypeError as exc:
                raise VendorDisconnected("subscribe_non_text") from exc
            except (aiohttp.ClientError, OSError) as exc:
                raise VendorDisconnected(f"recv_failed:{type(exc).__name__}") from exc
            try:
                obj = json.loads(raw)
            except ValueError:
                self._note_unparsable(len(raw))
                continue
            if not isinstance(obj, dict):
                self._note_unparsable(len(raw))
                continue
            if obj.get("trnm") == "PING":
                try:
                    await asyncio.wait_for(self._echo_ping(raw), timeout=max(0.0, deadline - time.monotonic()))
                except TimeoutError as exc:
                    raise VendorDisconnected(f"ack_timeout:{trnm}") from exc
                continue
            if obj.get("trnm") == "REAL":
                self._buffer_real(obj, time.monotonic_ns(), time.time_ns())
                continue
            if obj.get("trnm") == trnm:
                return obj
            self._note_unparsable(len(raw))

    async def _echo_ping(self, raw: str) -> None:
        self.pingpong_count += 1
        self.last_pingpong_wall_ns = time.time_ns()
        try:
            await self._ws.send_str(raw)
        except (aiohttp.ClientError, OSError) as exc:
            raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc

    def _buffer_real(self, obj: dict[str, Any], recv_mono_ns: int, recv_wall_ns: int) -> None:
        frames = self._split_real(obj, recv_mono_ns, recv_wall_ns)
        self._pending.extend(frames)

    def _split_real(self, obj: dict[str, Any], recv_mono_ns: int, recv_wall_ns: int) -> list[L0Frame]:
        data = obj.get("data")
        if not isinstance(data, list) or not data:
            self._note_unparsable(len(json.dumps(obj, ensure_ascii=False)))
            return []
        frames: list[L0Frame] = []
        for entry in data:
            frame = self._entry_frame(entry, recv_mono_ns, recv_wall_ns)
            if frame is None:
                self._note_unparsable(len(json.dumps(entry, ensure_ascii=False)) if isinstance(entry, dict) else 0)
                continue
            frames.append(frame)
        return frames

    def _entry_frame(self, entry: Any, recv_mono_ns: int, recv_wall_ns: int) -> L0Frame | None:
        if not isinstance(entry, dict):
            return None
        type_code = entry.get("type")
        item = entry.get("item")
        values = entry.get("values")
        if not isinstance(type_code, str) or type_code not in _REV_STREAM_TYPE:
            return None
        if not isinstance(item, str) or not item or not isinstance(values, dict):
            return None
        event_time = values.get("20")
        self._seq += 1
        return L0Frame(
            vendor="kiwoom",
            stream=_REV_STREAM_TYPE[type_code],
            symbol=item,
            raw=json.dumps(entry, separators=(",", ":"), ensure_ascii=False),
            recv_mono_ns=recv_mono_ns,
            recv_wall_ns=recv_wall_ns,
            conn_seq=self._seq,
            conn_id=self._conn_id,
            venue=MarketVenue.UNKNOWN if "_" in item else MarketVenue.KRX,
            session=MarketSession.REGULAR,
            exchange_event_time=event_time if isinstance(event_time, str) else "",
        )

    async def connect(self) -> None:
        try:
            token = await self._issue_token()
        except (VendorAuthRejected, VendorDisconnected):
            raise
        except Exception as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        try:
            self._ws = await self._http.ws_connect(
                self._ws_url, headers={"User-Agent": self._user_agent}, compress=0, heartbeat=None
            )
        except (aiohttp.ClientError, OSError, ValueError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._seq = 0
        self._conn_id = f"kiwoom-{time.time_ns()}"
        self._pending = []
        self.pingpong_count = 0
        self.last_pingpong_wall_ns = None
        self.unparsable_frames = 0
        await self._send({"trnm": "LOGIN", "token": token})
        reply = await self._await_reply("LOGIN")
        try:
            login_code = int(str(reply.get("return_code", "")).strip())
        except (TypeError, ValueError):
            raise VendorDisconnected("login_malformed") from None
        if login_code != 0:
            raise VendorAuthRejected(f"kiwoom_login:{reply.get('return_code')}")

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        for symbol, stream in pairs:
            if stream not in KIWOOM_STREAM_TYPE:
                raise ValueError(f"unsupported stream: {stream}")
            if "_" in symbol:
                raise ValueError("Kiwoom subscriptions require bare KRX codes")
        allowed = pairs[: self.capacity_pairs]
        per_type: dict[str, list[tuple[str, str]]] = {}
        for symbol, stream in allowed:
            per_type.setdefault(KIWOOM_STREAM_TYPE[stream], []).append((symbol, stream))
        limit = self.capacity_pairs // max(1, len(self._streams))
        ordered_acks: dict[tuple[str, str], VendorAck] = {}
        for grp_no, (type_code, group) in enumerate(per_type.items(), start=1):
            symbols = [symbol for symbol, _ in group]
            if len(symbols) > limit:
                head, tail = group[:limit], group[limit:]
                for symbol, stream in tail:
                    ordered_acks[(symbol, stream)] = VendorAck(symbol, stream, False, "capacity")
                group = head
                symbols = [symbol for symbol, _ in group]
            if not group:
                continue
            await self._send(
                {"trnm": "REG", "grp_no": str(grp_no), "refresh": "1", "data": [{"item": symbols, "type": [type_code]}]}
            )
            reply = await self._await_reply("REG")
            code = str(reply.get("return_code", ""))
            accepted = code == "0"
            for symbol, stream in group:
                ordered_acks[(symbol, stream)] = VendorAck(symbol, stream, accepted, code if code else "unknown")
        by_pair: dict[tuple[str, str], VendorAck] = {}
        for symbol, stream in pairs[self.capacity_pairs :]:
            by_pair[(symbol, stream)] = VendorAck(symbol, stream, False, "capacity")
        by_pair.update(ordered_acks)
        return [by_pair[(symbol, stream)] for symbol, stream in pairs]

    async def recv(self) -> L0Frame:
        if self._pending:
            return self._pending.pop(0)
        while True:
            try:
                raw = await self._ws.receive_str()
            except aiohttp.WSMessageTypeError as exc:
                raise VendorDisconnected(f"ws_non_text_frame:{str(exc)[:80]}") from exc
            except (aiohttp.ClientError, OSError) as exc:
                raise VendorDisconnected(f"recv_failed:{type(exc).__name__}") from exc
            recv_mono_ns = time.monotonic_ns()
            recv_wall_ns = time.time_ns()
            try:
                obj = json.loads(raw)
            except ValueError:
                self._note_unparsable(len(raw))
                continue
            if not isinstance(obj, dict):
                self._note_unparsable(len(raw))
                continue
            if obj.get("trnm") == "PING":
                await self._echo_ping(raw)
                continue
            if obj.get("trnm") == "REAL":
                frames = self._split_real(obj, recv_mono_ns, recv_wall_ns)
                if not frames:
                    continue
                self._pending.extend(frames[1:])
                return frames[0]
            self._note_unparsable(len(raw))

    async def aclose(self) -> None:
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            await ws.close()
        except (aiohttp.ClientError, OSError) as exc:
            logger.debug("[DATA] stage=kiwoom_close status=SUPPRESSED error_type=%s", type(exc).__name__)
