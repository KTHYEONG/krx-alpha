"""Layer 3: JSON-over-WebSocket OAuth 어댑터 공통 기반 (client_credentials 공유 토큰 스토어)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, ClassVar

import aiohttp

from src.marketdata.toss_token_store import IssuedToken, TokenStoreLockTimeout, TossTokenStore
from src.realtime.contracts import L0Frame, VendorAck, VendorAuthRejected, VendorDisconnected


class JsonWsOAuthAdapter(ABC):
    """VendorAdapter base for JSON-over-WebSocket vendors authenticated by an OAuth client_credentials token."""

    name: str
    capacity_pairs: int
    unparsable_frames: int

    VENDOR: ClassVar[str]
    UNPARSABLE_LOG: ClassVar[str]
    CLOSE_LOG: ClassVar[str]
    SUBSCRIBE_TR_TYPE: ClassVar[str]
    MALFORMED_ACK_FATAL: ClassVar[bool]
    ECHO_PINGPONG: ClassVar[bool]
    REPLACE_TOKEN_ON_WS_AUTH_REJECT: ClassVar[bool]

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        http: Any,
        token_store: TossTokenStore,
        streams: tuple[str, ...],
        capacity_pairs: int,
        token_url: str,
        ws_url: str,
        heartbeat_s: float,
        ack_timeout_s: float,
        subscribe_spacing_s: float | None,
    ) -> None:
        """Bind one websocket session to a host-shared token store; no I/O."""
        self.name = self.VENDOR
        self.capacity_pairs = capacity_pairs
        self._app_key = app_key
        self._app_secret = app_secret
        self._http = http
        self._streams = streams
        self._token_url = token_url
        self._ws_url = ws_url
        self._heartbeat_s = heartbeat_s
        self._ack_timeout_s = ack_timeout_s
        self._subscribe_spacing_s = subscribe_spacing_s
        self._token_store = token_store
        self._ws: Any = None
        self._token: str | None = None
        self._seq = 0
        self._conn_id: str = ""
        self._pending: list[L0Frame] = []
        self.unparsable_frames = 0
        self._unparsable_warned = False

    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(type(self).__module__)

    @abstractmethod
    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]: ...

    @abstractmethod
    def _data_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None: ...

    @abstractmethod
    def _token_failure(self, status: int | None, code: str) -> VendorDisconnected | None: ...

    def _check_capacity(self, pairs: list[tuple[str, str]]) -> None:
        return None

    def _check_ack_code(self, rsp_cd: str) -> None:
        return None

    def _all_rejected_is_auth(self, acks: list[VendorAck]) -> bool:
        return True

    def _counts_as_unparsable(self, header: dict[str, Any]) -> bool:
        return False

    async def _on_subscribe_send_failure(self, acks: list[VendorAck], exc: Exception) -> VendorDisconnected:
        return VendorDisconnected(f"subscribe_failed:{type(exc).__name__}")

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    async def _issue_token(self) -> IssuedToken:
        try:
            async with self._http.post(
                self._token_url,
                data={
                    "grant_type": "client_credentials",
                    "appkey": self._app_key,
                    "appsecretkey": self._app_secret,
                    "scope": "oob",
                },
            ) as resp:
                data = await resp.json()
                if not isinstance(data, dict):
                    raise VendorDisconnected("connect_failed:token_response_not_object")
                status = getattr(resp, "status", None)
                if "access_token" not in data:
                    exc = self._token_failure(status, str(data.get("error_code", "")))
                    if exc is not None:
                        raise exc
                token = str(data["access_token"])
        except (VendorAuthRejected, VendorDisconnected):
            raise
        except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, OSError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        expires_in = data.get("expires_in") if isinstance(data, dict) else None
        lifetime = (
            float(expires_in)
            if isinstance(expires_in, (int, float)) and not isinstance(expires_in, bool) and expires_in > 0
            else None
        )
        return IssuedToken(token, lifetime)

    async def connect(self) -> None:
        """Acquire a shared token and open the socket; raises VendorDisconnected / VendorAuthRejected."""
        try:
            token = await self._token_store.aget_or_issue(self._issue_token)
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc
        self._token = token
        try:
            self._ws = await (self._http.ws_connect(self._ws_url, heartbeat=self._heartbeat_s)).__aenter__()
        except VendorAuthRejected:
            if not self.REPLACE_TOKEN_ON_WS_AUTH_REJECT:
                raise
            await self._replace_rejected_token(token)
            raise
        except VendorDisconnected:
            raise
        except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, OSError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._seq = 0
        self._conn_id = f"{self.VENDOR}-{time.time_ns()}"
        self._pending = []
        self.unparsable_frames = 0
        self._unparsable_warned = False

    async def _replace_rejected_token(self, token: str) -> None:
        try:
            await self._token_store.areplace_rejected(token, self._issue_token)
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc

    def _note_unparsable(self, frame_len: int) -> None:
        self.unparsable_frames += 1
        if not self._unparsable_warned:
            self._unparsable_warned = True
            self._logger.warning(self.UNPARSABLE_LOG, self._conn_id, frame_len)

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        """Subscribe pairs sequentially and return acks in send order; raises VendorDisconnected / VendorAuthRejected."""
        self._check_capacity(pairs)
        acks: list[VendorAck] = []
        for idx, (symbol, stream) in enumerate(pairs):
            if self._subscribe_spacing_s is not None and idx > 0:
                await asyncio.sleep(self._subscribe_spacing_s)
            try:
                await self._ws.send_str(
                    json.dumps(
                        {
                            "header": {"token": self._token, "tr_type": self.SUBSCRIBE_TR_TYPE},
                            "body": self._subscribe_body(symbol, stream),
                        }
                    )
                )
            except (aiohttp.ClientError, OSError) as exc:
                raise await self._on_subscribe_send_failure(acks, exc) from exc
            resp: dict[str, Any] | None = None
            while resp is None:
                try:
                    raw = await asyncio.wait_for(self._ws.receive_str(), timeout=self._ack_timeout_s)
                except TimeoutError as exc:
                    raise VendorDisconnected(f"ack_timeout:{symbol}:{stream}") from exc
                except TypeError as exc:
                    raise VendorDisconnected("subscribe_non_text") from exc
                try:
                    o = json.loads(raw)
                except ValueError as exc:
                    if self.MALFORMED_ACK_FATAL:
                        raise VendorDisconnected(f"malformed_ack:{stream}") from exc
                    self._note_unparsable(len(raw))
                    continue
                if not isinstance(o, dict):
                    if self.MALFORMED_ACK_FATAL:
                        raise VendorDisconnected(f"malformed_ack:{stream}")
                    self._note_unparsable(len(raw))
                    continue
                header = o.get("header", {})
                if not isinstance(header, dict):
                    continue
                if self.ECHO_PINGPONG and header.get("tr_cd") == "PINGPONG":
                    try:
                        await self._ws.send_str(raw)
                    except (aiohttp.ClientError, OSError) as exc:
                        raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc
                    continue
                frame = self._data_frame(o, header, raw)
                if frame is not None:
                    self._pending.append(frame)
                    continue
                if "rsp_cd" in header:
                    resp = o
            rsp_cd = str(resp["header"].get("rsp_cd"))
            self._check_ack_code(rsp_cd)
            acks.append(VendorAck(symbol, stream, rsp_cd == "00000", rsp_cd))
        if acks and not any(a.accepted for a in acks) and self._all_rejected_is_auth(acks):
            codes = ",".join(sorted({a.code for a in acks}))
            if self._token is not None:
                await self._replace_rejected_token(self._token)
            raise VendorAuthRejected(f"auth_rejected:all_acks_rejected:{codes}")
        return acks

    async def recv(self) -> L0Frame:
        """Return the next data frame (stashed frames first); raises VendorDisconnected on close/transport errors."""
        if self._pending:
            return self._pending.pop(0)
        while True:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.PING:
                try:
                    await self._ws.pong()
                except (aiohttp.ClientError, OSError) as exc:
                    raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc
                continue
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.data
                try:
                    o = json.loads(data)
                except ValueError:
                    self._note_unparsable(len(data) if isinstance(data, str) else 0)
                    continue
                if not isinstance(o, dict):
                    self._note_unparsable(len(data) if isinstance(data, str) else 0)
                    continue
                header = o.get("header", {})
                if not isinstance(header, dict):
                    continue
                if self.ECHO_PINGPONG and header.get("tr_cd") == "PINGPONG":
                    try:
                        await self._ws.send_str(data)
                    except (aiohttp.ClientError, OSError) as exc:
                        raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc
                    continue
                frame = self._data_frame(o, header, data)
                if frame is not None:
                    return frame
                if self._counts_as_unparsable(header):
                    self._note_unparsable(len(data) if isinstance(data, str) else 0)
                continue
            detail = str(msg.type)
            if msg.type == aiohttp.WSMsgType.CLOSE:
                detail = f"{detail}:code={msg.data}:reason={getattr(msg, 'extra', '') or ''}"
            raise VendorDisconnected(detail)

    async def aclose(self) -> None:
        """Close the socket idempotently, suppressing transport errors."""
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            await ws.close()
        except (aiohttp.ClientError, OSError) as exc:
            self._logger.debug(self.CLOSE_LOG, type(exc).__name__)
