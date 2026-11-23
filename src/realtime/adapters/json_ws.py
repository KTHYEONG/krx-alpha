"""Layer 3: JSON-over-WebSocket OAuth 어댑터 공통 기반 (client_credentials 공유 토큰 스토어)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar

import aiohttp

from deploy.shared.host_broker_contract import (
    IssuedToken,
    TokenIssueThrottled,
)
from deploy.shared.host_broker_contract import (
    TokenStoreLockTimeout as CanonicalLockTimeout,
)
from src.marketdata.host_token_store import AsyncRotatingTokenStore, TokenStoreFailureError
from src.marketdata.toss_token_store import IssuedToken as TossIssuedToken
from src.marketdata.toss_token_store import TokenStoreLockTimeout, TossTokenStore
from src.realtime.contracts import L0Frame, VendorAck, VendorAuthRejected, VendorDisconnected

LOCAL_REJECT_PREFIX = "local:"


class RejectedPairHold:
    """Remembers vendor-rejected (symbol, stream) pairs so reconnects stop resending them until the hold expires.

    Share one instance across every adapter of a process (primary and standby) so a standby never resends a pair
    the primary already learned is rejected.
    """

    def __init__(self, hold_s: float, *, clock: Callable[[], float] = time.monotonic) -> None:
        if hold_s <= 0:
            raise ValueError(f"hold_s must be positive, got {hold_s}")
        self._hold_s = hold_s
        self._clock = clock
        self._until: dict[tuple[str, str], tuple[float, str]] = {}

    @property
    def hold_s(self) -> float:
        return self._hold_s

    def hold(self, pair: tuple[str, str], code: str) -> None:
        self._until[pair] = (self._clock() + self._hold_s, code)

    def held_code(self, pair: tuple[str, str]) -> str | None:
        """Vendor code that put `pair` on hold, or None when it is not held (an expired hold is dropped)."""
        entry = self._until.get(pair)
        if entry is None:
            return None
        if self._clock() >= entry[0]:
            del self._until[pair]
            return None
        return entry[1]


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
        token_store: TossTokenStore | AsyncRotatingTokenStore,
        streams: tuple[str, ...],
        capacity_pairs: int,
        token_url: str,
        ws_url: str,
        heartbeat_s: float,
        ack_timeout_s: float,
        subscribe_spacing_s: float | None,
        rejected_hold: RejectedPairHold | None = None,
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
        self._live_lock = asyncio.Lock()
        self._live_waiter: asyncio.Future[str] | None = None
        self._late_acks = 0
        self._subscribed: list[tuple[str, str]] = []
        self._rejected_hold = rejected_hold

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

    def _invalid_key(self, symbol: str, stream: str) -> str | None:
        """Reason a pair must never be sent (the vendor would reject it), or None when it is sendable."""
        return None

    def _is_pair_rejection(self, code: str) -> bool:
        """True when `code` rejects only the pair (bad key), never the session or token."""
        return False

    def _local_reject(self, symbol: str, stream: str) -> VendorAck | None:
        """Reject a pair without sending it: invalid key, or still on hold after a vendor rejection."""
        reason = self._invalid_key(symbol, stream)
        if reason is None and self._rejected_hold is not None:
            held = self._rejected_hold.held_code((symbol, stream))
            if held is not None:
                reason = f"held:{held}"
        if reason is None:
            return None
        return VendorAck(symbol, stream, False, f"{LOCAL_REJECT_PREFIX}{reason}")

    def _note_vendor_rejection(self, ack: VendorAck, *, session_proven: bool) -> None:
        """Hold a vendor-rejected pair when the rejection is pair-specific (bad-key code, or a peer pair was accepted)."""
        if self._rejected_hold is None or ack.accepted or ack.code == "timeout":
            return
        if not (session_proven or self._is_pair_rejection(ack.code)):
            return
        self._rejected_hold.hold((ack.symbol, ack.stream), ack.code)
        self._logger.warning(
            "[DATA] stage=subscribe_hold status=HELD vendor=%s conn_id=%s symbol=%s stream=%s code=%s hold_s=%.0f",
            self.VENDOR,
            self._conn_id,
            ack.symbol,
            ack.stream,
            ack.code,
            self._rejected_hold.hold_s,
        )

    def _auth_suspects(self, acks: list[VendorAck]) -> list[VendorAck]:
        """Answered vendor rejections that may mean a bad token (local and pair-specific rejections excluded)."""
        return [
            a
            for a in acks
            if a.code != "timeout" and not a.code.startswith(LOCAL_REJECT_PREFIX) and not self._is_pair_rejection(a.code)
        ]

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
            if isinstance(self._token_store, AsyncRotatingTokenStore):
                token = await self._token_store.aget_or_issue(self._issue_token)
            else:
                async def toss_issue() -> TossIssuedToken:
                    issued = await self._issue_token()
                    return TossIssuedToken(issued.access_token, issued.expires_in_seconds)

                token = await self._token_store.aget_or_issue(toss_issue)
        except (TokenStoreFailureError, CanonicalLockTimeout, TokenIssueThrottled, TokenStoreLockTimeout, OSError) as exc:
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
        self._live_waiter = None
        self._late_acks = 0
        self._subscribed = []

    async def _replace_rejected_token(self, token: str) -> None:
        try:
            if isinstance(self._token_store, AsyncRotatingTokenStore):
                await self._token_store.areplace_rejected(token, self._issue_token)
            else:
                async def toss_issue() -> TossIssuedToken:
                    issued = await self._issue_token()
                    return TossIssuedToken(issued.access_token, issued.expires_in_seconds)

                await self._token_store.areplace_rejected(token, toss_issue)
        except (TokenStoreFailureError, CanonicalLockTimeout, TokenIssueThrottled, TokenStoreLockTimeout, OSError) as exc:
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
        sent = 0
        for symbol, stream in pairs:
            local = self._local_reject(symbol, stream)
            if local is not None:
                acks.append(local)
                continue
            if self._subscribe_spacing_s is not None and sent > 0:
                await asyncio.sleep(self._subscribe_spacing_s)
            sent += 1
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
            ack = VendorAck(symbol, stream, rsp_cd == "00000", rsp_cd)
            acks.append(ack)
            self._note_vendor_rejection(ack, session_proven=any(a.accepted for a in acks))
        self._subscribed.extend((a.symbol, a.stream) for a in acks if a.accepted)
        suspects = self._auth_suspects(acks)
        if suspects and not any(a.accepted for a in acks) and self._all_rejected_is_auth(suspects):
            codes = ",".join(sorted({a.code for a in suspects}))
            if self._token is not None:
                await self._replace_rejected_token(self._token)
            raise VendorAuthRejected(f"auth_rejected:all_acks_rejected:{codes}")
        return acks

    def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
        """Pairs accepted on the current connection (reset on connect)."""
        return tuple(self._subscribed)

    async def subscribe_live(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        """Subscribe while recv() is running; acks are routed through recv(); never reads the socket itself.

        Raises: ValueError when cumulative pairs exceed capacity (nothing sent); VendorDisconnected on send failure;
        VendorAuthRejected when every answered ack is rejected. Ack timeouts return accepted=False, code="timeout".
        """
        async with self._live_lock:
            if len(self._subscribed) + len(pairs) > self.capacity_pairs:
                raise ValueError(
                    f"live subscribe exceeds capacity: {len(self._subscribed)}+{len(pairs)}>{self.capacity_pairs}"
                )
            acks: list[VendorAck] = []
            sent = 0
            for symbol, stream in pairs:
                local = self._local_reject(symbol, stream)
                if local is not None:
                    acks.append(local)
                    continue
                if self._subscribe_spacing_s is not None and sent > 0:
                    await asyncio.sleep(self._subscribe_spacing_s)
                sent += 1
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
                ack = await self._await_live_ack(symbol, stream)
                acks.append(ack)
                if ack.accepted:
                    self._subscribed.append((symbol, stream))
                # A live connection already carries accepted pairs, so a rejection here is pair-specific.
                self._note_vendor_rejection(ack, session_proven=bool(self._subscribed))
            suspects = self._auth_suspects(acks)
            if suspects and not any(a.accepted for a in acks) and self._all_rejected_is_auth(suspects):
                codes = ",".join(sorted({a.code for a in suspects}))
                if self._token is not None:
                    await self._replace_rejected_token(self._token)
                raise VendorAuthRejected(f"auth_rejected:all_acks_rejected:{codes}")
            return acks

    async def _await_live_ack(self, symbol: str, stream: str) -> VendorAck:
        waiter: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._live_waiter = waiter
        try:
            rsp_cd = await asyncio.wait_for(waiter, timeout=self._ack_timeout_s)
        except TimeoutError:
            self._late_acks += 1
            self._logger.warning(
                "[DATA] stage=live_subscribe status=TIMEOUT conn_id=%s symbol=%s stream=%s",
                self._conn_id,
                symbol,
                stream,
            )
            return VendorAck(symbol, stream, False, "timeout")
        finally:
            self._live_waiter = None
        self._check_ack_code(rsp_cd)
        return VendorAck(symbol, stream, rsp_cd == "00000", rsp_cd)

    def _route_live_ack(self, header: dict[str, Any]) -> bool:
        waiter = self._live_waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(str(header["rsp_cd"]))
            return True
        if self._late_acks > 0:
            self._late_acks -= 1
            self._logger.info("[DATA] stage=live_subscribe status=LATE_ACK_IGNORED conn_id=%s", self._conn_id)
            return True
        return False

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
                if "rsp_cd" in header and self._route_live_ack(header):
                    continue
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
