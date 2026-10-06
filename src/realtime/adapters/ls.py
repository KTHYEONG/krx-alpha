"""LS증권 실시간 어댑터 (평문 프레임 정규화)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Mapping
from typing import Any

import aiohttp

from src.marketdata.toss_token_store import IssuedToken, TokenStoreLockTimeout, TossTokenStore
from src.realtime.contracts import L0Frame, VendorAck, VendorAuthRejected, VendorDisconnected

logger = logging.getLogger(__name__)

LS_TOKEN_URL = "https://openapi.ls-sec.co.kr:8080/oauth2/token"  # noqa: S105 - public endpoint, not a secret
LS_WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket"

LS_TR_CD: dict[tuple[str, str], str] = {
    ("H0STCNT0", "KOSPI"): "S3_",
    ("H0STCNT0", "KOSDAQ"): "K3_",
    ("H0STASP0", "KOSPI"): "H1_",
    ("H0STASP0", "KOSDAQ"): "HA_",
}

_REV_TR_CD: dict[str, str] = {v: k[0] for k, v in LS_TR_CD.items()}


class LsRealtimeAdapter:
    """LS plain-text realtime adapter.

    Failure contract: every socket write failure (``aiohttp.ClientError``, ``OSError``), malformed
    handshake payload and shared-token-store failure is raised as ``VendorDisconnected`` with a
    ``<kind>:<ExcType>`` detail; credential rejection is ``VendorAuthRejected``. Callers (the streamer)
    therefore never see raw transport or parsing exceptions.
    """

    name: str
    capacity_pairs: int
    unparsable_frames: int

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        http: Any,
        market_of: Mapping[str, str],
        token_store: TossTokenStore,
        streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0"),
        capacity_pairs: int = 200,
        token_url: str = LS_TOKEN_URL,
        ws_url: str = LS_WS_URL,
        heartbeat_s: float = 10.0,
        ack_timeout_s: float = 10.0,
    ) -> None:
        """Bind one LS websocket session to a host-shared token store.

        ``token_store`` is required: whether LS reissuance invalidates live tokens is
        unverified, so every process holding the same app key shares one store
        (single issuer) rather than risk revoking a peer's session, and the adapter
        must not resolve its location from ambient env.
        """
        self.name = "ls"
        self.capacity_pairs = capacity_pairs
        self._app_key = app_key
        self._app_secret = app_secret
        self._http = http
        self._market_of = market_of
        self._streams = streams
        self._token_url = token_url
        self._ws_url = ws_url
        self._heartbeat_s = heartbeat_s
        self._ack_timeout_s = ack_timeout_s
        self._token_store = token_store
        self._ws: Any = None
        self._token: str | None = None
        self._seq = 0
        self._conn_id: str = ""
        self._pending: list[L0Frame] = []  # subscribe 중 끼어든 데이터 프레임 (recv 가 먼저 소진)
        self.unparsable_frames = 0
        self._unparsable_warned = False

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
                status = getattr(resp, 'status', None)
                if 'access_token' not in data:
                    code = str(data.get('error_code', ''))
                    if status in (401, 403) or code:
                        raise VendorAuthRejected(f'auth_rejected:{status}:{code}')
                token = str(data["access_token"])
        except (VendorAuthRejected, VendorDisconnected):
            raise
        except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, OSError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        expires_in = data.get("expires_in") if isinstance(data, dict) else None
        lifetime = float(expires_in) if isinstance(expires_in, (int, float)) and not isinstance(expires_in, bool) and expires_in > 0 else None
        return IssuedToken(token, lifetime)

    async def connect(self) -> None:
        try:
            token = await self._token_store.aget_or_issue(self._issue_token)
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc
        self._token = token
        try:
            self._ws = await (self._http.ws_connect(self._ws_url, heartbeat=self._heartbeat_s)).__aenter__()
        except VendorAuthRejected:
            await self._replace_rejected_token(token)
            raise
        except VendorDisconnected:
            raise
        except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, OSError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._seq = 0
        self._conn_id = f"{self.name}-{time.time_ns()}"
        self._pending = []
        self.unparsable_frames = 0
        self._unparsable_warned = False

    async def _replace_rejected_token(self, token: str) -> None:
        try:
            await self._token_store.areplace_rejected(token, self._issue_token)
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc

    def _build_frame(self, o: dict[str, Any], raw: str) -> L0Frame:
        self._seq += 1
        return L0Frame(
            "ls",
            _REV_TR_CD[o["header"]["tr_cd"]],
            o["header"]["tr_key"],
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._seq,
            conn_id=self._conn_id,
        )

    @staticmethod
    def _is_data_frame(o: dict[str, Any]) -> bool:
        header = o.get("header", {})
        if not isinstance(header, dict):
            return False
        return header.get("tr_cd") in _REV_TR_CD and "tr_key" in header

    def _note_unparsable(self, frame_len: int) -> None:
        self.unparsable_frames += 1
        if not self._unparsable_warned:
            self._unparsable_warned = True
            logger.warning(
                "[DATA] stage=ls_frame status=UNPARSEABLE conn_id=%s frame_len=%d",
                self._conn_id,
                frame_len,
            )

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        """Subscribe symbols and return per-pair vendor acks.

        Raises:
            VendorAuthRejected: ``auth_rejected:all_acks_rejected:<codes>`` when at least one subscription
                was attempted and every ack was rejected. A subscription that yields no data cannot be
                distinguished from an invalid token, so the token is reported rejected to the shared store
                (subject to its rotation cooldown) before raising.
        """
        acks: list[VendorAck] = []
        for symbol, stream in pairs:
            tr_cd = LS_TR_CD[(stream, self._market_of[symbol])]
            try:
                await self._ws.send_str(
                    json.dumps(
                        {
                            "header": {"token": self._token, "tr_type": "3"},
                            "body": {"tr_cd": tr_cd, "tr_key": symbol},
                        }
                    )
                )
            except (aiohttp.ClientError, OSError) as exc:
                if acks or self._token is None:
                    raise VendorDisconnected(f"subscribe_failed:{type(exc).__name__}") from exc
                # LS closes the socket right after connect when the token is expired; no auth error is sent.
                await self._replace_rejected_token(self._token)
                raise VendorAuthRejected(f"auth_rejected:subscribe_reset:{type(exc).__name__}") from exc
            # 구독 응답 대기 중에도 이미 구독된 심볼의 실시간 데이터가 끼어들 수 있어
            # ACK 로 인식될 때까지 프레임을 분류하며 소비한다 (데이터 유실 방지).
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
                    raise VendorDisconnected(f"malformed_ack:{stream}") from exc
                if not isinstance(o, dict):
                    raise VendorDisconnected(f"malformed_ack:{stream}")
                header = o.get("header", {})
                if not isinstance(header, dict):
                    continue
                if header.get("tr_cd") == "PINGPONG":
                    try:
                        await self._ws.send_str(raw)
                    except (aiohttp.ClientError, OSError) as exc:
                        raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc
                    continue
                if self._is_data_frame(o):
                    self._pending.append(self._build_frame(o, raw))
                    continue
                if "rsp_cd" in header:
                    resp = o
                    continue
                # 인식 불가 시스템 프레임: 크래시 대신 스킵.
            rsp_cd = str(resp["header"].get("rsp_cd"))
            acks.append(VendorAck(symbol, stream, resp["header"].get("rsp_cd") == "00000", rsp_cd))
        if acks and not any(a.accepted for a in acks):
            codes = ",".join(sorted({a.code for a in acks}))
            if self._token is not None:
                await self._replace_rejected_token(self._token)
            raise VendorAuthRejected(f"auth_rejected:all_acks_rejected:{codes}")
        return acks

    async def recv(self) -> L0Frame:
        if self._pending:
            return self._pending.pop(0)
        # keepalive 는 재귀가 아닌 루프로 소비한다: 무데이터 구간의 연속 PING 이 스택을 쌓지 않도록.
        while True:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.PING:
                try:
                    await self._ws.pong()
                except (aiohttp.ClientError, OSError) as exc:
                    raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc
                continue
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    o = json.loads(msg.data)
                except ValueError:
                    self._note_unparsable(len(msg.data) if isinstance(msg.data, str) else 0)
                    continue
                if not isinstance(o, dict):
                    self._note_unparsable(len(msg.data) if isinstance(msg.data, str) else 0)
                    continue
                header = o.get("header", {})
                if not isinstance(header, dict):
                    continue
                if header.get("tr_cd") == "PINGPONG":
                    try:
                        await self._ws.send_str(msg.data)
                    except (aiohttp.ClientError, OSError) as exc:
                        raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc
                    continue
                if self._is_data_frame(o):
                    return self._build_frame(o, msg.data)
                # ACK 지연 도착 등 데이터가 아닌 프레임은 스킵하고 계속 수신 (크래시 방지).
                continue
            raise VendorDisconnected(str(msg.type))

    async def aclose(self) -> None:
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            await ws.close()
        except (aiohttp.ClientError, OSError) as exc:
            logger.debug("[DATA] stage=ls_close status=SUPPRESSED error_type=%s", type(exc).__name__)
