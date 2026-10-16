"""DB증권 실시간 어댑터 (KRX S00 체결, S01 10호가)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import Mapping
from typing import Any, Final

import aiohttp

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

DB_WS_URL: Final = "wss://openapi.dbsec.co.kr:7070/websocket"
DB_TOKEN_URL: Final = "https://openapi.dbsec.co.kr:8443/oauth2/token"  # noqa: S105 - public endpoint, not a secret

DB_TR_CD: Mapping[str, str] = {"H0STCNT0": "S00", "H0STASP0": "S01"}

_REV_TR_CD: dict[str, str] = {v: k for k, v in DB_TR_CD.items()}

_AUTH_REJECT_CODES: frozenset[str] = frozenset({"IGW00121", "IGW00123", "10009"})

_SYMBOL_RE = re.compile(r"[0-9A-Z]{6}")

_EVENT_TIME_FIELD: Mapping[str, str] = {"S00": "StckCntghour", "S01": "BsopHour"}
_HHMMSS_RE = re.compile(r"(?:[01]\d|2[0-3])[0-5]\d[0-5]\d")


class DbRealtimeAdapter:
    """DB Securities KRX-venue realtime adapter (S00 trades, S01 L10 book) behind the VendorAdapter contract."""

    name: str
    capacity_pairs: int
    unparsable_frames: int

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        http: Any,
        token_store: TossTokenStore,
        streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0"),
        capacity_pairs: int = 50,
        token_url: str = DB_TOKEN_URL,
        ws_url: str = DB_WS_URL,
        heartbeat_s: float = 10.0,
        ack_timeout_s: float = 10.0,
        subscribe_spacing_s: float = 0.12,
    ) -> None:
        """Bind one DB websocket session to a host-shared token store."""
        if set(streams) - set(DB_TR_CD):
            raise ValueError(f"unsupported streams: {streams}")
        self.name = "db"
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
        self._conn_id = ""
        self._pending: list[L0Frame] = []
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
                status = getattr(resp, "status", None)
                if "access_token" not in data:
                    code = str(data.get("error_code", ""))
                    if code == "IGW00201":
                        raise VendorDisconnected("token_throttled")
                    if status in (401, 403) or code:
                        raise VendorAuthRejected(f"auth_rejected:{status}:{code}")
                    raise VendorDisconnected(f"connect_failed:token_rejected:{status}:{code}")
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
        try:
            token = await self._token_store.aget_or_issue(self._issue_token)
        except (TokenStoreLockTimeout, OSError) as exc:
            raise VendorDisconnected(f"token_store_failed:{type(exc).__name__}") from exc
        self._token = token
        try:
            self._ws = await (self._http.ws_connect(self._ws_url, heartbeat=self._heartbeat_s)).__aenter__()
        except (TimeoutError, aiohttp.ClientError, ValueError, KeyError, OSError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._seq = 0
        self._conn_id = f"db-{time.time_ns()}"
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
            logger.warning(
                "[DATA] stage=db_frame status=UNPARSEABLE conn_id=%s frame_len=%d",
                self._conn_id,
                frame_len,
            )

    @staticmethod
    def _extract_symbol(o: dict[str, Any]) -> str | None:
        header = o.get("header")
        body = o.get("body")
        if isinstance(body, dict):
            iscd = body.get("ShrnIscd")
            if isinstance(iscd, str):
                match = _SYMBOL_RE.search(iscd)
                if match:
                    return match.group(0)
        if isinstance(header, dict):
            tr_key = header.get("tr_key")
            if isinstance(tr_key, str):
                match = _SYMBOL_RE.search(tr_key.removeprefix("J "))
                if match:
                    return match.group(0)
        return None

    def _build_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None:
        tr_cd = header.get("tr_cd")
        if tr_cd not in _REV_TR_CD or "rsp_cd" in header:
            return None
        symbol = self._extract_symbol(o)
        if symbol is None:
            return None
        body = o.get("body")
        event_time = body.get(_EVENT_TIME_FIELD[str(tr_cd)]) if isinstance(body, dict) else None
        self._seq += 1
        return L0Frame(
            "db",
            _REV_TR_CD[str(tr_cd)],
            symbol,
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._seq,
            conn_id=self._conn_id,
            venue=MarketVenue.KRX,
            session=MarketSession.REGULAR,
            exchange_event_time=event_time if isinstance(event_time, str) and _HHMMSS_RE.fullmatch(event_time) else "",
        )

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        """Subscribe symbols one pair at a time, matching acks by send order."""
        if len(pairs) > self.capacity_pairs:
            raise VendorDisconnected(f"capacity_exceeded:{len(pairs)}>{self.capacity_pairs}")
        acks: list[VendorAck] = []
        for idx, (symbol, stream) in enumerate(pairs):
            if idx:
                await asyncio.sleep(self._subscribe_spacing_s)
            try:
                await self._ws.send_str(
                    json.dumps(
                        {
                            "header": {"token": self._token, "tr_type": "1"},
                            "body": {"tr_cd": DB_TR_CD[stream], "tr_key": f"J {symbol}"},
                        }
                    )
                )
            except (aiohttp.ClientError, OSError) as exc:
                raise VendorDisconnected(f"subscribe_failed:{type(exc).__name__}") from exc
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
                except ValueError:
                    self._note_unparsable(len(raw))
                    continue
                if not isinstance(o, dict):
                    self._note_unparsable(len(raw))
                    continue
                header = o.get("header")
                if not isinstance(header, dict):
                    continue
                if "rsp_cd" in header:
                    resp = o
                    continue
                frame = self._build_frame(o, header, raw)
                if frame is not None:
                    self._pending.append(frame)
            rsp_cd = str(resp["header"].get("rsp_cd"))
            if rsp_cd == "10011":
                raise VendorDisconnected("session_limit")
            acks.append(VendorAck(symbol, stream, rsp_cd == "00000", rsp_cd))
        if acks and not any(a.accepted for a in acks) and all(a.code in _AUTH_REJECT_CODES for a in acks):
            codes = ",".join(sorted({a.code for a in acks}))
            if self._token is not None:
                await self._replace_rejected_token(self._token)
            raise VendorAuthRejected(f"auth_rejected:all_acks_rejected:{codes}")
        return acks

    async def recv(self) -> L0Frame:
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
                header = o.get("header")
                if not isinstance(header, dict):
                    continue
                if "rsp_cd" in header:
                    continue
                frame = self._build_frame(o, header, data)
                if frame is None:
                    if header.get("tr_cd") in _REV_TR_CD:
                        self._note_unparsable(len(data) if isinstance(data, str) else 0)
                    continue
                return frame
            raise VendorDisconnected(str(msg.type))

    async def aclose(self) -> None:
        ws, self._ws = self._ws, None
        if ws is None:
            return
        try:
            await ws.close()
        except (aiohttp.ClientError, OSError) as exc:
            logger.debug("[DATA] stage=db_close status=SUPPRESSED error_type=%s", type(exc).__name__)
