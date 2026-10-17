"""DB증권 실시간 어댑터 (KRX S00 체결, S01 10호가)."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Any, Final

from src.marketdata.toss_token_store import TossTokenStore
from src.realtime.adapters.json_ws import JsonWsOAuthAdapter
from src.realtime.contracts import (
    L0Frame,
    MarketSession,
    MarketVenue,
    VendorAck,
    VendorAuthRejected,
    VendorDisconnected,
)

DB_WS_URL: Final = "wss://openapi.dbsec.co.kr:7070/websocket"
DB_TOKEN_URL: Final = "https://openapi.dbsec.co.kr:8443/oauth2/token"  # noqa: S105 - public endpoint, not a secret

DB_TR_CD: Mapping[str, str] = {"H0STCNT0": "S00", "H0STASP0": "S01"}

_REV_TR_CD: dict[str, str] = {v: k for k, v in DB_TR_CD.items()}

_AUTH_REJECT_CODES: frozenset[str] = frozenset({"IGW00121", "IGW00123", "10009"})

_SYMBOL_RE = re.compile(r"[0-9A-Z]{6}")

_EVENT_TIME_FIELD: Mapping[str, str] = {"S00": "StckCntghour", "S01": "BsopHour"}
_HHMMSS_RE = re.compile(r"(?:[01]\d|2[0-3])[0-5]\d[0-5]\d")


class DbRealtimeAdapter(JsonWsOAuthAdapter):
    """DB Securities KRX-venue realtime adapter (S00 trades, S01 L10 book) behind the VendorAdapter contract."""

    VENDOR = "db"
    UNPARSABLE_LOG = "[DATA] stage=db_frame status=UNPARSEABLE conn_id=%s frame_len=%d"
    CLOSE_LOG = "[DATA] stage=db_close status=SUPPRESSED error_type=%s"
    SUBSCRIBE_TR_TYPE = "1"
    MALFORMED_ACK_FATAL = False
    ECHO_PINGPONG = False
    REPLACE_TOKEN_ON_WS_AUTH_REJECT = False

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
        super().__init__(
            app_key=app_key,
            app_secret=app_secret,
            http=http,
            token_store=token_store,
            streams=streams,
            capacity_pairs=capacity_pairs,
            token_url=token_url,
            ws_url=ws_url,
            heartbeat_s=heartbeat_s,
            ack_timeout_s=ack_timeout_s,
            subscribe_spacing_s=subscribe_spacing_s,
        )

    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]:
        return {"tr_cd": DB_TR_CD[stream], "tr_key": f"J {symbol}"}

    def _data_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None:
        return self._build_frame(o, header, raw)

    def _token_failure(self, status: int | None, code: str) -> VendorDisconnected | None:
        if code == "IGW00201":
            return VendorDisconnected("token_throttled")
        if status in (401, 403) or code:
            return VendorAuthRejected(f"auth_rejected:{status}:{code}")
        return VendorDisconnected(f"connect_failed:token_rejected:{status}:{code}")

    def _check_capacity(self, pairs: list[tuple[str, str]]) -> None:
        if len(pairs) > self.capacity_pairs:
            raise VendorDisconnected(f"capacity_exceeded:{len(pairs)}>{self.capacity_pairs}")

    def _check_ack_code(self, rsp_cd: str) -> None:
        if rsp_cd == "10011":
            raise VendorDisconnected("session_limit")

    def _all_rejected_is_auth(self, acks: list[VendorAck]) -> bool:
        return all(a.code in _AUTH_REJECT_CODES for a in acks)

    def _counts_as_unparsable(self, header: dict[str, Any]) -> bool:
        return "rsp_cd" not in header and header.get("tr_cd") in _REV_TR_CD

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
        return L0Frame(
            "db",
            _REV_TR_CD[str(tr_cd)],
            symbol,
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._next_seq(),
            conn_id=self._conn_id,
            venue=MarketVenue.KRX,
            session=MarketSession.REGULAR,
            exchange_event_time=event_time if isinstance(event_time, str) and _HHMMSS_RE.fullmatch(event_time) else "",
        )
