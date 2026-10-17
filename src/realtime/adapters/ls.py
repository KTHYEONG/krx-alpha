"""LS증권 실시간 어댑터 (평문 프레임 정규화)."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from src.marketdata.toss_token_store import TossTokenStore
from src.realtime.adapters.json_ws import JsonWsOAuthAdapter
from src.realtime.contracts import L0Frame, VendorAck, VendorAuthRejected, VendorDisconnected

LS_TOKEN_URL = "https://openapi.ls-sec.co.kr:8080/oauth2/token"  # noqa: S105 - public endpoint, not a secret
LS_WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket"

LS_TR_CD: dict[tuple[str, str], str] = {
    ("H0STCNT0", "KOSPI"): "S3_",
    ("H0STCNT0", "KOSDAQ"): "K3_",
    ("H0STASP0", "KOSPI"): "H1_",
    ("H0STASP0", "KOSDAQ"): "HA_",
}

_REV_TR_CD: dict[str, str] = {v: k[0] for k, v in LS_TR_CD.items()}


class LsRealtimeAdapter(JsonWsOAuthAdapter):
    """LS plain-text realtime adapter.

    Failure contract: every socket write failure (``aiohttp.ClientError``, ``OSError``), malformed
    handshake payload and shared-token-store failure is raised as ``VendorDisconnected`` with a
    ``<kind>:<ExcType>`` detail; credential rejection is ``VendorAuthRejected``. Callers (the streamer)
    therefore never see raw transport or parsing exceptions.
    """

    VENDOR = "ls"
    UNPARSABLE_LOG = "[DATA] stage=ls_frame status=UNPARSEABLE conn_id=%s frame_len=%d"
    CLOSE_LOG = "[DATA] stage=ls_close status=SUPPRESSED error_type=%s"
    SUBSCRIBE_TR_TYPE = "3"
    MALFORMED_ACK_FATAL = True
    ECHO_PINGPONG = True
    REPLACE_TOKEN_ON_WS_AUTH_REJECT = True

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
        self._market_of = market_of
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
            subscribe_spacing_s=None,
        )

    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]:
        return {"tr_cd": LS_TR_CD[(stream, self._market_of[symbol])], "tr_key": symbol}

    def _data_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None:
        return self._build_frame(o, raw) if self._is_data_frame(o) else None

    def _token_failure(self, status: int | None, code: str) -> VendorDisconnected | None:
        if status in (401, 403) or code:
            return VendorAuthRejected(f"auth_rejected:{status}:{code}")
        return None

    async def _on_subscribe_send_failure(self, acks: list[VendorAck], exc: Exception) -> VendorDisconnected:
        if acks or self._token is None:
            return VendorDisconnected(f"subscribe_failed:{type(exc).__name__}")
        # LS closes the socket right after connect when the token is expired; no auth error is sent.
        await self._replace_rejected_token(self._token)
        return VendorAuthRejected(f"auth_rejected:subscribe_reset:{type(exc).__name__}")

    def _build_frame(self, o: dict[str, Any], raw: str) -> L0Frame:
        return L0Frame(
            "ls",
            _REV_TR_CD[o["header"]["tr_cd"]],
            o["header"]["tr_key"],
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._next_seq(),
            conn_id=self._conn_id,
        )

    @staticmethod
    def _is_data_frame(o: dict[str, Any]) -> bool:
        header = o.get("header", {})
        if not isinstance(header, dict):
            return False
        return header.get("tr_cd") in _REV_TR_CD and "tr_key" in header
