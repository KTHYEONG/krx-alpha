"""LS증권 실시간 어댑터 (평문 프레임 정규화)."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from src.core.config import LS_MARKET_STATE_PAIRS, LS_MAX_SUBSCRIPTIONS_PER_SESSION
from src.marketdata.host_token_store import AsyncRotatingTokenStore
from src.marketdata.toss_token_store import TossTokenStore
from src.realtime.adapters.json_ws import JsonWsOAuthAdapter, RejectedPairHold
from src.realtime.contracts import (
    L0Frame,
    MarketSession,
    MarketVenue,
    VendorAck,
    VendorAuthRejected,
    VendorDisconnected,
)

LS_TOKEN_URL = "https://openapi.ls-sec.co.kr:8080/oauth2/token"  # noqa: S105 - public endpoint, not a secret
LS_WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket"

LS_TR_CD: dict[tuple[str, str], str] = {
    ("H0STCNT0", "KOSPI"): "S3_",
    ("H0STCNT0", "KOSDAQ"): "K3_",
    ("H0STASP0", "KOSPI"): "H1_",
    ("H0STASP0", "KOSDAQ"): "HA_",
}

_REV_TR_CD: dict[str, str] = {v: k[0] for k, v in LS_TR_CD.items()}
_MARKET_TR_BY_STREAM: dict[str, str] = {"LS_JIF": "JIF", "LS_VI": "VI_"}
_MARKET_STREAM_BY_TR: dict[str, str] = {tr: stream for stream, tr in _MARKET_TR_BY_STREAM.items()}
_MARKET_PAIRS: frozenset[tuple[str, str]] = frozenset(LS_MARKET_STATE_PAIRS)

LS_NXT_STREAMS: tuple[str, str] = ("H0NXCNT0", "H0NXASP0")
_NXT_TR_BY_STREAM: dict[str, str] = {"H0NXCNT0": "NS3", "H0NXASP0": "NH1"}
_NXT_STREAM_BY_TR: dict[str, str] = {tr: stream for stream, tr in _NXT_TR_BY_STREAM.items()}
# KRX short codes are six characters; newer listings mix capital letters into digits (e.g. 0088M0).
_KRX_SHORT_CODE = re.compile(r"[0-9A-Z]{6}")
# LS rejects a malformed tr_key with 10009 ("tr_key의 길이를 확인해주세요", 2026-11-23 probe), then closes the socket.
LS_PAIR_REJECT_CODES: frozenset[str] = frozenset({"10009"})
LS_AFTER_KRX_STREAMS: tuple[str, str] = ("H0STCNT0", "H0STASP0")


def _nxt_invalid_key(symbol: str, stream: str) -> str | None:
    if _KRX_SHORT_CODE.fullmatch(symbol) is None:
        return "invalid_tr_key"
    if stream not in _NXT_TR_BY_STREAM:
        return "unknown_stream"
    return None


def _nxt_subscribe_body(symbol: str, stream: str) -> dict[str, str]:
    return {"tr_cd": _NXT_TR_BY_STREAM[stream], "tr_key": f"N{symbol}   "}


def _nxt_frame(
    header: dict[str, Any],
    raw: str,
    next_seq: Callable[[], int],
    conn_id: str,
    session: MarketSession,
) -> L0Frame | None:
    if "rsp_cd" in header or header.get("tr_cd") not in _NXT_STREAM_BY_TR or "tr_key" not in header:
        return None
    key = str(header["tr_key"]).strip()
    return L0Frame(
        "ls",
        _NXT_STREAM_BY_TR[str(header["tr_cd"])],
        key[1:] if key[:1] in ("N", "U") else key,
        raw,
        time.monotonic_ns(),
        time.time_ns(),
        next_seq(),
        conn_id=conn_id,
        venue=MarketVenue.NXT,
        session=session,
    )


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
        token_store: TossTokenStore | AsyncRotatingTokenStore,
        streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0"),
        capacity_pairs: int = LS_MAX_SUBSCRIPTIONS_PER_SESSION,
        token_url: str = LS_TOKEN_URL,
        ws_url: str = LS_WS_URL,
        heartbeat_s: float = 10.0,
        ack_timeout_s: float = 10.0,
        rejected_hold: RejectedPairHold | None = None,
    ) -> None:
        """Bind one LS websocket session to a host-shared token store.

        ``token_store`` is required: whether LS reissuance invalidates live tokens is
        unverified, so every process holding the same app key shares one store
        (single issuer) rather than risk revoking a peer's session, and the adapter
        must not resolve its location from ambient env.

        LS answers one rejected subscribe by closing the whole socket, so keys are validated before sending
        and ``rejected_hold`` (shared by every adapter of the process) keeps a rejected pair from being
        resent on each reconnect.
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
            rejected_hold=rejected_hold,
        )

    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]:
        if stream in _MARKET_TR_BY_STREAM:
            return {"tr_cd": _MARKET_TR_BY_STREAM[stream], "tr_key": symbol}
        return {"tr_cd": LS_TR_CD[(stream, self._market_of[symbol])], "tr_key": symbol}

    def _invalid_key(self, symbol: str, stream: str) -> str | None:
        if stream in _MARKET_TR_BY_STREAM:
            return None if (symbol, stream) in _MARKET_PAIRS else "unknown_stream"
        if _KRX_SHORT_CODE.fullmatch(symbol) is None:
            return "invalid_tr_key"
        if (stream, self._market_of.get(symbol)) not in LS_TR_CD:
            return "unknown_market"
        return None

    def _is_pair_rejection(self, code: str) -> bool:
        return code in LS_PAIR_REJECT_CODES

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
        header = o["header"]
        if header["tr_cd"] in _MARKET_STREAM_BY_TR:
            return self._build_market_frame(o, raw)
        return L0Frame(
            "ls",
            _REV_TR_CD[header["tr_cd"]],
            header["tr_key"],
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._next_seq(),
            conn_id=self._conn_id,
        )

    def _build_market_frame(self, o: dict[str, Any], raw: str) -> L0Frame:
        stream = _MARKET_STREAM_BY_TR[o["header"]["tr_cd"]]
        symbol = next(key for key, name in LS_MARKET_STATE_PAIRS if name == stream)
        body = o.get("body")
        event_time = body.get("time", "") if isinstance(body, dict) and stream == "LS_VI" else ""
        return L0Frame(
            "ls",
            stream,
            symbol,
            raw,
            time.monotonic_ns(),
            time.time_ns(),
            self._next_seq(),
            conn_id=self._conn_id,
            venue=MarketVenue.KRX,
            exchange_event_time=event_time if len(str(event_time)) == 6 and str(event_time).isdigit() else "",
        )

    @staticmethod
    def _is_data_frame(o: dict[str, Any]) -> bool:
        header = o.get("header", {})
        if not isinstance(header, dict):
            return False
        if header.get("tr_cd") in _MARKET_STREAM_BY_TR and "rsp_cd" not in header:
            return True
        return header.get("tr_cd") in _REV_TR_CD and "tr_key" in header


class LsNxtRealtimeAdapter(LsRealtimeAdapter):
    """LS adapter for NXT trades (NS3) and the 10-level book (NH1); KRX TR frames are ignored.

    LS keys NXT symbols as ``N<code>`` padded with three spaces; frames carry the logical NXT stream ids.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("streams", LS_NXT_STREAMS)
        kwargs.setdefault("market_of", {})
        super().__init__(**kwargs)

    def _invalid_key(self, symbol: str, stream: str) -> str | None:
        return _nxt_invalid_key(symbol, stream)

    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]:
        return _nxt_subscribe_body(symbol, stream)

    def _data_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None:
        return _nxt_frame(header, raw, self._next_seq, self._conn_id, MarketSession.REGULAR)


class LsAftermarketRealtimeAdapter(LsRealtimeAdapter):
    """One LS connection for KRX aftermarket trades and book (S3_/K3_, H1_/HA_) plus NXT aftermarket (NS3, NH1).

    Frames are stamped with their venue and session from the TR family; market-state TRs are never subscribed here.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("streams", (*LS_AFTER_KRX_STREAMS, *LS_NXT_STREAMS))
        super().__init__(**kwargs)

    def _invalid_key(self, symbol: str, stream: str) -> str | None:
        if stream in _NXT_TR_BY_STREAM:
            return _nxt_invalid_key(symbol, stream)
        if stream in LS_AFTER_KRX_STREAMS:
            return super()._invalid_key(symbol, stream)
        return "unknown_stream"

    def _subscribe_body(self, symbol: str, stream: str) -> dict[str, str]:
        if stream in _NXT_TR_BY_STREAM:
            return _nxt_subscribe_body(symbol, stream)
        return super()._subscribe_body(symbol, stream)

    def _data_frame(self, o: dict[str, Any], header: dict[str, Any], raw: str) -> L0Frame | None:
        nxt = _nxt_frame(header, raw, self._next_seq, self._conn_id, MarketSession.NXT_AFTER)
        if nxt is not None:
            return nxt
        if header.get("tr_cd") in _REV_TR_CD and self._is_data_frame(o):
            return replace(self._build_frame(o, raw), venue=MarketVenue.KRX, session=MarketSession.KRX_AFTER)
        return None
