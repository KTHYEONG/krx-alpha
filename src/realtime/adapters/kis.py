"""KIS 실시간 어댑터 (KRX 애프터 / NXT venue 분리)."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import aiohttp

from src.realtime.contracts import (
    L0Frame,
    MarketVenue,
    VendorAck,
    VendorAuthRejected,
    VendorDisconnected,
)
from src.realtime.kis_lease import KisWebSocketLease
from src.realtime.session import StreamRoute

logger = logging.getLogger(__name__)

KIS_WS_URL = "ws://ops.koreainvestment.com:21000"
KIS_APPROVAL_URL = "https://openapi.koreainvestment.com:9443/oauth2/Approval"

KRX_STREAMS: tuple[str, str] = ("H0STCNT0", "H0STASP0")
NXT_STREAMS: tuple[str, str] = ("H0NXCNT0", "H0NXASP0")


def _is_pingpong(raw: str) -> bool:
    """Return True when ``raw`` is a KIS application-level heartbeat frame.

    KIS sends ``{"header": {"tr_id": "PINGPONG", ...}}`` on idle sockets and
    drops the session unless the identical text is echoed back. Only JSON
    objects whose ``header.tr_id`` equals ``"PINGPONG"`` qualify; non-JSON,
    non-object or header-less payloads return False so callers keep their
    fail-closed handling of unknown frames.
    """
    try:
        obj = json.loads(raw)
    except ValueError:
        return False
    if not isinstance(obj, dict):
        return False
    header = obj.get("header")
    if not isinstance(header, dict):
        return False
    return str(header.get("tr_id", "")) == "PINGPONG"


def _expected_streams(route: StreamRoute) -> tuple[str, str]:
    return NXT_STREAMS if route.venue == MarketVenue.NXT else KRX_STREAMS


class KisRealtimeAdapter:
    name: str
    capacity_pairs: int

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        http: Any,
        route: StreamRoute,
        allowed_streams: tuple[str, str],
        capacity_pairs: int,
        lease: KisWebSocketLease | None = None,
    ) -> None:
        expected = _expected_streams(route)
        if tuple(allowed_streams) != expected:
            raise VendorDisconnected(f"route_mismatch:{route.venue.value}:{','.join(allowed_streams)}")
        if not app_key or not app_secret: raise VendorAuthRejected("auth_rejected:missing_credentials")  # noqa: E701
        self.name = "kis"
        self.capacity_pairs = capacity_pairs
        self._app_key = app_key
        self._app_secret = app_secret
        self._http = http
        self._route = route
        self._allowed = tuple(allowed_streams)
        self._lease = lease
        self._ws: Any = None
        self._approval_key: str | None = None
        self._seq = 0
        self._conn_id = ""
        self._pending: list[L0Frame] = []
        self.pingpong_count: int = 0
        self.last_pingpong_wall_ns: int | None = None

    async def connect(self) -> None:
        if self._lease is not None:
            await self._lease.acquire()
        try:
            async with self._http.post(
                KIS_APPROVAL_URL,
                json={"grant_type": "client_credentials", "appkey": self._app_key, "secretkey": self._app_secret},
            ) as resp:
                data = await resp.json()
                status = getattr(resp, "status", 200)
                if status != 200:
                    raise VendorAuthRejected(f"auth_rejected:{status}")
                approval = data.get("approval_key") if isinstance(data, dict) else None
                if not approval:
                    raise VendorAuthRejected(f"auth_rejected:{status}:no_approval_key")
                self._approval_key = str(approval)
            self._ws = await self._http.ws_connect(KIS_WS_URL)
        except (VendorAuthRejected, VendorDisconnected):
            raise
        except (aiohttp.ClientError, TimeoutError, OSError, ValueError) as exc:
            raise VendorDisconnected(f"connect_failed:{type(exc).__name__}") from exc
        self._seq = 0
        self._conn_id = f"kis-{time.time_ns()}"
        self._pending = []
        self.pingpong_count = 0
        self.last_pingpong_wall_ns = None

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:  # pragma: no cover - live KIS subscribe/ACK boundary (G0-gated)
        if len(pairs) > self.capacity_pairs:
            raise VendorDisconnected(f"capacity_exceeded:{len(pairs)}>{self.capacity_pairs}")
        acks: list[VendorAck] = []
        for symbol, stream in pairs:
            if stream not in self._allowed:
                raise VendorDisconnected(f"route_mismatch:{stream}")
            await self._ws.send_str(
                json.dumps({
                    "header": {"approval_key": self._approval_key, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                    "body": {"input": {"tr_id": stream, "tr_key": symbol}},
                })
            )
            ack = await self._await_subscribe_ack(symbol, stream)
            acks.append(ack)
        return acks

    async def _await_subscribe_ack(self, symbol: str, stream: str) -> VendorAck:  # pragma: no cover - exercised only via live subscribe path
        # 40개 심볼을 순차 구독하는 동안 이미 등록된 심볼의 실시간 틱("0|"/"1|" 접두)이나
        # PINGPONG 하트비트가 다음 ACK보다 먼저 도착할 수 있다(실측 확인). 이를 이번 요청의
        # ACK으로 오인해 json.loads 에 실패시키지 않도록, ACK이 아닌 프레임은 건너뛰며
        # 실시간 틱은 recv() 가 나중에 소비하도록 큐에 적재한다.
        while True:
            raw = await self._ws.receive_str()
            if raw[:1] in ("0", "1") and "|" in raw:
                self._pending.extend(self._parse_envelope(raw))
                continue
            if _is_pingpong(raw):
                await self._echo_pingpong(raw)
                continue
            return self._parse_ack(raw, symbol, stream)

    async def _echo_pingpong(self, raw: str) -> None:
        """Echo a heartbeat frame verbatim.

        Records receipt (count, wall time) before echoing, so a failed echo is
        still visible in telemetry. Emits one DEBUG
        ``[DATA] stage=kis_pingpong conn_id=<id> count=<n>`` per heartbeat.

        Raises:
            VendorDisconnected: ``pingpong_echo_failed:<ExcType>`` when the socket
                write fails (``OSError``, incl. aiohttp
                ``ClientConnectionResetError``), so the streamer takes its normal
                reconnect path instead of crashing on a raw transport error.
        """
        self.pingpong_count += 1
        self.last_pingpong_wall_ns = time.time_ns()
        logger.debug(
            "[DATA] stage=kis_pingpong conn_id=%s count=%d", self._conn_id, self.pingpong_count
        )
        try:
            await self._ws.send_str(raw)
        except OSError as exc:
            raise VendorDisconnected(f"pingpong_echo_failed:{type(exc).__name__}") from exc

    def _parse_ack(self, raw: str, symbol: str, stream: str) -> VendorAck:  # pragma: no cover - exercised only via live subscribe path
        try:
            o = json.loads(raw)
        except ValueError as exc:
            raise VendorDisconnected(f"malformed_ack:{stream}") from exc
        header = o.get("header", {}) if isinstance(o, dict) else {}
        body = o.get("body", {}) if isinstance(o, dict) else {}
        result = body if isinstance(body, dict) else header
        rt_cd = str(result.get("rt_cd", result.get("rsp_cd", "")))
        if rt_cd in ("", "null", "None"):
            raise VendorDisconnected(f"malformed_ack:{stream}")
        msg_cd = str(result.get("msg_cd", ""))
        if rt_cd != "0" and msg_cd.startswith("IGW"):
            raise VendorAuthRejected(f"auth_rejected:{rt_cd}:{msg_cd}")
        return VendorAck(symbol, stream, rt_cd == "0", rt_cd)

    def _parse_envelope(self, raw: str) -> list[L0Frame]:
        parts = raw.split("|", 3)
        if len(parts) != 4: raise VendorDisconnected(f"unknown_frame:{raw[:32]}")  # noqa: E701
        _flag, tr_id, count_s, body = parts
        if tr_id not in self._allowed: raise VendorDisconnected(f"route_mismatch:{tr_id}")  # noqa: E701
        try:
            count = int(count_s)
        except ValueError as exc:
            raise VendorDisconnected(f"malformed_count:{tr_id}") from exc
        if count <= 0:
            raise VendorDisconnected(f"malformed_count:{tr_id}")
        fields = body.split("^")
        if len(fields) % count != 0:
            raise VendorDisconnected(f"malformed_rows:{tr_id}")
        width = len(fields) // count
        if width < 2:
            raise VendorDisconnected(f"malformed_rows:{tr_id}")
        frames: list[L0Frame] = []
        for i in range(count):
            row = "^".join(fields[i * width : (i + 1) * width])
            row_fields = row.split("^")
            if len(row_fields) < 2: raise VendorDisconnected(f"malformed_row:{tr_id}")  # noqa: E701
            self._seq += 1
            frames.append(
                L0Frame(
                    vendor="kis",
                    stream=tr_id,
                    symbol=row_fields[0],
                    raw=row,
                    recv_mono_ns=time.monotonic_ns(),
                    recv_wall_ns=time.time_ns(),
                    conn_seq=self._seq,
                    conn_id=self._conn_id,
                    venue=self._route.venue,
                    session=self._route.session,
                    exchange_event_time=row_fields[1],
                )
            )
        return frames

    async def recv(self) -> L0Frame:
        """Return the next market-data frame of this connection.

        Heartbeats are answered and consumed inside the call; they never reach
        the pending queue, never advance ``conn_seq`` and are never recorded to
        L0. Any other frame without the ``|`` envelope still raises
        ``VendorDisconnected("unknown_frame:<first 32 chars>")`` (fail-closed).
        """
        if self._pending:
            return self._pending.pop(0)
        if self._conn_id == "":
            self._conn_id = f"kis-{time.time_ns()}"
        while True:
            raw = await self._ws.receive_str()
            if "|" not in raw:
                if _is_pingpong(raw):
                    await self._echo_pingpong(raw)
                    continue
                raise VendorDisconnected(f"unknown_frame:{raw[:32]}")
            frames = self._parse_envelope(raw)
            self._pending.extend(frames[1:])
            return frames[0]

    async def aclose(self) -> None:
        ws, self._ws = self._ws, None
        try:
            if ws is not None:
                try:
                    await ws.close()
                except (aiohttp.ClientError, OSError) as exc:
                    logger.debug("[DATA] stage=kis_close status=SUPPRESSED error_type=%s", type(exc).__name__)
        finally:
            if self._lease is not None:
                await self._lease.release()
