"""KIS 실시간 어댑터 (KRX 애프터 / NXT venue 분리)."""

from __future__ import annotations

import json
import time
from typing import Any

from src.realtime.contracts import (
    L0Frame,
    MarketVenue,
    VendorAck,
    VendorAuthRejected,
    VendorDisconnected,
)
from src.realtime.kis_lease import KisWebSocketLease
from src.realtime.session import StreamRoute

KIS_WS_URL = "ws://ops.koreainvestment.com:21000"
KIS_APPROVAL_URL = "https://openapi.koreainvestment.com:9443/oauth2/Approval"

KRX_STREAMS: tuple[str, str] = ("H0STCNT0", "H0STASP0")
NXT_STREAMS: tuple[str, str] = ("H0NXCNT0", "H0NXASP0")


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

    async def connect(self) -> None:  # pragma: no cover - live KIS approval/WebSocket boundary (G0-gated, needs production credentials)
        if self._lease is not None:
            await self._lease.acquire()
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
        self._seq = 0
        self._conn_id = f"kis-{time.time_ns()}"
        self._pending = []

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
            raw = await self._ws.receive_str()
            ack = self._parse_ack(raw, symbol, stream)
            acks.append(ack)
        return acks

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
        if self._pending:
            return self._pending.pop(0)
        if self._conn_id == "":
            self._conn_id = f"kis-{time.time_ns()}"
        raw = await self._ws.receive_str()
        if "|" not in raw: raise VendorDisconnected(f"unknown_frame:{raw[:32]}")  # noqa: E701
        frames = self._parse_envelope(raw)
        self._pending.extend(frames[1:])
        return frames[0]

    async def aclose(self) -> None:  # pragma: no cover - live WebSocket close path
        if self._ws is not None:
            await self._ws.close()
        if self._lease is not None:
            await self._lease.release()
