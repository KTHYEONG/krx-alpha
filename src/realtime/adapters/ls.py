"""LS증권 실시간 어댑터 (평문 프레임 정규화)."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from typing import Any

import aiohttp

from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected

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
    name: str
    capacity_pairs: int

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        http: Any,
        market_of: Mapping[str, str],
        streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0"),
        capacity_pairs: int = 200,
        token_url: str = LS_TOKEN_URL,
        ws_url: str = LS_WS_URL,
    ) -> None:
        self.name = "ls"
        self.capacity_pairs = capacity_pairs
        self._app_key = app_key
        self._app_secret = app_secret
        self._http = http
        self._market_of = market_of
        self._streams = streams
        self._token_url = token_url
        self._ws_url = ws_url
        self._ws: Any = None
        self._token: str | None = None
        self._seq = 0
        self._conn_id: str = ""
        self._pending: list[L0Frame] = []  # subscribe 중 끼어든 데이터 프레임 (recv 가 먼저 소진)

    async def connect(self) -> None:
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
        self._token = str(data["access_token"])
        self._ws = await (self._http.ws_connect(self._ws_url)).__aenter__()
        self._seq = 0
        self._conn_id = f"{self.name}-{time.time_ns()}"
        self._pending = []

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
        return header.get("tr_cd") in _REV_TR_CD and "tr_key" in header

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        acks: list[VendorAck] = []
        for symbol, stream in pairs:
            tr_cd = LS_TR_CD[(stream, self._market_of[symbol])]
            await self._ws.send_str(
                json.dumps(
                    {
                        "header": {"token": self._token, "tr_type": "3"},
                        "body": {"tr_cd": tr_cd, "tr_key": symbol},
                    }
                )
            )
            # 구독 응답 대기 중에도 이미 구독된 심볼의 실시간 데이터가 끼어들 수 있어
            # ACK 로 인식될 때까지 프레임을 분류하며 소비한다 (데이터 유실 방지).
            resp: dict[str, Any] | None = None
            while resp is None:
                raw = await self._ws.receive_str()
                o = json.loads(raw)
                header = o.get("header", {})
                if header.get("tr_cd") == "PINGPONG":
                    await self._ws.send_str(raw)
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
        return acks

    async def recv(self) -> L0Frame:
        if self._pending:
            return self._pending.pop(0)
        # keepalive 는 재귀가 아닌 루프로 소비한다: 무데이터 구간의 연속 PING 이 스택을 쌓지 않도록.
        while True:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.PING:
                await self._ws.pong()
                continue
            if msg.type == aiohttp.WSMsgType.TEXT:
                o = json.loads(msg.data)
                header = o.get("header", {})
                if header.get("tr_cd") == "PINGPONG":
                    await self._ws.send_str(msg.data)
                    continue
                if self._is_data_frame(o):
                    return self._build_frame(o, msg.data)
                # ACK 지연 도착 등 데이터가 아닌 프레임은 스킵하고 계속 수신 (크래시 방지).
                continue
            raise VendorDisconnected(str(msg.type))

    async def aclose(self) -> None:
        if self._ws is not None:
            await self._ws.close()
