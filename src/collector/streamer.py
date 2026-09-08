"""실시간 스트리머 코어 루프 (재접속/replay/gap/flush)."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Any, Protocol

from src.collector.session import CollectorSession
from src.collector.vendor import L0Frame, VendorAck, VendorAdapter, VendorDisconnected


class FrameSink(Protocol):
    def record(self, frame: L0Frame) -> None: ...
    def note_ack(self, vendor: str, ack: VendorAck) -> None: ...
    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None: ...
    def flush(self) -> int: ...


@dataclass
class SessionFrameSink:
    session: CollectorSession

    def record(self, frame: L0Frame) -> None:
        self.session.record_frame(
            vendor=frame.vendor,
            stream=frame.stream,
            raw=frame.raw,
            recv_mono_ns=frame.recv_mono_ns,
            recv_wall_ns=frame.recv_wall_ns,
            conn_id=frame.vendor,
            conn_seq=frame.conn_seq,
        )

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        self.session.note_ack(
            vendor=vendor, tr_id=ack.stream, symbol=ack.symbol, rt_cd=ack.code, accepted=ack.accepted
        )

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        self.session.note_gap(symbol=symbol, gap_start_ns=start_ns, gap_end_ns=end_ns, reason=reason)

    def flush(self) -> int:
        return self.session.flush_journals()


class RealtimeStreamer:
    def __init__(
        self,
        *,
        adapter: VendorAdapter,
        sink: FrameSink,
        replay_pairs: list[tuple[str, str]],
        flush_every: int = 200,
    ) -> None:
        self._adapter = adapter
        self._sink = sink
        self._replay_pairs = replay_pairs
        self._flush_every = flush_every

    async def pump(self, stop: asyncio.Event) -> str:
        try:
            await self._adapter.connect()
            acks = await self._adapter.subscribe(self._replay_pairs)
            for ack in acks:
                self._sink.note_ack(self._adapter.name, ack)
            n = 0
            while not stop.is_set():
                recv_task = asyncio.ensure_future(self._adapter.recv())
                stop_task = asyncio.ensure_future(stop.wait())
                done, pending = await asyncio.wait(
                    {recv_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for p in pending:
                    p.cancel()
                for p in pending:
                    with contextlib.suppress(asyncio.CancelledError):
                        await p
                if recv_task in done:
                    try:
                        frame = recv_task.result()
                    except VendorDisconnected:
                        self._sink.flush()
                        return "disconnect"
                    self._sink.record(frame)
                    n += 1
                    if n % self._flush_every == 0:
                        self._sink.flush()
                    continue
            self._sink.flush()
            return "stopped"
        finally:
            await self._adapter.aclose()

    async def run_forever(
        self,
        stop: asyncio.Event,
        *,
        max_cycles: int | None = None,
        backoff_s: float = 1.0,
        sleep: Any = None,
    ) -> None:
        cycle = 0
        while not stop.is_set():
            start_ns = time.time_ns()
            reason = await self.pump(stop)
            end_ns = time.time_ns()
            for sym in sorted({s for s, _ in self._replay_pairs}):
                self._sink.note_gap(sym, start_ns, end_ns, reason)
            cycle += 1
            if (max_cycles is not None and cycle >= max_cycles) or stop.is_set():
                break
            await (sleep or asyncio.sleep)(backoff_s)
