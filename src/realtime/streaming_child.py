"""Shared post-bootstrap streaming child lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import aiohttp

from src.core.config import CollectorSettings
from src.core.errors import SlotBudgetExceededError
from src.realtime.connection_handover import Handover, SwapRequest
from src.realtime.contracts import L0Frame, VendorAck, VendorAdapter
from src.realtime.lag_monitor import LagMonitor
from src.realtime.streamer import FrameSink, RealtimeStreamer, SessionFrameSink


@dataclass(frozen=True)
class HandoverSettings:
    request: SwapRequest
    max_s: float
    cooldown_s: float
    window: Callable[[], bool] | None = None
    max_sessions: int | None = None
    stall_lag_s: float | None = None
    refresh_cooldown_s: float = 30.0


@dataclass(frozen=True)
class ClockOverrides:
    ntp_host: str
    max_clock_offset_ns: int


def resolve_clock_overrides(
    ntp_host: object | None, max_clock_offset_ns: object | None, settings: CollectorSettings
) -> ClockOverrides:
    """Resolve explicit CLI values over settings with coercion."""
    raw_host: Any = ntp_host
    raw_offset: Any = max_clock_offset_ns
    return ClockOverrides(
        ntp_host=str(raw_host) if raw_host is not None else str(settings.ntp_host),
        max_clock_offset_ns=int(raw_offset) if raw_offset is not None else int(settings.max_clock_offset_ns),
    )


class FrameObserver(Protocol):
    def observe(self, frame: L0Frame) -> None: ...


class LagMonitoringSink:
    """FrameSink decorator feeding every recorded frame to a `FrameObserver` (e.g. `LagMonitor`)."""

    def __init__(self, inner: FrameSink, monitor: FrameObserver) -> None:
        self._inner = inner
        self._monitor = monitor

    def record(self, frame: L0Frame) -> None:
        self._inner.record(frame)
        self._monitor.observe(frame)

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        self._inner.note_ack(vendor, ack)

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        self._inner.note_gap(symbol, start_ns, end_ns, reason)

    def flush(self) -> int:
        return self._inner.flush()


async def run_streaming_child(
    *,
    session: Any,
    adapter_factory: Callable[[aiohttp.ClientSession], VendorAdapter],
    replay_pairs: list[tuple[str, str]],
    silence_limit: Callable[[], float | None],
    max_cycles: int | None,
    streamer_factory: Callable[..., RealtimeStreamer],
    stop_deadline_ns: int | None = None,
    close_writer: bool = False,
    lag_monitor: LagMonitor | None = None,
    frame_observers: tuple[FrameObserver, ...] = (),
    handover: HandoverSettings | None = None,
) -> None:
    """Drive one bootstrapped session until SIGTERM/deadline/max_cycles, then persist."""
    stop = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    deadline_task: asyncio.Task[None] | None = None
    if stop_deadline_ns is not None:

        async def _deadline_watch() -> None:
            delay_s = (stop_deadline_ns - time.time_ns()) / 1_000_000_000
            if delay_s > 0:
                await asyncio.sleep(delay_s)
            stop.set()

        deadline_task = asyncio.ensure_future(_deadline_watch())
    http: aiohttp.ClientSession | None = None
    try:
        http = aiohttp.ClientSession()
        adapter = adapter_factory(http)
        pairs = list(replay_pairs)
        capacity = getattr(adapter, "capacity_pairs", None)
        if capacity is not None and len(pairs) > capacity:
            raise SlotBudgetExceededError(f"planned pairs {len(pairs)} exceed capacity {capacity}")
        sink: FrameSink = SessionFrameSink(session=session)
        if lag_monitor is not None:
            sink = LagMonitoringSink(sink, lag_monitor)
        for observer in frame_observers:
            sink = LagMonitoringSink(sink, observer)
        extra: dict[str, Any] = {}
        if handover is not None:
            extra["handover"] = Handover(
                adapter_factory=lambda: adapter_factory(http),
                pairs=pairs,
                request=handover.request,
                max_s=handover.max_s,
                cooldown_s=handover.cooldown_s,
                window=handover.window,
                max_sessions=handover.max_sessions,
                stall_lag_s=handover.stall_lag_s,
                refresh_cooldown_s=handover.refresh_cooldown_s,
            )
        streamer = streamer_factory(
            adapter=adapter,
            sink=sink,
            replay_pairs=pairs,
            silence_limit=silence_limit,
            **extra,
        )
        await streamer.run_forever(stop, max_cycles=max_cycles)
    finally:
        if deadline_task is not None:
            deadline_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await deadline_task
        if http is not None:
            await http.close()
    persist = getattr(session, "persist", None)
    if callable(persist):
        persist()
    if close_writer:
        manifest = getattr(session, "manifest", None)
        if manifest is not None and hasattr(manifest, "writer_closed_at_ns"):
            manifest.writer_closed_at_ns = time.time_ns()
        if manifest is not None and callable(persist):
            persist()
