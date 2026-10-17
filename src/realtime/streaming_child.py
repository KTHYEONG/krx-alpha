"""Shared post-bootstrap streaming child lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

from src.core.config import CollectorSettings
from src.core.errors import SlotBudgetExceededError
from src.realtime.contracts import VendorAdapter
from src.realtime.streamer import RealtimeStreamer, SessionFrameSink


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
        capacity = getattr(adapter, "capacity_pairs", None)
        if capacity is not None and len(replay_pairs) > capacity:
            raise SlotBudgetExceededError(f"planned pairs {len(replay_pairs)} exceed capacity {capacity}")
        streamer = streamer_factory(
            adapter=adapter,
            sink=SessionFrameSink(session=session),
            replay_pairs=replay_pairs,
            silence_limit=silence_limit,
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
