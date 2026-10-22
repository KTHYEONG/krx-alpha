"""Proactive reconnect: open a standby vendor connection, then cut over once the old one has caught up."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.realtime.contracts import L0Frame, VendorAdapter
from src.realtime.lag_monitor import event_second_of

logger = logging.getLogger(__name__)

# Duplicate-suppression memory: only payloads still in flight on the lagging connection matter, so the oldest quarter is dropped beyond this.
_SEEN_CAP = 400_000


class SwapRequest:
    """One-shot flag raised by the lag monitor and consumed by the streamer."""

    def __init__(self) -> None:
        self._requested = False

    def request(self) -> None:
        self._requested = True

    def take(self) -> bool:
        requested, self._requested = self._requested, False
        return requested


@dataclass(frozen=True)
class CutOver:
    adapter: VendorAdapter
    recv_task: asyncio.Task[L0Frame]


@dataclass
class _Standby:
    adapter: VendorAdapter
    recv_task: asyncio.Task[L0Frame]
    started: float
    first_event_s: int | None = None


class Handover:
    """Bring-up and cut-over state machine for one streamer, driven from its receive loop.

    The old connection stays open while a standby subscribes the same pairs, because frames the vendor
    is still delivering late exist only on the old connection. The cut-over happens once the old
    connection's event time has reached the standby's first event, the old connection fails, or
    `max_s` elapsed. A `window` additionally starts a standby ahead of time and keeps both connections live
    until it closes, so a vendor that stalls subscriptions at the open still has a ready connection. While both are open, a frame whose raw payload was already recorded from the
    other connection is dropped, so L0 and downstream partitions never hold the same event twice.
    A standby failure never affects the old connection.
    """

    def __init__(
        self,
        *,
        adapter_factory: Callable[[], VendorAdapter],
        pairs: list[tuple[str, str]],
        request: SwapRequest,
        max_s: float,
        cooldown_s: float,
        window: Callable[[], bool] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_s <= 0 or cooldown_s < 0:
            raise ValueError("max_s must be positive and cooldown_s non-negative")
        self._factory = adapter_factory
        self._pairs = pairs
        self._request = request
        self._max_s = max_s
        self._cooldown_s = cooldown_s
        self._window = window
        self._monotonic = monotonic
        self._opening: asyncio.Task[VendorAdapter | None] | None = None
        self._standby: _Standby | None = None
        self._last_start: float | None = None
        self._seen: dict[int, bool] = {}
        self._primary_event_s = -1
        self._dropped = 0

    @property
    def active(self) -> bool:
        return self._opening is not None or self._standby is not None

    def tasks(self) -> set[asyncio.Future[Any]]:
        """Tasks the receive loop must also wait on."""
        found: set[asyncio.Future[Any]] = set()
        if self._opening is not None:
            found.add(self._opening)
        if self._standby is not None:
            found.add(self._standby.recv_task)
        return found

    def maybe_start(self, *, eligible: bool) -> None:
        """Start a standby when the monitor asked for one or the hot-standby window is open, `eligible`, and the cooldown has passed."""
        requested = self._request.take()
        if not (requested or (self._window is not None and self._window())) or not eligible or self.active:
            return
        now = self._monotonic()
        if self._last_start is not None and now - self._last_start < self._cooldown_s:
            return
        self._last_start = now
        self._opening = asyncio.ensure_future(self._open())
        logger.warning("[DATA] stage=stream_handover status=STARTED")

    async def _open(self) -> VendorAdapter | None:
        adapter: VendorAdapter | None = None
        try:
            adapter = self._factory()
            await adapter.connect()
            acks = await adapter.subscribe(self._pairs)
            if not acks or not all(ack.accepted for ack in acks):
                raise ValueError("standby subscription rejected")
        except asyncio.CancelledError:
            if adapter is not None:
                await self._close(adapter)
            raise
        except Exception as exc:  # noqa: BLE001 - a standby failure must never touch the live connection
            logger.warning("[DATA] stage=stream_handover status=OPEN_FAILED error_type=%s", type(exc).__name__)
            if adapter is not None:
                await self._close(adapter)
            return None
        return adapter

    @staticmethod
    async def _close(adapter: VendorAdapter) -> None:
        with contextlib.suppress(Exception):
            await adapter.aclose()

    def admit(self, frame: L0Frame, *, from_standby: bool) -> bool:
        """Account `frame` and return False when the other connection already recorded the same payload."""
        if self._standby is None:
            return True
        event_s = event_second_of(frame)
        if event_s is not None:
            if from_standby:
                if self._standby.first_event_s is None:
                    self._standby.first_event_s = event_s
            else:
                self._primary_event_s = max(self._primary_event_s, event_s)
        key = hash((frame.stream, frame.symbol, frame.raw))
        if len(self._seen) > _SEEN_CAP:
            for stale in list(itertools.islice(self._seen, _SEEN_CAP // 4)):
                del self._seen[stale]
        owner = self._seen.setdefault(key, from_standby)
        if owner != from_standby:
            self._dropped += 1
            return False
        return True

    async def step(self, done: set[asyncio.Future[Any]], primary_recv: asyncio.Task[L0Frame]) -> CutOver | None:
        """Advance after a wait: adopt a finished standby, drop a failed one, and cut over when due."""
        if self._opening is not None and self._opening in done:
            adapter = self._opening.result()
            self._opening = None
            if adapter is not None:
                self._standby = _Standby(adapter, asyncio.ensure_future(adapter.recv()), self._monotonic())
                self._seen.clear()
                self._primary_event_s = -1
                self._dropped = 0
        standby = self._standby
        if standby is None:
            return None
        if standby.recv_task.done() and standby.recv_task in done and standby.recv_task.exception() is not None:
            logger.warning("[DATA] stage=stream_handover status=STANDBY_LOST")
            await self._drop_standby()
            return None
        if self._window is not None and self._window():
            return None
        caught_up = standby.first_event_s is not None and self._primary_event_s >= standby.first_event_s
        expired = self._monotonic() - standby.started >= self._max_s
        if not (caught_up or expired):
            return None
        primary_recv.cancel()
        await asyncio.gather(primary_recv, return_exceptions=True)
        return await self._cut_over("caught_up" if caught_up else "forced")

    def restart_standby_recv(self) -> None:
        """Re-arm the standby's recv after one of its frames was consumed."""
        if self._standby is not None:
            self._standby.recv_task = asyncio.ensure_future(self._standby.adapter.recv())

    def standby_frame(self, done: set[asyncio.Future[Any]]) -> L0Frame | None:
        """Return the standby's finished frame, if any, without raising."""
        standby = self._standby
        if standby is None or standby.recv_task not in done or standby.recv_task.exception() is not None:
            return None
        return standby.recv_task.result()

    async def on_primary_failure(self) -> CutOver | None:
        """Promote a live standby when the old connection died; None when there is no standby."""
        if self._standby is None:
            return None
        return await self._cut_over("primary_failed")

    async def _cut_over(self, reason: str) -> CutOver:
        standby = self._standby
        assert standby is not None  # noqa: S101 - callers check
        logger.warning(
            "[DATA] stage=stream_handover status=CUT_OVER reason=%s dropped_duplicates=%d", reason, self._dropped
        )
        self._standby = None
        self._seen.clear()
        return CutOver(standby.adapter, standby.recv_task)

    async def _drop_standby(self) -> None:
        standby, self._standby = self._standby, None
        self._seen.clear()
        if standby is not None:
            standby.recv_task.cancel()
            await asyncio.gather(standby.recv_task, return_exceptions=True)
            await self._close(standby.adapter)

    async def aclose(self) -> None:
        """Cancel any bring-up in flight and close the standby; safe to call repeatedly."""
        if self._opening is not None:
            opening, self._opening = self._opening, None
            opening.cancel()
            outcome = (await asyncio.gather(opening, return_exceptions=True))[0]
            if outcome is not None and not isinstance(outcome, BaseException):
                await self._close(outcome)
        await self._drop_standby()
