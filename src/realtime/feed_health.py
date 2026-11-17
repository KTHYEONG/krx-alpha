"""Per-child feed health: rolling counters published as `feed-health.v1` for the daemon to read."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import pathlib
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from src.realtime.contracts import L0Frame, VendorAck
from src.realtime.lag_monitor import delivery_lag_s

logger = logging.getLogger(__name__)

SCHEMA = "feed-health.v1"
LAG_MIN_SAMPLES = 20
_WARN_EVERY_S = 60.0
_NS = 1_000_000_000


def feed_id_for(vendor: str, venue: str, shard_index: int, slot: str | None = None) -> str:
    """Return the stable feed id for a streaming child; `slot` selects the KIS dynamic feed."""
    if slot is not None:
        if vendor != "kis":
            raise ValueError(f"dynamic slot is only defined for kis, got {vendor!r}")
        return f"kis:{slot}"
    if vendor == "kis":
        if venue not in ("krx", "nxt"):
            raise ValueError(f"unknown kis venue {venue!r}")
        return f"kis-{venue}:{shard_index}"
    if vendor == "ls" and venue == "nxt":
        return f"ls-nxt:{shard_index}"
    if vendor in ("ls", "db", "kiwoom", "toss"):
        return f"{vendor}:{shard_index}"
    raise ValueError(f"unknown vendor {vendor!r}")


def _nearest_rank(sorted_values: list[float], percent: int) -> float:
    rank = (percent * len(sorted_values) + 99) // 100
    return sorted_values[max(rank, 1) - 1]


class FeedHealthTracker:
    """Accumulates frame, lag, connection and subscription state; O(1) per frame, snapshot on demand."""

    def __init__(
        self,
        *,
        feed_id: str,
        vendor: str,
        venue: str,
        session: str,
        session_date: dt.date,
        pid: int,
        started_wall: float,
        window_s: float = 60.0,
        sample_every: int = 8,
        max_samples: int = 2000,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if window_s <= 0 or sample_every < 1 or max_samples < 1:
            raise ValueError("window_s, sample_every and max_samples must be positive")
        self._feed_id = feed_id
        self._vendor = vendor
        self._venue = venue
        self._session = session
        self._session_date = session_date
        self._pid = pid
        self._started_wall = started_wall
        self._window_s = window_s
        self._sample_every = sample_every
        self._wall_clock = wall_clock
        self._seen = 0
        self._frames_total = 0
        self._last_frame_wall: float | None = None
        self._stream_last: dict[str, float] = {}
        self._second_counts: deque[list[int]] = deque()
        self._window_frames = 0
        self._lags: deque[tuple[float, float]] = deque(maxlen=max_samples)
        self._connected = False
        self._connected_wall: float | None = None
        self._disconnects_total = 0
        self._last_disconnect_wall: float | None = None
        self._last_disconnect_reason: str | None = None
        self._planned = 0
        self._accepted = 0
        self._rejected = 0
        self._pending_since: float | None = None

    def observe(self, frame: L0Frame) -> None:
        recv_s = frame.recv_wall_ns / _NS
        self._frames_total += 1
        self._last_frame_wall = recv_s
        self._stream_last[frame.stream] = recv_s
        second = frame.recv_wall_ns // _NS
        if self._second_counts and self._second_counts[-1][0] == second:
            self._second_counts[-1][1] += 1
        else:
            self._second_counts.append([second, 1])
        self._window_frames += 1
        self._drop_seconds_before(second - self._window_s)
        self._seen += 1
        if self._seen % self._sample_every == 0:
            lag = delivery_lag_s(frame)
            if lag is not None:
                self._lags.append((recv_s, lag))

    def note_connected(self, wall: float) -> None:
        self._connected = True
        self._connected_wall = wall

    def note_disconnected(self, wall: float, reason: str) -> None:
        self._connected = False
        self._disconnects_total += 1
        self._last_disconnect_wall = wall
        self._last_disconnect_reason = reason
        # Replayed subscriptions after reconnect re-report every pair; counts must describe the current connection only.
        self._planned = 0
        self._accepted = 0
        self._rejected = 0
        self._pending_since = None

    def note_acks(self, *, planned: int, accepted: int, rejected: int) -> None:
        self._planned += planned
        self._accepted += accepted
        self._rejected += rejected
        if self._planned - self._accepted - self._rejected > 0:
            if self._pending_since is None:
                self._pending_since = self._wall_clock()
        else:
            self._pending_since = None

    on_connected = note_connected
    on_disconnected = note_disconnected
    on_acks = note_acks

    def snapshot(self, now_wall: float) -> dict[str, Any]:
        self._drop_seconds_before(int(now_wall - self._window_s))
        lag_cutoff = now_wall - self._window_s
        while self._lags and self._lags[0][0] < lag_cutoff:
            self._lags.popleft()
        lags = sorted(lag for _, lag in self._lags)
        enough = len(lags) >= LAG_MIN_SAMPLES
        return {
            "schema": SCHEMA,
            "feed_id": self._feed_id,
            "vendor": self._vendor,
            "venue": self._venue,
            "session": self._session,
            "session_date": self._session_date.isoformat(),
            "pid": self._pid,
            "started_wall": self._started_wall,
            "updated_wall": now_wall,
            "connected": self._connected,
            "disconnects_total": self._disconnects_total,
            "last_disconnect_wall": self._last_disconnect_wall,
            "last_disconnect_reason": self._last_disconnect_reason,
            "frames_total": self._frames_total,
            "last_frame_wall": self._last_frame_wall,
            "stream_last_frame_wall": dict(self._stream_last),
            "window_s": self._window_s,
            "window_frames": self._window_frames,
            "lag_p50_s": _nearest_rank(lags, 50) if enough else None,
            "lag_p99_s": _nearest_rank(lags, 99) if enough else None,
            "lag_samples": len(lags),
            "planned_pairs": self._planned,
            "accepted_pairs": self._accepted,
            "rejected_pairs": self._rejected,
            "pairs_pending_since_wall": self._pending_since,
        }

    def _drop_seconds_before(self, cutoff: float) -> None:
        while self._second_counts and self._second_counts[0][0] < cutoff:
            self._window_frames -= self._second_counts.popleft()[1]


class _InnerSink(Protocol):
    def record(self, frame: L0Frame) -> None: ...
    def note_ack(self, vendor: str, ack: VendorAck) -> None: ...
    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None: ...
    def flush(self) -> int: ...


class FeedHealthSink:
    """FrameSink decorator: the inner sink runs first and tracker failures never reach the data path."""

    def __init__(
        self,
        inner: _InnerSink,
        tracker: FeedHealthTracker,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._inner = inner
        self._tracker = tracker
        self._monotonic = monotonic
        self._failures = 0
        self._last_warn: float | None = None

    def record(self, frame: L0Frame) -> None:
        self._inner.record(frame)
        try:
            self._tracker.observe(frame)
        except Exception as exc:  # noqa: BLE001 - health accounting must never break the receive loop
            self._failures += 1
            now = self._monotonic()
            if self._last_warn is None or now - self._last_warn >= _WARN_EVERY_S:
                self._last_warn = now
                logger.warning(
                    "[SYS] stage=feed_health status=OBSERVE_FAIL error_type=%s failures=%d",
                    type(exc).__name__,
                    self._failures,
                )

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        self._inner.note_ack(vendor, ack)

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        self._inner.note_gap(symbol, start_ns, end_ns, reason)

    def flush(self) -> int:
        return self._inner.flush()


def write_feed_health(path: pathlib.Path, payload: dict[str, Any]) -> None:
    """Atomically replace `path` with `payload` as sorted JSON; raises OSError on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


class FeedHealthPublisher:
    """Writes the tracker snapshot every `interval_s`; a write failure is logged at most once a minute and retried."""

    def __init__(
        self,
        tracker: FeedHealthTracker,
        path: pathlib.Path,
        *,
        interval_s: float,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self._tracker = tracker
        self._path = path
        self._interval_s = interval_s
        self._wall_clock = wall_clock
        self._last_warn: float | None = None

    async def run(self) -> None:
        try:
            while True:
                self._publish(self._tracker.snapshot(self._wall_clock()))
                await asyncio.sleep(self._interval_s)
        except asyncio.CancelledError:
            final = self._tracker.snapshot(self._wall_clock())
            final["connected"] = False
            self._publish(final)
            raise

    def _publish(self, payload: dict[str, Any]) -> None:
        try:
            write_feed_health(self._path, payload)
        except OSError as exc:
            now = self._wall_clock()
            if self._last_warn is None or now - self._last_warn >= _WARN_EVERY_S:
                self._last_warn = now
                logger.warning(
                    "[SYS] stage=feed_health status=WRITE_FAIL error_type=%s path=%s",
                    type(exc).__name__,
                    self._path.name,
                )


@dataclass(frozen=True)
class FeedHealthConfig:
    """Child-side health publishing: the tracker that observes the child, its file, and the write cadence."""

    tracker: FeedHealthTracker
    path: pathlib.Path
    interval_s: float


def build_feed_health_tracker(
    *, feed_id: str, vendor: str, venue: str, session: str, session_date: dt.date
) -> FeedHealthTracker:
    return FeedHealthTracker(
        feed_id=feed_id,
        vendor=vendor,
        venue=venue,
        session=session,
        session_date=session_date,
        pid=os.getpid(),
        started_wall=time.time(),
    )
