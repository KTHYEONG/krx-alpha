"""Live delivery-lag watch over received frames (receive wall time minus exchange event time)."""

from __future__ import annotations

import datetime as dt
import logging
import re
import statistics
import time
from collections.abc import Callable
from zoneinfo import ZoneInfo

from src.realtime.contracts import L0Frame

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_EVENT_TIME_RE = re.compile(r'"(?:chetime|hotime)"\s*:\s*"(\d{6})"')
_NS = 1_000_000_000


def event_second_of(frame: L0Frame) -> int | None:
    """Return the frame's exchange event time as second-of-day, or None when absent or malformed."""
    stamp = frame.exchange_event_time
    if len(stamp) != 6 or not stamp.isdigit():
        match = _EVENT_TIME_RE.search(frame.raw)
        if match is None:
            return None
        stamp = match.group(1)
    hour, minute, second = int(stamp[:2]), int(stamp[2:4]), int(stamp[4:])
    if hour > 23 or minute > 59 or second > 59:
        return None
    return hour * 3600 + minute * 60 + second


class LagMonitor:
    """Alerts once per episode when the windowed median delivery lag exceeds `threshold_s`, then calls `on_exceeded`.

    Event times carry one-second resolution, so lag is accurate to about one second. Only every
    `sample_every`-th frame is parsed to keep the receive loop cheap. `observe` never raises.
    """

    def __init__(
        self,
        *,
        threshold_s: float,
        window_s: float = 5.0,
        sample_every: int = 8,
        min_samples: int = 5,
        wall_ns: Callable[[], int] = time.time_ns,
        on_exceeded: Callable[[], None] | None = None,
    ) -> None:
        if threshold_s <= 0 or window_s <= 0:
            raise ValueError("threshold_s and window_s must be positive")
        if sample_every < 1 or min_samples < 1:
            raise ValueError("sample_every and min_samples must be >= 1")
        self._threshold_s = threshold_s
        self._window_s = window_s
        self._sample_every = sample_every
        self._min_samples = min_samples
        self._wall_ns = wall_ns
        self._on_exceeded = on_exceeded
        self._seen = 0
        self._window_id: int | None = None
        self._lags: list[float] = []
        self._alerted = False

    @property
    def alerted(self) -> bool:
        return self._alerted

    def observe(self, frame: L0Frame) -> None:
        """Account one received frame; evaluates the previous window when `frame` opens a new one."""
        self._seen += 1
        if self._seen % self._sample_every:
            return
        try:
            lag_s = self._lag_s(frame)
            if lag_s is None:
                return
            window_id = int(frame.recv_wall_ns // (self._window_s * _NS))
            if self._window_id is not None and window_id != self._window_id:
                self._close_window(frame.stream)
            self._window_id = window_id
            self._lags.append(lag_s)
        except Exception:  # noqa: BLE001 - monitoring must never break the receive loop
            logger.debug("[DATA] stage=ingest_lag status=SKIP", exc_info=True)

    @staticmethod
    def _lag_s(frame: L0Frame) -> float | None:
        stamp = frame.exchange_event_time
        if len(stamp) != 6 or not stamp.isdigit():
            match = _EVENT_TIME_RE.search(frame.raw)
            if match is None:
                return None
            stamp = match.group(1)
        received = dt.datetime.fromtimestamp(frame.recv_wall_ns / _NS, tz=_KST)
        event = received.replace(hour=int(stamp[:2]), minute=int(stamp[2:4]), second=int(stamp[4:]), microsecond=0)
        lag = (received - event).total_seconds()
        return None if lag < -60.0 else max(0.0, lag)

    def _close_window(self, stream: str) -> None:
        lags, self._lags = self._lags, []
        if len(lags) < self._min_samples:
            return
        p50 = statistics.median(lags)
        if not self._alerted and p50 > self._threshold_s:
            self._alerted = True
            logger.critical(
                "[DATA] stage=ingest_lag status=CRITICAL reason=live_lag_exceeded stream=%s p50_s=%.1f max_s=%.1f "
                "threshold_s=%.1f samples=%d",
                stream,
                p50,
                max(lags),
                self._threshold_s,
                len(lags),
            )
            if self._on_exceeded is not None:
                self._on_exceeded()
        elif self._alerted and p50 <= self._threshold_s / 2:
            self._alerted = False
            logger.info(
                "[DATA] stage=ingest_lag status=RECOVERED stream=%s p50_s=%.1f threshold_s=%.1f",
                stream,
                p50,
                self._threshold_s,
            )
