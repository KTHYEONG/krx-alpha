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
LAG_DISCARD_BELOW_S = -60.0
# DB stamps every pre-open book frame (08:30-08:50) with 000000; no KRX or NXT session trades at midnight.
PLACEHOLDER_EVENT_TIME = "000000"


def _event_stamp(frame: L0Frame) -> str | None:
    """Frame's HHMMSS exchange event time, or None when absent, malformed or the vendor placeholder."""
    stamp = frame.exchange_event_time
    if len(stamp) != 6 or not stamp.isdigit():
        match = _EVENT_TIME_RE.search(frame.raw)
        if match is None:
            return None
        stamp = match.group(1)
    return None if stamp == PLACEHOLDER_EVENT_TIME else stamp


def delivery_lag_s(frame: L0Frame) -> float | None:
    """Return receive wall time minus exchange event time in seconds, or None when the event time is unusable.

    Lags below ``LAG_DISCARD_BELOW_S`` are treated as clock or parse noise and discarded; other negatives clamp to 0.
    """
    stamp = _event_stamp(frame)
    if stamp is None:
        return None
    received = dt.datetime.fromtimestamp(frame.recv_wall_ns / _NS, tz=_KST)
    event = received.replace(hour=int(stamp[:2]), minute=int(stamp[2:4]), second=int(stamp[4:]), microsecond=0)
    lag = (received - event).total_seconds()
    return None if lag < LAG_DISCARD_BELOW_S else max(0.0, lag)


def event_second_of(frame: L0Frame) -> int | None:
    """Return the frame's exchange event time as second-of-day, or None when absent, malformed or a placeholder."""
    stamp = _event_stamp(frame)
    if stamp is None:
        return None
    hour, minute, second = int(stamp[:2]), int(stamp[2:4]), int(stamp[4:])
    if hour > 23 or minute > 59 or second > 59:
        return None
    return hour * 3600 + minute * 60 + second


class LagMonitor:
    """Alerts once per episode when the windowed median delivery lag exceeds `threshold_s`, then calls `on_exceeded`.

    Event times carry one-second resolution, so lag is accurate to about one second. Only every
    `sample_every`-th frame is parsed to keep the receive loop cheap. `observe` never raises.
    A minority connection that duplicates the window's dominant connection (shares symbols with it, as a
    hot standby does) is left out: during that overlap the vendor may deliver a minority of frames late
    on either connection, which says nothing about the delivery the session relies on. Connections over
    disjoint symbol shards are all judged.
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
        self._events_s: list[float] = []
        self._lag_conns: list[str] = []
        self._lag_symbols: list[str] = []
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
            self._events_s.append(frame.recv_wall_ns / _NS - lag_s)
            self._lag_conns.append(frame.conn_id)
            self._lag_symbols.append(frame.symbol)
        except Exception:  # noqa: BLE001 - monitoring must never break the receive loop
            logger.debug("[DATA] stage=ingest_lag status=SKIP", exc_info=True)

    @staticmethod
    def _lag_s(frame: L0Frame) -> float | None:
        return delivery_lag_s(frame)

    def _close_window(self, stream: str) -> None:
        all_lags, self._lags = self._lags, []
        events, self._events_s = self._events_s, []
        conns, self._lag_conns = self._lag_conns, []
        symbols, self._lag_symbols = self._lag_symbols, []
        dominant = max(set(conns), key=lambda conn: (conns.count(conn), conn))
        symbols_of: dict[str, set[str]] = {}
        for conn, symbol in zip(conns, symbols, strict=True):
            symbols_of.setdefault(conn, set()).add(symbol)
        judged = {conn for conn, owned in symbols_of.items() if conn == dominant or owned.isdisjoint(symbols_of[dominant])}
        # A frame stamped far behind its connection's newest stamp in the window carries a frozen time, not its
        # delivery time (DB re-sends the closing book stamped 1530xx after 15:30; 11-23 15:40 read as 577 s lag).
        # A real backlog delays one connection's frames together, so their stamps stay close to each other.
        newest: dict[str, float] = {}
        for conn, event in zip(conns, events, strict=True):
            newest[conn] = max(event, newest.get(conn, event))
        lags = [
            lag
            for lag, conn, event in zip(all_lags, conns, events, strict=True)
            if conn in judged and event >= newest[conn] - self._threshold_s
        ]
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
