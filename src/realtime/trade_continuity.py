"""Live trade-loss proof per symbol and connection, mirroring the EOD tick-loss rule in `storage.quality`."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

from src.realtime.contracts import L0Frame
from src.storage.quality_kis import (
    KIS_TICK_CVOLUME,
    KIS_TICK_MDCHECNT,
    KIS_TICK_MIN_FIELDS,
    KIS_TICK_MSCHECNT,
    KIS_TICK_VOLUME,
)

logger = logging.getLogger(__name__)

TRADE_CONTINUITY_VENDORS: Final[frozenset[str]] = frozenset({"ls", "kis"})
TRADE_CONTINUITY_REPORT_INTERVAL_S: Final[float] = 60.0
_TRADE_STREAMS: Final[frozenset[str]] = frozenset({"H0STCNT0", "H0NXCNT0"})
# Vendor batching delivers several trades in one message; only a near-single-trade gap proves a missed trade.
_MAX_CHECNT_DELTA_FOR_LOSS: Final[int] = 1
_TOP_SYMBOLS: Final[int] = 5


def _counters(vendor: str, raw: str) -> tuple[int, int, int] | None:
    """Return (volume, cvolume, total_checnt) from one trade frame, or None when a field is missing or malformed."""
    try:
        if vendor == "ls":
            body = json.loads(raw)["body"]
            return int(body["volume"]), int(body["cvolume"]), int(body["mdchecnt"]) + int(body["mschecnt"])
        parts = raw.split("^")
        if len(parts) < KIS_TICK_MIN_FIELDS:
            return None
        return (
            int(parts[KIS_TICK_VOLUME]),
            int(parts[KIS_TICK_CVOLUME]),
            int(parts[KIS_TICK_MDCHECNT]) + int(parts[KIS_TICK_MSCHECNT]),
        )
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        return None


@dataclass
class _Interval:
    events: int = 0
    lost_volume: int = 0
    regressions: int = 0
    lost_by_symbol: dict[str, int] = field(default_factory=dict)


class TradeContinuityWatch:
    """FrameObserver that counts proven trade losses per (connection, symbol) and logs one line per stream per interval."""

    def __init__(
        self,
        *,
        vendor: str,
        report_interval_s: float = TRADE_CONTINUITY_REPORT_INTERVAL_S,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if vendor not in TRADE_CONTINUITY_VENDORS:
            raise ValueError(f"trade continuity unsupported for vendor {vendor!r}")
        if report_interval_s <= 0:
            raise ValueError("report_interval_s must be positive")
        self._vendor = vendor
        self._report_interval_s = report_interval_s
        self._monotonic = monotonic
        self._last_report = monotonic()
        self._last: dict[tuple[str, str], tuple[int, int]] = {}
        self._intervals: dict[str, _Interval] = {}
        self._loss_events = 0
        self._lost_volume = 0
        self._regressions = 0

    @property
    def loss_events(self) -> int:
        return self._loss_events

    @property
    def lost_volume(self) -> int:
        return self._lost_volume

    @property
    def regressions(self) -> int:
        return self._regressions

    def observe(self, frame: L0Frame) -> None:
        if frame.stream not in _TRADE_STREAMS:
            return
        try:
            counters = _counters(self._vendor, frame.raw)
            if counters is None:
                return
            self._account(frame, *counters)
            self._maybe_report()
        except Exception:
            logger.debug("[DATA] stage=trade_continuity status=SKIP vendor=%s stream=%s", self._vendor, frame.stream, exc_info=True)

    def _account(self, frame: L0Frame, volume: int, cvolume: int, checnt: int) -> None:
        key = (frame.conn_id, frame.symbol)
        previous = self._last.get(key)
        self._last[key] = (volume, checnt)
        if previous is None:
            return
        prev_volume, prev_checnt = previous
        interval = self._intervals.setdefault(frame.stream, _Interval())
        if volume < prev_volume:
            self._regressions += 1
            interval.regressions += 1
            return
        vol_delta = volume - prev_volume
        if cvolume <= 0 or checnt - prev_checnt > _MAX_CHECNT_DELTA_FOR_LOSS or vol_delta <= cvolume:
            return
        lost = vol_delta - cvolume
        self._loss_events += 1
        self._lost_volume += lost
        interval.events += 1
        interval.lost_volume += lost
        interval.lost_by_symbol[frame.symbol] = interval.lost_by_symbol.get(frame.symbol, 0) + lost

    def _maybe_report(self) -> None:
        now = self._monotonic()
        if now - self._last_report < self._report_interval_s:
            return
        self._last_report = now
        for name, interval in self._intervals.items():
            if interval.events == 0 and interval.regressions == 0:
                continue
            top = sorted(interval.lost_by_symbol.items(), key=lambda item: (-item[1], item[0]))[:_TOP_SYMBOLS]
            logger.warning(
                "[DATA] stage=trade_continuity status=LOSS vendor=%s stream=%s events=%d lost_volume=%d regressions=%d "
                "symbols=%d top=%s interval_s=%.0f",
                self._vendor,
                name,
                interval.events,
                interval.lost_volume,
                interval.regressions,
                len(interval.lost_by_symbol),
                ",".join(f"{symbol}:{lost}" for symbol, lost in top),
                self._report_interval_s,
            )
        self._intervals.clear()
