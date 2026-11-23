"""LS market-state lane: JIF/VI_ frames go to a side append-only JSONL journal, never to the L0 tick path."""

from __future__ import annotations

import json
import logging
import pathlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Final, Protocol, TextIO

from src.core.config import LS_MARKET_STATE_PAIRS, LS_MARKET_STATE_STREAMS
from src.realtime.contracts import L0Frame, VendorAck

logger = logging.getLogger(__name__)

VI_MAX_HALT_S: Final[float] = 600.0
# JIF jstatus (KOSPI/KOSDAQ): 61/68 CB stage 1/2 halt, 62/70 its re-opening call auction, 69 CB stage 3 day end;
# 63/71 end the auction and trading resumes, so they clear the halt.
_HALT_JSTATUS: Final[frozenset[str]] = frozenset({"61", "62", "68", "69", "70"})
_VI_STREAM: Final[str] = "LS_VI"
_JIF_STREAM: Final[str] = "LS_JIF"
_MARKET_PAIRS: Final[frozenset[tuple[str, str]]] = frozenset(LS_MARKET_STATE_PAIRS)
_MARKET_SYMBOLS: Final[frozenset[str]] = frozenset(symbol for symbol, _ in LS_MARKET_STATE_PAIRS)
_WRITE_WARN_INTERVAL_S: Final[float] = 60.0


class _Sink(Protocol):
    def record(self, frame: L0Frame) -> None: ...
    def note_ack(self, vendor: str, ack: VendorAck) -> None: ...
    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None: ...
    def flush(self) -> int: ...


class MarketStateSink:
    """Sink decorator that diverts market-state frames and acks to a side JSONL file; all else passes through."""

    def __init__(
        self,
        inner: _Sink,
        path: pathlib.Path,
        *,
        wall_clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._inner = inner
        self._path = path
        self._wall_clock_ns = wall_clock_ns
        self._monotonic = monotonic
        self._handle: TextIO | None = None
        self._last_warn: float | None = None

    def record(self, frame: L0Frame) -> None:
        if frame.stream not in LS_MARKET_STATE_STREAMS:
            self._inner.record(frame)
            return
        line = json.dumps(
            {
                "recv_wall_ns": frame.recv_wall_ns,
                "stream": frame.stream,
                "symbol": frame.symbol,
                "conn_id": frame.conn_id,
                "conn_seq": frame.conn_seq,
                "raw": frame.raw,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        self._append(line)

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        if (ack.symbol, ack.stream) not in _MARKET_PAIRS:
            self._inner.note_ack(vendor, ack)
            return
        logger.info(
            "[DATA] stage=market_state status=ACK stream=%s accepted=%s code=%s",
            ack.stream,
            ack.accepted,
            ack.code,
        )

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        if symbol in _MARKET_SYMBOLS:
            return
        self._inner.note_gap(symbol, start_ns, end_ns, reason)

    def flush(self) -> int:
        return self._inner.flush()

    def _append(self, line: str) -> None:
        try:
            if self._handle is None:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._handle = self._path.open("a", encoding="utf-8", buffering=1)
            self._handle.write(line + "\n")
        except OSError as exc:
            self._warn_write_failure(exc)

    def _warn_write_failure(self, exc: OSError) -> None:
        now = self._monotonic()
        if self._last_warn is not None and now - self._last_warn < _WRITE_WARN_INTERVAL_S:
            return
        self._last_warn = now
        logger.warning("[SYS] stage=market_state status=WRITE_FAIL error_type=%s", type(exc).__name__)


@dataclass(frozen=True)
class MarketStateView:
    """Market state replayed from the side journal; queries are evaluated at a caller-supplied wall time."""

    vi_by_symbol: Mapping[str, tuple[tuple[float, str], ...]] = field(default_factory=dict)
    jif_by_market: Mapping[str, tuple[tuple[float, str], ...]] = field(default_factory=dict)

    def halted_symbols(self, now_wall: float) -> frozenset[str]:
        halted: set[str] = set()
        for symbol, events in self.vi_by_symbol.items():
            latest = _latest_before(events, now_wall)
            if latest is None:
                continue
            triggered_at, vi_gubun = latest
            if vi_gubun != "0" and now_wall - triggered_at <= VI_MAX_HALT_S:
                halted.add(symbol)
        return frozenset(halted)

    def market_halted(self, now_wall: float) -> frozenset[str]:
        halted: set[str] = set()
        for jangubun, events in self.jif_by_market.items():
            latest = _latest_before(events, now_wall)
            if latest is not None and latest[1] in _HALT_JSTATUS:
                halted.add(jangubun)
        return frozenset(halted)


def read_market_state(path: pathlib.Path) -> MarketStateView:
    """Replay the side journal; unreadable or malformed lines are skipped, a missing file is an empty view."""
    vi: dict[str, list[tuple[float, str]]] = {}
    jif: dict[str, list[tuple[float, str]]] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        lines = []
    for line in lines:
        try:
            record = json.loads(line)
            body = json.loads(record["raw"]).get("body", {})
            wall_s = int(record["recv_wall_ns"]) / 1_000_000_000
            stream = record["stream"]
        except (ValueError, KeyError, TypeError, AttributeError):
            continue
        if stream == _VI_STREAM and isinstance(body.get("ref_shcode"), str):
            vi.setdefault(body["ref_shcode"], []).append((wall_s, str(body.get("vi_gubun", ""))))
        elif stream == _JIF_STREAM and isinstance(body.get("jangubun"), str):
            jif.setdefault(body["jangubun"], []).append((wall_s, str(body.get("jstatus", ""))))
    return MarketStateView(
        vi_by_symbol={key: tuple(events) for key, events in vi.items()},
        jif_by_market={key: tuple(events) for key, events in jif.items()},
    )


def _latest_before(events: tuple[tuple[float, str], ...], now_wall: float) -> tuple[float, str] | None:
    latest: tuple[float, str] | None = None
    for event in events:
        if event[0] < now_wall:
            latest = event
    return latest


__all__: list[str] = ["VI_MAX_HALT_S", "MarketStateSink", "MarketStateView", "read_market_state"]
