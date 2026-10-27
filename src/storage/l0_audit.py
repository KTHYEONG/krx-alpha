"""Read-only derivation of open-latency minutes and the missing-trade ledger from L0 journals."""

from __future__ import annotations

import bisect
import contextlib
import dataclasses
import datetime as dt
import itertools
import json
import logging
import os
import pathlib
import re
import time
from array import array
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import BinaryIO, Final, NamedTuple
from zoneinfo import ZoneInfo

import numpy as np
import zstandard as zstd

from src.core.config import AuditSettings
from src.core.session_anchors import SessionAnchors
from src.realtime.contracts import MarketVenue
from src.storage.layout import L0_JOURNAL_GLOB, l0_partition_key, l0_partition_relpath
from src.storage.market_phase import EventKind, MarketPhase, classify_market_phase, phase_windows_for
from src.storage.quality_kis import (
    KIS_TICK_CVOLUME,
    KIS_TICK_MDCHECNT,
    KIS_TICK_MIN_FIELDS,
    KIS_TICK_MSCHECNT,
    KIS_TICK_PRICE,
    KIS_TICK_SYMBOL,
    KIS_TICK_TIME,
    KIS_TICK_VOLUME,
)

_KST: Final = ZoneInfo("Asia/Seoul")
_TICK_STREAM: Final = "H0STCNT0"
_LEDGER_VENDORS: Final[frozenset[str]] = frozenset({"ls", "kis"})
_BATCH_BYTES: Final = 32 * 1024 * 1024
_MINUTE_SAMPLE_OVERHEAD_BYTES: Final = 80
_KEY_SEP: Final = "|"
_SCHEMA_VERSION: Final = 1
_SEQUENCE_CACHE_SIZE: Final = 1024
_OK_REASON: Final = "ok"
_REGULAR_PHASE: Final = MarketPhase.REGULAR.value
_HOTIME_RE: Final = re.compile(r'"hotime"\s*:\s*"([^"]*)"')
_INT_RE: Final = re.compile(r"[+-]?\d+")

logger = logging.getLogger(__name__)


class AuditStatus(StrEnum):
    """Availability of one derived audit scope."""

    OK = "OK"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class SourceFile:
    """Inventory of one L0 journal file read for the audit."""

    path: str
    stream: str
    bytes: int
    rows: int
    truncated: bool


@dataclass(frozen=True)
class LatencyMinute:
    """Delivery-latency distribution of one KST receive minute.

    Frames of a minority connection that overlaps a larger one in time (a hot-standby carrying only what the
    primary lacked) are excluded from `count`/`max_s`/`min_s` and reported as `secondary_*`; `p50_s` is the
    dominant connection's median.
    """

    minute_kst: str
    count: int
    p50_s: float
    max_s: float
    min_s: float
    secondary_count: int = 0
    secondary_max_s: float = 0.0


@dataclass(frozen=True)
class EventHole:
    """Run of exchange-event seconds with no frame from any symbol, inside the regular window."""

    start: str
    end: str
    seconds: int


@dataclass(frozen=True)
class StreamLatency:
    """Per-minute latency of one requested stream plus its event-time holes (longest first, capped)."""

    stream: str
    status: AuditStatus
    reason: str
    rows: int
    event_time_missing: int
    minutes: tuple[LatencyMinute, ...]
    event_holes: tuple[EventHole, ...] = ()
    event_hole_seconds: int = 0


@dataclass(frozen=True)
class LossInterval:
    """One counter gap between two consecutive accepted frames of a segment."""

    symbol: str
    phase: str
    kind: str
    missing_trades: int
    volume_deficit: int
    cum_prev: int
    cum_next: int
    chetime_prev: str
    chetime_next: str
    recv_prev_ns: int
    recv_next_ns: int
    conn_changed: bool


@dataclass(frozen=True)
class SymbolLedger:
    """Missing-trade accounting of one symbol."""

    symbol: str
    status: str
    frames: int
    trades_observed: int
    confirmed_missing: int
    count_only_missing: int
    net_missing: int
    volume_deficit: int
    duplicates: int
    regressions: int
    resets: int
    decode_fail: int
    intervals: tuple[LossInterval, ...]
    intervals_truncated: int
    manifest_frames: int | None


@dataclass(frozen=True)
class LedgerTotals:
    """Session loss budget summed over symbols with a usable counter."""

    symbols: int
    symbols_with_loss: int
    symbols_without_counter: int
    trades_observed: int
    confirmed_missing: int
    count_only_missing: int
    net_missing_positive: int
    volume_deficit: int
    loss_ratio: float
    trades_received: int | None = None


@dataclass(frozen=True)
class SessionAudit:
    """One routed L0 session audit: latency minutes plus the missing-trade ledger."""

    schema_version: int
    session_date: dt.date
    vendor: str
    venue: str
    session: str
    status: AuditStatus
    reason: str
    generated_at_ns: int
    sources: tuple[SourceFile, ...]
    latency: tuple[StreamLatency, ...]
    symbols: tuple[SymbolLedger, ...]
    totals: LedgerTotals
    tail_silence_s: float | None
    last_regular_tick_recv_ns: int | None


@dataclass(frozen=True)
class LatencyBreach:
    """One minute whose delivery latency exceeded the alert threshold."""

    stream: str
    minute_kst: str
    max_s: float
    p50_s: float
    count: int


@dataclass(frozen=True)
class _Envelope:
    raw: str
    recv_ns: int
    conn_id: str
    conn_seq: int | None
    exchange_event_time: str
    symbol: str


@dataclass(frozen=True)
class _TickFrame:
    cum: int
    volume: int
    cvolume: int
    price: str | None
    chetime: str
    recv_ns: int
    conn_id: str
    conn_seq: int | None


@dataclass
class _Segment:
    phase: str
    first_cum: int | None = None
    max_cum: int = 0
    max_frame: _TickFrame | None = None
    accepted: int = 0
    last: _TickFrame | None = None
    run: list[_TickFrame] = field(default_factory=list)
    intervals: list[LossInterval] = field(default_factory=list)
    completed_trades: int = 0
    completed_net: int = 0
    confirmed: int = 0
    count_only: int = 0
    deficit: int = 0
    interval_count: int = 0
    seen_conns: set[str] = field(default_factory=set)


@dataclass
class _SymbolState:
    accepted_total: int = 0
    duplicates: int = 0
    regressions: int = 0
    resets: int = 0
    decode_fail: int = 0
    first_cum: int | None = None
    varied: bool = False
    segments: dict[str, _Segment] = field(default_factory=dict)
    frames: int = 0


@dataclass
class _StreamAccumulator:
    stream: str
    rows: int = 0
    missing: int = 0
    failed: bool = False
    order: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    samples: dict[str, array[float]] = field(default_factory=dict)
    evicted: set[str] = field(default_factory=set)
    stored_bytes: int = 0
    event_seconds: set[int] = field(default_factory=set)
    final: dict[str, LatencyMinute] = field(default_factory=dict)
    spans: dict[str, tuple[int, int]] = field(default_factory=dict)


class _BudgetExceededError(Exception):
    """Internal control flow: the derivation ran past its monotonic time budget."""


class _SeenSequences:
    """Exact per-connection set of observed conn_seq values stored as merged integer intervals."""

    def __init__(self) -> None:
        self._starts: list[int] = []
        self._ends: list[int] = []

    def __contains__(self, seq: object) -> bool:
        if not isinstance(seq, int) or isinstance(seq, bool):
            return False
        idx = bisect.bisect_right(self._starts, seq) - 1
        return idx >= 0 and seq <= self._ends[idx]

    def add(self, seq: int) -> None:
        starts, ends = self._starts, self._ends
        idx = bisect.bisect_left(starts, seq)
        if idx > 0 and seq <= ends[idx - 1] + 1:
            idx -= 1
            new_start = starts[idx]
            new_end = ends[idx] if ends[idx] >= seq else seq
        elif idx < len(starts) and seq >= starts[idx] - 1 and seq <= ends[idx] + 1:
            new_start = starts[idx] if starts[idx] <= seq else seq
            new_end = ends[idx] if ends[idx] >= seq else seq
        else:
            starts.insert(idx, seq)
            ends.insert(idx, seq)
            return
        starts[idx] = new_start
        ends[idx] = new_end
        nxt = idx + 1
        while nxt < len(starts) and starts[nxt] - 1 <= ends[idx]:
            if ends[nxt] > ends[idx]:
                ends[idx] = ends[nxt]
            del starts[nxt]
            del ends[nxt]

    def __len__(self) -> int:
        return len(self._starts)


def _valid_hhmmss(value: object) -> dt.time | None:
    if not isinstance(value, str) or len(value) != 6 or not value.isascii() or not value.isdigit():
        return None
    hour, minute, second = int(value[0:2]), int(value[2:4]), int(value[4:6])
    if hour > 23 or minute > 59 or second > 59:
        return None
    return dt.time(hour, minute, second)


def _parse_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if _INT_RE.fullmatch(text) is None:
            return None
        try:
            return int(text, 10)
        except ValueError:
            return None
    return None


def _hhmmss(second_of_day: int) -> str:
    return f"{second_of_day // 3600:02d}{second_of_day % 3600 // 60:02d}{second_of_day % 60:02d}"


def _event_holes(seconds: set[int], *, start_s: int, end_s: int, min_hole_s: int, cap: int) -> tuple[tuple[EventHole, ...], int]:
    """Return the `cap` longest runs of at least `min_hole_s` empty event seconds in `[start_s, end_s)` and their total seconds.

    A window with no event at all yields no holes: that is a missing stream, reported by status instead.
    """
    inside = sorted(x for x in seconds if start_s <= x < end_s)
    if not inside:
        return (), 0
    edges = [start_s - 1, *inside, end_s]
    runs = [(a + 1, b - 1) for a, b in itertools.pairwise(edges) if b - a - 1 >= min_hole_s]
    total = sum(last - first + 1 for first, last in runs)
    longest = sorted(runs, key=lambda run: (run[0] - run[1], run[0]))[:cap]
    holes = tuple(EventHole(_hhmmss(first), _hhmmss(last), last - first + 1) for first, last in sorted(longest))
    return holes, total


def _iter_line_batches(path: pathlib.Path, batch_bytes: int, truncated: list[bool]) -> Iterator[bytes]:
    try:
        out = b""
        with open(path, "rb") as handle:
            for chunk in _iter_zstd_blocks(handle, truncated):
                out += chunk
                while len(out) >= batch_bytes:
                    cut = out.rfind(b"\n", 0, batch_bytes)
                    if cut < 0:
                        cut = out.find(b"\n")
                        if cut < 0:
                            if len(out) > _BATCH_BYTES:
                                raise ValueError("oversized journal line")
                            break
                    yield out[: cut + 1]
                    out = out[cut + 1 :]
        if out.strip():
            yield out
    except (OSError, ValueError, zstd.ZstdError):
        truncated[0] = True


def _iter_zstd_blocks(handle: BinaryIO, truncated: list[bool]) -> Iterator[bytes]:
    # Zstd blocks expand to at most 128 KiB; feeding compressed file-sized chunks does not bound output.
    try:
        while True:
            start = handle.tell()
            prefix = handle.read(18)
            if not prefix:
                return
            header_size = zstd.frame_header_size(prefix)
            header = prefix[:header_size]
            params = zstd.get_frame_parameters(header)
            handle.seek(start + header_size)
            decoder = zstd.ZstdDecompressor().decompressobj()
            decoder.decompress(header)
            last = False
            while not last:
                block = handle.read(3)
                if len(block) != 3:
                    raise ValueError("truncated block header")
                bits = int.from_bytes(block, "little")
                last = bool(bits & 1)
                kind = (bits >> 1) & 3
                size = bits >> 3
                if kind == 3 or size > 128 * 1024:
                    raise ValueError("invalid block")
                size = 1 if kind == 1 else size
                payload = handle.read(size)
                decoded = decoder.decompress(block + payload)
                if decoded:
                    yield decoded
                if len(payload) != size:
                    raise ValueError("truncated block")
            if params.has_checksum:
                decoder.decompress(handle.read(4))
            if not decoder.eof:
                raise ValueError("truncated frame")
    except (ValueError, zstd.ZstdError):
        truncated[0] = True


def _decode_envelope(line: bytes) -> _Envelope | None:
    try:
        record = json.loads(line)
    except ValueError:
        return None
    if not isinstance(record, dict):
        return None
    raw = record.get("raw")
    recv = record.get("recv_wall_ns")
    if not isinstance(raw, str) or isinstance(recv, bool) or not isinstance(recv, int):
        return None
    conn = record.get("conn_id")
    event_time = record.get("exchange_event_time")
    symbol = record.get("symbol")
    return _Envelope(
        raw=raw,
        recv_ns=recv,
        conn_id=conn if isinstance(conn, str) else "",
        conn_seq=_parse_int(record.get("conn_seq")),
        exchange_event_time=event_time if isinstance(event_time, str) else "",
        symbol=symbol if isinstance(symbol, str) else "",
    )


def _parse_body(raw: str) -> dict[str, object]:
    try:
        payload = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    body = payload.get("body")
    return body if isinstance(body, dict) else {}


def _tick_body(vendor: str, raw: str) -> dict[str, object]:
    """Return the normalized tick body of one L0 record; empty dict when undecodable."""
    if vendor != "kis":
        return _parse_body(raw)
    parts = raw.split("^")
    if len(parts) < KIS_TICK_MIN_FIELDS:
        return {}
    return {
        "shcode": parts[KIS_TICK_SYMBOL],
        "chetime": parts[KIS_TICK_TIME],
        "price": parts[KIS_TICK_PRICE],
        "cvolume": parts[KIS_TICK_CVOLUME],
        "volume": parts[KIS_TICK_VOLUME],
        "mdchecnt": parts[KIS_TICK_MDCHECNT],
        "mschecnt": parts[KIS_TICK_MSCHECNT],
    }


def _finalize_minute(minute: str, samples: Sequence[float]) -> LatencyMinute:
    """Return count/p50/max/min over the recv-minute's latency samples; samples non-empty."""
    ordered = np.frombuffer(samples, dtype=np.float64) if isinstance(samples, array) else np.array(samples, dtype=np.float64)
    median_index = (len(ordered) - 1) // 2
    minimum, maximum = float(ordered.min()), float(ordered.max())
    ordered.partition(median_index)
    return LatencyMinute(
        minute_kst=minute.partition(_KEY_SEP)[0],
        count=len(ordered),
        p50_s=float(ordered[median_index]),
        max_s=maximum,
        min_s=minimum,
    )


def _latency_key(recv_ns: int, conn_id: str) -> str:
    minute = dt.datetime.fromtimestamp(recv_ns // 1_000_000_000, tz=_KST).strftime("%H:%M")
    return f"{minute}{_KEY_SEP}{conn_id}"


def _latency_sample(stream: str, vendor: str, day: dt.date, record: _Envelope) -> tuple[str, float] | None:
    try:
        moment = _valid_hhmmss(record.exchange_event_time)
        if moment is None:
            if stream == _TICK_STREAM:
                moment = _valid_hhmmss(_tick_body(vendor, record.raw).get("chetime"))
            else:
                match = _HOTIME_RE.search(record.raw)
                moment = _valid_hhmmss(match.group(1) if match is not None else None)
        if moment is None:
            return None
        event_epoch = dt.datetime(
            day.year, day.month, day.day, moment.hour, moment.minute, moment.second, tzinfo=_KST
        ).timestamp()
        key = _latency_key(record.recv_ns, record.conn_id)
        return key, (record.recv_ns - int(event_epoch) * 1_000_000_000) / 1_000_000_000
    except (ValueError, OverflowError, OSError):
        return None


def _merge_connections(minute: str, parts: list[tuple[str, LatencyMinute, tuple[int, int]]]) -> LatencyMinute:
    """Fold per-connection minutes into one; a minority connection overlapping the dominant one in time is secondary."""
    dominant_conn, dominant, (low, high) = max(parts, key=lambda part: (part[1].count, part[0]))
    judged = [dominant]
    secondary_count, secondary_max = 0, 0.0
    for conn, entry, (start, end) in parts:
        if conn == dominant_conn:
            continue
        if start <= high and end >= low:
            secondary_count += entry.count
            secondary_max = max(secondary_max, entry.max_s)
        else:
            judged.append(entry)
    return LatencyMinute(
        minute_kst=minute,
        count=sum(entry.count for entry in judged),
        p50_s=dominant.p50_s,
        max_s=max(entry.max_s for entry in judged),
        min_s=min(entry.min_s for entry in judged),
        secondary_count=secondary_count,
        secondary_max_s=secondary_max,
    )


def _zero_totals() -> LedgerTotals:
    return LedgerTotals(
        symbols=0,
        symbols_with_loss=0,
        symbols_without_counter=0,
        trades_observed=0,
        confirmed_missing=0,
        count_only_missing=0,
        net_missing_positive=0,
        volume_deficit=0,
        loss_ratio=0.0,
    )


def _is_duplicate(prev: _TickFrame, frame: _TickFrame) -> bool:
    return (frame.cum, frame.volume, frame.cvolume, frame.price, frame.chetime) == (
        prev.cum,
        prev.volume,
        prev.cvolume,
        prev.price,
        prev.chetime,
    )


def _non_decreasing(run: list[_TickFrame]) -> bool:
    return all(later.cum >= earlier.cum for earlier, later in itertools.pairwise(run))


class _Derivation:
    def __init__(
        self,
        journal_root: pathlib.Path,
        vendor: str,
        venue: str,
        session: str,
        day: dt.date,
        anchors: SessionAnchors,
        settings: AuditSettings,
        manifest_frame_counts: Mapping[str, Mapping[str, int]] | None,
        progress: Callable[[], None] | None,
        monotonic: Callable[[], float],
    ) -> None:
        self._journal_root = journal_root
        self._vendor = vendor
        self._venue = venue
        self._session = session
        self._day = day
        self._anchors = anchors
        self._settings = settings
        self._manifest_frame_counts = manifest_frame_counts
        self._progress = progress
        self._monotonic = monotonic
        self._started = monotonic()
        self._pinged = 0
        self._windows = phase_windows_for(anchors)
        try:
            self._venue_enum: MarketVenue | None = MarketVenue(venue)
        except ValueError:
            self._venue_enum = None
        self._sources: list[SourceFile] = []
        self._symbols: dict[str, _SymbolState] = {}
        self._last_regular_tick_ns: int | None = None
        self._seen: dict[str, _SeenSequences] = {}
        self._file_passes = 0
        self._sample_bytes_peak = 0
        self._trades_received = 0

    def ping(self) -> None:
        if self._progress is not None:
            self._progress()
        self._pinged += 1

    def check_budget(self) -> None:
        if self._monotonic() - self._started > self._settings.time_budget_s:
            raise _BudgetExceededError

    def symbol_state(self, symbol: str) -> _SymbolState:
        state = self._symbols.get(symbol)
        if state is None:
            state = _SymbolState()
            self._symbols[symbol] = state
        return state

    def process_stream(self, stream: str) -> StreamLatency:
        acc = _StreamAccumulator(stream=stream)
        if self._venue_enum is None:
            return self._finish_stream(acc, AuditStatus.UNAVAILABLE, "journal_missing")
        key = l0_partition_key(self._vendor, self._venue, self._session, stream, self._day)
        if key is None:
            return self._finish_stream(acc, AuditStatus.UNAVAILABLE, "journal_missing")
        files = sorted((self._journal_root / l0_partition_relpath(key)).glob(L0_JOURNAL_GLOB), key=lambda p: p.name)
        if not files:
            return self._finish_stream(acc, AuditStatus.UNAVAILABLE, "journal_missing")
        budget = self._settings.latency_sample_budget_bytes
        for path in files:
            self.check_budget()
            self.process_file(stream, acc, path)
        while acc.samples:
            minute, held = acc.samples.popitem()
            acc.final[minute] = _finalize_minute(minute, held)
            del held
        acc.stored_bytes = 0
        deferred = [minute for minute in acc.order if minute not in acc.final]
        pos = 0
        while pos < len(deferred):
            batch: list[str] = []
            projected = 0
            while pos < len(deferred):
                need = acc.counts[deferred[pos]] * 8 + _MINUTE_SAMPLE_OVERHEAD_BYTES
                if batch and projected + need > budget:
                    break
                batch.append(deferred[pos])
                projected += need
                pos += 1
            wanted = set(batch)
            collected: dict[str, array[float]] = {}
            batch_bytes = 0
            for path in files:
                self.check_budget()
                truncated = [False]
                self._file_passes += 1
                for blob in _iter_line_batches(path, _BATCH_BYTES, truncated):
                    for line in blob.splitlines():
                        record = _decode_envelope(line)
                        if record is None:
                            continue
                        item = _latency_sample(stream, self._vendor, self._day, record)
                        if item is None:
                            continue
                        minute, value = item
                        if minute in wanted:
                            arr: array[float] | None = collected.get(minute)
                            if arr is None:
                                arr = array("d")
                                collected[minute] = arr
                                batch_bytes += _MINUTE_SAMPLE_OVERHEAD_BYTES
                            arr.append(value)
                            batch_bytes += 8
                            self._sample_bytes_peak = max(self._sample_bytes_peak, batch_bytes)
                            del arr
                    self.ping()
                    self.check_budget()
            for minute in batch:
                held = collected.pop(minute, array("d"))
                if len(held) != acc.counts[minute]:
                    raise ValueError("journal sample count changed during latency collection")
                acc.final[minute] = _finalize_minute(minute, held)
                del held
        if acc.rows == 0:
            return self._finish_stream(acc, AuditStatus.UNAVAILABLE, "io_error" if acc.failed else "no_decodable_records")
        if acc.failed:
            return self._finish_stream(acc, AuditStatus.PARTIAL, "truncated")
        return self._finish_stream(acc, AuditStatus.OK, _OK_REASON)

    def process_file(self, stream: str, acc: _StreamAccumulator, path: pathlib.Path) -> None:
        truncated: list[bool] = [False]
        file_rows = 0
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
            truncated[0] = True
        else:
            self._file_passes += 1
            for blob in _iter_line_batches(path, _BATCH_BYTES, truncated):
                for line in blob.split(b"\n"):
                    if not line.strip():
                        continue
                    record = _decode_envelope(line)
                    if record is None:
                        truncated[0] = True
                        continue
                    file_rows += 1
                    try:
                        self.handle_record(stream, acc, record)
                    except (ValueError, OverflowError, OSError):
                        file_rows -= 1
                        truncated[0] = True
                    finally:
                        if stream == _TICK_STREAM and record.conn_id and record.conn_seq is not None:
                            seen = self._seen.get(record.conn_id)
                            if seen is None:
                                seen = self._seen[record.conn_id] = _SeenSequences()
                            seen.add(record.conn_seq)
                self.ping()
                self.check_budget()
        rel = path.relative_to(self._journal_root).as_posix()
        self._sources.append(SourceFile(path=rel, stream=stream, bytes=size, rows=file_rows, truncated=truncated[0]))
        acc.rows += file_rows
        acc.failed = acc.failed or truncated[0]

    def handle_record(self, stream: str, acc: _StreamAccumulator, record: _Envelope) -> None:
        moment = _valid_hhmmss(record.exchange_event_time)
        body: dict[str, object] | None = None
        if moment is None:
            if stream == _TICK_STREAM:
                body = _tick_body(self._vendor, record.raw)
                moment = _valid_hhmmss(body.get("chetime"))
            else:
                match = _HOTIME_RE.search(record.raw)
                moment = _valid_hhmmss(match.group(1) if match is not None else None)
        if moment is None:
            acc.missing += 1
        else:
            event_epoch = dt.datetime(
                self._day.year, self._day.month, self._day.day, moment.hour, moment.minute, moment.second, tzinfo=_KST
            ).timestamp()
            acc.event_seconds.add(moment.hour * 3600 + moment.minute * 60 + moment.second)
            key = _latency_key(record.recv_ns, record.conn_id)
            span = acc.spans.get(key)
            acc.spans[key] = (
                (record.recv_ns, record.recv_ns)
                if span is None
                else (min(span[0], record.recv_ns), max(span[1], record.recv_ns))
            )
            self.add_sample(acc, key, (record.recv_ns - int(event_epoch) * 1_000_000_000) / 1_000_000_000)
        if stream == _TICK_STREAM and self._vendor in _LEDGER_VENDORS:
            if body is None or self._vendor != "ls":
                body = _tick_body(self._vendor, record.raw)
            if _decode_received_tick(body) is not None:
                self._trades_received += 1
            self.accept_tick(record, body)

    def add_sample(self, acc: _StreamAccumulator, minute: str, latency: float) -> None:
        budget = self._settings.latency_sample_budget_bytes
        known = acc.counts.get(minute)
        if known is None:
            acc.order.append(minute)
            acc.counts[minute] = 1
            cost = 8 + _MINUTE_SAMPLE_OVERHEAD_BYTES
            if acc.stored_bytes + cost > budget:
                acc.evicted.add(minute)
                return
            acc.samples[minute] = array("d", [latency])
            acc.stored_bytes += cost
            if acc.stored_bytes > self._sample_bytes_peak:
                self._sample_bytes_peak = acc.stored_bytes
            return
        acc.counts[minute] = known + 1
        if minute in acc.evicted:
            return
        if acc.stored_bytes + 8 <= budget:
            acc.samples[minute].append(latency)
            acc.stored_bytes += 8
            if acc.stored_bytes > self._sample_bytes_peak:
                self._sample_bytes_peak = acc.stored_bytes
            return
        for candidate in reversed(acc.order):
            if candidate in acc.samples:
                acc.stored_bytes -= len(acc.samples[candidate]) * 8 + _MINUTE_SAMPLE_OVERHEAD_BYTES
                del acc.samples[candidate]
                acc.evicted.add(candidate)
                if acc.stored_bytes + 8 <= budget:
                    break
        if minute in acc.evicted:
            return
        acc.samples[minute].append(latency)
        acc.stored_bytes += 8

    def accept_tick(self, record: _Envelope, body: dict[str, object]) -> None:
        raw_symbol = body.get("shcode")
        symbol = raw_symbol if isinstance(raw_symbol, str) and raw_symbol else (record.symbol or None)
        che_raw = body.get("chetime")
        moment = _valid_hhmmss(che_raw)
        cum = _tick_cum(body)
        volume = _parse_int(body.get("volume"))
        cvolume = _parse_int(body.get("cvolume"))
        if symbol is None or not isinstance(raw_symbol, str) or not raw_symbol or moment is None or cum is None or volume is None or cvolume is None:
            if symbol is not None:
                self.symbol_state(symbol).decode_fail += 1
            return
        assert isinstance(che_raw, str)
        phase = classify_market_phase(self._venue_enum or MarketVenue.KRX, che_raw, EventKind.TRADE, windows=self._windows).value
        if phase == _REGULAR_PHASE and (
            self._last_regular_tick_ns is None or record.recv_ns > self._last_regular_tick_ns
        ):
            self._last_regular_tick_ns = record.recv_ns
        state = self.symbol_state(symbol)
        if self.sequence_duplicate(record):
            state.duplicates += 1
            return
        price_raw = body.get("price")
        frame = _TickFrame(
            cum=cum,
            volume=volume,
            cvolume=cvolume,
            price=price_raw if isinstance(price_raw, str) else None,
            chetime=che_raw,
            recv_ns=record.recv_ns,
            conn_id=record.conn_id,
            conn_seq=record.conn_seq,
        )
        if state.first_cum is None:
            state.first_cum = cum
        elif cum != state.first_cum:
            state.varied = True
        segment = state.segments.get(phase)
        if segment is None:
            segment = _Segment(phase=phase)
            state.segments[phase] = segment
        self.advance_segment(symbol, state, segment, frame)

    def sequence_duplicate(self, record: _Envelope) -> bool:
        if not record.conn_id or record.conn_seq is None:
            return False
        seen = self._seen.get(record.conn_id)
        if seen is None:
            seen = self._seen[record.conn_id] = _SeenSequences()
        if record.conn_seq in seen:
            return True
        seen.add(record.conn_seq)
        return False

    def advance_segment(self, symbol: str, state: _SymbolState, segment: _Segment, frame: _TickFrame) -> None:
        prev = segment.last
        if (prev is not None and _is_duplicate(prev, frame)) or (
            segment.run and _is_duplicate(segment.run[-1], frame)
        ):
            state.duplicates += 1
            return
        if self.is_overlap(segment, frame):
            self.absorb_overlap(state, segment, frame)
            return
        segment.seen_conns.add(frame.conn_id)
        state.accepted_total += 1
        state.frames += 1
        if segment.first_cum is None:
            segment.first_cum = frame.cum
            segment.max_cum = frame.cum
            segment.max_frame = frame
            segment.accepted = 1
            segment.last = frame
            return
        if frame.cum < segment.max_cum:
            state.regressions += 1
            segment.run.append(frame)
            confirm = self._settings.counter_reset_confirm_frames
            segment.run[:] = segment.run[-confirm:]
            if len(segment.run) >= confirm and _non_decreasing(segment.run):
                base = segment.run[-confirm]
                segment.completed_trades += segment.max_cum - segment.first_cum + 1
                segment.completed_net += segment.max_cum - segment.first_cum - (segment.accepted - 1)
                segment.first_cum = base.cum
                segment.max_cum = base.cum
                segment.max_frame = base
                segment.accepted = 1
                for item in segment.run[-confirm + 1 :]:
                    self.record_advance(symbol, segment, item)
                segment.run.clear()
                segment.last = frame
                state.resets += 1
            return
        segment.run.clear()
        self.record_advance(symbol, segment, frame)
        segment.last = frame

    @staticmethod
    def is_overlap(segment: _Segment, frame: _TickFrame) -> bool:
        """A frame from an earlier connection delivered after another connection advanced the counter (proactive handover)."""
        prev = segment.max_frame
        return (
            prev is not None
            and frame.cum <= segment.max_cum
            and frame.conn_id != prev.conn_id
            and frame.conn_id in segment.seen_conns
        )

    @staticmethod
    def absorb_overlap(state: _SymbolState, segment: _Segment, frame: _TickFrame) -> None:
        """Shrink the loss interval containing a late frame's counter; counters outside any interval are plain duplicates."""
        for idx, interval in enumerate(segment.intervals):
            if not interval.cum_prev < frame.cum < interval.cum_next:
                continue
            remaining = interval.missing_trades - 1
            deficit_left = max(0, interval.volume_deficit - frame.cvolume)
            segment.deficit -= interval.volume_deficit - deficit_left
            if interval.kind == "confirmed":
                segment.confirmed -= 1
            else:
                segment.count_only -= 1
            if remaining == 0:
                del segment.intervals[idx]
                segment.interval_count -= 1
            else:
                segment.intervals[idx] = dataclasses.replace(interval, missing_trades=remaining, volume_deficit=deficit_left)
            segment.accepted += 1
            state.accepted_total += 1
            state.frames += 1
            return
        state.duplicates += 1

    def record_advance(self, symbol: str, segment: _Segment, frame: _TickFrame) -> None:
        prev = segment.max_frame
        if prev is not None:
            missing = frame.cum - segment.max_cum - 1
            if missing >= 1:
                deficit = max(0, (frame.volume - prev.volume) - frame.cvolume)
                segment.intervals.append(
                    LossInterval(
                        symbol=symbol,
                        phase=segment.phase,
                        kind="confirmed" if deficit > 0 else "count_only",
                        missing_trades=missing,
                        volume_deficit=deficit,
                        cum_prev=segment.max_cum,
                        cum_next=frame.cum,
                        chetime_prev=prev.chetime,
                        chetime_next=frame.chetime,
                        recv_prev_ns=prev.recv_ns,
                        recv_next_ns=frame.recv_ns,
                        conn_changed=frame.conn_id != prev.conn_id,
                    )
                )
                segment.interval_count += 1
                if deficit > 0:
                    segment.confirmed += missing
                else:
                    segment.count_only += missing
                segment.deficit += deficit
                segment.intervals.sort(key=lambda iv: (-iv.missing_trades, iv.recv_prev_ns))
                del segment.intervals[self._settings.max_intervals_per_symbol:]
        segment.max_cum = frame.cum
        segment.max_frame = frame
        segment.accepted += 1

    def _finish_stream(self, acc: _StreamAccumulator, status: AuditStatus, reason: str) -> StreamLatency:
        by_minute: dict[str, list[tuple[str, LatencyMinute, tuple[int, int]]]] = {}
        for key in sorted(acc.final):
            minute, _, conn = key.partition(_KEY_SEP)
            by_minute.setdefault(minute, []).append((conn, acc.final[key], acc.spans[key]))
        minutes = [_merge_connections(minute, parts) for minute, parts in by_minute.items()]
        open_t, auction_t = self._anchors.regular_open, self._anchors.closing_auction_start
        holes, hole_seconds = _event_holes(
            acc.event_seconds,
            start_s=open_t.hour * 3600 + open_t.minute * 60 + open_t.second,
            end_s=auction_t.hour * 3600 + auction_t.minute * 60 + auction_t.second,
            min_hole_s=self._settings.event_hole_min_s,
            cap=self._settings.max_event_holes,
        )
        return StreamLatency(
            stream=acc.stream,
            status=status,
            reason=reason,
            rows=acc.rows,
            event_time_missing=acc.missing,
            minutes=tuple(minutes),
            event_holes=holes,
            event_hole_seconds=hole_seconds,
        )

    def finalize_ledger(self) -> tuple[tuple[SymbolLedger, ...], LedgerTotals]:
        ledgers: list[SymbolLedger] = []
        for symbol in sorted(self._symbols):
            state = self._symbols[symbol]
            regular = [seg for seg in state.segments.values() if seg.phase == _REGULAR_PHASE]
            trades = sum(seg.completed_trades + seg.max_cum - seg.first_cum + 1 for seg in regular if seg.first_cum is not None)
            net = sum(seg.completed_net + (seg.max_cum - seg.first_cum) - (seg.accepted - 1) for seg in regular if seg.first_cum is not None)
            intervals = [iv for seg in state.segments.values() for iv in seg.intervals]
            confirmed = sum(seg.confirmed for seg in regular)
            count_only = sum(seg.count_only for seg in regular)
            deficit = sum(seg.deficit for seg in regular)
            ordered = sorted(intervals, key=lambda iv: (-iv.missing_trades, iv.recv_prev_ns))
            cap = self._settings.max_intervals_per_symbol
            if state.accepted_total == 0 or (state.accepted_total > 1 and not state.varied):
                status = "NO_COUNTER"
            else:
                status = "OK"
            manifest_frames: int | None = None
            if self._manifest_frame_counts is not None:
                per_stream = self._manifest_frame_counts.get(symbol)
                if per_stream is not None:
                    manifest_frames = per_stream.get(_TICK_STREAM)
            ledgers.append(
                SymbolLedger(
                    symbol=symbol,
                    status=status,
                    frames=state.frames,
                    trades_observed=trades,
                    confirmed_missing=confirmed,
                    count_only_missing=count_only,
                    net_missing=net,
                    volume_deficit=deficit,
                    duplicates=state.duplicates,
                    regressions=state.regressions,
                    resets=state.resets,
                    decode_fail=state.decode_fail,
                    intervals=tuple(ordered[:cap]),
                    intervals_truncated=sum(seg.interval_count for seg in state.segments.values()) - len(ordered[:cap]),
                    manifest_frames=manifest_frames,
                )
            )
        ok_ledgers = [entry for entry in ledgers if entry.status == "OK"]
        trades_total = sum(entry.trades_observed for entry in ok_ledgers)
        confirmed_total = sum(entry.confirmed_missing for entry in ok_ledgers)
        totals = LedgerTotals(
            symbols=len(ok_ledgers),
            symbols_with_loss=sum(1 for entry in ok_ledgers if entry.confirmed_missing + entry.count_only_missing > 0),
            symbols_without_counter=sum(1 for entry in ledgers if entry.status == "NO_COUNTER"),
            trades_observed=trades_total,
            confirmed_missing=confirmed_total,
            count_only_missing=sum(entry.count_only_missing for entry in ok_ledgers),
            net_missing_positive=sum(max(0, entry.net_missing) for entry in ok_ledgers),
            volume_deficit=sum(entry.volume_deficit for entry in ok_ledgers),
            loss_ratio=(confirmed_total / trades_total) if trades_total else 0.0,
            trades_received=self._trades_received,
        )
        return tuple(ledgers), totals

    def tail_silence(self) -> tuple[float | None, int | None]:
        if self._last_regular_tick_ns is None:
            return None, None
        closing = dt.datetime(
            self._day.year,
            self._day.month,
            self._day.day,
            self._anchors.closing_auction_start.hour,
            self._anchors.closing_auction_start.minute,
            self._anchors.closing_auction_start.second,
            tzinfo=_KST,
        )
        close_ns = int(closing.timestamp() * 1_000_000_000)
        return max(0, close_ns - self._last_regular_tick_ns) / 1_000_000_000, self._last_regular_tick_ns


class ReceivedTick(NamedTuple):
    symbol: str
    chetime: int
    cum: int
    price: int
    volume: int


def _decode_received_tick(body: dict[str, object]) -> ReceivedTick | None:
    symbol = body.get("shcode")
    moment = _valid_hhmmss(body.get("chetime"))
    cum, price, volume = _tick_cum(body), _parse_int(body.get("price")), _parse_int(body.get("cvolume"))
    if not isinstance(symbol, str) or moment is None or cum is None or price is None or volume is None:
        return None
    return ReceivedTick(symbol, moment.hour * 10000 + moment.minute * 100 + moment.second, cum, price, volume)


def iter_received_ticks(
    journal_root: pathlib.Path, *, vendor: str, venue: str, session: str, day: dt.date
) -> Iterator[ReceivedTick]:
    """Yield every decodable trade of the routed tick partition in journal order; nothing is written."""
    key = l0_partition_key(vendor, venue, session, _TICK_STREAM, day)
    if key is None:
        return
    files = sorted((journal_root / l0_partition_relpath(key)).glob(L0_JOURNAL_GLOB), key=lambda p: p.name)
    for path in files:
        for blob in _iter_line_batches(path, _BATCH_BYTES, [False]):
            for line in blob.splitlines():
                record = _decode_envelope(line)
                if record is None:
                    continue
                tick = _decode_received_tick(_tick_body(vendor, record.raw))
                if tick is not None:
                    yield tick


def _tick_cum(body: dict[str, object]) -> int | None:
    mdchecnt = _parse_int(body.get("mdchecnt"))
    mschecnt = _parse_int(body.get("mschecnt"))
    if mdchecnt is None or mschecnt is None:
        return None
    return mdchecnt + mschecnt


def _safe_stamp(wall_ns: Callable[[], int]) -> int:
    try:
        return wall_ns()
    except (OSError, ValueError):
        return 0


def derive_session_audit(
    journal_root: pathlib.Path,
    *,
    vendor: str,
    venue: str,
    session: str,
    day: dt.date,
    anchors: SessionAnchors,
    settings: AuditSettings,
    manifest_frame_counts: Mapping[str, Mapping[str, int]] | None = None,
    progress: Callable[[], None] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    wall_ns: Callable[[], int] = time.time_ns,
) -> SessionAudit:
    """Derive per-minute stream latency and the per-symbol missing-trade ledger for one routed L0 session; read-only, never raises on bad input (returns `UNAVAILABLE` with a reason)."""
    latency: tuple[StreamLatency, ...] = ()
    started = monotonic()
    run: _Derivation | None = None

    def _emit(audit: SessionAudit) -> SessionAudit:
        passes = run._file_passes if run is not None else 0
        peak = run._sample_bytes_peak if run is not None else 0
        elapsed = monotonic() - started
        if audit.status is AuditStatus.OK:
            logger.info(
                "[DATA] stage=session_audit_timing vendor=%s session=%s date=%s status=%s reason=%s elapsed_s=%.1f file_passes=%d sample_bytes_peak=%d",
                vendor, session, day.isoformat(), audit.status.value, audit.reason,
                elapsed, passes, peak,
            )
        else:
            logger.warning(
                "[DATA] stage=session_audit_timing vendor=%s session=%s date=%s status=%s reason=%s elapsed_s=%.1f file_passes=%d sample_bytes_peak=%d",
                vendor, session, day.isoformat(), audit.status.value, audit.reason,
                elapsed, passes, peak,
            )
        return audit

    try:
        generated_at_ns = wall_ns()
        run = _Derivation(
            journal_root=journal_root,
            vendor=vendor,
            venue=venue,
            session=session,
            day=day,
            anchors=anchors,
            settings=settings,
            manifest_frame_counts=manifest_frame_counts,
            progress=progress,
            monotonic=monotonic,
        )
        ordered = list(dict.fromkeys(settings.latency_streams))
        if vendor in _LEDGER_VENDORS or _TICK_STREAM in ordered:
            ordered = [_TICK_STREAM, *(stream for stream in ordered if stream != _TICK_STREAM)]
        results = tuple(run.process_stream(stream) for stream in ordered)
        latency = tuple(entry for entry in results if entry.stream in settings.latency_streams)
        if run._pinged == 0:
            run.ping()
        run.check_budget()
        if all(entry.status is AuditStatus.UNAVAILABLE for entry in results):
            return _emit(SessionAudit(
                schema_version=_SCHEMA_VERSION,
                session_date=day,
                vendor=vendor,
                venue=venue,
                session=session,
                status=AuditStatus.UNAVAILABLE,
                reason="unsupported_vendor" if vendor not in _LEDGER_VENDORS else results[0].reason,
                generated_at_ns=generated_at_ns,
                sources=tuple(run._sources),
                latency=latency,
                symbols=(),
                totals=_zero_totals(),
                tail_silence_s=None,
                last_regular_tick_recv_ns=None,
            ))
        if vendor not in _LEDGER_VENDORS:
            return _emit(SessionAudit(
                schema_version=_SCHEMA_VERSION,
                session_date=day,
                vendor=vendor,
                venue=venue,
                session=session,
                status=AuditStatus.UNAVAILABLE,
                reason="unsupported_vendor",
                generated_at_ns=generated_at_ns,
                sources=tuple(run._sources),
                latency=latency,
                symbols=(),
                totals=_zero_totals(),
                tail_silence_s=None,
                last_regular_tick_recv_ns=None,
            ))
        symbols, totals = run.finalize_ledger()
        run.check_budget()
        tail_silence_s, last_regular_tick_recv_ns = run.tail_silence()
        if last_regular_tick_recv_ns is None:
            return _emit(SessionAudit(
                schema_version=_SCHEMA_VERSION,
                session_date=day,
                vendor=vendor,
                venue=venue,
                session=session,
                status=AuditStatus.UNAVAILABLE,
                reason="no_decodable_records",
                generated_at_ns=generated_at_ns,
                sources=tuple(run._sources),
                latency=latency,
                symbols=symbols,
                totals=totals,
                tail_silence_s=None,
                last_regular_tick_recv_ns=None,
            ))
        if all(entry.status is AuditStatus.OK for entry in results):
            status, reason = AuditStatus.OK, _OK_REASON
        else:
            status = AuditStatus.PARTIAL
            reason = next(entry.reason for entry in results if entry.status is not AuditStatus.OK)
        return _emit(SessionAudit(
            schema_version=_SCHEMA_VERSION,
            session_date=day,
            vendor=vendor,
            venue=venue,
            session=session,
            status=status,
            reason=reason,
            generated_at_ns=generated_at_ns,
            sources=tuple(run._sources),
            latency=latency,
            symbols=symbols,
            totals=totals,
            tail_silence_s=tail_silence_s,
            last_regular_tick_recv_ns=last_regular_tick_recv_ns,
        ))
    except _BudgetExceededError:
        return _emit(SessionAudit(
            schema_version=_SCHEMA_VERSION,
            session_date=day,
            vendor=vendor,
            venue=venue,
            session=session,
            status=AuditStatus.UNAVAILABLE,
            reason="time_budget",
            generated_at_ns=_safe_stamp(wall_ns),
            sources=(),
            latency=(),
            symbols=(),
            totals=_zero_totals(),
            tail_silence_s=None,
            last_regular_tick_recv_ns=None,
        ))
    except (OSError, ValueError, zstd.ZstdError):
        return _emit(SessionAudit(
            schema_version=_SCHEMA_VERSION,
            session_date=day,
            vendor=vendor,
            venue=venue,
            session=session,
            status=AuditStatus.UNAVAILABLE,
            reason="io_error",
            generated_at_ns=_safe_stamp(wall_ns),
            sources=(),
            latency=latency,
            symbols=(),
            totals=_zero_totals(),
            tail_silence_s=None,
            last_regular_tick_recv_ns=None,
        ))


def _audit_to_jsonable(audit: SessionAudit) -> dict[str, object]:
    return {
        "schema_version": audit.schema_version,
        "session_date": audit.session_date.isoformat(),
        "vendor": audit.vendor,
        "venue": audit.venue,
        "session": audit.session,
        "status": audit.status.value,
        "reason": audit.reason,
        "generated_at_ns": audit.generated_at_ns,
        "sources": [
            {
                "path": source.path,
                "stream": source.stream,
                "bytes": source.bytes,
                "rows": source.rows,
                "truncated": source.truncated,
            }
            for source in audit.sources
        ],
        "latency": [
            {
                "stream": entry.stream,
                "status": entry.status.value,
                "reason": entry.reason,
                "rows": entry.rows,
                "event_time_missing": entry.event_time_missing,
                "event_hole_seconds": entry.event_hole_seconds,
                "event_holes": [
                    {"start": hole.start, "end": hole.end, "seconds": hole.seconds} for hole in entry.event_holes
                ],
                "minutes": [
                    {
                        "minute_kst": minute.minute_kst,
                        "count": minute.count,
                        "p50_s": minute.p50_s,
                        "max_s": minute.max_s,
                        "min_s": minute.min_s,
                        "secondary_count": minute.secondary_count,
                        "secondary_max_s": minute.secondary_max_s,
                    }
                    for minute in entry.minutes
                ],
            }
            for entry in audit.latency
        ],
        "symbols": [
            {
                "symbol": entry.symbol,
                "status": entry.status,
                "frames": entry.frames,
                "trades_observed": entry.trades_observed,
                "confirmed_missing": entry.confirmed_missing,
                "count_only_missing": entry.count_only_missing,
                "net_missing": entry.net_missing,
                "volume_deficit": entry.volume_deficit,
                "duplicates": entry.duplicates,
                "regressions": entry.regressions,
                "resets": entry.resets,
                "decode_fail": entry.decode_fail,
                "intervals": [
                    {
                        "symbol": interval.symbol,
                        "phase": interval.phase,
                        "kind": interval.kind,
                        "missing_trades": interval.missing_trades,
                        "volume_deficit": interval.volume_deficit,
                        "cum_prev": interval.cum_prev,
                        "cum_next": interval.cum_next,
                        "chetime_prev": interval.chetime_prev,
                        "chetime_next": interval.chetime_next,
                        "recv_prev_ns": interval.recv_prev_ns,
                        "recv_next_ns": interval.recv_next_ns,
                        "conn_changed": interval.conn_changed,
                    }
                    for interval in entry.intervals
                ],
                "intervals_truncated": entry.intervals_truncated,
                "manifest_frames": entry.manifest_frames,
            }
            for entry in audit.symbols
        ],
        "totals": {
            "symbols": audit.totals.symbols,
            "symbols_with_loss": audit.totals.symbols_with_loss,
            "symbols_without_counter": audit.totals.symbols_without_counter,
            "trades_observed": audit.totals.trades_observed,
            "trades_received": audit.totals.trades_received,
            "confirmed_missing": audit.totals.confirmed_missing,
            "count_only_missing": audit.totals.count_only_missing,
            "net_missing_positive": audit.totals.net_missing_positive,
            "volume_deficit": audit.totals.volume_deficit,
            "loss_ratio": audit.totals.loss_ratio,
        },
        "tail_silence_s": audit.tail_silence_s,
        "last_regular_tick_recv_ns": audit.last_regular_tick_recv_ns,
    }


def write_session_audit(audit: SessionAudit, path: pathlib.Path) -> None:
    """Atomically write the audit JSON (tmp + `os.replace`); raises `OSError` on I/O failure."""
    payload = json.dumps(_audit_to_jsonable(audit), ensure_ascii=False)
    tmp = path.parent / f".{path.name}.{os.getpid()}.tmp"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)
    except OSError:
        with contextlib.suppress(OSError):
            tmp.unlink(missing_ok=True)
        raise


def latency_breaches(
    audit: SessionAudit, *, threshold_s: float, window_start: dt.time, window_end: dt.time
) -> tuple[LatencyBreach, ...]:
    """Minutes inside `[window_start, window_end)` whose max latency strictly exceeds `threshold_s`, ordered by stream then minute."""
    breaches = [
        LatencyBreach(
            stream=entry.stream,
            minute_kst=minute.minute_kst,
            max_s=minute.max_s,
            p50_s=minute.p50_s,
            count=minute.count,
        )
        for entry in audit.latency
        for minute in entry.minutes
        if window_start <= dt.time(int(minute.minute_kst[0:2]), int(minute.minute_kst[3:5])) < window_end
        and minute.max_s > threshold_s
    ]
    breaches.sort(key=lambda breach: (breach.stream, breach.minute_kst))
    return tuple(breaches)
