"""Offline analysis of feed_probe captures against the same day's L0 journals (feed resilience Part 1).

Reads capture.<vendor>.<step>.jsonl and summary.<vendor>.json written by tools/feed_probe.py, decodes what the existing
decoders can decode, compares trades with reference L0 journals, and writes report.json and report.md.
LS NXT and DB frames are decoded here; Toss is raw_only. Vendors without a decoder are reported as not_decoded or
raw_only and fail their gate, never silently pass.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import glob
import itertools
import json
import math
import pathlib
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

from src.realtime.contracts import L0Frame
from src.realtime.lag_monitor import event_second_of
from src.storage.l0_audit import (
    _BATCH_BYTES,
    ReceivedTick,
    _decode_envelope,
    _decode_received_tick,
    _iter_line_batches,
    _tick_body,
)
from src.storage.layout import L0_JOURNAL_GLOB, l0_partition_key, l0_partition_relpath

_KST = ZoneInfo("Asia/Seoul")
_NS = 1_000_000_000
_CONFLATION_OK = 0.995
_BOOK_LEVELS = 10
_KRX_TICK_STREAM = "H0STCNT0"
_NXT_TICK_STREAM = "H0NXCNT0"
_DB_TRADE_TR = "S00"
_DB_BOOK_TR = "S01"
_DB_TRADE_EVENT_FIELD = "StckCntghour"
_DB_BOOK_EVENT_FIELD = "BsopHour"
_CAPTURE_RE = re.compile(r"capture\.(?P<vendor>[a-z-]+)\.(?P<step>\d+)\.jsonl")
_TOSS_TRADE_PREFIX = "trade:kr:"
_TOSS_BOOK_PREFIX = "orderbook:kr:"
_DB_BOOK_GATE = "G-PROBE-DB-BOOK"
_DB_TICK_GATE = "G-PROBE-DB-TICK"


@dataclasses.dataclass(frozen=True)
class ProbeCriteria:
    min_accepted_pairs: int = 50
    conflation_p50: float = _CONFLATION_OK
    book_completeness: float = 0.99
    liquid_top_n: int = 10
    toss_trade_match: float = 0.85


@dataclasses.dataclass(frozen=True)
class DecodedFrame:
    recv_wall_ns: int
    symbol: str
    event_second: int | None


@dataclasses.dataclass(frozen=True)
class DecodedBook:
    recv_wall_ns: int
    symbol: str
    asks: tuple[tuple[int, int], ...]
    bids: tuple[tuple[int, int], ...]


@dataclasses.dataclass(frozen=True)
class LagStats:
    count: int
    missing: int
    p50_s: float | None
    p99_s: float | None
    max_s: float | None
    event_hole_seconds: int


@dataclasses.dataclass(frozen=True)
class ConflationStats:
    symbol_minutes: int
    p50_ratio: float | None
    share_below_threshold: float | None
    extra_minutes: int


@dataclasses.dataclass(frozen=True)
class TradeMatchStats:
    symbol_minutes: int
    p50_matched_share: float | None
    unexplained: int


@dataclasses.dataclass(frozen=True)
class BookStats:
    frames: int
    completeness: float | None
    crossed: int
    monotone_violations: int


@dataclasses.dataclass(frozen=True)
class RateStats:
    frames: int
    max_per_window: int
    mean_per_s: float | None


def _nearest_rank(values: Sequence[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(p * len(ordered)) - 1)]


def _second_of_day_kst(recv_wall_ns: int) -> int:
    moment = dt.datetime.fromtimestamp(recv_wall_ns / _NS, _KST)
    return moment.hour * 3600 + moment.minute * 60 + moment.second


def lag_stats(frames: Sequence[DecodedFrame]) -> LagStats:
    """Receive second minus exchange event second, clamped at zero like LagMonitor; frames without event time are counted."""
    lags: list[float] = []
    seconds: set[int] = set()
    missing = 0
    for frame in frames:
        if frame.event_second is None:
            missing += 1
            continue
        seconds.add(frame.event_second)
        lags.append(float(max(0, _second_of_day_kst(frame.recv_wall_ns) - frame.event_second)))
    holes = 0
    if seconds:
        holes = (max(seconds) - min(seconds) + 1) - len(seconds)
    return LagStats(
        count=len(lags),
        missing=missing,
        p50_s=_nearest_rank(lags, 0.50),
        p99_s=_nearest_rank(lags, 0.99),
        max_s=max(lags) if lags else None,
        event_hole_seconds=holes,
    )


def _minute_key(tick: ReceivedTick) -> tuple[str, int]:
    return (tick.symbol.zfill(6), tick.chetime // 100)


def conflation_ratio(
    candidate: Sequence[ReceivedTick],
    reference: Sequence[ReceivedTick],
    *,
    scope: Mapping[str, tuple[int, int]] | None = None,
) -> ConflationStats:
    """Per symbol-minute share of reference trades (matched by cumulative trade count) the candidate also printed.

    scope maps each symbol to the HHMMSS span the candidate was observing; reference trades outside it are not judged.
    None judges every reference trade.
    """
    cand: dict[tuple[str, int], list[int]] = defaultdict(list)
    ref: dict[tuple[str, int], list[int]] = defaultdict(list)
    for tick in candidate:
        cand[_minute_key(tick)].append(tick.cum)
    for tick in reference:
        span = None if scope is None else scope.get(tick.symbol.zfill(6))
        if scope is None or (span is not None and span[0] <= tick.chetime <= span[1]):
            ref[_minute_key(tick)].append(tick.cum)
    ratios: list[float] = []
    for key, ref_cums in ref.items():
        ref_set = set(ref_cums)
        matched = len(ref_set & set(cand.get(key, [])))
        ratios.append(matched / len(ref_set))
    extra = sum(1 for key, cums in cand.items() if len(cums) > len(ref.get(key, [])))
    below = sum(1 for r in ratios if r < _CONFLATION_OK)
    return ConflationStats(
        symbol_minutes=len(ratios),
        p50_ratio=_nearest_rank(ratios, 0.50),
        share_below_threshold=(below / len(ratios)) if ratios else None,
        extra_minutes=extra,
    )


def trade_match(
    candidate: Sequence[ReceivedTick],
    reference: Sequence[ReceivedTick],
    *,
    scope: Mapping[str, tuple[int, int]],
) -> TradeMatchStats:
    """Exact (HHMMSS, price, volume) multiset match of in-span reference trades per symbol-minute.

    Used for unified streams that carry no cumulative count. unexplained counts candidate trades with no reference twin.
    """
    cand: dict[tuple[str, int], Counter[tuple[int, int, int]]] = defaultdict(Counter)
    ref: dict[tuple[str, int], Counter[tuple[int, int, int]]] = defaultdict(Counter)
    for tick in candidate:
        cand[_minute_key(tick)][(tick.chetime, tick.price, tick.volume)] += 1
    for tick in reference:
        span = scope.get(tick.symbol.zfill(6))
        if span is not None and span[0] <= tick.chetime <= span[1]:
            ref[_minute_key(tick)][(tick.chetime, tick.price, tick.volume)] += 1
    unexplained = 0
    for key, cand_counts in cand.items():
        matched = sum((cand_counts & ref.get(key, Counter())).values())
        unexplained += sum(cand_counts.values()) - matched
    shares: list[float] = []
    for key, ref_counts in ref.items():
        matched = sum((ref_counts & cand.get(key, Counter())).values())
        shares.append(matched / sum(ref_counts.values()))
    return TradeMatchStats(
        symbol_minutes=len(shares),
        p50_matched_share=_nearest_rank(shares, 0.50),
        unexplained=unexplained,
    )


def book_completeness(frames: Sequence[DecodedBook]) -> BookStats:
    """Populated levels over 2 * 10 per frame; crossed books and non-monotone level prices are counted."""
    shares: list[float] = []
    crossed = 0
    monotone = 0
    for frame in frames:
        ask_pop = sum(1 for price, qty in frame.asks if price > 0 and qty > 0)
        bid_pop = sum(1 for price, qty in frame.bids if price > 0 and qty > 0)
        shares.append((ask_pop + bid_pop) / (2 * _BOOK_LEVELS))
        asks = [p for p, q in frame.asks if p > 0 and q > 0]
        bids = [p for p, q in frame.bids if p > 0 and q > 0]
        if asks and bids and min(asks) <= max(bids):
            crossed += 1
        if any(b < a for a, b in itertools.pairwise(asks)):
            monotone += 1
        if any(b > a for a, b in itertools.pairwise(bids)):
            monotone += 1
    return BookStats(
        frames=len(frames),
        completeness=(sum(shares) / len(shares)) if shares else None,
        crossed=crossed,
        monotone_violations=monotone,
    )


def rate_stats(frames: Sequence[DecodedFrame], *, window_s: float) -> RateStats:
    """Peak frame count in any window of window_s and the mean rate over the observed span."""
    times = sorted(f.recv_wall_ns for f in frames)
    if not times:
        return RateStats(frames=0, max_per_window=0, mean_per_s=None)
    window_ns = int(window_s * _NS)
    peak = 0
    lo = 0
    for hi, stamp in enumerate(times):
        while stamp - times[lo] >= window_ns:
            lo += 1
        peak = max(peak, hi - lo + 1)
    span_s = (times[-1] - times[0]) / _NS
    mean = len(times) / span_s if span_s > 0 else None
    return RateStats(frames=len(times), max_per_window=peak, mean_per_s=mean)


def decide_gate(results: Mapping[str, bool]) -> bool:
    """A gate passes only when it has criteria and every one of them holds."""
    return bool(results) and all(results.values())


class _Capture(NamedTuple):
    recv_wall_ns: int
    raw: str


def _read_captures(capture_dir: pathlib.Path, vendor: str) -> list[_Capture]:
    rows: list[_Capture] = []
    for path in sorted(glob.glob(str(capture_dir / f"capture.{vendor}.*.jsonl"))):
        if _CAPTURE_RE.fullmatch(pathlib.Path(path).name) is None:
            continue
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                entry = json.loads(line)
                rows.append(_Capture(int(entry["recv_wall_ns"]), str(entry["raw"])))
    return rows


def _decode_ls_nxt(rows: Iterable[_Capture]) -> tuple[list[DecodedFrame], list[DecodedBook], list[ReceivedTick]]:
    frames: list[DecodedFrame] = []
    books: list[DecodedBook] = []
    trades: list[ReceivedTick] = []
    for row in rows:
        try:
            header = json.loads(row.raw).get("header", {})
        except (ValueError, AttributeError):
            continue
        tr_cd = header.get("tr_cd") if isinstance(header, dict) else None
        body = _tick_body("ls", row.raw)
        symbol = str(header.get("tr_key", "")).strip()[1:] if isinstance(header, dict) else ""
        if tr_cd == "NS3":
            stamp = str(body.get("chetime", ""))
            frame = L0Frame("ls", "H0STCNT0", symbol, row.raw, 0, row.recv_wall_ns, 0, "", exchange_event_time=stamp)
            frames.append(DecodedFrame(row.recv_wall_ns, symbol, event_second_of(frame)))
            tick = _decode_received_tick(body)
            if tick is not None:
                trades.append(tick)
        elif tr_cd == "NH1":
            book = _book_from_body(row.recv_wall_ns, symbol, body)
            if book is not None:
                books.append(book)
    return frames, books, trades


def _db_field(body: Mapping[str, object], name: str, missing: Counter[str]) -> object:
    if name not in body:
        missing[f"field_missing:{name}"] += 1
        return None
    return body[name]


def _db_symbol(header: Mapping[str, object], body: Mapping[str, object]) -> str:
    iscd = body.get("ShrnIscd")
    if isinstance(iscd, str) and iscd.strip():
        return iscd.strip()
    tr_key = header.get("tr_key")
    return tr_key.removeprefix("J ").strip() if isinstance(tr_key, str) else ""


def _second_to_hhmmss(second: int) -> int:
    return (second // 3600) * 10000 + ((second % 3600) // 60) * 100 + second % 60


def _db_levels(
    body: Mapping[str, object], price_prefix: str, qty_prefix: str, missing: Counter[str]
) -> tuple[tuple[int, int], ...]:
    levels: list[tuple[int, int]] = []
    for n in range(1, _BOOK_LEVELS + 1):
        price = _as_int(_db_field(body, f"{price_prefix}{n}", missing))
        qty = _as_int(_db_field(body, f"{qty_prefix}{n}", missing))
        levels.append((price or 0, qty or 0))
    return tuple(levels)


def _decode_db(
    rows: Iterable[_Capture], missing: Counter[str]
) -> tuple[list[DecodedFrame], list[DecodedBook], list[ReceivedTick]]:
    """Decode DB S00 trades and S01 books straight from raw JSON; absent or unparsable fields are counted in `missing`."""
    frames: list[DecodedFrame] = []
    books: list[DecodedBook] = []
    trades: list[ReceivedTick] = []
    for row in rows:
        try:
            entry = json.loads(row.raw)
        except ValueError:
            missing["unparsable_frame"] += 1
            continue
        header = entry.get("header") if isinstance(entry, dict) else None
        body = entry.get("body") if isinstance(entry, dict) else None
        if not isinstance(header, dict) or not isinstance(body, dict) or "rsp_cd" in header:
            continue
        tr_cd = header.get("tr_cd")
        if tr_cd not in (_DB_TRADE_TR, _DB_BOOK_TR):
            continue
        symbol = _db_symbol(header, body)
        if not symbol:
            missing["field_missing:ShrnIscd"] += 1
            continue
        if tr_cd == _DB_TRADE_TR:
            stamp = _db_field(body, _DB_TRADE_EVENT_FIELD, missing)
            frame = L0Frame(
                "db",
                _KRX_TICK_STREAM,
                symbol,
                row.raw,
                0,
                row.recv_wall_ns,
                0,
                "",
                exchange_event_time=str(stamp or ""),
            )
            second = event_second_of(frame)
            frames.append(DecodedFrame(row.recv_wall_ns, symbol, second))
            price = _as_int(_db_field(body, "StckPrpr", missing))
            volume = _as_int(_db_field(body, "CntgVol", missing))
            sell_count = _as_int(_db_field(body, "SelnCntgcsnu", missing))
            buy_count = _as_int(_db_field(body, "ShnuCntgcsnu", missing))
            if second is None or price is None or volume is None or sell_count is None or buy_count is None:
                continue
            cum = sell_count + buy_count
            trades.append(ReceivedTick(symbol, _second_to_hhmmss(second), cum, price, volume))
        else:
            _db_field(body, _DB_BOOK_EVENT_FIELD, missing)
            books.append(
                DecodedBook(
                    row.recv_wall_ns,
                    symbol,
                    _db_levels(body, "Askp", "AskpRsqn", missing),
                    _db_levels(body, "Bidp", "BidpRsqn", missing),
                )
            )
    return frames, books, trades


def _toss_event_second(stamp: object) -> int | None:
    if not isinstance(stamp, str):
        return None
    try:
        moment = dt.datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    local = moment.astimezone(_KST)
    return local.hour * 3600 + local.minute * 60 + local.second


def _toss_levels(levels: object) -> tuple[tuple[int, int], ...]:
    out: list[tuple[int, int]] = []
    if isinstance(levels, list):
        for item in levels[:_BOOK_LEVELS]:
            price = _as_int(item.get("price")) if isinstance(item, dict) else None
            qty = _as_int(item.get("volume")) if isinstance(item, dict) else None
            out.append((price or 0, qty or 0))
    out.extend([(0, 0)] * (_BOOK_LEVELS - len(out)))
    return tuple(out)


def _decode_toss(rows: Iterable[_Capture]) -> tuple[list[DecodedFrame], list[DecodedBook], list[ReceivedTick]]:
    """Decode Toss unified-stream trade and orderbook data frames; PONG and control frames are skipped."""
    frames: list[DecodedFrame] = []
    books: list[DecodedBook] = []
    trades: list[ReceivedTick] = []
    for row in rows:
        try:
            entry = json.loads(row.raw)
        except ValueError:
            continue
        if not isinstance(entry, dict) or entry.get("type") != "message":
            continue
        topic = entry.get("topic")
        data = entry.get("data")
        if not isinstance(topic, str) or not isinstance(data, dict):
            continue
        if topic.startswith(_TOSS_TRADE_PREFIX):
            symbol = topic.removeprefix(_TOSS_TRADE_PREFIX)
            second = _toss_event_second(data.get("timestamp"))
            frames.append(DecodedFrame(row.recv_wall_ns, symbol, second))
            price = _as_int(data.get("price"))
            volume = _as_int(data.get("volume"))
            if second is None or price is None or volume is None:
                continue
            trades.append(ReceivedTick(symbol, _second_to_hhmmss(second), 0, price, volume))
        elif topic.startswith(_TOSS_BOOK_PREFIX):
            symbol = topic.removeprefix(_TOSS_BOOK_PREFIX)
            books.append(
                DecodedBook(row.recv_wall_ns, symbol, _toss_levels(data.get("asks")), _toss_levels(data.get("bids")))
            )
    return frames, books, trades


def _book_from_body(recv_wall_ns: int, symbol: str, body: Mapping[str, object]) -> DecodedBook | None:
    def level(side: str, n: int) -> tuple[int, int]:
        price = _as_int(body.get(f"{'offer' if side == 'ask' else 'bid'}ho{n}"))
        qty = _as_int(body.get(f"{'offer' if side == 'ask' else 'bid'}rem{n}"))
        return (price or 0, qty or 0)

    if not body:
        return None
    asks = tuple(level("ask", n) for n in range(1, _BOOK_LEVELS + 1))
    bids = tuple(level("bid", n) for n in range(1, _BOOK_LEVELS + 1))
    return DecodedBook(recv_wall_ns, symbol, asks, bids)


def _as_int(value: object) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _minute_of(recv_wall_ns: int) -> int:
    return _second_of_day_kst(recv_wall_ns) // 60


def _trade_spans(trades: Sequence[ReceivedTick]) -> dict[str, tuple[int, int]]:
    """Per symbol, the first and last HHMMSS at which the candidate printed a trade."""
    spans: dict[str, tuple[int, int]] = {}
    for trade in trades:
        symbol = trade.symbol.zfill(6)
        low, high = spans.get(symbol, (trade.chetime, trade.chetime))
        spans[symbol] = (min(low, trade.chetime), max(high, trade.chetime))
    return spans


def _reference_rows(
    journal_root: pathlib.Path, day: dt.date, references: Sequence[str]
) -> tuple[list[ReceivedTick], list[DecodedFrame]]:
    """Read same-day regular-session L0 trade journals once: decoded trades and per-record lag samples.

    References look like 'ls-krx' or 'kis-nxt'; NXT venues use the H0NXCNT0 stream, everything else H0STCNT0.
    """
    ticks: list[ReceivedTick] = []
    lags: list[DecodedFrame] = []
    for ref in references:
        vendor, _, venue = ref.partition("-")
        venue = venue or "krx"
        stream = _NXT_TICK_STREAM if venue == "nxt" else _KRX_TICK_STREAM
        key = l0_partition_key(vendor, venue, "regular", stream, day)
        if key is None:
            continue
        for path in sorted((journal_root / l0_partition_relpath(key)).glob(L0_JOURNAL_GLOB), key=lambda p: p.name):
            for blob in _iter_line_batches(path, _BATCH_BYTES, [False]):
                for line in blob.splitlines():
                    record = _decode_envelope(line)
                    if record is None or not record.symbol:
                        continue
                    frame = L0Frame(
                        vendor,
                        stream,
                        record.symbol,
                        record.raw,
                        0,
                        record.recv_ns,
                        0,
                        record.conn_id,
                        exchange_event_time=record.exchange_event_time,
                    )
                    lags.append(DecodedFrame(record.recv_ns, record.symbol, event_second_of(frame)))
                    tick = _decode_received_tick(_tick_body(vendor, record.raw))
                    if tick is not None:
                        ticks.append(tick)
    return ticks, lags


def analyze(
    capture_dir: pathlib.Path,
    journal_root: pathlib.Path,
    day: dt.date,
    *,
    vendor: str,
    reference_vendors: Sequence[str],
    criteria: ProbeCriteria | None = None,
) -> dict[str, Any]:
    """Build the gate report for one probed vendor; reference_vendors look like 'ls-krx' or 'kis-nxt'."""
    summary_path = capture_dir / f"summary.{vendor}.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    rows = _read_captures(capture_dir, vendor)
    result: dict[str, Any] = {
        "vendor": vendor,
        "day": day.isoformat(),
        "frames": len(rows),
        "accepted_pairs": summary.get("cap_pairs"),
        "decode": "raw_only",
    }
    if "second_session" in summary:
        result["second_session"] = summary["second_session"]
    criteria = criteria or ProbeCriteria()
    frames: list[DecodedFrame] = []
    books: list[DecodedBook] = []
    trades: list[ReceivedTick] = []
    missing: Counter[str] = Counter()
    if vendor == "ls-nxt":
        frames, books, trades = _decode_ls_nxt(rows)
        result["decode"] = "decoded"
    elif vendor == "db":
        frames, books, trades = _decode_db(rows, missing)
        result["decode"] = "decoded"
    elif vendor == "toss":
        frames, books, trades = _decode_toss(rows)
        result["decode"] = "decoded"
    result["field_missing"] = dict(sorted(missing.items()))
    result["rate"] = dataclasses.asdict(
        rate_stats(frames or [DecodedFrame(r.recv_wall_ns, "", None) for r in rows], window_s=1.0)
    )
    result["lag"] = dataclasses.asdict(lag_stats(frames))
    ref_trades, ref_lags = _reference_rows(journal_root, day, reference_vendors) if reference_vendors else ([], [])
    if vendor == "toss":
        result["trade_match"] = dataclasses.asdict(trade_match(trades, ref_trades, scope=_trade_spans(trades)))
    else:
        result["conflation"] = dataclasses.asdict(conflation_ratio(trades, ref_trades, scope=_trade_spans(trades)))
    keys = {(f.symbol, _minute_of(f.recv_wall_ns)) for f in frames}
    matched_ref_lags = [r for r in ref_lags if (r.symbol, _minute_of(r.recv_wall_ns)) in keys]
    result["reference_lag"] = dataclasses.asdict(lag_stats(matched_ref_lags))
    counts = Counter(f.symbol for f in frames)
    liquid = {symbol for symbol, _ in counts.most_common(criteria.liquid_top_n)}
    liquid_books = [b for b in books if b.symbol in liquid]
    result["book"] = dataclasses.asdict(book_completeness(liquid_books))
    result["gates"] = _gates(vendor, result, criteria)
    if vendor == "db":
        result["gates_split"] = _db_gates_split(result, criteria)
    return result


def _lag_within_reference(lag: Mapping[str, Any], reference: Mapping[str, Any]) -> bool:
    """Candidate p50 and p99 both at or below the reference p99 over the same symbol-minutes; no sample means fail."""
    ref_p99 = reference.get("p99_s")
    if lag.get("p50_s") is None or lag.get("p99_s") is None or ref_p99 is None:
        return False
    return lag["p50_s"] <= ref_p99 and lag["p99_s"] <= ref_p99


def _accepted_ok(result: Mapping[str, Any], criteria: ProbeCriteria) -> bool:
    return (result.get("accepted_pairs") or 0) >= criteria.min_accepted_pairs


def _book_ok(result: Mapping[str, Any], criteria: ProbeCriteria) -> bool:
    book = result["book"]["completeness"]
    return book is not None and book >= criteria.book_completeness


def _share_ok(share: float | None, threshold: float) -> bool:
    return share is not None and share >= threshold


def _gate(name: str, checks: dict[str, bool], result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "pass": decide_gate(checks),
        "criteria": checks,
        "note": f"reference lag samples on matched symbol-minutes: {result['reference_lag']['count']}",
    }


def _db_gates_split(result: Mapping[str, Any], criteria: ProbeCriteria) -> dict[str, Any]:
    """Book gate (accepted pairs, book, lag) and tick gate (conflation) for DB; both must pass for the legacy verdict."""
    book_checks = {
        "accepted_pairs": _accepted_ok(result, criteria),
        "book_completeness": _book_ok(result, criteria),
        "lag_within_reference": _lag_within_reference(result["lag"], result["reference_lag"]),
    }
    conf = result["conflation"]["p50_ratio"]
    tick_checks = {"conflation_p50": conf is not None and conf >= criteria.conflation_p50}
    return {
        _DB_BOOK_GATE: _gate(_DB_BOOK_GATE, book_checks, result),
        _DB_TICK_GATE: _gate(_DB_TICK_GATE, tick_checks, result),
    }


def _gates(vendor: str, result: Mapping[str, Any], criteria: ProbeCriteria) -> dict[str, Any]:
    name = f"G-PROBE-{vendor.upper().replace('-', '')}"
    if result["decode"] != "decoded":
        return {"name": name, "pass": False, "criteria": {}, "note": "not_decoded: no verified schema for this vendor"}
    if vendor == "db":
        split = _db_gates_split(result, criteria)
        checks = {key: value for gate in split.values() for key, value in gate["criteria"].items()}
        return _gate(name, checks, result) | {"pass": all(gate["pass"] for gate in split.values())}
    if vendor == "toss":
        share = result["trade_match"]["p50_matched_share"]
        checks = {
            "accepted_pairs": _accepted_ok(result, criteria),
            "book_completeness": _book_ok(result, criteria),
            "lag_within_reference": _lag_within_reference(result["lag"], result["reference_lag"]),
            "trade_match_p50": _share_ok(share, criteria.toss_trade_match),
        }
        return _gate(name, checks, result)
    conf = result["conflation"]["p50_ratio"]
    checks = {
        "accepted_pairs": _accepted_ok(result, criteria),
        "conflation_p50": conf is not None and conf >= criteria.conflation_p50,
        "book_completeness": _book_ok(result, criteria),
        "lag_within_reference": _lag_within_reference(result["lag"], result["reference_lag"]),
    }
    return _gate(name, checks, result)


def _markdown(report: Mapping[str, Any]) -> str:
    gate = report["gates"]
    lines = [
        f"# Feed probe report: {report['vendor']} ({report['day']})",
        "",
        f"- frames: {report['frames']}",
        f"- accepted pairs: {report['accepted_pairs']}",
        f"- decode: {report['decode']}",
        f"- gate {gate['name']}: {'PASS' if gate['pass'] else 'FAIL'}",
        f"- note: {gate.get('note', '')}",
        "",
    ]
    lines.extend(
        f"- gate {sub['name']}: {'PASS' if sub['pass'] else 'FAIL'} {sub['criteria']}"
        for sub in report.get("gates_split", {}).values()
    )
    if "gates_split" in report:
        lines.append("")
    for key in ("rate", "lag", "reference_lag", "conflation", "trade_match", "book", "field_missing", "second_session"):
        if key not in report:
            continue
        lines.append(f"## {key}")
        lines.extend(f"- {k}: {v}" for k, v in report[key].items())
        lines.append("")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="feed_probe_report.py", description="Analyze feed_probe captures offline.")
    parser.add_argument("--capture-dir", type=pathlib.Path, required=True)
    parser.add_argument("--journal-root", type=pathlib.Path, required=True)
    parser.add_argument("--day", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--vendor", required=True)
    parser.add_argument("--reference", action="append", default=[], help="e.g. ls-krx, kis-nxt (repeatable)")
    args = parser.parse_args(argv)
    report = analyze(
        args.capture_dir,
        args.journal_root,
        args.day,
        vendor=args.vendor,
        reference_vendors=args.reference,
    )
    (args.capture_dir / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (args.capture_dir / "report.md").write_text(_markdown(report), encoding="utf-8")
    print(json.dumps({"gate": report["gates"]["name"], "pass": report["gates"]["pass"]}))  # noqa: T201
    return 0 if report["gates"]["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
