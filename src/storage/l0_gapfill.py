"""Derive gap-fill trade rows for L0 counter gaps from an external tick tape; never touches L0 journals."""

from __future__ import annotations

import bisect
import copy
import datetime as dt
import json
import logging
import os
import pathlib
from collections import Counter
from collections.abc import Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, NamedTuple, cast

import polars as pl

from src.core.paths import DataPaths

logger = logging.getLogger(__name__)

_TAPE_COLUMNS = ("symbol", "ts_hms", "price", "volume")
_ROW_SCHEMA: dict[str, Any] = {
    "symbol": pl.String,
    "chetime": pl.String,
    "price": pl.Int64,
    "volume": pl.Int64,
    "tape_seq": pl.Int64,
    "interval_id": pl.Int64,
    "cum_prev": pl.Int64,
    "cum_next": pl.Int64,
    "vendor": pl.String,
    "provenance": pl.String,
}


class GapfillStatus(StrEnum):
    FULL = "full"
    PARTIAL = "partial"
    NONE = "none"
    OVER = "over"


@dataclass(frozen=True)
class GapfillInterval:
    """Recovery outcome of one audited counter gap."""

    interval_id: int
    symbol: str
    chetime_prev: str
    chetime_next: str
    cum_prev: int
    cum_next: int
    missing_trades: int
    recovered: int
    residual: int
    status: GapfillStatus
    surplus: int = 0
    recovered_xvendor: int = 0


def _audit_intervals(audit: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    return [
        (str(ledger["symbol"]).zfill(6), interval)
        for ledger in audit.get("symbols", ())
        for interval in ledger.get("intervals", ())
        if int(interval.get("missing_trades", 0)) > 0 and interval.get("chetime_prev") and interval.get("chetime_next")
    ]


def _indexed_audit_intervals(audit: Mapping[str, Any]) -> list[tuple[int, str, Mapping[str, Any]]]:
    return [(interval_id, symbol, interval) for interval_id, (symbol, interval) in enumerate(_audit_intervals(audit))]


class PeerTrade(NamedTuple):
    acml: int
    chetime: int
    price: int
    qty: int


def peer_trades_for_gaps(
    audit: Mapping[str, Any], journal_root: pathlib.Path, *, peer: str, day: dt.date
) -> dict[str, list[PeerTrade]]:
    """Peer vendor's regular-session trades per symbol inside any audited interval's cum-volume span, sorted by acml; read-only."""
    from src.storage.l0_audit import _BATCH_BYTES, _decode_envelope, _iter_line_batches, _tick_body
    from src.storage.layout import L0_JOURNAL_GLOB, l0_partition_key, l0_partition_relpath

    spans: dict[str, list[tuple[int, int]]] = {}
    for ledger in audit.get("symbols", ()):
        if not isinstance(ledger, Mapping):
            continue
        symbol = ledger.get("symbol")
        intervals = ledger.get("intervals", ())
        if not isinstance(symbol, str) or not isinstance(intervals, Sequence):
            continue
        for interval in intervals:
            if not isinstance(interval, Mapping):
                continue
            try:
                missing = int(interval.get("missing_trades", 0))
            except (TypeError, ValueError):
                continue
            if missing <= 0:
                continue
            lo, hi = interval.get("acml_prev"), interval.get("acml_next_before")
            if isinstance(lo, bool) or isinstance(hi, bool) or not isinstance(lo, int) or not isinstance(hi, int):
                continue
            if hi <= lo:
                continue
            spans.setdefault(symbol.zfill(6), []).append((lo, hi))
    if not spans:
        return {}
    merged: dict[str, list[tuple[int, int]]] = {}
    starts: dict[str, list[int]] = {}
    for symbol, intervals in spans.items():
        ordered = sorted(intervals)
        compact: list[tuple[int, int]] = []
        for lo, hi in ordered:
            if compact and lo <= compact[-1][1]:
                compact[-1] = (compact[-1][0], max(hi, compact[-1][1]))
            else:
                compact.append((lo, hi))
        merged[symbol] = compact
        starts[symbol] = [lo for lo, _ in compact]
    key = l0_partition_key(peer, "krx", "regular", "H0STCNT0", day)
    assert key is not None
    partition: pathlib.Path = journal_root / l0_partition_relpath(key)
    try:
        files = sorted(partition.glob(L0_JOURNAL_GLOB), key=lambda p: p.name) if partition.is_dir() else []
    except OSError:
        return {}
    if not files:
        return {}
    found: dict[str, dict[int, PeerTrade]] = {}
    conflicts = 0
    for path in files:
        for blob in _iter_line_batches(path, _BATCH_BYTES, [False]):
            for line in blob.splitlines():
                record = _decode_envelope(line)
                if record is None:
                    continue
                body = _tick_body(peer, record.raw)
                if not body:
                    continue
                raw_symbol = body.get("shcode")
                if not isinstance(raw_symbol, str) or not raw_symbol:
                    continue
                symbol = raw_symbol.zfill(6)
                bounds = merged.get(symbol)
                if bounds is None:
                    continue
                try:
                    acml = int(str(body.get("volume")).strip())
                    qty = abs(int(str(body.get("cvolume")).strip()))
                    price = abs(int(str(body.get("price")).strip()))
                    chetime = int(str(body.get("chetime")).strip())
                except (TypeError, ValueError, AttributeError):
                    continue
                if not (0 <= chetime <= 235959):
                    continue
                index = bisect.bisect_right(starts[symbol], acml) - 1
                if index < 0 or not bounds[index][0] < acml <= bounds[index][1]:
                    continue
                per_symbol = found.setdefault(symbol, {})
                existing = per_symbol.get(acml)
                if existing is not None:
                    if (existing.price, existing.qty) != (price, qty):
                        conflicts += 1
                    continue
                per_symbol[acml] = PeerTrade(acml=acml, chetime=chetime, price=price, qty=qty)
    if conflicts > 0:
        logger.warning("[DATA] stage=xvendor_fill peer=%s conflicts=%d", peer, conflicts)
    return {symbol: [per_symbol[acml] for acml in sorted(per_symbol)] for symbol, per_symbol in found.items()}


def derive_cross_vendor_fill(
    audit: Mapping[str, Any], peers: Sequence[tuple[str, Mapping[str, Sequence[PeerTrade]]]]
) -> tuple[pl.DataFrame, dict[int, int], dict[str, Any]]:
    """Exact fill rows for audited intervals from peer trades keyed by cumulative volume; returns (rows, recovered_by_interval_id, residual_audit)."""
    items = _indexed_audit_intervals(audit)
    by_id = {interval_id: (symbol, interval) for interval_id, symbol, interval in items}
    picked: dict[int, list[tuple[PeerTrade, str]]] = {}
    rejected = 0
    for interval_id, symbol, interval in items:
        missing = int(interval.get("missing_trades", 0))
        lo, hi = interval.get("acml_prev"), interval.get("acml_next_before")
        if missing <= 0 or isinstance(lo, bool) or isinstance(hi, bool) or not isinstance(lo, int) or not isinstance(hi, int):
            continue
        if hi <= lo:
            continue
        seen: dict[int, tuple[PeerTrade, str]] = {}
        for peer_id, per_symbol in peers:
            for trade in per_symbol.get(symbol, ()):
                if lo < trade.acml <= hi and trade.acml not in seen:
                    seen[trade.acml] = (trade, peer_id)
        if not seen:
            continue
        ordered = [seen[acml] for acml in sorted(seen)]
        if sum(trade.qty for trade, _ in ordered) > hi - lo or len(ordered) > missing:
            rejected += 1
            continue
        picked[interval_id] = ordered
    entries: list[dict[str, Any]] = []
    recovered: dict[int, int] = {}
    recovered_qty: dict[int, int] = {}
    for interval_id, trades in picked.items():
        symbol, interval = by_id[interval_id]
        recovered[interval_id] = len(trades)
        recovered_qty[interval_id] = sum(trade.qty for trade, _ in trades)
        cum_prev, cum_next = int(interval["cum_prev"]), int(interval["cum_next"])
        for trade, peer_id in trades:
            entries.append(
                {
                    "symbol": symbol,
                    "chetime": f"{trade.chetime:06d}",
                    "price": trade.price,
                    "volume": trade.qty,
                    "tape_seq": trade.acml,
                    "interval_id": interval_id,
                    "cum_prev": cum_prev,
                    "cum_next": cum_next,
                    "vendor": peer_id,
                    "provenance": f"gapfill:xvendor_{peer_id}",
                }
            )
    rows = pl.DataFrame(entries, schema=_ROW_SCHEMA) if entries else pl.DataFrame(schema=_ROW_SCHEMA)
    rows = rows.sort(["symbol", "chetime", "tape_seq"])
    residual: dict[str, Any] = copy.deepcopy(dict(audit))
    res_by_id = {interval_id: interval for interval_id, _, interval in _indexed_audit_intervals(residual)}
    for interval_id, count in recovered.items():
        res_interval = cast("dict[str, Any]", res_by_id.get(interval_id))
        current_missing = int(res_interval.get("missing_trades", 0))
        res_interval["missing_trades"] = max(0, current_missing - count)
        try:
            current_deficit = int(res_interval.get("volume_deficit", 0))
        except (TypeError, ValueError):
            current_deficit = 0
        res_interval["volume_deficit"] = max(0, current_deficit - recovered_qty[interval_id])
    residual["xvendor_rejected_intervals"] = rejected
    return rows, recovered, residual


@dataclass(frozen=True)
class GapfillSource:
    """One external trade tape; `tape` needs `symbol`, `ts_hms`, `price`, `volume`."""

    vendor: str
    provenance: str
    tape: pl.DataFrame


_PICKED_SCHEMA: dict[str, Any] = {
    "symbol": pl.String,
    "ts_hms": pl.Int64,
    "price": pl.Int64,
    "volume": pl.Int64,
    "tape_seq": pl.Int64,
}
_TapeRow = tuple[int, int, int]
_Received = Mapping[tuple[str, int], Sequence[tuple[int, int, int]]]


@dataclass
class _Cluster:
    """Audited gaps of one symbol whose second ranges touch; their pooled trades cannot be told apart."""

    members: list[int]
    symbol: str
    lo: int
    hi: int


def _clusters(items: Sequence[tuple[int, str, Mapping[str, Any]]]) -> list[_Cluster]:
    clusters: list[_Cluster] = []
    open_by_symbol: dict[str, _Cluster] = {}
    for interval_id, symbol, interval in sorted(items, key=lambda it: (it[1], int(it[2]["chetime_prev"]))):
        lo, hi = int(interval["chetime_prev"]), int(interval["chetime_next"])
        current = open_by_symbol.get(symbol)
        if current is not None and lo <= current.hi:
            current.hi = max(current.hi, hi)
            current.members.append(interval_id)
            continue
        current = _Cluster([interval_id], symbol, lo, hi)
        open_by_symbol[symbol] = current
        clusters.append(current)
    return clusters


def _pooled_rows(
    by_ts: Mapping[int, Sequence[_TapeRow]], lo: int, hi: int, received: _Received | None, symbol: str
) -> list[tuple[int, _TapeRow]]:
    """Tape trades of `symbol` in seconds `[lo, hi]` that the live frames did not receive, in tape order.

    With `received` every second is reconciled as a (price, volume) multiset difference, so partly received
    boundary seconds contribute only their missing trades. Without it only seconds strictly inside the gap are
    taken: the boundary seconds may already be held by the live frames, which makes `residual` a lower bound.
    """
    rows: list[tuple[int, _TapeRow]] = []
    for ts in sorted(by_ts):
        if not lo <= ts <= hi:
            continue
        if received is None:
            if lo < ts < hi:
                rows.extend((ts, row) for row in by_ts[ts])
            continue
        have = Counter((price, volume) for _, price, volume in received.get((symbol, ts), ()))
        for row in by_ts[ts]:
            key = (row[1], row[2])
            if have[key] > 0:
                have[key] -= 1
            else:
                rows.append((ts, row))
    return rows


def _trim_surplus(pool: list[tuple[int, _TapeRow]], keep: int, deficit: int) -> list[tuple[int, _TapeRow]]:
    """Keep `keep` pooled rows in tape order.

    A single excess row is the one whose volume equals the pool's volume beyond `deficit`; otherwise the
    smallest-volume rows (latest first on ties) are dropped, which bounds the volume distortion.
    """
    drop = len(pool) - keep
    if drop == 1 and deficit > 0:
        excess = sum(row[2] for _, row in pool) - deficit
        for index in range(len(pool) - 1, -1, -1):
            if pool[index][1][2] == excess:
                return pool[:index] + pool[index + 1 :]
    smallest = sorted(range(len(pool)), key=lambda index: (pool[index][1][2], -index))[:drop]
    return [item for index, item in enumerate(pool) if index not in set(smallest)]


def derive_gapfill(
    audit: Mapping[str, Any],
    sources: Sequence[GapfillSource],
    received: _Received | None = None,
    *,
    interval_ids: Sequence[int] | None = None,
    prior_recovered: Mapping[int, int] | None = None,
) -> tuple[pl.DataFrame, tuple[GapfillInterval, ...]]:
    """Return tape trades that fill every positive-missing interval of the audit, plus per-interval outcomes.

    Gaps of one symbol whose second ranges touch form a cluster: their missing trades are pooled, the first
    source with any pooled trade supplies them, and the pool is handed to the cluster's intervals in time order
    (exact in total, best effort between intervals sharing a second). `received` maps (symbol, chetime) to the
    live frames' (cum, price, volume) and lets the boundary seconds be reconciled (see `_pooled_rows`); the
    gaps' second ranges must be covered by it. A pool larger than the cluster's missing count means the tape holds
    trades the venue never counted (duplicates): `missing` rows are kept (the row whose volume explains the excess is dropped, else the smallest-volume ones) and the excess is reported
    as `surplus` with status `over` on the cluster's last interval. Raises `ValueError` when a tape lacks `symbol`, `ts_hms`, `price` or `volume`.
    """
    indexed_sources: list[tuple[GapfillSource, dict[str, dict[int, list[_TapeRow]]]]] = []
    for source in sources:
        missing_columns = [name for name in _TAPE_COLUMNS if name not in source.tape.columns]
        if missing_columns:
            raise ValueError(f"tape lacks columns {missing_columns}")
        by_symbol: dict[str, dict[int, list[_TapeRow]]] = {}
        for seq, symbol, ts, price, volume in source.tape.select(_TAPE_COLUMNS).with_row_index("tape_seq").iter_rows():
            by_symbol.setdefault(symbol, {}).setdefault(int(ts), []).append((int(seq), int(price), int(volume)))
        indexed_sources.append((source, by_symbol))
    base = _audit_intervals(audit)
    if interval_ids is not None:
        pinned = [int(item) for item in interval_ids]
        if len(pinned) != len(base) or len(set(pinned)) != len(pinned):
            raise ValueError(f"interval_ids length or uniqueness invalid for {len(base)} audited intervals")
        items = [(pinned[pos], symbol, interval) for pos, (symbol, interval) in enumerate(base)]
    else:
        items = [(interval_id, symbol, interval) for interval_id, (symbol, interval) in enumerate(base)]
    by_id = {interval_id: (symbol, interval) for interval_id, symbol, interval in items}
    parts: list[pl.DataFrame] = []
    outcomes: dict[int, GapfillInterval] = {}
    for cluster in _clusters(items):
        members = [by_id[interval_id][1] for interval_id in cluster.members]
        chosen: GapfillSource | None = None
        pool: list[tuple[int, _TapeRow]] = []
        for candidate, by_symbol in indexed_sources:
            pool = _pooled_rows(by_symbol.get(cluster.symbol, {}), cluster.lo, cluster.hi, received, cluster.symbol)
            if pool:
                chosen = candidate
                break
        cluster_missing = sum(int(interval["missing_trades"]) for interval in members)
        surplus = max(0, len(pool) - cluster_missing)
        if surplus:
            pool = _trim_surplus(pool, cluster_missing, sum(int(interval.get("volume_deficit", 0)) for interval in members))
        remaining = len(pool)
        for position, (interval_id, interval) in enumerate(zip(cluster.members, members, strict=True)):
            residual_missing = int(interval["missing_trades"])
            prior = int(prior_recovered.get(interval_id, 0)) if prior_recovered else 0
            original_missing = residual_missing + prior
            last = position == len(members) - 1
            tape_recovered = min(residual_missing, remaining)
            remaining -= tape_recovered
            total_recovered = prior + tape_recovered
            if last and surplus:
                status = GapfillStatus.OVER
            elif total_recovered == 0:
                status = GapfillStatus.NONE
            elif total_recovered < original_missing:
                status = GapfillStatus.PARTIAL
            else:
                status = GapfillStatus.FULL
            outcomes[interval_id] = GapfillInterval(
                interval_id=interval_id,
                symbol=cluster.symbol,
                chetime_prev=str(interval["chetime_prev"]),
                chetime_next=str(interval["chetime_next"]),
                cum_prev=int(interval["cum_prev"]),
                cum_next=int(interval["cum_next"]),
                missing_trades=original_missing,
                recovered=total_recovered,
                residual=max(0, original_missing - total_recovered),
                status=status,
                surplus=surplus if last else 0,
                recovered_xvendor=prior,
            )
        if pool and chosen is not None:
            cursor = 0
            for interval_id in cluster.members:
                take = outcomes[interval_id].recovered - outcomes[interval_id].recovered_xvendor
                frame = pl.DataFrame(
                    [
                        {"symbol": cluster.symbol, "ts_hms": ts, "price": price, "volume": volume, "tape_seq": seq}
                        for ts, (seq, price, volume) in pool[cursor : cursor + take]
                    ],
                    schema=_PICKED_SCHEMA,
                )
                cursor += take
                if frame.height == 0:
                    continue
                parts.append(
                    frame.select(
                        pl.col("symbol"),
                        pl.col("ts_hms").cast(pl.String).str.zfill(6).alias("chetime"),
                        pl.col("price"),
                        pl.col("volume"),
                        pl.col("tape_seq"),
                        pl.lit(interval_id, dtype=pl.Int64).alias("interval_id"),
                        pl.lit(outcomes[interval_id].cum_prev, dtype=pl.Int64).alias("cum_prev"),
                        pl.lit(outcomes[interval_id].cum_next, dtype=pl.Int64).alias("cum_next"),
                        pl.lit(chosen.vendor).alias("vendor"),
                        pl.lit(chosen.provenance).alias("provenance"),
                    )
                )
    rows = pl.concat(parts) if parts else pl.DataFrame(schema=_ROW_SCHEMA)
    return rows.sort(["symbol", "chetime", "tape_seq"]), tuple(outcomes[i] for i in sorted(outcomes))


def summarize_gapfill(outcomes: Sequence[GapfillInterval]) -> dict[str, int]:
    """Totals for the report header; `residual` sums per-interval lower bounds."""
    return {
        "intervals": len(outcomes),
        "missing_trades": sum(item.missing_trades for item in outcomes),
        "recovered": sum(item.recovered for item in outcomes),
        "residual": sum(item.residual for item in outcomes),
    }


def write_gapfill(
    rows: pl.DataFrame,
    outcomes: Sequence[GapfillInterval],
    *,
    rows_path: pathlib.Path,
    report_path: pathlib.Path,
    notes: Mapping[str, Any] | None = None,
) -> None:
    """Atomically write the gap-fill rows (zstd parquet) and the JSON report (with optional `notes`); raises `OSError` on I/O failure."""
    report = {
        "totals": summarize_gapfill(outcomes),
        "notes": dict(notes or {}),
        "intervals": [asdict(item) for item in outcomes],
    }
    for target, writer in (
        (rows_path, lambda tmp: rows.write_parquet(tmp, compression="zstd")),
        (report_path, lambda tmp: tmp.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")),
    ):
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
        try:
            writer(tmp)
            os.replace(tmp, target)
        except OSError:
            tmp.unlink(missing_ok=True)
            raise


def received_trades_for_gaps(
    audit: Mapping[str, Any], journal_root: pathlib.Path, *, vendor: str, day: dt.date
) -> dict[tuple[str, int], list[tuple[int, int, int]]]:
    """Live (cum, price, volume) per (symbol, chetime) for every second touched by an audited gap; read-only."""
    from src.storage.l0_audit import iter_received_ticks

    ranges: dict[str, list[tuple[int, int]]] = {}
    for ledger in audit.get("symbols", ()):
        if not isinstance(ledger, Mapping):
            continue
        symbol = ledger.get("symbol")
        intervals = ledger.get("intervals", ())
        if not isinstance(symbol, str) or not isinstance(intervals, Sequence):
            continue
        for interval in intervals:
            if not isinstance(interval, Mapping):
                continue
            prev = interval.get("chetime_prev")
            nxt = interval.get("chetime_next")
            if prev is None or nxt is None:
                continue
            try:
                lo, hi = int(prev), int(nxt)
            except (TypeError, ValueError):
                continue
            if lo <= hi:
                ranges.setdefault(symbol.zfill(6), []).append((lo, hi))
    wanted: dict[str, list[tuple[int, int]]] = {}
    for symbol, intervals in ranges.items():
        merged: list[tuple[int, int]] = []
        for lo, hi in sorted(intervals):
            if merged and lo <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(hi, merged[-1][1]))
            else:
                merged.append((lo, hi))
        wanted[symbol] = merged
    found: dict[tuple[str, int], list[tuple[int, int, int]]] = {}
    for tick in iter_received_ticks(journal_root, vendor=vendor, venue="krx", session="regular", day=day):
        symbol = tick.symbol.zfill(6)
        spans = wanted.get(symbol, [])
        index = bisect.bisect_right(spans, (tick.chetime, float("inf"))) - 1
        if index >= 0 and tick.chetime <= spans[index][1]:
            found.setdefault((symbol, tick.chetime), []).append((tick.cum, tick.price, tick.volume))
    return found


def load_tape_for_symbols(
    paths: DataPaths, day: dt.date, symbols: AbstractSet[str]
) -> pl.DataFrame:
    """Kiwoom tape rows (`symbol`, `ts_hms`, `price`, `volume`) of `day` restricted to `symbols`; empty frame with that schema when no partition exists."""
    schema: dict[str, Any] = {
        "symbol": pl.String,
        "ts_hms": pl.Int64,
        "price": pl.Int64,
        "volume": pl.Int64,
    }
    wanted = {str(symbol).zfill(6) for symbol in symbols if str(symbol)}
    if not wanted:
        return pl.DataFrame(schema=schema)
    partition = paths.tick_tape_root / f"dt={day.isoformat()}"
    try:
        files = sorted(partition.glob("*.parquet")) if partition.is_dir() else []
    except OSError:
        files = []
    if not files:
        return pl.DataFrame(schema=schema)
    frame = (
        pl.scan_parquet(list(files))
        .select(
            pl.col("symbol").cast(pl.String).str.zfill(6).alias("symbol"),
            pl.col("ts_hms").cast(pl.Int64),
            pl.col("price").cast(pl.Int64),
            pl.col("volume").cast(pl.Int64),
        )
        .filter(pl.col("symbol").is_in(wanted))
        .collect(engine="streaming")
    )
    return frame.select(list(schema)).cast(schema)  # type: ignore[arg-type]
