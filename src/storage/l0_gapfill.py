"""Derive gap-fill trade rows for L0 counter gaps from an external tick tape; never touches L0 journals."""

from __future__ import annotations

import json
import os
import pathlib
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

import polars as pl

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


def _audit_intervals(audit: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    return [
        (str(ledger["symbol"]), interval)
        for ledger in audit.get("symbols", ())
        for interval in ledger.get("intervals", ())
        if int(interval.get("missing_trades", 0)) > 0 and interval.get("chetime_prev") and interval.get("chetime_next")
    ]


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
    audit: Mapping[str, Any], sources: Sequence[GapfillSource], received: _Received | None = None
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
    items = [(interval_id, symbol, interval) for interval_id, (symbol, interval) in enumerate(_audit_intervals(audit))]
    parts: list[pl.DataFrame] = []
    outcomes: dict[int, GapfillInterval] = {}
    for cluster in _clusters(items):
        members = [items[interval_id][2] for interval_id in cluster.members]
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
            missing = int(interval["missing_trades"])
            last = position == len(members) - 1
            recovered = min(missing, remaining)
            remaining -= recovered
            if last and surplus:
                status = GapfillStatus.OVER
            elif recovered == 0:
                status = GapfillStatus.NONE
            elif recovered < missing:
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
                missing_trades=missing,
                recovered=recovered,
                residual=max(0, missing - recovered),
                status=status,
                surplus=surplus if last else 0,
            )
        if pool and chosen is not None:
            cursor = 0
            for interval_id in cluster.members:
                take = outcomes[interval_id].recovered
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
