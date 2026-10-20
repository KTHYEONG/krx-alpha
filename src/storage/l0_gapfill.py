"""Derive gap-fill trade rows for L0 counter gaps from an external tick tape; never touches L0 journals."""

from __future__ import annotations

import json
import os
import pathlib
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


def derive_gapfill(
    audit: Mapping[str, Any], sources: Sequence[GapfillSource]
) -> tuple[pl.DataFrame, tuple[GapfillInterval, ...]]:
    """Return tape trades strictly between the two audited frames of every positive-missing interval, plus per-interval outcomes.

    Each interval takes the trades of the first source that has any inside it. Raises `ValueError` when a tape lacks
    `symbol`, `ts_hms`, `price` or `volume`. Boundary-second trades are excluded because the live frames may already
    hold part of them, so `residual` is a lower bound of the still-missing trades.
    """
    indexed_sources: list[tuple[GapfillSource, dict[str, pl.DataFrame]]] = []
    for source in sources:
        missing_columns = [name for name in _TAPE_COLUMNS if name not in source.tape.columns]
        if missing_columns:
            raise ValueError(f"tape lacks columns {missing_columns}")
        indexed = (
            source.tape.select(_TAPE_COLUMNS).with_row_index("tape_seq").with_columns(pl.col("tape_seq").cast(pl.Int64))
        )
        indexed_sources.append(
            (source, {key[0]: frame for key, frame in indexed.group_by("symbol", maintain_order=True)})
        )
    parts: list[pl.DataFrame] = []
    outcomes: list[GapfillInterval] = []
    for interval_id, (symbol, interval) in enumerate(_audit_intervals(audit)):
        lo = int(interval["chetime_prev"])
        hi = int(interval["chetime_next"])
        missing = int(interval["missing_trades"])
        chosen: GapfillSource | None = None
        picked = pl.DataFrame(
            schema={
                "symbol": pl.String,
                "ts_hms": pl.Int64,
                "price": pl.Int64,
                "volume": pl.Int64,
                "tape_seq": pl.Int64,
            }
        )
        for candidate, by_symbol in indexed_sources:
            frame = by_symbol.get(symbol)
            if frame is None:
                continue
            hits = frame.filter((pl.col("ts_hms") > lo) & (pl.col("ts_hms") < hi))
            if hits.height > 0:
                picked, chosen = hits, candidate
                break
        recovered = picked.height
        if recovered > 0 and chosen is not None:
            parts.append(
                picked.select(
                    pl.col("symbol"),
                    pl.col("ts_hms").cast(pl.String).str.zfill(6).alias("chetime"),
                    pl.col("price").cast(pl.Int64),
                    pl.col("volume").cast(pl.Int64),
                    pl.col("tape_seq"),
                    pl.lit(interval_id, dtype=pl.Int64).alias("interval_id"),
                    pl.lit(int(interval["cum_prev"]), dtype=pl.Int64).alias("cum_prev"),
                    pl.lit(int(interval["cum_next"]), dtype=pl.Int64).alias("cum_next"),
                    pl.lit(chosen.vendor).alias("vendor"),
                    pl.lit(chosen.provenance).alias("provenance"),
                )
            )
        if recovered == 0:
            status = GapfillStatus.NONE
        elif recovered < missing:
            status = GapfillStatus.PARTIAL
        elif recovered == missing:
            status = GapfillStatus.FULL
        else:
            status = GapfillStatus.OVER
        outcomes.append(
            GapfillInterval(
                interval_id=interval_id,
                symbol=symbol,
                chetime_prev=str(interval["chetime_prev"]),
                chetime_next=str(interval["chetime_next"]),
                cum_prev=int(interval["cum_prev"]),
                cum_next=int(interval["cum_next"]),
                missing_trades=missing,
                recovered=recovered,
                residual=max(0, missing - recovered),
                status=status,
            )
        )
    rows = pl.concat(parts) if parts else pl.DataFrame(schema=_ROW_SCHEMA)
    return rows.sort(["symbol", "chetime", "tape_seq"]), tuple(outcomes)


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
