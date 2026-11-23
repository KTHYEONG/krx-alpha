"""Secondary-feed patch backup: upload only primary-loss windows of duplicate feeds.

A secondary partition is a duplicate feed whose accepted symbols are all covered by the
primary (LS) feed. Its full L1 stays on the VPS; the uploaded object is a patch that keeps
only rows inside the primary's loss and gap windows. Any doubt yields the full-upload path.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.config import SecondaryPatchSettings
from src.core.paths import DataPaths
from src.storage.parquet_options import l1_encoding_metadata, l1_parquet_writer_kwargs

logger = logging.getLogger(__name__)

_PRIMARY_VENDOR: str = "ls"
_VENUE: str = "krx"
_SESSION: str = "regular"
_GLOBAL_KEY: str = "*"
_NS_PER_S: int = 1_000_000_000
_SYMBOL: str = "symbol"
_RECV: str = "recv_wall_ns"
_ROW: str = "_row"
_PATCH_BATCH_ROWS: int = 65_536
_WINDOW_SCHEMA: dict[str, pl.DataType] = {"w_symbol": pl.String(), "w_lo": pl.Int64(), "w_hi": pl.Int64()}
_GLOBAL_SCHEMA: dict[str, pl.DataType] = {"g_lo": pl.Int64(), "g_hi": pl.Int64()}

Windows = dict[str, list[tuple[int, int]]]


@dataclass(frozen=True, slots=True)
class SecondaryInputs:
    """Primary-side evidence that makes one secondary partition eligible for patch backup."""

    audit: dict[str, Any]
    gap_windows: Windows


def _merge(spans: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for lo, hi in sorted(spans):
        if merged and lo <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return [(lo, hi) for lo, hi in merged]


@dataclass(frozen=True, slots=True)
class _ManifestView:
    accepted: frozenset[str]
    gaps: tuple[dict[str, Any], ...]


def _read_manifest(path: pathlib.Path) -> _ManifestView:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"manifest is not an object: {path}")
    acks = raw.get("subscription_acks", [])
    gaps = raw.get("gaps", [])
    if not isinstance(acks, list) or not isinstance(gaps, list):
        raise ValueError(f"manifest acks/gaps malformed: {path}")
    accepted = frozenset(str(a["symbol"]) for a in acks if a.get("accepted") is True)
    return _ManifestView(accepted=accepted, gaps=tuple(gaps))


def _manifest_paths(paths: DataPaths, day: dt.date, vendor: str, venue: str) -> tuple[pathlib.Path, ...]:
    if vendor == "kis":
        return paths.regular_kis_manifest_paths(day, venue)
    if vendor == "db":
        candidates = (paths.db_crosscheck_manifest_path(day, 0), paths.db_crosscheck_manifest_path(day, 1))
        return tuple(p for p in candidates if p.is_file())
    raise ValueError(f"no secondary manifest layout for vendor {vendor!r}")


def secondary_inputs(
    paths: DataPaths,
    day: dt.date,
    vendor: str,
    venue: str,
    session: str,
    settings: SecondaryPatchSettings,
) -> SecondaryInputs | None:
    """Return the primary evidence for a secondary partition, or None for full upload.

    Non-configured vendors and non-regular-krx partitions return None silently. Any
    missing, unparsable, or non-subset input returns None with a WARNING or INFO line.
    """
    if vendor not in settings.vendors or venue != _VENUE or session != _SESSION:
        return None
    margin_ns = round(settings.margin_s * _NS_PER_S)
    try:
        audit_path = paths.session_audit_path(day, _PRIMARY_VENDOR)
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        if not isinstance(audit, dict) or not isinstance(audit.get("symbols"), list):
            raise ValueError("audit missing symbols list")
        ls_manifests = [_read_manifest(p) for p in paths.ls_manifest_paths(day)]
        ls_accepted: set[str] = set().union(*(m.accepted for m in ls_manifests))
        secondary_manifests = _manifest_paths(paths, day, vendor, venue)
        if not secondary_manifests:
            raise FileNotFoundError(f"no {vendor} manifest for {day.isoformat()}")
        vendor_accepted: set[str] = set().union(*(_read_manifest(p).accepted for p in secondary_manifests))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        logger.warning(
            "[DATA] stage=secondary_patch status=FULL_UPLOAD reason=input_unreadable vendor=%s day=%s error=%s",
            vendor,
            day.isoformat(),
            str(exc),
        )
        return None
    if not vendor_accepted or not vendor_accepted <= ls_accepted:
        logger.info(
            "[DATA] stage=secondary_patch status=FULL_UPLOAD reason=symbols_not_subset vendor=%s day=%s",
            vendor,
            day.isoformat(),
        )
        return None
    gap_windows: Windows = {}
    for manifest in ls_manifests:
        for gap in manifest.gaps:
            key = str(gap.get("symbol") or _GLOBAL_KEY)
            gap_windows.setdefault(key, []).append(
                (int(gap["gap_start_ns"]) - margin_ns, int(gap["gap_end_ns"]) + margin_ns)
            )
    return SecondaryInputs(audit=audit, gap_windows={k: _merge(v) for k, v in gap_windows.items()})


def is_secondary_partition(
    paths: DataPaths,
    day: dt.date,
    vendor: str,
    venue: str,
    session: str,
    settings: SecondaryPatchSettings,
) -> bool:
    """True when the partition may be backed up as a patch; any doubt returns False (full upload)."""
    return secondary_inputs(paths, day, vendor, venue, session, settings) is not None


def patch_windows(
    audit: Mapping[str, Any],
    margin_s: float,
    extra: Mapping[str, Sequence[tuple[int, int]]] | None = None,
) -> Windows:
    """Per-symbol merged receive-time windows from primary loss intervals plus extra spans."""
    margin_ns = round(margin_s * _NS_PER_S)
    raw: dict[str, list[tuple[int, int]]] = {}
    for entry in audit.get("symbols", []):
        symbol = str(entry["symbol"])
        for interval in entry.get("intervals", []):
            prev = interval.get("recv_prev_ns")
            nxt = interval.get("recv_next_ns")
            if not isinstance(prev, int) or not isinstance(nxt, int):
                continue
            raw.setdefault(symbol, []).append((min(prev, nxt) - margin_ns, max(prev, nxt) + margin_ns))
    for key, spans in (extra or {}).items():
        raw.setdefault(key, []).extend((int(lo), int(hi)) for lo, hi in spans)
    return {key: _merge(spans) for key, spans in sorted(raw.items())}


def _window_frames(windows: Mapping[str, Sequence[tuple[int, int]]]) -> tuple[pl.DataFrame, pl.DataFrame]:
    rows = [(symbol, lo, hi) for symbol, spans in windows.items() if symbol != _GLOBAL_KEY for lo, hi in spans]
    globals_ = [(lo, hi) for lo, hi in windows.get(_GLOBAL_KEY, ())]
    per_symbol = pl.DataFrame(rows, schema=_WINDOW_SCHEMA, orient="row")
    global_frame = pl.DataFrame(globals_, schema=_GLOBAL_SCHEMA, orient="row")
    return per_symbol, global_frame


def _kept_rows(table: pa.Table, per_symbol: pl.DataFrame, global_frame: pl.DataFrame) -> pa.Array:
    frame = pl.DataFrame(table.select([_SYMBOL, _RECV])).with_row_index(_ROW)
    hits: list[pl.DataFrame] = []
    if per_symbol.height:
        hits.append(
            frame.join_where(
                per_symbol,
                pl.col(_SYMBOL) == pl.col("w_symbol"),
                pl.col(_RECV) >= pl.col("w_lo"),
                pl.col(_RECV) <= pl.col("w_hi"),
            ).select(_ROW)
        )
    if global_frame.height:
        hits.append(
            frame.join_where(
                global_frame,
                pl.col(_RECV) >= pl.col("g_lo"),
                pl.col(_RECV) <= pl.col("g_hi"),
            ).select(_ROW)
        )
    if not hits:
        return pa.array([], type=pa.int64())
    kept = pl.concat(hits).unique()[_ROW].sort().cast(pl.Int64)
    return pa.array(kept.to_numpy(), type=pa.int64())


def write_secondary_patch(
    full_l1: pathlib.Path,
    out: pathlib.Path,
    windows: Mapping[str, Sequence[tuple[int, int]]],
) -> int:
    """Write the rows of `full_l1` inside `windows` to `out` atomically; always writes, even with zero rows.

    Returns:
        Number of rows written.
    """
    per_symbol, global_frame = _window_frames(windows)
    source = pq.ParquetFile(full_l1)
    schema = source.schema_arrow.remove_metadata()
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.parent / (out.name + ".tmp")
    kept_total = 0
    wrote_any = False
    writer = pq.ParquetWriter(tmp, schema, **l1_parquet_writer_kwargs(schema))
    try:
        writer.add_key_value_metadata(l1_encoding_metadata())
        for batch in source.iter_batches(batch_size=_PATCH_BATCH_ROWS):
            table = pa.Table.from_batches([batch])
            kept = _kept_rows(table, per_symbol, global_frame)
            writer.write_table(table.take(kept))
            kept_total += len(kept)
            wrote_any = True
        if not wrote_any:
            writer.write_table(schema.empty_table())
    except BaseException:
        writer.close()
        tmp.unlink(missing_ok=True)
        raise
    writer.close()
    os.replace(tmp, out)
    return kept_total


def publish_secondary_l1(
    paths: DataPaths,
    inputs: SecondaryInputs,
    *,
    day: dt.date,
    vendor: str,
    venue: str,
    session: str,
    stream: str,
    full_l1: pathlib.Path,
    settings: SecondaryPatchSettings,
) -> int:
    """Write the patch object, then move the full L1 to the local-only tree; returns patch rows.

    Raises:
        OSError, ValueError, pyarrow and polars errors: the full L1 is left in place when the
        patch write fails, so the caller must not treat the partition as patch-backed.
    """
    windows = patch_windows(inputs.audit, settings.margin_s, inputs.gap_windows)
    patch = paths.secondary_patch_l1(vendor, venue, session, stream, day)
    full_rows = pq.read_metadata(full_l1).num_rows
    rows = write_secondary_patch(full_l1, patch, windows)
    local = paths.l1_local_root / full_l1.relative_to(paths.archive_root)
    local.parent.mkdir(parents=True, exist_ok=True)
    os.replace(full_l1, local)
    ratio = rows / full_rows if full_rows else 0.0
    logger.info(
        "[DATA] stage=secondary_patch status=OK vendor=%s stream=%s day=%s windows=%d rows=%d full_rows=%d ratio=%.4f",
        vendor,
        stream,
        day.isoformat(),
        sum(len(spans) for spans in windows.values()),
        rows,
        full_rows,
        ratio,
    )
    return rows
