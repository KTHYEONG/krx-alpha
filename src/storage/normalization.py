"""L0 partition normalization into deduplicated, quality-annotated L1 parquet."""

from __future__ import annotations

import io
import json
import logging
import os
import pathlib
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import zstandard as zstd

from src.core.config import DataQualitySettings
from src.core.errors import KrxAlphaError
from src.storage.layout import L0_JOURNAL_GLOB, route_for_l0_partition
from src.storage.market_phase import MARKET_PHASE_METADATA_KEY, MARKET_PHASE_WINDOWS, PhaseWindow, annotate_market_phase
from src.storage.quality import (
    DqStatus,
    QuoteQualitySummary,
    TickQualitySummary,
    decode_and_flag_quotes,
    decode_tick_raw_fields,
    evaluate_stream_quality,
    sum_quote_summaries,
    summarize_tick_fields_bucketed,
)

logger = logging.getLogger(__name__)

_L0_SCHEMA: dict[str, Any] = {
    "raw": pl.String, "recv_mono_ns": pl.Int64, "recv_wall_ns": pl.Int64,
    "conn_id": pl.String, "conn_seq": pl.Int64, "vendor": pl.String,
    "tr_id": pl.String, "venue": pl.String, "session": pl.String,
    "stream": pl.String, "symbol": pl.String, "exchange_event_time": pl.String,
}
_L0_REQUIRED_COLUMNS: tuple[str, ...] = (
    "raw", "recv_mono_ns", "recv_wall_ns", "conn_id", "conn_seq", "vendor", "tr_id",
)
_BATCH_BYTES: int = 32 * 2**20
_GATHER_ROWS: int = 20_000
_TICK_BUCKETS: int = 16
_RAW_HASH_SEEDS: tuple[int, int] = (0x9E3779B1, 0x85EBCA77)
_WORK_DIR_PREFIX: str = 'krx-l1-normalize-'
_CONN_CODE_MAX: int = 2**31 - 1
_KEY_BUF_INIT: int = 131_072


class L1NormalizationError(KrxAlphaError):
    """Signal a corrupt or unprocessable partition without deleting its L0 input."""


class L1StorageIOError(KrxAlphaError):
    """Local storage failed (disk full, I/O error) while normalizing a partition.

    The source partition is intact; it must stay in L0 and be retried, never
    quarantined as corrupt.
    """


def _iter_line_batches(path: pathlib.Path, batch_bytes: int) -> Iterator[bytes]:
    dctx = zstd.ZstdDecompressor()
    carry = b""
    with open(path, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as reader:
        while True:
            buf = reader.read(batch_bytes)
            if not buf:
                break
            buf = carry + buf
            cut = buf.rfind(b"\n")
            if cut < 0:
                carry = buf
                continue
            yield buf[: cut + 1]
            carry = buf[cut + 1 :]
    if carry.strip():
        yield carry


@dataclass(frozen=True, slots=True)
class _KeyColumns:
    conn_code: np.ndarray
    conn_seq: np.ndarray
    recv_wall_ns: np.ndarray
    h1: np.ndarray
    h2: np.ndarray


def _dedup_sort_order(keys: _KeyColumns) -> tuple[np.ndarray, int, np.ndarray, np.ndarray]:
    """Return wall-ordered row ids, conflict count and aligned dropped/kept ids.

    Pair enumeration order depends on connection codes, not connection strings.
    """
    n = int(keys.conn_seq.size)
    if n == 0:
        z = np.empty(0, dtype=np.int64)
        return (z, 0, z.copy(), z.copy())
    perm = np.lexsort((keys.h2, keys.h1, keys.conn_seq, keys.conn_code))
    new_key = np.empty(n, dtype=bool)
    new_key[0] = True
    tmp = keys.conn_code[perm]
    new_key[1:] = tmp[1:] != tmp[:-1]
    del tmp
    tmp = keys.conn_seq[perm]
    new_key[1:] |= tmp[1:] != tmp[:-1]
    del tmp
    new_raw = new_key.copy()
    tmp = keys.h1[perm]
    new_raw[1:] |= tmp[1:] != tmp[:-1]
    del tmp
    tmp = keys.h2[perm]
    new_raw[1:] |= tmp[1:] != tmp[:-1]
    del tmp
    key_starts = np.flatnonzero(new_key)
    del new_key
    conflict = int((np.add.reduceat(new_raw, key_starts, dtype=np.int64) > 1).sum())
    del new_raw
    keep = np.minimum.reduceat(perm, key_starts)
    dropped = np.empty(n - keep.size, dtype=np.int64)
    kept = np.empty_like(dropped)
    if dropped.size:
        offset = 0
        for lo in range(0, n, _GATHER_ROWS):
            hi = min(lo + _GATHER_ROWS, n)
            kept_for = keep[np.searchsorted(key_starts, np.arange(lo, hi), side="right") - 1]
            mask = perm[lo:hi] != kept_for
            count = int(mask.sum())
            dropped[offset : offset + count] = perm[lo:hi][mask]
            kept[offset : offset + count] = kept_for[mask]
            offset += count
            del kept_for, mask
    del key_starts, perm
    order = keep[np.lexsort((keep, keys.recv_wall_ns[keep]))].astype(np.int64, copy=False)
    del keep
    return (order, conflict, dropped, kept)


class _SpillReader:
    def __init__(self, paths: list[pathlib.Path], starts: list[int]) -> None:
        self._paths = list(paths)
        self._starts = np.asarray(list(starts), dtype=np.int64)
        self._cache: tuple[int, pl.DataFrame] | None = None

    def take(self, ids: np.ndarray) -> pl.DataFrame:
        ids64 = np.asarray(ids, dtype=np.int64)
        file_of = np.searchsorted(self._starts, ids64, side="right") - 1
        parts: list[pl.DataFrame] = []
        positions: list[np.ndarray] = []
        for fi in np.unique(file_of):
            idx = int(fi)
            mask = file_of == fi
            if self._cache is None or self._cache[0] != idx:
                self._cache = (idx, pl.read_parquet(self._paths[idx]))
            frame = self._cache[1]
            parts.append(frame[(ids64[mask] - self._starts[idx]).tolist()])
            positions.append(np.flatnonzero(mask))
        result = pl.concat(parts)
        taken: pl.DataFrame = result[np.argsort(np.concatenate(positions), kind="stable").tolist()].select(list(_L0_SCHEMA))
        return taken


def normalize_l0_partition(
    part_dir: pathlib.Path,
    out_path: pathlib.Path,
    *,
    work_root: pathlib.Path | None = None,
    dq_settings: DataQualitySettings | None = None,
    phase_windows: tuple[PhaseWindow, ...] = MARKET_PHASE_WINDOWS,
    listing_symbols: frozenset[str] = frozenset(),
) -> int:
    """Write one deduplicated, quality-annotated L1 partition from L0 records.

    Args:
        part_dir: Immutable source partition.
        out_path: Final L1 Parquet path.
        work_root: Optional bounded spill workspace.
        dq_settings: Typed quality thresholds.
        phase_windows: Market-phase windows for the partition date.
        listing_symbols: Symbols judged under the listing-day price band for this partition date.

    Returns:
        Number of persisted L1 records.

    Raises:
        L1NormalizationError: Source decode, schema, write, or finalization failed.
    """
    part = pathlib.Path(part_dir)
    route = route_for_l0_partition(part)
    stream_name = route.stream
    legacy_venue = route.venue
    legacy_session = route.session.value
    zst_files = sorted(part.glob(L0_JOURNAL_GLOB))
    if not zst_files:
        raise L1NormalizationError(f"no .zst files in {part}")
    if work_root is None:
        work_dir = pathlib.Path(tempfile.mkdtemp(prefix=_WORK_DIR_PREFIX))
    else:
        work_dir = pathlib.Path(work_root) / f"{part.parent.parent.name}.{part.parent.name}.{part.name}"
        shutil.rmtree(work_dir, ignore_errors=True)
        work_dir.mkdir(parents=True)
    spill_dir = work_dir / "in"
    tick_dir = work_dir / "tick"
    spill_dir.mkdir(parents=True, exist_ok=True)
    tick_dir.mkdir(parents=True, exist_ok=True)
    out = pathlib.Path(out_path)
    tmp_path = out.parent / (out.name + ".tmp")
    writer: pq.ParquetWriter | None = None
    succeeded = False
    try:
        spill_paths: list[pathlib.Path] = []
        starts: list[int] = []
        conn_index: dict[str, int] = {}
        cap = 0
        code_buf = np.empty(0, dtype=np.int32)
        seq_buf = np.empty(0, dtype=np.int64)
        wall_buf = np.empty(0, dtype=np.int64)
        h1_buf = np.empty(0, dtype=np.uint64)
        h2_buf = np.empty(0, dtype=np.uint64)
        raw_records = 0
        for zf in zst_files:
            for blob in _iter_line_batches(zf, _BATCH_BYTES):
                df = pl.read_ndjson(io.BytesIO(blob), schema=_L0_SCHEMA)
                if df.height == 0:
                    continue
                if any(df[col].null_count() > 0 for col in _L0_REQUIRED_COLUMNS):
                    raise L1NormalizationError(f"null field in {zf}")
                df = df.with_columns(
                    pl.col("venue").fill_null(legacy_venue),
                    pl.col("session").fill_null(legacy_session),
                    pl.col("stream").fill_null(pl.col("tr_id")).fill_null(stream_name),
                    pl.col("symbol").fill_null(""),
                    pl.col("exchange_event_time").fill_null(""),
                ).select(list(_L0_SCHEMA))
                spill = spill_dir / f"{len(spill_paths):06d}.parquet"
                df.write_parquet(spill, compression="lz4")
                starts.append(raw_records)
                spill_paths.append(spill)
                n = df.height
                if raw_records + n > cap:
                    cap = max(_KEY_BUF_INIT, cap * 2 if cap else _KEY_BUF_INIT, raw_records + n)
                    code_buf.resize(cap, refcheck=False)
                    seq_buf.resize(cap, refcheck=False)
                    wall_buf.resize(cap, refcheck=False)
                    h1_buf.resize(cap, refcheck=False)
                    h2_buf.resize(cap, refcheck=False)
                for i, cid in enumerate(df["conn_id"].to_list()):
                    code = conn_index.get(cid)
                    if code is None:
                        code = len(conn_index)
                        if code >= _CONN_CODE_MAX:
                            raise L1NormalizationError(f"too many conn_id in {part}")
                        conn_index[cid] = code
                    code_buf[raw_records + i] = code
                seq_buf[raw_records : raw_records + n] = df["conn_seq"].to_numpy()
                wall_buf[raw_records : raw_records + n] = df["recv_wall_ns"].to_numpy()
                h1_buf[raw_records : raw_records + n] = df["raw"].hash(_RAW_HASH_SEEDS[0]).to_numpy()
                h2_buf[raw_records : raw_records + n] = df["raw"].hash(_RAW_HASH_SEEDS[1]).to_numpy()
                del df
                raw_records += n
        if raw_records == 0:
            raise L1NormalizationError(f"zero rows: {part}")
        code_buf.resize(raw_records, refcheck=False)
        seq_buf.resize(raw_records, refcheck=False)
        wall_buf.resize(raw_records, refcheck=False)
        h1_buf.resize(raw_records, refcheck=False)
        h2_buf.resize(raw_records, refcheck=False)
        keys = _KeyColumns(
            conn_code=code_buf,
            conn_seq=seq_buf,
            recv_wall_ns=wall_buf,
            h1=h1_buf,
            h2=h2_buf,
        )
        del code_buf, seq_buf, wall_buf, h1_buf, h2_buf, conn_index, blob
        order, conflict, dropped_idx, kept_idx = _dedup_sort_order(keys)
        del keys
        if conflict > 0:
            raise L1NormalizationError(f"conn_seq collision in {part}: {conflict} groups")
        reader = _SpillReader(spill_paths, starts)
        for lo in range(0, dropped_idx.size, _GATHER_ROWS):
            if not reader.take(dropped_idx[lo : lo + _GATHER_ROWS]).get_column("raw").equals(
                reader.take(kept_idx[lo : lo + _GATHER_ROWS]).get_column("raw")
            ):
                raise L1NormalizationError(f"conn_seq collision in {part}: hash-equal payload mismatch")
        out.parent.mkdir(parents=True, exist_ok=True)
        quote_parts: list[QuoteQualitySummary] = []
        tick_rows = 0
        vendors: set[str] = set()
        phase_counts: dict[str, int] = {}
        settings = dq_settings if dq_settings is not None else DataQualitySettings()
        for ci, lo in enumerate(range(0, order.size, _GATHER_ROWS)):
            chunk = reader.take(order[lo : lo + _GATHER_ROWS])
            chunk = annotate_market_phase(chunk, windows=phase_windows)
            for phase, count in chunk["market_phase"].value_counts().iter_rows():
                phase_counts[str(phase)] = phase_counts.get(str(phase), 0) + int(count)
            vendors.update(v for v in chunk["vendor"].unique().to_list() if v is not None)
            qs = decode_and_flag_quotes(chunk)
            if qs is not None:
                quote_parts.append(qs)
            fields = decode_tick_raw_fields(chunk)
            if fields is not None:
                tick_rows += fields.height
                fields.write_parquet(tick_dir / f"{ci:06d}.parquet")
            table = chunk.to_arrow(compat_level=pl.CompatLevel.oldest())
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, table.schema, compression="zstd")
            writer.write_table(table)
        assert writer is not None
        del reader, chunk, table, fields
        tick_summary: TickQualitySummary | None = (
            summarize_tick_fields_bucketed(
                pl.scan_parquet(tick_dir / "*.parquet"),
                rows=tick_rows,
                buckets=_TICK_BUCKETS,
                settings=settings,
                listing_symbols=listing_symbols,
            )
            if tick_rows > 0
            else None
        )
        quote_summary: QuoteQualitySummary | None = sum_quote_summaries(quote_parts) if quote_parts else None
        vendor_label = ",".join(sorted(vendors)) if vendors else ""
        phases_json = json.dumps(phase_counts, sort_keys=True, separators=(",", ":"))
        if tick_summary is not None or quote_summary is not None:
            verdict = evaluate_stream_quality(tick_summary, quote_summary, settings=settings)
            writer.add_key_value_metadata({**verdict.to_metadata(), MARKET_PHASE_METADATA_KEY: phases_json})
            writer.close()
            writer = None
            if verdict.status is DqStatus.FAIL:
                dq_log = logger.critical
            elif verdict.status is DqStatus.WARN:
                dq_log = logger.warning
            else:
                dq_log = logger.info
            reasons = ",".join(verdict.reasons)
            if tick_summary is not None:
                quality = tick_summary
                dq_log(
                    "[DATA] stage=quality tr_id=%s vendor=%s rows=%d decode_fail=%d zero_volume=%d price_band_violation=%d cum_volume_regression=%d schema_disagree=%d tick_loss=%d lost_volume=%d part=%s listing_symbols=%d status=%s reasons=%s",
                    stream_name,
                    vendor_label,
                    quality.rows,
                    quality.decode_fail,
                    quality.zero_volume,
                    quality.price_band_violation,
                    quality.cum_volume_regression,
                    quality.schema_disagree,
                    quality.tick_loss,
                    quality.lost_volume,
                    str(part),
                    len(listing_symbols),
                    verdict.status.value,
                    reasons,
                )
            if quote_summary is not None:
                quote_quality = quote_summary
                dq_log(
                    "[DATA] stage=quality tr_id=%s vendor=%s rows=%d decode_fail=%d ladder_disorder=%d crossed_book=%d negative_remain=%d total_remain_short=%d status=%s reasons=%s",
                    stream_name,
                    vendor_label,
                    quote_quality.rows,
                    quote_quality.decode_fail,
                    quote_quality.ladder_disorder,
                    quote_quality.crossed_book,
                    quote_quality.negative_remain,
                    quote_quality.total_remain_short,
                    verdict.status.value,
                    reasons,
                )
        else:
            writer.add_key_value_metadata({MARKET_PHASE_METADATA_KEY: phases_json})
            writer.close()
            writer = None
        os.replace(tmp_path, out)
        logger.info(
            "[DATA] stage=normalize part=%s raw_records=%d l1_rows=%d dedup_dropped=%d conn_seq_conflict=%d phases=%s status=OK",
            str(part),
            raw_records,
            int(order.size),
            raw_records - int(order.size),
            conflict,
            phases_json,
        )
        succeeded = True
        return int(order.size)
    except zstd.ZstdError as exc:
        raise L1NormalizationError(f"normalize failed: {part} ({exc})") from exc
    except OSError as exc:
        raise L1StorageIOError(f"normalize failed: {part} ({exc})") from exc
    except (ValueError, pl.exceptions.ComputeError) as exc:
        raise L1NormalizationError(f"normalize failed: {part} ({exc})") from exc
    finally:
        if writer is not None:
            writer.close()
        if not succeeded:
            tmp_path.unlink(missing_ok=True)
        shutil.rmtree(work_dir, ignore_errors=True)
