"""수집기 디스크 워터마크 및 저널 보존 가드."""

from __future__ import annotations

import datetime as dt
import io
import logging
import os
import pathlib
import re
import shutil
from zoneinfo import ZoneInfo

import polars as pl
import zstandard as zstd

from src.core.errors import KrxAlphaError
from src.storage.quality import QuoteQualitySummary, TickQualitySummary, decode_and_flag_quotes, decode_and_flag_ticks

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_DT_RE = re.compile(r"dt=(\d{4})-(\d{2})-(\d{2})")


class StorageExhaustedError(KrxAlphaError):
    """여유 디스크가 워터마크 미만일 때 발생하는 Fail-Closed 신호."""


class L1NormalizationError(KrxAlphaError):
    """L1 정규화 실패 fail-closed 신호."""


def check_disk_watermark(path: pathlib.Path, *, min_free_gb: float = 3.0) -> bool:
    usage = shutil.disk_usage(path)
    return usage.free >= min_free_gb * (1024**3)


def normalize_l0_partition(part_dir: pathlib.Path, out_path: pathlib.Path) -> int:
    part = pathlib.Path(part_dir)
    zst_files = sorted(part.glob("*.jsonl.zst"))
    if not zst_files:
        raise L1NormalizationError(f"no .zst files in {part}")
    out = pathlib.Path(out_path)
    tmp_path = out.parent / (out.name + ".tmp")
    try:
        dctx = zstd.ZstdDecompressor()
        chunks: list[bytes] = []
        for zf in zst_files:
            with open(zf, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as reader:
                chunks.append(reader.read())
        df = pl.read_ndjson(io.BytesIO(b"".join(chunks)))
        df = df.with_columns([
            pl.col("recv_mono_ns").cast(pl.Int64),
            pl.col("recv_wall_ns").cast(pl.Int64),
            pl.col("conn_seq").cast(pl.Int64),
            pl.col("raw").cast(pl.String),
            pl.col("conn_id").cast(pl.String),
            pl.col("vendor").cast(pl.String),
            pl.col("tr_id").cast(pl.String),
        ])
        df = df.unique(subset=["conn_id", "conn_seq"], keep="first", maintain_order=True)
        df = df.sort("recv_wall_ns")
        if df.height == 0:
            raise ValueError(f"zero rows after dedup: {part}")
        quality: TickQualitySummary | None = decode_and_flag_ticks(df)
        if quality is not None:
            status = "WARN" if any((quality.decode_fail, quality.zero_volume, quality.price_band_violation, quality.cum_volume_regression, quality.schema_disagree, quality.tick_loss, quality.tick_duplicate, quality.lost_volume)) else "OK"
            (logger.warning if status == "WARN" else logger.info)(
                "[DATA] stage=quality tr_id=%s rows=%d decode_fail=%d zero_volume=%d price_band_violation=%d cum_volume_regression=%d schema_disagree=%d tick_loss=%d tick_duplicate=%d lost_volume=%d status=%s",
                "H0STCNT0",
                quality.rows,
                quality.decode_fail,
                quality.zero_volume,
                quality.price_band_violation,
                quality.cum_volume_regression,
                quality.schema_disagree,
                quality.tick_loss,
                quality.tick_duplicate,
                quality.lost_volume,
                status,
            )
        quote_quality: QuoteQualitySummary | None = decode_and_flag_quotes(df)
        if quote_quality is not None:
            quote_status = "WARN" if any((quote_quality.decode_fail, quote_quality.ladder_disorder, quote_quality.crossed_book, quote_quality.negative_remain, quote_quality.total_remain_short)) else "OK"
            (logger.warning if quote_status == "WARN" else logger.info)(
                "[DATA] stage=quality tr_id=%s rows=%d decode_fail=%d ladder_disorder=%d crossed_book=%d negative_remain=%d total_remain_short=%d status=%s",
                "H0STASP0",
                quote_quality.rows,
                quote_quality.decode_fail,
                quote_quality.ladder_disorder,
                quote_quality.crossed_book,
                quote_quality.negative_remain,
                quote_quality.total_remain_short,
                quote_status,
            )
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(tmp_path, compression="zstd")
        os.replace(tmp_path, out)
    except (zstd.ZstdError, OSError, ValueError, pl.exceptions.ComputeError) as exc:
        tmp_path.unlink(missing_ok=True)
        raise L1NormalizationError(f"normalize failed: {part} ({exc})") from exc
    return int(df.height)


def prune_old_journals(
    root: pathlib.Path, archive_root: pathlib.Path | None = None, *, retain_days: int = 3, reference_date: dt.date | None = None
) -> int:
    if archive_root is None:
        return 0
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    deleted = 0
    archive_base = pathlib.Path(archive_root)
    for part in [p for p in pathlib.Path(root).rglob("dt=*") if p.is_dir()]:
        m = _DT_RE.fullmatch(part.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        vendor = part.parent.parent.name
        stream = part.parent.name
        out_path = archive_base / vendor / stream / f"{part.name}.parquet"
        try:
            rows = normalize_l0_partition(part, out_path)
        except L1NormalizationError as exc:
            logger.critical("[DATA] stage=prune status=FAIL reason=%s part=%s", str(exc), str(part))
            continue
        if rows <= 0 or not out_path.exists():
            logger.critical("[DATA] stage=prune status=FAIL reason=unverified part=%s", str(part))
            continue
        deleted += sum(1 for f in part.rglob("*") if f.is_file())
        shutil.rmtree(part)
    return deleted


def prune_local_l1(
    archive_root: pathlib.Path,
    *,
    retain_days: int = 30,
    reference_date: dt.date | None = None,
    confirmed_remote: set[str] | None = None,
) -> int:
    if confirmed_remote is None:
        return 0
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    root = pathlib.Path(archive_root)
    purged = 0
    for pq in sorted(root.rglob("*.parquet")):
        m = _DT_RE.search(pq.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        rel = "l1/" + pq.relative_to(root).as_posix()
        if rel not in confirmed_remote:
            continue
        pq.unlink()
        purged += 1
    return purged
