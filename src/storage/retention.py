"""수집기 디스크 워터마크 및 저널 보존 가드."""

from __future__ import annotations

import datetime as dt
import io
import json
import logging
import os
import pathlib
import re
import shutil
import tempfile
from collections.abc import Callable, Iterator
from collections.abc import Set as AbstractSet
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import zstandard as zstd

from src.core.config import DataQualitySettings
from src.core.errors import KrxAlphaError
from src.storage.market_phase import MARKET_PHASE_METADATA_KEY, annotate_market_phase
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

_KST = ZoneInfo("Asia/Seoul")
_DT_RE = re.compile(r"dt=(\d{4})-(\d{2})-(\d{2})")

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


class StorageExhaustedError(KrxAlphaError):
    """여유 디스크가 워터마크 미만일 때 발생하는 Fail-Closed 신호."""


class L1NormalizationError(KrxAlphaError):
    """L1 정규화 실패 fail-closed 신호."""


class L1WorkerCrashError(KrxAlphaError):
    """정규화 워커 프로세스 비정상 종료(OOM SIGKILL 등) 신호 — 데이터 결함 아님, 격리 금지."""


def check_disk_watermark(path: pathlib.Path, *, min_free_gb: float = 3.0) -> bool:
    usage = shutil.disk_usage(path)
    return usage.free >= min_free_gb * (1024**3)


def _iter_line_batches(path: pathlib.Path, batch_bytes: int) -> Iterator[bytes]:
    dctx = zstd.ZstdDecompressor()
    carry = b""
    with open(path, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as reader:
        while True:
            buf = reader.read(batch_bytes)
            if not buf:
                break
            buf = carry + buf
            # 고정 크기 배치는 반드시 개행에서만 절단해야 JSON 행이 깨지지 않는다
            cut = buf.rfind(b"\n")
            if cut < 0:
                carry = buf
                continue
            yield buf[: cut + 1]
            carry = buf[cut + 1 :]
    # json.dumps는 원시 개행을 배출하지 않으므로 EOF 잔여물은 온전한 한 행이다
    if carry.strip():
        yield carry


def _dedup_sort_order(keys: pl.DataFrame) -> tuple[np.ndarray, int, np.ndarray, np.ndarray]:
    conn = keys["conn_id"].rank("dense").to_numpy().astype(np.int64)
    seq = keys["conn_seq"].to_numpy()
    wall = keys["recv_wall_ns"].to_numpy()
    h1 = keys["h1"].to_numpy()
    h2 = keys["h2"].to_numpy()
    # polars 다중키 group_by/unique는 키 대비 +280MB를 쓰므로 키 배열만으로 정렬한다
    perm = np.lexsort((h2, h1, seq, conn))
    c = conn[perm]
    s = seq[perm]
    a = h1[perm]
    b = h2[perm]
    new_key = np.empty(seq.size, dtype=bool)
    new_key[0] = True
    new_key[1:] = (c[1:] != c[:-1]) | (s[1:] != s[:-1])
    new_raw = new_key.copy()
    new_raw[1:] |= (a[1:] != a[:-1]) | (b[1:] != b[:-1])
    key_starts = np.flatnonzero(new_key)
    conflict = int((np.add.reduceat(new_raw.astype(np.int64), key_starts) > 1).sum())
    # 같은 신원의 첫 행이 L0 읽기 순서상 최소 인덱스이므로 keep-first가 보존된다
    keep = np.minimum.reduceat(perm, key_starts)
    kept_for = keep[np.cumsum(new_key) - 1]
    mask = perm != kept_for
    dropped = perm[mask]
    kept = kept_for[mask]
    order = keep[np.lexsort((keep, wall[keep]))]
    return (order.astype(np.int64), conflict, dropped.astype(np.int64), kept.astype(np.int64))


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
            # gather 청크 하나만 메모리에 두기 위해 최근 spill 파일 하나만 캐시한다
            if self._cache is None or self._cache[0] != idx:
                self._cache = (idx, pl.read_parquet(self._paths[idx]))
            frame = self._cache[1]
            parts.append(frame[(ids64[mask] - self._starts[idx]).tolist()])
            positions.append(np.flatnonzero(mask))
        result = pl.concat(parts)
        taken: pl.DataFrame = result[np.argsort(np.concatenate(positions), kind="stable").tolist()].select(list(_L0_SCHEMA))
        return taken


def normalize_l0_partition(part_dir: pathlib.Path, out_path: pathlib.Path, *, work_root: pathlib.Path | None = None, dq_settings: DataQualitySettings | None = None) -> int:
    """Normalize one L0 day-partition into a deduplicated, sorted L1 parquet file.

    The partition's DQ verdict is embedded in the output parquet footer under
    ``krx_alpha.dq``; a FAIL verdict is logged at CRITICAL but does not block the write because L1 retains every raw
    frame. Per-phase row counts from ``market_phase`` are embedded under
    ``krx_alpha.market_phase`` as a JSON object mapping each phase value to
    its row count.

    Args:
        part_dir: L0 partition directory holding hourly .jsonl.zst files.
        out_path: Destination L1 parquet path (written atomically via .tmp).
        work_root: Spill directory root; a fresh temp dir when None.
        dq_settings: DQ ratio ceilings; defaults to DataQualitySettings().

    Returns:
        Number of L1 rows written.

    Raises:
        L1NormalizationError: For every data or IO fault (fail-closed, no output).
    """
    part = pathlib.Path(part_dir)
    stream_name = part.parent.name
    session_name = part.parent.parent.name
    is_routed_partition = session_name in {"regular", "krx_after", "nxt_after"}
    legacy_venue = "krx" if not is_routed_partition else part.parent.parent.parent.name
    legacy_session = "regular" if not is_routed_partition else session_name
    zst_files = sorted(part.glob("*.jsonl.zst"))
    if not zst_files:
        raise L1NormalizationError(f"no .zst files in {part}")
    if work_root is None:
        work_dir = pathlib.Path(tempfile.mkdtemp(prefix=_WORK_DIR_PREFIX))
    else:
        # spill이 archive 트리에 섞이면 L1로 오업로드되므로 work 루트에 격리한다
        work_dir = pathlib.Path(work_root) / f"{part.parent.parent.name}.{part.parent.name}.{part.name}"
        # SIGKILL이 남긴 spill 잔재는 다음 시도 전에 쓸어낸다
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
        key_frames: list[pl.DataFrame] = []
        raw_records = 0
        # 패스 1: 압축 해제 배치 하나씩 spill하고 키 배열만 누적한다
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
                key_frames.append(
                    df.select(
                        "conn_id",
                        "conn_seq",
                        "recv_wall_ns",
                        pl.col("raw").hash(_RAW_HASH_SEEDS[0]).alias("h1"),
                        pl.col("raw").hash(_RAW_HASH_SEEDS[1]).alias("h2"),
                    )
                )
                raw_records += df.height
        if raw_records == 0:
            raise L1NormalizationError(f"zero rows: {part}")
        keys = pl.concat(key_frames, rechunk=True)
        # 배치별 키 프레임과 연결본이 pass 2 내내 살아 있으면 호가 DQ 순간 피크와 겹쳐 cgroup 한도를 넘는다
        del key_frames
        order, conflict, dropped_idx, kept_idx = _dedup_sort_order(keys)
        del keys
        # 레거시 재접속 구간의 신원 붕괴는 조용히 버리면 안 되므로 fail-closed 한다
        if conflict > 0:
            raise L1NormalizationError(f"conn_seq collision in {part}: {conflict} groups")
        # 해시는 후보 지명만 하고 실제 삭제는 원문 바이트 동등성으로 확정한다
        reader = _SpillReader(spill_paths, starts)
        for lo in range(0, dropped_idx.size, _GATHER_ROWS):
            if not reader.take(dropped_idx[lo : lo + _GATHER_ROWS]).get_column("raw").equals(
                reader.take(kept_idx[lo : lo + _GATHER_ROWS]).get_column("raw")
            ):
                raise L1NormalizationError(f"conn_seq collision in {part}: hash-equal payload mismatch")
        # 패스 2: 정렬 순서대로 묶어 gather하고 row-group 단위로 쓴다
        out.parent.mkdir(parents=True, exist_ok=True)
        quote_parts: list[QuoteQualitySummary] = []
        tick_rows = 0
        vendors: set[str] = set()
        phase_counts: dict[str, int] = {}
        settings = dq_settings if dq_settings is not None else DataQualitySettings()
        for ci, lo in enumerate(range(0, order.size, _GATHER_ROWS)):
            chunk = reader.take(order[lo : lo + _GATHER_ROWS])
            chunk = annotate_market_phase(chunk)
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
            # polars 업그레이드와 무관하게 large_string 물리 타입을 고정한다
            table = chunk.to_arrow(compat_level=pl.CompatLevel.oldest())
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, table.schema, compression="zstd")
            writer.write_table(table)
        # raw_records>=1 and conflict==0 이므로 최소 한 청크는 써서 writer가 열려 있다
        assert writer is not None
        # 버킷 요약 순간 피크와 겹치지 않도록 gather 캐시·마지막 청크를 먼저 놓는다
        del reader, chunk, table, fields
        tick_summary: TickQualitySummary | None = (
            summarize_tick_fields_bucketed(pl.scan_parquet(tick_dir / "*.parquet"), rows=tick_rows, buckets=_TICK_BUCKETS)
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
                    "[DATA] stage=quality tr_id=%s vendor=%s rows=%d decode_fail=%d zero_volume=%d price_band_violation=%d cum_volume_regression=%d schema_disagree=%d tick_loss=%d lost_volume=%d status=%s reasons=%s",
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
    except (zstd.ZstdError, OSError, ValueError, pl.exceptions.ComputeError) as exc:
        raise L1NormalizationError(f"normalize failed: {part} ({exc})") from exc
    finally:
        if writer is not None:
            writer.close()
        if not succeeded:
            tmp_path.unlink(missing_ok=True)
        shutil.rmtree(work_dir, ignore_errors=True)


class PruneStats(int):
    """L0 prune 결과: int 값은 deleted 파일 수와 동일하게 비교된다."""

    _deleted: int
    _normalized: int

    def __new__(cls, deleted: int, normalized: int = 0) -> PruneStats:
        obj = int.__new__(cls, deleted)
        obj._deleted = int(deleted)
        obj._normalized = int(normalized)
        return obj

    @property
    def deleted(self) -> int:
        return self._deleted

    @property
    def normalized(self) -> int:
        return self._normalized


def prune_old_journals(
    root: Any = None,
    archive_root: Any = None,
    *args: Any,
    retain_days: int = 3,
    reference_date: dt.date | None = None,
    quarantine_root: pathlib.Path | None = None,
    normalizer: Callable[[pathlib.Path, pathlib.Path], int] | None = None,
    verified_remote_l1: AbstractSet[str] | None = None,
) -> PruneStats:
    # 신규 규격 호출(prune_old_journals(policy, now, journal_root, archive_root))
    # 과 기존 호출(prune_old_journals(root, archive_root))을 모두 수용한다.
    journal_root: Any
    archive_base_raw: Any
    if len(args) == 2:
        ref_candidate = archive_root
        journal_root = args[0]
        archive_base_raw = args[1]
        if isinstance(ref_candidate, dt.date):
            reference_date = ref_candidate
    else:
        journal_root = root
        archive_base_raw = archive_root
    if archive_base_raw is None or journal_root is None:
        return PruneStats(0, 0)
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    deleted = 0
    normalized = 0
    archive_base = pathlib.Path(str(archive_base_raw)) if not isinstance(archive_base_raw, pathlib.Path) else archive_base_raw
    for part in [p for p in pathlib.Path(str(journal_root)).rglob("dt=*") if p.is_dir()]:
        m = _DT_RE.fullmatch(part.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        rel_parent = part.relative_to(pathlib.Path(str(journal_root))).parent
        out_path = archive_base / rel_parent / f"{part.name}.parquet"
        try:
            normalize = normalizer if normalizer is not None else normalize_l0_partition
            rows = normalize(part, out_path)
        except L1WorkerCrashError as exc:
            # OOM SIGKILL 같은 인프라는 데이터 결함이 아니므로 격리 없이 보존한다
            logger.critical("[DATA] stage=prune status=FAIL reason=worker_crash part=%s error=%s", str(part), str(exc))
            continue
        except L1NormalizationError as exc:
            logger.critical("[DATA] stage=prune status=FAIL reason=%s part=%s", str(exc), str(part))
            if quarantine_root is not None:
                rel_part = part.relative_to(pathlib.Path(str(journal_root)))
                dest = pathlib.Path(quarantine_root) / rel_part
                if dest.exists():
                    logger.critical(
                        "[DATA] stage=quarantine part=%s status=FAIL reason=quarantine_exists", str(part)
                    )
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(part), str(dest))
                logger.critical(
                    "[DATA] stage=quarantine part=%s dest=%s status=MOVED", str(part), str(dest)
                )
            continue
        if rows <= 0 or not out_path.exists():
            logger.critical("[DATA] stage=prune status=FAIL reason=unverified part=%s", str(part))
            continue
        normalized += 1
        rel = "l1/" + out_path.relative_to(archive_base).as_posix()
        if verified_remote_l1 is None or rel not in verified_remote_l1:
            continue
        deleted += sum(1 for f in part.rglob("*") if f.is_file())
        shutil.rmtree(part)
    return PruneStats(deleted, normalized)


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
    for pq_file in sorted(root.rglob("*.parquet")):
        m = _DT_RE.search(pq_file.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        rel = "l1/" + pq_file.relative_to(root).as_posix()
        if rel not in confirmed_remote:
            continue
        pq_file.unlink()
        purged += 1
    return purged
