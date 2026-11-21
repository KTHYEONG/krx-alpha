"""Re-encode v1 L1 parquet objects to the v2 writer settings, one verified object at a time."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
import re
import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.storage.layout import L1_REPO_PREFIX
from src.storage.parquet_options import (
    L1_ENCODING_METADATA_KEY,
    L1_ENCODING_VERSION,
    l1_encoding_metadata,
    l1_parquet_writer_kwargs,
)
from src.storage.remote import RemoteArchiveError, RemoteObject

if TYPE_CHECKING:
    from src.storage.remote import GDriveArchiver

logger = logging.getLogger(__name__)

_KST: Final = ZoneInfo("Asia/Seoul")
_PARTITION_NAME: Final[re.Pattern[str]] = re.compile(r"dt=(\d{4}-\d{2}-\d{2})\.parquet")
_NON_JOURNAL_TOP_LEVEL: Final[frozenset[str]] = frozenset({"snapshot", "history"})
_REPAIRED_DERIVED: Final[str] = "ticks_repaired"
_ENCODING_KEY: Final[bytes] = L1_ENCODING_METADATA_KEY.encode()
_ENCODING_VALUE: Final[bytes] = L1_ENCODING_VERSION.encode()


@dataclass(frozen=True)
class ReencodeResult:
    path: str
    rows: int
    bytes_before: int
    bytes_after: int
    status: str


def _has_v2_encoding(metadata: dict[bytes, bytes] | None) -> bool:
    return metadata is not None and metadata.get(_ENCODING_KEY) == _ENCODING_VALUE


def _row_groups_equal(source: pathlib.Path, written: pathlib.Path) -> bool:
    with pq.ParquetFile(source) as src_file, pq.ParquetFile(written) as dst_file:
        if src_file.num_row_groups != dst_file.num_row_groups:
            return False
        # One row-group pair in memory at a time keeps the verification bounded.
        for index in range(src_file.num_row_groups):
            if not src_file.read_row_group(index).equals(dst_file.read_row_group(index)):
                return False
    return True


def reencode_l1_file(src: pathlib.Path, dst_tmp: pathlib.Path) -> ReencodeResult:
    """Rewrite one L1 parquet with the v2 writer settings, keeping rows, row groups and metadata.

    Writes ``dst_tmp`` only; the caller decides whether it replaces anything. A file whose
    metadata already carries the v2 key is skipped without writing. The rewrite is verified
    by re-reading ``dst_tmp`` row group by row group; a mismatch deletes ``dst_tmp`` and
    reports ``verify_failed``.
    """
    source = pathlib.Path(src)
    target = pathlib.Path(dst_tmp)
    bytes_before = source.stat().st_size
    with pq.ParquetFile(source) as parquet:
        schema = parquet.schema_arrow
        rows = parquet.metadata.num_rows
        if _has_v2_encoding(schema.metadata):
            return ReencodeResult(str(source), rows, bytes_before, bytes_before, "already_v2")
        metadata = {**(schema.metadata or {}), **{k.encode(): v.encode() for k, v in l1_encoding_metadata().items()}}
        with pq.ParquetWriter(target, schema.with_metadata(metadata), **l1_parquet_writer_kwargs(schema)) as writer:
            for index in range(parquet.num_row_groups):
                table = parquet.read_row_group(index)
                writer.write_table(table, row_group_size=max(table.num_rows, 1))
    if not _row_groups_equal(source, target):
        target.unlink(missing_ok=True)
        return ReencodeResult(str(source), rows, bytes_before, bytes_before, "verify_failed")
    return ReencodeResult(str(source), rows, bytes_before, target.stat().st_size, "reencoded")


def _partition_day(repo_path: str) -> dt.date | None:
    match = _PARTITION_NAME.fullmatch(pathlib.PurePosixPath(repo_path).name)
    if match is None:
        return None
    return dt.date.fromisoformat(match.group(1))


def _is_backlog_candidate(repo_path: str, cutoff: dt.date) -> bool:
    """Journal-backed or ticks_repaired L1 partitions strictly older than the local retention cutoff."""
    parts = pathlib.PurePosixPath(repo_path[len(L1_REPO_PREFIX) :]).parts
    if not parts or parts[0] in _NON_JOURNAL_TOP_LEVEL:
        return False
    if parts[0] == "derived" and (len(parts) < 2 or parts[1] != _REPAIRED_DERIVED):
        return False
    day = _partition_day(repo_path)
    return day is not None and day < cutoff


def _load_ledger(path: pathlib.Path) -> set[tuple[str, int]]:
    """(repo_path, size) pairs already verified on the remote; torn or foreign lines are ignored."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return set()
    done: set[tuple[str, int]] = set()
    for line in lines:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict) and isinstance(entry.get("repo_path"), str) and isinstance(entry.get("size"), int):
            done.add((entry["repo_path"], entry["size"]))
    return done


def _append_ledger(path: pathlib.Path, repo_path: str, size: int, md5: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"repo_path": repo_path, "size": size, "md5": md5}, sort_keys=True)
    with path.open("ab+") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() > 0:
            handle.seek(-1, os.SEEK_END)
            if handle.read(1) != b"\n":
                handle.write(b"\n")
        handle.write(line.encode("utf-8") + b"\n")
        handle.flush()
        os.fsync(handle.fileno())


def _download(archiver: GDriveArchiver, repo_path: str, dest: pathlib.Path) -> bool:
    remote = f"{archiver._remote_name}:{archiver._remote_path}/{repo_path}"  # noqa: SLF001 - same remote as uploads
    return bool(archiver._run(["rclone", "copyto", remote, str(dest)]).returncode == 0)  # noqa: SLF001


def _reencode_one(
    archiver: GDriveArchiver,
    repo_path: str,
    remote: RemoteObject,
    *,
    l1_root: pathlib.Path,
    work_dir: pathlib.Path,
    ledger_path: pathlib.Path,
) -> ReencodeResult:
    relative = repo_path[len(L1_REPO_PREFIX) :]
    local = l1_root / relative
    if local.is_file() and local.stat().st_size != remote.size:
        logger.warning(
            "[DATA] stage=l1_reencode path=%s status=local_mismatch local=%d remote=%d",
            repo_path,
            local.stat().st_size,
            remote.size,
        )
        return ReencodeResult(repo_path, 0, 0, 0, "local_mismatch")
    has_local = local.is_file()
    downloaded = work_dir / "src" / relative
    staged = local.with_name(local.name + ".v2.tmp") if has_local else work_dir / "out" / relative
    staged.parent.mkdir(parents=True, exist_ok=True)
    source = local if has_local else downloaded
    try:
        if not has_local:
            downloaded.parent.mkdir(parents=True, exist_ok=True)
            if not _download(archiver, repo_path, downloaded) or not archiver.matches(remote, downloaded):
                logger.error("[DATA] stage=l1_reencode path=%s status=error reason=download", repo_path)
                return ReencodeResult(repo_path, 0, 0, 0, "error")
        result = replace(reencode_l1_file(source, staged), path=repo_path)
        if result.status == "verify_failed":
            logger.critical(
                "[DATA] stage=l1_reencode path=%s status=verify_failed reason=row_group_mismatch", repo_path
            )
        elif result.status == "reencoded":
            if has_local:
                os.replace(staged, local)
                upload_source = local
            else:
                upload_source = staged
            try:
                uploaded = archiver.upload_and_verify(upload_source, repo_path)
            except RemoteArchiveError as exc:
                logger.critical("[DATA] stage=l1_reencode path=%s status=error reason=upload detail=%s", repo_path, exc)
                uploaded = False
            if uploaded:
                _append_ledger(ledger_path, repo_path, upload_source.stat().st_size, archiver.local_md5(upload_source))
            else:
                logger.critical("[DATA] stage=l1_reencode path=%s status=error reason=remote_verify", repo_path)
                result = replace(result, status="error")
        elif result.status == "already_v2":
            # Files written v2 by the normalizer are recorded too, so later runs skip them without a download.
            _append_ledger(ledger_path, repo_path, remote.size, archiver.local_md5(source))
        logger.info(
            "[DATA] stage=l1_reencode path=%s status=%s bytes_before=%d bytes_after=%d",
            repo_path,
            result.status,
            result.bytes_before,
            result.bytes_after,
        )
        return result
    except (OSError, KrxAlphaError, pa.ArrowException) as exc:
        logger.error(
            "[DATA] stage=l1_reencode path=%s status=error reason=%s", repo_path, type(exc).__name__, exc_info=True
        )
        return ReencodeResult(repo_path, 0, 0, 0, "error")
    finally:
        downloaded.unlink(missing_ok=True)
        staged.unlink(missing_ok=True)


def run_backlog(
    archiver: GDriveArchiver,
    *,
    l1_root: pathlib.Path,
    work_dir: pathlib.Path,
    deadline_monotonic: float,
    ledger_path: pathlib.Path,
) -> list[ReencodeResult]:
    """Re-encode every eligible remote L1 object older than the local retention window, oldest day first.

    Stops between files once ``deadline_monotonic`` passes. An object whose (repo_path, size) is in
    ``ledger_path`` was verified in an earlier run and is reported as ``already_v2`` without any transfer.
    """
    retain_days = CollectorSettings().journal_retain_days
    cutoff = dt.datetime.now(_KST).date() - dt.timedelta(days=retain_days)
    objects = archiver.remote_objects(L1_REPO_PREFIX)
    candidates = sorted(
        (path for path in objects if _is_backlog_candidate(path, cutoff)),
        key=lambda path: (_partition_day(path) or cutoff, path),
    )
    done = _load_ledger(ledger_path)
    results: list[ReencodeResult] = []
    for repo_path in candidates:
        if time.monotonic() >= deadline_monotonic:
            logger.info("[DATA] stage=l1_reencode status=DEADLINE remaining=%d", len(candidates) - len(results))
            break
        remote = objects[repo_path]
        if remote.duplicate:
            logger.warning("[DATA] stage=l1_reencode path=%s status=SKIP reason=duplicate_remote_name", repo_path)
            continue
        if (repo_path, remote.size) in done:
            results.append(ReencodeResult(repo_path, 0, remote.size, remote.size, "already_v2"))
            continue
        results.append(
            _reencode_one(archiver, repo_path, remote, l1_root=l1_root, work_dir=work_dir, ledger_path=ledger_path)
        )
    return results
