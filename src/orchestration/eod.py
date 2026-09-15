"""EOD 유지보수/오프로드 (daemon 에서 추출한 타입드 유스케이스)."""

from __future__ import annotations

import datetime as dt
import functools
import logging
import pathlib
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.realtime.manifest import SessionManifest
from src.storage.normalize_worker import run_isolated_normalize
from src.storage.remote import GDriveArchiver, RcloneArchiver, RemoteArchiveError, SyncStats
from src.storage.retention import prune_local_l1, prune_old_journals

logger = logging.getLogger(__name__)

MAX_SESSION_GAP_S: float = 600.0
_REGULAR_OPEN = dt.time(9, 0)
_REGULAR_CLOSE = dt.time(15, 30)
_KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class EodOffloadResult:
    l1: SyncStats
    manifests: SyncStats
    verified_remote_l1: frozenset[str]
    purged: int = 0


def run_eod_maintenance(
    journal_root: pathlib.Path,
    *,
    retain_days: int = 3,
    today: dt.date | None = None,
    archive_root: pathlib.Path | None = None,
    quarantine_root: pathlib.Path | None = None,
    work_root: pathlib.Path | None = None,
    verified_remote_l1: AbstractSet[str] | None = None,
) -> int:
    return prune_old_journals(
        journal_root,
        retain_days=retain_days,
        reference_date=today,
        archive_root=archive_root,
        quarantine_root=quarantine_root,
        verified_remote_l1=verified_remote_l1,
        # 데몬 OOM crash loop를 막기 위해 정규화는 자식 프로세스로 격리한다
        normalizer=functools.partial(run_isolated_normalize, work_root=work_root),
    )


def run_eod_offload(
    archive_root: pathlib.Path,
    manifest_root: pathlib.Path | None = None,
    remote: Any | None = None,
    *,
    archiver: Any = None,
    reference_date: dt.date | None = None,
    retain_days: int = 30,
) -> EodOffloadResult:
    arc = remote if remote is not None else archiver
    if arc is None:
        arc = GDriveArchiver.try_from_env()
    if arc is None:
        logger.critical("[DAEMON] stage=eod_offload status=FAIL reason=rclone_settings_missing")
        empty = SyncStats()
        return EodOffloadResult(l1=empty, manifests=SyncStats(), verified_remote_l1=frozenset(), purged=0)
    l1_stats = arc.sync_l1_tree(archive_root)
    manifest_path = pathlib.Path(manifest_root) if manifest_root is not None else pathlib.Path(archive_root).parent / "manifest"
    archiver = arc
    manifests_stats = archiver.sync_manifest_tree(manifest_path)
    if l1_stats.failed_verification > 0 or manifests_stats.failed_verification > 0:
        raise RemoteArchiveError(
            f"offload verification failed: l1={l1_stats.failed_verification} manifests={manifests_stats.failed_verification}"
        )
    sizes = arc.remote_file_sizes("l1/") if hasattr(arc, "remote_file_sizes") else {}
    local_root = pathlib.Path(archive_root)
    verified: set[str] = set()
    for pq in sorted(local_root.rglob("*.parquet")):
        rel = "l1/" + pq.relative_to(local_root).as_posix()
        if sizes.get(rel) == pq.stat().st_size:
            verified.add(rel)
    confirmed = arc.remote_files("l1/")
    purged = prune_local_l1(
        archive_root, retain_days=retain_days, reference_date=reference_date, confirmed_remote=confirmed
    )
    return EodOffloadResult(
        l1=l1_stats, manifests=manifests_stats, verified_remote_l1=frozenset(verified), purged=purged
    )


def _regular_session_gap_s(gaps: list[dict[str, object]], date: dt.date) -> float:
    open_ns = int(dt.datetime.combine(date, _REGULAR_OPEN, tzinfo=_KST).timestamp()) * 1_000_000_000
    close_ns = int(dt.datetime.combine(date, _REGULAR_CLOSE, tzinfo=_KST).timestamp()) * 1_000_000_000
    clipped: list[tuple[int, int]] = []
    for g in gaps:
        start = int(g["gap_start_ns"])  # type: ignore[call-overload]
        end = int(g["gap_end_ns"])  # type: ignore[call-overload]
        s = max(start, open_ns)
        e = min(end, close_ns)
        if e > s:
            clipped.append((s, e))
    clipped.sort()
    merged: list[list[int]] = []
    for s, e in clipped:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    total_ns = sum(e - s for s, e in merged)
    return total_ns / 1e9


def classify_remote_failure(message: str) -> str:
    if "invalid_grant" in message or "couldn't fetch token" in message:
        return "auth_expired"
    return "remote_error"


def _aftermarket_streams_ok(loaded: SessionManifest) -> bool:
    venue = str(loaded.venue)
    required = {"H0STCNT0", "H0STASP0"} if venue == "krx" else {"H0NXCNT0", "H0NXASP0"} if venue == "nxt" else set()
    accepted = {str(a.get("tr_id")) for a in loaded.subscription_acks if a.get("accepted") is True}
    return bool(required) and required.issubset(accepted)


def aftermarket_eod_ready(*, manifests: list[pathlib.Path], date: dt.date, now: dt.datetime) -> bool:
    if now.astimezone(_KST).time() < dt.time(20, 0): return False  # noqa: E701 - 20:00 전 EOD 차단
    if not manifests: return False  # noqa: E701 - 대상 manifest 없이 성공 표기 금지
    closed: list[bool] = []
    routes: set[tuple[str, str]] = set()
    for path in manifests:
        try:
            loaded = SessionManifest.load(pathlib.Path(path))
        except (ValueError, KeyError, TypeError, OSError) as exc:
            logger.critical("[DAEMON] stage=eod_readiness status=FAIL manifest=%s error=%s", str(path), str(exc))
            closed.append(False)
            continue
        routes.add((str(loaded.venue), str(loaded.session)))
        closed.append(
            loaded.session_date == date
            and loaded.writer_closed_at_ns is not None
            and loaded.expected_close_ns > 0
            and _aftermarket_streams_ok(loaded)
        )
    required_routes = {("krx", "krx_after"), ("nxt", "nxt_after")}
    return len(closed) == 2 and all(closed) and routes == required_routes


def check_backup_freshness(
    *, manifest_dir: pathlib.Path, today: dt.date, archiver: Any = None
) -> list[str]:
    arc = archiver if archiver is not None else RcloneArchiver.try_from_env()
    if arc is None:
        return []
    local: list[str] = []
    base = pathlib.Path(manifest_dir)
    for p in sorted(base.rglob("*.json")):
        rel = p.relative_to(base).as_posix()
        stem_date = p.name.split(".")[0]
        try:
            d = dt.date.fromisoformat(stem_date)
        except ValueError:
            continue
        if d < today:
            local.append(rel)
    remote = {p.removeprefix("manifests/") for p in arc.remote_files("manifests/")}
    return sorted(n for n in local if n not in remote)


def check_session_reconciliation(
    *,
    bars_store: pathlib.Path,
    manifest_path: pathlib.Path,
    date: dt.date,
    journal_root: pathlib.Path | None = None,
    streams: tuple[str, ...] = (),
    vendor: str = "ls",
    max_gap_s: float = MAX_SESSION_GAP_S,
) -> bool:
    store = pathlib.Path(bars_store)
    manifest = pathlib.Path(manifest_path)
    if not store.exists():
        return True
    rows = pl.scan_parquet(store).filter(pl.col("date") == date).collect().height
    if rows == 0:
        return True
    if not manifest.exists():
        return False
    if journal_root is None:
        return True
    issues: list[str] = []
    issues.extend(
        f"journal_missing:{stream}"
        for stream in streams
        if not list((pathlib.Path(journal_root) / vendor / stream / f"dt={date.isoformat()}").glob("*.jsonl.zst"))
    )
    try:
        loaded = SessionManifest.load(manifest)
        gap_s = _regular_session_gap_s(loaded.gaps, date)
        if gap_s > max_gap_s:
            issues.append(f"gap_exceeded:{int(gap_s)}s")
    except (ValueError, KeyError, TypeError):
        issues.append("manifest_unreadable")
    if issues:
        logger.critical(
            "[DATA] stage=session_reconciliation status=FAIL date=%s reasons=%s",
            date.isoformat(),
            ",".join(issues),
        )
        return False
    return True
