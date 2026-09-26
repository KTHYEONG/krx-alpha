"""L1 Parquet -> rclone 원격 오프로드 (업로드 후 바이트 크기 검증)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
import re
import shutil
import subprocess
from collections.abc import Callable
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import Any

from src.core.config import RcloneArchiveSettings
from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

_L1_DT_RE = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})\.parquet$")


def l0_partition_for_l1(repo_path: str) -> str | None:
    """Map a remote L1 object path to the remote L0 partition directory it supersedes.

    Why: L1 parquet retains every raw L0 frame losslessly, so once an L1 object is
    verified remotely its L0 journal partition is redundant offsite.

    Args:
        repo_path: Remote-relative posix path, e.g. "l1/ls/krx/regular/H0STASP0/dt=2026-09-18.parquet".

    Returns:
        "l0/<parent>/dt=YYYY-MM-DD" for journal-backed L1 objects; None for snapshot
        datasets ("l1/snapshot/..."), non-"l1/" paths, or names not matching "dt=YYYY-MM-DD.parquet".
    """
    if not repo_path.startswith("l1/"):
        return None
    rest = repo_path[len("l1/") :]
    if rest == "snapshot" or rest.startswith("snapshot/"):
        return None
    if "/" not in rest:
        return None
    parent, _, name = rest.rpartition("/")
    if not parent:
        return None
    match = _L1_DT_RE.match(name)
    if match is None:
        return None
    try:
        dt.date.fromisoformat(match.group(1))
    except ValueError:
        return None
    return f"l0/{parent}/{name[: -len('.parquet')]}"


class RemoteArchiveError(KrxAlphaError):
    """원격 업로드/조회 실패 fail-closed 신호."""


@dataclass
class SyncStats:
    """원격 동기화 검증 카운터."""

    uploaded: int = 0
    skipped_verified: int = 0
    failed_verification: int = 0


@dataclass
class PurgeStats:
    """원격 L0 정리 카운터."""

    purged: int = 0
    skipped_local_present: int = 0
    skipped_absent: int = 0
    failed: int = 0


class GDriveArchiver:
    """L1 파티션을 rclone CLI 서브프로세스로 원격에 업로드하고 원격 크기로 검증한다."""

    def __init__(
        self,
        config: Any | None = None,
        *,
        remote_name: str | None = None,
        remote_path: str | None = None,
        runner: Callable[..., Any] | None = None,
        timeout_s: float = 600.0,
    ) -> None:
        cfg_name = getattr(config, "remote_name", None) if config is not None else None
        cfg_path = getattr(config, "remote_path", None) if config is not None else None
        cfg_runner = getattr(config, "runner", None) if config is not None else None
        self._remote_name = remote_name if remote_name is not None else (cfg_name or "gdrive")
        self._remote_path = (
            remote_path if remote_path is not None else (cfg_path or "quant-lake/live/krx-alpha/data")
        )
        self._runner = runner if runner is not None else (cfg_runner or subprocess.run)
        self._timeout_s = timeout_s
        self._progress: Callable[[], None] | None = None

    def bind_progress(self, progress: Callable[[], None] | None) -> None:
        """Report liveness after every rclone call so EOD never goes silent longer than one call."""
        self._progress = progress

    @classmethod
    def try_from_env(cls) -> GDriveArchiver | None:
        if shutil.which("rclone") is None:
            return None
        settings = RcloneArchiveSettings()
        return cls(
            remote_name=settings.remote_name,
            remote_path=settings.remote_path,
            timeout_s=settings.rclone_timeout_s,
        )

    def _run(self, cmd: list[str]) -> Any:
        """Run one rclone call; a timeout surfaces as a non-zero result."""
        try:
            return self._runner(cmd, capture_output=True, text=True, check=False, timeout=self._timeout_s)
        except subprocess.TimeoutExpired as exc:
            return subprocess.CompletedProcess(cmd, 124, stdout="", stderr=f"rclone timed out after {exc.timeout}s")
        finally:
            if self._progress is not None:
                self._progress()

    def repo_path_for(self, archive_root: pathlib.Path, local_parquet: pathlib.Path) -> str:
        rel = pathlib.Path(local_parquet).relative_to(archive_root).as_posix()
        return f"l1/{rel}"

    def manifest_repo_path_for(self, manifest_root: pathlib.Path, local_json: pathlib.Path) -> str:
        rel = pathlib.Path(local_json).relative_to(manifest_root).as_posix()
        return f"manifests/{rel}"

    def _remote_size(self, repo_path: str) -> int:
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._run(["rclone", "lsjson", dest])
        entries = json.loads(result.stdout) if result.returncode == 0 else []
        if not entries:
            raise RemoteArchiveError(f"verify failed: {repo_path}")
        return int(entries[0]["Size"])

    def upload_and_verify(self, local_parquet: pathlib.Path, repo_path: str) -> bool:
        local = pathlib.Path(local_parquet)
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._run(["rclone", "copyto", str(local), dest])
        if result.returncode != 0:
            raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
        return self._remote_size(repo_path) == local.stat().st_size

    def _lsjson_recursive(self, prefix: str = "") -> list[dict[str, Any]]:
        norm = prefix.rstrip("/")
        dest = f"{self._remote_name}:{self._remote_path}/{norm}" if norm else f"{self._remote_name}:{self._remote_path}"
        result = self._run(["rclone", "lsjson", dest, "--recursive", "--fast-list", "--files-only"])
        if result.returncode == 3:
            return []
        if result.returncode != 0:
            raise RemoteArchiveError(f"lsjson failed: {result.stderr}")
        try:
            entries: list[dict[str, Any]] = json.loads(result.stdout)
        except ValueError as exc:
            raise RemoteArchiveError("lsjson failed: invalid JSON") from exc
        if not norm:
            return entries
        if any(
            str(entry.get("Path", "")) == norm
            or str(entry.get("Path", "")).startswith(f"{norm}/")
            for entry in entries
        ):
            return entries
        reframed: list[dict[str, Any]] = []
        for entry in entries:
            path = str(entry.get("Path", ""))
            reframed.append({**entry, "Path": f"{norm}/{path}" if path else norm})
        return reframed

    def remote_files(self, prefix: str) -> set[str]:
        norm = prefix[:-1] if prefix.endswith("/") else prefix
        entries = self._lsjson_recursive(norm)
        return {
            str(entry["Path"])
            for entry in entries
            if not entry.get("IsDir", False) and str(entry.get("Path", "")).startswith(prefix)
        }

    def remote_file_sizes(self, remote_prefix: str) -> dict[str, int]:
        norm = remote_prefix[:-1] if remote_prefix.endswith("/") else remote_prefix
        entries = self._lsjson_recursive(norm)
        sizes: dict[str, int] = {}
        for entry in entries:
            if entry.get("IsDir", False):
                continue
            path = str(entry.get("Path", ""))
            if not path.startswith(remote_prefix):
                continue
            raw_size = entry.get("Size", None)
            try:
                size = int(raw_size)  # type: ignore[arg-type]
            except (TypeError, ValueError) as exc:
                raise RemoteArchiveError(f"lsjson failed: {remote_prefix} invalid size") from exc
            if size < 0:
                raise RemoteArchiveError(f"lsjson failed: {remote_prefix} invalid size")
            sizes[path] = size
        return sizes

    def sync_l1_tree(self, local_root: pathlib.Path, *, progress: Callable[[], None] | None = None) -> SyncStats:
        root = pathlib.Path(local_root)
        sizes = self.remote_file_sizes("l1/")
        stats = SyncStats()
        for pq in sorted(root.rglob("*.parquet")):
            try:
                repo_path = self.repo_path_for(root, pq)
                local_size = pq.stat().st_size
                if sizes.get(repo_path) == local_size:
                    stats.skipped_verified += 1
                    continue
                try:
                    dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
                    result = self._run(["rclone", "copyto", str(pq), dest])
                    if result.returncode != 0:
                        raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
                    remote_size = self._remote_size(repo_path)
                    if remote_size != local_size:
                        logger.critical(
                            "[DATA] stage=rclone_offload status=FAIL reason=size_mismatch path=%s", repo_path
                        )
                        stats.failed_verification += 1
                        continue
                    stats.uploaded += 1
                    sizes[repo_path] = local_size
                except RemoteArchiveError:
                    logger.critical("[DATA] stage=rclone_offload status=FAIL path=%s", repo_path)
                    stats.failed_verification += 1
                    continue
            finally:
                if progress is not None:
                    progress()
        return stats

    def sync_manifest_tree(self, manifest_root: pathlib.Path) -> SyncStats:
        root = pathlib.Path(manifest_root)
        sizes = self.remote_file_sizes("manifests/")
        stats = SyncStats()
        for jf in sorted(root.rglob("*.json")):
            repo_path = self.manifest_repo_path_for(root, jf)
            local_size = jf.stat().st_size
            if sizes.get(repo_path) == local_size:
                stats.skipped_verified += 1
                continue
            dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
            result = self._run(["rclone", "copyto", str(jf), dest])
            if result.returncode != 0:
                raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
            if self._remote_size(repo_path) != local_size:
                raise RemoteArchiveError(f"verify failed: {repo_path}")
            stats.uploaded += 1
            sizes[repo_path] = local_size
        return stats

    def purge_superseded_l0(
        self, verified_remote_l1: AbstractSet[str], journal_root: pathlib.Path,
        *, progress: Callable[[], None] | None = None,
    ) -> PurgeStats:
        """Remove remote L0 partitions made redundant by remote-verified L1 objects.

        A partition is purged only when (a) its L1 object is in ``verified_remote_l1``,
        (b) the local journal partition ``journal_root/<parent>/dt=D`` no longer exists
        (so the nightly host copy cannot re-upload it), and (c) the remote directory exists.
        Purge moves objects to Drive trash (rclone drive default), keeping a manual undo window.

        Args:
            verified_remote_l1: Remote-relative L1 paths whose remote size equals the local size.
            journal_root: Local L0 journal root (``data/l0``).

        Returns:
            Counts of purged, skipped_local_present, skipped_absent, failed partitions.
        """
        stats = PurgeStats()
        l0_dirs = sorted(
            {l0_dir for path in verified_remote_l1 if (l0_dir := l0_partition_for_l1(path)) is not None}
        )
        base = pathlib.Path(journal_root)
        for l0_dir in l0_dirs:
            try:
                if not l0_dir.startswith("l0/"):
                    continue
                try:
                    if (base / l0_dir[len("l0/") :]).exists():
                        stats.skipped_local_present += 1
                        continue
                    dest = f"{self._remote_name}:{self._remote_path}/{l0_dir}"
                    probe = self._run(["rclone", "lsjson", dest, "--max-depth", "1"])
                    if probe.returncode == 3:
                        stats.skipped_absent += 1
                        continue
                    if probe.returncode != 0:
                        stats.failed += 1
                        logger.critical("[DATA] stage=l0_remote_purge part=%s status=FAIL", l0_dir)
                        continue
                    purged = self._run(["rclone", "purge", dest])
                    if purged.returncode != 0:
                        stats.failed += 1
                        logger.critical("[DATA] stage=l0_remote_purge part=%s status=FAIL", l0_dir)
                        continue
                    stats.purged += 1
                    logger.info("[DATA] stage=l0_remote_purge part=%s status=PURGED", l0_dir)
                except Exception:
                    stats.failed += 1
                    logger.critical("[DATA] stage=l0_remote_purge part=%s status=FAIL", l0_dir)
                    continue
            finally:
                if progress is not None:
                    progress()
        logger.info(
            "[DATA] stage=l0_remote_purge purged=%d skipped_local_present=%d skipped_absent=%d failed=%d",
            stats.purged,
            stats.skipped_local_present,
            stats.skipped_absent,
            stats.failed,
        )
        return stats


class RcloneArchiver(GDriveArchiver):
    """하위 호환 별칭: 기존 호출자는 RcloneArchiver를 계속 사용한다."""
