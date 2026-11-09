"""L1 Parquet -> rclone 원격 오프로드 (업로드 후 크기와 md5로 검증)."""

from __future__ import annotations

import hashlib
import json
import logging
import pathlib
import shutil
import subprocess
from collections.abc import Callable
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import Any

from src.core.config import RcloneArchiveSettings
from src.core.errors import KrxAlphaError
from src.storage.layout import L0_REPO_PREFIX, L1_REPO_PREFIX, l0_partition_for_l1, l1_repo_path

logger = logging.getLogger(__name__)


class RemoteArchiveError(KrxAlphaError):
    """원격 업로드/조회 실패 fail-closed 신호."""


class RemoteTransportError(RemoteArchiveError):
    """Remote listing failed for a transport or auth reason, not because the object is absent."""

    def __init__(self, returncode: int, detail: str, repo_path: str) -> None:
        self.returncode = returncode
        self.detail = detail
        super().__init__(f"verify failed: lsjson rc={returncode} detail={detail}: {repo_path}")


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


@dataclass(frozen=True)
class RemoteObject:
    """One remote file as listed by Drive; ``duplicate`` marks a path listed more than once with differing content."""

    size: int
    md5: str | None
    duplicate: bool = False


_STDERR_BRIEF_CHARS = 300
_HASH_FLAGS = ["--hash", "--hash-type", "md5"]
_MD5_CHUNK = 1024 * 1024


def _brief(stderr: str | None) -> str:
    """Single-line tail of rclone stderr, enough to tell a timeout, quota or auth fault apart."""
    text = " ".join((stderr or "").split())
    return text[-_STDERR_BRIEF_CHARS:] if text else "-"


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
        io_idle_timeout_s: float | None = None,
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
        self._idle_flags = [] if io_idle_timeout_s is None else ["--timeout", f"{io_idle_timeout_s:g}s"]
        self._progress: Callable[[], None] | None = None
        self._md5_cache: dict[pathlib.Path, tuple[int, int, str]] = {}

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
            io_idle_timeout_s=settings.rclone_io_idle_timeout_s,
        )

    def _run(self, cmd: list[str]) -> Any:
        """Run one rclone call; a timeout surfaces as a non-zero result."""
        cmd = [*cmd, *self._idle_flags]
        try:
            return self._runner(cmd, capture_output=True, text=True, check=False, timeout=self._timeout_s)
        except subprocess.TimeoutExpired as exc:
            return subprocess.CompletedProcess(cmd, 124, stdout="", stderr=f"rclone timed out after {exc.timeout}s")
        finally:
            if self._progress is not None:
                self._progress()

    def repo_path_for(self, archive_root: pathlib.Path, local_parquet: pathlib.Path) -> str:
        return l1_repo_path(pathlib.Path(local_parquet).relative_to(archive_root))

    def manifest_repo_path_for(self, manifest_root: pathlib.Path, local_json: pathlib.Path) -> str:
        rel = pathlib.Path(local_json).relative_to(manifest_root).as_posix()
        return f"manifests/{rel}"

    def local_md5(self, path: pathlib.Path) -> str:
        """md5 hex digest of a local file, cached per (size, mtime_ns) so one EOD hashes each file once."""
        local = pathlib.Path(path)
        st = local.stat()
        cached = self._md5_cache.get(local)
        if cached is not None and cached[0] == st.st_size and cached[1] == st.st_mtime_ns:
            return cached[2]
        digest = hashlib.md5(usedforsecurity=False)
        with local.open("rb") as handle:
            while chunk := handle.read(_MD5_CHUNK):
                digest.update(chunk)
        hexdigest = digest.hexdigest()
        self._md5_cache[local] = (st.st_size, st.st_mtime_ns, hexdigest)
        return hexdigest

    def matches(self, remote: RemoteObject | None, local: pathlib.Path) -> bool:
        """True only when the remote object is unique and equals the local file in size and md5 (fail-closed)."""
        if remote is None or remote.duplicate or remote.md5 is None:
            return False
        return remote.size == pathlib.Path(local).stat().st_size and remote.md5 == self.local_md5(local)

    def _remote_object(self, repo_path: str) -> RemoteObject:
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._run(["rclone", "lsjson", dest, *_HASH_FLAGS])
        if result.returncode not in (0, 3):
            raise RemoteTransportError(result.returncode, _brief(result.stderr), repo_path)
        entries = json.loads(result.stdout) if result.returncode == 0 else []
        if not entries:
            raise RemoteArchiveError(f"verify failed: {repo_path}")
        objects = _objects_by_path([{**entry, "Path": repo_path} for entry in entries], repo_path)
        return objects[repo_path]

    def upload_and_verify(self, local_parquet: pathlib.Path, repo_path: str) -> bool:
        local = pathlib.Path(local_parquet)
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._run(["rclone", "copyto", str(local), dest])
        if result.returncode != 0:
            raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
        return self.matches(self._remote_object(repo_path), local)

    def _lsjson_recursive(self, prefix: str = "", *, with_hash: bool = False) -> list[dict[str, Any]]:
        norm = prefix.rstrip("/")
        dest = f"{self._remote_name}:{self._remote_path}/{norm}" if norm else f"{self._remote_name}:{self._remote_path}"
        cmd = ["rclone", "lsjson", dest, "--recursive", "--fast-list", "--files-only"]
        result = self._run([*cmd, *_HASH_FLAGS] if with_hash else cmd)
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

    def remote_objects(self, remote_prefix: str) -> dict[str, RemoteObject]:
        """Size and md5 of every remote file under ``remote_prefix``; duplicate names are flagged, not merged.

        Raises:
            RemoteArchiveError: listing failed or an entry has an invalid size.
        """
        norm = remote_prefix[:-1] if remote_prefix.endswith("/") else remote_prefix
        return _objects_by_path(self._lsjson_recursive(norm, with_hash=True), remote_prefix)

    def verified_paths(self, local_root: pathlib.Path, remote_prefix: str = L1_REPO_PREFIX) -> set[str]:
        """Repo paths of local L1 parquet files whose remote copy matches in size and md5."""
        root = pathlib.Path(local_root)
        objects = self.remote_objects(remote_prefix)
        verified: set[str] = set()
        for pq in sorted(root.rglob("*.parquet")):
            rel = self.repo_path_for(root, pq)
            if self.matches(objects.get(rel), pq):
                verified.add(rel)
        return verified

    def _upload_verified(self, local: pathlib.Path, repo_path: str, local_size: int) -> str | None:
        """Upload one object and verify its remote size and md5; return a failure reason or None when verified."""
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._run(["rclone", "copyto", str(local), dest])
        if result.returncode != 0:
            return f"upload_failed rc={result.returncode} detail={_brief(result.stderr)}"
        try:
            remote = self._remote_object(repo_path)
        except RemoteTransportError as exc:
            return f"verify_transport rc={exc.returncode} detail={exc.detail}"
        except RemoteArchiveError:
            return "verify_missing"
        if remote.duplicate:
            return "duplicate_remote_name"
        if remote.size != local_size:
            return f"size_mismatch remote={remote.size} local={local_size}"
        if remote.md5 is None or remote.md5 != self.local_md5(local):
            return f"md5_mismatch remote={remote.md5}"
        return None

    def sync_l1_tree(self, local_root: pathlib.Path, *, progress: Callable[[], None] | None = None) -> SyncStats:
        """Upload L1 objects whose remote size differs; a failed object is retried once after the pass."""
        root = pathlib.Path(local_root)
        objects = self.remote_objects(L1_REPO_PREFIX)
        stats = SyncStats()
        retry: list[tuple[pathlib.Path, str, int]] = []
        for pq in sorted(root.rglob("*.parquet")):
            try:
                repo_path = self.repo_path_for(root, pq)
                local_size = pq.stat().st_size
                if self.matches(objects.get(repo_path), pq):
                    stats.skipped_verified += 1
                    continue
                reason = self._upload_verified(pq, repo_path, local_size)
                if reason is None:
                    stats.uploaded += 1
                    continue
                # Drive and network faults are often transient; retrying after the pass spaces the attempts apart.
                logger.warning("[DATA] stage=rclone_offload status=RETRY path=%s reason=%s", repo_path, reason)
                retry.append((pq, repo_path, local_size))
            finally:
                if progress is not None:
                    progress()
        for pq, repo_path, local_size in retry:
            try:
                reason = self._upload_verified(pq, repo_path, local_size)
                if reason is None:
                    stats.uploaded += 1
                    logger.info("[DATA] stage=rclone_offload status=RECOVERED path=%s", repo_path)
                    continue
                logger.critical("[DATA] stage=rclone_offload status=FAIL path=%s reason=%s", repo_path, reason)
                stats.failed_verification += 1
            finally:
                if progress is not None:
                    progress()
        return stats

    def sync_manifest_tree(self, manifest_root: pathlib.Path) -> SyncStats:
        """Upload manifests whose remote size differs; a failed one is retried once after the pass, then raises."""
        root = pathlib.Path(manifest_root)
        objects = self.remote_objects("manifests/")
        stats = SyncStats()
        retry: list[tuple[pathlib.Path, str, int]] = []
        for jf in sorted(path for path in root.rglob("*") if path.is_file() and path.suffix in {".json", ".jsonl"}):
            repo_path = self.manifest_repo_path_for(root, jf)
            local_size = jf.stat().st_size
            if self.matches(objects.get(repo_path), jf):
                stats.skipped_verified += 1
                continue
            reason = self._upload_verified(jf, repo_path, local_size)
            if reason is None:
                stats.uploaded += 1
                continue
            logger.warning("[DATA] stage=manifest_offload status=RETRY path=%s reason=%s", repo_path, reason)
            retry.append((jf, repo_path, local_size))
        for jf, repo_path, local_size in retry:
            reason = self._upload_verified(jf, repo_path, local_size)
            if reason is not None:
                raise RemoteArchiveError(f"manifest offload failed: {repo_path} {reason}")
            stats.uploaded += 1
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
                if not l0_dir.startswith(L0_REPO_PREFIX):
                    continue
                try:
                    if (base / l0_dir[len(L0_REPO_PREFIX) :]).exists():
                        stats.skipped_local_present += 1
                        continue
                    dest = f"{self._remote_name}:{self._remote_path}/{l0_dir}"
                    probe = self._run(["rclone", "lsjson", dest, "--max-depth", "1"])
                    if probe.returncode == 3:
                        stats.skipped_absent += 1
                        continue
                    if probe.returncode != 0:
                        stats.failed += 1
                        logger.critical(
                            "[DATA] stage=l0_remote_purge part=%s status=FAIL reason=probe rc=%d detail=%s",
                            l0_dir, probe.returncode, _brief(probe.stderr),
                        )
                        continue
                    purged = self._run(["rclone", "purge", dest])
                    if purged.returncode != 0:
                        stats.failed += 1
                        logger.critical(
                            "[DATA] stage=l0_remote_purge part=%s status=FAIL reason=purge rc=%d detail=%s",
                            l0_dir, purged.returncode, _brief(purged.stderr),
                        )
                        continue
                    stats.purged += 1
                    logger.info("[DATA] stage=l0_remote_purge part=%s status=PURGED", l0_dir)
                except Exception:
                    stats.failed += 1
                    logger.critical(
                        "[DATA] stage=l0_remote_purge part=%s status=FAIL reason=exception",
                        l0_dir,
                        exc_info=True,
                    )
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


def _objects_by_path(entries: list[dict[str, Any]], remote_prefix: str) -> dict[str, RemoteObject]:
    objects: dict[str, RemoteObject] = {}
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
        hashes = entry.get("Hashes")
        md5 = str(hashes["md5"]).lower() if isinstance(hashes, dict) and hashes.get("md5") else None
        prior = objects.get(path)
        if prior is not None:
            # Drive allows duplicate names and a reader gets an arbitrary copy: identical copies are still
            # verifiable (cleanup only), conflicting copies are never verified.
            conflict = prior.duplicate or prior.size != size or prior.md5 is None or prior.md5 != md5
            logger.warning(
                "[DATA] stage=remote_verify status=%s path=%s",
                "DUPLICATE_CONFLICT" if conflict else "DUPLICATE_IDENTICAL",
                path,
            )
            objects[path] = RemoteObject(size=size, md5=md5, duplicate=conflict)
            continue
        objects[path] = RemoteObject(size=size, md5=md5)
    return objects


class RcloneArchiver(GDriveArchiver):
    """하위 호환 별칭: 기존 호출자는 RcloneArchiver를 계속 사용한다."""
