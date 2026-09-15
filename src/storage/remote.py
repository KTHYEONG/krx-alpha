"""L1 Parquet -> rclone 원격 오프로드 (업로드 후 바이트 크기 검증)."""

from __future__ import annotations

import json
import logging
import pathlib
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.core.config import RcloneArchiveSettings
from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)


class RemoteArchiveError(KrxAlphaError):
    """원격 업로드/조회 실패 fail-closed 신호."""


@dataclass
class SyncStats:
    """원격 동기화 검증 카운터."""

    uploaded: int = 0
    skipped_verified: int = 0
    failed_verification: int = 0


class GDriveArchiver:
    """L1 파티션을 rclone CLI 서브프로세스로 원격에 업로드하고 원격 크기로 검증한다."""

    def __init__(
        self,
        config: Any | None = None,
        *,
        remote_name: str | None = None,
        remote_path: str | None = None,
        runner: Callable[..., Any] | None = None,
    ) -> None:
        cfg_name = getattr(config, "remote_name", None) if config is not None else None
        cfg_path = getattr(config, "remote_path", None) if config is not None else None
        cfg_runner = getattr(config, "runner", None) if config is not None else None
        self._remote_name = remote_name if remote_name is not None else (cfg_name or "gdrive")
        self._remote_path = (
            remote_path if remote_path is not None else (cfg_path or "quant-lake/live/krx-alpha/data")
        )
        self._runner = runner if runner is not None else (cfg_runner or subprocess.run)

    @classmethod
    def try_from_env(cls) -> GDriveArchiver | None:
        if shutil.which("rclone") is None:
            return None
        settings = RcloneArchiveSettings()
        return cls(remote_name=settings.remote_name, remote_path=settings.remote_path)

    def repo_path_for(self, archive_root: pathlib.Path, local_parquet: pathlib.Path) -> str:
        rel = pathlib.Path(local_parquet).relative_to(archive_root).as_posix()
        return f"l1/{rel}"

    def manifest_repo_path_for(self, manifest_root: pathlib.Path, local_json: pathlib.Path) -> str:
        rel = pathlib.Path(local_json).relative_to(manifest_root).as_posix()
        return f"manifests/{rel}"

    def _remote_size(self, repo_path: str) -> int:
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._runner(["rclone", "lsjson", dest], capture_output=True, text=True, check=False)
        entries = json.loads(result.stdout) if result.returncode == 0 else []
        if not entries:
            raise RemoteArchiveError(f"verify failed: {repo_path}")
        return int(entries[0]["Size"])

    def upload_and_verify(self, local_parquet: pathlib.Path, repo_path: str) -> bool:
        local = pathlib.Path(local_parquet)
        dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
        result = self._runner(
            ["rclone", "copyto", str(local), dest], capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
        return self._remote_size(repo_path) == local.stat().st_size

    def _lsjson_recursive(self) -> list[dict[str, Any]]:
        remote = f"{self._remote_name}:{self._remote_path}"
        result = self._runner(
            ["rclone", "lsjson", remote, "--recursive"], capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            raise RemoteArchiveError(f"lsjson failed: {result.stderr}")
        try:
            entries: list[dict[str, Any]] = json.loads(result.stdout)
        except ValueError as exc:
            raise RemoteArchiveError("lsjson failed: invalid JSON") from exc
        return entries

    def remote_files(self, prefix: str) -> set[str]:
        entries = self._lsjson_recursive()
        return {
            str(entry["Path"])
            for entry in entries
            if not entry.get("IsDir", False) and str(entry.get("Path", "")).startswith(prefix)
        }

    def remote_file_sizes(self, remote_prefix: str) -> dict[str, int]:
        entries = self._lsjson_recursive()
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

    def sync_l1_tree(self, local_root: pathlib.Path) -> SyncStats:
        root = pathlib.Path(local_root)
        sizes = self.remote_file_sizes("l1/")
        stats = SyncStats()
        for pq in sorted(root.rglob("*.parquet")):
            repo_path = self.repo_path_for(root, pq)
            local_size = pq.stat().st_size
            if sizes.get(repo_path) == local_size:
                stats.skipped_verified += 1
                continue
            try:
                dest = f"{self._remote_name}:{self._remote_path}/{repo_path}"
                result = self._runner(
                    ["rclone", "copyto", str(pq), dest], capture_output=True, text=True, check=False
                )
                if result.returncode != 0:
                    raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
                refreshed = self.remote_file_sizes("l1/")
                if refreshed.get(repo_path) != local_size:
                    logger.critical(
                        "[DATA] stage=rclone_offload status=FAIL reason=size_mismatch path=%s", repo_path
                    )
                    stats.failed_verification += 1
                    continue
                stats.uploaded += 1
                sizes = refreshed
            except RemoteArchiveError:
                logger.critical("[DATA] stage=rclone_offload status=FAIL path=%s", repo_path)
                stats.failed_verification += 1
                continue
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
            result = self._runner(
                ["rclone", "copyto", str(jf), dest], capture_output=True, text=True, check=False
            )
            if result.returncode != 0:
                raise RemoteArchiveError(f"upload failed: {repo_path} {result.stderr}")
            refreshed = self.remote_file_sizes("manifests/")
            if refreshed.get(repo_path) != local_size:
                raise RemoteArchiveError(f"verify failed: {repo_path}")
            stats.uploaded += 1
            sizes = refreshed
        return stats


class RcloneArchiver(GDriveArchiver):
    """하위 호환 별칭: 기존 호출자는 RcloneArchiver를 계속 사용한다."""
