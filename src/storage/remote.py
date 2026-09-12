"""L1 Parquet -> rclone 원격 오프로드 (업로드 후 바이트 크기 검증)."""

from __future__ import annotations

import json
import logging
import pathlib
import shutil
import subprocess
from collections.abc import Callable
from typing import Any

from src.core.config import RcloneArchiveSettings
from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)


class RemoteArchiveError(KrxAlphaError):
    """원격 업로드/조회 실패 fail-closed 신호."""


class RcloneArchiver:
    """L1 파티션을 rclone CLI 서브프로세스로 원격에 업로드하고 원격 크기로 검증한다."""

    def __init__(
        self,
        *,
        remote_name: str,
        remote_path: str,
        runner: Callable[..., Any] = subprocess.run,
    ) -> None:
        self._remote_name = remote_name
        self._remote_path = remote_path
        self._runner = runner

    @classmethod
    def try_from_env(cls) -> RcloneArchiver | None:
        if shutil.which("rclone") is None:
            return None
        settings = RcloneArchiveSettings()
        return cls(remote_name=settings.remote_name, remote_path=settings.remote_path)

    def repo_path_for(self, archive_root: pathlib.Path, local_parquet: pathlib.Path) -> str:
        rel = pathlib.Path(local_parquet).relative_to(archive_root).as_posix()
        return f"l1/{rel}"

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

    def remote_files(self, prefix: str) -> set[str]:
        remote = f"{self._remote_name}:{self._remote_path}"
        result = self._runner(
            ["rclone", "lsjson", remote, "--recursive"], capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            raise RemoteArchiveError(f"lsjson failed: {prefix} {result.stderr}")
        try:
            entries = json.loads(result.stdout)
        except ValueError as exc:
            raise RemoteArchiveError(f"lsjson failed: {prefix} invalid JSON") from exc
        return {
            str(entry["Path"])
            for entry in entries
            if not entry.get("IsDir", False) and str(entry.get("Path", "")).startswith(prefix)
        }

    def sync_l1_tree(self, archive_root: pathlib.Path) -> dict[str, int]:
        root = pathlib.Path(archive_root)
        remote = self.remote_files("l1/")
        stats = {"uploaded": 0, "skipped": 0, "failed": 0}
        for pq in sorted(root.rglob("*.parquet")):
            repo_path = self.repo_path_for(root, pq)
            if repo_path in remote:
                stats["skipped"] += 1
                continue
            try:
                verified = self.upload_and_verify(pq, repo_path)
            except RemoteArchiveError:
                logger.critical("[DATA] stage=rclone_offload status=FAIL path=%s", repo_path)
                stats["failed"] += 1
                continue
            if verified:
                stats["uploaded"] += 1
            else:
                logger.critical("[DATA] stage=rclone_offload status=FAIL reason=size_mismatch path=%s", repo_path)
                stats["failed"] += 1
        return stats
