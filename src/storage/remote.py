"""L1 Parquet -> HuggingFace private Dataset 오프로드 (업로드 후 바이트 크기 검증)."""

from __future__ import annotations

import logging
import pathlib

from huggingface_hub import HfApi
from huggingface_hub.errors import HfHubHTTPError
from pydantic import ValidationError

from src.core.config import HfArchiveSettings
from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)


class RemoteArchiveError(KrxAlphaError):
    """HF 업로드/조회 실패 fail-closed 신호."""


class HfDatasetArchiver:
    """L1 파티션을 HF Dataset repo 로 업로드하고 원격 크기로 검증한다."""

    def __init__(self, *, token: str, repo_id: str, api: HfApi | None = None) -> None:
        self._api = api if api is not None else HfApi(token=token)
        self._repo_id = repo_id

    @classmethod
    def try_from_env(cls) -> HfDatasetArchiver | None:
        try:
            settings = HfArchiveSettings()  # type: ignore[call-arg]
        except ValidationError:
            return None
        return cls(token=settings.hf_token, repo_id=settings.hf_dataset_repo)

    def repo_path_for(self, archive_root: pathlib.Path, local_parquet: pathlib.Path) -> str:
        rel = pathlib.Path(local_parquet).relative_to(archive_root).as_posix()
        return f"l1/{rel}"

    def upload_and_verify(self, local_parquet: pathlib.Path, repo_path: str) -> bool:
        local = pathlib.Path(local_parquet)
        try:
            self._api.upload_file(
                path_or_fileobj=str(local),
                path_in_repo=repo_path,
                repo_id=self._repo_id,
                repo_type="dataset",
                commit_message=f"l1 offload {repo_path}",
            )
        except (HfHubHTTPError, OSError) as exc:
            raise RemoteArchiveError(f"upload failed: {repo_path}") from exc
        info = self._api.get_paths_info(self._repo_id, [repo_path], repo_type="dataset")
        remote_size = next((getattr(i, "size", None) for i in info if getattr(i, "path", None) == repo_path), None)
        return remote_size == local.stat().st_size

    def remote_files(self, prefix: str) -> set[str]:
        try:
            files = self._api.list_repo_files(self._repo_id, repo_type="dataset")
        except (HfHubHTTPError, OSError) as exc:
            raise RemoteArchiveError("list_repo_files failed") from exc
        return {f for f in files if f.startswith(prefix)}

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
                logger.critical("[DATA] stage=hf_offload status=FAIL path=%s", repo_path)
                stats["failed"] += 1
                continue
            if verified:
                stats["uploaded"] += 1
            else:
                logger.critical("[DATA] stage=hf_offload status=FAIL reason=size_mismatch path=%s", repo_path)
                stats["failed"] += 1
        return stats
