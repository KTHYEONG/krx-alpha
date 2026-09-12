"""Rclone remote archive unit tests."""

from __future__ import annotations


def test_rclone_archiver_repo_path_for_builds_l1_prefixed_posix_path(tmp_path) -> None:
    from src.storage.remote import RcloneArchiver

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data")
    archive_root = tmp_path / "l1"
    local = archive_root / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"

    assert arc.repo_path_for(archive_root, local) == "l1/kis/H0STCNT0/dt=2026-09-01.parquet"


def test_upload_and_verify_returns_true_on_size_match(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    pq = tmp_path / "dt=2026-09-01.parquet"
    pq.write_bytes(b"x" * 100)
    calls: list[list[str]] = []

    def _runner(args, **kwargs):
        calls.append(args)
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"Path": "x", "Size": 100, "IsDir": False}]), stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    ok = arc.upload_and_verify(pq, "l1/kis/H0STCNT0/dt=2026-09-01.parquet")

    assert ok is True
    assert calls[0] == ["rclone", "copyto", str(pq), "gdrive:quant-lake/live/krx-alpha/data/l1/kis/H0STCNT0/dt=2026-09-01.parquet"]


def test_upload_and_verify_returns_false_on_size_mismatch(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    pq = tmp_path / "dt=2026-09-01.parquet"
    pq.write_bytes(b"x" * 100)

    def _runner(args, **kwargs):
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"Path": "x", "Size": 7, "IsDir": False}]), stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    ok = arc.upload_and_verify(pq, "l1/kis/H0STCNT0/dt=2026-09-01.parquet")

    assert ok is False


def test_upload_and_verify_raises_on_copyto_failure(tmp_path) -> None:
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import RcloneArchiver, RemoteArchiveError

    pq = tmp_path / "dt=2026-09-01.parquet"
    pq.write_bytes(b"x" * 10)

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="network unreachable")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    with pytest.raises(RemoteArchiveError, match="upload"):
        arc.upload_and_verify(pq, "l1/kis/H0STCNT0/dt=2026-09-01.parquet")


def test_upload_and_verify_raises_when_size_verification_fails(tmp_path) -> None:
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import RcloneArchiver, RemoteArchiveError

    pq = tmp_path / "dt=2026-09-01.parquet"
    pq.write_bytes(b"x" * 10)

    def _runner(args, **kwargs):
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="timeout")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    with pytest.raises(RemoteArchiveError, match="verify"):
        arc.upload_and_verify(pq, "l1/kis/H0STCNT0/dt=2026-09-01.parquet")


def test_remote_files_filters_dirs_and_prefix() -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    entries = [
        {"Path": "l1", "IsDir": True},
        {"Path": "l1/kis/H0STCNT0/dt=2026-09-01.parquet", "IsDir": False, "Size": 10},
        {"Path": "bars/daily.parquet", "IsDir": False, "Size": 5},
    ]

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout=json.dumps(entries), stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    assert arc.remote_files("l1/") == {"l1/kis/H0STCNT0/dt=2026-09-01.parquet"}


def test_remote_files_raises_on_lsjson_failure() -> None:
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import RcloneArchiver, RemoteArchiveError

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="directory not found")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    with pytest.raises(RemoteArchiveError, match="lsjson"):
        arc.remote_files("l1/")


def test_sync_l1_tree_uploads_new_skips_existing(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    (root / "dt=2026-09-01.parquet").write_bytes(b"a" * 50)
    (root / "dt=2026-09-02.parquet").write_bytes(b"b" * 60)

    calls: list[list[str]] = []

    def _runner(args, **kwargs):
        calls.append(args)
        if "--recursive" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps([{"Path": "l1/kis/H0STCNT0/dt=2026-09-01.parquet", "IsDir": False, "Size": 50}]),
                stderr="",
            )
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": "x", "IsDir": False, "Size": 60}]),
            stderr="",
        )

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    stats = arc.sync_l1_tree(tmp_path / "l1")

    assert stats == {"uploaded": 1, "skipped": 1, "failed": 0}


def test_sync_l1_tree_counts_failed_upload(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    (root / "dt=2026-09-01.parquet").write_bytes(b"a" * 50)

    def _runner(args, **kwargs):
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout=json.dumps([]), stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    stats = arc.sync_l1_tree(tmp_path / "l1")

    assert stats == {"uploaded": 0, "skipped": 0, "failed": 1}


def test_remote_files_raises_on_invalid_json() -> None:
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import RcloneArchiver, RemoteArchiveError

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="not json", stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    with pytest.raises(RemoteArchiveError, match="lsjson"):
        arc.remote_files("l1/")


def test_sync_l1_tree_counts_size_mismatch(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    (root / "dt=2026-09-01.parquet").write_bytes(b"a" * 50)

    def _runner(args, **kwargs):
        if "--recursive" in args:
            return SimpleNamespace(returncode=0, stdout=json.dumps([]), stderr="")
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"Path": "x", "IsDir": False, "Size": 1}]), stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    stats = arc.sync_l1_tree(tmp_path / "l1")

    assert stats == {"uploaded": 0, "skipped": 0, "failed": 1}


def test_try_from_env_returns_none_without_rclone_binary(monkeypatch) -> None:
    import src.storage.remote as remote_mod

    monkeypatch.setattr(remote_mod.shutil, "which", lambda name: None)

    assert remote_mod.RcloneArchiver.try_from_env() is None


def test_try_from_env_builds_archiver_when_binary_present(monkeypatch) -> None:
    import src.storage.remote as remote_mod

    monkeypatch.setattr(remote_mod.shutil, "which", lambda name: "/usr/bin/rclone")
    monkeypatch.delenv("KRX_ALPHA_BACKUP_REMOTE_NAME", raising=False)
    monkeypatch.delenv("KRX_ALPHA_BACKUP_REMOTE_PATH", raising=False)

    arc = remote_mod.RcloneArchiver.try_from_env()

    assert isinstance(arc, remote_mod.RcloneArchiver)
    assert arc._remote_name == "gdrive"
    assert arc._remote_path == "quant-lake/live/krx-alpha/data"

