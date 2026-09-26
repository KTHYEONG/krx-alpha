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
    remote_state = {"l1/kis/H0STCNT0/dt=2026-09-01.parquet": 50}

    def _runner(args, **kwargs):
        calls.append(args)
        if "--recursive" in args:
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps(
                    [{"Path": p, "IsDir": False, "Size": s} for p, s in sorted(remote_state.items())]
                ),
                stderr="",
            )
        if args[1] == "copyto":
            from pathlib import Path as _P

            dest = args[3]
            prefix = "quant-lake/live/krx-alpha/data/"
            repo_path = dest.split(prefix, 1)[1] if prefix in dest else _P(args[2]).name
            remote_state[repo_path] = _P(args[2]).stat().st_size
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": "x", "IsDir": False, "Size": 60}]),
            stderr="",
        )

    arc = RcloneArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)

    stats = arc.sync_l1_tree(tmp_path / "l1")

    assert stats.uploaded == 1
    assert stats.skipped_verified == 1
    assert stats.failed_verification == 0


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

    assert stats.uploaded == 0
    assert stats.skipped_verified == 0
    assert stats.failed_verification == 1


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

    assert stats.uploaded == 0
    assert stats.skipped_verified == 0
    assert stats.failed_verification == 1


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


def _write_l1(root, data: bytes):
    from pathlib import Path

    root = Path(root)
    target = root / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return target


def _relative(local):
    from pathlib import Path

    # Mirror GDriveArchiver.repo_path_for relative to the sync root used in
    # contract skeletons (tmp_path itself): l1/<path-below-kis>.
    parts = Path(local).parts
    if "l1" in parts:
        idx = len(parts) - 1 - list(reversed(parts)).index("l1")
        rel = Path(*parts[idx + 1 :]).as_posix()
        return f"l1/{rel}"
    if "kis" in parts:
        idx = list(parts).index("kis")
        rel = Path(*parts[idx:]).as_posix()
        return f"l1/{rel}"
    return Path(local).name


def _write_manifests(root, names):
    from pathlib import Path

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    for name in names:
        (root / name).write_text("{}", encoding="utf-8")


class _FakeRcloneState:
    def __init__(self):
        from types import SimpleNamespace

        import json

        self.remote_sizes: dict[str, int] = {}
        self.upload_order: list[str] = []
        self.config = SimpleNamespace(
            remote_name="gdrive",
            remote_path="quant-lake/live/krx-alpha/data",
            runner=self._runner,
        )
        self._json = json

    def _runner(self, args, **kwargs):
        from types import SimpleNamespace

        # lsjson recursive listing: return current remote_sizes plus
        # non-file entries exercising the IsDir/prefix guards.
        if "lsjson" in args and "--recursive" in args:
            entries = [
                {"Path": path, "Size": size, "IsDir": False}
                for path, size in sorted(self.remote_sizes.items())
            ]
            entries.append({"Path": "l1", "IsDir": True})
            entries.append({"Path": "other/foreign.parquet", "Size": 9, "IsDir": False})
            return SimpleNamespace(returncode=0, stdout=self._json.dumps(entries), stderr="")
        # lsjson single object probe.
        if "lsjson" in args:
            dest = args[2] if len(args) > 2 else ""
            # dest is like gdrive:path/<repo_path>; match by suffix.
            matched = [(p, s) for p, s in self.remote_sizes.items() if dest.endswith(p)]
            if not matched:
                return SimpleNamespace(returncode=1, stdout="", stderr="not found")
            path, size = matched[0]
            return SimpleNamespace(
                returncode=0, stdout=self._json.dumps([{"Path": path, "Size": size, "IsDir": False}]), stderr=""
            )
        # copyto upload: record basename order, mirror local size.
        if "copyto" in args:
            src = args[2]
            dest = args[3] if len(args) > 3 else ""
            from pathlib import Path as _P

            # Derive repo-relative path by suffix matching local layout.
            local = _P(src)
            data = local.read_bytes()
            # Find repo path: search by filename under known roots is ambiguous,
            # so resolve via remote dest suffix after base prefix.
            prefix = "quant-lake/live/krx-alpha/data/"
            repo_path = dest.split(prefix, 1)[1] if prefix in dest else local.name
            self.remote_sizes[repo_path] = len(data)
            if repo_path.startswith("manifests/") or repo_path.startswith("manifest/"):
                self.upload_order.append(repo_path.split("/", 1)[1])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="unsupported")


import pytest


@pytest.fixture
def fake_rclone():
    return _FakeRcloneState()


def test_sync_l1_tree_skips_only_size_verified_object(tmp_path, fake_rclone):
    from src.storage.remote import GDriveArchiver

    local = _write_l1(tmp_path, b'1234')
    fake_rclone.remote_sizes = {_relative(local): 4}
    stats = GDriveArchiver(fake_rclone.config).sync_l1_tree(tmp_path)
    assert stats.skipped_verified == 1
    assert stats.uploaded == 0


def test_sync_l1_tree_reuploads_size_mismatch_and_verifies(tmp_path, fake_rclone):
    from src.storage.remote import GDriveArchiver

    local = _write_l1(tmp_path, b'1234')
    fake_rclone.remote_sizes = {_relative(local): 3}
    stats = GDriveArchiver(fake_rclone.config).sync_l1_tree(tmp_path)
    assert stats.uploaded == 1
    assert stats.failed_verification == 0


def test_sync_manifest_tree_uploads_sorted_json_and_verifies_size(tmp_path, fake_rclone):
    from src.storage.remote import GDriveArchiver

    _write_manifests(tmp_path, ['b.json', 'a.json'])
    stats = GDriveArchiver(fake_rclone.config).sync_manifest_tree(tmp_path)
    assert stats.uploaded == 2
    assert stats.failed_verification == 0
    assert fake_rclone.upload_order == ['a.json', 'b.json']



def test_remote_file_sizes_raises_on_unparseable_size():
    import json
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import GDriveArchiver, RemoteArchiveError

    for bad_size in (None, 'not-an-int', -1):
        def _runner(args, _bad_size=bad_size, **kwargs):
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps([{'Path': 'l1/kis/bad.parquet', 'IsDir': False, 'Size': _bad_size}]),
                stderr='',
            )

        archiver = GDriveArchiver(remote_name='gdrive', remote_path='quant-lake/live/krx-alpha/data', runner=_runner)
        with pytest.raises(RemoteArchiveError, match='invalid size'):
            archiver.remote_file_sizes('l1/')


def test_sync_manifest_tree_skips_size_verified_object(tmp_path, fake_rclone):
    from src.storage.remote import GDriveArchiver

    _write_manifests(tmp_path, ['a.json'])
    fake_rclone.remote_sizes = {'manifests/a.json': 2}
    stats = GDriveArchiver(fake_rclone.config).sync_manifest_tree(tmp_path)

    assert stats.uploaded == 0
    assert stats.skipped_verified == 1
    assert stats.failed_verification == 0
    assert fake_rclone.upload_order == []


def test_sync_manifest_tree_raises_on_unverified_upload(tmp_path):
    import json
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import GDriveArchiver, RemoteArchiveError

    manifest = tmp_path / 'a.json'
    manifest.write_text('{}', encoding='utf-8')

    for mode, message in (('copy_failure', 'upload failed'), ('stale_size', 'verify failed')):
        def _runner(args, _mode=mode, **kwargs):
            if 'copyto' in args:
                if _mode == 'copy_failure':
                    return SimpleNamespace(returncode=1, stdout='', stderr='network down')
                return SimpleNamespace(returncode=0, stdout='', stderr='')
            return SimpleNamespace(
                returncode=0,
                stdout=json.dumps([{'Path': 'manifests/a.json', 'IsDir': False, 'Size': 1}]),
                stderr='',
            )

        archiver = GDriveArchiver(remote_name='gdrive', remote_path='quant-lake/live/krx-alpha/data', runner=_runner)
        with pytest.raises(RemoteArchiveError, match=message):
            archiver.sync_manifest_tree(tmp_path)


def test_l0_partition_for_l1_maps_journal_partitions() -> None:
    from src.storage.remote import l0_partition_for_l1

    assert (
        l0_partition_for_l1("l1/ls/krx/regular/H0STASP0/dt=2026-09-18.parquet")
        == "l0/ls/krx/regular/H0STASP0/dt=2026-09-18"
    )
    assert l0_partition_for_l1("l1/ls/H0STASP0/dt=2026-09-11.parquet") == "l0/ls/H0STASP0/dt=2026-09-11"


def test_l0_partition_for_l1_returns_none_for_snapshot_and_malformed() -> None:
    from src.storage.remote import l0_partition_for_l1

    assert l0_partition_for_l1("l1/snapshot/ranking/dt=2026-09-18.parquet") is None
    assert l0_partition_for_l1("bars/daily.parquet") is None
    assert l0_partition_for_l1("l1/x/foo.parquet") is None
    assert l0_partition_for_l1("l1/ls/H0STASP0/dt=2026-13-99.parquet") is None
    assert l0_partition_for_l1("l1/dt=2026-09-18.parquet") is None
    assert l0_partition_for_l1("l1//dt=2026-09-18.parquet") is None


def test_sync_l1_tree_lists_l1_prefix_once_with_fast_list(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    for i in range(3):
        part = tmp_path / "ls" / "H0STASP0" / f"dt=2026-09-{10 + i:02d}.parquet"
        part.parent.mkdir(parents=True, exist_ok=True)
        part.write_bytes(b"x" * (10 + i))
    calls: list[list[str]] = []
    remote_sizes: dict[str, int] = {}

    def _runner(args, **kwargs):
        calls.append(args)
        if "--recursive" in args:
            assert args[2].endswith("/l1")
            assert "--fast-list" in args
            return SimpleNamespace(returncode=0, stdout=json.dumps([]), stderr="")
        if args[1] == "copyto":
            import pathlib as _pl

            dest = args[3]
            repo_path = dest.split("quant-lake/live/krx-alpha/data/", 1)[1]
            remote_sizes[repo_path] = _pl.Path(args[2]).stat().st_size
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        dest = args[2]
        repo_path = dest.split("quant-lake/live/krx-alpha/data/", 1)[1]
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": repo_path, "Size": remote_sizes[repo_path], "IsDir": False}]),
            stderr="",
        )

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.sync_l1_tree(tmp_path)

    recursive = [c for c in calls if "--recursive" in c]
    singles = [c for c in calls if len(c) > 1 and c[1] == "lsjson" and "--recursive" not in c]
    assert len(recursive) == 1
    assert stats.uploaded == 3
    assert len(singles) == 3


def test_sync_manifest_tree_lists_manifests_prefix_once_sorted(tmp_path) -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    for name in ("b.json", "a.json"):
        (tmp_path / name).write_text("{}", encoding="utf-8")
    calls: list[list[str]] = []
    uploaded: list[str] = []

    def _runner(args, **kwargs):
        calls.append(args)
        if "--recursive" in args:
            assert args[2].endswith("/manifests")
            assert "--fast-list" in args
            return SimpleNamespace(returncode=0, stdout=json.dumps([]), stderr="")
        if args[1] == "copyto":
            uploaded.append(args[3].rsplit("/", 1)[-1])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        dest = args[2]
        repo_path = dest.split("quant-lake/live/krx-alpha/data/", 1)[1]
        import pathlib as _pl

        # single-object verify: find local size by name
        local = tmp_path / repo_path.split("/", 1)[1]
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": repo_path, "Size": _pl.Path(local).stat().st_size, "IsDir": False}]),
            stderr="",
        )

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.sync_manifest_tree(tmp_path)

    assert stats.uploaded == 2
    assert uploaded == ["a.json", "b.json"]
    assert sum(1 for c in calls if "--recursive" in c) == 1


def test_missing_remote_prefix_returns_empty_listing(tmp_path) -> None:
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        if "--recursive" in args:
            return SimpleNamespace(returncode=3, stdout="", stderr="directory not found")
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        import json

        dest = args[2]
        repo_path = dest.split("quant-lake/live/krx-alpha/data/", 1)[1]
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": repo_path, "Size": 5, "IsDir": False}]),
            stderr="",
        )

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    assert arc.remote_file_sizes("l1/") == {}

    root = tmp_path / "l1root"
    (root / "ls" / "H0STASP0").mkdir(parents=True)
    (root / "ls" / "H0STASP0" / "dt=2026-09-18.parquet").write_bytes(b"x" * 5)
    stats = arc.sync_l1_tree(root)
    assert stats.uploaded == 1


def test_sync_l1_tree_counts_failed_verification_without_relisting(tmp_path, caplog) -> None:
    import json
    import logging
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    part = tmp_path / "ls" / "H0STASP0"
    part.mkdir(parents=True)
    (part / "dt=2026-09-18.parquet").write_bytes(b"x" * 10)

    def _runner(args, **kwargs):
        if "--recursive" in args:
            return SimpleNamespace(returncode=0, stdout=json.dumps([]), stderr="")
        if args[1] == "copyto":
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0, stdout=json.dumps([{"Path": "x", "Size": 9, "IsDir": False}]), stderr=""
        )

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    with caplog.at_level(logging.CRITICAL):
        stats = arc.sync_l1_tree(tmp_path)

    assert stats.failed_verification == 1
    assert stats.uploaded == 0


def test_lsjson_recursive_reframes_prefix_relative_entries() -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        assert args[2].endswith("/l1")
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": "ls/H0STASP0/dt=2026-09-18.parquet", "Size": 5, "IsDir": False}]),
            stderr="",
        )

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    assert arc.remote_file_sizes("l1/") == {"l1/ls/H0STASP0/dt=2026-09-18.parquet": 5}


def test_lsjson_recursive_empty_prefix_lists_root() -> None:
    import json
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        assert args[2] == "gdrive:quant-lake/live/krx-alpha/data"
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps([{"Path": "l1/a.parquet", "Size": 1, "IsDir": False}]),
            stderr="",
        )

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    assert arc.remote_files("") == {"l1/a.parquet"}


def test_purge_superseded_l0_requires_absent_local(tmp_path) -> None:
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    journal = tmp_path / "l0"
    present_parent = journal / "ls" / "krx" / "regular" / "H0STASP1"
    present_parent.mkdir(parents=True)
    (present_parent / "dt=2026-09-18").mkdir(parents=True)
    verified = {
        "l1/ls/krx/regular/H0STASP0/dt=2026-09-18.parquet",
        "l1/ls/krx/regular/H0STASP1/dt=2026-09-18.parquet",
    }
    purged_targets: list[str] = []

    def _runner(args, **kwargs):
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout="[]", stderr="")
        assert args[1] == "purge"
        purged_targets.append(args[2])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.purge_superseded_l0(verified, journal)

    assert stats.purged == 1
    assert stats.skipped_local_present == 1
    assert len(purged_targets) == 1
    assert purged_targets[0].endswith("/l0/ls/krx/regular/H0STASP0/dt=2026-09-18")


def test_purge_superseded_l0_skips_absent_remote(tmp_path) -> None:
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        assert args[1] == "lsjson"
        return SimpleNamespace(returncode=3, stdout="", stderr="not found")

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.purge_superseded_l0({"l1/ls/H0STASP0/dt=2026-09-11.parquet"}, tmp_path / "l0")

    assert stats.skipped_absent == 1
    assert stats.purged == 0


def test_purge_superseded_l0_failures_never_raise(tmp_path, caplog) -> None:
    import logging
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout="[]", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    with caplog.at_level(logging.CRITICAL):
        stats = arc.purge_superseded_l0({"l1/ls/H0STASP0/dt=2026-09-11.parquet"}, tmp_path / "l0")

    assert stats.failed == 1
    assert "stage=l0_remote_purge" in caplog.text


def test_purge_superseded_l0_probe_failure_counts_failed(tmp_path) -> None:
    from types import SimpleNamespace

    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.purge_superseded_l0({"l1/ls/H0STASP0/dt=2026-09-11.parquet"}, tmp_path / "l0")

    assert stats.failed == 1


def test_purge_superseded_l0_never_leaves_l0_tree(tmp_path, monkeypatch) -> None:
    from types import SimpleNamespace

    import src.storage.remote as remote_mod
    from src.storage.remote import GDriveArchiver

    mutating: list[list[str]] = []
    real_mapper = remote_mod.l0_partition_for_l1

    def _runner(args, **kwargs):
        if args[1] in ("purge", "copyto"):
            mutating.append(args)
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout="[]", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        remote_mod,
        "l0_partition_for_l1",
        lambda p: "bars/daily" if p == "l1/x/y.parquet" else real_mapper(p),
    )
    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.purge_superseded_l0(
        {"l1/x/y.parquet", "l1/snapshot/ranking/dt=2026-09-18.parquet"}, tmp_path / "l0"
    )

    assert stats.purged == 0
    assert mutating == []

    def _runner2(args, **kwargs):
        if args[1] in ("purge",):
            mutating.append(args)
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout="[]", stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    arc2 = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner2)
    stats2 = arc2.purge_superseded_l0({"l1/ls/H0STASP0/dt=2026-09-11.parquet"}, tmp_path / "l0")
    assert stats2.purged == 1
    assert all("/l0/" in argv[2] for argv in mutating if argv[1] == "purge")


def test_purge_superseded_l0_runner_exception_counts_failed(tmp_path) -> None:
    from src.storage.remote import GDriveArchiver

    def _runner(args, **kwargs):
        raise OSError("spawn failed")

    arc = GDriveArchiver(remote_name="gdrive", remote_path="quant-lake/live/krx-alpha/data", runner=_runner)
    stats = arc.purge_superseded_l0({"l1/ls/H0STASP0/dt=2026-09-11.parquet"}, tmp_path / "l0")

    assert stats.failed == 1


def test_sync_l1_tree_calls_progress_per_file(tmp_path) -> None:
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    root = tmp_path / "l1" / "kis" / "H0STCNT0"
    root.mkdir(parents=True)
    (root / "dt=2026-09-01.parquet").write_bytes(b"a" * 50)
    (root / "dt=2026-09-02.parquet").write_bytes(b"b" * 60)

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="[]", stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="q", runner=_runner)
    progress_calls: list[None] = []

    stats = arc.sync_l1_tree(tmp_path / "l1", progress=lambda: progress_calls.append(None))

    assert stats.failed_verification == 2
    assert len(progress_calls) == 2


def test_purge_superseded_l0_calls_progress_per_dir(tmp_path) -> None:
    from types import SimpleNamespace

    from src.storage.remote import RcloneArchiver

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=3, stdout="", stderr="")

    arc = RcloneArchiver(remote_name="gdrive", remote_path="q", runner=_runner)
    progress_calls: list[None] = []

    stats = arc.purge_superseded_l0(
        {"l1/ls/H0STCNT0/dt=2026-09-01.parquet"},
        tmp_path / "l0",
        progress=lambda: progress_calls.append(None),
    )

    assert stats.skipped_absent == 1
    assert len(progress_calls) == 1


def test_every_rclone_call_reports_progress(tmp_path) -> None:
    import json
    import subprocess
    from types import SimpleNamespace

    import pytest

    from src.storage.remote import RcloneArchiver, RemoteArchiveError

    pq = tmp_path / "dt=2026-09-01.parquet"
    pq.write_bytes(b"x" * 100)
    calls: list[list[str]] = []

    def _runner(args, **kwargs):
        calls.append(args)
        if args[1] == "copyto":
            raise subprocess.TimeoutExpired(args, 600)
        return SimpleNamespace(returncode=0, stdout=json.dumps([{"Path": "x", "Size": 100, "IsDir": False}]), stderr="")

    progress: list[int] = []
    arc = RcloneArchiver(remote_name="gdrive", remote_path="p", runner=_runner)
    arc.bind_progress(lambda: progress.append(1))

    with pytest.raises(RemoteArchiveError):
        arc.upload_and_verify(pq, "l1/kis/H0STCNT0/dt=2026-09-01.parquet")
    arc.remote_files("l1/")

    # 타임아웃으로 끝난 호출을 포함해 rclone 호출마다 정확히 한 번씩 진행 신호가 나간다.
    assert len(progress) == len(calls) >= 2
