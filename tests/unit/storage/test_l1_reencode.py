"""L1 backlog re-encode invariants, run against an in-memory fake remote (no rclone)."""

from __future__ import annotations

import datetime as dt
import hashlib
import io
import json
import logging
import pathlib
import subprocess
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import src.storage.l1_reencode as l1_mod
from src.storage.l1_reencode import reencode_l1_file, run_backlog
from src.storage.parquet_options import L1_ENCODING_METADATA_KEY, l1_encoding_metadata
from src.storage.remote import GDriveArchiver, RemoteObject

_PREFIX = "l1/"
_TODAY = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()


class FakeRemote(GDriveArchiver):
    def __init__(self, files: dict[str, bytes]) -> None:
        super().__init__(remote_name="gdrive", remote_path="root")
        self.files = files
        self.uploads: list[str] = []
        self.downloads: list[str] = []

    def _run(self, cmd, *, timeout_s=None):
        if cmd[:2] == ["rclone", "copyto"] and cmd[2].startswith("gdrive:root/"):
            repo_path = cmd[2].removeprefix("gdrive:root/")
            self.downloads.append(repo_path)
            pathlib.Path(cmd[3]).write_bytes(self.files[repo_path])
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected rclone call {cmd}")

    def remote_objects(self, remote_prefix):
        return {
            path: RemoteObject(size=len(data), md5=hashlib.md5(data, usedforsecurity=False).hexdigest())
            for path, data in self.files.items()
            if path.startswith(remote_prefix)
        }

    def upload_and_verify(self, local_parquet, repo_path):
        data = pathlib.Path(local_parquet).read_bytes()
        self.files[repo_path] = data
        self.uploads.append(repo_path)
        return self.matches(self.remote_objects(repo_path)[repo_path], pathlib.Path(local_parquet))


def _table(rows: int = 600) -> pa.Table:
    return pa.table(
        {
            "seq": pa.array(range(rows), type=pa.int64()),
            "symbol": pa.array(["005930", "000660"] * (rows // 2), type=pa.string()),
            "raw": pa.array([f"r{i % 7}" for i in range(rows)], type=pa.string()),
        }
    ).replace_schema_metadata({b"dq": b"ok"})


def _write_v1(path: pathlib.Path, table: pa.Table | None = None) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = _table() if table is None else table
    pq.write_table(table, path, row_group_size=200)
    return path.read_bytes()


def _v1_bytes(tmp_path: pathlib.Path, name: str = "v1.parquet") -> bytes:
    return _write_v1(tmp_path / name)


def test_reencode_preserves_table_and_metadata(tmp_path) -> None:
    src = tmp_path / "dt=2026-01-05.parquet"
    _write_v1(src)
    dst = tmp_path / "out.parquet"

    result = reencode_l1_file(src, dst)

    assert result.status == "reencoded"
    assert result.bytes_after == dst.stat().st_size < result.bytes_before
    before = pq.ParquetFile(src)
    after = pq.ParquetFile(dst)
    assert after.read().equals(before.read())
    assert after.num_row_groups == before.num_row_groups
    meta = after.schema_arrow.metadata
    assert meta[b"dq"] == b"ok"
    assert meta[L1_ENCODING_METADATA_KEY.encode()] == b"v2"


def test_already_v2_skipped(tmp_path) -> None:
    src = tmp_path / "dt=2026-01-05.parquet"
    pq.write_table(_table().replace_schema_metadata(l1_encoding_metadata()), src)
    dst = tmp_path / "out.parquet"

    result = reencode_l1_file(src, dst)

    assert result.status == "already_v2"
    assert not dst.exists()


def test_verify_failure_leaves_sources(tmp_path, monkeypatch, caplog) -> None:
    class _CorruptingWriter(pq.ParquetWriter):
        def write_table(self, table, row_group_size=None):
            bumped = table.set_column(0, "seq", pc.add(table.column("seq"), pa.scalar(1)))
            super().write_table(bumped, row_group_size=row_group_size)

    monkeypatch.setattr(l1_mod.pq, "ParquetWriter", _CorruptingWriter)
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({path: v1})
    l1_root = tmp_path / "local"
    local = l1_root / path[len(_PREFIX) :]
    local.parent.mkdir(parents=True)
    local.write_bytes(v1)

    with caplog.at_level(logging.CRITICAL, logger=l1_mod.logger.name):
        results = run_backlog(
            archiver,
            l1_root=l1_root,
            work_dir=tmp_path / "work",
            deadline_monotonic=1e18,
            ledger_path=tmp_path / "done.jsonl",
        )

    assert [r.status for r in results] == ["verify_failed"]
    assert local.read_bytes() == v1
    assert archiver.files[path] == v1
    assert archiver.uploads == []
    assert any(rec.levelno == logging.CRITICAL for rec in caplog.records)


def test_remote_only_object_downloaded_reencoded_and_uploaded(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({path: v1})
    l1_root = tmp_path / "local"

    results = run_backlog(
        archiver,
        l1_root=l1_root,
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.status for r in results] == ["reencoded"]
    assert archiver.uploads == [path]
    assert (
        pq.ParquetFile(io.BytesIO(archiver.files[path])).schema_arrow.metadata[L1_ENCODING_METADATA_KEY.encode()]
        == b"v2"
    )
    assert not (l1_root / path[len(_PREFIX) :]).exists()
    assert not any((tmp_path / "work").rglob("*.parquet"))


def test_already_v2_remote_is_not_uploaded(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v2_file = tmp_path / "v2.parquet"
    pq.write_table(
        _table().replace_schema_metadata(
            {**{b"dq": b"ok"}, **{k.encode(): v.encode() for k, v in l1_encoding_metadata().items()}}
        ),
        v2_file,
    )
    archiver = FakeRemote({path: v2_file.read_bytes()})

    results = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.status for r in results] == ["already_v2"]
    assert archiver.uploads == []

    # Then: the v2 object is ledgered, so the next run neither downloads nor uploads it
    archiver.downloads.clear()
    again = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )
    assert [r.status for r in again] == ["already_v2"]
    assert archiver.downloads == []


def test_deadline_stops_between_files(tmp_path, monkeypatch) -> None:
    first = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    second = "l1/ls/krx/regular/H0STASP0/dt=2026-01-06.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({first: v1, second: v1})
    clock = iter([0.0, 10.0, 10.0])
    monkeypatch.setattr(l1_mod.time, "monotonic", lambda: next(clock))

    results = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=5.0,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.path for r in results] == [first]
    assert archiver.uploads == [first]
    assert archiver.files[second] == v1
    assert (
        pq.ParquetFile(io.BytesIO(archiver.files[first])).schema_arrow.metadata[L1_ENCODING_METADATA_KEY.encode()]
        == b"v2"
    )


def test_recent_partitions_excluded(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_JOURNAL_RETAIN_DAYS", "3")
    recent = (_TODAY - dt.timedelta(days=1)).isoformat()
    old = "2026-01-05"
    v1 = _v1_bytes(tmp_path / "remote")
    files = {
        f"l1/ls/krx/regular/H0STASP0/dt={recent}.parquet": v1,
        f"l1/ls/krx/regular/H0STASP0/dt={old}.parquet": v1,
        "l1/snapshot/kis/dt=2026-01-05.parquet": v1,
        "l1/derived/feed_selection/dt=2026-01-05.parquet": v1,
        "l1/derived/ticks_repaired/ls/regular/dt=2026-01-05.parquet": v1,
        "l1/ls/krx/regular/H0STASP0/part.parquet": v1,
    }
    archiver = FakeRemote(files)

    results = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert sorted(r.path for r in results) == [
        "l1/derived/ticks_repaired/ls/regular/dt=2026-01-05.parquet",
        f"l1/ls/krx/regular/H0STASP0/dt={old}.parquet",
    ]


def test_row_group_count_mismatch_fails_verification(tmp_path) -> None:
    src = tmp_path / "src.parquet"
    other = tmp_path / "other.parquet"
    pq.write_table(_table(), src, row_group_size=200)
    pq.write_table(_table(), other, row_group_size=300)

    assert l1_mod._row_groups_equal(src, other) is False


def test_local_copy_reencoded_in_place_and_uploaded(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({path: v1})
    l1_root = tmp_path / "local"
    local = l1_root / path[len(_PREFIX) :]
    local.parent.mkdir(parents=True)
    local.write_bytes(v1)

    results = run_backlog(
        archiver,
        l1_root=l1_root,
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.status for r in results] == ["reencoded"]
    assert archiver.uploads == [path]
    assert pq.ParquetFile(local).schema_arrow.metadata[L1_ENCODING_METADATA_KEY.encode()] == b"v2"
    assert not local.with_name(local.name + ".v2.tmp").exists()


def test_download_failure_reports_error_without_upload(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    archiver = FakeRemote({path: _v1_bytes(tmp_path / "remote")})
    archiver._run = lambda cmd, *, timeout_s=None: subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")

    results = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.status for r in results] == ["error"]
    assert archiver.uploads == []


@pytest.mark.parametrize("mode", ["raise", "unverified"])
def test_upload_failure_reports_error(tmp_path, monkeypatch, mode) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({path: v1})
    l1_root = tmp_path / "local"
    local = l1_root / path[len(_PREFIX) :]
    local.parent.mkdir(parents=True)
    local.write_bytes(v1)

    def _upload(local_parquet, repo_path):
        if mode == "raise":
            raise l1_mod.RemoteArchiveError("upload failed")
        return False

    monkeypatch.setattr(archiver, "upload_and_verify", _upload)

    results = run_backlog(
        archiver,
        l1_root=l1_root,
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.status for r in results] == ["error"]


def test_corrupt_remote_object_reports_error(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    archiver = FakeRemote({path: b"not a parquet file"})

    results = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert [r.status for r in results] == ["error"]
    assert archiver.uploads == []


def test_duplicate_remote_name_is_skipped(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    archiver = FakeRemote({path: _v1_bytes(tmp_path / "remote")})
    archiver.remote_objects = lambda prefix: {path: RemoteObject(size=1, md5=None, duplicate=True)}

    results = run_backlog(
        archiver,
        l1_root=tmp_path / "local",
        work_dir=tmp_path / "work",
        deadline_monotonic=1e18,
        ledger_path=tmp_path / "done.jsonl",
    )

    assert results == []
    assert archiver.uploads == []


def test_ledger_skips_verified_objects_on_resume(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    archiver = FakeRemote({path: _v1_bytes(tmp_path / "remote")})
    ledger = tmp_path / "done.jsonl"
    l1_root = tmp_path / "local"
    first = run_backlog(
        archiver, l1_root=l1_root, work_dir=tmp_path / "work", deadline_monotonic=1e18, ledger_path=ledger
    )
    assert [r.status for r in first] == ["reencoded"]

    archiver.downloads.clear()
    archiver.uploads.clear()
    second = run_backlog(
        archiver, l1_root=l1_root, work_dir=tmp_path / "work", deadline_monotonic=1e18, ledger_path=ledger
    )

    assert [r.status for r in second] == ["already_v2"]
    assert archiver.downloads == []
    assert archiver.uploads == []


def test_torn_ledger_line_is_ignored(tmp_path) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({path: v1})
    ledger = tmp_path / "done.jsonl"
    ledger.write_text('{"repo_path": "l1/other", "size": 1, "md5": "x"}\n{"repo_path": "l1/ls', encoding="utf-8")

    results = run_backlog(
        archiver, l1_root=tmp_path / "local", work_dir=tmp_path / "work", deadline_monotonic=1e18, ledger_path=ledger
    )

    assert [r.status for r in results] == ["reencoded"]
    lines = ledger.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[-1])["repo_path"] == path


def test_local_size_mismatch_is_left_untouched(tmp_path, caplog) -> None:
    path = "l1/ls/krx/regular/H0STASP0/dt=2026-01-05.parquet"
    v1 = _v1_bytes(tmp_path / "remote")
    archiver = FakeRemote({path: v1})
    l1_root = tmp_path / "local"
    local = l1_root / path[len(_PREFIX) :]
    local.parent.mkdir(parents=True)
    local.write_bytes(b"stale")

    with caplog.at_level(logging.WARNING, logger=l1_mod.logger.name):
        results = run_backlog(
            archiver,
            l1_root=l1_root,
            work_dir=tmp_path / "work",
            deadline_monotonic=1e18,
            ledger_path=tmp_path / "done.jsonl",
        )

    assert [r.status for r in results] == ["local_mismatch"]
    assert archiver.downloads == []
    assert archiver.uploads == []
    assert local.read_bytes() == b"stale"
    assert archiver.files[path] == v1
    assert any(rec.levelno == logging.WARNING and "local_mismatch" in rec.getMessage() for rec in caplog.records)
