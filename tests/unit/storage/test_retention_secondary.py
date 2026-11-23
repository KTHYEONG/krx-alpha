"""Secondary-feed retention: patch-gated L0 deletion, normalize-time move, and local-only pruning."""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import os
import time

import pyarrow as pa
import pyarrow.parquet as pq

from src.core.config import SecondaryPatchSettings
from src.core.paths import DataPaths
from src.realtime.manifest import SessionManifest
from src.storage.layout import l1_repo_path
from src.storage.retention import prune_local_secondary, prune_old_journals

PART_DAY = dt.date(2026, 11, 1)
REF = dt.date(2026, 11, 23)
SETTINGS = SecondaryPatchSettings(vendors=("kis", "db"), margin_s=30.0, local_retain_days=14)
NS = 1_000_000_000
STREAM = "H0STCNT0"


def _manifest(path: pathlib.Path, symbols: list[str]) -> None:
    manifest = SessionManifest(session_date=PART_DAY, clock_offset_ns=0, started_at_ns=0)
    for symbol in symbols:
        manifest.record_ack(vendor="x", tr_id="T", symbol=symbol, rt_cd="0", accepted=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest.save(path)


def _setup(tmp_path: pathlib.Path, *, kis: list[str]) -> tuple[DataPaths, pathlib.Path, pathlib.Path]:
    paths = DataPaths(tmp_path / "data")
    _manifest(paths.ls_manifest_path(PART_DAY, 0), ["A", "B"])
    _manifest(paths.regular_kis_manifest_path(PART_DAY, 0, "krx"), kis)
    audit = paths.session_audit_path(PART_DAY, "ls")
    audit.parent.mkdir(parents=True, exist_ok=True)
    audit.write_text(
        json.dumps({"symbols": [{"symbol": "A", "intervals": [{"recv_prev_ns": 10 * NS, "recv_next_ns": 11 * NS}]}]}),
        encoding="utf-8",
    )
    part = paths.journal_root / "kis" / "krx" / "regular" / STREAM / f"dt={PART_DAY.isoformat()}"
    part.mkdir(parents=True)
    (part / "00.jsonl.zst").write_bytes(b"x")
    return paths, part, paths.archive_root / "kis" / "krx" / "regular" / STREAM / f"dt={PART_DAY.isoformat()}.parquet"


def _fake_normalizer(out_rows: list[tuple[str, int, str]]):
    def normalize(part: pathlib.Path, out: pathlib.Path) -> int:
        out.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "symbol": pa.array([s for s, _, _ in out_rows], pa.string()),
                "recv_wall_ns": pa.array([t for _, t, _ in out_rows], pa.int64()),
                "raw": pa.array([r for _, _, r in out_rows], pa.string()),
            }
        )
        pq.write_table(table, out)
        return len(out_rows)

    return normalize


ROWS = [("A", 10 * NS + 5, "in"), ("A", 50 * NS, "out"), ("B", 10 * NS, "other")]


def _prune(paths: DataPaths, normalizer, *, verified: set[str] | None = None, reuse: bool = False):
    return prune_old_journals(
        paths.journal_root,
        archive_root=paths.archive_root,
        retain_days=3,
        reference_date=REF,
        normalizer=normalizer,
        verified_remote_l1=verified,
        reuse_fresh_l1=reuse,
        data_paths=paths,
        secondary=SETTINGS,
    )


def _patch_rel(paths: DataPaths) -> str:
    return l1_repo_path(paths.secondary_patch_l1("kis", "krx", "regular", STREAM, PART_DAY).relative_to(paths.archive_root))


def test_secondary_full_l1_moved_to_local(tmp_path) -> None:
    paths, _, full = _setup(tmp_path, kis=["A", "B"])
    _prune(paths, _fake_normalizer(ROWS))
    assert not full.exists()
    local = paths.l1_local_root / full.relative_to(paths.archive_root)
    assert pq.read_metadata(local).num_rows == 3
    patch = paths.secondary_patch_l1("kis", "krx", "regular", STREAM, PART_DAY)
    assert pq.read_table(patch).column("raw").to_pylist() == ["in"]


def test_secondary_l0_deleted_after_patch_verified(tmp_path) -> None:
    paths, part, _ = _setup(tmp_path, kis=["A", "B"])
    _prune(paths, _fake_normalizer(ROWS), verified={_patch_rel(paths)})
    assert not part.exists()


def test_secondary_l0_kept_without_patch(tmp_path) -> None:
    paths, part, full = _setup(tmp_path, kis=["A", "B"])
    full_rel = l1_repo_path(full.relative_to(paths.archive_root))
    _prune(paths, _fake_normalizer(ROWS), verified={full_rel})
    assert part.exists()


def test_non_secondary_partition_keeps_full_upload_rule(tmp_path) -> None:
    paths, part, full = _setup(tmp_path, kis=["A", "D"])
    full_rel = l1_repo_path(full.relative_to(paths.archive_root))
    _prune(paths, _fake_normalizer(ROWS), verified={full_rel})
    assert full.exists()
    assert not part.exists()


def test_rerun_reuses_patch_without_renormalizing(tmp_path) -> None:
    paths, part, _ = _setup(tmp_path, kis=["A", "B"])
    _prune(paths, _fake_normalizer(ROWS))

    def must_not_run(_part: pathlib.Path, _out: pathlib.Path) -> int:
        raise AssertionError("normalizer invoked on a fresh patch")

    stats = _prune(paths, must_not_run, verified={_patch_rel(paths)}, reuse=True)
    assert stats.reused == 1
    assert not part.exists()


def test_local_secondary_pruned_after_retain_days(tmp_path) -> None:
    paths = DataPaths(tmp_path / "data")
    old = paths.l1_local_root / "kis" / "krx" / "regular" / STREAM / "dt=2026-11-08.parquet"
    recent = paths.l1_local_root / "kis" / "krx" / "regular" / STREAM / "dt=2026-11-10.parquet"
    remote_side = paths.archive_root / "kis" / "krx" / "regular" / STREAM / "dt=2026-11-08.parquet"
    for f in (old, recent, remote_side):
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"p")
    assert prune_local_secondary(paths, REF, SETTINGS) == 1
    assert not old.exists()
    assert recent.exists()
    assert remote_side.exists()


def test_stale_audit_forces_rederivation(tmp_path) -> None:
    paths, _, _ = _setup(tmp_path, kis=["A", "B"])
    _prune(paths, _fake_normalizer(ROWS))
    audit = paths.session_audit_path(PART_DAY, "ls")
    future = time.time() + 100
    os.utime(audit, (future, future))
    calls: list[pathlib.Path] = []
    normalizer = _fake_normalizer(ROWS)

    def counting(part: pathlib.Path, out: pathlib.Path) -> int:
        calls.append(out)
        return normalizer(part, out)

    _prune(paths, counting, verified={_patch_rel(paths)}, reuse=True)
    assert len(calls) == 1


def test_part_newer_than_patch_forces_renormalization(tmp_path) -> None:
    paths, part, _ = _setup(tmp_path, kis=["A", "B"])
    _prune(paths, _fake_normalizer(ROWS))
    newer = part / "01.jsonl.zst"
    newer.write_bytes(b"y")
    future = time.time() + 100
    os.utime(newer, (future, future))
    calls: list[pathlib.Path] = []
    normalizer = _fake_normalizer(ROWS)

    def counting(part_dir: pathlib.Path, out: pathlib.Path) -> int:
        calls.append(out)
        return normalizer(part_dir, out)

    _prune(paths, counting, verified={_patch_rel(paths)}, reuse=True)
    assert len(calls) == 1


def test_patch_failure_keeps_l0_and_full_l1(tmp_path, monkeypatch) -> None:
    paths, part, full = _setup(tmp_path, kis=["A", "B"])

    def boom(*_args: object, **_kwargs: object) -> int:
        raise OSError("disk full")

    monkeypatch.setattr("src.storage.retention.publish_secondary_l1", boom)
    stats = _prune(paths, _fake_normalizer(ROWS), verified={_patch_rel(paths)})
    assert stats.failed == 1
    assert part.exists()
    assert full.exists()


def test_patch_hold_keeps_l0_and_records_patch_rel(tmp_path) -> None:
    paths, part, _ = _setup(tmp_path, kis=["A", "B"])
    stats = prune_old_journals(
        paths.journal_root,
        archive_root=paths.archive_root,
        retain_days=3,
        reference_date=REF,
        normalizer=_fake_normalizer(ROWS),
        verified_remote_l1={_patch_rel(paths)},
        hold=lambda _part: "repair pending",
        data_paths=paths,
        secondary=SETTINGS,
    )
    assert part.exists()
    assert stats.held == {_patch_rel(paths)}


def test_corrupt_patch_forces_renormalization(tmp_path) -> None:
    paths, _, _ = _setup(tmp_path, kis=["A", "B"])
    _prune(paths, _fake_normalizer(ROWS))
    paths.secondary_patch_l1("kis", "krx", "regular", STREAM, PART_DAY).write_bytes(b"not parquet")
    calls: list[pathlib.Path] = []
    normalizer = _fake_normalizer(ROWS)

    def counting(part: pathlib.Path, out: pathlib.Path) -> int:
        calls.append(out)
        return normalizer(part, out)

    _prune(paths, counting, verified={_patch_rel(paths)}, reuse=True)
    assert len(calls) == 1
