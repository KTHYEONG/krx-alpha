"""Secondary-feed patch backup: eligibility, loss windows, and patch writer invariants."""

from __future__ import annotations

import datetime as dt
import json
import logging

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.core.config import SecondaryPatchSettings
from src.core.paths import DataPaths
from src.realtime.manifest import SessionManifest
from src.storage.secondary_patch import (
    is_secondary_partition,
    patch_windows,
    secondary_inputs,
    write_secondary_patch,
)

DAY = dt.date(2026, 11, 2)
NS = 1_000_000_000


def _settings(**overrides: object) -> SecondaryPatchSettings:
    return SecondaryPatchSettings(**{"vendors": ("kis", "db"), "margin_s": 30.0, "local_retain_days": 14, **overrides})


def _manifest(path, symbols: list[str], *, gaps: list[tuple[str, int, int]] = ()) -> None:
    manifest = SessionManifest(session_date=DAY, clock_offset_ns=0, started_at_ns=0)
    for symbol in symbols:
        manifest.record_ack(vendor="x", tr_id="T", symbol=symbol, rt_cd="0", accepted=True)
    for symbol, start, end in gaps:
        manifest.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="test")
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest.save(path)


def _audit(paths: DataPaths, intervals: dict[str, list[tuple[int, int]]]) -> None:
    payload = {
        "symbols": [
            {"symbol": symbol, "intervals": [{"recv_prev_ns": lo, "recv_next_ns": hi} for lo, hi in spans]}
            for symbol, spans in intervals.items()
        ]
    }
    path = paths.session_audit_path(DAY, "ls")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _setup_secondary(paths: DataPaths, ls: list[str], kis: list[str]) -> None:
    _manifest(paths.ls_manifest_path(DAY, 0), ls)
    _manifest(paths.regular_kis_manifest_path(DAY, 0, "krx"), kis)


def _write_l1(path, rows: list[tuple[str, int, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "symbol": pa.array([s for s, _, _ in rows], pa.string()),
            "recv_wall_ns": pa.array([t for _, t, _ in rows], pa.int64()),
            "raw": pa.array([r for _, _, r in rows], pa.string()),
        }
    )
    pq.write_table(table, path)


def test_subset_partition_is_secondary(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A", "B", "C"], kis=["A", "B"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is True


def test_kis_only_symbol_forces_full_upload(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A", "B"], kis=["A", "D"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False


def test_empty_secondary_acceptance_forces_full_upload(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=[])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False


def test_missing_audit_forces_full_upload(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    with caplog.at_level(logging.WARNING, logger="src.storage.secondary_patch"):
        assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False
    assert any("stage=secondary_patch status=FULL_UPLOAD" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(("venue", "session"), [("nxt", "regular"), ("krx", "krx_after")])
def test_nxt_or_extended_never_secondary(tmp_path, venue, session) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "kis", venue, session, _settings()) is False


def test_unconfigured_vendor_is_never_secondary(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "ls", "krx", "regular", _settings()) is False


def test_windows_merge_with_margin() -> None:
    audit = {"symbols": [{"symbol": "A", "intervals": [
        {"recv_prev_ns": 0, "recv_next_ns": 1 * NS},
        {"recv_prev_ns": 11 * NS, "recv_next_ns": 12 * NS},
    ]}]}
    assert patch_windows(audit, 30.0) == {"A": [(-30 * NS, 42 * NS)]}


def test_windows_skip_intervals_without_receive_stamps() -> None:
    audit = {"symbols": [{"symbol": "A", "intervals": [{"recv_prev_ns": None, "recv_next_ns": 5 * NS}]}]}
    assert patch_windows(audit, 30.0) == {}


def test_patch_keeps_only_window_rows(tmp_path) -> None:
    full = tmp_path / "full.parquet"
    rows = [("A", 100, "a0"), ("A", 500, "a1"), ("A", 1000, "a2"), ("B", 500, "b0"), ("C", 500, "c0")]
    _write_l1(full, rows)
    out = tmp_path / "patch" / "dt=2026-11-02.parquet"
    written = write_secondary_patch(full, out, {"A": [(400, 600)], "B": [(0, 10)]})
    patch = pq.read_table(out)
    assert written == 1
    assert patch.column("raw").to_pylist() == ["a1"]
    assert patch.schema.equals(pq.read_schema(full).remove_metadata(), check_metadata=False)


def test_global_windows_apply_to_every_symbol(tmp_path) -> None:
    full = tmp_path / "full.parquet"
    _write_l1(full, [("A", 100, "a0"), ("B", 100, "b0"), ("B", 900, "b1")])
    out = tmp_path / "patch.parquet"
    assert write_secondary_patch(full, out, {"*": [(50, 150)]}) == 2
    assert sorted(pq.read_table(out).column("raw").to_pylist()) == ["a0", "b0"]


def test_empty_patch_still_written(tmp_path) -> None:
    full = tmp_path / "full.parquet"
    _write_l1(full, [("A", 100, "a0")])
    out = tmp_path / "patch.parquet"
    assert write_secondary_patch(full, out, {}) == 0
    assert out.is_file()
    assert pq.read_metadata(out).num_rows == 0
    assert pq.read_schema(out).names == ["symbol", "recv_wall_ns", "raw"]


def test_malformed_manifest_forces_full_upload(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    _audit(paths, {})
    paths.regular_kis_manifest_path(DAY, 0, "krx").write_text("[]", encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="src.storage.secondary_patch"):
        assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False
    assert any("reason=input_unreadable" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("acks_body", ['{"symbol": "A"}', '[{"accepted": true}]'])
def test_manifest_acks_malformed_force_full_upload(tmp_path, acks_body) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    _audit(paths, {})
    paths.regular_kis_manifest_path(DAY, 0, "krx").write_text(
        f'{{"subscription_acks": {acks_body}}}', encoding="utf-8"
    )
    assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False


def test_audit_without_symbols_forces_full_upload(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    path = paths.session_audit_path(DAY, "ls")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")
    assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False


def test_missing_secondary_manifest_forces_full_upload(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _manifest(paths.ls_manifest_path(DAY, 0), ["A"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "kis", "krx", "regular", _settings()) is False


def test_unknown_vendor_layout_forces_full_upload(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _setup_secondary(paths, ls=["A"], kis=["A"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "zzz", "krx", "regular", _settings(vendors=("zzz",))) is False


def test_db_partition_uses_crosscheck_manifests(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _manifest(paths.ls_manifest_path(DAY, 0), ["A", "B"])
    _manifest(paths.db_crosscheck_manifest_path(DAY, 0), ["A"])
    _audit(paths, {})
    assert is_secondary_partition(paths, DAY, "db", "krx", "regular", _settings()) is True


def test_gap_windows_from_ls_manifests_carry_margin(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _manifest(paths.ls_manifest_path(DAY, 0), ["A"], gaps=[("A", 100 * NS, 110 * NS), ("", 500 * NS, 520 * NS)])
    _manifest(paths.regular_kis_manifest_path(DAY, 0, "krx"), ["A"])
    _audit(paths, {})
    inputs = secondary_inputs(paths, DAY, "kis", "krx", "regular", _settings())
    assert inputs is not None
    assert inputs.gap_windows == {"A": [(70 * NS, 140 * NS)], "*": [(470 * NS, 550 * NS)]}


def test_extra_windows_are_merged_per_symbol() -> None:
    audit = {"symbols": [{"symbol": "A", "intervals": [{"recv_prev_ns": 0, "recv_next_ns": 0}]}]}
    windows = patch_windows(audit, 0.0, {"A": [(-5, 3)], "B": [(7, 9)]})
    assert windows == {"A": [(-5, 3)], "B": [(7, 9)]}


def test_failed_write_leaves_no_partial_patch(tmp_path, monkeypatch) -> None:
    full = tmp_path / "full.parquet"
    _write_l1(full, [("A", 100, "a0")])
    out = tmp_path / "patch.parquet"

    def boom(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("writer failure")

    monkeypatch.setattr("src.storage.secondary_patch._kept_rows", boom)
    with pytest.raises(RuntimeError, match="writer failure"):
        write_secondary_patch(full, out, {"A": [(0, 200)]})
    assert not out.exists()
    assert not (tmp_path / "patch.parquet.tmp").exists()


def test_zero_row_source_yields_schema_bearing_patch(tmp_path) -> None:
    full = tmp_path / "full.parquet"
    _write_l1(full, [])
    out = tmp_path / "patch.parquet"
    assert write_secondary_patch(full, out, {"A": [(0, 10)]}) == 0
    assert pq.read_metadata(out).num_rows == 0
    assert pq.read_schema(out).names == ["symbol", "recv_wall_ns", "raw"]
