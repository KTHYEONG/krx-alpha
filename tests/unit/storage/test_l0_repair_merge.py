"""Part 3 repaired-ticks table: build, validation, paths, layout and job wiring."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib

import polars as pl
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from src.core.config import RepairSettings
from src.core.paths import DataPaths

DAY = dt.date(2026, 10, 23)
KST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime(2026, 10, 24, 0, 10, tzinfo=KST)
DEADLINE = NOW.replace(hour=5, minute=30)


def _write_journal(paths: DataPaths, day: dt.date, vendor: str, trades: list[tuple[str, str, int]]) -> None:
    part = paths.journal_root / vendor / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    records = []
    for seq, (symbol, chetime, cum) in enumerate(trades):
        body = {
            "shcode": symbol,
            "chetime": chetime,
            "mdchecnt": "0",
            "mschecnt": str(cum),
            "price": "100",
            "cvolume": "1",
            "volume": str(cum),
        }
        records.append(
            {
                "raw": json.dumps({"body": body}),
                "conn_id": "c",
                "conn_seq": seq,
                "recv_wall_ns": 1_000_000_000 + seq,
            }
        )
    (part / "09.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress("".join(json.dumps(r) + "\n" for r in records).encode())
    )


def _write_audit(
    paths: DataPaths,
    day: dt.date,
    vendor: str,
    *,
    observed: int,
    confirmed: int,
    symbols: tuple[dict, ...] = (),
    status: str = "OK",
) -> pathlib.Path:
    doc = {
        "schema_version": 1,
        "session_date": day.isoformat(),
        "vendor": vendor,
        "venue": "krx",
        "session": "regular",
        "status": status,
        "reason": "ok",
        "generated_at_ns": 1,
        "sources": [],
        "latency": [],
        "symbols": list(symbols),
        "totals": {
            "symbols": len(symbols),
            "symbols_with_loss": 0,
            "symbols_without_counter": 0,
            "trades_observed": observed,
            "trades_received": observed,
            "confirmed_missing": confirmed,
            "count_only_missing": 0,
            "net_missing_positive": 0,
            "volume_deficit": 0,
            "loss_ratio": 0.0,
        },
        "tail_silence_s": 0.0,
        "last_regular_tick_recv_ns": 1,
    }
    target = paths.session_audit_path(day, vendor)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(doc), encoding="utf-8")
    return target


def _write_gapfill(
    paths: DataPaths,
    day: dt.date,
    vendor: str,
    audit_path: pathlib.Path,
    rows: list[dict],
    *,
    residual: int,
    recovered: int,
) -> None:
    rows_path, report_path = paths.gapfill_paths(day, vendor)
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    schema = {
        "symbol": pl.String,
        "chetime": pl.String,
        "price": pl.Int64,
        "volume": pl.Int64,
        "tape_seq": pl.Int64,
        "interval_id": pl.Int64,
        "cum_prev": pl.Int64,
        "cum_next": pl.Int64,
        "vendor": pl.String,
        "provenance": pl.String,
    }
    frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    frame.write_parquet(rows_path, compression="zstd")
    report_path.write_text(
        json.dumps(
            {
                "totals": {
                    "intervals": 1,
                    "missing_trades": recovered + residual,
                    "recovered": recovered,
                    "residual": residual,
                },
                "notes": {"audit_sha256": hashlib.sha256(audit_path.read_bytes()).hexdigest()},
                "intervals": [{"interval_id": 0}],
            }
        ),
        encoding="utf-8",
    )


def _gap_symbol(confirmed: int) -> dict:
    return {"symbol": "000001", "confirmed_missing": confirmed, "intervals_truncated": 0, "intervals": []}


def test_paths_shape_and_vendor_guard(tmp_path) -> None:
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 23)
    assert paths.repaired_ticks_path(day, "ls") == (
        tmp_path / "l1" / "derived" / "ticks_repaired" / "ls" / "regular" / "dt=2026-10-23.parquet"
    )
    assert paths.repair_summary_path(day, "ls") == tmp_path / "manifest" / "repair" / "2026-10-23.ls.summary.json"
    with pytest.raises(ValueError, match="vendor"):
        paths.repaired_ticks_path(day, "")
    with pytest.raises(ValueError, match="vendor"):
        paths.repair_summary_path(day, "")


def test_live_rows_match_audit_trades(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090001", 2), ("000002", "090002", 7)])
    _write_audit(paths, DAY, "ls", observed=3, confirmed=0)
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    assert summary.live_rows == summary.audit_trades_observed == 3
    assert summary.filled_rows == 0
    assert summary.residual == 0
    frame = pl.read_parquet(paths.repaired_ticks_path(DAY, "ls"))
    assert frame.columns == ["symbol", "chetime", "price", "volume", "cum", "source", "interval_id", "recv_wall_ns"]
    assert frame["source"].to_list() == ["live"] * 3
    assert frame["recv_wall_ns"].to_list() == [1_000_000_000, 1_000_000_001, 1_000_000_002]
    assert frame["cum"].to_list() == [1, 2, 7]
    doc = json.loads(paths.repair_summary_path(DAY, "ls").read_text())
    assert doc["status"] == "OK"
    assert doc["live_rows"] == 3
    assert doc["audit_trades_observed"] == 3
    assert doc["gapfill_report_sha256"] is None
    assert doc["count_note"] is None


def test_corrupt_journal_lines_skipped(tmp_path) -> None:
    import zstandard as zstd

    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    part = paths.journal_root / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    body = {
        "shcode": "000001",
        "chetime": "090000",
        "mdchecnt": "0",
        "mschecnt": "1",
        "price": "100",
        "cvolume": "1",
        "volume": "1",
    }
    lines = [
        json.dumps({"raw": json.dumps({"body": body}), "conn_id": "c", "conn_seq": 0, "recv_wall_ns": 7}),
        "not json",
        json.dumps({"raw": json.dumps({"bogus": True}), "conn_id": "c", "conn_seq": 1, "recv_wall_ns": 8}),
    ]
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress("".join(l + "\n" for l in lines).encode()))
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    assert summary.live_rows == 1


def test_key_none_yields_no_ticks(tmp_path, monkeypatch) -> None:
    from src.storage import l0_repair_merge as merge

    monkeypatch.setattr(merge, "l0_partition_key", lambda *args, **kwargs: None)
    assert list(merge._iter_live_ticks(tmp_path, "ls", DAY)) == []


def test_audit_variants_raise(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    target = paths.session_audit_path(DAY, "ls")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{torn", encoding="utf-8")
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    target.write_text(json.dumps({"status": "OK"}), encoding="utf-8")
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    doc = _audit_doc()
    doc["totals"]["trades_observed"] = "x"
    target.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    doc = _audit_doc()
    doc["vendor"] = "kis"
    target.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    with pytest.raises(ValueError, match="batch_rows"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls", batch_rows=0)


def _audit_doc() -> dict:
    return {
        "schema_version": 1,
        "session_date": DAY.isoformat(),
        "vendor": "ls",
        "venue": "krx",
        "session": "regular",
        "status": "OK",
        "reason": "ok",
        "generated_at_ns": 1,
        "sources": [],
        "latency": [],
        "symbols": [],
        "totals": {
            "symbols": 0,
            "symbols_with_loss": 0,
            "symbols_without_counter": 0,
            "trades_observed": 1,
            "trades_received": 1,
            "confirmed_missing": 0,
            "count_only_missing": 0,
            "net_missing_positive": 0,
            "volume_deficit": 0,
            "loss_ratio": 0.0,
        },
        "tail_silence_s": 0.0,
        "last_regular_tick_recv_ns": 1,
    }


def test_mismatch_not_published(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090001", 2)])
    _write_audit(paths, DAY, "ls", observed=3, confirmed=0)
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "MISMATCH"
    assert not paths.repaired_ticks_path(DAY, "ls").exists()
    assert not paths.repair_summary_path(DAY, "ls").exists()


def test_filled_rows_carry_provenance(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 10), ("000001", "090002", 14)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=3, symbols=(_gap_symbol(3),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": i,
                "interval_id": 0,
                "cum_prev": 10,
                "cum_next": 14,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
            for i in range(3)
        ],
        residual=0,
        recovered=3,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    assert summary.filled_rows == 3
    frame = pl.read_parquet(paths.repaired_ticks_path(DAY, "ls"))
    filled = frame.filter(pl.col("source") != "live")
    assert filled["source"].to_list() == ["gapfill:kiwoom_tape"] * 3
    assert filled["cum"].to_list() == [11, 12, 13]
    assert filled["recv_wall_ns"].to_list() == [None, None, None]
    assert filled["interval_id"].to_list() == [0, 0, 0]


def test_filled_rows_skip_late_arriving_live_counter(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(
        paths, DAY, "ls", [("000001", "090000", 10), ("000001", "090002", 14), ("000001", "090003", 12)]
    )
    audit_path = _write_audit(paths, DAY, "ls", observed=3, confirmed=2, symbols=(_gap_symbol(2),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": i,
                "interval_id": 0,
                "cum_prev": 10,
                "cum_next": 14,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
            for i in range(2)
        ],
        residual=0,
        recovered=2,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    frame = pl.read_parquet(paths.repaired_ticks_path(DAY, "ls"))
    filled = frame.filter(pl.col("source") != "live")["cum"].to_list()
    assert filled == [11, 13]


def test_filled_cum_never_collides_with_live(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 10), ("000001", "090002", 14)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=3, symbols=(_gap_symbol(3),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": i,
                "interval_id": 0,
                "cum_prev": 10,
                "cum_next": 14,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
            for i in range(3)
        ],
        residual=0,
        recovered=3,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    frame = pl.read_parquet(paths.repaired_ticks_path(DAY, "ls"))
    live_cums = set(frame.filter(pl.col("source") == "live").filter(pl.col("symbol") == "000001")["cum"].to_list())
    filled_cums = frame.filter(pl.col("source") != "live").filter(pl.col("symbol") == "000001")["cum"].to_list()
    assert filled_cums
    assert len(set(filled_cums)) == len(filled_cums)
    assert all(10 < cum < 14 for cum in filled_cums)
    assert not (set(filled_cums) & live_cums)

    audit_path.write_text(
        json.dumps(
            {
                **json.loads(audit_path.read_text()),
                "totals": {
                    "symbols": 1,
                    "symbols_with_loss": 0,
                    "symbols_without_counter": 0,
                    "trades_observed": 2,
                    "trades_received": 2,
                    "confirmed_missing": 4,
                    "count_only_missing": 0,
                    "net_missing_positive": 0,
                    "volume_deficit": 0,
                    "loss_ratio": 0.0,
                },
            }
        ),
        encoding="utf-8",
    )
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": i,
                "interval_id": 0,
                "cum_prev": 9,
                "cum_next": 14,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
            for i in range(4)
        ],
        residual=0,
        recovered=4,
    )
    colliding = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert colliding.status == "MISMATCH"
    assert len(pl.read_parquet(paths.repaired_ticks_path(DAY, "ls"))) == 5
    assert (
        not paths.repair_summary_path(DAY, "ls").exists()
        or json.loads(paths.repair_summary_path(DAY, "ls").read_text())["audit_sha256"]
        != hashlib.sha256(audit_path.read_bytes()).hexdigest()
    )


def test_residual_recorded(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090003", 4)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=3, symbols=(_gap_symbol(3),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 4,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=2,
        recovered=1,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    assert summary.filled_rows == 1
    assert summary.residual == 2
    doc = json.loads(paths.repair_summary_path(DAY, "ls").read_text())
    assert doc["residual"] == 2
    assert doc["filled_rows"] == 1


def test_count_note_when_confirmed_differs(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090003", 4)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=5, symbols=(_gap_symbol(5),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 4,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=2,
        recovered=1,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    assert summary.count_note == "gapfill_counts_intervals"
    doc = json.loads(paths.repair_summary_path(DAY, "ls").read_text())
    assert doc["count_note"] == "gapfill_counts_intervals"


def test_clean_day_has_no_filled_rows(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "OK"
    assert summary.filled_rows == 0
    assert summary.residual == 0


def test_deterministic_output(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 10), ("000001", "090002", 14)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=3, symbols=(_gap_symbol(3),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": i,
                "interval_id": 0,
                "cum_prev": 10,
                "cum_next": 14,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
            for i in range(3)
        ],
        residual=0,
        recovered=3,
    )
    kwargs = {"paths": paths, "journal_root": paths.journal_root, "day": DAY, "vendor": "ls"}
    assert build_repaired_ticks(**kwargs).status == "OK"
    first = pl.read_parquet(paths.repaired_ticks_path(DAY, "ls")).to_dicts()
    assert build_repaired_ticks(**kwargs).status == "OK"
    assert pl.read_parquet(paths.repaired_ticks_path(DAY, "ls")).to_dicts() == first


def test_bounded_batches(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [(f"{i:06d}", f"09000{i}", i + 1) for i in range(5)])
    _write_audit(paths, DAY, "ls", observed=5, confirmed=0)
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls", batch_rows=2)
    assert summary.status == "OK"
    assert summary.live_rows == 5
    meta = pq.read_metadata(paths.repaired_ticks_path(DAY, "ls"))
    assert meta.num_rows == 5
    assert all(meta.row_group(i).num_rows <= 2 for i in range(meta.num_row_groups))


def test_stale_audit_invalidates_file(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks, repaired_ticks_valid

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    audit_path = _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    assert build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls").status == "OK"
    assert repaired_ticks_valid(paths, DAY, "ls") is True
    doc = json.loads(audit_path.read_text())
    doc["generated_at_ns"] = 2
    audit_path.write_text(json.dumps(doc), encoding="utf-8")
    assert repaired_ticks_valid(paths, DAY, "ls") is False


def test_unusable_inputs_raise(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0, status="UNAVAILABLE")
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    with pytest.raises(ValueError, match="vendor"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="")


def test_merged_subtree_not_mapped_to_l0() -> None:
    from src.storage.layout import l0_partition_for_l1

    assert l0_partition_for_l1("l1/derived/ticks_repaired/ls/regular/dt=2026-10-23.parquet") is None
    assert l0_partition_for_l1("l1/derived/ticks_repaired/kis/regular/dt=2026-10-23.parquet") is None
    assert l0_partition_for_l1("l1/snapshot/foo/dt=2026-10-23.parquet") is None
    assert (
        l0_partition_for_l1("l1/ls/krx/regular/H0STCNT0/dt=2026-10-23.parquet")
        == "l0/ls/krx/regular/H0STCNT0/dt=2026-10-23"
    )


def _write_ledger(paths: DataPaths, day: dt.date, symbol: str = "000001") -> None:
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    with paths.tick_tape_ledger.open("a") as handle:
        handle.write(json.dumps({"symbol": symbol, "date": day.isoformat(), "status": "complete"}) + "\n")


def _run(paths: DataPaths, **kwargs):
    from src.core.config import AuditSettings
    from src.storage import l0_repair_job as job

    args = {
        "paths": paths,
        "journal_root": paths.journal_root,
        "audit_settings": AuditSettings(latency_streams=("H0STCNT0",)),
        "repair_settings": RepairSettings(),
        "retain_days": 3,
        "window_date": DAY,
        "deadline": DEADLINE,
        "now": lambda: NOW,
        "run_id": "test",
    }
    return job.run_repair(**(args | kwargs))


def test_repaired_day_gets_merged(tmp_path) -> None:
    from src.marketdata.tick_tape_backfill import read_ledger_done
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    _write_ledger(paths, DAY)
    settings = RepairSettings(merge_required=True)
    assert (
        derive_day_repair(paths, DAY, "ls", settings=settings, tape_done=read_ledger_done(paths.tick_tape_ledger)).state
        is RepairState.MERGE_PENDING
    )
    summary = _run(paths, repair_settings=settings)
    assert summary.merged == 1
    assert summary.failed == 0
    assert paths.repaired_ticks_path(DAY, "ls").exists()
    assert paths.repair_summary_path(DAY, "ls").exists()
    assert (
        derive_day_repair(paths, DAY, "ls", settings=settings, tape_done=read_ledger_done(paths.tick_tape_ledger)).state
        is RepairState.REPAIRED
    )


def test_merge_failure_keeps_hold(tmp_path) -> None:
    import pathlib as _pathlib

    from src.marketdata.tick_tape_backfill import read_ledger_done
    from src.storage.l0_repair import RepairState, derive_day_repair, l0_hold_reason

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090001", 2), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    _write_ledger(paths, DAY)
    settings = RepairSettings(hold_enabled=True, merge_required=True)
    summary = _run(paths, repair_settings=settings)
    assert summary.failed == 1
    assert summary.merged == 0
    tape_done = read_ledger_done(paths.tick_tape_ledger)
    assert (
        derive_day_repair(paths, DAY, "ls", settings=settings, tape_done=tape_done).state is RepairState.MERGE_PENDING
    )
    assert (
        l0_hold_reason(
            _pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=2026-10-23"),
            DAY,
            paths=paths,
            settings=settings,
            tape_done=tape_done,
            cutoff=DAY + dt.timedelta(days=1),
        )
        == "ls:merge_pending"
    )


def test_merge_not_required_leaves_state(tmp_path) -> None:
    from src.marketdata.tick_tape_backfill import read_ledger_done
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    _write_ledger(paths, DAY)
    settings = RepairSettings()
    assert (
        derive_day_repair(paths, DAY, "ls", settings=settings, tape_done=read_ledger_done(paths.tick_tape_ledger)).state
        is RepairState.REPAIRED
    )


def _write_report(paths: DataPaths, day: dt.date, vendor: str, doc: dict) -> pathlib.Path:
    _, report_path = paths.gapfill_paths(day, vendor)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(doc), encoding="utf-8")
    return report_path


def test_report_variants_raise(tmp_path, monkeypatch) -> None:
    import os as _os

    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    digest = hashlib.sha256(audit_path.read_bytes()).hexdigest()
    good_totals = {"intervals": 1, "missing_trades": 1, "recovered": 1, "residual": 0}
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _, report_path = paths.gapfill_paths(DAY, "ls")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_bytes(b"{truncated")
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_report(paths, DAY, "ls", [])
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_report(paths, DAY, "ls", {"totals": {"residual": -1, "recovered": 0}, "intervals": [], "notes": {}})
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_report(paths, DAY, "ls", {"totals": dict(good_totals), "intervals": {}, "notes": {}})
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_report(paths, DAY, "ls", {"totals": dict(good_totals), "intervals": [], "notes": []})
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_report(
        paths,
        DAY,
        "ls",
        {"totals": dict(good_totals), "intervals": [], "notes": {"audit_sha256": "0" * 64}},
    )
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    _write_report(paths, DAY, "ls", {"totals": dict(good_totals), "intervals": [], "notes": {}})
    _os.utime(report_path, (audit_path.stat().st_mtime_ns // 1_000_000_000 - 60,) * 2)
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    real_stat = pathlib.Path.stat

    def _boom(self, *args, **kwargs):
        if self == report_path and "audit_sha256" not in json.loads(report_path.read_bytes()):
            raise OSError("stat lost")
        return real_stat(self, *args, **kwargs)

    _write_report(paths, DAY, "ls", {"totals": dict(good_totals), "intervals": [], "notes": {}})
    monkeypatch.setattr(pathlib.Path, "stat", _boom)
    with pytest.raises(ValueError, match="report"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert digest is not None


def test_gapfill_rows_variants(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    rows_path, _ = paths.gapfill_paths(DAY, "ls")
    _write_gapfill(paths, DAY, "ls", audit_path, [], residual=0, recovered=0)
    rows_path.write_bytes(b"not parquet")
    _write_report(
        paths,
        DAY,
        "ls",
        {
            "totals": {"intervals": 1, "missing_trades": 1, "recovered": 1, "residual": 0},
            "intervals": [],
            "notes": {"audit_sha256": hashlib.sha256(audit_path.read_bytes()).hexdigest()},
        },
    )
    with pytest.raises(ValueError, match="rows"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")


def test_gapfill_height_mismatch_not_published(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": i,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
            for i in range(2)
        ],
        residual=0,
        recovered=1,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "MISMATCH"
    assert not paths.repaired_ticks_path(DAY, "ls").exists()


def test_gapfill_unusable_cells_raise(tmp_path) -> None:
    import polars as pl

    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    rows_path, _ = paths.gapfill_paths(DAY, "ls")
    frame = pl.read_parquet(rows_path).with_columns(pl.lit(None, dtype=pl.Int64).alias("cum_prev"))
    frame.write_parquet(rows_path, compression="zstd")
    with pytest.raises(ValueError, match="rows"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert not paths.repaired_ticks_path(DAY, "ls").exists()


def test_gapfill_range_violation_not_published(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 13,
                "cum_next": 14,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert summary.status == "MISMATCH"
    assert not paths.repaired_ticks_path(DAY, "ls").exists()


def test_audit_stat_failure_raises(tmp_path, monkeypatch) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    target = _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    real_stat = pathlib.Path.stat

    def _boom(self, *args, **kwargs):
        if self == target:
            raise OSError("stat lost")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "stat", _boom)
    with pytest.raises(ValueError, match="audit"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")


def test_parquet_replace_failure_raises(tmp_path, monkeypatch) -> None:
    import os as _os

    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    monkeypatch.setattr(_os, "replace", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("replace lost")))
    with pytest.raises(OSError, match="replace"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")


def test_summary_write_failure_cleans_parquet(tmp_path, monkeypatch) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    real_write = pathlib.Path.write_text

    def _boom(self, *args, **kwargs):
        if ".summary.json" in self.name:
            raise OSError("summary lost")
        return real_write(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "write_text", _boom)
    with pytest.raises(OSError, match="summary"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert not paths.repaired_ticks_path(DAY, "ls").exists()


def test_repaired_ticks_valid_variants(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks, repaired_ticks_valid

    paths = DataPaths(tmp_path)
    assert repaired_ticks_valid(paths, DAY, "ls") is False
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    audit_path = _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    assert build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls").status == "OK"
    assert repaired_ticks_valid(paths, DAY, "ls") is True
    summary_path = paths.repair_summary_path(DAY, "ls")
    summary_path.write_text("{torn", encoding="utf-8")
    assert repaired_ticks_valid(paths, DAY, "ls") is False
    summary_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    assert repaired_ticks_valid(paths, DAY, "ls") is False
    summary_path.write_text(
        json.dumps({"status": "MISMATCH", "day": DAY.isoformat(), "vendor": "ls"}), encoding="utf-8"
    )
    assert repaired_ticks_valid(paths, DAY, "ls") is False
    summary_path.write_text(
        json.dumps({"status": "OK", "day": "2026-10-22", "vendor": "ls", "audit_sha256": "x"}), encoding="utf-8"
    )
    assert repaired_ticks_valid(paths, DAY, "ls") is False
    audit_path.unlink()
    assert repaired_ticks_valid(paths, DAY, "ls") is False


def test_prebuild_mismatch_counts_as_failure(tmp_path, monkeypatch, caplog) -> None:
    import logging as _logging

    from src.storage import l0_repair_job as job
    from src.storage.l0_repair_merge import RepairedTicksSummary

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    monkeypatch.setattr(
        job,
        "build_repaired_ticks",
        lambda **kwargs: RepairedTicksSummary(DAY, "ls", 1, 0, 0, 1, "x" * 64, "MISMATCH"),
    )
    with caplog.at_level(_logging.WARNING):
        summary = _run(paths)
    assert summary.status == "FAIL"
    assert summary.merged == 0
    assert summary.failed == 1
    assert "status=MISMATCH" in caplog.text


def test_gapfill_rows_missing_column_raises(tmp_path) -> None:
    import polars as pl

    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    rows_path, _ = paths.gapfill_paths(DAY, "ls")
    pl.read_parquet(rows_path).drop("provenance").write_parquet(rows_path, compression="zstd")
    with pytest.raises(ValueError, match="rows"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")


def test_filled_batches_flush(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=1, symbols=(_gap_symbol(1),))
    _write_gapfill(
        paths,
        DAY,
        "ls",
        audit_path,
        [
            {
                "symbol": "000001",
                "chetime": "090001",
                "price": 100,
                "volume": 1,
                "tape_seq": 0,
                "interval_id": 0,
                "cum_prev": 1,
                "cum_next": 3,
                "vendor": "kiwoom",
                "provenance": "gapfill:kiwoom_tape",
            }
        ],
        residual=0,
        recovered=1,
    )
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls", batch_rows=1)
    assert summary.status == "OK"
    assert summary.live_rows == 2
    assert summary.filled_rows == 1
    meta = pq.read_metadata(paths.repaired_ticks_path(DAY, "ls"))
    assert meta.num_rows == 3
    assert all(meta.row_group(i).num_rows <= 1 for i in range(meta.num_row_groups))


def test_prebuild_merges_transitioned_day(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090001", 2)])
    summary = _run(paths)
    assert summary.status == "OK"
    assert summary.merged == 1
    assert summary.failed == 0
    assert paths.repaired_ticks_path(DAY, "ls").exists()
    assert paths.repair_summary_path(DAY, "ls").exists()


def test_writer_close_failure_never_publishes(tmp_path, monkeypatch) -> None:
    from src.storage import l0_repair_merge as merge

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    original = pq.ParquetWriter.close

    def fail_close(self):
        original(self)
        raise OSError("footer failed")

    monkeypatch.setattr(pq.ParquetWriter, "close", fail_close)
    with pytest.raises(OSError, match="footer"):
        merge.build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert not paths.repaired_ticks_path(DAY, "ls").exists()
    assert not paths.repair_summary_path(DAY, "ls").exists()


def test_corrupt_output_and_invalid_counts_do_not_release_hold(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks, repaired_ticks_valid

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    summary = paths.repair_summary_path(DAY, "ls")
    original = summary.read_bytes()
    doc = json.loads(original)
    doc["live_rows"] = True
    summary.write_text(json.dumps(doc))
    assert not repaired_ticks_valid(paths, DAY, "ls")
    doc["live_rows"] = 1
    doc["filled_rows"] = 1
    summary.write_text(json.dumps(doc))
    assert not repaired_ticks_valid(paths, DAY, "ls")
    summary.write_bytes(original)
    paths.repaired_ticks_path(DAY, "ls").write_bytes(b"torn")
    assert not repaired_ticks_valid(paths, DAY, "ls")


def test_existing_clean_day_prebuilt_once_without_gate(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    assert _run(paths).merged == 1
    assert _run(paths).merged == 0


def test_kis_merge_is_rejected_before_decode(tmp_path) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    with pytest.raises(ValueError, match="vendor"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="kis")


def test_merge_exception_counts_as_failure_and_keeps_hold(tmp_path, monkeypatch) -> None:
    from src.storage import l0_repair_job as job
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    _write_audit(paths, DAY, "ls", observed=1, confirmed=0)

    def fail(**kwargs):
        raise OSError("merge failed")

    monkeypatch.setattr(job, "build_repaired_ticks", fail)
    settings = RepairSettings(merge_required=True)
    result = _run(paths, repair_settings=settings)
    assert result.failed == 1
    assert result.merged == 0
    assert derive_day_repair(paths, DAY, "ls", settings=settings, tape_done=set()).state is RepairState.MERGE_PENDING


@pytest.mark.parametrize("legacy", [False, True])
def test_real_audit_gap_is_merged_without_changing_audit(tmp_path, legacy) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 3)])
    assert _run(paths).audited == 1
    audit_path = paths.session_audit_path(DAY, "ls")
    actual = json.loads(audit_path.read_bytes())
    assert actual["totals"]["trades_observed"] == 3
    assert actual["totals"]["trades_received"] == 2
    assert actual["totals"]["confirmed_missing"] == 1
    if legacy:
        del actual["totals"]["trades_received"]
        audit_path.write_text(json.dumps(actual))
    original_audit = audit_path.read_bytes()
    _write_gapfill(paths, DAY, "ls", audit_path, [{
        "symbol": "000001", "chetime": "090001", "price": 100, "volume": 1, "tape_seq": 0,
        "interval_id": 0, "cum_prev": 1, "cum_next": 3, "vendor": "kiwoom", "provenance": "gapfill:kiwoom_tape",
    }], residual=0, recovered=1)
    _write_ledger(paths, DAY)
    settings = RepairSettings(merge_required=True)
    result = _run(paths, repair_settings=settings)
    assert result.failed == 0
    assert result.merged == 1
    assert audit_path.read_bytes() == original_audit
    frame = pl.read_parquet(paths.repaired_ticks_path(DAY, "ls"))
    assert frame.height == 3
    assert frame.filter(pl.col("source") == "live").height == 2
    summary = json.loads(paths.repair_summary_path(DAY, "ls").read_bytes())
    assert summary["audit_trades_observed"] == 3
    assert summary["audit_trades_received"] == summary["live_rows"] == 2
    assert summary["live_count_basis"] == ("legacy_frames_plus_duplicates" if legacy else "trades_received")
    assert derive_day_repair(paths, DAY, "ls", settings=settings, tape_done={("000001", DAY)}).state is RepairState.REPAIRED


def test_gapfill_input_is_read_in_bounded_batches(tmp_path, monkeypatch) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1), ("000001", "090002", 5)])
    audit_path = _write_audit(paths, DAY, "ls", observed=2, confirmed=3, symbols=(_gap_symbol(3),))
    _write_gapfill(paths, DAY, "ls", audit_path, [{
        "symbol": "000001", "chetime": "090001", "price": 100, "volume": 1, "tape_seq": i,
        "interval_id": 0, "cum_prev": 1, "cum_next": 5, "vendor": "kiwoom", "provenance": "gapfill:kiwoom_tape",
    } for i in range(3)], residual=0, recovered=3)
    sizes = []
    original = pq.ParquetFile.iter_batches

    def batches(self, *args, **kwargs):
        for batch in original(self, *args, **kwargs):
            sizes.append(batch.num_rows)
            yield batch

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", batches)
    result = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls", batch_rows=2)
    assert result.status == "OK"
    assert sizes == [2, 1]


@pytest.mark.parametrize("value", [None, True, -1, 1.5, "1"])
def test_invalid_received_count_never_falls_back(tmp_path, value) -> None:
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    audit = _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    doc = json.loads(audit.read_bytes())
    doc["totals"]["trades_received"] = value
    audit.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="trades_received"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    assert not paths.repaired_ticks_path(DAY, "ls").exists()


@pytest.mark.parametrize("mutation", [
    "missing_symbols", "invalid_symbol", "missing_counts", "decode_fail", "missing_sources", "invalid_source",
    "missing_rows", "truncated", "source_count_mismatch",
])
def test_ambiguous_legacy_audit_keeps_merge_pending(tmp_path, mutation) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair
    from src.storage.l0_repair_merge import build_repaired_ticks

    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [("000001", "090000", 1)])
    audit = _write_audit(paths, DAY, "ls", observed=1, confirmed=0)
    doc = json.loads(audit.read_bytes())
    del doc["totals"]["trades_received"]
    doc["symbols"] = [{"symbol": "000001", "confirmed_missing": 0, "frames": 1, "duplicates": 0, "decode_fail": 0}]
    doc["sources"] = [{"stream": "H0STCNT0", "rows": 1, "truncated": False}]
    if mutation == "missing_symbols":
        del doc["symbols"]
    elif mutation == "invalid_symbol":
        doc["symbols"] = [None]
    elif mutation == "missing_counts":
        del doc["symbols"][0]["duplicates"]
    elif mutation == "decode_fail":
        doc["symbols"][0]["decode_fail"] = 1
    elif mutation == "missing_sources":
        del doc["sources"]
    elif mutation == "invalid_source":
        doc["sources"] = [None]
    elif mutation == "missing_rows":
        del doc["sources"][0]["rows"]
    elif mutation == "truncated":
        doc["sources"][0]["truncated"] = True
    else:
        doc["sources"][0]["rows"] = 2
    audit.write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="legacy"):
        build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=DAY, vendor="ls")
    repair = derive_day_repair(paths, DAY, "ls", settings=RepairSettings(merge_required=True), tape_done=set())
    assert repair.state in (RepairState.MERGE_PENDING, RepairState.UNAUDITED)


def test_legacy_receipt_count_includes_duplicates_and_no_counter_symbols(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    _write_journal(paths, DAY, "ls", [
        ("000001", "090000", 1), ("000001", "090000", 1),
        ("000002", "090000", 1), ("000002", "090001", 1),
    ])
    assert _run(paths).merged == 1
    audit = paths.session_audit_path(DAY, "ls")
    doc = json.loads(audit.read_bytes())
    assert doc["totals"]["trades_observed"] == 1
    assert doc["totals"]["trades_received"] == 4
    assert doc["symbols"][1]["status"] == "NO_COUNTER"
    del doc["totals"]["trades_received"]
    doc["sources"].append({"stream": "H0STASP0", "rows": 100, "truncated": False})
    audit.write_text(json.dumps(doc))
    original = audit.read_bytes()
    assert _run(paths).merged == 1
    assert audit.read_bytes() == original
    summary = json.loads(paths.repair_summary_path(DAY, "ls").read_bytes())
    assert summary["live_rows"] == summary["audit_trades_received"] == 4
    assert summary["audit_trades_observed"] == 1
