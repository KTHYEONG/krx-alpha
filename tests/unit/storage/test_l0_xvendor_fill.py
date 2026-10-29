"""Cross-vendor gap fill keyed by cumulative volume (part 1/4) guard tests."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import pathlib

import polars as pl
import pytest
import zstandard as zstd
from pydantic import ValidationError

from src.core.config import RepairSettings
from src.storage.l0_gapfill import (
    GapfillSource,
    PeerTrade,
    derive_cross_vendor_fill,
    derive_gapfill,
    peer_trades_for_gaps,
)


def _interval(missing=3, **overrides):
    base = {
        "missing_trades": missing,
        "chetime_prev": "090000",
        "chetime_next": "090002",
        "cum_prev": 10,
        "cum_next": 14,
        "volume_deficit": 250,
        "acml_prev": 1000,
        "acml_next_before": 1250,
    }
    base.update(overrides)
    return base


def _audit(*intervals, symbol="000001"):
    return {"symbols": [{"symbol": symbol, "intervals": list(intervals)}]}


def test_audit_records_acml_bounds() -> None:
    import tempfile
    from pathlib import Path
    from zoneinfo import ZoneInfo

    from src.core.config import AuditSettings
    from src.core.session_anchors import standard_session_anchors
    from src.storage.l0_audit import _audit_to_jsonable, derive_session_audit

    day = dt.date(2026, 10, 15)
    kst = ZoneInfo("Asia/Seoul")

    def ns(h, m, s=0):
        return int(dt.datetime(day.year, day.month, day.day, h, m, s, tzinfo=kst).timestamp() * 1_000_000_000)

    def tick(md, ms, volume, cvolume, chetime, seq):
        body = {
            "shcode": "005930", "price": "70000", "cvolume": str(cvolume), "volume": str(volume),
            "chetime": chetime, "mdchecnt": str(md), "mschecnt": str(ms),
        }
        return {
            "raw": json.dumps({"body": body}), "recv_wall_ns": ns(9, 0, seq),
            "conn_id": "c", "conn_seq": seq,
        }

    root = Path(tempfile.mkdtemp())
    part = root / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True)
    recs = [tick(0, 10, 1000, 10, "090000", 0), tick(0, 14, 1300, 50, "090002", 2)]
    (part / "09.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress("".join(json.dumps(r) + "\n" for r in recs).encode())
    )
    audit = derive_session_audit(
        root, vendor="ls", venue="krx", session="regular", day=day,
        anchors=standard_session_anchors(day), settings=AuditSettings(),
    )
    doc = _audit_to_jsonable(audit)
    (interval,) = doc["symbols"][0]["intervals"]
    assert interval["acml_prev"] == 1000
    assert interval["acml_next_before"] == 1250


def test_legacy_audit_without_acml_is_tape_only() -> None:
    legacy = _audit({k: v for k, v in _interval().items() if not k.startswith("acml_")})
    tape = pl.DataFrame({"symbol": ["000001"], "ts_hms": [90001], "price": [100], "volume": [1]})
    rows, recovered, _ = derive_cross_vendor_fill(legacy, [("kis", {"000001": [PeerTrade(1100, 90001, 100, 1)]})])
    assert rows.height == 0
    assert recovered == {}
    first, _ = derive_gapfill(legacy, [GapfillSource("kiwoom", "p", tape)], None)
    second, _ = derive_gapfill(legacy, [GapfillSource("kiwoom", "p", tape)], None)
    assert first.equals(second)


def test_empty_volume_span_is_ignored() -> None:
    audit = _audit(_interval(missing=2, acml_prev=5, acml_next_before=5, volume_deficit=0))
    peers = [("kis", {"000001": [PeerTrade(5, 90001, 100, 1)]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}
    assert residual.get("xvendor_rejected_intervals") == 0


def test_exact_peer_fill_recovers_all_missing() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [
        PeerTrade(1100, 90001, 5000, 100), PeerTrade(1200, 90001, 5100, 100), PeerTrade(1250, 90002, 5200, 50),
    ]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 3
    assert set(rows["provenance"].to_list()) == {"gapfill:xvendor_kis"}
    assert rows["tape_seq"].to_list() == [1100, 1200, 1250]
    assert recovered == {0: 3}
    assert residual["symbols"][0]["intervals"][0]["missing_trades"] == 0
    assert residual["symbols"][0]["intervals"][0]["volume_deficit"] == 0


def test_volume_overflow_rejects_interval() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [PeerTrade(1100, 90001, 1, 200), PeerTrade(1200, 90001, 1, 200)]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}
    assert residual.get("xvendor_rejected_intervals") == 1


def test_count_overflow_rejects_interval_without_trim() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [
        PeerTrade(1100, 90001, 1, 10), PeerTrade(1150, 90001, 1, 10),
        PeerTrade(1200, 90001, 1, 10), PeerTrade(1250, 90002, 1, 10),
    ]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}
    assert residual.get("xvendor_rejected_intervals") == 1


def test_peer_precedence_and_dedupe() -> None:
    audit = _audit(_interval(missing=1, acml_next_before=1100, volume_deficit=100))
    peers = [
        ("kis", {"000001": [PeerTrade(1100, 90001, 100, 100)]}),
        ("kiwoom", {"000001": [PeerTrade(1100, 90001, 100, 100)]}),
    ]
    rows, recovered, _ = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 1
    assert recovered == {0: 1}
    assert rows["vendor"].to_list() == ["kis"]


def test_tape_does_not_duplicate_xvendor_rows() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [PeerTrade(1100, 90001, 100, 100), PeerTrade(1200, 90001, 101, 100)]})]
    xv_rows, xv_recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert xv_rows.height == 2
    received = {("000001", 90001): [(-1, 100, 100), (-1, 101, 100)]}
    tape = pl.DataFrame({
        "symbol": ["000001"] * 3, "ts_hms": [90001] * 3,
        "price": [100, 101, 102], "volume": [100, 100, 50],
    })
    rows, outcomes = derive_gapfill(
        residual, [GapfillSource("kiwoom", "gapfill:kiwoom_tape", tape)],
        received, interval_ids=[0], prior_recovered=xv_recovered,
    )
    (outcome,) = outcomes
    assert (outcome.recovered, outcome.recovered_xvendor, outcome.residual) == (3, 2, 0)
    assert outcome.missing_trades == 3
    assert rows.height == 1


def test_interval_ids_stable_across_residual() -> None:
    first = _interval(missing=1, acml_prev=1000, acml_next_before=1100, volume_deficit=100,
                      chetime_prev="090000", chetime_next="090001", cum_prev=10, cum_next=12)
    second = _interval(missing=2, acml_prev=2000, acml_next_before=2300, volume_deficit=300,
                       chetime_prev="090010", chetime_next="090011", cum_prev=20, cum_next=23)
    audit = {"symbols": [{"symbol": "000001", "intervals": [first, second]}]}
    peers = [("kis", {"000001": [PeerTrade(1100, 90000, 7, 100)]})]
    _, xv_recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert xv_recovered == {0: 1}
    tape = pl.DataFrame({
        "symbol": ["000001", "000001"], "ts_hms": [90010, 90010],
        "price": [9, 9], "volume": [150, 150],
    })
    rows, outcomes = derive_gapfill(
        residual, [GapfillSource("kiwoom", "gapfill:kiwoom_tape", tape)],
        {}, interval_ids=[1], prior_recovered=xv_recovered,
    )
    assert [o.interval_id for o in outcomes] == [1]
    assert rows["interval_id"].to_list() == [1, 1]


def test_missing_peer_partition_is_harmless(tmp_path) -> None:
    day = dt.date(2026, 10, 20)
    audit = _audit(_interval())
    assert peer_trades_for_gaps(audit, tmp_path, peer="kis", day=day) == {}
    rows, recovered, _ = derive_cross_vendor_fill(audit, [("kis", {})])
    assert rows.height == 0
    assert recovered == {}


def _kraw(symbol, chetime, price, volume, cvolume, cum=99):
    from src.storage import quality_kis as q

    fields = ["0"] * q.KIS_TICK_MIN_FIELDS
    fields[q.KIS_TICK_SYMBOL] = symbol
    fields[q.KIS_TICK_TIME] = chetime
    fields[q.KIS_TICK_PRICE] = price
    fields[q.KIS_TICK_VOLUME] = volume
    fields[q.KIS_TICK_CVOLUME] = cvolume
    fields[q.KIS_TICK_MDCHECNT] = "0"
    fields[q.KIS_TICK_MSCHECNT] = str(cum)
    return "^".join(fields)


def test_peer_trades_hostile_inputs(caplog, tmp_path: pathlib.Path) -> None:
    day = dt.date(2026, 10, 20)
    audit = {
        "symbols": [
            "not-a-mapping",
            {"symbol": 123, "intervals": []},
            {"symbol": "000001", "intervals": "not-a-sequence"},
            {"symbol": "000001", "intervals": ["not-a-mapping", {"missing_trades": "bad"},
                                               {"missing_trades": 0, "acml_prev": 1, "acml_next_before": 2},
                                               {"missing_trades": 1, "acml_prev": True, "acml_next_before": 2},
                                               {"missing_trades": 1, "acml_prev": 5, "acml_next_before": 5},
                                               {"missing_trades": 2, "acml_prev": 1000,
                                                "acml_next_before": 1200},
                                               {"missing_trades": 2, "acml_prev": 1100,
                                                "acml_next_before": 1300}]},
        ]
    }
    part = tmp_path / "kis" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True)
    lines = [
        "not json",
        json.dumps({"raw": "0^1", "recv_wall_ns": 1}),
        json.dumps({"raw": _kraw("", "090001", "100", "1100", "10"), "recv_wall_ns": 2}),
        json.dumps({"raw": _kraw("000002", "090001", "100", "1100", "10"), "recv_wall_ns": 3}),
        json.dumps({"raw": _kraw("000001", "090001", "bad", "1100", "10"), "recv_wall_ns": 4}),
        json.dumps({"raw": _kraw("000001", "250000", "100", "1100", "10"), "recv_wall_ns": 5}),
        json.dumps({"raw": _kraw("000001", "090001", "100", "9999", "10"), "recv_wall_ns": 6}),
        json.dumps({"raw": _kraw("000001", "090001", "100", "1100", "10"), "recv_wall_ns": 7}),
        json.dumps({"raw": _kraw("000001", "090001", "100", "1100", "10"), "recv_wall_ns": 8}),
        json.dumps({"raw": _kraw("000001", "090001", "200", "1100", "20"), "recv_wall_ns": 9}),
        json.dumps({"raw": _kraw("000001", "090001", "100", "1200", "10"), "recv_wall_ns": 10}),
    ]
    (part / "09.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress("".join(item + "\n" for item in lines).encode()))
    with caplog.at_level(logging.WARNING, logger="src.storage.l0_gapfill"):
        found = peer_trades_for_gaps(audit, tmp_path, peer="kis", day=day)
    assert [trade.acml for trade in found["000001"]] == [1100, 1200]
    assert found["000001"][0].qty == 10
    assert any(record.message.startswith("[DATA] stage=xvendor_fill") for record in caplog.records)


def test_peer_trades_glob_error_is_empty(tmp_path: pathlib.Path, monkeypatch) -> None:
    import pathlib as _pathlib

    day = dt.date(2026, 10, 20)
    part = tmp_path / "kis" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True)
    monkeypatch.setattr(_pathlib.Path, "glob", lambda self, pattern: (_ for _ in ()).throw(OSError("down")))
    assert peer_trades_for_gaps(_audit(_interval()), tmp_path, peer="kis", day=day) == {}


def test_cross_vendor_deficit_garbage_tolerated() -> None:
    audit = _audit(_interval(volume_deficit="bad"))
    peers = [("kis", {"000001": [PeerTrade(1100, 90001, 100, 100)]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 1
    assert recovered == {0: 1}
    assert residual["symbols"][0]["intervals"][0]["volume_deficit"] == 0


def test_interval_ids_length_mismatch_raises() -> None:
    tape = pl.DataFrame({"symbol": ["000001"], "ts_hms": [90001], "price": [100], "volume": [1]})
    with pytest.raises(ValueError, match="interval_ids length"):
        derive_gapfill(_audit(_interval()), [GapfillSource("kiwoom", "p", tape)], None, interval_ids=[0, 1])


def test_xvendor_peers_validated() -> None:
    assert RepairSettings().xvendor_peers == ("kis",)
    with pytest.raises(ValidationError):
        RepairSettings(xvendor_peers=("KIS",))
    with pytest.raises(ValidationError):
        RepairSettings(xvendor_peers=("kis", "kis"))


def test_repair_job_applies_xvendor_fill(tmp_path: pathlib.Path) -> None:
    from src.core.config import AuditSettings
    from src.core.paths import DataPaths
    from src.storage import l0_repair_job as job

    day = dt.date(2026, 10, 26)
    kst = dt.timezone(dt.timedelta(hours=9))
    paths = DataPaths(tmp_path)

    def write_ls(trades):
        part = paths.journal_root / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        records = []
        for seq, (chetime, cum, volume, cvolume) in enumerate(trades):
            body = {"shcode": "000001", "chetime": chetime, "mdchecnt": "0",
                    "mschecnt": str(cum), "price": "100",
                    "cvolume": str(cvolume), "volume": str(volume)}
            records.append({"raw": json.dumps({"body": body}), "conn_id": "c", "conn_seq": seq,
                            "recv_wall_ns": int(dt.datetime.combine(
                                day, dt.time(int(chetime[0:2]), int(chetime[2:4]), int(chetime[4:6])),
                                kst).timestamp() * 1e9)})
        (part / "09.jsonl.zst").write_bytes(
            zstd.ZstdCompressor().compress("".join(json.dumps(r) + "\n" for r in records).encode()))

    write_ls([("090000", 1, 1, 1), ("090002", 3, 3, 1)])
    kis_part = paths.journal_root / "kis" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    kis_part.mkdir(parents=True, exist_ok=True)
    (kis_part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress(
        (json.dumps({"raw": _kraw("000001", "090001", "100", "2", "1"),
                     "recv_wall_ns": 1, "conn_id": "k", "conn_seq": 0}) + "\n").encode()))
    audit_path = paths.session_audit_path(day, "ls")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps({
        "session_date": day.isoformat(), "vendor": "ls", "venue": "krx", "session": "regular",
        "status": "OK",
        "totals": {"confirmed_missing": 1, "trades_observed": 2, "trades_received": 2},
        "symbols": [{"symbol": "000001", "confirmed_missing": 1, "intervals": [{
            "missing_trades": 1, "chetime_prev": "090000", "chetime_next": "090002",
            "cum_prev": 1, "cum_next": 3, "volume_deficit": 1,
            "acml_prev": 1, "acml_next_before": 2}, {
            "missing_trades": 0, "chetime_prev": "090005", "chetime_next": "090006",
            "cum_prev": 4, "cum_next": 5}]}]}), encoding="utf-8")
    tape_path = paths.tick_tape_partition(day, "test")
    tape_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["000001", "000001"], "ts_hms": [90001, 90001],
                  "price": [100, 101], "volume": [1, 1]}).write_parquet(tape_path)
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    with paths.tick_tape_ledger.open("a") as handle:
        handle.write(json.dumps({"symbol": "000001", "date": day.isoformat(),
                                 "status": "complete"}) + "\n")
    now = dt.datetime(2026, 10, 27, 0, 10, tzinfo=kst)
    summary = job.run_repair(
        paths=paths, journal_root=paths.journal_root, audit_settings=AuditSettings(latency_streams=("H0STCNT0",)),
        repair_settings=RepairSettings(), retain_days=3, window_date=day,
        deadline=now.replace(hour=5, minute=30), now=lambda: now, run_id="xvendor-test")
    assert summary.failed == 0
    assert summary.gapfilled == 1
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["notes"]["xvendor_peers"] == ["kis"]
    assert report["notes"]["xvendor_recovered"] == 1
    assert report["notes"]["xvendor_rejected_intervals"] == 0
    assert report["totals"]["recovered"] == 1
    assert report["totals"]["residual"] == 0
    assert report["intervals"][0]["status"] == "full"
    rows = pl.read_parquet(rows_path)
    assert rows.height == 1
    assert rows["provenance"].to_list() == ["gapfill:xvendor_kis"]


def test_merge_accepts_xvendor_rows(tmp_path) -> None:
    from src.core.paths import DataPaths
    from src.storage.l0_repair_merge import build_repaired_ticks

    day = dt.date(2026, 10, 23)
    paths = DataPaths(tmp_path)
    part = paths.journal_root / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    records = []
    for seq, (chetime, cum) in enumerate([("090000", 10), ("090002", 14)]):
        body = {"shcode": "000001", "chetime": chetime, "mdchecnt": "0", "mschecnt": str(cum),
                "price": "100", "cvolume": "1", "volume": str(cum)}
        records.append({"raw": json.dumps({"body": body}), "conn_id": "c", "conn_seq": seq,
                        "recv_wall_ns": 1_000_000_000 + seq})
    (part / "09.jsonl.zst").write_bytes(
        zstd.ZstdCompressor().compress("".join(json.dumps(r) + "\n" for r in records).encode()))
    audit_path = paths.session_audit_path(day, "ls")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_doc = {
        "schema_version": 1, "session_date": day.isoformat(), "vendor": "ls", "venue": "krx",
        "session": "regular", "status": "OK", "reason": "ok", "generated_at_ns": 1,
        "sources": [], "latency": [],
        "symbols": [{"symbol": "000001", "status": "OK", "frames": 2, "trades_observed": 2,
                     "confirmed_missing": 3, "count_only_missing": 0, "net_missing": 3,
                     "volume_deficit": 0, "duplicates": 0, "regressions": 0, "resets": 0,
                     "decode_fail": 0, "intervals": [_interval()], "intervals_truncated": 0,
                     "manifest_frames": None}],
        "totals": {"symbols": 1, "symbols_with_loss": 1, "symbols_without_counter": 0,
                   "trades_observed": 2, "trades_received": 2, "confirmed_missing": 3,
                   "count_only_missing": 0, "net_missing_positive": 3, "volume_deficit": 0,
                   "loss_ratio": 0.0},
        "tail_silence_s": 0.0, "last_regular_tick_recv_ns": 1,
    }
    audit_path.write_text(json.dumps(audit_doc), encoding="utf-8")
    gap_rows = [
        {"symbol": "000001", "chetime": "090001", "price": 100, "volume": 100, "tape_seq": 1100,
         "interval_id": 0, "cum_prev": 10, "cum_next": 14, "vendor": "kis",
         "provenance": "gapfill:xvendor_kis"},
        {"symbol": "000001", "chetime": "090001", "price": 100, "volume": 100, "tape_seq": 1200,
         "interval_id": 0, "cum_prev": 10, "cum_next": 14, "vendor": "kiwoom",
         "provenance": "gapfill:kiwoom_tape"},
    ]
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(gap_rows, schema={
        "symbol": pl.String, "chetime": pl.String, "price": pl.Int64, "volume": pl.Int64,
        "tape_seq": pl.Int64, "interval_id": pl.Int64, "cum_prev": pl.Int64, "cum_next": pl.Int64,
        "vendor": pl.String, "provenance": pl.String,
    }).write_parquet(rows_path, compression="zstd")
    report_path.write_text(json.dumps({
        "totals": {"intervals": 1, "missing_trades": 3, "recovered": 2, "residual": 1},
        "notes": {"audit_sha256": hashlib.sha256(audit_path.read_bytes()).hexdigest()},
        "intervals": [{"interval_id": 0}],
    }), encoding="utf-8")
    summary = build_repaired_ticks(paths=paths, journal_root=paths.journal_root, day=day, vendor="ls")
    assert summary.status == "OK"
    assert summary.filled_rows == 2
    frame = pl.read_parquet(paths.repaired_ticks_path(day, "ls"))
    filled = frame.filter(pl.col("source") != "live").sort("cum")
    assert filled["source"].to_list() == ["gapfill:xvendor_kis", "gapfill:kiwoom_tape"]
    assert filled["cum"].to_list() == [11, 12]
