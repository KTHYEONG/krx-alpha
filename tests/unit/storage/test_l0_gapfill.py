from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from src.storage.l0_gapfill import GapfillSource, GapfillStatus, derive_gapfill, summarize_gapfill, write_gapfill


def _src(tape: pl.DataFrame, vendor: str = "kiwoom", provenance: str = "p") -> list[GapfillSource]:
    return [GapfillSource(vendor, provenance, tape)]


def _audit(*intervals: dict[str, object], symbol: str = "000001") -> dict[str, object]:
    return {"symbols": [{"symbol": symbol, "intervals": list(intervals)}]}


def _interval(missing: int, prev: str = "090056", nxt: str = "090151") -> dict[str, object]:
    return {"missing_trades": missing, "chetime_prev": prev, "chetime_next": nxt, "cum_prev": 10, "cum_next": 10 + missing + 1}


def _tape(*rows: tuple[str, int, int, int]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["symbol", "ts_hms", "price", "volume"], orient="row")


def test_boundary_seconds_are_excluded_and_recovery_is_partial() -> None:
    tape = _tape(
        ("000001", 90056, 100, 1),
        ("000001", 90057, 101, 2),
        ("000001", 90150, 102, 3),
        ("000001", 90151, 103, 4),
    )
    rows, outcomes = derive_gapfill(_audit(_interval(5)), _src(tape))
    assert rows["chetime"].to_list() == ["090057", "090150"]
    (outcome,) = outcomes
    assert (outcome.recovered, outcome.residual, outcome.status) == (2, 3, GapfillStatus.PARTIAL)
    assert set(rows["vendor"]) == {"kiwoom"}
    assert set(rows["provenance"]) == {"p"}


def test_full_none_and_over_statuses_and_symbol_isolation() -> None:
    tape = _tape(
        ("000001", 90100, 100, 1),
        ("000001", 90101, 100, 1),
        ("000002", 90100, 50, 1),
    )
    audit = {
        "symbols": [
            {"symbol": "000001", "intervals": [_interval(2), _interval(1, "091000", "091100")]},
            {"symbol": "000002", "intervals": [_interval(0)]},
            {"symbol": "000003", "intervals": [_interval(4)]},
        ]
    }
    rows, outcomes = derive_gapfill(audit, _src(tape))
    assert [o.status for o in outcomes] == [GapfillStatus.FULL, GapfillStatus.NONE, GapfillStatus.NONE]
    assert rows["symbol"].unique().to_list() == ["000001"]
    over_rows, over = derive_gapfill(_audit(_interval(1)), _src(tape))
    assert over[0].status is GapfillStatus.OVER
    assert over[0].residual == 0
    assert over_rows.height == 2


def test_tape_order_within_a_second_is_preserved_and_totals_add_up() -> None:
    tape = _tape(("000001", 90100, 3, 1), ("000001", 90100, 2, 1), ("000001", 90100, 1, 1))
    rows, outcomes = derive_gapfill(_audit(_interval(3)), _src(tape))
    assert rows["price"].to_list() == [3, 2, 1]
    assert summarize_gapfill(outcomes) == {"intervals": 1, "missing_trades": 3, "recovered": 3, "residual": 0}


def test_missing_tape_columns_raise_and_empty_result_keeps_schema() -> None:
    with pytest.raises(ValueError, match="tape lacks columns"):
        derive_gapfill(_audit(_interval(1)), _src(pl.DataFrame({"symbol": ["000001"]})))
    rows, outcomes = derive_gapfill(_audit(), _src(_tape(("000001", 90100, 1, 1))))
    assert rows.height == 0
    assert "interval_id" in rows.columns
    assert outcomes == ()


def test_write_gapfill_is_atomic_and_round_trips(tmp_path: Path) -> None:
    tape = _tape(("000001", 90100, 5, 7))
    rows, outcomes = derive_gapfill(_audit(_interval(2)), _src(tape))
    rows_path = tmp_path / "gapfill" / "ls" / "regular" / "d.parquet"
    report_path = tmp_path / "gapfill" / "ls" / "regular" / "d.report.json"
    write_gapfill(rows, outcomes, rows_path=rows_path, report_path=report_path)
    assert pl.read_parquet(rows_path).equals(rows)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["totals"]["residual"] == 1
    assert report["intervals"][0]["status"] == "partial"
    assert sorted(p.name for p in rows_path.parent.iterdir()) == ["d.parquet", "d.report.json"]


def test_cli_writes_outputs_under_data_root_and_reports_absent_audit(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    import argparse
    import datetime as dt

    from src.cli.gapfill_ticks import run
    from src.core.paths import DataPaths

    day = dt.date(2026, 10, 20)
    paths = DataPaths(tmp_path)
    tape_path = tmp_path / "tape.parquet"
    _tape(("000001", 90100, 5, 7)).write_parquet(tape_path)
    args = argparse.Namespace(
        day=day, vendor="ls", tape=tape_path, tape_vendor="kiwoom", kis_min_missing=0, data_root=tmp_path
    )
    assert run(args) == 2
    audit_path = paths.session_audit_path(day, "ls")
    audit_path.parent.mkdir(parents=True)
    audit_path.write_text(json.dumps(_audit(_interval(2))), encoding="utf-8")
    assert run(args) == 0
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    assert rows_path.exists()
    assert report_path.exists()
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["recovered"] == 1
    with pytest.raises(ValueError, match="vendor must be"):
        paths.gapfill_paths(day, "")


def test_first_source_with_trades_wins_per_interval() -> None:
    primary = _tape(("000001", 90100, 1, 1))
    fallback = _tape(("000001", 90100, 9, 9), ("000002", 90100, 5, 5))
    audit = {
        "symbols": [
            {"symbol": "000001", "intervals": [_interval(1)]},
            {"symbol": "000002", "intervals": [_interval(1)]},
        ]
    }
    sources = [GapfillSource("kiwoom", "pk", primary), GapfillSource("kis_rest", "pr", fallback)]
    rows, outcomes = derive_gapfill(audit, sources)
    assert rows.select("symbol", "vendor", "price").rows() == [("000001", "kiwoom", 1), ("000002", "kis_rest", 5)]
    assert [o.status for o in outcomes] == [GapfillStatus.FULL, GapfillStatus.FULL]


def test_cli_queries_kis_only_for_tape_less_intervals_and_records_saturation(tmp_path: Path) -> None:
    import argparse
    import datetime as dt

    from src.cli.gapfill_ticks import run
    from src.core.paths import DataPaths

    day = dt.date(2026, 10, 20)
    paths = DataPaths(tmp_path)
    audit_path = paths.session_audit_path(day, "ls")
    audit_path.parent.mkdir(parents=True)
    audit = {
        "symbols": [
            {"symbol": "000001", "intervals": [_interval(1)]},
            {"symbol": "000009", "intervals": [_interval(2), _interval(1, "091000", "091001")]},
        ]
    }
    audit_path.write_text(json.dumps(audit), encoding="utf-8")
    tape_path = tmp_path / "tape.parquet"
    _tape(("000001", 90100, 5, 7)).write_parquet(tape_path)
    calls: list[tuple[str, str, str]] = []

    def fetch(symbol, start, end):
        calls.append((symbol, start.strftime("%H%M%S"), end.strftime("%H%M%S")))
        return ({"hour": "090100", "price": 11, "volume": 3}, {"hour": "090101", "price": 12, "volume": 4}), ("090101",)

    args = argparse.Namespace(
        day=day, vendor="ls", tape=tape_path, tape_vendor="kiwoom", kis_min_missing=2, data_root=tmp_path
    )
    assert run(args, kis_fetcher=fetch) == 0
    assert calls == [("000009", "090056", "090151")]
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    rows = pl.read_parquet(rows_path)
    assert rows.group_by("vendor").len().sort("vendor").rows() == [("kis_rest", 2), ("kiwoom", 1)]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["notes"] == {"anchored_on_journal": False, "kis_saturated_seconds": {"000009": ["090101"]}}


def test_write_gapfill_cleans_temp_file_and_reraises_on_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    rows, outcomes = derive_gapfill(_audit(_interval(2)), _src(_tape(("000001", 90100, 5, 7))))

    def broken_replace(src: object, dst: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", broken_replace)
    with pytest.raises(OSError, match="disk full"):
        write_gapfill(rows, outcomes, rows_path=tmp_path / "d.parquet", report_path=tmp_path / "d.report.json")
    assert list(tmp_path.glob(".*.tmp")) == []


def test_received_frames_reconcile_the_boundary_seconds() -> None:
    tape = _tape(
        ("000001", 90056, 100, 1),
        ("000001", 90056, 100, 1),
        ("000001", 90057, 101, 2),
        ("000001", 90151, 103, 4),
        ("000001", 90151, 104, 5),
    )
    received = {("000001", 90056): [(10, 100, 1)], ("000001", 90151): [(14, 103, 4)]}
    rows, outcomes = derive_gapfill(_audit(_interval(3)), _src(tape), received)
    assert rows.select("chetime", "price", "volume").rows() == [("090056", 100, 1), ("090057", 101, 2), ("090151", 104, 5)]
    assert (outcomes[0].recovered, outcomes[0].status) == (3, GapfillStatus.FULL)


def test_touching_gaps_share_one_pool_in_time_order() -> None:
    tape = _tape(("000001", 90100, 1, 1), ("000001", 90100, 2, 1), ("000001", 90101, 3, 1), ("000001", 90101, 4, 1))
    first, second = _interval(1, "090059", "090100"), _interval(2, "090100", "090102")
    rows, outcomes = derive_gapfill(_audit(first, second), _src(tape), {})
    assert [o.recovered for o in outcomes] == [1, 3]
    assert [o.status for o in outcomes] == [GapfillStatus.FULL, GapfillStatus.OVER]
    assert rows["interval_id"].to_list() == [0, 1, 1, 1]
    assert rows.height == 4


def test_gaps_of_different_symbols_or_apart_do_not_share_a_pool() -> None:
    tape = _tape(("000001", 90100, 1, 1), ("000002", 90100, 2, 1), ("000001", 91000, 3, 1))
    audit = {
        "symbols": [
            {"symbol": "000001", "intervals": [_interval(1, "090059", "090101"), _interval(1, "090959", "091001")]},
            {"symbol": "000002", "intervals": [_interval(1, "090059", "090101")]},
        ]
    }
    rows, outcomes = derive_gapfill(audit, _src(tape), {})
    assert [o.status for o in outcomes] == [GapfillStatus.FULL] * 3
    assert rows.height == 3


def test_journal_root_anchors_the_cli_on_received_frames(tmp_path: Path) -> None:
    import argparse
    import datetime as dt

    import zstandard as zstd

    from src.cli.gapfill_ticks import run
    from src.core.paths import DataPaths

    day = dt.date(2026, 10, 20)
    paths = DataPaths(tmp_path)
    audit_path = paths.session_audit_path(day, "ls")
    audit_path.parent.mkdir(parents=True)
    audit_path.write_text(json.dumps(_audit(_interval(1, "090056", "090058"))), encoding="utf-8")
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True)

    def _record(chetime: str, cum: int, price: int, volume: int) -> dict[str, object]:
        body = {"shcode": "000001", "chetime": chetime, "mdchecnt": "0", "mschecnt": str(cum), "price": str(price), "cvolume": str(volume)}
        return {"raw": json.dumps({"header": {}, "body": body}), "recv_wall_ns": 1, "conn_id": "c", "conn_seq": cum, "symbol": "000001"}

    junk = {"raw": "{}", "recv_wall_ns": 1, "conn_id": "c", "conn_seq": 99, "symbol": "000001"}
    lines = [_record("090056", 10, 100, 1), _record("090058", 12, 103, 4), _record("090057", 9, 0, 0), junk]
    lines[2]["raw"] = json.dumps({"header": {}, "body": {"shcode": "000001", "chetime": "09:57"}})
    payload = ("".join(json.dumps(item) + "\n" for item in lines) + "not json\n").encode()
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress(payload))
    tape_path = tmp_path / "tape.parquet"
    _tape(("000001", 90056, 100, 1), ("000001", 90057, 101, 2), ("000001", 90058, 103, 4)).write_parquet(tape_path)
    args = argparse.Namespace(
        day=day, vendor="ls", tape=tape_path, tape_vendor="kiwoom", kis_min_missing=0, data_root=tmp_path,
        journal_root=tmp_path / "l0",
    )

    assert run(args) == 0

    rows_path, report_path = paths.gapfill_paths(day, "ls")
    assert pl.read_parquet(rows_path).select("chetime", "price").rows() == [("090057", 101)]
    assert json.loads(report_path.read_text(encoding="utf-8"))["notes"] == {"anchored_on_journal": True}


def test_iter_received_ticks_without_a_routable_partition_yields_nothing(tmp_path: Path) -> None:
    import datetime as dt

    from src.storage.l0_audit import iter_received_ticks

    assert list(iter_received_ticks(tmp_path, vendor="ls", venue="nowhere", session="regular", day=dt.date(2026, 10, 20))) == []


def test_short_pool_leaves_later_touching_gaps_without_rows() -> None:
    tape = _tape(("000001", 90100, 1, 1))
    rows, outcomes = derive_gapfill(_audit(_interval(3, "090059", "090100"), _interval(2, "090100", "090102")), _src(tape), {})
    assert [(o.recovered, o.status) for o in outcomes] == [(1, GapfillStatus.PARTIAL), (0, GapfillStatus.NONE)]
    assert rows["interval_id"].to_list() == [0]
