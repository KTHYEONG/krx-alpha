"""Canonical feed selection: pure per-minute rules, deterministic derived output, and vendor-partition immutability."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.core.paths import DataPaths
from src.storage import feed_selection as fs
from src.storage.feed_selection import SelectionSpec, build_feed_selection, select_minutes

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 11, 16)
MINUTES = [f"2026-11-16T09:{minute:02d}" for minute in range(10, 15)]
SPEC = SelectionSpec()


def _select(candidates, *, demoted=None, windows=None, spec=SPEC):
    return select_minutes(
        venue="krx",
        session="regular",
        kind="tick",
        primary_vendor="ls",
        candidates=candidates,
        demoted_from=demoted or {},
        active_windows=windows or {},
        spec=spec,
    )


def _minute_wall(minute: str) -> float:
    return dt.datetime.strptime(minute, "%Y-%m-%dT%H:%M").replace(tzinfo=KST).timestamp()


def test_selection_primary_when_healthy() -> None:
    counts = dict.fromkeys(MINUTES, 5)
    rows = _select({"ls": {("005930", m): 5 for m in MINUTES}, "kis": {("005930", m): 5 for m in MINUTES}})
    assert len(rows) == len(MINUTES)
    assert all(row.reason == "primary_ok" and row.selected_vendor == "ls" for row in rows)
    assert counts


def test_selection_secondary_on_primary_gap() -> None:
    kis = {("005930", m): 40 for m in MINUTES[:5]}
    ls = {("005930", m): 0 for m in MINUTES[:5]}
    ls.update({("005930", m): 5 for m in MINUTES[5:]})
    rows = _select({"ls": {k: v for k, v in ls.items() if v}, "kis": kis})
    gap = [row for row in rows if row.minute_kst in MINUTES[:5]]
    assert all(row.selected_vendor == "kis" and row.reason == "secondary_only" for row in gap)
    assert all(row.selected_vendor == "ls" for row in rows if row.minute_kst not in MINUTES[:5])


def test_demoted_prefers_secondary_after_instant() -> None:
    frames = {("005930", m): 10 for m in MINUTES}
    kis = {("005930", m): 10 for m in MINUTES}
    demoted_at = _minute_wall(MINUTES[2])
    rows = _select({"ls": frames, "kis": kis}, demoted={"005930": demoted_at})
    before = [row for row in rows if row.minute_kst < MINUTES[2]]
    after = [row for row in rows if row.minute_kst >= MINUTES[2]]
    assert all(row.selected_vendor == "ls" for row in before)
    assert all(row.selected_vendor == "kis" and row.reason == "demoted" for row in after)


def test_tie_and_small_advantage_keep_primary() -> None:
    rows = _select({"ls": {("005930", MINUTES[0]): 10}, "kis": {("005930", MINUTES[0]): 12}})
    assert rows[0].selected_vendor == "ls"
    assert rows[0].reason == "primary_ok"
    tie = _select({"ls": {("005930", MINUTES[0]): 10}, "kis": {("005930", MINUTES[0]): 10}})
    assert tie[0].selected_vendor == "ls"


def test_secondary_outside_active_window_is_not_eligible() -> None:
    kis = {("005930", m): 40 for m in MINUTES}
    window_start = _minute_wall(MINUTES[3])
    rows = _select(
        {"ls": {}, "kis": kis},
        windows={("kis", "005930"): [(window_start, window_start + 3600)]},
    )
    assert all(row.selected_vendor == "ls" for row in rows if row.minute_kst < MINUTES[3])
    assert all(row.selected_vendor == "kis" for row in rows if row.minute_kst >= MINUTES[3])


def test_no_better_source_when_primary_below_threshold() -> None:
    rows = _select({"ls": {("005930", MINUTES[0]): 0}, "kis": {}})
    assert rows == []
    rows = _select({"ls": {("005930", MINUTES[0]): 0}, "kis": {("005930", MINUTES[0]): 1}})
    assert rows[0].selected_vendor == "ls"
    assert rows[0].reason == "no_better_source"


def test_rows_sorted_and_frames_recorded() -> None:
    rows = _select({"ls": {("000660", MINUTES[1]): 2, ("005930", MINUTES[0]): 3}, "kis": {}})
    assert [(row.symbol, row.minute_kst) for row in rows] == sorted((row.symbol, row.minute_kst) for row in rows)
    assert {row.frames_primary for row in rows} == {2, 3}


def _ls_tick_raw(symbol: str, price: int) -> str:
    body = {
        "shcode": symbol, "price": str(price), "cvolume": "10", "volume": "100", "change": "0",
        "sign": "2", "drate": "0.0", "mdchecnt": "1", "mschecnt": "1",
    }
    return json.dumps({"header": {"tr_cd": "H0STCNT0", "tr_key": symbol}, "body": body})


def _kis_tick_raw(symbol: str, price: int) -> str:
    fields = [symbol, "093000", str(price), "2", "0", "0.0"] + ["0"] * 6 + ["10", "100", "0", "1", "1"]
    fields += ["0"] * (17 - len(fields))
    return "^".join(fields)


def _db_tick_raw(symbol: str, price: int) -> str:
    body = {
        "ShrnIscd": symbol, "StckPrpr": str(price), "CntgVol": "10", "AcmlVol": "100", "PrdyVrss": "0",
        "PrdyVrsssign": "2", "PrdyCtrt": "0.0", "SelnCntgcsnu": "1", "ShnuCntgcsnu": "1",
    }
    return json.dumps({"header": {"tr_cd": "S00", "tr_key": symbol}, "body": body})


def _write_l1(paths: DataPaths, vendor: str, venue: str, stream: str, minutes: list[str]) -> pathlib.Path:
    path = paths.archive_root / vendor / venue / "regular" / stream / f"dt={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    walls = [int(dt.datetime.fromisoformat(minute).replace(tzinfo=KST).timestamp() * 1e9) for minute in minutes]
    raw = {"kis": _kis_tick_raw, "db": _db_tick_raw}.get(vendor, _ls_tick_raw)("005930", 70000)
    table = pa.table({"raw": [raw] * len(minutes), "recv_wall_ns": pa.array(walls, pa.int64())})
    pq.write_table(table, path)
    return path


def _digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def paths(tmp_path) -> DataPaths:
    return DataPaths(tmp_path / "data")


def test_selection_no_secondary_writes_status_only(paths) -> None:
    _write_l1(paths, "ls", "krx", "H0STCNT0", ["2026-11-16T09:10"])
    summary = build_feed_selection(DAY, paths=paths, spec=SPEC)
    assert summary.status == "NO_SECONDARY"
    assert not fs.selection_partition_path(paths, DAY).exists()
    payload = json.loads(fs.selection_summary_path(paths, DAY).read_text(encoding="utf-8"))
    assert payload["status"] == "NO_SECONDARY"


def test_selection_builder_is_idempotent_and_never_touches_vendor_partitions(paths) -> None:
    ls_path = _write_l1(paths, "ls", "krx", "H0STCNT0", ["2026-11-16T09:10", "2026-11-16T09:11"])
    kis_path = _write_l1(paths, "kis", "krx", "H0STCNT0", ["2026-11-16T09:10"] * 4)
    before = {p: _digest(p) for p in (ls_path, kis_path)}
    first = build_feed_selection(DAY, paths=paths, spec=SPEC)
    out = fs.selection_partition_path(paths, DAY)
    first_bytes = out.read_bytes()
    second = build_feed_selection(DAY, paths=paths, spec=SPEC)
    assert out.read_bytes() == first_bytes
    assert first == second
    assert first.status == "OK"
    assert {p: _digest(p) for p in (ls_path, kis_path)} == before
    table = pq.read_table(out).to_pylist()
    by_minute = {row["minute_kst"]: row for row in table if row["kind"] == "tick"}
    assert by_minute["2026-11-16T09:10"]["selected_vendor"] == "kis"
    assert by_minute["2026-11-16T09:11"]["selected_vendor"] == "ls"


def _write_symbol_rows(paths: DataPaths, vendor: str, venue: str, stream: str, rows: list[tuple[str, str]]) -> None:
    path = paths.archive_root / vendor / venue / "regular" / stream / f"dt={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    walls = [int(dt.datetime.fromisoformat(minute).replace(tzinfo=KST).timestamp() * 1e9) for _, minute in rows]
    raws = [_kis_tick_raw(symbol, 70000) if vendor == "kis" else _ls_tick_raw(symbol, 70000) for symbol, _ in rows]
    pq.write_table(pa.table({"raw": raws, "recv_wall_ns": pa.array(walls, pa.int64())}), path)


def test_demotion_expands_via_group_plan(paths) -> None:
    symbols = ["005930", "000660", "035420"]
    minutes = ["2026-11-16T09:10", "2026-11-16T09:11", "2026-11-16T09:12"]
    _write_symbol_rows(paths, "ls", "krx", "H0STCNT0", [(s, m) for s in symbols for m in minutes])
    _write_symbol_rows(paths, "kis", "krx", "H0STCNT0", [(s, m) for s in symbols for m in minutes])
    plan_wall = _minute_wall(minutes[0]) - 60
    demote_wall = _minute_wall(minutes[1])
    log = [
        {"kind": "group_plan", "group": "ls:0", "symbols": symbols, "wall": plan_wall, "mode": "active"},
        {"kind": "TRANSITION", "group": "ls:0", "new": "DAY_DEMOTED", "old": "DEGRADED", "wall": demote_wall, "mode": "active"},
    ]
    log_path = paths.feed_control_log_path(DAY)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("\n".join(json.dumps(row) for row in log) + "\n", encoding="utf-8")

    build_feed_selection(DAY, paths=paths, spec=SPEC)
    table = pq.read_table(fs.selection_partition_path(paths, DAY)).to_pylist()
    tick = [row for row in table if row["kind"] == "tick"]
    assert {row["symbol"] for row in tick} == set(symbols)
    for row in tick:
        if row["minute_kst"] == minutes[0]:
            assert row["selected_vendor"] == "ls"
            assert row["reason"] == "primary_ok"
        else:
            assert row["selected_vendor"] == "kis"
            assert row["reason"] == "demoted", row


def test_controller_log_tolerates_bad_rows_and_admission_windows_gate_secondary(paths, caplog) -> None:
    minutes = ["2026-11-16T09:10", "2026-11-16T09:11"]
    _write_symbol_rows(paths, "ls", "krx", "H0STCNT0", [("005930", m) for m in minutes])
    _write_symbol_rows(paths, "kis", "krx", "H0STCNT0", [("005930", m) for m in minutes for _ in range(4)])
    admit_wall = _minute_wall(minutes[1])
    log_path = paths.feed_control_log_path(DAY)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        "not json",
        "[]",
        json.dumps({"kind": "TRANSITION", "group": "ls:0", "new": "DAY_DEMOTED", "wall": admit_wall}),
        json.dumps({"kind": "ADMIT", "feed": "kis:5", "mode": "active", "symbols": ["005930"], "wall": admit_wall}),
    ]
    log_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    with caplog.at_level("WARNING", logger="src.storage.feed_selection"):
        build_feed_selection(DAY, paths=paths, spec=SPEC)
    assert "reason=demotion_without_plan" in caplog.text
    tick = {row["minute_kst"]: row for row in pq.read_table(fs.selection_partition_path(paths, DAY)).to_pylist()
            if row["kind"] == "tick"}
    assert tick[minutes[0]]["selected_vendor"] == "ls"
    assert tick[minutes[1]]["selected_vendor"] == "kis"
    assert tick[minutes[1]]["reason"] == "primary_gap"


def test_unmapped_venue_is_not_selected(paths) -> None:
    _write_symbol_rows(paths, "ls", "krx", "H0STCNT0", [("005930", "2026-11-16T09:10")])
    _write_symbol_rows(paths, "kis", "krx", "H0STCNT0", [("005930", "2026-11-16T09:10")])
    krx_only = SelectionSpec(primary_by_venue=(("krx", "ls"),))
    summary = build_feed_selection(DAY, paths=paths, spec=krx_only)
    assert summary.status == "OK"
    assert {row["venue"] for row in pq.read_table(fs.selection_partition_path(paths, DAY)).to_pylist()} == {"krx"}


def test_selection_never_prefers_conflated_db_trades_over_lossless_source() -> None:
    minute = MINUTES[0]
    candidates = {"ls": {}, "kis": {("005930", minute): 3}, "db": {("005930", minute): 40}}
    rows = _select(candidates)
    assert [(row.selected_vendor, row.reason) for row in rows] == [("kis", "secondary_only")]


def test_conflated_db_trades_are_selected_only_when_no_lossless_source_has_frames() -> None:
    minute = MINUTES[0]
    rows = _select({"ls": {}, "db": {("005930", minute): 40}})
    assert [(row.selected_vendor, row.reason) for row in rows] == [("db", "secondary_only")]


def test_lossless_source_outside_its_window_does_not_block_conflated_db() -> None:
    minute = MINUTES[0]
    window = {("kis", "005930"): [(_minute_wall("2026-11-16T10:00"), _minute_wall("2026-11-16T11:00"))]}
    candidates = {"ls": {}, "kis": {("005930", minute): 3}, "db": {("005930", minute): 40}}
    rows = _select(candidates, windows=window)
    assert [row.selected_vendor for row in rows] == ["db"]


def test_conflated_vendor_rule_applies_to_trade_stream_only() -> None:
    minute = MINUTES[0]
    candidates = {"ls": {}, "kis": {("005930", minute): 3}, "db": {("005930", minute): 40}}
    rows = select_minutes(
        venue="krx",
        session="regular",
        kind="book",
        primary_vendor="ls",
        candidates=candidates,
        demoted_from={},
        active_windows={},
        spec=SPEC,
    )
    assert [row.selected_vendor for row in rows] == ["db"]


def test_selection_spec_declares_db_as_conflated_by_default() -> None:
    assert SPEC.conflated_trade_vendors == frozenset({"db"})


def test_summary_counts_conflated_selected_minutes_per_vendor(paths) -> None:
    only_db = ["2026-11-16T09:10", "2026-11-16T09:11"]
    shared = "2026-11-16T09:12"
    _write_l1(paths, "ls", "krx", "H0STCNT0", [shared] * 2)
    _write_l1(paths, "db", "krx", "H0STCNT0", [*only_db, shared] * 4)
    summary = build_feed_selection(DAY, paths=paths, spec=SPEC)
    assert summary.status == "OK"
    assert summary.vendors["db"]["conflated_selected_minutes"] == len(only_db)
    assert summary.vendors["ls"]["conflated_selected_minutes"] == 0
    payload = json.loads(fs.selection_summary_path(paths, DAY).read_text(encoding="utf-8"))
    assert payload["vendors"]["db"]["conflated_selected_minutes"] == len(only_db)
    tick = {row["minute_kst"]: row["selected_vendor"] for row in pq.read_table(fs.selection_partition_path(paths, DAY)).to_pylist() if row["kind"] == "tick"}
    assert tick == {only_db[0]: "db", only_db[1]: "db", shared: "ls"}


def _toss_tick_raw() -> str:
    return json.dumps({"type": "message", "topic": "trade:kr:005930", "data": {"price": "70000", "volume": "1"}})


def _write_unified_toss(paths: DataPaths, minutes: list[str]) -> None:
    path = paths.archive_root / "toss" / "unified" / "regular" / "H0STCNT0" / f"dt={DAY.isoformat()}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    walls = [int(dt.datetime.fromisoformat(minute).replace(tzinfo=KST).timestamp() * 1e9) for minute in minutes]
    pq.write_table(pa.table({"raw": [_toss_tick_raw()] * len(minutes), "recv_wall_ns": pa.array(walls, pa.int64())}), path)


def test_selection_spec_declares_toss_as_mixed_venue_by_default() -> None:
    assert SPEC.mixed_venue_vendors == frozenset({"toss"})


def test_mixed_venue_vendor_is_last_resort_on_trades() -> None:
    minute = MINUTES[0]
    window = {("toss", "005930"): [(_minute_wall("2026-11-16T09:00"), _minute_wall("2026-11-16T10:00"))]}
    only_toss = _select({"ls": {}, "toss": {("005930", minute): 9}}, windows=window)
    assert [(r.selected_vendor, r.mixed_venue) for r in only_toss] == [("toss", True)]
    with_kis = _select({"ls": {}, "kis": {("005930", minute): 4}, "toss": {("005930", minute): 9}}, windows=window)
    assert [(r.selected_vendor, r.mixed_venue) for r in with_kis] == [("kis", False)]
    with_conflated = _select({"ls": {}, "db": {("005930", minute): 4}, "toss": {("005930", minute): 9}}, windows=window)
    assert [r.selected_vendor for r in with_conflated] == ["db"]
    healthy = _select({"ls": {("005930", minute): 5}, "toss": {("005930", minute): 90}}, windows=window)
    assert [(r.selected_vendor, r.mixed_venue) for r in healthy] == [("ls", False)]


def test_mixed_venue_vendor_outside_its_window_is_never_selected() -> None:
    minute = MINUTES[0]
    rows = _select({"ls": {}, "toss": {("005930", minute): 9}}, windows={("toss", "005930"): []})
    assert [r.selected_vendor for r in rows] == ["ls"]


def test_mixed_venue_vendor_never_selected_for_quotes() -> None:
    minute = MINUTES[0]
    rows = select_minutes(
        venue="krx", session="regular", kind="book", primary_vendor="ls",
        candidates={"ls": {}, "toss": {("005930", minute): 9}},
        demoted_from={}, active_windows={}, spec=SPEC,
    )
    assert [(r.selected_vendor, r.mixed_venue) for r in rows] == [("ls", False)]


def test_summary_and_rows_count_mixed_selected_minutes_for_both_venues(paths) -> None:
    only_toss = ["2026-11-16T09:10", "2026-11-16T09:11"]
    _write_l1(paths, "ls", "krx", "H0STCNT0", ["2026-11-16T09:12"] * 2)
    _write_unified_toss(paths, [*only_toss, "2026-11-16T09:12"] * 4)
    summary = build_feed_selection(DAY, paths=paths, spec=SPEC)
    assert summary.vendors["toss"]["mixed_selected_minutes"] == len(only_toss)
    assert summary.vendors["ls"].get("mixed_selected_minutes", 0) == 0
    rows = [r for r in pq.read_table(fs.selection_partition_path(paths, DAY)).to_pylist() if r["kind"] == "tick"]
    assert {r["minute_kst"]: (r["selected_vendor"], r["mixed_venue"]) for r in rows} == {
        only_toss[0]: ("toss", True),
        only_toss[1]: ("toss", True),
        "2026-11-16T09:12": ("ls", False),
    }
