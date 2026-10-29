"""Kiwoom tick-body mapping guard tests (part 3/4)."""

from __future__ import annotations

import json

from src.storage.l0_audit import _tick_body


def _raw(item="005930", values=None):
    return json.dumps(
        {"type": "0B", "item": item, "values": values or {"20": "112114", "10": "-916000", "15": "-1", "13": "74090", "290": "2"}},
        separators=(",", ":"),
        ensure_ascii=False,
    )


def test_kiwoom_tick_body_mapping() -> None:
    body = _tick_body("kiwoom", _raw())
    assert body == {"shcode": "005930", "chetime": "112114", "price": "916000", "cvolume": "1", "volume": "74090"}
    assert "mdchecnt" not in body
    assert "mschecnt" not in body


def test_kiwoom_tick_body_preserves_suffix_item() -> None:
    body = _tick_body("kiwoom", _raw(item="005930_NX"))
    assert body["shcode"] == "005930_NX"


def test_kiwoom_tick_body_malformed_is_empty() -> None:
    assert _tick_body("kiwoom", "not json") == {}
    assert _tick_body("kiwoom", json.dumps([1, 2])) == {}
    assert _tick_body("kiwoom", json.dumps({"item": "", "values": {}})) == {}
    assert _tick_body("kiwoom", json.dumps({"item": "005930", "values": "nope"})) == {}
    assert _tick_body("kiwoom", json.dumps({"item": "005930", "values": {"20": "112114", "10": "bad", "15": "1", "13": "9"}})) == {}
    assert _tick_body("kiwoom", json.dumps({"item": "005930", "values": {"20": "112114", "10": "1", "15": "bad", "13": "9"}})) == {}
    assert _tick_body("kiwoom", json.dumps({"item": "005930", "values": {"20": "", "10": "1", "15": "1", "13": "9"}})) == {}
    assert _tick_body("kiwoom", json.dumps({"item": "005930", "values": {"20": "112114", "10": "1", "15": "1", "13": ""}})) == {}


def test_kiwoom_tick_body_compact_raw_roundtrip() -> None:
    entry = {"type": "0B", "name": "삼성전자", "item": "005930", "values": {"20": "112114", "10": "1", "15": "1", "13": "9", "290": "2"}}
    raw = json.dumps(entry, separators=(",", ":"), ensure_ascii=False)
    assert _tick_body("kiwoom", raw)["shcode"] == "005930"


def test_kiwoom_journal_reports_symbols_without_counter(tmp_path) -> None:
    import datetime as dt

    import zstandard as zstd

    from src.core.config import AuditSettings
    from src.core.session_anchors import standard_session_anchors
    from src.storage.l0_audit import AuditStatus, derive_session_audit

    day = dt.date(2026, 10, 29)
    part = tmp_path / "kiwoom" / "krx" / "regular" / "H0STCNT0" / f"dt={day}"
    part.mkdir(parents=True)
    record = {"raw": _raw(), "recv_wall_ns": 1, "symbol": "005930", "conn_id": "c", "conn_seq": 1}
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress((json.dumps(record) + "\n").encode()))
    audit = derive_session_audit(
        tmp_path, vendor="kiwoom", venue="krx", session="regular", day=day,
        anchors=standard_session_anchors(day), settings=AuditSettings(latency_streams=("H0STCNT0",)),
    )
    assert audit.status is AuditStatus.OK
    assert audit.totals.symbols_without_counter == 1
    assert audit.symbols[0].status == "NO_COUNTER"
