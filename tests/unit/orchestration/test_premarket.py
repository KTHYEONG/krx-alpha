"""Invariant guard tests for src/orchestration/premarket.py (part4 pure helpers)."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from src.core.paths import DataPaths
from src.core.session_anchors import SessionAnchors, standard_session_anchors
from src.orchestration.premarket import (
    PREMARKET_POLL_S,
    premarket_collection_due,
    premarket_eod_report,
    premarket_pool_due,
    premarket_stream_cmd,
    premarket_wake_cap_s,
    premarket_window,
)

KST = ZoneInfo("Asia/Seoul")
D = dt.date(2026, 9, 16)
SAT = dt.date(2026, 9, 19)


def _shifted(day: dt.date) -> SessionAnchors:
    return SessionAnchors(
        date=day,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(21, 0),
        source="default",
    )


def _at(day: dt.date, hour: int, minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


def test_collection_window_is_half_open_with_lead() -> None:
    anchors = standard_session_anchors(D)
    assert premarket_window(anchors) == (dt.time(8, 0), dt.time(8, 50))
    assert premarket_collection_due(_at(D, 7, 57, 59), anchors=anchors, start_lead_s=120.0) is False
    assert premarket_collection_due(_at(D, 7, 58, 0), anchors=anchors, start_lead_s=120.0) is True
    assert premarket_collection_due(_at(D, 8, 49, 59), anchors=anchors, start_lead_s=120.0) is True
    assert premarket_collection_due(_at(D, 8, 50, 0), anchors=anchors, start_lead_s=120.0) is False


def test_weekend_is_never_due() -> None:
    anchors = standard_session_anchors(SAT)
    assert premarket_collection_due(_at(SAT, 8, 10), anchors=anchors, start_lead_s=120.0) is False
    assert premarket_pool_due(_at(SAT, 8, 10), anchors=anchors, settle_s=300.0) is False


def test_shifted_session_day_is_skipped() -> None:
    anchors = _shifted(D)
    assert premarket_collection_due(_at(D, 9, 30), anchors=anchors, start_lead_s=120.0) is False


def test_pool_becomes_due_after_close_plus_settle() -> None:
    anchors = standard_session_anchors(D)
    assert premarket_pool_due(_at(D, 20, 4, 59), anchors=anchors, settle_s=300.0) is False
    assert premarket_pool_due(_at(D, 20, 5, 0), anchors=anchors, settle_s=300.0) is True


def test_pool_never_due_across_midnight() -> None:
    anchors = standard_session_anchors(D)
    assert premarket_pool_due(_at(D, 23, 59), anchors=anchors, settle_s=5 * 3600.0) is False


def test_wake_cap_lands_on_collector_start() -> None:
    anchors = standard_session_anchors(D)
    assert premarket_wake_cap_s(_at(D, 7, 0), anchors=anchors, start_lead_s=120.0) == 3480.0
    assert premarket_wake_cap_s(_at(D, 8, 10), anchors=anchors, start_lead_s=120.0) == PREMARKET_POLL_S


def test_no_cap_outside_feature_windows() -> None:
    anchors = standard_session_anchors(D)
    assert premarket_wake_cap_s(_at(D, 12, 0), anchors=anchors, start_lead_s=120.0) is None
    assert premarket_wake_cap_s(_at(SAT, 8, 10), anchors=standard_session_anchors(SAT), start_lead_s=120.0) is None
    assert premarket_wake_cap_s(_at(D, 8, 10), anchors=_shifted(D), start_lead_s=120.0) is None


def test_command_carries_only_contract_flags(tmp_path) -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    paths = DataPaths(tmp_path / "data")
    shard = AftermarketShard(MarketVenue.NXT, 0, ("005930", "000660"), ("H0NXCNT0", "H0NXASP0"), "5", "fp5")
    cmd = premarket_stream_cmd(D, paths, shard=shard)
    assert "collect-premarket" in cmd
    for flag in (
        "--session-date",
        "--journal-root",
        "--manifest-path",
        "--candidates-path",
        "--credential-slot",
        "--credential-key-id",
        "--symbols",
    ):
        assert flag in cmd
    assert cmd[cmd.index("--manifest-path") + 1] == str(paths.premarket_manifest_path(D))
    assert cmd[cmd.index("--candidates-path") + 1] == str(paths.premarket_candidates(D))
    assert cmd[cmd.index("--credential-slot") + 1] == "5"
    assert cmd[cmd.index("--symbols") + 1] == "005930,000660"
    assert "--venue" not in cmd
    assert "--shard-index" not in cmd
    joined = " ".join(cmd)
    assert "fp5" in joined


def _write_manifest(path, *, day, session, writer_closed, planned, acks, expected_close=10**18) -> None:
    from src.realtime.manifest import SessionManifest

    manifest = SessionManifest(
        session_date=day,
        clock_offset_ns=0,
        started_at_ns=0,
        venue="nxt",
        session=session,
        expected_close_ns=expected_close,
        writer_closed_at_ns=writer_closed,
        planned_pairs=[{"symbol": s, "tr_id": t} for s, t in planned],
        shard_index=0,
        credential_key_id="fp5",
    )
    for symbol, tr_id, accepted in acks:
        manifest.record_ack(vendor="kis", tr_id=tr_id, symbol=symbol, rt_cd="0" if accepted else "1", accepted=accepted)
    manifest.save(path)


def test_eod_report_reflects_closure_and_coverage(tmp_path) -> None:
    planned = [("005930", "H0NXCNT0"), ("005930", "H0NXASP0")]
    closed_all = tmp_path / "closed_all.json"
    _write_manifest(
        closed_all,
        day=D,
        session="nxt_pre",
        writer_closed=10**18,
        planned=planned,
        acks=[("005930", "H0NXCNT0", True), ("005930", "H0NXASP0", True)],
    )
    assert premarket_eod_report(closed_all, date=D) == (True, 2, 2)

    closed_half = tmp_path / "closed_half.json"
    _write_manifest(
        closed_half,
        day=D,
        session="nxt_pre",
        writer_closed=10**18,
        planned=planned,
        acks=[("005930", "H0NXCNT0", True), ("005930", "H0NXASP0", False)],
    )
    report = premarket_eod_report(closed_half, date=D)
    assert (report.closed, report.accepted_pairs, report.planned_pairs) == (True, 1, 2)

    open_manifest = tmp_path / "open.json"
    _write_manifest(
        open_manifest,
        day=D,
        session="nxt_pre",
        writer_closed=None,
        planned=planned,
        acks=[("005930", "H0NXCNT0", True), ("005930", "H0NXASP0", True)],
    )
    assert premarket_eod_report(open_manifest, date=D).closed is False

    wrong_session = tmp_path / "wrong.json"
    _write_manifest(
        wrong_session,
        day=D,
        session="aftermarket",
        writer_closed=10**18,
        planned=planned,
        acks=[("005930", "H0NXCNT0", True), ("005930", "H0NXASP0", True)],
    )
    assert premarket_eod_report(wrong_session, date=D).closed is False

    assert premarket_eod_report(tmp_path / "missing.json", date=D) == (False, 0, 0)
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    assert premarket_eod_report(corrupt, date=D) == (False, 0, 0)
