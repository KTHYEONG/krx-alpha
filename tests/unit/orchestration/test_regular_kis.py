"""Unit guards for regular KIS collection window and child argv (part4)."""

from __future__ import annotations

import datetime as dt
import sys
from zoneinfo import ZoneInfo

import pytest

from src.core.session_anchors import standard_session_anchors
from src.orchestration.regular_kis import regular_kis_collection_due, regular_kis_stream_cmd
from src.realtime.contracts import MarketVenue
from src.realtime.kis_sharding import AftermarketShard

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 16)


def _at(day, hour, minute, second=0):
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


def test_collection_due_covers_lead_to_grace() -> None:
    anchors = standard_session_anchors(DAY)
    assert regular_kis_collection_due(_at(DAY, 8, 27, 59), anchors=anchors, start_lead_s=1920.0, close_grace_s=120.0) is False
    assert regular_kis_collection_due(_at(DAY, 8, 28, 0), anchors=anchors, start_lead_s=1920.0, close_grace_s=120.0) is True
    assert regular_kis_collection_due(_at(DAY, 12, 0, 0), anchors=anchors, start_lead_s=1920.0, close_grace_s=120.0) is True
    assert regular_kis_collection_due(_at(DAY, 15, 31, 59), anchors=anchors, start_lead_s=1920.0, close_grace_s=120.0) is True
    assert regular_kis_collection_due(_at(DAY, 15, 32, 0), anchors=anchors, start_lead_s=1920.0, close_grace_s=120.0) is False


def test_collection_due_rejects_weekends_but_allows_shifted_days() -> None:
    from src.core.session_anchors import AnchorSource, SessionAnchors

    anchors = standard_session_anchors(DAY)
    saturday = dt.date(2026, 9, 19)
    assert regular_kis_collection_due(_at(saturday, 10, 0), anchors=anchors, start_lead_s=1920.0, close_grace_s=120.0) is False
    shifted = SessionAnchors(
        date=DAY,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(21, 0),
        source=AnchorSource.VENDOR,
    )
    assert regular_kis_collection_due(_at(DAY, 12, 0), anchors=shifted, start_lead_s=1920.0, close_grace_s=120.0) is True


def test_stream_cmd_layout(tmp_path) -> None:
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path / "data")
    shard = AftermarketShard(MarketVenue.KRX, 3, ("005930",), ("H0STCNT0", "H0STASP0"), "2", "fp2")
    cmd = regular_kis_stream_cmd(DAY, paths, shard=shard)
    assert cmd[:3] == [sys.executable, "-m", "src.cli.main"]
    assert cmd[3] == "collect-regular-kis"
    assert "--session-date" in cmd
    assert DAY.isoformat() in cmd
    assert str(paths.candidates) in cmd
    assert str(paths.regular_kis_manifest_path(DAY, 3)) in cmd
    assert str(paths.journal_root) in cmd
    assert cmd[cmd.index("--shard-index") + 1] == "3"
    assert cmd[cmd.index("--credential-slot") + 1] == "2"
    assert cmd[cmd.index("--credential-key-id") + 1] == "fp2"
    assert cmd[cmd.index("--symbols") + 1] == "005930"
    assert cmd[cmd.index("--venue") + 1] == "krx"


def test_nxt_collection_due_window_and_shifted_day() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import SessionAnchors, AnchorSource, standard_session_anchors
    from src.orchestration.regular_kis import regular_nxt_collection_due

    kst = ZoneInfo("Asia/Seoul")
    anchors = standard_session_anchors(DAY)

    def due(hour: int, minute: int, second: int = 0, *, a=anchors) -> bool:
        now = dt.datetime(DAY.year, DAY.month, DAY.day, hour, minute, second, tzinfo=kst)
        return regular_nxt_collection_due(now, anchors=a, start_lead_s=120.0, close_grace_s=120.0)

    assert (due(8, 57, 59), due(8, 58, 0), due(15, 21, 59), due(15, 22, 0)) == (False, True, True, False)
    shifted = SessionAnchors(
        date=DAY, regular_open=dt.time(9, 5), closing_auction_start=dt.time(15, 25),
        regular_close=dt.time(15, 30), after_market_end=dt.time(18, 0), source=AnchorSource.DEFAULT,
    )
    assert due(9, 0, a=shifted) is False


def test_disjoint_sets_skip_excluded_in_order() -> None:
    from src.orchestration.regular_kis import disjoint_hot_symbol_sets

    result = disjoint_hot_symbol_sets(tuple("abcdefgh"), frozenset("ab"), (2, 3))
    assert result == (("c", "d"), ("e", "f", "g"))


def test_disjoint_sets_short_when_exhausted() -> None:
    from src.orchestration.regular_kis import disjoint_hot_symbol_sets

    result = disjoint_hot_symbol_sets(("x", "y", "z"), frozenset(), (2, 2))
    assert result == (("x", "y"), ("z",))


def test_disjoint_sets_reject_duplicates_or_negative() -> None:
    from src.core.errors import KrxAlphaError
    from src.orchestration.regular_kis import disjoint_hot_symbol_sets

    with pytest.raises(KrxAlphaError):
        disjoint_hot_symbol_sets(("a", "a"), frozenset(), (1,))
    with pytest.raises(KrxAlphaError):
        disjoint_hot_symbol_sets(("a", "b"), frozenset(), (-1,))


def test_prior_ls_frame_loads_reads_latest_closed_day_before_today(tmp_path) -> None:
    import datetime as dt

    from src.core.paths import DataPaths
    from src.orchestration.regular_kis import prior_ls_frame_loads
    from src.realtime.manifest import SessionManifest

    paths = DataPaths(tmp_path)
    today = dt.date(2026, 11, 24)

    def _write(day: dt.date, shard: int, counts: dict[str, dict[str, int]] | None) -> None:
        target = paths.manifest_path(day) if shard == 0 else paths.ls_manifest_path(day, shard)
        target.parent.mkdir(parents=True, exist_ok=True)
        SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0, frame_counts=counts).save(target)

    assert prior_ls_frame_loads(paths, today) is None
    _write(dt.date(2026, 11, 20), 0, {"000001": {"H0STCNT0": 1, "H0STASP0": 2}})
    _write(dt.date(2026, 11, 22), 0, None)  # counting off: no load evidence that day
    _write(dt.date(2026, 11, 23), 0, {"000001": {"H0STCNT0": 5, "H0STASP0": 7}, "000002": {"H0STCNT0": 1}})
    _write(dt.date(2026, 11, 23), 1, {"000003": {"H0STCNT0": 4}})
    # Today's (still open) manifest must never feed today's plan: point-in-time.
    _write(today, 0, {"000001": {"H0STCNT0": 999}})
    assert prior_ls_frame_loads(paths, today) == (dt.date(2026, 11, 23), {"000001": 12, "000002": 1, "000003": 4})
    # A day whose manifests track no counts is skipped; the walk back finds the earlier day.
    assert prior_ls_frame_loads(paths, dt.date(2026, 11, 23)) == (dt.date(2026, 11, 20), {"000001": 3})
    paths.manifest_path(dt.date(2026, 11, 23)).write_text("not-json", encoding="utf-8")
    assert prior_ls_frame_loads(paths, today) is None
    # Outside the lookback window nothing is used.
    assert prior_ls_frame_loads(paths, dt.date(2026, 12, 5)) is None


def test_child_and_daemon_blocks_agree_with_prior_loads(tmp_path) -> None:
    import json

    from src.core.config import CollectorSettings
    from src.orchestration.regular_kis import ls_shard_blocks

    symbols = [f"{i:06d}" for i in range(1, 201)]
    pool = tmp_path / "pool.json"
    pool.write_text(json.dumps({"candidates": [{"symbol": s, "rank": i} for i, s in enumerate(symbols)]}), encoding="utf-8")
    collector = CollectorSettings(vendor="ls", ls_active_sessions=2)
    # One dominant name (as on a theme day) moves its peers; linear loads would coincide with the snake fallback.
    loads = {s: (100_000 if i == 5 else 1000 - 4 * i) for i, s in enumerate(symbols)}
    blocks = ls_shard_blocks(pool, collector, loads=loads)
    assert blocks == ls_shard_blocks(pool, collector, loads=dict(loads))
    assert blocks != ls_shard_blocks(pool, collector)
    # The lowest-priority name overflows: shard 1 gives one symbol to the LS market-state pairs.
    assert sorted(blocks[0] + blocks[1]) == symbols[:199]
