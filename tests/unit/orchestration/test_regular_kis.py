"""Unit guards for regular KIS collection window and child argv (part4)."""

from __future__ import annotations

import datetime as dt
import sys
from zoneinfo import ZoneInfo

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
    assert "--venue" not in cmd
