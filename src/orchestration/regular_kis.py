"""KRX regular-session KIS co-primary shard helpers (pure, no daemon import)."""

from __future__ import annotations

import datetime as dt
import sys
from zoneinfo import ZoneInfo

from src.core.paths import DataPaths
from src.core.session_anchors import SessionAnchors
from src.realtime.contracts import MarketVenue
from src.realtime.kis_sharding import AftermarketShard

_KST = ZoneInfo("Asia/Seoul")

__all__ = [
    "regular_kis_collection_due",
    "regular_kis_dynamic_stream_cmd",
    "regular_kis_stream_cmd",
    "regular_nxt_collection_due",
]


def regular_kis_collection_due(
    now: dt.datetime, *, anchors: SessionAnchors, start_lead_s: float, close_grace_s: float
) -> bool:
    """True iff KST weekday and now in [regular_open - start_lead_s, regular_close + close_grace_s)."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5:
        return False
    start_dt = dt.datetime.combine(kst.date(), anchors.regular_open, tzinfo=_KST) - dt.timedelta(
        seconds=start_lead_s
    )
    end_dt = dt.datetime.combine(kst.date(), anchors.regular_close, tzinfo=_KST) + dt.timedelta(
        seconds=close_grace_s
    )
    return start_dt <= kst < end_dt


def regular_nxt_collection_due(
    now: dt.datetime, *, anchors: SessionAnchors, start_lead_s: float, close_grace_s: float
) -> bool:
    """True iff KST weekday, unshifted anchors, and now in [regular_open - start_lead_s, closing_auction_start + close_grace_s)."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5 or anchors.open_shift != dt.timedelta(0):
        return False
    start_dt = dt.datetime.combine(kst.date(), anchors.regular_open, tzinfo=_KST) - dt.timedelta(
        seconds=start_lead_s
    )
    end_dt = dt.datetime.combine(kst.date(), anchors.closing_auction_start, tzinfo=_KST) + dt.timedelta(
        seconds=close_grace_s
    )
    return start_dt <= kst < end_dt


def regular_kis_stream_cmd(today: dt.date, paths: DataPaths, *, shard: AftermarketShard) -> list[str]:
    """argv for ``collect-regular-kis`` (same interpreter and module entry as the other children)."""
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-regular-kis",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.regular_kis_manifest_path(today, shard.shard_index, shard.venue)),
        "--candidates-path",
        str(paths.candidates if shard.venue is MarketVenue.KRX else paths.regular_nxt_candidates(today)),
        "--shard-index",
        str(shard.shard_index),
        "--venue",
        shard.venue.value,
        "--credential-slot",
        shard.credential_slot,
        "--credential-key-id",
        shard.credential_key_id,
        "--symbols",
        ",".join(shard.symbols),
    ]


def regular_kis_dynamic_stream_cmd(
    today: dt.date, paths: DataPaths, *, slot: str, key_id: str, venue: str = "krx"
) -> list[str]:
    """argv for ``collect-regular-kis-dynamic`` on a KIS dynamic slot; the default KRX argv omits ``--venue``."""
    venue_args = ["--venue", venue] if venue != "krx" else []
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-regular-kis-dynamic",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.regular_kis_manifest_path(today, int(slot), venue)),
        "--credential-slot",
        slot,
        "--credential-key-id",
        key_id,
        *venue_args,
    ]
