"""KRX regular-session KIS co-primary shard helpers (pure, no daemon import)."""

from __future__ import annotations

import datetime as dt
import sys
from zoneinfo import ZoneInfo

from src.core.paths import DataPaths
from src.core.session_anchors import SessionAnchors
from src.realtime.kis_sharding import AftermarketShard

_KST = ZoneInfo("Asia/Seoul")

__all__ = [
    "regular_kis_collection_due",
    "regular_kis_stream_cmd",
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
        str(paths.regular_kis_manifest_path(today, shard.shard_index)),
        "--candidates-path",
        str(paths.candidates),
        "--shard-index",
        str(shard.shard_index),
        "--credential-slot",
        shard.credential_slot,
        "--credential-key-id",
        shard.credential_key_id,
        "--symbols",
        ",".join(shard.symbols),
    ]
