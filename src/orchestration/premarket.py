"""NXT premarket orchestration helpers (pure, no daemon import)."""

from __future__ import annotations

import datetime as dt
import pathlib
import sys
from typing import NamedTuple
from zoneinfo import ZoneInfo

from src.core.paths import DataPaths
from src.core.session_anchors import (
    STANDARD_NXT_PREMARKET_END,
    STANDARD_NXT_PREMARKET_OPEN,
    SessionAnchors,
)
from src.realtime.kis_sharding import AftermarketShard
from src.realtime.manifest import SessionManifest

_KST = ZoneInfo("Asia/Seoul")

PREMARKET_POLL_S: float = 10.0


def premarket_window(anchors: SessionAnchors) -> tuple[dt.time, dt.time]:
    """Premarket [start, end) on the anchors' date: the NXT premarket constants shifted with ``shift_pre_open``."""
    return (anchors.shift_pre_open(STANDARD_NXT_PREMARKET_OPEN), anchors.shift_pre_open(STANDARD_NXT_PREMARKET_END))


def premarket_collection_due(now: dt.datetime, *, anchors: SessionAnchors, start_lead_s: float) -> bool:
    """True iff the collector should run at ``now`` (KST weekday, unshifted session, inside [start - lead, end)).

    Shifted-session days (``anchors.open_shift != 0``) are never due: NXT premarket timing on special
    days is unverified, so collection is skipped fail-closed instead of guessed.
    """
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5:
        return False
    if anchors.open_shift != dt.timedelta(0):
        return False
    start, end = premarket_window(anchors)
    start_dt = dt.datetime.combine(kst.date(), start, tzinfo=_KST) - dt.timedelta(seconds=start_lead_s)
    end_dt = dt.datetime.combine(kst.date(), end, tzinfo=_KST)
    return start_dt <= kst < end_dt


def premarket_pool_due(now: dt.datetime, *, anchors: SessionAnchors, settle_s: float) -> bool:
    """True iff the evening pool refresh may run: KST weekday and ``now >= after_market_end + settle_s`` on the same date."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5:
        return False
    due_dt = dt.datetime.combine(kst.date(), anchors.after_market_end, tzinfo=_KST) + dt.timedelta(seconds=settle_s)
    if due_dt.date() != kst.date():
        return False
    return kst >= due_dt


def premarket_wake_cap_s(now: dt.datetime, *, anchors: SessionAnchors, start_lead_s: float) -> float | None:
    """Maximum daemon sleep that still wakes at collector start and polls inside the window; None when no cap applies."""
    kst = now.astimezone(_KST)
    if kst.weekday() >= 5:
        return None
    if anchors.open_shift != dt.timedelta(0):
        return None
    start, end = premarket_window(anchors)
    start_dt = dt.datetime.combine(kst.date(), start, tzinfo=_KST) - dt.timedelta(seconds=start_lead_s)
    end_dt = dt.datetime.combine(kst.date(), end, tzinfo=_KST)
    if kst < start_dt:
        midnight = dt.datetime.combine(kst.date() + dt.timedelta(days=1), dt.time(0, 0), tzinfo=_KST)
        return max(0.0, min((start_dt - kst).total_seconds(), (midnight - kst).total_seconds()))
    if kst < end_dt:
        return PREMARKET_POLL_S
    return None


def premarket_stream_cmd(today: dt.date, paths: DataPaths, *, shard: AftermarketShard) -> list[str]:
    """argv for ``collect-premarket`` (same interpreter and module entry as the other children)."""
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-premarket",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.premarket_manifest_path(today)),
        "--candidates-path",
        str(paths.premarket_candidates(today)),
        "--credential-slot",
        shard.credential_slot,
        "--credential-key-id",
        shard.credential_key_id,
        "--symbols",
        ",".join(shard.symbols),
    ]


class PremarketEodReport(NamedTuple):
    closed: bool
    accepted_pairs: int
    planned_pairs: int


def premarket_eod_report(manifest_path: pathlib.Path, *, date: dt.date) -> PremarketEodReport:
    """Closure and ACK coverage of the premarket manifest; never raises on a missing or corrupt file (closed=False, zeros)."""
    try:
        manifest = SessionManifest.load(pathlib.Path(manifest_path))
    except (ValueError, KeyError, TypeError, OSError):
        return PremarketEodReport(closed=False, accepted_pairs=0, planned_pairs=0)
    planned = [(str(pair.get("symbol")), str(pair.get("tr_id"))) for pair in manifest.planned_pairs]
    planned_set = set(planned)
    accepted = {
        (str(ack.get("symbol")), str(ack.get("tr_id")))
        for ack in manifest.subscription_acks
        if ack.get("accepted") is True
    }
    accepted_pairs = len(accepted & planned_set)
    closed = (
        manifest.session_date == date
        and manifest.venue == "nxt"
        and manifest.session == "nxt_pre"
        and manifest.writer_closed_at_ns is not None
        and manifest.expected_close_ns > 0
    )
    return PremarketEodReport(closed=closed, accepted_pairs=accepted_pairs, planned_pairs=len(planned))


__all__ = [
    "PREMARKET_POLL_S",
    "PremarketEodReport",
    "premarket_collection_due",
    "premarket_eod_report",
    "premarket_pool_due",
    "premarket_stream_cmd",
    "premarket_wake_cap_s",
    "premarket_window",
]
