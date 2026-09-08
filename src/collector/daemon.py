"""Collector 24/7 daemon schedule and lifecycle manager."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import NamedTuple
from zoneinfo import ZoneInfo

from src.collector.storage_guard import prune_old_journals

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")


class CollectorDaemonSchedule(NamedTuple):
    pre_market: dt.time = dt.time(8, 20, 0)
    streamer_start: dt.time = dt.time(8, 20, 0)
    scanner_start: dt.time = dt.time(8, 50, 0)
    market_close: dt.time = dt.time(15, 40, 0)
    eod_done: dt.time = dt.time(16, 0, 0)


def get_target_state(now_dt: dt.datetime) -> str:
    kst_dt = now_dt.astimezone(_KST)
    if kst_dt.weekday() in (5, 6):
        return "WEEKEND_SLEEP"

    t = kst_dt.time()
    if t < dt.time(8, 20, 0):
        return "PRE_MARKET_SLEEP"
    if t < dt.time(8, 50, 0):
        return "STREAMER_ACTIVE"
    if t < dt.time(15, 40, 0):
        return "FULL_ACTIVE"
    if t < dt.time(16, 0, 0):
        return "POST_MARKET_EOD"
    return "NIGHT_SLEEP"


def calc_sleep_seconds(now_dt: dt.datetime, target_time: dt.time) -> float:
    kst_dt = now_dt.astimezone(_KST)
    target_dt = kst_dt.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=target_time.second,
        microsecond=0,
    )
    if target_dt <= kst_dt:
        target_dt += dt.timedelta(days=1)
    diff = (target_dt - kst_dt).total_seconds()
    return float(diff)


def run_eod_maintenance(
    journal_root: pathlib.Path,
    *,
    retain_days: int = 3,
    today: dt.date | None = None,
) -> int:
    return prune_old_journals(journal_root, retain_days=retain_days, reference_date=today)
