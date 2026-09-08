"""Collector 24/7 daemon schedule and lifecycle manager."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import Any, NamedTuple
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


def run_collector_daemon(*, sleep_fn: Any = None, max_cycles: int | None = None) -> None:
    import time
    sleeper = sleep_fn if sleep_fn is not None else time.sleep
    sched = CollectorDaemonSchedule()
    cycle = 0

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("[DAEMON] stage=start status=ONLINE timezone=Asia/Seoul")

    while True:
        cycle += 1
        now = dt.datetime.now(_KST)
        state = get_target_state(now)
        logger.info("[DAEMON] cycle=%d now=%s state=%s", cycle, now.strftime("%Y-%m-%d %H:%M:%S"), state)

        if state == "WEEKEND_SLEEP":
            sleep_sec = 3600.0  # 주말엔 1시간씩 대기
        elif state == "PRE_MARKET_SLEEP":
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 300.0)
        elif state in ("STREAMER_ACTIVE", "FULL_ACTIVE"):
            # 장중 활성 주기 (추후 스캐너/스트리머 프로세스 감시)
            sleep_sec = 10.0
        elif state == "POST_MARKET_EOD":
            # 15:40 EOD 유지보수 (정규화 및 오래된 저널 prune)
            try:
                data_root = pathlib.Path("data/l0")
                if data_root.exists():
                    deleted = run_eod_maintenance(data_root)
                    logger.info("[DAEMON] stage=eod_maintenance deleted_partitions=%d status=OK", deleted)
            except Exception as e:  # noqa: BLE001
                logger.error("[DAEMON] stage=eod_maintenance error=%s", str(e))
            sleep_sec = 60.0
        else:  # NIGHT_SLEEP
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 1800.0)

        logger.info("[DAEMON] sleeping for %.1f seconds...", sleep_sec)
        sleeper(sleep_sec)

        if max_cycles is not None and cycle >= max_cycles:
            logger.info("[DAEMON] reached max_cycles=%d, exiting gracefully.", max_cycles)
            break


def main() -> None:
    run_collector_daemon()


if __name__ == "__main__":
    main()
