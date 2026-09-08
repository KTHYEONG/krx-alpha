"""Collector 24/7 daemon schedule and lifecycle manager."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import sys
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

from src.collector.storage_guard import prune_old_journals
from src.collector.supervisor import ProcessSupervisor, RestartCircuitBreaker

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
    archive_root: pathlib.Path | None = None,
) -> int:
    return prune_old_journals(journal_root, retain_days=retain_days, reference_date=today, archive_root=archive_root)


def run_eod_offload(
    archive_root: pathlib.Path,
    *,
    archiver: Any = None,
    reference_date: dt.date | None = None,
) -> dict[str, int]:
    from src.collector.remote_archive import HfDatasetArchiver
    from src.collector.storage_guard import prune_local_l1

    arc = archiver if archiver is not None else HfDatasetArchiver.try_from_env()
    if arc is None:
        logger.critical("[DAEMON] stage=eod_offload status=FAIL reason=hf_settings_missing")
        return {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    stats = arc.sync_l1_tree(archive_root)
    confirmed = arc.remote_files("l1/")
    stats["purged"] = prune_local_l1(
        archive_root, retain_days=30, reference_date=reference_date, confirmed_remote=confirmed
    )
    return stats


def run_session_orchestration(
    *,
    today: dt.date,
    bars_store: pathlib.Path,
    market_map_path: pathlib.Path,
    candidates_path: pathlib.Path,
    universe_out_path: pathlib.Path,
    streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0"),
    ls_capacity: int = 200,
) -> bool:
    import argparse

    import polars as pl

    from src.cli import bars_refresh, universe_plan
    from src.collector.vendor import SubscriptionPlanner, VendorCapacity

    bars_rc = bars_refresh.run(
        argparse.Namespace(
            store_path=str(bars_store),
            market_map_path=str(market_map_path),
            ref_date=today.isoformat(),
            window_days=90,
        )
    )
    if bars_rc != 0:
        logger.error("[DAEMON] stage=orchestration status=FAIL step=bars_refresh rc=%d", bars_rc)
    if not bars_store.exists():
        logger.critical("[DAEMON] stage=orchestration status=FAIL reason=no_bars_store")
        return False
    decision_date = pl.scan_parquet(bars_store).select(pl.col("date").max()).collect().item()
    slot_budget = SubscriptionPlanner(streams=streams).symbol_budget([VendorCapacity("ls", ls_capacity)])
    plan_rc = universe_plan.run(
        argparse.Namespace(
            bars_path=str(bars_store),
            decision_date=decision_date.isoformat(),
            out_path=str(universe_out_path),
            slot_budget=slot_budget,
            candidates_path=str(candidates_path),
        )
    )
    if plan_rc != 0:
        logger.error("[DAEMON] stage=orchestration status=FAIL step=universe_plan rc=%d", plan_rc)
    ready = _candidates_ready(candidates_path)
    logger.info(
        "[DAEMON] stage=orchestration status=OK decision_date=%s ready=%s",
        decision_date.isoformat(),
        ready,
    )
    return ready


def _candidates_ready(path: pathlib.Path) -> bool:
    from src.collector.ipc import read_candidates

    data = read_candidates(path)
    return bool(data and data.get("candidates"))


def run_collector_daemon(
    *, sleep_fn: Any = None, max_cycles: int | None = None, now_fn: Any = None
) -> None:
    import time
    sleeper = sleep_fn if sleep_fn is not None else time.sleep
    sched = CollectorDaemonSchedule()
    cycle = 0

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("[DAEMON] stage=start status=ONLINE timezone=Asia/Seoul")

    clock = now_fn if now_fn is not None else (lambda: dt.datetime.now(_KST))
    orchestrated_for: dt.date | None = None
    supervisor: ProcessSupervisor | None = None

    while True:
        cycle += 1
        now = clock()
        state = get_target_state(now)
        logger.info("[DAEMON] cycle=%d now=%s state=%s", cycle, now.strftime("%Y-%m-%d %H:%M:%S"), state)

        if state == "WEEKEND_SLEEP":
            sleep_sec = 3600.0  # 주말엔 1시간씩 대기
        elif state == "PRE_MARKET_SLEEP":
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 300.0)
        elif state in ("STREAMER_ACTIVE", "FULL_ACTIVE"):
            today = now.date()
            data_root = pathlib.Path("data")
            if orchestrated_for != today:
                bars_store = data_root / "bars" / "daily.parquet"
                market_map_path = data_root / "market_map.json"
                candidates_path = data_root / "candidates.json"
                universe_out_path = data_root / "universe" / f"{today.isoformat()}.parquet"
                ready = run_session_orchestration(
                    today=today,
                    bars_store=bars_store,
                    market_map_path=market_map_path,
                    candidates_path=candidates_path,
                    universe_out_path=universe_out_path,
                )
                orchestrated_for = today
                if ready:
                    manifest_path = data_root / "manifest" / f"{today.isoformat()}.json"
                    cmd = [
                        sys.executable,
                        "-m",
                        "src.cli.main",
                        "collect-stream",
                        "--session-date",
                        today.isoformat(),
                        "--journal-root",
                        str(data_root / "l0"),
                        "--manifest-path",
                        str(manifest_path),
                        "--candidates-path",
                        str(candidates_path),
                        "--market-map",
                        str(market_map_path),
                    ]
                    supervisor = ProcessSupervisor(cmd=cmd, breaker=RestartCircuitBreaker())
                else:
                    supervisor = None
                    logger.critical("[DAEMON] stage=session status=FAIL reason=candidates_not_ready")
            if supervisor is not None:
                result = supervisor.ensure_running()
                if result == "circuit_open":
                    logger.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
            sleep_sec = 10.0
        elif state == "POST_MARKET_EOD":
            if supervisor is not None:
                stop_result = supervisor.stop(timeout_s=15.0)
                logger.info("[DAEMON] stage=streamer_stop result=%s", stop_result)
            # 15:40 EOD 유지보수 (정규화 및 오래된 저널 prune)
            try:
                data_root = pathlib.Path("data/l0")
                if data_root.exists():
                    deleted = run_eod_maintenance(data_root, archive_root=pathlib.Path("data/l1"))
                    offload = run_eod_offload(pathlib.Path("data/l1"))
                    logger.info(
                        "[DAEMON] stage=eod_maintenance deleted_partitions=%d uploaded=%d purged=%d status=OK",
                        deleted, offload["uploaded"], offload["purged"],
                    )
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
