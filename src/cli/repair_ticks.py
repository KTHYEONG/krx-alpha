"""repair-ticks CLI (one-shot nightly audit and gap-fill child; never runs inside the daemon loop)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
import signal
import threading
import uuid
from zoneinfo import ZoneInfo

from src.core.config import AuditSettings, CollectorSettings, RepairSettings, TickTapeBackfillSettings
from src.core.paths import DataPaths
from src.marketdata.tick_tape_backfill import read_ledger_done
from src.storage.l0_repair import derive_day_repair
from src.storage.l0_repair_job import repair_candidate_units, run_repair

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def _now_kst() -> dt.datetime:
    return dt.datetime.now(_KST)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register `repair-ticks` (one-shot nightly child; never runs inside the daemon loop)."""
    parser = subparsers.add_parser("repair-ticks")
    parser.add_argument("--deadline", default=None, help="ISO KST deadline; defaults to the next window end")
    parser.add_argument("--day", default=None, type=dt.date.fromisoformat, help="restrict to one day (manual re-run)")
    parser.add_argument("--data-root", type=pathlib.Path, default=None)
    parser.add_argument("--journal-root", type=pathlib.Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="print per-day states without writing")
    parser.set_defaults(handler=run)


def _next_window_end(now: dt.datetime, settings: TickTapeBackfillSettings) -> dt.datetime:
    end = settings.window_end_kst
    candidate = dt.datetime.combine(now.astimezone(_KST).date(), end, tzinfo=_KST)
    if candidate <= now.astimezone(_KST):
        candidate += dt.timedelta(days=1)
    return candidate


def _parse_deadline(raw: str | None, settings: TickTapeBackfillSettings, now: dt.datetime) -> dt.datetime:
    if raw is None:
        return _next_window_end(now, settings)
    moment = dt.datetime.fromisoformat(str(raw))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=_KST)
    return moment.astimezone(_KST)


def _window_identity(now: dt.datetime, settings: TickTapeBackfillSettings) -> dt.date:
    start, end = settings.window_start_kst, settings.window_end_kst
    wall = now.astimezone(_KST)
    if start <= end:
        return wall.date()
    return wall.date() if wall.time() >= start else wall.date() - dt.timedelta(days=1)


def run(args: argparse.Namespace) -> int:
    """Run `run_repair`; returns 0 ok/degraded-with-progress, 2 skip or nothing to do, 4 setup failure or FAIL."""
    try:
        return _run(args)
    except Exception as exc:  # noqa: BLE001 - invalid configuration and I/O map to setup failure
        logger.error("[DATA] stage=repair_ticks status=FAIL reason=setup error=%s", type(exc).__name__)
        return 4


def _run(args: argparse.Namespace) -> int:
    repair_settings = RepairSettings()
    if not repair_settings.job_enabled and not getattr(args, "dry_run", False):
        logger.info("[DATA] stage=repair_ticks status=SKIP reason=disabled")
        return 2
    collector = CollectorSettings()
    root = getattr(args, "data_root", None)
    paths = DataPaths(pathlib.Path(root)) if root is not None else collector.paths
    journal_arg = getattr(args, "journal_root", None)
    journal_root = pathlib.Path(journal_arg) if journal_arg is not None else collector.paths.journal_root
    tape_settings = TickTapeBackfillSettings()
    now = _now_kst()
    try:
        deadline = min(
            _parse_deadline(getattr(args, "deadline", None), tape_settings, now),
            _next_window_end(now, tape_settings),
        )
    except ValueError:
        logger.error("[DATA] stage=repair_ticks status=FAIL reason=invalid_deadline")
        return 4
    window_date = _window_identity(now, tape_settings)
    only_day = getattr(args, "day", None)
    if getattr(args, "dry_run", False):
        tape_done = read_ledger_done(paths.tick_tape_ledger)
        units = repair_candidate_units(
            journal_root, repair_settings=repair_settings, window_date=window_date,
            retain_days=collector.journal_retain_days, only_day=only_day,
        )
        entries: list[dict[str, str]] = []
        for day, vendor in units:
            state = derive_day_repair(paths, day, vendor, settings=repair_settings, tape_done=tape_done)
            entries.append({"day": day.isoformat(), "vendor": vendor, "state": state.state.value})
        print(json.dumps({"days": entries}))  # noqa: T201 - CLI result goes to stdout
        return 0
    audit_settings = AuditSettings()
    run_id = uuid.uuid4().hex[:12]
    stop_event = threading.Event()

    def _request_stop(signum: int, frame: object) -> None:
        stop_event.set()

    prev_sigterm = signal.getsignal(signal.SIGTERM)
    prev_sigint = signal.getsignal(signal.SIGINT)
    try:
        signal.signal(signal.SIGTERM, _request_stop)
        signal.signal(signal.SIGINT, _request_stop)
        summary = run_repair(
            paths=paths,
            journal_root=journal_root,
            audit_settings=audit_settings,
            repair_settings=repair_settings,
            retain_days=int(collector.journal_retain_days),
            window_date=window_date,
            deadline=deadline,
            now=_now_kst,
            run_id=run_id,
            only_day=only_day,
            should_stop=stop_event.is_set,
        )
    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)
    if summary.status == "FAIL":
        return 4
    if summary.status == "SKIP":
        return 2
    if summary.status not in ("OK", "DEGRADED"):
        return 4
    return 0
