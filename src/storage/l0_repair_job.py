"""Nightly L0 audit and gap-fill job for gate-vendor tick partitions."""

from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import logging
import pathlib
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from src.core.config import AuditSettings, RepairSettings
from src.core.paths import DataPaths
from src.core.session_anchors import standard_session_anchors
from src.marketdata.tick_tape_backfill import read_ledger_done
from src.storage.l0_audit import AuditStatus, derive_session_audit, write_session_audit
from src.storage.l0_gapfill import (
    GapfillSource,
    derive_gapfill,
    load_tape_for_symbols,
    received_trades_for_gaps,
    write_gapfill,
)
from src.storage.l0_repair import RepairState, derive_day_repair
from src.storage.l0_repair_merge import build_repaired_ticks, repaired_ticks_valid

logger = logging.getLogger(__name__)
_HEARTBEAT_S = 60.0


@dataclass(frozen=True)
class RepairRunSummary:
    status: str
    days: int
    audited: int
    gapfilled: int
    waiting_tape: int
    failed: int
    run_id: str
    merged: int = 0


def repair_candidate_units(
    journal_root: pathlib.Path,
    *,
    repair_settings: RepairSettings,
    window_date: dt.date,
    retain_days: int,
    only_day: dt.date | None = None,
) -> tuple[tuple[dt.date, str], ...]:
    """Read local gate-vendor tick partition dates in retention order."""
    earliest = window_date - dt.timedelta(days=retain_days + repair_settings.hold_max_extra_days + 1)
    units: set[tuple[dt.date, str]] = set()
    for vendor in repair_settings.gate_vendors:
        base = journal_root / vendor / "krx" / "regular" / "H0STCNT0"
        if not base.exists():
            continue
        for entry in base.iterdir():
            if not entry.is_dir() or not entry.name.startswith("dt="):
                continue
            try:
                day = dt.date.fromisoformat(entry.name[3:])
            except ValueError:
                continue
            if earliest <= day <= window_date and (only_day is None or day == only_day):
                units.add((day, vendor))
    return tuple(sorted(units))


def _existing_audit_ok(paths: DataPaths, day: dt.date, vendor: str) -> bool:
    try:
        raw = json.loads(paths.session_audit_path(day, vendor).read_bytes())
    except (OSError, ValueError):
        return False
    return (
        isinstance(raw, dict)
        and raw.get("status") in ("OK", "PARTIAL")
        and raw.get("session_date") == day.isoformat()
        and raw.get("vendor") == vendor
    )


@contextmanager
def _heartbeat(day: dt.date, vendor: str, run_id: str) -> Iterator[None]:
    stopped = threading.Event()

    def emit() -> None:
        while not stopped.wait(_HEARTBEAT_S):
            logger.info(
                "[DATA] stage=repair_ticks progress=heartbeat day=%s vendor=%s run_id=%s",
                day,
                vendor,
                run_id,
            )

    worker = threading.Thread(target=emit, daemon=True)
    worker.start()
    try:
        yield
    finally:
        stopped.set()
        worker.join()


def _run_locked(
    *,
    paths: DataPaths,
    journal_root: pathlib.Path,
    audit_settings: AuditSettings,
    repair_settings: RepairSettings,
    retain_days: int,
    window_date: dt.date,
    deadline: dt.datetime,
    now: Callable[[], dt.datetime],
    run_id: str,
    only_day: dt.date | None,
    should_stop: Callable[[], bool],
) -> RepairRunSummary:
    units = repair_candidate_units(
        journal_root,
        repair_settings=repair_settings,
        window_date=window_date,
        retain_days=retain_days,
        only_day=only_day,
    )
    if not units:
        return RepairRunSummary("SKIP", 0, 0, 0, 0, 0, run_id)
    tape_done = read_ledger_done(paths.tick_tape_ledger)
    audited = gapfilled = waiting = failed = attempted = merged = 0
    pending_unaudited = False

    def stopped() -> bool:
        return should_stop() or now() >= deadline

    for day, vendor in units:
        if stopped():
            break
        attempted += 1
        before_state = "unknown"
        after_state = "unknown"
        try:
            repair = derive_day_repair(paths, day, vendor, settings=repair_settings, tape_done=tape_done)
            before_state = after_state = repair.state.value
            with _heartbeat(day, vendor, run_id):
                if repair.state is RepairState.UNAUDITED and not _existing_audit_ok(paths, day, vendor):
                    audit = derive_session_audit(
                        journal_root,
                        vendor=vendor,
                        venue="krx",
                        session="regular",
                        day=day,
                        anchors=standard_session_anchors(day),
                        settings=audit_settings,
                        manifest_frame_counts=None,
                    )
                    write_session_audit(audit, paths.session_audit_path(day, vendor))
                    if audit.status is AuditStatus.UNAVAILABLE:
                        raise ValueError("audit unavailable")
                    audited += 1
                    repair = derive_day_repair(paths, day, vendor, settings=repair_settings, tape_done=tape_done)
                if repair.state is RepairState.GAPFILL_PENDING and vendor == "ls" and not stopped():
                    audit_bytes = paths.session_audit_path(day, vendor).read_bytes()
                    audit_doc = json.loads(audit_bytes)
                    symbols = {entry["symbol"] for entry in audit_doc["symbols"] if entry["confirmed_missing"] > 0}
                    tape = load_tape_for_symbols(paths, day, symbols)
                    received = received_trades_for_gaps(audit_doc, journal_root, vendor=vendor, day=day)
                    rows, outcomes = derive_gapfill(
                        audit_doc,
                        [GapfillSource("kiwoom", "gapfill:kiwoom_tape", tape)],
                        received,
                    )
                    rows_path, report_path = paths.gapfill_paths(day, vendor)
                    write_gapfill(
                        rows,
                        outcomes,
                        rows_path=rows_path,
                        report_path=report_path,
                        notes={
                            "anchored_on_journal": True,
                            "audit_sha256": hashlib.sha256(audit_bytes).hexdigest(),
                            "generated_by": "repair-ticks",
                            "run_id": run_id,
                        },
                    )
                    gapfilled += 1
                    repair = derive_day_repair(paths, day, vendor, settings=repair_settings, tape_done=tape_done)
                if repair.state is RepairState.TAPE_PENDING:
                    waiting += 1
                    logger.info(
                        "[DATA] stage=repair_ticks pending_tape day=%s vendor=%s symbol_days=%d",
                        day,
                        vendor,
                        repair.gap_symbol_days_pending,
                    )
                elif (
                    repair.state in (RepairState.MERGE_PENDING, RepairState.REPAIRED, RepairState.CLEAN)
                    and vendor == "ls" and vendor in repair_settings.repair_vendors
                    and not repaired_ticks_valid(paths, day, vendor)
                    and not stopped()
                ):
                    try:
                        merge = build_repaired_ticks(paths=paths, journal_root=journal_root, day=day, vendor=vendor)
                    except Exception as exc:  # noqa: BLE001 - isolate all per-day failures, not KeyboardInterrupt
                        failed += 1
                        logger.error(
                            "[DATA] stage=repair_ticks status=FAIL day=%s vendor=%s error=%s",
                            day,
                            vendor,
                            type(exc).__name__,
                        )
                    else:
                        if merge.status != "OK":
                            failed += 1
                            logger.error("[DATA] stage=repair_ticks status=MISMATCH day=%s vendor=%s", day, vendor)
                        else:
                            merged += 1
                            repair = derive_day_repair(
                                paths, day, vendor, settings=repair_settings, tape_done=tape_done
                            )
                after_state = repair.state.value
        except Exception as exc:  # noqa: BLE001 - isolate all per-day failures, not KeyboardInterrupt
            failed += 1
            logger.error(
                "[DATA] stage=repair_ticks status=FAIL day=%s vendor=%s error=%s",
                day,
                vendor,
                type(exc).__name__,
            )
        finally:
            logger.info(
                "[DATA] stage=repair_ticks day=%s vendor=%s state=%s->%s run_id=%s",
                day,
                vendor,
                before_state,
                after_state,
                run_id,
            )
    for day, vendor in units:
        try:
            pending_unaudited |= (
                derive_day_repair(
                    paths,
                    day,
                    vendor,
                    settings=repair_settings,
                    tape_done=tape_done,
                ).state
                is RepairState.UNAUDITED
            )
        except Exception:  # noqa: BLE001 - unreadable state cannot certify a clean summary
            pending_unaudited = True
    status = "FAIL" if attempted and failed == attempted else "DEGRADED" if failed or pending_unaudited else "OK"
    return RepairRunSummary(status, len({day for day, _ in units}), audited, gapfilled, waiting, failed, run_id, merged)


def run_repair(
    *,
    paths: DataPaths,
    journal_root: pathlib.Path,
    audit_settings: AuditSettings,
    repair_settings: RepairSettings,
    retain_days: int,
    window_date: dt.date,
    deadline: dt.datetime,
    now: Callable[[], dt.datetime],
    run_id: str,
    only_day: dt.date | None = None,
    should_stop: Callable[[], bool] = lambda: False,
) -> RepairRunSummary:
    """Audit and gap-fill retained gate-vendor L0, oldest first, under a non-blocking lock."""
    try:
        if deadline.tzinfo is None:
            raise ValueError("deadline must be timezone-aware")
        paths.work_root.mkdir(parents=True, exist_ok=True)
        with (paths.work_root / "repair.lock").open("a+b") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                summary = RepairRunSummary("SKIP", 0, 0, 0, 0, 0, run_id)
            else:
                summary = _run_locked(
                    paths=paths,
                    journal_root=journal_root,
                    audit_settings=audit_settings,
                    repair_settings=repair_settings,
                    retain_days=retain_days,
                    window_date=window_date,
                    deadline=deadline,
                    now=now,
                    run_id=run_id,
                    only_day=only_day,
                    should_stop=should_stop,
                )
    except Exception as exc:  # noqa: BLE001 - setup failures fail closed; KeyboardInterrupt propagates
        logger.error("[DATA] stage=repair_ticks status=FAIL reason=setup error=%s", type(exc).__name__)
        summary = RepairRunSummary("FAIL", 0, 0, 0, 0, 1, run_id)
    logger.info(
        "[DATA] stage=repair_ticks status=%s run_id=%s days=%d audited=%d gapfilled=%d waiting_tape=%d failed=%d merged=%d",
        summary.status,
        run_id,
        summary.days,
        summary.audited,
        summary.gapfilled,
        summary.waiting_tape,
        summary.failed,
        summary.merged,
    )
    return summary
