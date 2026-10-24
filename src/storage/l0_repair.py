"""Audited L0 repair state and deletion hold for gate-vendor tick partitions."""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import pathlib
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

from src.core.config import RepairSettings
from src.core.paths import DataPaths
from src.storage.layout import l0_partition_key, l0_partition_relpath

logger = logging.getLogger(__name__)


class RepairState(StrEnum):
    CLEAN = "clean"
    UNAUDITED = "unaudited"
    TAPE_PENDING = "tape_pending"
    GAPFILL_PENDING = "gapfill_pending"
    MERGE_PENDING = "merge_pending"
    REPAIRED = "repaired"


@dataclass(frozen=True)
class DayRepair:
    day: dt.date
    vendor: str
    state: RepairState
    confirmed_missing: int
    residual: int | None
    gap_symbol_days_pending: int


_Count = Annotated[int, Field(strict=True, ge=0)]


class _AuditTotals(BaseModel):
    confirmed_missing: _Count


class _AuditSymbol(_AuditTotals):
    symbol: str = Field(min_length=1, strict=True)


class _RepairAudit(BaseModel):
    session_date: dt.date
    status: Literal["OK", "PARTIAL"]
    vendor: str
    venue: Literal["krx"]
    session: Literal["regular"]
    totals: _AuditTotals
    symbols: list[_AuditSymbol]


class _ReportTotals(BaseModel):
    residual: _Count


class _GapfillReport(BaseModel):
    totals: _ReportTotals
    intervals: list[dict[str, Any]]
    notes: dict[str, Any] = Field(default_factory=dict)


def _merged_ticks_valid(paths: DataPaths, day: dt.date, vendor: str) -> bool:
    import importlib

    try:
        mod = importlib.import_module("src.storage.l0_repair_merge")
        validator = mod.repaired_ticks_valid
        return bool(validator(paths, day, vendor))
    except Exception:  # noqa: BLE001 - missing module or torn file degrades to pending
        return False


def _report_valid(report_path: pathlib.Path, *, audit_bytes: bytes, audit_mtime_ns: int) -> _GapfillReport | None:
    try:
        body = _GapfillReport.model_validate_json(report_path.read_bytes())
    except (OSError, ValueError):
        return None
    if "audit_sha256" in body.notes:
        digest = hashlib.sha256(audit_bytes).hexdigest()
        if body.notes["audit_sha256"] != digest:
            return None
        return body
    try:
        report_mtime_ns = report_path.stat().st_mtime_ns
    except OSError:
        return None
    if report_mtime_ns < audit_mtime_ns:
        return None
    return body


def derive_day_repair(
    paths: DataPaths,
    day: dt.date,
    vendor: str,
    *,
    settings: RepairSettings,
    tape_done: AbstractSet[tuple[str, dt.date]],
) -> DayRepair:
    """Classify one (day, vendor) from the persisted audit, tape ledger, gap-fill report and merged-ticks file; read-only, never raises on missing or torn files."""
    if not vendor:
        raise ValueError("vendor must be a non-empty journal vendor id")
    audit_path = paths.session_audit_path(day, vendor)
    try:
        audit_bytes = audit_path.read_bytes()
        audit_mtime_ns = audit_path.stat().st_mtime_ns
        audit = _RepairAudit.model_validate_json(audit_bytes)
    except (OSError, ValueError):
        return DayRepair(day, vendor, RepairState.UNAUDITED, 0, None, 0)
    if audit.session_date != day or audit.vendor != vendor:
        return DayRepair(day, vendor, RepairState.UNAUDITED, 0, None, 0)
    confirmed = audit.totals.confirmed_missing
    gap_symbols = {entry.symbol for entry in audit.symbols if entry.confirmed_missing > 0}
    if vendor not in settings.repair_vendors:
        return DayRepair(day, vendor, RepairState.CLEAN, confirmed, None, 0)
    if confirmed == 0:
        if settings.merge_required and not _merged_ticks_valid(paths, day, vendor):
            return DayRepair(day, vendor, RepairState.MERGE_PENDING, confirmed, None, 0)
        return DayRepair(day, vendor, RepairState.CLEAN, confirmed, None, 0)
    if not gap_symbols:
        return DayRepair(day, vendor, RepairState.UNAUDITED, confirmed, None, 0)
    pending = sum(1 for symbol in gap_symbols if (symbol, day) not in tape_done)
    if pending > 0:
        return DayRepair(day, vendor, RepairState.TAPE_PENDING, confirmed, None, pending)
    _, report_path = paths.gapfill_paths(day, vendor)
    report = _report_valid(report_path, audit_bytes=audit_bytes, audit_mtime_ns=audit_mtime_ns)
    if report is None:
        return DayRepair(day, vendor, RepairState.GAPFILL_PENDING, confirmed, None, 0)
    if settings.merge_required and not _merged_ticks_valid(paths, day, vendor):
        return DayRepair(day, vendor, RepairState.MERGE_PENDING, confirmed, report.totals.residual, 0)
    return DayRepair(day, vendor, RepairState.REPAIRED, confirmed, report.totals.residual, 0)


def audited_tick_vendor(
    l0_relpath: pathlib.PurePath, *, gate_vendors: tuple[str, ...] | None = None
) -> str | None:
    """Vendor id when the relpath is a `krx/regular/H0STCNT0` L0 partition of a gate vendor, else None."""
    parts = pathlib.PurePath(l0_relpath).parts
    if len(parts) != 5:
        return None
    vendor, venue, session, stream, dt_part = parts
    vendors = gate_vendors if gate_vendors is not None else RepairSettings.model_fields["gate_vendors"].default
    if vendor not in vendors:
        return None
    if not dt_part.startswith("dt="):
        return None
    try:
        day = dt.date.fromisoformat(dt_part[3:])
    except ValueError:
        return None
    key = l0_partition_key(vendor, venue, session, stream, day)
    if (
        key is None
        or (venue, session, stream) != ("krx", "regular", "H0STCNT0")
        or l0_partition_relpath(key) != pathlib.PurePosixPath(l0_relpath)
    ):
        return None
    return vendor


def l0_hold_reason(
    part_relpath: pathlib.PurePath,
    part_date: dt.date,
    *,
    paths: DataPaths,
    settings: RepairSettings,
    tape_done: AbstractSet[tuple[str, dt.date]],
    cutoff: dt.date,
) -> str | None:
    """Reason string (`<vendor>:<state>`) when deleting this L0 partition must wait, else None."""
    if not settings.hold_enabled:
        return None
    vendor = audited_tick_vendor(part_relpath, gate_vendors=settings.gate_vendors)
    if vendor is None:
        return None
    repair = derive_day_repair(paths, part_date, vendor, settings=settings, tape_done=tape_done)
    if repair.state is RepairState.CLEAN or repair.state is RepairState.REPAIRED:
        return None
    if (cutoff - part_date).days > settings.hold_max_extra_days:
        logger.critical(
            "[DATA] stage=repair_hold status=EXPIRED vendor=%s day=%s state=%s",
            vendor,
            part_date.isoformat(),
            repair.state.value,
        )
        return None
    return f"{vendor}:{repair.state.value}"
