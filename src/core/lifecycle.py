"""Daemon crash-restart lifecycle record (kill vs crash disambiguation)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DaemonLifecycleRecord:
    run_id: str
    started_at: dt.datetime
    clean_exit: bool
    crash_error: str | None
    crash_alert_day: dt.date | None
    crash_alerts_sent: int


def _to_json(record: DaemonLifecycleRecord) -> dict[str, Any]:
    return {
        "run_id": record.run_id,
        "started_at": record.started_at.isoformat(),
        "clean_exit": record.clean_exit,
        "crash_error": record.crash_error,
        "crash_alert_day": record.crash_alert_day.isoformat() if record.crash_alert_day is not None else None,
        "crash_alerts_sent": record.crash_alerts_sent,
    }


def _from_json(raw: Any) -> DaemonLifecycleRecord:
    if not isinstance(raw, dict):
        raise TypeError("lifecycle payload must be an object")
    alert_day = raw.get("crash_alert_day")
    return DaemonLifecycleRecord(
        run_id=str(raw["run_id"]),
        started_at=dt.datetime.fromisoformat(str(raw["started_at"])),
        clean_exit=bool(raw["clean_exit"]),
        crash_error=str(raw["crash_error"]) if raw.get("crash_error") is not None else None,
        crash_alert_day=dt.date.fromisoformat(str(alert_day)) if alert_day is not None else None,
        crash_alerts_sent=int(raw["crash_alerts_sent"]),
    )


def read_lifecycle(path: pathlib.Path) -> DaemonLifecycleRecord | None:
    """Return the persisted lifecycle record, or None when missing or invalid."""
    try:
        return _from_json(json.loads(pathlib.Path(path).read_text(encoding="utf-8")))
    except Exception as exc:
        logger.debug("[SYS] stage=daemon_lifecycle status=UNREADABLE reason=%s", type(exc).__name__)
        return None


def write_lifecycle(path: pathlib.Path, record: DaemonLifecycleRecord) -> None:
    """Persist the lifecycle record atomically via tmp + rename."""
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    tmp.write_text(json.dumps(_to_json(record), ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)


def crash_alert_allowed(record: DaemonLifecycleRecord | None, today: dt.date, budget: int) -> bool:
    """Return whether another crash alert may be sent for ``today``."""
    day = record.crash_alert_day if record is not None else None
    if day != today:
        return True
    sent = record.crash_alerts_sent if record is not None else 0
    return sent < budget
