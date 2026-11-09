"""Observability base: per-component logging, persistent JSONL events and operator page emails."""

import atexit
import datetime as dt
import logging
import pathlib
import queue
import sys
import threading
import time
from collections.abc import Callable
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import IO

from src.core.alerts import (
    ALERT_COOLDOWN_S,
    ALERT_DAILY_CAP,
    EmailAlertHandler,
    FileAlertLedger,
    PageKind,
    gmail_sender,
    is_alert_record,
    page,
    page_resolved,
)
from src.core.config import AlertSettings, ObservabilitySettings, export_run_id
from src.core.log_format import (
    EventFileFilter,
    JsonlEventFormatter,
    KeyValueFormatter,
    clear_process_identity,
    format_record_exception,
    format_record_timestamp,
    process_identity,
    process_identity_tags,
    set_process_identity,
)

EVENT: dict[str, bool] = {"krx_event": True}
_ONCE_MAX_KEYS: int = 256
_ONCE_LOCK = threading.Lock()
_ONCE_SEEN: dict[str, dt.date] = {}
LOG_FILE_MAX_BYTES: int = 5 * 2**20
LOG_FILE_BACKUPS: int = 5
_MANAGED_ATTR = "_krx_alpha_managed"
_LISTENERS: list[QueueListener] = []

__all__ = [
    "ALERT_COOLDOWN_S",
    "ALERT_DAILY_CAP",
    "EVENT",
    "LOG_FILE_BACKUPS",
    "LOG_FILE_MAX_BYTES",
    "EmailAlertHandler",
    "EventFileFilter",
    "JsonlEventFormatter",
    "KeyValueFormatter",
    "PageKind",
    "clear_process_identity",
    "configure_logging",
    "format_record_exception",
    "format_record_timestamp",
    "gmail_sender",
    "new_run_id",
    "once_per_day",
    "page",
    "page_resolved",
    "process_identity",
    "process_identity_tags",
    "reset_once_per_day",
    "set_process_identity",
    "shutdown_logging",
]


def once_per_day(key: str, day: dt.date) -> bool:
    """Return True the first time ``key`` is seen on ``day``; tracking is process-local and bounded."""
    with _ONCE_LOCK:
        if _ONCE_SEEN.get(key) == day:
            return False
        if key not in _ONCE_SEEN and len(_ONCE_SEEN) >= _ONCE_MAX_KEYS:
            _ONCE_SEEN.clear()
        _ONCE_SEEN[key] = day
        return True


def reset_once_per_day() -> None:
    """Forget every tracked key (test isolation)."""
    with _ONCE_LOCK:
        _ONCE_SEEN.clear()


def new_run_id(component: str, *, now_ns: int | None = None) -> str:
    ns = time.time_ns() if now_ns is None else now_ns
    return f"{component}-{ns // 1_000_000}"


def configure_logging(
    component: str,
    *,
    log_dir: pathlib.Path | None = None,
    level: int | str = logging.INFO,
    stream: IO[str] | None = None,
    alert_settings: AlertSettings | None = None,
    alert_sender: Callable[[str, str], None] | None = None,
    alert_ledger_path: pathlib.Path | None = None,
) -> str:
    """Install current process handlers and return a correlation run ID.

    Only records marked with ``page``/``page_resolved`` reach the email path; other levels stay log-only.
    """
    run_id = ObservabilitySettings().run_id or new_run_id(component)
    export_run_id(run_id)
    shutdown_logging()
    root = logging.getLogger()
    root.setLevel(level)
    stream_handler = logging.StreamHandler(stream or sys.stderr)
    stream_handler.setFormatter(KeyValueFormatter(component=component, run_id=run_id))
    setattr(stream_handler, _MANAGED_ATTR, True)
    root.addHandler(stream_handler)
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_dir / f"events-{component}.jsonl",
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUPS,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonlEventFormatter(component=component, run_id=run_id))
        file_handler.addFilter(EventFileFilter())
        setattr(file_handler, _MANAGED_ATTR, True)
        root.addHandler(file_handler)
    alerts = alert_settings if alert_settings is not None else AlertSettings()
    sys_logger = logging.getLogger(__name__)
    if alerts.enabled:
        q: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
        queue_handler = QueueHandler(q)
        queue_handler.addFilter(is_alert_record)
        setattr(queue_handler, _MANAGED_ATTR, True)
        if alert_ledger_path is not None:
            email_handler = EmailAlertHandler(
                component=component,
                run_id=run_id,
                sender=alert_sender or gmail_sender(alerts),
                ledger=FileAlertLedger(alert_ledger_path),
                clock=time.time,
            )
        else:
            email_handler = EmailAlertHandler(
                component=component,
                run_id=run_id,
                sender=alert_sender or gmail_sender(alerts),
            )
        listener = QueueListener(q, email_handler, respect_handler_level=False)
        listener.start()
        _LISTENERS.append(listener)
        root.addHandler(queue_handler)
        sys_logger.info("[SYS] stage=alert status=ENABLED")
    else:
        sys_logger.info("[SYS] stage=alert status=DISABLED")
    return run_id


def shutdown_logging() -> None:
    """Flush and stop owned queue listeners without discarding diagnostics."""
    for listener in _LISTENERS:
        listener.stop()
    _LISTENERS.clear()
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _MANAGED_ATTR, False):
            root.removeHandler(handler)
            handler.close()


atexit.register(shutdown_logging)
