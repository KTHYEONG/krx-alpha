"""관측 가능성 기반: 컴포넌트별 로깅·영속 JSONL 이벤트·CRITICAL 이메일 알림."""

import logging
import pathlib
import queue
import sys
import time
from collections.abc import Callable
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import IO

from src.core.alerts import (
    _ACTION_HINTS as _ACTION_HINTS,
)
from src.core.alerts import (
    _REASON_LABELS as _REASON_LABELS,
)
from src.core.alerts import (
    _STAGE_LABELS as _STAGE_LABELS,
)
from src.core.alerts import (
    ALERT_COOLDOWN_S,
    ALERT_DAILY_CAP,
    EmailAlertHandler,
    format_alert_html,
    format_alert_subject,
    format_alert_text,
    format_digest_html,
    format_digest_text,
    gmail_sender,
    send_digest,
)
from src.core.config import AlertSettings, ObservabilitySettings, export_run_id
from src.core.log_format import (
    EventFileFilter,
    JsonlEventFormatter,
    KeyValueFormatter,
    format_record_exception,
    format_record_timestamp,
)

EVENT: dict[str, bool] = {"krx_event": True}
LOG_FILE_MAX_BYTES: int = 5 * 2**20
LOG_FILE_BACKUPS: int = 5
_MANAGED_ATTR = "_krx_alpha_managed"
_LISTENERS: list[QueueListener] = []

_format_ts = format_record_timestamp
_format_exc = format_record_exception

__all__ = [
    "ALERT_COOLDOWN_S",
    "ALERT_DAILY_CAP",
    "EVENT",
    "LOG_FILE_BACKUPS",
    "LOG_FILE_MAX_BYTES",
    "_ACTION_HINTS",
    "_REASON_LABELS",
    "_STAGE_LABELS",
    "EmailAlertHandler",
    "EventFileFilter",
    "JsonlEventFormatter",
    "KeyValueFormatter",
    "configure_logging",
    "format_alert_html",
    "format_alert_subject",
    "format_alert_text",
    "format_digest_html",
    "format_digest_text",
    "format_record_exception",
    "format_record_timestamp",
    "gmail_sender",
    "new_run_id",
    "send_digest",
    "shutdown_logging",
]


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
) -> str:
    """Install current process handlers and return a correlation run ID."""
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
        queue_handler.setLevel(logging.CRITICAL)
        setattr(queue_handler, _MANAGED_ATTR, True)
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
