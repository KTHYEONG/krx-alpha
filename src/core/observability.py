"""관측 가능성 기반: 컴포넌트별 로깅·영속 JSONL 이벤트·CRITICAL 이메일 알림."""

import datetime as dt
import json
import logging
import pathlib
import queue
import re
import smtplib
import sys
import time
import traceback
from collections.abc import Callable
from email.message import EmailMessage
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from typing import IO
from zoneinfo import ZoneInfo

from src.core.config import AlertSettings, ObservabilitySettings, export_run_id

EVENT: dict[str, bool] = {"krx_event": True}
LOG_FILE_MAX_BYTES: int = 5 * 2**20
LOG_FILE_BACKUPS: int = 5
ALERT_COOLDOWN_S: float = 1800.0
ALERT_DAILY_CAP: int = 20
_MANAGED_ATTR = "_krx_alpha_managed"
_KV_RE = re.compile(r"(\w+)=(\S+)")
_KST = ZoneInfo("Asia/Seoul")
_LISTENERS: list[QueueListener] = []


def new_run_id(component: str, *, now_ns: int | None = None) -> str:
    ns = time.time_ns() if now_ns is None else now_ns
    return f"{component}-{ns // 1_000_000}"


def _format_ts(record: logging.LogRecord) -> str:
    return dt.datetime.fromtimestamp(record.created, tz=_KST).isoformat(timespec="milliseconds")


def _format_exc(record: logging.LogRecord) -> str | None:
    if record.exc_info:
        return "".join(traceback.format_exception(*record.exc_info))
    return record.exc_text or None


class KeyValueFormatter(logging.Formatter):
    def __init__(self, *, component: str, run_id: str) -> None:
        super().__init__()
        self._component = component
        self._run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        ts = _format_ts(record)
        base = f"{ts} level={record.levelname} comp={self._component} run={self._run_id} {record.getMessage()}"
        tb = _format_exc(record)
        if tb is not None:
            base += " exc=" + json.dumps(tb, ensure_ascii=False)
        return base


class JsonlEventFormatter(logging.Formatter):
    def __init__(self, *, component: str, run_id: str) -> None:
        super().__init__()
        self._component = component
        self._run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        payload = {
            "ts": _format_ts(record),
            "level": record.levelname,
            "component": self._component,
            "run_id": self._run_id,
            "logger": record.name,
            "msg": msg,
            "fields": dict(_KV_RE.findall(msg)),
            "exc": _format_exc(record),
        }
        return json.dumps(payload, ensure_ascii=False)


class EventFileFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING or bool(getattr(record, "krx_event", False))


class EmailAlertHandler(logging.Handler):
    def __init__(
        self,
        *,
        component: str,
        run_id: str,
        sender: Callable[[str, str], None],
        cooldown_s: float = ALERT_COOLDOWN_S,
        daily_cap: int = ALERT_DAILY_CAP,
        clock: Callable[[], float] = time.monotonic,
        today: Callable[[], dt.date] | None = None,
    ) -> None:
        super().__init__(level=logging.CRITICAL)
        self._component = component
        self._run_id = run_id
        self._sender = sender
        self._cooldown_s = cooldown_s
        self._daily_cap = daily_cap
        self._clock = clock
        self._today = today if today is not None else (lambda: dt.datetime.now(_KST).date())
        self._sent_today = 0
        self._current_day: dt.date | None = None
        self._last_sent: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        key = str(record.getMessage())[:80]
        day = self._today()
        if self._current_day is None:
            self._current_day = day
        if day != self._current_day:
            self._current_day = day
            self._sent_today = 0
            self._last_sent.clear()
        if self._sent_today >= self._daily_cap:
            return
        now = self._clock()
        last = self._last_sent.get(key)
        if last is not None and (now - last) < self._cooldown_s:
            return
        subject = f"[krx-alpha] CRITICAL {self._component}: {record.getMessage()[:80]}"
        body = JsonlEventFormatter(component=self._component, run_id=self._run_id).format(record)
        try:
            self._sender(subject, body)
        except (smtplib.SMTPException, OSError) as exc:
            logging.getLogger(__name__).warning("[SYS] stage=alert status=FAIL reason=%s", type(exc).__name__)
            return
        self._last_sent[key] = now
        self._sent_today += 1


def gmail_sender(settings: AlertSettings) -> Callable[[str, str], None]:
    def send(subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = settings.alert_gmail_user
        msg["To"] = settings.alert_gmail_to
        msg.set_content(body)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as smtp:
            smtp.login(settings.alert_gmail_user, settings.alert_gmail_app_password)
            smtp.send_message(msg)

    return send


def configure_logging(
    component: str,
    *,
    log_dir: pathlib.Path | None = None,
    level: int | str = logging.INFO,
    stream: IO[str] | None = None,
    alert_settings: AlertSettings | None = None,
    alert_sender: Callable[[str, str], None] | None = None,
) -> str:
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
    for listener in _LISTENERS:
        listener.stop()
    _LISTENERS.clear()
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _MANAGED_ATTR, False):
            root.removeHandler(handler)
            handler.close()
