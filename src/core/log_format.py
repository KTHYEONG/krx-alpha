"""Structured log formatting and event filtering for process logs."""

import datetime as dt
import json
import logging
import re
import traceback
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")
_KV_RE = re.compile(r"(\w+)=(\S+)")


def format_record_timestamp(record: logging.LogRecord) -> str:
    """Return the current KST millisecond timestamp for a log record."""
    return dt.datetime.fromtimestamp(record.created, tz=_KST).isoformat(timespec="milliseconds")


def format_record_exception(record: logging.LogRecord) -> str | None:
    """Preserve the full exception traceback when present."""
    if record.exc_info:
        return "".join(traceback.format_exception(*record.exc_info))
    return record.exc_text or None


class KeyValueFormatter(logging.Formatter):
    """Single-line key-value stream formatter with run correlation."""

    def __init__(self, *, component: str, run_id: str) -> None:
        super().__init__()
        self._component = component
        self._run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        ts = format_record_timestamp(record)
        base = f"{ts} level={record.levelname} comp={self._component} run={self._run_id} {record.getMessage()}"
        tb = format_record_exception(record)
        if tb is not None:
            base += " exc=" + json.dumps(tb, ensure_ascii=False)
        return base


class JsonlEventFormatter(logging.Formatter):
    """JSONL event formatter extracting key-value fields from messages."""

    def __init__(self, *, component: str, run_id: str) -> None:
        super().__init__()
        self._component = component
        self._run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        payload = {
            "ts": format_record_timestamp(record),
            "level": record.levelname,
            "component": self._component,
            "run_id": self._run_id,
            "logger": record.name,
            "msg": msg,
            "fields": dict(_KV_RE.findall(msg)),
            "exc": format_record_exception(record),
        }
        return json.dumps(payload, ensure_ascii=False)


class EventFileFilter(logging.Filter):
    """Retain WARNING+ records and explicitly marked INFO events."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING or bool(getattr(record, "krx_event", False))
