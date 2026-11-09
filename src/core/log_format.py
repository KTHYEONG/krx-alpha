"""Structured log formatting and event filtering for process logs."""

import datetime as dt
import json
import logging
import os
import re
import traceback
from typing import Final
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")
_KV_RE = re.compile(r"(\w+)=(\S+)")
_IDENTITY_VALUE_RE = re.compile(r"[A-Za-z0-9:_-]{1,24}")
PROCESS_IDENTITY_KEYS: Final[tuple[str, ...]] = ("vendor", "venue", "shard", "slot", "key_id")
_TAG_KEYS: Final[tuple[str, ...]] = ("venue", "shard", "slot")
_process_identity: dict[str, str] = {}


def format_record_timestamp(record: logging.LogRecord) -> str:
    """Return the current KST millisecond timestamp for a log record."""
    return dt.datetime.fromtimestamp(record.created, tz=_KST).isoformat(timespec="milliseconds")


def format_record_exception(record: logging.LogRecord) -> str | None:
    """Preserve the full exception traceback when present."""
    if record.exc_info:
        return "".join(traceback.format_exception(*record.exc_info))
    return record.exc_text or None


def set_process_identity(
    *, vendor: str, venue: str, shard: str, slot: str | None = None, key_id: str | None = None
) -> None:
    """Replace the current process identity; raises ValueError on a value outside the identifier class."""
    values = {"vendor": vendor, "venue": venue, "shard": shard, "slot": slot, "key_id": key_id}
    present = {key: value for key, value in values.items() if value is not None}
    for value in present.values():
        if not _IDENTITY_VALUE_RE.fullmatch(value):
            raise ValueError(f"process identity value {value!r} must match [A-Za-z0-9:_-]{{1,24}}")
    _process_identity.clear()
    _process_identity.update(present)


def clear_process_identity() -> None:
    _process_identity.clear()


def process_identity() -> dict[str, str]:
    return {key: _process_identity[key] for key in PROCESS_IDENTITY_KEYS if key in _process_identity}


def process_identity_tags() -> str:
    """Return the alert-dedup identity tags for message text (venue, shard, slot only)."""
    return "".join(f" {key}={_process_identity[key]}" for key in _TAG_KEYS if key in _process_identity)


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
            "pid": os.getpid(),
            "logger": record.name,
            "msg": msg,
            "fields": dict(_KV_RE.findall(msg)),
            "exc": format_record_exception(record),
        }
        if _process_identity:
            payload["ctx"] = process_identity()
        return json.dumps(payload, ensure_ascii=False)


class EventFileFilter(logging.Filter):
    """Retain WARNING+ records and explicitly marked INFO events."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.WARNING or bool(getattr(record, "krx_event", False))
