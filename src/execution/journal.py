"""주문 감사저널: 일자별 JSONL append-only 기록 (민감정보 마스킹)."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import os
import pathlib
from collections.abc import Callable
from typing import Any

from src.core.config import ExecutionMode

logger = logging.getLogger(__name__)

REDACT_KEYS: frozenset[str] = frozenset(
    {"cano", "appkey", "appsecret", "secretkey", "authorization", "access_token", "approval_key"}
)


def redact(value: Any) -> Any:
    """민감 키를 재귀 마스킹한다."""
    if isinstance(value, dict):
        return {k: ("***" if str(k).lower() in REDACT_KEYS else redact(v)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


class OrderJournal:
    """일자별 JSONL 감사로그 기록기."""

    def __init__(self, *, root: pathlib.Path, mode: ExecutionMode, now: Callable[[], dt.datetime]) -> None:
        self._root = root
        self._mode = mode
        self._now = now

    def append(self, event: str, **fields: Any) -> dict[str, Any]:
        ts = self._now()
        converted: dict[str, Any] = {}
        for key, value in fields.items():
            if dataclasses.is_dataclass(value) and not isinstance(value, type):
                converted[key] = redact(dataclasses.asdict(value))
            else:
                converted[key] = redact(value)
        record: dict[str, Any] = {"ts": ts.isoformat(), "mode": self._mode.value, "event": event, **converted}
        path = self._root / f"{ts.date().isoformat()}.jsonl"
        self._root.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        logger.debug("[EXEC] stage=journal event=%s ts=%s", event, record["ts"])
        return record
