"""Per-KST-day restart counters persisted so the EOD digest survives a process recreate."""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import logging
import os
import pathlib
from enum import StrEnum
from typing import Final

logger = logging.getLogger(__name__)

_VERSION: Final = 1


class DailyCounter(StrEnum):
    STREAMER_RESTARTS = "streamer_restarts"
    AFTERMARKET_RESTARTS = "aftermarket_restarts"
    SHARD_STALE_RESTARTS = "shard_stale_restarts"


def _zeros() -> dict[DailyCounter, int]:
    return dict.fromkeys(DailyCounter, 0)


def _parse(raw: str) -> tuple[dt.date, dict[DailyCounter, int]]:
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("version") != _VERSION:
        raise ValueError("unsupported version")
    day = dt.date.fromisoformat(payload["day"])
    raw_counts = payload["counts"]
    if not isinstance(raw_counts, dict):
        raise ValueError("counts is not an object")
    counts = _zeros()
    for name, value in raw_counts.items():
        if type(value) is not int or value < 0:
            raise ValueError("count is not a non-negative integer")
        counts[DailyCounter(name)] = value
    return day, counts


class DailyCounters:
    """Day-scoped restart counts backed by one JSON file; I/O failures are logged, never raised."""

    def __init__(self, path: pathlib.Path) -> None:
        self._path = path
        self._loaded = False
        self._day: dt.date | None = None
        self._counts = _zeros()
        self._warned: dict[str, dt.date] = {}

    def bump(self, counter: DailyCounter, day: dt.date) -> int:
        counter = DailyCounter(counter)
        self._ensure_loaded(day)
        if self._day != day:
            self._day = day
            self._counts = _zeros()
        self._counts[counter] += 1
        self._persist(day)
        return self._counts[counter]

    def snapshot(self, day: dt.date) -> dict[DailyCounter, int]:
        self._ensure_loaded(day)
        if self._day != day:
            return _zeros()
        return dict(self._counts)

    def _ensure_loaded(self, day: dt.date) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except (OSError, UnicodeDecodeError) as exc:
            self._warn("READ_FAIL", day, exc)
            return
        try:
            self._day, self._counts = _parse(raw)
        except (ValueError, TypeError, KeyError) as exc:
            self._warn("READ_FAIL", day, exc)

    def _persist(self, day: dt.date) -> None:
        payload = {
            "version": _VERSION,
            "day": day.isoformat(),
            "counts": {counter.value: n for counter, n in self._counts.items()},
        }
        tmp = self._path.with_name(f".{self._path.name}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
            os.replace(tmp, self._path)
        except OSError as exc:
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)
            self._warn("WRITE_FAIL", day, exc)

    def _warn(self, status: str, day: dt.date, exc: Exception) -> None:
        if self._warned.get(status) == day:
            return
        self._warned[status] = day
        logger.warning(
            "[SYS] stage=daily_counters status=%s reason=%s path=%s",
            status,
            type(exc).__name__,
            self._path,
            exc_info=True,
        )
