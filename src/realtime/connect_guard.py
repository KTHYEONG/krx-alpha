"""Cross-process websocket connect-rate guard for vendors that cap connects per minute per account."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import logging
import os
import pathlib
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterator

from src.core.observability import EVENT

logger = logging.getLogger(__name__)

_WAIT_MARGIN_S = 0.5
_WAIT_LOG_MIN_S = 1.0


class ConnectRateGuard:
    """Admits at most `max_connects` connects per `window_s` across every process sharing `path`."""

    def __init__(
        self,
        path: pathlib.Path,
        *,
        max_connects: int,
        window_s: float,
        vendor: str = "db",
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if max_connects < 1 or window_s <= 0:
            raise ValueError("max_connects must be >= 1 and window_s must be positive")
        self._path = pathlib.Path(path)
        self._lock_path = self._path.with_name(f"{self._path.name}.lock")
        self._max = max_connects
        self._window_s = window_s
        self._vendor = vendor
        self._clock = clock
        self._sleep = sleep

    async def wait_turn(self) -> None:
        """Record one connect once the window has room; the file lock is never held while sleeping."""
        while True:
            wait_s = self._try_enter()
            if wait_s <= 0:
                return
            if wait_s > _WAIT_LOG_MIN_S:
                logger.warning(
                    "[SYS] stage=connect_guard status=WAIT vendor=%s wait_s=%.1f", self._vendor, wait_s, extra=EVENT
                )
            await self._sleep(wait_s)

    def _try_enter(self) -> float:
        """Return 0.0 after recording a connect, else the seconds until the oldest entry ages out."""
        with self._locked():
            now = self._clock()
            recent = [stamp for stamp in self._load(now) if now - stamp < self._window_s]
            if len(recent) < self._max:
                recent.append(now)
                self._store(recent)
                return 0.0
            return min(recent) + self._window_s + _WAIT_MARGIN_S - now

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def _load(self, now: float) -> list[float]:
        try:
            raw = self._path.read_bytes()
        except FileNotFoundError:
            return []
        try:
            payload = json.loads(raw)
            if not isinstance(payload, list) or any(
                isinstance(stamp, bool) or not isinstance(stamp, (int, float)) for stamp in payload
            ):
                raise ValueError("connect guard file must be a list of numbers")
            return [float(stamp) for stamp in payload]
        except ValueError as exc:
            target = self._path.with_name(f"{self._path.name}.corrupt-{int(now)}")
            os.replace(self._path, target)
            logger.error(
                "[SYS] stage=connect_guard status=CORRUPT vendor=%s renamed=%s reason=%s",
                self._vendor,
                target.name,
                type(exc).__name__,
                extra=EVENT,
            )
            return []

    def _store(self, stamps: list[float]) -> None:
        fd, tmp_name = tempfile.mkstemp(dir=self._path.parent, prefix=f".{self._path.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(stamps))
            os.replace(tmp_name, self._path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise
