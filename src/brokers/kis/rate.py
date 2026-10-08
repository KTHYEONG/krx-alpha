"""KIS REST request spacing (per-key rate limiter + host-wide pacing)."""

from __future__ import annotations

import fcntl
import hashlib
import logging
import os
import pathlib
import re
import threading
import time
from collections.abc import Callable
from typing import Protocol

from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

KIS_REST_RATE_PER_S: float = 18.0
"""Vendor contract rate shared by every host consumer of one KIS pacing file."""

HOST_ADMISSION_MARKER: str = ".host-admission"
"""Empty host-provisioned file asserting the admission directory is host-shared."""

_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class Pacer(Protocol):
    """Blocking admission gate passed before every vendor REST request."""

    def acquire(self) -> None: ...


class RateLimiter:
    """Process-private minimum-interval pacer for isolated environments."""

    def __init__(
        self,
        rate_per_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = 1.0 / rate_per_s
        self._clock = clock
        self._sleep = sleep
        self._next_allowed = float("-inf")

    def acquire(self) -> None:
        """Wait until this configured KIS key may send its next REST request."""
        now = self._clock()
        if now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._next_allowed
        self._next_allowed = now + self._interval


class HostPacingNotSharedError(KrxAlphaError):
    """The pacing directory is not the host-shared one, so pacing would be process-private."""


def _in_container() -> bool:
    return os.path.exists("/.dockerenv")


def host_state_path(cache_dir: pathlib.Path, vendor: str, credential: str, scope: str | None = None) -> pathlib.Path:
    """Return the protocol pacing path under the shared cache directory.

    Args:
        cache_dir: Host-shared directory (the KIS token cache mount).
        vendor: Vendor contract name matching ``[A-Za-z0-9_-]+``.
        credential: Raw credential the digest is derived from (never persisted).
        scope: Optional vendor scope (e.g. a Toss rate group) with the same charset.

    Raises:
        ValueError: If vendor/scope charset is violated or credential is empty.
    """
    if not _NAME_RE.match(vendor):
        raise ValueError(f"invalid vendor: {vendor!r}")
    if not credential:
        raise ValueError("credential must be non-empty")
    if scope is not None and not _NAME_RE.match(scope):
        raise ValueError(f"invalid scope: {scope!r}")
    digest = hashlib.sha256(credential.encode("utf-8")).hexdigest()[:12]
    name = f"admission_{vendor}_{digest}.state" if scope is None else f"admission_{vendor}_{digest}_{scope}.state"
    return pathlib.Path(cache_dir) / name


def kis_state_path(cache_dir: pathlib.Path, app_key: str) -> pathlib.Path:
    """Return the legacy KIS pacing path unchanged so running processes stay compatible."""
    if not app_key:
        raise ValueError("app_key must be non-empty")
    digest = hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:12]
    return pathlib.Path(cache_dir) / f"tps_{digest}.state"


class HostPacedRateLimiter:
    """Synchronous host-wide pacing over the shared protocol state file.

    Args:
        state_path: Protocol state file shared with every host consumer of the scope.
        rate_per_s: Vendor contract rate; must equal the other consumers' rate.
        max_lead_s: Reservation lead bound; None for critical callers.
        clock: Wall-clock seconds (time.time; the file stores epoch seconds).
        sleep: Blocking sleep.

    Raises:
        ValueError: Non-positive rate or lead.
        HostPacingNotSharedError: Inside a container without the host marker file.
    """

    def __init__(
        self,
        state_path: pathlib.Path,
        rate_per_s: float,
        max_lead_s: float | None = None,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_s <= 0:
            raise ValueError("rate_per_s must be positive")
        if max_lead_s is not None and max_lead_s <= 0:
            raise ValueError("max_lead_s must be positive")
        resolved = pathlib.Path(state_path)
        if _in_container() and not (resolved.parent / HOST_ADMISSION_MARKER).is_file():
            raise HostPacingNotSharedError(f"pacing dir is not host-shared: {resolved.parent}")
        self._state_path = resolved
        self._interval = 1.0 / float(rate_per_s)
        self._max_lead_s = max_lead_s
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()

    def _read_next_free_locked(self, fd: int) -> float:
        raw = os.read(fd, 64).decode("ascii", errors="replace").strip()
        if not raw:
            return 0.0
        try:
            return float(raw)
        except ValueError:
            logger.warning("[SYS] stage=kis_rate_limit status=STATE_RESET path=%s", self._state_path)
            return 0.0

    def _book_unconditional(self) -> float:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self._state_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                os.fchmod(fd, 0o600)
                next_free = self._read_next_free_locked(fd)
                now = self._clock()
                slot = max(now, next_free)
                os.lseek(fd, 0, os.SEEK_SET)
                os.ftruncate(fd, 0)
                os.write(fd, repr(slot + self._interval).encode("ascii"))
                return slot - now
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _try_book_or_wait(self) -> tuple[bool, float]:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self._state_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                os.fchmod(fd, 0o600)
                next_free = self._read_next_free_locked(fd)
                now = self._clock()
                lead = self._max_lead_s
                assert lead is not None
                slot = max(now, next_free)
                if slot - now <= lead:
                    os.lseek(fd, 0, os.SEEK_SET)
                    os.ftruncate(fd, 0)
                    os.write(fd, repr(slot + self._interval).encode("ascii"))
                    return True, slot - now
                return False, max(slot - now - lead, self._interval)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def acquire(self) -> None:
        """Block until this process owns the next host-wide slot for the scope."""
        if self._max_lead_s is None:
            with self._lock:
                delay = self._book_unconditional()
            if delay > 0:
                self._sleep(delay)
            return
        while True:
            with self._lock:
                booked, wait = self._try_book_or_wait()
            if wait > 0:
                self._sleep(wait)
            if booked:
                return
