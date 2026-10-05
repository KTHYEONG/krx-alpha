"""Host-wide single-issuer token store for rotating vendor credentials (Toss, LS)."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import logging
import os
import pathlib
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_POLL_INTERVAL_S = 0.01
_DEFAULT_LOCK_TIMEOUT_S = 30.0
_DEFAULT_EXPIRY_MARGIN_S = 600.0
# Peer writers (KCA) and vendors that omit expiry leave expires_at unset; such a token is trusted only this long,
# because a vendor-side expiry silently closes the socket instead of reporting auth rejection.
_DEFAULT_UNKNOWN_EXPIRY_MAX_AGE_S = 12 * 3600.0


@dataclass(frozen=True)
class IssuedToken:
    access_token: str
    expires_in_seconds: float | None


@dataclass(frozen=True)
class TokenRecord:
    access_token: str
    issued_at: dt.datetime
    expires_at: dt.datetime | None
    generation: int


class TokenStoreLockTimeout(RuntimeError):  # noqa: N818 - spec-mandated protocol name
    """The shared token lock was not obtained within the configured bound."""


def token_store_path(cache_dir: pathlib.Path, vendor: str, credential: str) -> pathlib.Path:
    """Return ``token_<vendor>_<sha12>.json`` under the shared cache directory."""
    if not vendor or not credential:
        raise ValueError("vendor and credential must be non-empty")
    digest = hashlib.sha256(credential.encode("utf-8")).hexdigest()[:12]
    return pathlib.Path(cache_dir) / f"token_{vendor}_{digest}.json"


def toss_token_path(cache_dir: pathlib.Path, app_key: str) -> pathlib.Path:
    """Return the shared Toss token store path for one client credential."""
    return token_store_path(cache_dir, "toss", app_key)


def ls_token_path(cache_dir: pathlib.Path, app_key: str) -> pathlib.Path:
    """Return the shared LS token store path for one client credential."""
    return token_store_path(cache_dir, "ls", app_key)


def _lock_path_for(path: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(f"{path}.lock")


def _parse_record(payload: object) -> TokenRecord | None:
    if not isinstance(payload, dict):
        return None
    if set(payload) != {"schema_version", "access_token", "issued_at", "expires_at", "generation"}:
        return None
    try:
        if payload.get("schema_version") != _SCHEMA_VERSION:
            return None
        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            return None
        issued_raw = payload.get("issued_at")
        expires_raw = payload.get("expires_at")
        generation = payload.get("generation")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
            return None
        if not isinstance(issued_raw, str):
            return None
        issued_at = dt.datetime.fromisoformat(issued_raw)
        if issued_at.tzinfo is None:
            return None
        expires_at: dt.datetime | None = None
        if expires_raw is not None:
            if not isinstance(expires_raw, str):
                return None
            expires_at = dt.datetime.fromisoformat(expires_raw)
            if expires_at.tzinfo is None:
                return None
        return TokenRecord(
            access_token=access_token, issued_at=issued_at, expires_at=expires_at, generation=generation
        )
    except (ValueError, TypeError):
        return None


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class TossTokenStore:
    """Host-wide single issuer for a shared rotating client credential.

    Toss invalidates the previous token at issuance, and KCA uses the same
    client. All issuance therefore goes through the shared protocol file under
    its lock, published with a generation. The synchronous methods serve
    thread-per-request callers; the ``a*`` variants serve the async LS adapter
    without blocking the event loop on the file lock.
    """

    def __init__(
        self,
        path: pathlib.Path,
        *,
        lock_timeout_s: float = _DEFAULT_LOCK_TIMEOUT_S,
        expiry_margin_s: float = _DEFAULT_EXPIRY_MARGIN_S,
        unknown_expiry_max_age_s: float = _DEFAULT_UNKNOWN_EXPIRY_MAX_AGE_S,
        clock: Callable[[], dt.datetime] = _utcnow,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if lock_timeout_s <= 0:
            raise ValueError("lock_timeout_s must be positive")
        if expiry_margin_s < 0:
            raise ValueError("expiry_margin_s must be non-negative")
        if unknown_expiry_max_age_s <= 0:
            raise ValueError("unknown_expiry_max_age_s must be positive")
        self._path = pathlib.Path(path)
        self._lock_timeout_s = float(lock_timeout_s)
        self._expiry_margin_s = float(expiry_margin_s)
        self._unknown_expiry_max_age = dt.timedelta(seconds=float(unknown_expiry_max_age_s))
        self._clock = clock
        self._sleep = sleep

    def _is_usable(self, record: TokenRecord, now: dt.datetime) -> bool:
        if record.expires_at is None:
            return now - record.issued_at < self._unknown_expiry_max_age
        return now < record.expires_at - dt.timedelta(seconds=self._expiry_margin_s)

    def read(self) -> TokenRecord | None:
        """Return the stored record, or None when absent or schema-invalid (logged, never raised)."""
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            logger.warning("[SYS] stage=shared_token status=UNREADABLE path=%s reason=%s", self._path.name, type(exc).__name__)
            return None
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, ValueError, TypeError):
            logger.warning("[SYS] stage=shared_token status=SCHEMA_INVALID path=%s", self._path.name)
            return None
        record = _parse_record(payload)
        if record is None:
            logger.warning("[SYS] stage=shared_token status=SCHEMA_INVALID path=%s", self._path.name)
            return None
        return record

    def _publish(self, record: TokenRecord) -> None:
        payload = {
            "schema_version": _SCHEMA_VERSION,
            "access_token": record.access_token,
            "issued_at": record.issued_at.isoformat(),
            "expires_at": record.expires_at.isoformat() if record.expires_at is not None else None,
            "generation": record.generation,
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=str(self._path.parent), prefix=".token_", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            os.chmod(tmp_name, 0o600)
            os.replace(tmp_name, self._path)
            os.chmod(str(self._path), 0o600)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise
        logger.info("[SYS] stage=shared_token status=PUBLISHED path=%s generation=%d", self._path.name, record.generation)

    def _build_record(self, issued: IssuedToken, previous: TokenRecord | None) -> TokenRecord:
        now = self._clock()
        expires_at: dt.datetime | None = None
        if issued.expires_in_seconds is not None:
            expires_at = now + dt.timedelta(seconds=float(issued.expires_in_seconds))
        generation = previous.generation + 1 if previous is not None else 1
        return TokenRecord(access_token=issued.access_token, issued_at=now, expires_at=expires_at, generation=generation)

    def _hold_lock(self) -> int:
        lock_path = _lock_path_for(self._path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.monotonic() + self._lock_timeout_s
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return fd
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TokenStoreLockTimeout(f"token lock not acquired: {lock_path}") from None
                    self._sleep(_POLL_INTERVAL_S)
        except BaseException:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            raise

    @staticmethod
    def _release_lock(fd: int) -> None:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    async def _ahold_lock(self) -> int:
        lock_path = _lock_path_for(self._path)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        deadline = time.monotonic() + self._lock_timeout_s
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return fd
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TokenStoreLockTimeout(f"token lock not acquired: {lock_path}") from None
                    await asyncio.sleep(_POLL_INTERVAL_S)
        except BaseException:
            self._release_lock(fd)
            raise

    def get_or_issue(self, issue: Callable[[], IssuedToken]) -> str:
        """Return a usable stored token, issuing exactly once host-wide when none is usable."""
        now = self._clock()
        cached = self.read()
        if cached is not None and self._is_usable(cached, now):
            return cached.access_token
        fd = self._hold_lock()
        try:
            now = self._clock()
            cached = self.read()
            if cached is not None and self._is_usable(cached, now):
                return cached.access_token
            record = self._build_record(issue(), cached)
            self._publish(record)
            return record.access_token
        finally:
            self._release_lock(fd)

    def replace_rejected(self, rejected_token: str, issue: Callable[[], IssuedToken]) -> str:
        """Adopt a peer-rotated token, or rotate once when the store still holds the rejected one."""
        fd = self._hold_lock()
        try:
            cached = self.read()
            if cached is not None and cached.access_token != rejected_token:
                return cached.access_token
            record = self._build_record(issue(), cached)
            self._publish(record)
            return record.access_token
        finally:
            self._release_lock(fd)

    async def aget_or_issue(self, issue: Callable[[], Awaitable[IssuedToken]]) -> str:
        """Async variant of :meth:`get_or_issue` for event-loop callers."""
        now = self._clock()
        cached = self.read()
        if cached is not None and self._is_usable(cached, now):
            return cached.access_token
        fd = await self._ahold_lock()
        try:
            now = self._clock()
            cached = self.read()
            if cached is not None and self._is_usable(cached, now):
                return cached.access_token
            record = self._build_record(await issue(), cached)
            self._publish(record)
            return record.access_token
        finally:
            self._release_lock(fd)

    async def areplace_rejected(self, rejected_token: str, issue: Callable[[], Awaitable[IssuedToken]]) -> str:
        """Async variant of :meth:`replace_rejected` for event-loop callers."""
        fd = await self._ahold_lock()
        try:
            cached = self.read()
            if cached is not None and cached.access_token != rejected_token:
                return cached.access_token
            record = self._build_record(await issue(), cached)
            self._publish(record)
            return record.access_token
        finally:
            self._release_lock(fd)


__all__ = [
    "IssuedToken",
    "TokenRecord",
    "TokenStoreLockTimeout",
    "TossTokenStore",
    "ls_token_path",
    "token_store_path",
    "toss_token_path",
]
