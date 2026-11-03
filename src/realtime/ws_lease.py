"""Host-wide WebSocket connection-slot ledger shared by every collector process on one credential."""

from __future__ import annotations

import contextlib
import dataclasses
import fcntl
import json
import logging
import os
import pathlib
import re
import secrets
import tempfile
import time
from collections.abc import Callable, Iterator
from enum import StrEnum
from typing import TypeGuard

from src.core.errors import KrxAlphaError
from src.core.observability import EVENT

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]+")


class LeaseRole(StrEnum):
    PRIMARY = "primary"
    STANDBY = "standby"


@dataclasses.dataclass(frozen=True)
class Lease:
    """One granted connection slot; the token is the only proof of ownership and is hidden from repr."""

    vendor: str
    key_id: str
    owner: str
    role: LeaseRole
    token: str = dataclasses.field(repr=False)
    expires_ns: int


class LeaseUnavailable(KrxAlphaError):  # noqa: N818 - spec-fixed name
    """No connection slot can be granted without exceeding the vendor cap."""


@dataclasses.dataclass
class _Entry:
    owner: str
    role: LeaseRole
    token: str = dataclasses.field(repr=False)
    expires_ns: int
    granted_ns: int
    preempt: bool = False


class WsLeaseLedger:
    """Cross-process slot ledger for one ``(vendor, key_id)`` cap; every call is one locked read-modify-write."""

    def __init__(
        self,
        directory: pathlib.Path,
        *,
        vendor: str,
        key_id: str,
        max_connections: int,
        ttl_s: float,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        if _IDENTIFIER.fullmatch(vendor) is None or _IDENTIFIER.fullmatch(key_id) is None:
            raise ValueError("vendor and key_id must match [A-Za-z0-9_-]+")
        ttl_ns = int(ttl_s * 1e9)
        if max_connections < 1 or ttl_ns <= 0:
            raise ValueError("max_connections must be >= 1 and ttl_s must be a positive duration")
        stem = f"wslease_{vendor}_{key_id}"
        self._dir = pathlib.Path(directory)
        self._path = self._dir / f"{stem}.json"
        self._lock_path = self._dir / f"{stem}.lock"
        self._vendor = vendor
        self._key_id = key_id
        self._cap = max_connections
        self._ttl_ns = ttl_ns
        self._clock_ns = clock_ns
        # Read and written only inside the file lock; carries the post-corruption standby block across processes.
        self._standby_blocked_until_ns = 0

    def acquire(self, owner: str, role: LeaseRole) -> Lease:
        """Grant one slot or raise LeaseUnavailable; a PRIMARY request on a full ledger marks the oldest STANDBY."""
        requested = LeaseRole(role)
        with self._locked():
            now = self._clock_ns()
            entries = self._drop_expired(self._load(), now)
            if requested == LeaseRole.STANDBY and now < self._standby_blocked_until_ns:
                self._store(entries)
                self._log("DENY", owner, requested, len(entries), logging.WARNING)
                raise LeaseUnavailable("quarantine")
            if len(entries) < self._cap:
                entry = _Entry(owner, requested, secrets.token_hex(16), now + self._ttl_ns, now)
                entries.append(entry)
                self._store(entries)
                self._log("GRANT", owner, requested, len(entries))
                return self._lease(entry)
            standbys = [entry for entry in entries if entry.role == LeaseRole.STANDBY]
            if requested == LeaseRole.PRIMARY and standbys:
                if any(entry.preempt for entry in standbys):
                    self._store(entries)
                    self._log("DENY", owner, requested, len(entries), logging.WARNING)
                    raise LeaseUnavailable("preempt_pending")
                victim = min(standbys, key=lambda entry: entry.granted_ns)
                victim.preempt = True
                self._store(entries)
                self._log("PREEMPT", victim.owner, victim.role, len(entries), logging.WARNING)
                raise LeaseUnavailable("preempt_pending")
            self._store(entries)
            self._log("DENY", owner, requested, len(entries), logging.WARNING)
            raise LeaseUnavailable("cap_reached")

    def renew(self, lease: Lease) -> bool:
        """Extend a live, non-preempted lease; False means it is gone and the caller must close the connection."""
        with self._locked():
            now = self._clock_ns()
            entries = self._drop_expired(self._load(), now)
            entry = _find(entries, lease.token)
            if entry is None or entry.preempt:
                self._store(entries)
                return False
            entry.expires_ns = now + self._ttl_ns
            self._store(entries)
            return True

    def release(self, lease: Lease) -> None:
        """Remove the lease's slot; releasing an absent or expired lease is a no-op."""
        with self._locked():
            now = self._clock_ns()
            entries = self._drop_expired(self._load(), now)
            entry = _find(entries, lease.token)
            if entry is not None:
                entries = [other for other in entries if other is not entry]
            self._store(entries)
            if entry is not None:
                self._log("RELEASE", entry.owner, entry.role, len(entries))

    def promote(self, lease: Lease) -> Lease:
        """Turn a live, non-preempted STANDBY lease into PRIMARY in place; the live count is unchanged."""
        with self._locked():
            now = self._clock_ns()
            entries = self._drop_expired(self._load(), now)
            entry = _find(entries, lease.token)
            if entry is None or entry.preempt or entry.role != LeaseRole.STANDBY:
                self._store(entries)
                raise LeaseUnavailable("standby_not_promotable")
            entry.role = LeaseRole.PRIMARY
            entry.expires_ns = now + self._ttl_ns
            self._store(entries)
            self._log("GRANT", entry.owner, entry.role, len(entries))
            return self._lease(entry)

    def _lease(self, entry: _Entry) -> Lease:
        return Lease(self._vendor, self._key_id, entry.owner, entry.role, entry.token, entry.expires_ns)

    def _log(
        self,
        action: str,
        owner: str,
        role: LeaseRole,
        live: int,
        level: int = logging.INFO,
    ) -> None:
        logger.log(
            level,
            "[SYS] stage=ws_lease vendor=%s key=%s owner=%s role=%s action=%s live=%d cap=%d",
            self._vendor,
            self._key_id,
            owner,
            role.value,
            action,
            live,
            self._cap,
            extra=EVENT,
        )

    def _drop_expired(self, entries: list[_Entry], now: int) -> list[_Entry]:
        live = [entry for entry in entries if entry.expires_ns > now]
        for entry in entries:
            if entry.expires_ns <= now:
                self._log("EXPIRE", entry.owner, entry.role, len(live))
        return live

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError as exc:
            raise LeaseUnavailable("ledger_unavailable") from exc
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
            except OSError as exc:
                raise LeaseUnavailable("ledger_unavailable") from exc
            yield
        finally:
            os.close(fd)

    def _load(self) -> list[_Entry]:
        self._standby_blocked_until_ns = 0
        try:
            raw = self._path.read_bytes()
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise LeaseUnavailable("ledger_unavailable") from exc
        try:
            payload = json.loads(raw)
            entries = _parse_entries(payload)
            self._standby_blocked_until_ns = _parse_blocked_until(payload)
            return entries
        except (ValueError, TypeError) as exc:
            self._quarantine(exc)
            raise LeaseUnavailable("ledger_corrupt") from exc

    def _quarantine(self, cause: Exception) -> None:
        target = self._path.with_name(f".{self._path.name}.corrupt-{time.time_ns()}")
        try:
            os.replace(self._path, target)
            renamed = target.name
        except OSError:
            renamed = "failed"
        logger.critical(
            "[SYS] stage=ws_lease status=CORRUPT vendor=%s key=%s renamed=%s reason=%s",
            self._vendor,
            self._key_id,
            renamed,
            type(cause).__name__,
            exc_info=cause,
            extra=EVENT,
        )
        # Live holders are unknown until each renews (every ttl/3) and re-acquires; until then only PRIMARY may enter.
        self._standby_blocked_until_ns = self._clock_ns() + self._ttl_ns
        with contextlib.suppress(LeaseUnavailable):
            self._store([])

    def _store(self, entries: list[_Entry]) -> None:
        payload: dict[str, object] = {
            "schema_version": _SCHEMA_VERSION,
            "leases": [_entry_json(entry) for entry in entries],
        }
        if self._standby_blocked_until_ns > self._clock_ns():
            payload["standby_blocked_until_ns"] = self._standby_blocked_until_ns
        body = json.dumps(payload, sort_keys=True)
        try:
            fd, tmp_name = tempfile.mkstemp(dir=self._dir, prefix=f".{self._path.name}.", suffix=".tmp")
        except OSError as exc:
            raise LeaseUnavailable("ledger_unwritable") from exc
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self._path)
        except OSError as exc:
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise LeaseUnavailable("ledger_unwritable") from exc


def _find(entries: list[_Entry], token: str) -> _Entry | None:
    return next((entry for entry in entries if entry.token == token), None)


def _is_int(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_entries(payload: object) -> list[_Entry]:
    if not isinstance(payload, dict) or payload.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("unsupported ledger schema")
    leases = payload.get("leases")
    if not isinstance(leases, list):
        raise ValueError("ledger leases must be a list")
    return [_parse_entry(item) for item in leases]


def _parse_blocked_until(payload: dict[str, object]) -> int:
    value = payload.get("standby_blocked_until_ns", 0)
    if not _is_int(value):
        raise ValueError("standby_blocked_until_ns must be an integer")
    return value


def _parse_entry(item: object) -> _Entry:
    if not isinstance(item, dict):
        raise ValueError("ledger entry must be an object")
    owner = item.get("owner")
    role = item.get("role")
    token = item.get("token")
    preempt = item.get("preempt")
    expires_ns = item.get("expires_ns")
    granted_ns = item.get("granted_ns")
    if (
        not isinstance(owner, str)
        or not isinstance(role, str)
        or not isinstance(token, str)
        or not isinstance(preempt, bool)
        or not _is_int(expires_ns)
        or not _is_int(granted_ns)
    ):
        raise ValueError("ledger entry field has invalid type")
    return _Entry(
        owner=owner,
        role=LeaseRole(role),
        token=token,
        expires_ns=expires_ns,
        granted_ns=granted_ns,
        preempt=preempt,
    )


def _entry_json(entry: _Entry) -> dict[str, object]:
    return {
        "owner": entry.owner,
        "role": entry.role.value,
        "token": entry.token,
        "expires_ns": entry.expires_ns,
        "granted_ns": entry.granted_ns,
        "preempt": entry.preempt,
    }
