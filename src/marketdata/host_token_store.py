"""Adapters for the host-wide broker token contract."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections.abc import Awaitable, Callable

from deploy.shared.host_broker_contract import (
    DEFAULT_UNKNOWN_EXPIRY_MAX_AGE_S,
    AsyncTokenStore,
    IssuedToken,
    TokenIssueThrottled,
    TokenStore,
    TokenStoreLockTimeout,
    token_record_path,
)

_LOCK_TIMEOUT_S = 30.0
_EXPIRY_MARGIN_S = 600.0


class TokenStoreFailureError(RuntimeError):  # noqa: N818 - local public exception contract
    """A host token operation failed without exposing token material."""


STORE_FAILURE_TYPES: tuple[type[BaseException], ...] = (
    TokenStoreFailureError,
    TokenStoreLockTimeout,
    TokenIssueThrottled,
    OSError,
)


class _RotationCooldownError(RuntimeError):
    pass


class AsyncRotatingTokenStore:
    """Preserve legacy async string-token and rejected-token cooldown behavior."""

    def __init__(
        self,
        path: pathlib.Path,
        *,
        vendor: str,
        rotation_cooldown_s: float,
        clock: Callable[[], dt.datetime],
    ) -> None:
        if rotation_cooldown_s < 0:
            raise ValueError("rotation_cooldown_s must be non-negative")
        self._cooldown = dt.timedelta(seconds=float(rotation_cooldown_s))
        self._clock = clock
        self._store = AsyncTokenStore(
            path,
            vendor=vendor,
            lock_timeout_seconds=_LOCK_TIMEOUT_S,
            expiry_margin_seconds=_EXPIRY_MARGIN_S,
            clock=clock,
            issue_throttle_policy="raise",
            unknown_expiry_max_age_seconds=DEFAULT_UNKNOWN_EXPIRY_MAX_AGE_S,
        )

    async def aget_or_issue(self, issue: Callable[[], Awaitable[IssuedToken]]) -> str:
        try:
            return (await self._store.get_or_issue(issue)).access_token
        except (TokenStoreLockTimeout, TokenIssueThrottled, OSError) as exc:
            raise TokenStoreFailureError(type(exc).__name__) from exc

    async def areplace_rejected(
        self,
        rejected_token: str,
        issue: Callable[[], Awaitable[IssuedToken]],
    ) -> str:
        async def bounded_issue() -> IssuedToken:
            current = self._store.read()
            now = self._clock()
            if current is not None and current.access_token == rejected_token:
                age = now - current.issued_at
                if age < self._cooldown:
                    raise _RotationCooldownError
            return await issue()

        try:
            return (await self._store.replace_rejected(rejected_token, bounded_issue)).access_token
        except _RotationCooldownError:
            return rejected_token
        except (TokenStoreLockTimeout, TokenIssueThrottled, OSError) as exc:
            raise TokenStoreFailureError(type(exc).__name__) from exc


def make_kiwoom_store(
    directory: pathlib.Path,
    app_key: str,
    *,
    clock: Callable[[], dt.datetime],
) -> TokenStore:
    """Create a blocking Kiwoom store using the canonical host record path."""
    return TokenStore(
        token_record_path(directory, "kiwoom", app_key),
        vendor="kiwoom",
        lock_timeout_seconds=_LOCK_TIMEOUT_S,
        expiry_margin_seconds=_EXPIRY_MARGIN_S,
        clock=clock,
        issue_throttle_policy="raise",
        unknown_expiry_max_age_seconds=DEFAULT_UNKNOWN_EXPIRY_MAX_AGE_S,
    )


def kiwoom_issued(token: str) -> IssuedToken:
    return IssuedToken(token, None)


def store_failure_types() -> tuple[type[BaseException], ...]:
    return STORE_FAILURE_TYPES
