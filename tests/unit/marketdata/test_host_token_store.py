from __future__ import annotations

import asyncio
import datetime as dt

import pytest

from deploy.shared.host_broker_contract import IssuedToken
from deploy.shared.host_broker_contract import TokenIssueThrottled
from src.marketdata.host_token_store import AsyncRotatingTokenStore, TokenStoreFailureError


def test_shared_async_stores_issue_once(tmp_path) -> None:
    asyncio.run(_shared_async_stores_issue_once(tmp_path))


async def _shared_async_stores_issue_once(tmp_path) -> None:
    now = dt.datetime(2026, 11, 4, tzinfo=dt.UTC)
    stores = [
        AsyncRotatingTokenStore(tmp_path / "token.json", vendor="ls", rotation_cooldown_s=10, clock=lambda: now)
        for _ in range(2)
    ]
    calls = 0

    async def issue() -> IssuedToken:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return IssuedToken("shared-token", 3600)

    tokens = await asyncio.gather(*(store.aget_or_issue(issue) for store in stores))
    assert tokens == ["shared-token", "shared-token"]
    assert calls == 1


def test_rejection_cooldown_and_peer_replacement(tmp_path) -> None:
    asyncio.run(_rejection_cooldown_and_peer_replacement(tmp_path))


async def _rejection_cooldown_and_peer_replacement(tmp_path) -> None:
    now = dt.datetime(2026, 11, 4, tzinfo=dt.UTC)
    path = tmp_path / "token.json"
    first = AsyncRotatingTokenStore(path, vendor="ls", rotation_cooldown_s=60, clock=lambda: now)
    peer = AsyncRotatingTokenStore(path, vendor="ls", rotation_cooldown_s=0, clock=lambda: now)
    issued = 0

    async def issue() -> IssuedToken:
        nonlocal issued
        issued += 1
        return IssuedToken("initial", 3600)

    assert await first.aget_or_issue(issue) == "initial"
    assert await first.areplace_rejected("initial", issue) == "initial"
    assert issued == 1

    async def peer_issue() -> IssuedToken:
        return IssuedToken("peer-rotated", 3600)

    assert await peer.areplace_rejected("initial", peer_issue) == "peer-rotated"
    assert await first.areplace_rejected("initial", issue) == "peer-rotated"
    assert issued == 1


def test_invalid_cooldown_is_rejected(tmp_path) -> None:
    now = dt.datetime(2026, 11, 4, tzinfo=dt.UTC)
    with pytest.raises(ValueError, match="rotation_cooldown_s"):
        AsyncRotatingTokenStore(tmp_path / "token.json", vendor="ls", rotation_cooldown_s=-1, clock=lambda: now)


def test_canonical_issue_failures_are_wrapped(tmp_path) -> None:
    asyncio.run(_canonical_issue_failures_are_wrapped(tmp_path))


async def _canonical_issue_failures_are_wrapped(tmp_path) -> None:
    now = dt.datetime(2026, 11, 4, tzinfo=dt.UTC)
    store = AsyncRotatingTokenStore(tmp_path / "token.json", vendor="ls", rotation_cooldown_s=0, clock=lambda: now)

    async def fail(*args):
        raise TokenIssueThrottled("throttled")

    store._store.get_or_issue = fail
    with pytest.raises(TokenStoreFailureError, match="TokenIssueThrottled"):
        await store.aget_or_issue(lambda: _unused_issue())

    store._store.replace_rejected = fail
    with pytest.raises(TokenStoreFailureError, match="TokenIssueThrottled"):
        await store.areplace_rejected("rejected", lambda: _unused_issue())


def test_dbsec_rejection_obeys_shared_issue_guard(tmp_path) -> None:
    asyncio.run(_dbsec_rejection_obeys_shared_issue_guard(tmp_path))


async def _dbsec_rejection_obeys_shared_issue_guard(tmp_path) -> None:
    now = dt.datetime(2026, 11, 4, tzinfo=dt.UTC)
    store = AsyncRotatingTokenStore(
        tmp_path / "token.json", vendor="dbsec", rotation_cooldown_s=0, clock=lambda: now
    )
    calls = 0

    async def issue() -> IssuedToken:
        nonlocal calls
        calls += 1
        return IssuedToken("db-token", 3600)

    assert await store.aget_or_issue(issue) == "db-token"
    with pytest.raises(TokenStoreFailureError, match="TokenIssueThrottled"):
        await store.areplace_rejected("db-token", issue)
    assert calls == 1


def test_dbsec_issue_guard_survives_failed_first_issuer(tmp_path) -> None:
    asyncio.run(_dbsec_issue_guard_survives_failed_first_issuer(tmp_path))


async def _dbsec_issue_guard_survives_failed_first_issuer(tmp_path) -> None:
    now = dt.datetime(2026, 11, 4, tzinfo=dt.UTC)
    path = tmp_path / "token.json"
    first = AsyncRotatingTokenStore(path, vendor="dbsec", rotation_cooldown_s=0, clock=lambda: now)
    second = AsyncRotatingTokenStore(path, vendor="dbsec", rotation_cooldown_s=0, clock=lambda: now)
    first_calls = 0
    second_calls = 0

    async def fail_after_guard() -> IssuedToken:
        nonlocal first_calls
        first_calls += 1
        raise RuntimeError("fake issuer failure")

    async def second_issue() -> IssuedToken:
        nonlocal second_calls
        second_calls += 1
        return IssuedToken("unexpected", 3600)

    with pytest.raises(RuntimeError, match="fake issuer failure"):
        await first.aget_or_issue(fail_after_guard)
    with pytest.raises(TokenStoreFailureError, match="TokenIssueThrottled"):
        await second.aget_or_issue(second_issue)
    assert first_calls == 1
    assert second_calls == 0


def test_unknown_expiry_token_expires_at_canonical_age(tmp_path) -> None:
    asyncio.run(_unknown_expiry_token_expires_at_canonical_age(tmp_path))


async def _unknown_expiry_token_expires_at_canonical_age(tmp_path) -> None:
    now = [dt.datetime(2026, 11, 4, tzinfo=dt.UTC)]
    store = AsyncRotatingTokenStore(
        tmp_path / "token.json", vendor="ls", rotation_cooldown_s=0, clock=lambda: now[0]
    )
    calls = 0

    async def issue() -> IssuedToken:
        nonlocal calls
        calls += 1
        return IssuedToken(f"unknown-{calls}", None)

    assert await store.aget_or_issue(issue) == "unknown-1"
    now[0] += dt.timedelta(seconds=12 * 60 * 60)
    assert await store.aget_or_issue(issue) == "unknown-2"
    assert calls == 2


async def _unused_issue() -> IssuedToken:
    raise AssertionError("issuer must not run")
