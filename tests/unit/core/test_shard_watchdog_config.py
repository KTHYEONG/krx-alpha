"""Invariant guards for KIS ack timeout and shard watchdog settings."""

from __future__ import annotations


def _clean_env(monkeypatch) -> None:
    import os

    for name in [n for n in os.environ if n.startswith("KRX_ALPHA_")]:
        monkeypatch.delenv(name, raising=False)


def test_defaults(monkeypatch) -> None:
    _clean_env(monkeypatch)
    from src.core.config import CollectorSettings, ShardWatchdogSettings

    assert CollectorSettings().kis_ack_timeout_s == 10.0
    wd = ShardWatchdogSettings()
    assert wd.aftermarket_stale_limit_s == 120.0
    assert wd.premarket_stale_limit_s == 120.0
    assert wd.max_restarts_per_day == 3
    assert wd.enabled is True


def test_env_override(monkeypatch) -> None:
    _clean_env(monkeypatch)
    from src.core.config import CollectorSettings, ShardWatchdogSettings

    monkeypatch.setenv("KRX_ALPHA_KIS_ACK_TIMEOUT_S", "3")
    monkeypatch.setenv("KRX_ALPHA_SHARD_WATCHDOG_MAX_RESTARTS_PER_DAY", "0")
    assert CollectorSettings().kis_ack_timeout_s == 3.0
    assert CollectorSettings().db_ack_timeout_s == 10.0
    assert ShardWatchdogSettings().max_restarts_per_day == 0


def test_non_positive_timeout_rejected() -> None:
    import pytest
    from pydantic import ValidationError

    from src.core.config import CollectorSettings

    with pytest.raises(ValidationError):
        CollectorSettings(kis_ack_timeout_s=0)


def test_limit_must_exceed_aftermarket_silence(monkeypatch) -> None:
    import pytest

    from src.core.config import (
        AftermarketSettings,
        ShardWatchdogSettings,
        resolve_collector_runtime,
    )

    _clean_env(monkeypatch)
    after = AftermarketSettings()
    wd = ShardWatchdogSettings(aftermarket_stale_limit_s=after.silence_limit_s)
    with pytest.raises(ValueError, match="aftermarket_stale_limit_s"):
        resolve_collector_runtime(aftermarket=after, shard_watchdog=wd)
    ok = ShardWatchdogSettings(aftermarket_stale_limit_s=after.silence_limit_s + 1)
    assert resolve_collector_runtime(aftermarket=after, shard_watchdog=ok).shard_watchdog is ok
    disabled = ShardWatchdogSettings(enabled=False, aftermarket_stale_limit_s=after.silence_limit_s)
    assert resolve_collector_runtime(aftermarket=after, shard_watchdog=disabled).shard_watchdog is disabled


def test_limit_must_exceed_premarket_silence(monkeypatch) -> None:
    import pytest

    from src.core.config import (
        PremarketSettings,
        ShardWatchdogSettings,
        resolve_collector_runtime,
    )

    _clean_env(monkeypatch)
    pre = PremarketSettings()
    wd = ShardWatchdogSettings(premarket_stale_limit_s=pre.silence_limit_s)
    with pytest.raises(ValueError, match="premarket_stale_limit_s"):
        resolve_collector_runtime(premarket=pre, shard_watchdog=wd)


def test_backward_compatibility(monkeypatch) -> None:
    _clean_env(monkeypatch)
    from src.core.config import CollectorSettings, resolve_collector_runtime

    runtime = resolve_collector_runtime(collector=CollectorSettings())
    assert runtime.shard_watchdog.aftermarket_stale_limit_s == 120.0
    assert runtime.collector.kis_ack_timeout_s == 10.0
