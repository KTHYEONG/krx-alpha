from __future__ import annotations

import os

import pytest

from src.core.config import (
    CollectorSettings,
    FeedControlSettings,
    IntradayAdmissionSettings,
    resolve_collector_runtime,
)


@pytest.fixture(autouse=True)
def _hermetic_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith("KRX_ALPHA_FEED_CONTROL_") or name.startswith("KRX_ALPHA_INTRADAY_"):
            monkeypatch.delenv(name, raising=False)


def test_defaults_are_inert() -> None:
    settings = FeedControlSettings()
    assert settings.watch_groups == ()
    assert settings.secondary_feeds == ()
    runtime = resolve_collector_runtime()
    assert runtime.feed_control == FeedControlSettings()


def test_secondary_requires_watch_groups() -> None:
    with pytest.raises(ValueError, match="requires non-empty watch_groups"):
        FeedControlSettings(secondary_feeds=("db:0",))


def test_validators_reject_bad_combinations() -> None:
    with pytest.raises(ValueError, match="failback_hold_s must exceed failover_after_s"):
        FeedControlSettings(watch_groups=("ls:0",), failback_hold_s=30.0, failover_after_s=30.0)
    with pytest.raises(ValueError, match="not supported feed ids"):
        FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("xx:9",))
    with pytest.raises(ValueError, match="demote_degraded_s must exceed failback_hold_s"):
        FeedControlSettings(watch_groups=("ls:0",), demote_degraded_s=600.0, failback_hold_s=600.0)
    with pytest.raises(ValueError, match="must be ls:<n>"):
        FeedControlSettings(watch_groups=("ls-main:0",))


def test_rejects_duplicate_groups_and_inverted_page_window() -> None:
    with pytest.raises(ValueError, match="must not contain duplicates"):
        FeedControlSettings(watch_groups=("ls:0", "ls:0"))
    with pytest.raises(ValueError, match="page_after_s must exceed failover_after_s"):
        FeedControlSettings(watch_groups=("ls:0",), page_after_s=30.0, failover_after_s=30.0)


def test_kis_secondary_conflicts_with_intraday_admission() -> None:
    feed_control = FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("kis:5",))
    with pytest.raises(ValueError, match="conflicts with intraday admission"):
        resolve_collector_runtime(feed_control=feed_control, intraday=IntradayAdmissionSettings(enabled=True))


def test_ls_silence_matches_collector_limit() -> None:
    assert FeedControlSettings().ls_silence_s == CollectorSettings().regular_silence_limit_s


def test_quality_floor_bounds_are_validated() -> None:
    from pydantic import ValidationError

    assert FeedControlSettings().quality_floor_availability == 0.995
    assert FeedControlSettings().quality_floor_days == 3
    with pytest.raises(ValidationError):
        FeedControlSettings(quality_floor_availability=1.5)
    with pytest.raises(ValidationError):
        FeedControlSettings(quality_floor_days=0)
