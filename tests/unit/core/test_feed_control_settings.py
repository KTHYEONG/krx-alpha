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


def test_hot_symbols_must_name_a_configured_secondary_feed() -> None:
    import pytest

    from src.core.config import FeedControlSettings

    FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0",), hot_symbols=(("db:0", 5),))
    with pytest.raises(ValueError, match="not in secondary_feeds"):
        FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0",), hot_symbols=(("db:1", 5),))
    with pytest.raises(ValueError, match="non-negative"):
        FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0",), hot_symbols=(("db:0", -1),))
    with pytest.raises(ValueError, match="repeat"):
        FeedControlSettings(
            watch_groups=("ls:0",), secondary_feeds=("db:0",), hot_symbols=(("db:0", 1), ("db:0", 2))
        )


def test_hot_pairs_cannot_exceed_db_pair_capacity() -> None:
    import pytest

    from src.core.config import CollectorSettings, FeedControlSettings, resolve_collector_runtime

    collector = CollectorSettings()
    per_stream_cap = collector.db_capacity_pairs // len(collector.streams)
    feed_ok = FeedControlSettings(
        watch_groups=("ls:0",), secondary_feeds=("db:0",), hot_symbols=(("db:0", per_stream_cap),)
    )
    resolve_collector_runtime(collector=collector, feed_control=feed_ok)
    feed_over = FeedControlSettings(
        watch_groups=("ls:0",), secondary_feeds=("db:0",), hot_symbols=(("db:0", per_stream_cap + 1),)
    )
    with pytest.raises(ValueError, match="db_capacity_pairs"):
        resolve_collector_runtime(collector=collector, feed_control=feed_over)


def test_db_max_sessions_is_bounded_to_two_connections() -> None:
    import pytest
    from pydantic import ValidationError

    from src.core.config import CollectorSettings

    assert CollectorSettings().db_max_sessions == 2
    with pytest.raises(ValidationError):
        CollectorSettings(db_max_sessions=3)


def test_toss_settings_defaults_are_trade_only_and_bounded() -> None:
    from pydantic import ValidationError

    settings = CollectorSettings()
    assert settings.toss_streams == ("H0STCNT0",)
    assert (settings.toss_capacity_pairs, settings.toss_max_sessions) == (100, 2)
    for kwargs in ({"toss_max_sessions": 3}, {"toss_declare_spacing_s": 0.1}, {"toss_streams": ()},
                   {"toss_streams": ("H0NXCNT0",)}, {"toss_streams": ("H0STCNT0", "H0STCNT0")}):
        with pytest.raises(ValidationError):
            CollectorSettings(**kwargs)
    assert CollectorSettings(toss_streams=("H0STCNT0", "H0STASP0")).toss_streams == ("H0STCNT0", "H0STASP0")


def test_toss_hot_pairs_use_toss_streams_and_capacity() -> None:
    collector = CollectorSettings()
    ok = FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("toss:0",), hot_symbols=(("toss:0", 100),))
    resolve_collector_runtime(collector=collector, feed_control=ok)
    over = FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("toss:0",), hot_symbols=(("toss:0", 101),))
    with pytest.raises(ValueError, match="toss_capacity_pairs"):
        resolve_collector_runtime(collector=collector, feed_control=over)
    both = CollectorSettings(toss_streams=("H0STCNT0", "H0STASP0"))
    with pytest.raises(ValueError, match="toss_capacity_pairs"):
        resolve_collector_runtime(collector=both, feed_control=ok)
