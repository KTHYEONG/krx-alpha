import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src.core.config import SnapshotSettings
from src.marketdata.snapshot_contracts import (
    COMMON_SNAPSHOT_COLUMNS,
    SNAPSHOT_DEDUP_KEYS,
    SNAPSHOT_SCHEMAS,
    SnapshotDataset,
    SnapshotJobKind,
    build_session_jobs,
    partition_due_jobs,
)

_KST = ZoneInfo("Asia/Seoul")
_SESSION_DATE = dt.date(2026, 9, 17)


def _settings() -> SnapshotSettings:
    return SnapshotSettings()


def test_schemas_share_common_columns_and_cover_dedup_keys() -> None:
    common = list(COMMON_SNAPSHOT_COLUMNS)
    for dataset in SnapshotDataset:
        schema = SNAPSHOT_SCHEMAS[dataset]
        assert list(schema)[:4] == common
        for key in SNAPSHOT_DEDUP_KEYS[dataset]:
            assert key in schema


def test_build_session_jobs_expands_counts_per_kind() -> None:
    from collections import Counter

    jobs = build_session_jobs(_settings(), _SESSION_DATE)

    counts = Counter(j.kind for j in jobs)
    assert counts[SnapshotJobKind.RANKING] == 390
    assert counts[SnapshotJobKind.INDEX_SNAPSHOT] == 78
    assert counts[SnapshotJobKind.INDEX_MINUTE_BAR] == 5
    assert counts[SnapshotJobKind.AUCTION_OPEN] == 5
    assert counts[SnapshotJobKind.AUCTION_CLOSE] == 4
    assert counts[SnapshotJobKind.INVESTOR_ESTIMATE] == 6
    assert counts[SnapshotJobKind.PROGRAM_TRADE] == 14
    assert counts[SnapshotJobKind.EOD_MINUTE_BARS] == 1
    assert counts[SnapshotJobKind.NEWS_TITLE] == 918


def test_build_session_jobs_not_after_is_next_same_kind_or_deadline() -> None:
    settings = _settings()
    jobs = build_session_jobs(settings, _SESSION_DATE)
    by_id = {j.job_id: j for j in jobs}

    assert by_id["auction_open@085930"].not_after == dt.datetime(2026, 9, 17, 9, 0, tzinfo=_KST)
    assert by_id["ranking@090000"].not_after == dt.datetime(2026, 9, 17, 9, 1, tzinfo=_KST)
    news_jobs = [j for j in jobs if j.kind is SnapshotJobKind.NEWS_TITLE]
    assert news_jobs[-1].not_after == dt.datetime(2026, 9, 17, 15, 39, tzinfo=_KST)
    for job in jobs:
        assert job.due_at < job.not_after
        assert job.due_at.tzinfo is not None


def test_build_session_jobs_orders_same_instant_by_kind_priority() -> None:
    jobs = build_session_jobs(_settings(), _SESSION_DATE)

    at_eod = [j for j in jobs if j.due_at == dt.datetime(2026, 9, 17, 15, 35, tzinfo=_KST)]
    kinds = [j.kind for j in at_eod]

    assert kinds.index(SnapshotJobKind.INVESTOR_ESTIMATE) < kinds.index(SnapshotJobKind.PROGRAM_TRADE)
    assert kinds.index(SnapshotJobKind.PROGRAM_TRADE) < kinds.index(SnapshotJobKind.EOD_MINUTE_BARS)
    assert kinds == sorted(kinds, key=lambda k: list(SnapshotJobKind).index(k))
    assert jobs == build_session_jobs(_settings(), _SESSION_DATE)


def test_build_session_jobs_rejects_collapsed_or_duplicate_windows() -> None:
    settings = _settings()

    with pytest.raises(ValueError, match="due_at < not_after"):
        build_session_jobs(
            settings.model_copy(update={"investor_estimate_times": (dt.time(9, 35), settings.eod_collect_time)}),
            _SESSION_DATE,
        )
    with pytest.raises(ValueError, match="duplicate job_id"):
        build_session_jobs(
            settings.model_copy(
                update={
                    "investor_estimate_times": (
                        dt.time(9, 35, 0, 1),
                        dt.time(9, 35, 0, 2),
                        dt.time(10, 5),
                        dt.time(11, 25),
                        dt.time(13, 25),
                        dt.time(14, 35),
                    )
                }
            ),
            _SESSION_DATE,
        )


def test_partition_due_jobs_separates_expired_from_due() -> None:
    jobs = build_session_jobs(_settings(), _SESSION_DATE)
    now = dt.datetime(2026, 9, 17, 10, 0, 10, tzinfo=_KST)

    due, expired = partition_due_jobs(jobs, completed=set(), now=now)

    due_ids = {j.job_id for j in due}
    assert {"ranking@100000", "index_snapshot@100000", "news_title@100000"} <= due_ids
    expired_ids = {j.job_id for j in expired}
    assert "ranking@095900" in expired_ids
    future_ids = {j.job_id for j in jobs if j.due_at > now}
    assert not (set(due_ids) & future_ids)
    assert not (set(expired_ids) & future_ids)
    assert [j.job_id for j in due] == [j.job_id for j in jobs if j.job_id in due_ids]
    completed_due, _ = partition_due_jobs(jobs, completed={"ranking@100000"}, now=now)
    assert "ranking@100000" not in {j.job_id for j in completed_due}


def test_partition_due_jobs_rejects_naive_now() -> None:
    jobs = build_session_jobs(_settings(), _SESSION_DATE)

    with pytest.raises(ValueError, match="tz-aware"):
        partition_due_jobs(jobs, completed=set(), now=dt.datetime(2026, 9, 17, 10, 0, 10))


def _aware_at(hour: int, minute: int) -> dt.datetime:
    return dt.datetime(2026, 9, 17, hour, minute, tzinfo=_KST)


def test_coverage_series_always_ends_exactly_at_boundary() -> None:
    from src.marketdata.snapshot_contracts import _coverage_series

    result = _coverage_series(_aware_at(9, 0), 4800, _aware_at(15, 30))

    assert result == [
        _aware_at(10, 20),
        _aware_at(11, 40),
        _aware_at(13, 0),
        _aware_at(14, 20),
        _aware_at(15, 30),
    ]
    assert result[-1] == _aware_at(15, 30)


def test_coverage_series_does_not_duplicate_when_interval_divides_evenly() -> None:
    from src.marketdata.snapshot_contracts import _coverage_series

    result = _coverage_series(_aware_at(9, 0), 23400, _aware_at(15, 30))

    assert result == [_aware_at(15, 30)]


def test_index_minute_bar_not_after_defaults_to_run_end_deadline() -> None:
    settings = _settings()
    jobs = build_session_jobs(settings, _SESSION_DATE)
    by_id = {j.job_id: j for j in jobs}

    assert by_id["index_minute_bar@153000"].not_after == dt.datetime(2026, 9, 17, 15, 39, tzinfo=_KST)
    assert by_id["index_minute_bar@102000"].not_after == by_id["index_minute_bar@114000"].due_at
