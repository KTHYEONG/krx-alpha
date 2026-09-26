def _csat_anchors(day):
    import datetime as dt

    from src.core.session_anchors import AnchorSource, SessionAnchors

    return SessionAnchors(
        date=day,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )


def test_shift_snapshot_settings_moves_session_times_on_csat_day() -> None:
    import datetime as dt

    from src.core.config import SnapshotSettings
    from src.marketdata.snapshot_plan import SnapshotJobKind, build_session_jobs, shift_snapshot_settings

    day = dt.date(2026, 11, 19)
    shifted = shift_snapshot_settings(SnapshotSettings(), _csat_anchors(day))
    jobs = build_session_jobs(shifted, day)
    by_kind: dict = {}
    for job in jobs:
        by_kind.setdefault(job.kind, []).append(job)

    assert min(job.due_at for job in by_kind[SnapshotJobKind.AUCTION_OPEN]).time() == dt.time(9, 40)
    assert max(job.due_at for job in by_kind[SnapshotJobKind.AUCTION_CLOSE]).time() == dt.time(16, 29, 30)
    assert [job.due_at.time() for job in by_kind[SnapshotJobKind.EOD_MINUTE_BARS]] == [dt.time(16, 35)]


def test_shift_snapshot_settings_keeps_standard_plan_identical() -> None:
    import datetime as dt

    from src.core.config import SnapshotSettings
    from src.core.session_anchors import standard_session_anchors
    from src.marketdata.snapshot_plan import build_session_jobs, shift_snapshot_settings

    day = dt.date(2026, 10, 1)
    settings = SnapshotSettings()
    shifted = shift_snapshot_settings(settings, standard_session_anchors(day))

    assert shifted == settings
    shifted.check_snapshot_contract()
    assert build_session_jobs(shifted, day) == build_session_jobs(settings, day)


def test_csat_shifted_settings_satisfy_shifted_contract() -> None:
    import datetime as dt

    import pytest

    from src.core.config import SnapshotSettings, validate_snapshot_order
    from src.marketdata.snapshot_plan import shift_snapshot_settings

    shifted = shift_snapshot_settings(SnapshotSettings(), _csat_anchors(dt.date(2026, 11, 19)))

    # 수능일 run_end(16:39)는 이동된 close 전이(16:40) 기준으로 계약을 만족하고, 표준 close 기준으로는 위반이다.
    validate_snapshot_order(shifted, market_close=dt.time(16, 40))
    with pytest.raises(ValueError, match="session time order"):
        validate_snapshot_order(shifted, market_close=dt.time(15, 40))
