import datetime as dt

import pytest

from src.core.session_anchors import (
    AnchorSource,
    SessionAnchorError,
    SessionAnchors,
    load_session_anchors,
    resolve_session_anchors,
    save_session_anchors,
    standard_session_anchors,
)


def _csat_anchors() -> SessionAnchors:
    return SessionAnchors(
        date=dt.date(2025, 11, 13),
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )


def test_standard_anchors_have_zero_shift() -> None:
    anchors = standard_session_anchors(dt.date(2026, 10, 1))

    assert anchors.open_shift == dt.timedelta(0)
    assert anchors.close_shift == dt.timedelta(0)
    assert anchors.source is AnchorSource.DEFAULT


def test_csat_anchors_shift_pre_open_and_post_close() -> None:
    anchors = _csat_anchors()

    assert anchors.shift_pre_open(dt.time(8, 30)) == dt.time(9, 30)
    assert anchors.shift_post_close(dt.time(15, 40)) == dt.time(16, 40)


def test_first_trading_day_anchors_shift_open_only() -> None:
    anchors = SessionAnchors(
        date=dt.date(2026, 1, 2),
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(15, 20),
        regular_close=dt.time(15, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    assert anchors.open_shift == dt.timedelta(hours=1)
    assert anchors.close_shift == dt.timedelta(0)


def test_out_of_order_anchors_rejected() -> None:
    with pytest.raises(SessionAnchorError):
        SessionAnchors(
            date=dt.date(2026, 10, 1),
            regular_open=dt.time(9, 0),
            closing_auction_start=dt.time(15, 30),
            regular_close=dt.time(15, 20),
            after_market_end=dt.time(20, 0),
            source=AnchorSource.VENDOR,
        )


def test_shift_beyond_cap_rejected() -> None:
    with pytest.raises(SessionAnchorError):
        SessionAnchors(
            date=dt.date(2026, 10, 1),
            regular_open=dt.time(12, 30),
            closing_auction_start=dt.time(15, 20),
            regular_close=dt.time(15, 30),
            after_market_end=dt.time(20, 0),
            source=AnchorSource.VENDOR,
        )


def test_save_then_load_round_trips(tmp_path) -> None:
    anchors = _csat_anchors()

    save_session_anchors(tmp_path, anchors)

    assert (tmp_path / "dt=2025-11-13.json").is_file()
    assert load_session_anchors(tmp_path, dt.date(2025, 11, 13)) == anchors


def test_resave_overwrites_prefetched_record(tmp_path) -> None:
    day = dt.date(2026, 10, 1)
    early = standard_session_anchors(day)
    late = SessionAnchors(
        date=day,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    save_session_anchors(tmp_path, early)
    save_session_anchors(tmp_path, late)

    assert load_session_anchors(tmp_path, day) == late


def test_corrupt_file_resolves_to_standard(tmp_path) -> None:
    day = dt.date(2026, 10, 1)
    (tmp_path / "dt=2026-10-01.json").write_text("{not json", encoding="utf-8")

    assert load_session_anchors(tmp_path, day) is None
    resolved = resolve_session_anchors(tmp_path, day)
    assert resolved == standard_session_anchors(day)
    assert resolved.source is AnchorSource.DEFAULT


def test_record_for_another_date_ignored(tmp_path) -> None:
    (tmp_path / "dt=2026-10-01.json").write_text(
        '{"date": "2025-11-13", "regular_open": "10:00:00", "closing_auction_start": "16:20:00",'
        ' "regular_close": "16:30:00", "after_market_end": "20:00:00", "source": "vendor"}',
        encoding="utf-8",
    )

    assert load_session_anchors(tmp_path, dt.date(2026, 10, 1)) is None


def test_shift_leaving_calendar_day_rejected() -> None:
    anchors = SessionAnchors(
        date=dt.date(2026, 10, 1),
        regular_open=dt.time(6, 0),
        closing_auction_start=dt.time(15, 20),
        regular_close=dt.time(15, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    with pytest.raises(SessionAnchorError):
        anchors.shift_pre_open(dt.time(1, 0))


def test_non_object_file_loads_as_none(tmp_path) -> None:
    (tmp_path / "dt=2026-10-01.json").write_text("[1, 2]", encoding="utf-8")

    assert load_session_anchors(tmp_path, dt.date(2026, 10, 1)) is None


def test_resolve_returns_saved_record(tmp_path) -> None:
    anchors = _csat_anchors()
    save_session_anchors(tmp_path, anchors)

    assert resolve_session_anchors(tmp_path, dt.date(2025, 11, 13)) == anchors


def test_resolve_missing_file_returns_standard(tmp_path) -> None:
    resolved = resolve_session_anchors(tmp_path, dt.date(2026, 10, 1))

    assert resolved == standard_session_anchors(dt.date(2026, 10, 1))


def test_load_returns_none_for_invalid_anchor_order(tmp_path) -> None:
    import datetime as dt
    import json

    from src.core.session_anchors import AnchorSource, load_session_anchors, resolve_session_anchors

    day = dt.date(2026, 11, 19)
    (tmp_path / f"dt={day.isoformat()}.json").write_text(
        json.dumps(
            {
                "date": day.isoformat(),
                "regular_open": "10:00:00",
                "closing_auction_start": "16:20:00",
                "regular_close": "16:00:00",
                "after_market_end": "20:00:00",
                "source": "vendor",
            }
        ),
        encoding="utf-8",
    )

    # 불변식 위반 파일도 예외 없이 None: 데몬 step 과 수집 자식이 복구 전에 죽지 않아야 한다.
    assert load_session_anchors(tmp_path, day) is None
    assert resolve_session_anchors(tmp_path, day).source is AnchorSource.DEFAULT


def test_load_returns_none_when_anchor_path_is_directory(tmp_path) -> None:
    import datetime as dt

    from src.core.session_anchors import load_session_anchors

    day = dt.date(2026, 11, 19)
    (tmp_path / f"dt={day.isoformat()}.json").mkdir()

    assert load_session_anchors(tmp_path, day) is None
