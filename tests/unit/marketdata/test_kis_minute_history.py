"""Invariant tests for the KIS historical minute day walk."""

from __future__ import annotations

import datetime as dt

import pytest

from src.brokers.kis.data import KisMinuteBar
from src.execution.contracts import KisApiError
from src.marketdata.kis_minute_history import walk_kis_minute_day

_DAY = dt.date(2026, 10, 20)


def _hms(minutes: int) -> int:
    base = dt.datetime(2026, 10, 20, 9, 0) + dt.timedelta(minutes=minutes)
    return base.hour * 10000 + base.minute * 100 + base.second


def _bar(ts_hms: int) -> KisMinuteBar:
    return KisMinuteBar(day=_DAY, ts_hms=ts_hms, open=100, high=101, low=99, close=100, volume=10)


def _full_day_bars() -> list[KisMinuteBar]:
    return [_bar(_hms(m)) for m in range(391)]


def _paged_fetch(bars: list[KisMinuteBar], calls: list[int]):
    ordered = sorted(bars, key=lambda b: b.ts_hms, reverse=True)

    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        calls.append(before_hms)
        return tuple(bar for bar in ordered if bar.ts_hms <= before_hms)[:120]

    return _fetch


def test_full_day_in_four_calls() -> None:
    calls: list[int] = []
    walked = walk_kis_minute_day("A", _DAY, fetch=_paged_fetch(_full_day_bars(), calls), max_calls=10)
    assert walked.status == "complete"
    assert walked.calls == 4
    assert len(walked.bars) == 391
    assert [bar.ts_hms for bar in walked.bars] == sorted(bar.ts_hms for bar in walked.bars)
    assert walked.bars[0].ts_hms == 90000
    assert walked.bars[-1].ts_hms == 153000
    assert calls[0] == 153000


def test_non_progress_guard() -> None:
    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        return (_bar(150000),) if before_hms >= 150000 else (_bar(150500),)

    with pytest.raises(KisApiError):
        walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=4)


def test_bar_at_session_high_on_first_page_is_progress() -> None:
    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        return (_bar(153000),) if before_hms >= 153000 else ()

    walked = walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=4)
    assert walked.status == "incomplete"
    assert walked.calls == 2


def test_repeated_floor_page_completes_thin_symbol_day() -> None:
    thin = [_bar(_hms(m)) for m in range(2, 391, 7)]
    ordered = sorted(thin, key=lambda b: b.ts_hms, reverse=True)
    floor = ordered[-1]

    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        page = tuple(bar for bar in ordered if bar.ts_hms <= before_hms)[:120]
        return page if page else (floor,)

    walked = walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=10)
    assert (walked.status, walked.reason) == ("complete", "vendor_floor")
    assert [bar.ts_hms for bar in walked.bars] == sorted(bar.ts_hms for bar in thin)


def test_repeated_page_without_any_kept_bar_is_still_a_guard() -> None:
    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        return (_bar(155000),)

    with pytest.raises(KisApiError):
        walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=4)


def test_empty_first_page_is_vendor_empty() -> None:
    walked = walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: (), max_calls=4)
    assert walked.status == "vendor_empty"
    assert walked.reason == "no_bars"
    assert walked.bars == ()


def test_budget_exhaustion_is_incomplete() -> None:
    calls: list[int] = []
    walked = walk_kis_minute_day("A", _DAY, fetch=_paged_fetch(_full_day_bars(), calls), max_calls=2)
    assert walked.status == "incomplete"
    assert walked.calls == 2
    assert walked.bars == ()


def test_off_session_bars_excluded() -> None:
    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        assert before_hms == 153000
        return (_bar(85900), _bar(90000), _bar(153000), _bar(153100))

    walked = walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=4)
    assert walked.status == "complete"
    assert [bar.ts_hms for bar in walked.bars] == [90000, 153000]


def test_max_calls_below_one_raises() -> None:
    with pytest.raises(ValueError, match="max_calls"):
        walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: (), max_calls=0)


def test_boundary_mid_walk_is_incomplete() -> None:
    from src.marketdata.kis_minute_history import MinuteBackfillBoundaryError

    calls = {"n": 0}

    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        calls["n"] += 1
        if calls["n"] > 1:
            raise MinuteBackfillBoundaryError("EXHAUSTED", "boundary")
        return (_bar(153000), _bar(152900))

    walked = walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=4)
    assert walked.status == "incomplete"
    assert walked.calls == 1


def test_empty_second_page_after_open_is_complete() -> None:
    pages = [(_bar(153000), _bar(90000)), ()]
    walked = walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: pages.pop(0), max_calls=4)
    assert walked.status == "complete"
    assert [bar.ts_hms for bar in walked.bars] == [90000, 153000]


def test_empty_second_page_near_open_is_complete() -> None:
    pages = [(_bar(153000), _bar(90100)), ()]
    walked = walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: pages.pop(0), max_calls=4)
    assert walked.status == "complete"
    assert [bar.ts_hms for bar in walked.bars] == [90100, 153000]


def test_empty_second_page_without_open_is_incomplete() -> None:
    pages = [(_bar(153000), _bar(120000)), ()]
    walked = walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: pages.pop(0), max_calls=4)
    assert walked.status == "incomplete"


def test_duplicate_bars_deduped() -> None:
    pages = [(_bar(153000), _bar(152900)), (_bar(152900), _bar(90000))]
    walked = walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: pages.pop(0), max_calls=4)
    assert walked.status == "complete"
    assert [bar.ts_hms for bar in walked.bars] == [90000, 152900, 153000]


def test_budget_cutoff_near_open_is_incomplete() -> None:
    walked = walk_kis_minute_day(
        "A", _DAY, fetch=lambda before_hms: (_bar(153000), _bar(90100)), max_calls=1
    )
    assert walked.status == "incomplete"
    assert walked.bars == ()


def test_boundary_first_call_is_incomplete() -> None:
    from src.marketdata.kis_minute_history import MinuteBackfillBoundaryError

    def _fetch(before_hms: int) -> tuple[KisMinuteBar, ...]:
        raise MinuteBackfillBoundaryError("EXHAUSTED", "boundary")

    walked = walk_kis_minute_day("A", _DAY, fetch=_fetch, max_calls=4)
    assert walked.status == "incomplete"
    assert walked.calls == 0


def test_wrong_date_cannot_certify_day() -> None:
    bar = KisMinuteBar(day=_DAY - dt.timedelta(days=1), ts_hms=90000, open=100, high=101, low=99, close=100, volume=10)
    with pytest.raises(KisApiError, match="another date"):
        walk_kis_minute_day("A", _DAY, fetch=lambda before_hms: (bar,), max_calls=4)


def test_boundary_near_open_does_not_certify_day() -> None:
    from src.marketdata.kis_minute_history import MinuteBackfillBoundaryError

    def fetch(before_hms):
        if before_hms == 153000:
            return (_bar(90100),)
        raise MinuteBackfillBoundaryError("EXHAUSTED", "deadline")

    walked = walk_kis_minute_day("A", _DAY, fetch=fetch, max_calls=4)
    assert walked.status == "incomplete"
    assert walked.bars == ()
