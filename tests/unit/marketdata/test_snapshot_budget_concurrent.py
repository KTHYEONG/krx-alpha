"""Bounded concurrent fetches in ``_JobBudget.each_concurrent``."""

from __future__ import annotations

import datetime as dt
import itertools
import threading
import time

import pytest

from src.execution.contracts import KisApiError
from src.marketdata.snapshot_service import _JobBudget

NOT_AFTER = dt.datetime(2026, 11, 24, 9, 0, tzinfo=dt.UTC)
BEFORE = NOT_AFTER - dt.timedelta(seconds=1)
AFTER = NOT_AFTER + dt.timedelta(seconds=1)


def _budget(now_fn=lambda: BEFORE) -> _JobBudget:
    ticks = itertools.count(1)
    return _JobBudget(not_after=NOT_AFTER, now_fn=now_fn, wall_ns=lambda: next(ticks))


def _keys(count: int) -> tuple[str, ...]:
    return tuple(f"{index:06d}" for index in range(count))


def test_workers_one_equals_each() -> None:
    keys = _keys(5)

    def fetch(key: str) -> str:
        if key == "000002":
            raise KisApiError("EGW00201", "rate")
        return key * 2

    sequential = _budget()
    sequential_out = list(sequential.each(keys, fetch))
    concurrent = _budget()
    concurrent_out = concurrent.each_concurrent(keys, fetch, workers=1)

    assert concurrent_out == sequential_out
    assert (concurrent.attempted, concurrent.succeeded, concurrent.failed) == (
        sequential.attempted,
        sequential.succeeded,
        sequential.failed,
    )
    assert concurrent.failed_keys == sequential.failed_keys
    assert concurrent.truncated is sequential.truncated


def test_all_keys_once_in_order() -> None:
    keys = _keys(50)
    lock = threading.Lock()
    fetched: list[str] = []

    def fetch(key: str) -> str:
        with lock:
            fetched.append(key)
        return key

    budget = _budget()
    out = budget.each_concurrent(keys, fetch, workers=3)

    assert sorted(fetched) == sorted(keys)
    assert [result for result, _ in out] == list(keys)
    assert (budget.attempted, budget.succeeded) == (50, 50)


def test_in_flight_bounded() -> None:
    keys = _keys(50)
    lock = threading.Lock()
    in_flight = 0
    peak = 0
    started = 0
    gate = threading.Barrier(2, timeout=2)

    def fetch(key: str) -> str:
        nonlocal in_flight, peak, started
        with lock:
            started += 1
            first_wave = started <= 2
            in_flight += 1
            peak = max(peak, in_flight)
        if first_wave:
            gate.wait()
        time.sleep(0.001)
        with lock:
            in_flight -= 1
        return key

    _budget().each_concurrent(keys, fetch, workers=3)

    assert 2 <= peak <= 3


def test_deadline_stops_new_calls() -> None:
    keys = _keys(40)
    workers = 3
    allowed = 5
    lock = threading.Lock()
    calls = 0

    def now_fn() -> dt.datetime:
        nonlocal calls
        with lock:
            calls += 1
            return BEFORE if calls <= allowed else AFTER

    fetched: list[str] = []

    def fetch(key: str) -> str:
        with lock:
            fetched.append(key)
        return key

    budget = _budget(now_fn=now_fn)
    out = budget.each_concurrent(keys, fetch, workers=workers)

    assert budget.truncated is True
    assert len(fetched) == allowed
    assert budget.attempted <= allowed + workers
    assert len(out) == allowed


def test_vendor_error_counted() -> None:
    keys = _keys(10)
    bad = {"000001", "000004", "000008"}

    def fetch(key: str) -> str:
        if key in bad:
            raise KisApiError("EGW00201", f"rate {key}")
        return key

    budget = _budget()
    out = budget.each_concurrent(keys, fetch, workers=3)

    assert budget.failed == 3
    assert set(budget.failed_keys) == bad
    assert budget.first_error is not None
    assert len(out) == 7
    assert [result for result, _ in out] == [key for key in keys if key not in bad]


def test_non_vendor_error_propagates() -> None:
    keys = _keys(20)
    baseline = threading.active_count()

    def fetch(key: str) -> str:
        if key == "000003":
            raise RuntimeError("boom")
        time.sleep(0.001)
        return key

    with pytest.raises(RuntimeError, match="boom"):
        _budget().each_concurrent(keys, fetch, workers=3)

    assert threading.active_count() == baseline


def test_empty_keys_make_no_calls() -> None:
    budget = _budget()

    assert budget.each_concurrent((), lambda key: key, workers=3) == []
    assert (budget.attempted, budget.succeeded, budget.truncated) == (0, 0, False)
