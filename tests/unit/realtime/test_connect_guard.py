"""Invariant tests for the cross-process websocket connect-rate guard."""

from __future__ import annotations

import asyncio
import json
import logging

from src.realtime.connect_guard import ConnectRateGuard

_MAX = 4
_WINDOW = 60.0


class _FakeTime:
    def __init__(self) -> None:
        self.now = 1_000_000.0
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _guard(path, fake: _FakeTime, stamps: list[float] | None = None) -> ConnectRateGuard:
    guard = ConnectRateGuard(path, max_connects=_MAX, window_s=_WINDOW, clock=fake.clock, sleep=fake.sleep)
    if stamps is None:
        return guard
    inner = guard.wait_turn

    async def recording() -> None:
        await inner()
        stamps.append(fake.now)

    guard.wait_turn = recording  # type: ignore[method-assign]
    return guard


def _max_in_any_window(stamps: list[float]) -> int:
    return max(sum(1 for other in stamps if start <= other < start + _WINDOW) for start in stamps)


def test_connect_guard_allows_n_then_waits(tmp_path) -> None:
    fake = _FakeTime()
    stamps: list[float] = []
    guard = _guard(tmp_path / "g.json", fake, stamps)
    start = fake.now

    async def scenario() -> None:
        for _ in range(_MAX):
            await guard.wait_turn()
        assert fake.sleeps == []
        await guard.wait_turn()

    asyncio.run(scenario())
    assert fake.sleeps == [_WINDOW + 0.5]
    assert stamps[-1] >= start + _WINDOW + 0.5


def test_connect_guard_never_exceeds_cap_across_twenty_attempts(tmp_path) -> None:
    fake = _FakeTime()
    stamps: list[float] = []
    guard = _guard(tmp_path / "g.json", fake, stamps)

    async def scenario() -> None:
        for _ in range(20):
            await guard.wait_turn()
            fake.now += 1.0

    asyncio.run(scenario())
    assert len(stamps) == 20
    assert _max_in_any_window(stamps) <= _MAX


def test_connect_guard_shared_across_two_instances(tmp_path) -> None:
    fake = _FakeTime()
    stamps: list[float] = []
    path = tmp_path / "g.json"
    first, second = _guard(path, fake, stamps), _guard(path, fake, stamps)

    async def scenario() -> None:
        for _ in range(10):
            await first.wait_turn()
            await second.wait_turn()
            fake.now += 2.0

    asyncio.run(scenario())
    assert len(stamps) == 20
    assert _max_in_any_window(stamps) <= _MAX


def test_connect_guard_survives_corrupt_file(tmp_path, caplog) -> None:
    fake = _FakeTime()
    path = tmp_path / "g.json"
    path.write_text("{not json")
    guard = _guard(path, fake)
    with caplog.at_level(logging.WARNING):
        asyncio.run(guard.wait_turn())
    corrupt = [record for record in caplog.records if "stage=connect_guard status=CORRUPT" in record.getMessage()]
    assert len(corrupt) == 1
    assert len(list(tmp_path.glob("g.json.corrupt-*"))) == 1
    assert json.loads(path.read_text()) == [fake.now]
    assert fake.sleeps == []


def test_connect_guard_lock_not_held_while_sleeping(tmp_path) -> None:
    path = tmp_path / "g.json"
    fake = _FakeTime()
    entered_while_sleeping: list[bool] = []
    other = ConnectRateGuard(path, max_connects=_MAX + 1, window_s=_WINDOW, clock=fake.clock, sleep=fake.sleep)

    async def sleep_and_let_other_in(seconds: float) -> None:
        await other.wait_turn()
        entered_while_sleeping.append(True)
        fake.now += seconds

    blocked = ConnectRateGuard(path, max_connects=_MAX, window_s=_WINDOW, clock=fake.clock, sleep=sleep_and_let_other_in)

    async def scenario() -> None:
        for _ in range(_MAX):
            await blocked.wait_turn()
        await blocked.wait_turn()

    asyncio.run(scenario())
    assert entered_while_sleeping == [True]


def test_connect_guard_logs_waits_over_one_second(tmp_path, caplog) -> None:
    fake = _FakeTime()
    guard = _guard(tmp_path / "g.json", fake)

    async def scenario() -> None:
        for _ in range(_MAX + 1):
            await guard.wait_turn()

    with caplog.at_level(logging.WARNING):
        asyncio.run(scenario())
    waits = [record.getMessage() for record in caplog.records if "stage=connect_guard status=WAIT vendor=db" in record.getMessage()]
    assert len(waits) == 1


def test_connect_guard_rejects_invalid_limits(tmp_path) -> None:
    import pytest

    with pytest.raises(ValueError, match="max_connects"):
        ConnectRateGuard(tmp_path / "g.json", max_connects=0, window_s=_WINDOW)
    with pytest.raises(ValueError, match="max_connects"):
        ConnectRateGuard(tmp_path / "g.json", max_connects=_MAX, window_s=0)


def test_connect_guard_treats_wrong_shape_as_corrupt(tmp_path) -> None:
    fake = _FakeTime()
    path = tmp_path / "g.json"
    path.write_text(json.dumps({"stamps": [1.0]}))
    asyncio.run(_guard(path, fake).wait_turn())
    assert len(list(tmp_path.glob("g.json.corrupt-*"))) == 1


def test_connect_guard_failed_write_leaves_no_temp_file(tmp_path, monkeypatch) -> None:
    import pytest

    def broken_replace(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr("src.realtime.connect_guard.os.replace", broken_replace)
    with pytest.raises(OSError, match="disk full"):
        asyncio.run(_guard(tmp_path / "g.json", _FakeTime()).wait_turn())
    assert [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []
