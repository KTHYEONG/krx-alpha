from __future__ import annotations

import asyncio
import contextlib
import json
import pathlib
from collections.abc import Callable
from typing import Any

import pytest

from src.realtime.connection_handover import Handover, SwapRequest
from src.realtime.streamer import RealtimeStreamer
from src.realtime.ws_lease import Lease, LeaseRole, LeaseUnavailable, WsLeaseLedger
from tests.unit.realtime.test_connection_handover import _PAIRS, _Adapter, _frame, _Sink, _sync, _until

_TTL_S = 30.0
_KEY = "k1"


class _Clock:
    """Wall time for the ledger and monotonic time for the handover advance together."""

    def __init__(self) -> None:
        self.wall_ns = 1_700_000_000_000_000_000
        self.mono = 1000.0

    def advance(self, seconds: float) -> None:
        self.wall_ns += int(seconds * 1_000_000_000)
        self.mono += seconds


def _ledger(directory: pathlib.Path, clock: _Clock, cap: int) -> WsLeaseLedger:
    return WsLeaseLedger(
        directory, vendor="ls", key_id=_KEY, max_connections=cap, ttl_s=_TTL_S, clock_ns=lambda: clock.wall_ns
    )


def _rows(directory: pathlib.Path) -> list[dict[str, Any]]:
    return json.loads((directory / f"wslease_ls_{_KEY}.json").read_text(encoding="utf-8"))["leases"]


def _handover(
    ledger: WsLeaseLedger,
    clock: _Clock,
    request: SwapRequest,
    factory: Callable[[], _Adapter],
    primary_lease: Lease | None,
) -> Handover:
    return Handover(
        adapter_factory=factory,
        pairs=_PAIRS,
        request=request,
        max_s=60.0,
        cooldown_s=0.0,
        monotonic=lambda: clock.mono,
        lease_ledger=ledger,
        owner="owner",
        primary_lease=primary_lease,
        lease_ttl_s=_TTL_S,
    )


def _factory(made: list[_Adapter]) -> Callable[[], _Adapter]:
    def build() -> _Adapter:
        adapter = _Adapter()
        made.append(adapter)
        return adapter

    return build


def _pending_primary() -> asyncio.Task[Any]:
    return asyncio.ensure_future(asyncio.Event().wait())


async def _adopt(handover: Handover, request: SwapRequest, primary: asyncio.Task[Any]) -> _Adapter:
    request.request()
    handover.maybe_start(eligible=True)
    opening = handover._opening  # noqa: SLF001 - bring-up completion is the observable under test
    assert opening is not None
    await asyncio.wait({opening})
    await handover.step({opening}, primary)
    assert handover._standby is not None  # noqa: SLF001
    return handover._standby.adapter  # noqa: SLF001


async def _close(handover: Handover, primary: asyncio.Task[Any]) -> None:
    primary.cancel()
    await asyncio.gather(primary, return_exceptions=True)
    await handover.aclose()


@_sync
async def test_standby_denied_keeps_primary_untouched(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    ledger.acquire("other", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    old = _Adapter()
    sink = _Sink()
    stop = asyncio.Event()
    streamer = RealtimeStreamer(
        adapter=old,
        sink=sink,
        replay_pairs=_PAIRS,
        handover=handover,
        silence_limit=lambda: 30.0,
        silence_poll_interval_s=0.01,
        flush_interval_s=0.01,
    )
    task = asyncio.ensure_future(streamer.pump(stop))
    old.q.put_nowait(_frame("o0", "090000", "old"))
    await _until(lambda: sink.raws == ["o0"])
    request.request()
    await _until(lambda: "status=OPEN_SKIPPED reason=lease_denied" in caplog.text)
    old.q.put_nowait(_frame("o1", "090001", "old"))
    await _until(lambda: "o1" in sink.raws)
    assert made == []
    assert not old.closed
    stop.set()
    assert await asyncio.wait_for(task, 2.0) == "stopped"


@_sync
async def test_preempted_standby_is_dropped_and_primary_survives(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    primary = _pending_primary()
    try:
        standby = await _adopt(handover, request, primary)
        assert [row["role"] for row in _rows(tmp_path)] == ["primary", "standby"]
        with pytest.raises(LeaseUnavailable, match="preempt_pending"):
            ledger.acquire("rival", LeaseRole.PRIMARY)
        clock.advance(_TTL_S / 3)
        assert await handover.step(set(), primary) is None
        assert standby.closed
        assert not handover.active
        assert not primary.done()
        assert [(row["owner"], row["role"]) for row in _rows(tmp_path)] == [("owner", "primary")]
    finally:
        await _close(handover, primary)


@_sync
async def test_cut_over_on_a_full_ledger_leaves_one_primary_for_the_owner(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    primary = _pending_primary()
    try:
        standby = await _adopt(handover, request, primary)
        assert len(_rows(tmp_path)) == 2
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        handover.admit(_frame("o1", "090000", "old"), from_standby=False)
        cut = await handover.step(set(), primary)
        assert cut is not None
        assert cut.adapter is standby
        assert primary.cancelled()
        rows = _rows(tmp_path)
        assert [(row["owner"], row["role"]) for row in rows] == [("owner", "primary")]
        assert rows[0]["token"] == handover._primary_lease.token  # noqa: SLF001
        assert rows[0]["token"] != primary_lease.token
    finally:
        await _close(handover, primary)


@_sync
async def test_lapsed_primary_lease_is_reacquired_without_dropping_the_connection(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    handover = _handover(ledger, clock, SwapRequest(), _factory([]), primary_lease)
    primary = _pending_primary()
    try:
        clock.advance(_TTL_S + 1)
        assert await handover.step(set(), primary) is None
        assert not primary.done()
        assert handover._primary_lease is not None  # noqa: SLF001
        assert handover._primary_lease.token != primary_lease.token  # noqa: SLF001
        assert [(row["owner"], row["role"]) for row in _rows(tmp_path)] == [("owner", "primary")]
    finally:
        await _close(handover, primary)


@_sync
async def test_renewal_keeps_both_leases_alive_while_the_standby_is_held(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    primary = _pending_primary()
    try:
        standby = await _adopt(handover, request, primary)
        for _ in range(4):
            clock.advance(_TTL_S / 3)
            assert await handover.step(set(), primary) is None
        assert not standby.closed
        assert len(_rows(tmp_path)) == 2
    finally:
        await _close(handover, primary)


@_sync
async def test_standby_slot_lost_while_connecting_cancels_the_bring_up(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=3)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    connecting = _Adapter(hang_connect=True)
    request = SwapRequest()
    handover = _handover(ledger, clock, request, lambda: connecting, primary_lease)
    primary = _pending_primary()
    try:
        request.request()
        handover.maybe_start(eligible=True)
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.sleep(0.01)
        assert not opening.done()
        clock.advance(_TTL_S + 1)
        assert await handover.step(set(), primary) is None
        await asyncio.wait({opening})
        assert await handover.step({opening}, primary) is None
        assert connecting.closed
        assert not handover.active
        assert not primary.done()
        assert [row["role"] for row in _rows(tmp_path)] == ["primary"]
    finally:
        await _close(handover, primary)


@_sync
async def test_handover_without_ledger_cuts_over_with_no_lease_state() -> None:
    request = SwapRequest()
    new = _Adapter()
    handover = Handover(
        adapter_factory=lambda: new,
        pairs=_PAIRS,
        request=request,
        max_s=60.0,
        cooldown_s=0.0,
        lease_ledger=None,
        owner="",
    )
    primary = _pending_primary()
    try:
        assert await _adopt(handover, request, primary) is new
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        handover.admit(_frame("o1", "090000", "old"), from_standby=False)
        cut = await handover.step(set(), primary)
        assert cut is not None
        assert cut.adapter is new
        assert handover._primary_lease is None  # noqa: SLF001
        handover.release_primary_lease()
    finally:
        await _close(handover, primary)


def test_lease_ttl_must_be_positive_when_a_ledger_is_used(tmp_path: pathlib.Path) -> None:
    ledger = _ledger(tmp_path, _Clock(), cap=2)
    with pytest.raises(ValueError, match="lease_ttl_s"):
        Handover(
            adapter_factory=_Adapter,
            pairs=_PAIRS,
            request=SwapRequest(),
            max_s=10,
            cooldown_s=0,
            lease_ledger=ledger,
            owner="owner",
        )


@_sync
async def test_bring_up_finished_after_its_slot_lapsed_is_closed_not_adopted(tmp_path: pathlib.Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=3)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    primary = _pending_primary()
    try:
        request.request()
        handover.maybe_start(eligible=True)
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.wait({opening})
        clock.advance(_TTL_S + 1)
        assert await handover.step(set(), primary) is None
        assert await handover.step({opening}, primary) is None
        assert made[0].closed
        assert not handover.active
        assert not primary.done()
    finally:
        await _close(handover, primary)


@_sync
async def test_standby_lease_lapsed_at_cut_over_drops_only_the_standby(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    primary = _pending_primary()
    try:
        standby = await _adopt(handover, request, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        handover.admit(_frame("o1", "090000", "old"), from_standby=False)
        clock.wall_ns += int((_TTL_S + 1) * 1_000_000_000)
        assert await handover.step(set(), primary) is None
        assert standby.closed
        assert not handover.active
        assert not primary.done()
        assert "status=STANDBY_LOST reason=lease_lost" in caplog.text
    finally:
        await _close(handover, primary)


@_sync
async def test_unreadable_ledger_keeps_the_standby_connection(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)
    primary = _pending_primary()
    try:
        standby = await _adopt(handover, request, primary)
        (tmp_path / f"wslease_ls_{_KEY}.json").write_text("{not json", encoding="utf-8")
        clock.advance(_TTL_S / 3)
        assert await handover.step(set(), primary) is None
        assert not standby.closed
        assert handover.active
        assert not primary.done()
        assert "status=RENEW_DEFERRED" in caplog.text
    finally:
        await _close(handover, primary)


@_sync
async def test_lapsed_primary_with_a_full_ledger_leaves_the_owner_without_a_primary_lease(
    tmp_path: pathlib.Path,
) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    handover = _handover(ledger, clock, SwapRequest(), _factory([]), primary_lease)
    primary = _pending_primary()
    try:
        clock.advance(_TTL_S + 1)
        ledger.acquire("rival", LeaseRole.PRIMARY)
        ledger.acquire("rival", LeaseRole.PRIMARY)
        assert await handover.step(set(), primary) is None
        assert handover._primary_lease is None  # noqa: SLF001
        assert not primary.done()
        assert [(row["owner"], row["role"]) for row in _rows(tmp_path)] == [
            ("rival", "primary"),
            ("rival", "primary"),
        ]
    finally:
        await _close(handover, primary)


@_sync
async def test_standby_preempted_mid_cut_over_is_released_and_primary_reacquired(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock, cap=2)
    primary_lease = ledger.acquire("owner", LeaseRole.PRIMARY)
    made: list[_Adapter] = []
    request = SwapRequest()
    handover = _handover(ledger, clock, request, _factory(made), primary_lease)

    async def old_connection() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # A rival preempts the standby while the cut-over awaits this connection's cancellation,
            # i.e. after the standby lease was renewed and before it is promoted.
            with contextlib.suppress(LeaseUnavailable):
                ledger.acquire("rival", LeaseRole.PRIMARY)
            raise

    primary = asyncio.ensure_future(old_connection())
    try:
        standby = await _adopt(handover, request, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        handover.admit(_frame("o1", "090000", "old"), from_standby=False)
        cut = await handover.step(set(), primary)
        assert cut is not None
        assert cut.adapter is standby
        assert not standby.closed
        assert not handover.active
        assert "[SYS] stage=ws_lease status=PROMOTE_FAILED reason=standby_not_promotable" in caplog.text
        rows = _rows(tmp_path)
        assert [(row["owner"], row["role"]) for row in rows] == [("owner", "primary")]
        assert rows[0]["token"] == handover._primary_lease.token  # noqa: SLF001
        assert rows[0]["token"] != primary_lease.token
    finally:
        await _close(handover, primary)
