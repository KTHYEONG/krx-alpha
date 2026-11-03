from __future__ import annotations

import asyncio
import logging

import pytest

import src.realtime.session as session_mod
from src.realtime.contracts import VendorAck
from src.realtime.session import DynamicFeed, DynamicPairWatcher
from src.universe.intraday import DynamicEntry, DynamicPlan, write_dynamic_admissions

_CNT = "H0STCNT0"


class _Session:
    def note_ack(self, **kwargs) -> None:
        return None


class _Live:
    def __init__(self) -> None:
        self.calls = 0
        self.entered = asyncio.Event()

    def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
        return ()

    async def subscribe_live(self, pairs):
        self.calls += 1
        self.entered.set()
        await asyncio.Event().wait()
        return []


def _watcher(tmp_path) -> DynamicPairWatcher:
    feed = DynamicFeed(
        vendor="ls",
        feed="ls:1",
        streams=(_CNT,),
        ipc_path=tmp_path / "dynamic.json",
        membership_path=tmp_path / "membership.jsonl",
        ls_shard=1,
    )
    return DynamicPairWatcher(feed=feed, session=_Session(), static_pairs=[], poll_s=0.01)  # type: ignore[arg-type]


def test_tick_without_adapter_sends_nothing(tmp_path) -> None:
    assert asyncio.run(_watcher(tmp_path).tick(None)) == 0


def test_cancelling_watch_during_live_subscribe_propagates(tmp_path) -> None:
    write_dynamic_admissions(tmp_path / "dynamic.json", DynamicPlan(rev=1, entries=(DynamicEntry("000660", "ls:1", 1),)))
    watcher = _watcher(tmp_path)
    live = _Live()

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.ensure_future(watcher.run(lambda: live, stop))
        await live.entered.wait()
        task.cancel()
        await task

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(scenario())


def test_membership_write_failure_is_logged_not_raised(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.ERROR)
    write_dynamic_admissions(tmp_path / "dynamic.json", DynamicPlan(rev=1, entries=(DynamicEntry("000660", "ls:1", 1),)))

    class _Ack:
        def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
            return ()

        async def subscribe_live(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

    def failing_append(path, rows):
        raise OSError("disk full")

    monkeypatch.setattr(session_mod, "append_membership", failing_append)
    watcher = _watcher(tmp_path)

    sent = asyncio.run(watcher.tick(_Ack()))  # type: ignore[arg-type]

    assert sent == 1
    assert "reason=membership_write" in caplog.text
