from __future__ import annotations

import asyncio
import logging

import pytest

from src.cli import collect_stream
from src.core.config import CollectorSettings
from src.realtime.contracts import VendorAck
from src.realtime.session import DynamicFeed, DynamicPairWatcher
from src.universe.intraday import DynamicEntry, DynamicPlan, write_dynamic_admissions
from src.universe.membership import read_membership

_CNT = "H0STCNT0"
_ASP = "H0STASP0"


class _Session:
    def __init__(self) -> None:
        self.notes: list[dict] = []

    def note_ack(self, **kwargs) -> None:
        self.notes.append(kwargs)


class _Live:
    """Adapter fake: each call pops one scripted result (exception raised, list returned, None for all-accepted)."""

    def __init__(self, results=None, on_call=None) -> None:
        self.results = list(results or [])
        self.calls: list[list[tuple[str, str]]] = []
        self.on_call = on_call
        self.subscribed: list[tuple[str, str]] = []

    def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
        return tuple(self.subscribed)

    async def subscribe_live(self, pairs):
        self.calls.append(list(pairs))
        if self.on_call is not None:
            self.on_call(len(self.calls))
        outcome = self.results.pop(0) if self.results else None
        if isinstance(outcome, Exception):
            raise outcome
        acks = outcome if outcome is not None else [VendorAck(symbol, stream, True, "00000") for symbol, stream in pairs]
        self.subscribed.extend((a.symbol, a.stream) for a in acks if a.accepted)
        return acks


def _feed(tmp_path, feed: str = "ls:1", kis_slot: str | None = None) -> DynamicFeed:
    return DynamicFeed(
        vendor="ls",
        feed=feed,
        streams=(_CNT, _ASP),
        ipc_path=tmp_path / "dynamic.json",
        membership_path=tmp_path / "membership.jsonl",
        ls_shard=1 if feed.startswith("ls:") else None,
        kis_slot=kis_slot,
    )


def _publish(tmp_path, *entries: tuple[str, str]) -> None:
    write_dynamic_admissions(
        tmp_path / "dynamic.json",
        DynamicPlan(rev=_next_rev(tmp_path), entries=tuple(DynamicEntry(s, f, 1) for s, f in entries)),
    )


def _next_rev(tmp_path) -> int:
    from src.universe.intraday import read_dynamic_admissions

    return read_dynamic_admissions(tmp_path / "dynamic.json").rev + 1


def test_tick_subscribes_unseen_pairs_and_records_each_ack(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"), ("035420", "ls:1"), ("005930", "kis:5"))
    session = _Session()
    live = _Live(
        results=[
            [
                VendorAck("000660", _CNT, True, "00000"),
                VendorAck("000660", _ASP, True, "00000"),
                VendorAck("035420", _CNT, False, "10011"),
                VendorAck("035420", _ASP, False, "10011"),
            ]
        ]
    )
    watcher = DynamicPairWatcher(
        feed=_feed(tmp_path),
        session=session,  # type: ignore[arg-type]
        static_pairs=[("005930", _CNT)],
        wall_ns=lambda: 42,
    )

    sent = asyncio.run(watcher.tick(live))  # type: ignore[arg-type]

    assert sent == 4
    assert live.calls == [[("000660", _CNT), ("000660", _ASP), ("035420", _CNT), ("035420", _ASP)]]
    assert watcher.pairs() == [("005930", _CNT), ("000660", _CNT), ("000660", _ASP)]
    assert [(n["symbol"], n["accepted"], n["dynamic"]) for n in session.notes] == [
        ("000660", True, True),
        ("000660", True, True),
        ("035420", False, True),
        ("035420", False, True),
    ]
    rows = list(read_membership(tmp_path / "membership.jsonl"))
    assert {(r.symbol, r.source, r.tier, r.reason, r.decided_at_ns, r.ls_shard) for r in rows} == {
        ("000660", "ack", "B", "accepted:00000", 42, 1),
        ("035420", "ack", "B", "rejected:10011", 42, 1),
    }


def test_unchanged_revision_does_not_resubscribe(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"))
    live = _Live()
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    asyncio.run(watcher.tick(live))  # type: ignore[arg-type]
    assert asyncio.run(watcher.tick(live)) == 0  # type: ignore[arg-type]

    assert len(live.calls) == 1


def test_failed_subscribe_is_retried_on_next_tick(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"))
    live = _Live(results=[ValueError("capacity"), None])
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="capacity"):
        asyncio.run(watcher.tick(live))  # type: ignore[arg-type]
    assert asyncio.run(watcher.tick(live)) == 2  # type: ignore[arg-type]

    assert len(live.calls) == 2
    assert live.calls[0] == live.calls[1]


def test_new_adapter_only_receives_pairs_it_does_not_hold(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"))
    first, second = _Live(), _Live()
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    asyncio.run(watcher.tick(first))  # type: ignore[arg-type]
    second.subscribed = list(watcher.pairs())  # reconnect subscribed the full current set at connect time
    _publish(tmp_path, ("000660", "ls:1"), ("035420", "ls:1"))
    asyncio.run(watcher.tick(second))  # type: ignore[arg-type]

    assert second.calls == [[("035420", _CNT), ("035420", _ASP)]]


def test_cut_over_adapter_missing_admitted_pair_gets_it_without_new_revision(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"))
    first, standby = _Live(), _Live()
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    asyncio.run(watcher.tick(first))  # type: ignore[arg-type]
    sent = asyncio.run(watcher.tick(standby))  # type: ignore[arg-type]

    assert sent == 2
    assert standby.calls == [[("000660", _CNT), ("000660", _ASP)]]


def test_vendor_rejected_pair_is_not_resent_on_later_revisions(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"))
    live = _Live(results=[[VendorAck("000660", _CNT, False, "10011"), VendorAck("000660", _ASP, False, "10011")]])
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    asyncio.run(watcher.tick(live))  # type: ignore[arg-type]
    _publish(tmp_path, ("000660", "ls:1"), ("035420", "ls:1"))
    asyncio.run(watcher.tick(live))  # type: ignore[arg-type]

    assert live.calls[1] == [("035420", _CNT), ("035420", _ASP)]


def test_timed_out_pair_is_retried_on_next_revision(tmp_path) -> None:
    _publish(tmp_path, ("000660", "ls:1"))
    live = _Live(results=[[VendorAck("000660", _CNT, False, "timeout"), VendorAck("000660", _ASP, True, "00000")]])
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    asyncio.run(watcher.tick(live))  # type: ignore[arg-type]
    _publish(tmp_path, ("000660", "ls:1"))
    asyncio.run(watcher.tick(live))  # type: ignore[arg-type]

    assert live.calls[1] == [("000660", _CNT)]


def test_other_feed_entries_are_not_subscribed(tmp_path) -> None:
    _publish(tmp_path, ("005930", "kis:5"))
    live = _Live()
    watcher = DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[])  # type: ignore[arg-type]

    assert asyncio.run(watcher.tick(live)) == 0  # type: ignore[arg-type]
    assert live.calls == []


def test_run_logs_failure_and_keeps_polling(tmp_path, caplog) -> None:
    caplog.set_level(logging.ERROR)
    _publish(tmp_path, ("000660", "ls:1"))
    stop = asyncio.Event()

    def stop_after_second(count: int) -> None:
        if count == 2:
            stop.set()

    live = _Live(results=[OSError("ipc unreadable")], on_call=stop_after_second)
    watcher = DynamicPairWatcher(
        feed=_feed(tmp_path), session=_Session(), static_pairs=[], poll_s=0.001  # type: ignore[arg-type]
    )

    asyncio.run(watcher.run(lambda: live, stop))

    assert len(live.calls) == 2
    assert "stage=intraday_watch status=ERROR" in caplog.text


def test_watcher_rejects_non_positive_poll_interval(tmp_path) -> None:
    with pytest.raises(ValueError, match="poll_s"):
        DynamicPairWatcher(feed=_feed(tmp_path), session=_Session(), static_pairs=[], poll_s=0)  # type: ignore[arg-type]


def test_tier_a_and_disabled_flag_never_build_a_watcher(tmp_path, monkeypatch) -> None:
    settings = CollectorSettings(data_root=tmp_path)
    monkeypatch.delenv("KRX_ALPHA_INTRADAY_ENABLED", raising=False)
    assert (
        collect_stream._ls_dynamic_watch(
            session=_Session(),  # type: ignore[arg-type]
            session_date=_day(),
            settings=settings,
            vendor="ls",
            shard_index=1,
            static_pairs=[],
        )
        is None
    )
    monkeypatch.setenv("KRX_ALPHA_INTRADAY_ENABLED", "true")
    assert (
        collect_stream._ls_dynamic_watch(
            session=_Session(),  # type: ignore[arg-type]
            session_date=_day(),
            settings=settings,
            vendor="ls",
            shard_index=0,
            static_pairs=[],
        )
        is None
    )
    watcher = collect_stream._ls_dynamic_watch(
        session=_Session(),  # type: ignore[arg-type]
        session_date=_day(),
        settings=settings,
        vendor="ls",
        shard_index=1,
        static_pairs=[("005930", _CNT)],
    )
    assert watcher is not None
    assert watcher.pairs() == [("005930", _CNT)]


def _day():
    import datetime as dt

    return dt.date(2026, 11, 3)
