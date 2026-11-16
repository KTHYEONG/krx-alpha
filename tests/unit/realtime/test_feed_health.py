from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from zoneinfo import ZoneInfo

import pytest

from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
from src.realtime.feed_health import (
    FeedHealthPublisher,
    FeedHealthSink,
    FeedHealthTracker,
    feed_id_for,
    write_feed_health,
)
from src.realtime.lag_monitor import LagMonitor, delivery_lag_s
from src.realtime.session import DynamicFeed, DynamicPairWatcher
from src.realtime.streamer import RealtimeStreamer
from src.universe.intraday import DynamicEntry, DynamicPlan, read_dynamic_admissions, write_dynamic_admissions

_KST = ZoneInfo("Asia/Seoul")
_DAY = dt.date(2026, 11, 16)
_CNT = "H0STCNT0"
_ASP = "H0STASP0"
_BASE_S = dt.datetime(2026, 11, 16, 10, 0, 0, tzinfo=_KST).timestamp()
_EXPECTED_KEYS = {
    "schema",
    "feed_id",
    "vendor",
    "venue",
    "session",
    "session_date",
    "pid",
    "started_wall",
    "updated_wall",
    "connected",
    "disconnects_total",
    "last_disconnect_wall",
    "last_disconnect_reason",
    "frames_total",
    "last_frame_wall",
    "stream_last_frame_wall",
    "window_s",
    "window_frames",
    "lag_p50_s",
    "lag_p99_s",
    "lag_samples",
    "planned_pairs",
    "accepted_pairs",
    "rejected_pairs",
    "pairs_pending_since_wall",
}


def _hms(second_of_day: int) -> str:
    return f"{second_of_day // 3600:02d}{second_of_day % 3600 // 60:02d}{second_of_day % 60:02d}"


def _recv_ns(offset_s: float) -> int:
    return int((_BASE_S + offset_s) * 1_000_000_000)


def _event_second(offset_s: float) -> int:
    # Second-of-day of (base + offset) in KST, computed from the same instant the frame carries.
    stamp = dt.datetime.fromtimestamp(_BASE_S + offset_s, tz=_KST)
    return stamp.hour * 3600 + stamp.minute * 60 + stamp.second


def _frame(offset_s: float, *, stream: str = _CNT, lag_s: float = 0.0, event: str | None = None) -> L0Frame:
    stamp = event if event is not None else _hms(_event_second(offset_s - lag_s))
    return L0Frame(
        "ls", stream, "005930", f'{{"chetime":"{stamp}"}}', 1, _recv_ns(offset_s), 1, "ls-1", exchange_event_time=stamp
    )


def _tracker(**kwargs) -> FeedHealthTracker:
    options = {
        "feed_id": "ls:0",
        "vendor": "ls",
        "venue": "krx",
        "session": "regular",
        "session_date": _DAY,
        "pid": 7,
        "started_wall": _BASE_S,
        "wall_clock": lambda: _BASE_S + 1000.0,
    }
    options.update(kwargs)
    return FeedHealthTracker(**options)


class _Inner:
    def __init__(self) -> None:
        self.records: list[L0Frame] = []

    def record(self, frame: L0Frame) -> None:
        self.records.append(frame)

    def note_ack(self, vendor, ack) -> None:
        return None

    def note_gap(self, symbol, start_ns, end_ns, reason) -> None:
        return None

    def flush(self) -> int:
        return 0


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("ls", "krx", 2), "ls:2"),
        (("db", "krx", 0), "db:0"),
        (("kiwoom", "krx", 1), "kiwoom:1"),
        (("kis", "krx", 3), "kis-krx:3"),
        (("kis", "nxt", 1), "kis-nxt:1"),
        (("kis", "krx", 0, "2"), "kis:2"),
    ],
)
def test_feed_id_mapping(args, expected) -> None:
    assert feed_id_for(*args[:3], slot=args[3] if len(args) > 3 else None) == expected


def test_feed_id_rejects_unknown_vendor_and_slot_on_non_kis() -> None:
    with pytest.raises(ValueError, match="unknown vendor"):
        feed_id_for("nope", "krx", 0)
    with pytest.raises(ValueError, match="only defined for kis"):
        feed_id_for("ls", "krx", 0, slot="1")


def test_tracker_counts_frames_and_last_frame_per_stream() -> None:
    tracker = _tracker()
    tracker.observe(_frame(1.0, stream=_CNT))
    tracker.observe(_frame(2.0, stream=_ASP))
    tracker.observe(_frame(3.0, stream=_CNT))

    snap = tracker.snapshot(_BASE_S + 4.0)

    assert snap["frames_total"] == 3
    assert snap["last_frame_wall"] == pytest.approx(_BASE_S + 3.0)
    assert snap["stream_last_frame_wall"] == {_CNT: pytest.approx(_BASE_S + 3.0), _ASP: pytest.approx(_BASE_S + 2.0)}
    assert snap["window_frames"] == 3


def test_one_stream_silence_is_visible_per_stream() -> None:
    tracker = _tracker()
    for second in range(0, 30):
        tracker.observe(_frame(float(second), stream=_CNT))
    tracker.observe(_frame(0.5, stream=_ASP))

    snap = tracker.snapshot(_BASE_S + 30.0)

    assert snap["stream_last_frame_wall"][_CNT] == pytest.approx(_BASE_S + 29.0)
    assert snap["stream_last_frame_wall"][_ASP] == pytest.approx(_BASE_S + 0.5)


def test_lag_percentiles_are_nearest_rank() -> None:
    tracker = _tracker(sample_every=1, window_s=1000.0, max_samples=500)
    for index in range(100):
        tracker.observe(_frame(float(index), lag_s=float(index + 1)))

    snap = tracker.snapshot(_BASE_S + 100.0)

    assert snap["lag_samples"] == 100
    assert snap["lag_p50_s"] == pytest.approx(50.0)
    assert snap["lag_p99_s"] == pytest.approx(99.0)


def test_lag_percentiles_null_below_twenty_samples() -> None:
    tracker = _tracker(sample_every=1, window_s=1000.0)
    for index in range(19):
        tracker.observe(_frame(float(index), lag_s=2.0))

    snap = tracker.snapshot(_BASE_S + 20.0)

    assert snap["lag_samples"] == 19
    assert snap["lag_p50_s"] is None
    assert snap["lag_p99_s"] is None


def test_lag_rules_match_lag_monitor() -> None:
    clamped = _frame(10.0, event=_hms(_event_second(12.0)))
    discarded = _frame(10.0, event=_hms(_event_second(71.0)))
    missing = L0Frame("ls", _CNT, "005930", "{}", 1, _recv_ns(10.0), 1, "ls-1")
    normal = _frame(10.0, lag_s=3.0)

    assert delivery_lag_s(clamped) == 0.0
    assert delivery_lag_s(discarded) is None
    assert delivery_lag_s(missing) is None
    assert delivery_lag_s(normal) == pytest.approx(3.0)
    assert LagMonitor._lag_s(clamped) == delivery_lag_s(clamped)

    tracker = _tracker(sample_every=1, window_s=1000.0)
    for frame in (clamped, discarded, missing, normal):
        tracker.observe(frame)
    assert tracker.snapshot(_BASE_S + 20.0)["lag_samples"] == 2


def test_window_drops_old_samples_and_frames() -> None:
    tracker = _tracker(sample_every=1, window_s=60.0)
    for index in range(30):
        tracker.observe(_frame(float(index), lag_s=5.0))

    early = tracker.snapshot(_BASE_S + 30.0)
    late = tracker.snapshot(_BASE_S + 500.0)

    assert early["lag_samples"] == 30
    assert early["window_frames"] == 30
    assert late["lag_samples"] == 0
    assert late["window_frames"] == 0
    assert late["lag_p50_s"] is None


def test_sink_delegates_first_and_isolates_tracker_failures(caplog: pytest.LogCaptureFixture) -> None:
    class _Broken(FeedHealthTracker):
        def observe(self, frame: L0Frame) -> None:
            raise RuntimeError("boom")

    inner = _Inner()
    clock = [0.0]
    sink = FeedHealthSink(
        inner,
        _Broken(
            feed_id="ls:0", vendor="ls", venue="krx", session="regular", session_date=_DAY, pid=1, started_wall=0.0
        ),
        monotonic=lambda: clock[0],
    )
    frame = _frame(1.0)

    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            assert sink.record(frame) is None
        clock[0] = 30.0
        sink.record(frame)
        clock[0] = 61.0
        sink.record(frame)

    assert inner.records == [frame] * 5
    warnings = [r for r in caplog.records if "stage=feed_health status=OBSERVE_FAIL" in r.getMessage()]
    assert len(warnings) == 2


def test_publisher_writes_valid_json_periodically_and_final_snapshot_on_cancel(tmp_path) -> None:
    tracker = _tracker()
    tracker.note_connected(_BASE_S)
    path = tmp_path / "feed_status" / "ls__0.json"
    publisher = FeedHealthPublisher(tracker, path, interval_s=0.01)

    async def _drive() -> None:
        task = asyncio.ensure_future(publisher.run())
        await asyncio.sleep(0.05)
        assert json.loads(path.read_text(encoding="utf-8"))["connected"] is True
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_drive())

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["connected"] is False
    assert payload["schema"] == "feed-health.v1"
    assert list(path.parent.iterdir()) == [path]


def test_publisher_survives_write_failure(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    publisher = FeedHealthPublisher(_tracker(), blocker / "ls__0.json", interval_s=0.01)

    async def _drive() -> None:
        task = asyncio.ensure_future(publisher.run())
        await asyncio.sleep(0.05)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    with caplog.at_level(logging.WARNING):
        asyncio.run(_drive())

    warnings = [r for r in caplog.records if "stage=feed_health status=WRITE_FAIL" in r.getMessage()]
    assert len(warnings) == 1


def test_snapshot_keys_are_exactly_the_schema() -> None:
    snap = _tracker().snapshot(_BASE_S)

    assert set(snap) == _EXPECTED_KEYS
    assert snap["schema"] == "feed-health.v1"
    assert snap["session_date"] == "2026-11-16"


def test_write_feed_health_is_sorted_and_leaves_no_temp_file(tmp_path) -> None:
    path = tmp_path / "x.json"
    write_feed_health(path, {"b": 1, "a": 2})

    assert path.read_text(encoding="utf-8") == '{"a": 2, "b": 1}'
    assert path.stat().st_mode & 0o777 == 0o644
    assert [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


def test_pending_since_set_while_acks_outstanding_and_cleared_after_last_ack() -> None:
    clock = [100.0]
    tracker = _tracker(wall_clock=lambda: clock[0])

    tracker.note_acks(planned=3, accepted=0, rejected=0)
    assert tracker.snapshot(101.0)["pairs_pending_since_wall"] == 100.0

    clock[0] = 105.0
    tracker.note_acks(planned=0, accepted=1, rejected=0)
    assert tracker.snapshot(106.0)["pairs_pending_since_wall"] == 100.0

    tracker.note_acks(planned=0, accepted=0, rejected=1)
    assert tracker.snapshot(106.0)["pairs_pending_since_wall"] == 100.0

    clock[0] = 110.0
    tracker.note_acks(planned=0, accepted=1, rejected=0)
    snap = tracker.snapshot(111.0)
    assert snap["pairs_pending_since_wall"] is None
    assert (snap["planned_pairs"], snap["accepted_pairs"], snap["rejected_pairs"]) == (3, 2, 1)


def test_rejected_only_remainder_is_not_pending() -> None:
    tracker = _tracker()
    tracker.note_acks(planned=2, accepted=0, rejected=2)
    assert tracker.snapshot(_BASE_S)["pairs_pending_since_wall"] is None


def test_pair_counts_describe_current_connection_across_reconnects() -> None:
    tracker = _tracker()
    for cycle in range(3):
        tracker.note_connected(_BASE_S + cycle)
        tracker.note_acks(planned=200, accepted=190, rejected=10)
        tracker.note_disconnected(_BASE_S + cycle + 0.5, "disconnect")

    tracker.note_connected(_BASE_S + 3.0)
    tracker.note_acks(planned=200, accepted=200, rejected=0)

    snap = tracker.snapshot(_BASE_S + 4.0)
    assert (snap["planned_pairs"], snap["accepted_pairs"], snap["rejected_pairs"]) == (200, 200, 0)
    assert snap["disconnects_total"] == 3


def test_disconnect_replaces_dynamic_admission_with_replayed_total() -> None:
    clock = [100.0]
    tracker = _tracker(wall_clock=lambda: clock[0])
    tracker.note_connected(100.0)
    tracker.note_acks(planned=3, accepted=3, rejected=0)
    tracker.note_acks(planned=2, accepted=0, rejected=0)
    assert tracker.snapshot(101.0)["planned_pairs"] == 5

    tracker.note_disconnected(102.0, "disconnect")
    tracker.note_connected(103.0)
    tracker.note_acks(planned=4, accepted=4, rejected=0)

    snap = tracker.snapshot(104.0)
    assert (snap["planned_pairs"], snap["accepted_pairs"], snap["rejected_pairs"]) == (4, 4, 0)
    assert snap["pairs_pending_since_wall"] is None


def test_pending_since_cleared_on_disconnect_and_reset_on_next_pending_count() -> None:
    clock = [100.0]
    tracker = _tracker(wall_clock=lambda: clock[0])
    tracker.note_acks(planned=3, accepted=1, rejected=0)
    assert tracker.snapshot(101.0)["pairs_pending_since_wall"] == 100.0

    clock[0] = 105.0
    tracker.note_disconnected(105.0, "disconnect")
    assert tracker.snapshot(106.0)["pairs_pending_since_wall"] is None

    clock[0] = 110.0
    tracker.note_acks(planned=0, accepted=0, rejected=0)
    assert tracker.snapshot(111.0)["pairs_pending_since_wall"] is None

    tracker.note_acks(planned=2, accepted=0, rejected=0)
    assert tracker.snapshot(111.0)["pairs_pending_since_wall"] == 110.0


class _Adapter:
    name = "ls"
    capacity_pairs = 200

    async def connect(self) -> None:
        return None

    async def subscribe(self, pairs):
        return [VendorAck(s, t, i < 2, "00000" if i < 2 else "10011") for i, (s, t) in enumerate(pairs)]

    async def recv(self):
        raise VendorDisconnected("closed")

    async def aclose(self) -> None:
        return None


class _Sink:
    def record(self, frame) -> None:
        return None

    def note_ack(self, vendor, ack) -> None:
        return None

    def note_gap(self, symbol, start_ns, end_ns, reason) -> None:
        return None

    def flush(self) -> int:
        return 0


class _Observer:
    def __init__(self) -> None:
        self.events: list[tuple] = []

    def on_connected(self, wall: float) -> None:
        self.events.append(("connected",))

    def on_disconnected(self, wall: float, reason: str) -> None:
        self.events.append(("disconnected", reason))

    def on_acks(self, *, planned: int, accepted: int, rejected: int) -> None:
        self.events.append(("acks", planned, accepted, rejected))


def test_streamer_reports_connect_acks_and_disconnect_to_observer() -> None:
    observer = _Observer()

    async def _no_sleep(delay: float) -> None:
        return None

    streamer = RealtimeStreamer(
        adapter=_Adapter(),  # type: ignore[arg-type]
        sink=_Sink(),  # type: ignore[arg-type]
        replay_pairs=[("005930", _CNT), ("005930", _ASP), ("000660", _CNT)],
        observer=observer,
    )
    asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=1, sleep=_no_sleep))

    assert observer.events == [("acks", 3, 2, 1), ("connected",), ("disconnected", "disconnect")]


def test_streamer_without_observer_is_unchanged() -> None:
    async def _no_sleep(delay: float) -> None:
        return None

    streamer = RealtimeStreamer(
        adapter=_Adapter(),  # type: ignore[arg-type]
        sink=_Sink(),  # type: ignore[arg-type]
        replay_pairs=[("005930", _CNT)],
    )
    asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=1, sleep=_no_sleep))


class _Live:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.subscribed: list[tuple[str, str]] = []

    def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
        return tuple(self.subscribed)

    async def subscribe_live(self, pairs):
        if self.fail:
            raise VendorDisconnected("live gone")
        acks = [VendorAck(s, t, index == 0, "00000" if index == 0 else "10011") for index, (s, t) in enumerate(pairs)]
        self.subscribed.extend((a.symbol, a.stream) for a in acks if a.accepted)
        return acks


class _NullSession:
    def note_ack(self, **kwargs) -> None:
        return None


def _watch(tmp_path, observer) -> DynamicPairWatcher:
    write_dynamic_admissions(
        tmp_path / "dynamic.json",
        DynamicPlan(rev=1, entries=(DynamicEntry("000660", "ls:1", 1), DynamicEntry("035420", "ls:1", 1))),
    )
    feed = DynamicFeed(
        vendor="ls",
        feed="ls:1",
        streams=(_CNT,),
        ipc_path=tmp_path / "dynamic.json",
        membership_path=tmp_path / "membership.jsonl",
        ls_shard=1,
    )
    return DynamicPairWatcher(
        feed=feed,
        session=_NullSession(),  # type: ignore[arg-type]
        static_pairs=(),
        wall_ns=lambda: 1,
        observer=observer,
    )


def test_dynamic_watcher_reports_admissions_before_and_after_subscribe(tmp_path) -> None:
    observer = _Observer()
    watcher = _watch(tmp_path, observer)

    sent = asyncio.run(watcher.tick(_Live()))  # type: ignore[arg-type]

    assert sent == 2
    assert observer.events == [("acks", 2, 0, 0), ("acks", 0, 1, 1)]


def test_dynamic_watcher_failed_subscribe_counts_as_rejected(tmp_path) -> None:
    observer = _Observer()
    watcher = _watch(tmp_path, observer)

    with pytest.raises(VendorDisconnected):
        asyncio.run(watcher.tick(_Live(fail=True)))  # type: ignore[arg-type]

    assert observer.events == [("acks", 2, 0, 0), ("acks", 0, 0, 2)]
    assert read_dynamic_admissions(tmp_path / "dynamic.json").rev == 1


def test_collector_settings_default_feed_health_interval() -> None:
    from src.core.config import CollectorSettings

    assert CollectorSettings().feed_health_interval_s == pytest.approx(5.0)


def test_run_streaming_child_publishes_health_and_passes_observer(tmp_path, monkeypatch) -> None:
    from src.realtime.feed_health import FeedHealthConfig
    from src.realtime.streaming_child import run_streaming_child

    class _Http:
        async def close(self) -> None:
            return None

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _Http())
    seen: dict[str, object] = {}
    path = tmp_path / "feed_status" / "ls__0.json"
    tracker = _tracker()

    class _Streamer:
        def __init__(self, **kwargs) -> None:
            seen["observer"] = kwargs.get("observer")
            self._sink = kwargs["sink"]

        async def run_forever(self, stop, *, max_cycles=None) -> None:
            self._sink.record(_frame(1.0))
            await asyncio.sleep(0.03)

    class _Manifest:
        def record_frame_count(self, symbol, stream) -> None:
            return None

    class _Session:
        manifest = _Manifest()

        def record_frame(self, **kwargs) -> None:
            return None

    asyncio.run(
        run_streaming_child(
            session=_Session(),
            adapter_factory=lambda http: _Adapter(),  # type: ignore[arg-type,return-value]
            replay_pairs=[],
            silence_limit=lambda: None,
            max_cycles=1,
            streamer_factory=_Streamer,
            feed_health=FeedHealthConfig(tracker=tracker, path=path, interval_s=0.01),
        )
    )

    assert seen["observer"] is tracker
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["feed_id"] == "ls:0"
    assert payload["frames_total"] == 1
    assert payload["connected"] is False


def test_invalid_configuration_is_rejected(tmp_path) -> None:
    with pytest.raises(ValueError, match="unknown kis venue"):
        feed_id_for("kis", "xyz", 0)
    with pytest.raises(ValueError, match="must be positive"):
        _tracker(window_s=0.0)
    with pytest.raises(ValueError, match="interval_s must be positive"):
        FeedHealthPublisher(_tracker(), tmp_path / "x.json", interval_s=0.0)


def test_failed_serialization_leaves_no_temp_file(tmp_path) -> None:
    path = tmp_path / "x.json"
    with pytest.raises(TypeError):
        write_feed_health(path, {"bad": object()})  # type: ignore[dict-item]

    assert not path.exists()
    assert [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []
