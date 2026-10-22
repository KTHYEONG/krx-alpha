"""Invariant guards for the per-shard stale watchdog (pure module)."""

from __future__ import annotations

import datetime as dt

from src.core.session_anchors import standard_session_anchors
from src.orchestration.shard_watchdog import (
    ShardObservation,
    ShardVerdict,
    ShardWatchdog,
    shard_last_journal_wall,
    shard_window,
)
from src.realtime.contracts import MarketSession, MarketVenue

DAY = dt.date(2026, 9, 16)


def _obs(key, venue="nxt", running=True, started=None, journal=None):
    return ShardObservation(
        key=key, venue=venue, running=running, started_at_wall=started, last_journal_wall=journal
    )


def _wd(limit=120.0, budget=3):
    return ShardWatchdog(limit_s=limit, max_restarts_per_day=budget)


def test_fresh_shard_is_ok() -> None:
    now = 1_000_000.0
    (d,) = _wd().evaluate(
        [_obs("nxt:0", journal=now - 3)], now_wall=now, window_open_wall=now - 1000,
        in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.OK
    assert d.restart is False


def test_stale_triggers_restart() -> None:
    now = 1_000_000.0
    (d,) = _wd().evaluate(
        [_obs("nxt:0", journal=now - 121)], now_wall=now, window_open_wall=now - 1000,
        in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.STALE
    assert d.restart is True
    assert d.silent_s == 121


def test_threshold_equality_is_ok() -> None:
    now = 1_000_000.0
    (d,) = _wd().evaluate(
        [_obs("nxt:0", journal=now - 120)], now_wall=now, window_open_wall=now - 1000,
        in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.OK
    (d2,) = _wd().evaluate(
        [_obs("nxt:0", journal=now - 121)], now_wall=now, window_open_wall=now - 1000,
        in_window=True, day=DAY,
    )
    assert d2.verdict is ShardVerdict.STALE


def test_outside_window_never_stale() -> None:
    now = 1_000_000.0
    for in_window in (False,):
        (d,) = _wd().evaluate(
            [_obs("nxt:0", journal=now - 10_000)], now_wall=now, window_open_wall=now - 20_000,
            in_window=in_window, day=DAY,
        )
        assert d.verdict is ShardVerdict.OUTSIDE_WINDOW
        assert d.restart is False


def test_window_open_baseline() -> None:
    open_wall = 1_000_000.0
    wd = _wd()
    (d,) = wd.evaluate(
        [_obs("premarket", journal=open_wall - 86_400)], now_wall=open_wall + 60,
        window_open_wall=open_wall, in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.OK
    assert d.silent_s == 60
    (d2,) = wd.evaluate(
        [_obs("premarket", journal=open_wall - 86_400)], now_wall=open_wall + 121,
        window_open_wall=open_wall, in_window=True, day=DAY,
    )
    assert d2.verdict is ShardVerdict.STALE


def test_restarted_child_gets_full_limit() -> None:
    now = 1_000_000.0
    (d,) = _wd().evaluate(
        [_obs("nxt:0", started=now - 30, journal=now - 600)], now_wall=now,
        window_open_wall=now - 1000, in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.OK


def test_dead_child_not_judged() -> None:
    now = 1_000_000.0
    (d,) = _wd().evaluate(
        [_obs("nxt:0", running=False, journal=now - 10_000)], now_wall=now,
        window_open_wall=now - 20_000, in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.NOT_RUNNING
    assert d.restart is False


def test_clock_skew_clamps_to_zero() -> None:
    now = 1_000_000.0
    (d,) = _wd().evaluate(
        [_obs("nxt:0", journal=now + 50)], now_wall=now, window_open_wall=now - 1000,
        in_window=True, day=DAY,
    )
    assert d.silent_s == 0
    assert d.verdict is ShardVerdict.OK


def test_venue_wide_silence_no_restart() -> None:
    now = 1_000_000.0
    ds = _wd().evaluate(
        [_obs("nxt:0", journal=now - 500), _obs("nxt:1", journal=now - 600)],
        now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY,
    )
    assert [d.verdict for d in ds] == [ShardVerdict.VENUE_WIDE_SILENCE] * 2
    assert not any(d.restart for d in ds)


def test_one_fresh_sibling_keeps_other_eligible() -> None:
    now = 1_000_000.0
    ds = _wd().evaluate(
        [_obs("nxt:0", journal=now - 500), _obs("nxt:1", journal=now - 5)],
        now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY,
    )
    by_key = {d.key: d for d in ds}
    assert by_key["nxt:0"].verdict is ShardVerdict.STALE
    assert by_key["nxt:0"].restart is True
    assert by_key["nxt:1"].verdict is ShardVerdict.OK


def test_venues_independent() -> None:
    now = 1_000_000.0
    ds = _wd().evaluate(
        [_obs("nxt:0", journal=now - 500), _obs("nxt:1", journal=now - 600), _obs("krx:0", journal=now - 5, venue="krx")],
        now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY,
    )
    by_key = {d.key: d for d in ds}
    assert by_key["nxt:0"].verdict is ShardVerdict.VENUE_WIDE_SILENCE
    assert by_key["nxt:1"].verdict is ShardVerdict.VENUE_WIDE_SILENCE
    assert by_key["krx:0"].verdict is ShardVerdict.OK


def test_single_running_shard_judged_alone() -> None:
    now = 1_000_000.0
    ds = _wd().evaluate(
        [
            _obs("nxt:0", journal=now - 500),
            _obs("nxt:1", running=False, journal=now - 600),
        ],
        now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY,
    )
    by_key = {d.key: d for d in ds}
    assert by_key["nxt:0"].verdict is ShardVerdict.STALE
    assert by_key["nxt:1"].verdict is ShardVerdict.NOT_RUNNING


def test_restart_budget_exhausts_then_resets_next_day() -> None:
    wd = _wd(budget=3)
    now = 1_000_000.0
    kwargs = {"now_wall": now, "window_open_wall": now - 1000, "in_window": True}
    for _ in range(3):
        (d,) = wd.evaluate([_obs("nxt:0", journal=now - 500)], day=DAY, **kwargs)
        assert d.verdict is ShardVerdict.STALE
        wd.record_restart("nxt:0", day=DAY)
    (d,) = wd.evaluate([_obs("nxt:0", journal=now - 500)], day=DAY, **kwargs)
    assert d.verdict is ShardVerdict.BUDGET_EXHAUSTED
    assert d.restart is False
    nxt = DAY + dt.timedelta(days=1)
    (d2,) = wd.evaluate([_obs("nxt:0", journal=now - 500)], day=nxt, **kwargs)
    assert d2.verdict is ShardVerdict.STALE


def test_zero_budget_never_restarts_and_keys_independent() -> None:
    wd = _wd(budget=0)
    now = 1_000_000.0
    (d,) = wd.evaluate(
        [_obs("nxt:0", journal=now - 500)], now_wall=now, window_open_wall=now - 1000,
        in_window=True, day=DAY,
    )
    assert d.verdict is ShardVerdict.BUDGET_EXHAUSTED
    wd2 = _wd(budget=1)
    wd2.record_restart("nxt:0", day=DAY)
    now2 = now
    (a,) = wd2.evaluate(
        [_obs("nxt:0", journal=now2 - 500)], now_wall=now2, window_open_wall=now2 - 1000,
        in_window=True, day=DAY,
    )
    (b,) = wd2.evaluate(
        [_obs("nxt:1", journal=now2 - 500)], now_wall=now2, window_open_wall=now2 - 1000,
        in_window=True, day=DAY,
    )
    assert a.verdict is ShardVerdict.BUDGET_EXHAUSTED
    assert b.verdict is ShardVerdict.STALE


def test_evaluate_is_pure() -> None:
    wd = _wd()
    now = 1_000_000.0
    obs = [_obs("nxt:0", journal=now - 500)]
    first = wd.evaluate(obs, now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY)
    second = wd.evaluate(obs, now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY)
    assert first == second
    (third,) = wd.evaluate(obs, now_wall=now, window_open_wall=now - 1000, in_window=True, day=DAY)
    assert third.verdict is ShardVerdict.STALE


def test_record_restart_returns_day_total() -> None:
    wd = _wd()
    assert wd.record_restart("nxt:0", day=DAY) == 1
    assert wd.record_restart("nxt:0", day=DAY) == 2


def test_validation() -> None:
    import pytest

    with pytest.raises(ValueError, match="limit_s"):
        ShardWatchdog(limit_s=0, max_restarts_per_day=3)
    with pytest.raises(ValueError, match="max_restarts_per_day"):
        ShardWatchdog(limit_s=120, max_restarts_per_day=-1)


def test_window_computation_standard() -> None:
    anchors = standard_session_anchors(DAY)
    nxt_open, nxt_end = shard_window(MarketVenue.NXT, MarketSession.NXT_AFTER, day=DAY, anchors=anchors)
    assert (nxt_open.time(), nxt_end.time()) == (dt.time(15, 40), dt.time(20, 0))
    krx_open, krx_end = shard_window(MarketVenue.KRX, MarketSession.KRX_AFTER, day=DAY, anchors=anchors)
    assert (krx_open.time(), krx_end.time()) == (dt.time(16, 0), dt.time(20, 0))
    pre_open, pre_end = shard_window(MarketVenue.NXT, MarketSession.NXT_PRE, day=DAY, anchors=anchors)
    assert (pre_open.time(), pre_end.time()) == (dt.time(8, 0), dt.time(8, 50))
    assert nxt_open.tzinfo is not None


def test_window_computation_shifted() -> None:
    from src.core.session_anchors import AnchorSource, SessionAnchors

    anchors = SessionAnchors(
        date=DAY,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(21, 0),
        source=AnchorSource.VENDOR,
    )
    pre_open, pre_end = shard_window(MarketVenue.NXT, MarketSession.NXT_PRE, day=DAY, anchors=anchors)
    assert (pre_open.time(), pre_end.time()) == (dt.time(9, 0), dt.time(9, 50))
    krx_open, krx_end = shard_window(MarketVenue.KRX, MarketSession.KRX_AFTER, day=DAY, anchors=anchors)
    assert (krx_open.time(), krx_end.time()) == (dt.time(17, 0), dt.time(21, 0))
    nxt_open, _ = shard_window(MarketVenue.NXT, MarketSession.NXT_AFTER, day=DAY, anchors=anchors)
    assert nxt_open.time() == dt.time(16, 40)


def test_window_unsupported_pair() -> None:
    import pytest

    anchors = standard_session_anchors(DAY)
    with pytest.raises(ValueError, match="premarket window"):
        shard_window(MarketVenue.KRX, MarketSession.NXT_PRE, day=DAY, anchors=anchors)
    with pytest.raises(ValueError, match="regular window"):
        shard_window(MarketVenue.NXT, MarketSession.REGULAR, day=DAY, anchors=anchors)


def test_regular_window_is_open_to_closing_auction() -> None:
    anchors = standard_session_anchors(DAY)
    open_dt, end_dt = shard_window(MarketVenue.KRX, MarketSession.REGULAR, day=DAY, anchors=anchors)
    assert (open_dt.time(), end_dt.time()) == (dt.time(9, 0), dt.time(15, 20))


def _journal_path(root, *, stream, day, hour, tag):
    from src.storage.layout import l0_partition_key, l0_partition_relpath

    key = l0_partition_key("kis", "nxt", "nxt_after", stream, day)
    assert key is not None
    part = root / l0_partition_relpath(key)
    part.mkdir(parents=True, exist_ok=True)
    path = part / f"{hour:02d}.{tag}.jsonl.zst"
    path.write_bytes(b"x")
    return path


def test_journal_evidence_isolation(tmp_path) -> None:
    import os

    root = tmp_path / "l0"
    other_day = DAY + dt.timedelta(days=1)
    p_s0 = _journal_path(root, stream="H0NXCNT0", day=DAY, hour=16, tag="s0")
    p_s1 = _journal_path(root, stream="H0NXCNT0", day=DAY, hour=16, tag="s1")
    _journal_path(root, stream="H0NXCNT0", day=other_day, hour=16, tag="s0")
    untagged = p_s0.parent / "17.jsonl.zst"
    untagged.write_bytes(b"x")
    os.utime(p_s0, (1_000_000.0, 1_000_500.0))
    os.utime(p_s1, (1_000_000.0, 1_000_900.0))
    os.utime(untagged, (1_000_000.0, 1_001_900.0))

    got = shard_last_journal_wall(
        root, vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0", "H0NXASP0"), day=DAY, shard_index=0,
    )
    assert got == 1_000_500.0
    assert shard_last_journal_wall(
        root, vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0",), day=other_day, shard_index=1,
    ) is None
    assert shard_last_journal_wall(
        tmp_path / "missing", vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0",), day=DAY, shard_index=0,
    ) is None


def test_journal_max_across_streams(tmp_path) -> None:
    import os

    root = tmp_path / "l0"
    a = _journal_path(root, stream="H0NXCNT0", day=DAY, hour=16, tag="s2")
    b = _journal_path(root, stream="H0NXASP0", day=DAY, hour=17, tag="s2")
    os.utime(a, (1_000_000.0, 1_000_100.0))
    os.utime(b, (1_000_000.0, 1_000_200.0))
    got = shard_last_journal_wall(
        root, vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0", "H0NXASP0"), day=DAY, shard_index=2,
    )
    assert got == 1_000_200.0


def test_journal_mtime_semantics(tmp_path) -> None:
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.storage.journal import L0JournalWriter

    root = tmp_path / "l0"
    recv_wall_ns = 1_750_000_000_000_000_000
    writer = L0JournalWriter(
        root=root, vendor="kis", venue=MarketVenue.NXT, session=MarketSession.NXT_AFTER,
        stream="H0NXCNT0", file_tag="s0",
    )
    writer.append(
        raw="005930^154001", recv_mono_ns=1, recv_wall_ns=recv_wall_ns,
        conn_id="c", conn_seq=1, symbol="005930",
    )
    writer.flush()
    import datetime as _dt

    day = _dt.datetime.fromtimestamp(recv_wall_ns / 1e9, tz=_dt.UTC).astimezone(
        dt.timezone(dt.timedelta(hours=9))
    ).date()
    got = shard_last_journal_wall(
        root, vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0",), day=day, shard_index=0,
    )
    assert got == recv_wall_ns / 1e9


def test_window_venue_session_mismatch() -> None:
    import pytest

    anchors = standard_session_anchors(DAY)
    with pytest.raises(ValueError, match="nxt_after window"):
        shard_window(MarketVenue.KRX, MarketSession.NXT_AFTER, day=DAY, anchors=anchors)
    with pytest.raises(ValueError, match="krx_after window"):
        shard_window(MarketVenue.NXT, MarketSession.KRX_AFTER, day=DAY, anchors=anchors)


def test_journal_unknown_route_returns_none(tmp_path) -> None:
    assert shard_last_journal_wall(
        tmp_path / "l0", vendor="kis", venue="bogus", session="nxt_after",
        streams=("H0NXCNT0",), day=DAY, shard_index=0,
    ) is None


def test_journal_scan_survives_glob_error(tmp_path, monkeypatch) -> None:
    import pathlib

    root = tmp_path / "l0"
    _journal_path(root, stream="H0NXCNT0", day=DAY, hour=16, tag="s0")
    real_glob = pathlib.Path.glob
    calls = {"n": 0}

    def _glob(self, pattern):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("gone")
        return real_glob(self, pattern)

    monkeypatch.setattr(pathlib.Path, "glob", _glob)
    got = shard_last_journal_wall(
        root, vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0", "H0NXASP0"), day=DAY, shard_index=0,
    )
    assert got is None


def test_journal_scan_skips_vanished_files(tmp_path, monkeypatch) -> None:
    import os
    import pathlib

    root = tmp_path / "l0"
    path = _journal_path(root, stream="H0NXCNT0", day=DAY, hour=16, tag="s0")
    os.utime(path, (1_000_000.0, 1_000_100.0))
    real_stat = pathlib.Path.stat

    def _stat(self, *args, **kwargs):
        if self.name.endswith(".jsonl.zst"):
            raise OSError("vanished")
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "stat", _stat)
    got = shard_last_journal_wall(
        root, vendor="kis", venue="nxt", session="nxt_after",
        streams=("H0NXCNT0",), day=DAY, shard_index=0,
    )
    assert got is None
