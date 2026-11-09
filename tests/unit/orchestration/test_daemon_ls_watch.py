"""Invariant guards for the after-close LS shard watchdog: detect and page, never restart."""

from __future__ import annotations

import datetime as dt
import logging
import os
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.alerts import PageKind, page_kind
from src.core.calendar import SessionState
from src.core.config import CollectorSettings, ShardWatchdogSettings, resolve_collector_runtime
from src.core.session_anchors import standard_session_anchors
from src.orchestration.daemon import StreamChildKey
from src.realtime.ls_sharding import LsShard

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 11, 9)
STREAM = "S3_"
LIMIT_S = 900.0


def _at(hour: int, minute: int = 0, second: int = 0) -> dt.datetime:
    return dt.datetime(DAY.year, DAY.month, DAY.day, hour, minute, second, tzinfo=KST)


class _FakeSup:
    def __init__(self, *, running: bool = True, started_at: float | None = None) -> None:
        self._running = running
        self.started_at_wall = started_at
        self.calls: list[str] = []

    def is_running(self) -> bool:
        return self._running

    def ensure_running(self) -> str:
        self.calls.append("ensure_running")
        return "running"

    def stop_for_restart(self, *, reason: str, timeout_s: float = 15.0) -> str:
        self.calls.append("stop_for_restart")
        return "graceful"

    def stop(self, *, timeout_s: float = 15.0) -> str:
        self.calls.append("stop")
        return "graceful"


def _runner(tmp_path, *, wd: ShardWatchdogSettings | None = None) -> daemon_mod.DaemonRunner:
    collector = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(
        collector=collector,
        shard_watchdog=wd or ShardWatchdogSettings(ls_after_close_stale_limit_s=LIMIT_S),
    )
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(17), sleep=lambda s: None)


def _plan(*indexes: int) -> tuple[LsShard, ...]:
    return tuple(
        LsShard(shard_index=i, symbols=("005930",), streams=(STREAM,), dynamic_reserve_symbols=0, window_standby=i == 0)
        for i in indexes
    )


def _install(runner, fakes: dict[int, _FakeSup], *, plan_day: dt.date | None = DAY) -> None:
    runner._state.ls_shard_plan = _plan(*sorted(fakes))
    runner._state.ls_shard_plan_day = plan_day
    runner._children.regular = fakes.get(0)
    if 1 in fakes:
        runner._children.set(StreamChildKey.REGULAR_LS_B, fakes[1])


def _journal(runner, *, shard_index: int, age_s: float, now: dt.datetime, hour: int = 15) -> None:
    part = runner._paths.journal_root / "ls" / "krx" / "regular" / STREAM / f"dt={DAY.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    name = f"{hour:02d}.jsonl.zst" if shard_index == 0 else f"{hour:02d}.s{shard_index}.jsonl.zst"
    path = part / name
    path.write_bytes(b"x")
    ts = now.timestamp() - age_s
    os.utime(path, (ts, ts))


def _watch(runner, now: dt.datetime, state: SessionState = SessionState.AFTER_MARKET_ACTIVE) -> None:
    runner._watch_ls_shards(now, state, DAY, standard_session_anchors(DAY))


@pytest.fixture(autouse=True)
def _reset_watchdog_dedupe():
    from src.core.observability import reset_once_per_day

    reset_once_per_day()
    yield
    reset_once_per_day()


def _ls_records(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if "shard_watchdog" in r.getMessage()]


def test_silent_shard_pages_once(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    fakes = {0: _FakeSup(), 1: _FakeSup()}
    _install(runner, fakes)
    _journal(runner, shard_index=0, age_s=10, now=now)
    _journal(runner, shard_index=1, age_s=1000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
        _watch(runner, now)
    pages = [r for r in caplog.records if page_kind(r) is PageKind.LS_SHARD_STALE]
    assert len(pages) == 1
    assert "reason=ls_shard_silent shard=ls-01" in pages[0].getMessage()
    assert not any("shard=ls-00" in r.getMessage() for r in caplog.records)
    assert all(not f.calls for f in fakes.values())


def test_normal_off_regular_silence_does_not_page(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=0, age_s=450, now=now)
    _journal(runner, shard_index=1, age_s=450, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    assert not _ls_records(caplog)


def test_venue_wide_silence_not_stale_paged_early(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=0, age_s=1000, now=now)
    _journal(runner, shard_index=1, age_s=1000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    msgs = [r.getMessage() for r in caplog.records if "shard_watchdog" in r.getMessage()]
    assert len(msgs) == 1
    assert "reason=venue_wide_silence venue=krx" in msgs[0]
    assert not any(page_kind(r) is PageKind.LS_SHARD_STALE for r in caplog.records)


def test_venue_wide_silence_pages_after_extended_silence(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=0, age_s=2000, now=now)
    _journal(runner, shard_index=1, age_s=2000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
        _watch(runner, now)
    pages = [r for r in caplog.records if page_kind(r) is PageKind.LS_SHARD_STALE]
    assert len(pages) == 1
    assert "reason=ls_venue_wide_silent venue=krx" in pages[0].getMessage()
    assert pages[0].levelno == logging.CRITICAL


@pytest.mark.parametrize(("hour", "minute"), [(15, 20), (20, 0)])
def test_no_check_outside_window(tmp_path, caplog, monkeypatch, hour, minute) -> None:
    runner = _runner(tmp_path)
    now = _at(hour, minute)
    fakes = {0: _FakeSup(), 1: _FakeSup()}
    _install(runner, fakes)
    reads: list[str] = []
    monkeypatch.setattr(
        daemon_mod,
        "ls_shard_last_journal_wall",
        lambda *a, **k: reads.append("read") or None,
    )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    assert reads == []
    assert not _ls_records(caplog)


def test_no_plan_no_observation(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()}, plan_day=dt.date(2026, 11, 6))
    _journal(runner, shard_index=1, age_s=5000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    assert not _ls_records(caplog)


def test_wrong_state_not_checked(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=1, age_s=5000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now, state=SessionState.STREAMER_ACTIVE)
    assert not _ls_records(caplog)


def test_disabled_by_ls_flag(tmp_path, caplog) -> None:
    runner = _runner(tmp_path, wd=ShardWatchdogSettings(ls_after_close_enabled=False))
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=1, age_s=5000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    assert not _ls_records(caplog)


def test_disabled_by_master_flag(tmp_path, caplog) -> None:
    runner = _runner(tmp_path, wd=ShardWatchdogSettings(enabled=False))
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=1, age_s=5000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    assert not _ls_records(caplog)


def test_shard_zero_reads_untagged_files(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    _journal(runner, shard_index=0, age_s=10, now=now)
    _journal(runner, shard_index=1, age_s=1000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    pages = [r.getMessage() for r in caplog.records if page_kind(r) is PageKind.LS_SHARD_STALE]
    assert len(pages) == 1
    assert "shard=ls-01" in pages[0]


def test_restarted_child_gets_full_limit(tmp_path, caplog) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(started_at=now.timestamp() - 100), 1: _FakeSup()})
    _journal(runner, shard_index=0, age_s=10, now=now)
    _journal(runner, shard_index=1, age_s=5000, now=now)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    pages = [r for r in caplog.records if page_kind(r) is PageKind.LS_SHARD_STALE]
    assert len(pages) == 1
    assert "shard=ls-01" in pages[0].getMessage()
    fresh = _runner(tmp_path / "second")
    _install(fresh, {0: _FakeSup(), 1: _FakeSup(started_at=now.timestamp() - 100)})
    _journal(fresh, shard_index=0, age_s=10, now=now)
    _journal(fresh, shard_index=1, age_s=5000, now=now)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(fresh, now)
    assert not any(page_kind(r) is PageKind.LS_SHARD_STALE for r in caplog.records)


def test_error_contained(tmp_path, caplog, monkeypatch) -> None:
    runner = _runner(tmp_path)
    now = _at(17)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})

    def _boom(*a, **k):
        raise OSError("disk gone")

    monkeypatch.setattr(daemon_mod, "ls_shard_last_journal_wall", _boom)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, now)
    assert any("status=ERROR scope=ls reason=disk gone" in r.getMessage() for r in caplog.records)
    assert not any(page_kind(r) is PageKind.LS_SHARD_STALE for r in caplog.records)


def test_watchdog_oserror_logged_once_per_day_with_traceback(tmp_path, caplog, monkeypatch) -> None:
    runner = _runner(tmp_path)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})
    probes: list[int] = []

    def _denied(*a, **k):
        probes.append(1)
        raise PermissionError("denied")

    monkeypatch.setattr(daemon_mod, "ls_shard_last_journal_wall", _denied)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        for offset in range(100):
            _watch(runner, _at(17) + dt.timedelta(seconds=offset))
        assert len(probes) >= 100
        errors = [r for r in _ls_records(caplog) if "status=ERROR" in r.getMessage()]
        assert len(errors) == 1
        assert "scope=ls reason=denied date=2026-11-09" in errors[0].getMessage()
        assert errors[0].exc_info is not None
        runner._log_watchdog_error("ls", PermissionError("denied"), dt.datetime(2026, 11, 10, 17, 0, tzinfo=KST))
    errors = [r for r in _ls_records(caplog) if "status=ERROR" in r.getMessage()]
    assert len(errors) == 2
    assert "date=2026-11-10" in errors[1].getMessage()


def test_different_scopes_log_independently(tmp_path, caplog, monkeypatch) -> None:
    runner = _runner(tmp_path)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})

    def _ls_boom(*a, **k):
        raise OSError("ls gone")

    def _aftermarket_boom(self, *a, **k):
        raise OSError("aftermarket gone")

    monkeypatch.setattr(daemon_mod, "ls_shard_last_journal_wall", _ls_boom)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_watch_aftermarket_shards_guarded", _aftermarket_boom)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, _at(17))
        runner._watch_aftermarket_shards(_at(17), DAY, standard_session_anchors(DAY))
    errors = [r.getMessage() for r in _ls_records(caplog) if "status=ERROR" in r.getMessage()]
    assert len(errors) == 2
    assert any("scope=ls reason=ls gone" in m for m in errors)
    assert any("scope=aftermarket reason=aftermarket gone" in m for m in errors)


def test_watchdog_valueerror_same_path(tmp_path, caplog, monkeypatch) -> None:
    runner = _runner(tmp_path)
    _install(runner, {0: _FakeSup(), 1: _FakeSup()})

    def _bad(*a, **k):
        raise ValueError("bad clock")

    monkeypatch.setattr(daemon_mod, "ls_shard_last_journal_wall", _bad)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _watch(runner, _at(17))
    errors = [r for r in _ls_records(caplog) if "status=ERROR" in r.getMessage()]
    assert len(errors) == 1
    assert "scope=ls reason=bad clock" in errors[0].getMessage()
    assert errors[0].exc_info is not None
