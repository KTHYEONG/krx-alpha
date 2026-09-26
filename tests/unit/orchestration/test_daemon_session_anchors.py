"""Session-anchor wiring cases: persistence, shifted state machine, watchdogs, reselection."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def _csat_anchors(day):
    import datetime as _dt

    from src.core.session_anchors import AnchorSource, SessionAnchors

    return SessionAnchors(
        date=day,
        regular_open=_dt.time(10, 0),
        closing_auction_start=_dt.time(16, 20),
        regular_close=_dt.time(16, 30),
        after_market_end=_dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )


def _business_day(day, *, anchors=None, next_anchors=None):
    import datetime as _dt

    from src.marketdata.toss_calendar import TradingDay as _TD

    return _TD(
        date=day,
        is_business_day=True,
        previous_business_day=day - _dt.timedelta(days=1),
        next_business_day=day + _dt.timedelta(days=1),
        anchors=anchors,
        next_anchors=next_anchors,
    )


def _next_anchors(day):
    import datetime as _dt

    from src.core.session_anchors import AnchorSource, SessionAnchors

    return SessionAnchors(
        date=day,
        regular_open=_dt.time(10, 0),
        closing_auction_start=_dt.time(16, 20),
        regular_close=_dt.time(16, 30),
        after_market_end=_dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )


def _runner(tmp_path, monkeypatch, daemon_mod, *, business=None, after_market_enabled=False):
    import pathlib

    from src.core.config import CollectorSettings, resolve_collector_runtime

    runtime = resolve_collector_runtime(
        collector=CollectorSettings(
            data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=after_market_enabled
        )
    )
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: business)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda *a, **kw: {})

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            pass

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    now_holder: dict = {}

    def _now():
        return now_holder["now"]

    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=_now, sleep=lambda _: None)
    return runtime, runner, now_holder


def test_resolve_persists_today_and_next_day_anchors(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import load_session_anchors

    day = dt.date(2026, 11, 19)
    nxt = dt.date(2026, 11, 20)
    vendor_day = _business_day(day, anchors=_csat_anchors(day), next_anchors=_next_anchors(nxt))
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: vendor_day)

    out = daemon_mod._resolve_trading_day_with_cache(
        day, tmp_path / "cache.json", tmp_path / "calendar"
    )

    assert out is vendor_day
    assert load_session_anchors(tmp_path / "calendar", day) == _csat_anchors(day)
    assert load_session_anchors(tmp_path / "calendar", nxt) == _next_anchors(nxt)


def test_resolve_returns_day_when_anchor_write_fails(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging

    import src.orchestration.daemon as daemon_mod

    day = dt.date(2026, 11, 19)
    vendor_day = _business_day(day, anchors=_csat_anchors(day))
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: vendor_day)

    def _boom(directory, anchors):
        raise OSError("unwritable")

    monkeypatch.setattr(daemon_mod, "save_session_anchors", _boom)

    with caplog.at_level(logging.WARNING):
        out = daemon_mod._resolve_trading_day_with_cache(
            day, tmp_path / "cache.json", tmp_path / "calendar"
        )

    assert out is vendor_day
    assert any(
        r.levelno == logging.WARNING and "stage=session_anchors status=WRITE_FAIL" in r.getMessage()
        for r in caplog.records
    )
    # 앵커 쓰기 실패가 거래일 캐시 쓰기를 막지 않는다 (독립 쓰기).
    assert (tmp_path / "cache.json").exists()


def test_csat_day_stays_full_active_until_shifted_close(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.calendar import SessionState
    from src.core.session_anchors import save_session_anchors

    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod,
        business=_business_day(day), after_market_enabled=True,
    )
    save_session_anchors(runtime.paths.session_calendar_dir, _csat_anchors(day))
    now_holder["now"] = dt.datetime(2026, 11, 19, 16, 10, tzinfo=ZoneInfo("Asia/Seoul"))

    runner.step(now_holder["now"])

    assert runner.state.prev_state is SessionState.FULL_ACTIVE


def test_ingest_watchdog_silent_before_shifted_open(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import save_session_anchors

    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod, business=_business_day(day)
    )
    save_session_anchors(runtime.paths.session_calendar_dir, _csat_anchors(day))
    now_holder["now"] = dt.datetime(2026, 11, 19, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        runner.step(now_holder["now"])

    assert "status=STALE" not in caplog.text


def test_aftermarket_reselection_deferred_to_shifted_close(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import save_session_anchors

    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod,
        business=_business_day(day), after_market_enabled=True,
    )
    save_session_anchors(runtime.paths.session_calendar_dir, _csat_anchors(day))
    calls: list = []
    monkeypatch.setattr(
        daemon_mod, "refresh_aftermarket_candidates", lambda **kw: calls.append(kw)
    )
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: object())

    now_holder["now"] = dt.datetime(2026, 11, 19, 15, 35, tzinfo=ZoneInfo("Asia/Seoul"))
    runner.step(now_holder["now"])
    assert calls == []

    now_holder["now"] = dt.datetime(2026, 11, 19, 16, 32, tzinfo=ZoneInfo("Asia/Seoul"))
    runner.step(now_holder["now"])
    assert len(calls) == 1


def test_standard_day_transitions_and_watchdog_unchanged(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import types
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.calendar import SessionState

    kst = ZoneInfo("Asia/Seoul")
    digests: list = []
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **kw: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda *a, **kw: digests.append(a))
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **kw: types.SimpleNamespace(
            deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True
        ),
    )

    def _probe(time_hms):
        import pathlib

        from src.core.config import CollectorSettings, resolve_collector_runtime

        h, m, s = time_hms
        runtime = resolve_collector_runtime(
            collector=CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
        )
        monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_day(d))
        monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
        monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda *a, **kw: {})

        class _FakeSupervisor:
            def __init__(self, *, cmd, breaker=None):
                pass

            def ensure_running(self):
                return "started"

            def stop(self, *, timeout_s=15.0):
                return "graceful"

        monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
        now = dt.datetime(2026, 9, 14, h, m, s, tzinfo=kst)
        runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: now, sleep=lambda _: None)
        with caplog.at_level(logging.CRITICAL):
            runner.step(now)
        return runner

    assert _probe((9, 4, 0)).state.prev_state is SessionState.FULL_ACTIVE
    assert "status=STALE" not in caplog.text
    caplog.clear()
    _probe((9, 6, 0))
    assert "status=STALE" in caplog.text
    caplog.clear()
    _probe((15, 24, 0))
    assert "status=STALE" in caplog.text
    caplog.clear()
    _probe((15, 26, 0))
    assert "status=STALE" not in caplog.text
    assert _probe((15, 39, 59)).state.prev_state is SessionState.FULL_ACTIVE
    assert _probe((15, 40, 0)).state.prev_state is SessionState.POST_MARKET_EOD
    assert len(digests) == 1


def test_resolve_logs_vendor_missing_for_business_day_without_anchors(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging

    import src.orchestration.daemon as daemon_mod

    day = dt.date(2026, 11, 19)
    vendor_day = _business_day(day)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: vendor_day)

    with caplog.at_level(logging.WARNING):
        out = daemon_mod._resolve_trading_day_with_cache(
            day, tmp_path / "cache.json", tmp_path / "calendar"
        )

    assert out is vendor_day
    assert any(
        r.levelno == logging.WARNING and "stage=session_anchors status=DEFAULT" in r.getMessage()
        and "reason=vendor_missing" in r.getMessage()
        for r in caplog.records
    )


def _stub_aftermarket_plan(monkeypatch, daemon_mod):
    from types import SimpleNamespace

    from src.storage.market_phase import MarketVenue

    shards = (
        SimpleNamespace(venue=MarketVenue.NXT, shard_index=0),
        SimpleNamespace(venue=MarketVenue.KRX, shard_index=0),
    )
    monkeypatch.setattr(daemon_mod, "read_candidate_snapshot", lambda *a, **kw: SimpleNamespace(candidates=[]))
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())
    monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", lambda **kw: shards)
    monkeypatch.setattr(daemon_mod, "_aftermarket_stream_cmd", lambda *a, **kw: ["true"])


@pytest.mark.parametrize(
    ("hhmm", "expected"),
    [((16, 50), {"nxt:0"}), ((17, 1), {"nxt:0", "krx:0"})],
)
def test_csat_aftermarket_venues_start_at_shifted_times(tmp_path, monkeypatch, hhmm, expected) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import save_session_anchors

    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod,
        business=_business_day(day), after_market_enabled=True,
    )
    _stub_aftermarket_plan(monkeypatch, daemon_mod)
    save_session_anchors(runtime.paths.session_calendar_dir, _csat_anchors(day))
    now_holder["now"] = dt.datetime(2026, 11, 19, *hhmm, tzinfo=ZoneInfo("Asia/Seoul"))

    runner.step(now_holder["now"])

    # 표준 시각(NXT 15:40, KRX 16:00)이었다면 16:50 에 두 venue 가 모두 떴을 것이다.
    assert set(runner._children.aftermarket) == expected


def test_csat_restart_without_prefetch_resolves_anchors_before_state(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.calendar import SessionState
    from src.core.session_anchors import save_session_anchors

    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod,
        business=_business_day(day), after_market_enabled=True,
    )
    calls: list[dt.date] = []

    def _resolver(d, cache_path, anchors_dir):
        calls.append(d)
        save_session_anchors(anchors_dir, _csat_anchors(d))
        return _business_day(d, anchors=_csat_anchors(d))

    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", _resolver)
    eod = []
    monkeypatch.setattr(daemon_mod, "_run_eod_housekeeping", lambda *a, **kw: eod.append(a))
    now_holder["now"] = dt.datetime(2026, 11, 19, 16, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    runner.step(now_holder["now"])

    assert calls == [day]
    assert runner.state.prev_state is SessionState.FULL_ACTIVE
    assert eod == []


def test_csat_eod_reconciliation_receives_day_anchors(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import save_session_anchors

    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod,
        business=_business_day(day), after_market_enabled=True,
    )
    save_session_anchors(runtime.paths.session_calendar_dir, _csat_anchors(day))
    seen: dict = {}

    def _reconcile(**kwargs):
        seen.update(kwargs)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **kw: SimpleNamespace(maintenance_ok=True, offload_ok=True, deleted=0, uploaded=0, purged=0),
    )
    monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **kw: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda *a, **kw: True)
    now_holder["now"] = dt.datetime(2026, 11, 19, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    runner.step(now_holder["now"])

    assert seen["regular_open"] == dt.time(10, 0)
    assert seen["regular_close"] == dt.time(16, 30)


def test_csat_snapshot_child_supervised_until_shifted_run_end(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import save_session_anchors

    monkeypatch.setenv("KRX_ALPHA_SNAPSHOT_ENABLED", "true")
    day = dt.date(2026, 11, 19)
    runtime, runner, now_holder = _runner(
        tmp_path, monkeypatch, daemon_mod,
        business=_business_day(day), after_market_enabled=True,
    )
    save_session_anchors(runtime.paths.session_calendar_dir, _csat_anchors(day))
    ensured: list[str] = []

    class _Snap:
        last_exit_code = None

        def ensure_running(self):
            ensured.append("snapshot")
            return "running"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    runner._children.snapshot = _Snap()
    # 같은 날 오케스트레이션이 이미 끝난 상태로 두어 첫 step 이 감독자를 정리·교체하지 않게 한다
    runner.state.orchestration_day = day
    runner.state.orchestrated_for = day
    now_holder["now"] = dt.datetime(2026, 11, 19, 16, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    runner.step(now_holder["now"])

    # 표준 run_end(15:39) 이후지만 수능일 run_end(16:39) 이전이므로 계속 감독한다.
    assert ensured == ["snapshot"]
