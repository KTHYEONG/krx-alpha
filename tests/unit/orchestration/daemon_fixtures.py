"""Shared daemon test fakes: snapshot supervisors and trading-day constructors."""

from __future__ import annotations


def _holiday_trading_day(day) -> object:
    import datetime as _dt

    from src.marketdata.toss_calendar import TradingDay as _TD

    return _TD(
        date=day,
        is_business_day=False,
        previous_business_day=day - _dt.timedelta(days=1),
        next_business_day=day + _dt.timedelta(days=1),
    )


def _business_trading_day(day) -> object:
    import datetime as _dt

    from src.marketdata.toss_calendar import TradingDay as _TD

    return _TD(
        date=day,
        is_business_day=True,
        previous_business_day=day - _dt.timedelta(days=1),
        next_business_day=day + _dt.timedelta(days=1),
    )


def _snapshot_fake_supervisor(monkeypatch, daemon_mod):
    created: list = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            self.ensure_calls = 0
            self.stop_calls: list = []
            created.append(self)

        def ensure_running(self):
            self.ensure_calls += 1
            return 'started'

        def stop(self, *, timeout_s=15.0):
            self.stop_calls.append(timeout_s)
            return 'graceful'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)
    return created


def _snapshot_ready_daemon(monkeypatch, daemon_mod):
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)
