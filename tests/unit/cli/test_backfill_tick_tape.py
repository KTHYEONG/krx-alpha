"""CLI wiring tests for backfill-tick-tape."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.cli import backfill_tick_tape as cli
from src.cli.main import build_parser
from src.core.config import TickTapeBackfillSettings

_KST = ZoneInfo("Asia/Seoul")


def _history(paths, today=dt.date(2026, 10, 21)):
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    dates = [today - dt.timedelta(days=i) for i in range(45, 0, -1) if (today - dt.timedelta(days=i)).weekday() < 5]
    paths.bars_daily_dir.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame({"date": dates, "symbol": ["X"] * len(dates), "trade_value_100m": [0.0] * len(dates)})
    for month in {day.strftime("%Y-%m") for day in dates}:
        frame.filter(pl.col("date").dt.strftime("%Y-%m") == month).write_parquet(paths.bars_daily_dir / f"{month}.parquet")
    save_trading_day_cache(paths.calendar_cache, TradingDay(today, True, dates[-1], today + dt.timedelta(days=1)))


def _enabled(monkeypatch):
    monkeypatch.setenv("KRX_ALPHA_TICK_TAPE_ENABLED", "true")
    monkeypatch.setattr(cli, "_now_kst", lambda: dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST))


def test_subcommand_registered_next_to_gapfill() -> None:
    parser = build_parser()
    args = parser.parse_args(["backfill-tick-tape", "--dry-run"])
    assert args.handler is cli.run
    actions = [action for action in parser._actions if action.__class__.__name__ == "_SubParsersAction"]
    assert "backfill-tick-tape" in actions[0].choices
    assert "gapfill-ticks" in actions[0].choices


def test_missing_credentials_exit_4(tmp_path: pathlib.Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv("KIWOOM_APP_KEY", raising=False)
    monkeypatch.delenv("KIWOOM_SECRET_KEY", raising=False)
    args = type("Args", (), {"dry_run": True, "data_root": tmp_path, "max_pages": None, "deadline": None})()
    assert cli.run(args) == 4


def test_nothing_to_do_exit_2(tmp_path: pathlib.Path, monkeypatch, capsys) -> None:
    from src.core.paths import DataPaths

    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_pages": None, "deadline": None})()
    assert cli.run(args) == 2


def test_next_window_end_is_next_0530() -> None:
    settings = TickTapeBackfillSettings()
    evening = dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST)
    assert cli._next_window_end(evening, settings) == dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST)
    morning = dt.datetime(2026, 10, 22, 6, 0, tzinfo=_KST)
    assert cli._next_window_end(morning, settings) == dt.datetime(2026, 10, 23, 5, 30, tzinfo=_KST)


def test_resolve_sessions_skips_weekends(tmp_path: pathlib.Path) -> None:
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    _history(paths, dt.date(2026, 10, 20))
    sessions = cli.resolve_backfill_sessions(paths, 5, dt.date(2026, 10, 20))
    assert sessions == (
        dt.date(2026, 10, 13),
        dt.date(2026, 10, 14),
        dt.date(2026, 10, 15),
        dt.date(2026, 10, 16),
        dt.date(2026, 10, 19),
    )
    assert all(day.weekday() < 5 for day in sessions)
    assert json.dumps({"ok": True})


def test_prev_business_day_crosses_weekend(tmp_path) -> None:
    from src.core.paths import DataPaths
    from src.marketdata.tick_tape_backfill import _prev_business_day

    paths = DataPaths(tmp_path)
    _history(paths)
    assert _prev_business_day(dt.date(2026, 10, 19), paths) == dt.date(2026, 10, 16)
    assert _prev_business_day(dt.date(2026, 10, 21), paths) == dt.date(2026, 10, 20)


def test_sessions_honor_calendar_cache(tmp_path: pathlib.Path) -> None:
    from src.core.paths import DataPaths
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    paths = DataPaths(tmp_path)
    today = dt.date(2026, 10, 21)
    _history(paths, today)
    save_trading_day_cache(
        paths.calendar_cache,
        TradingDay(
            date=today,
            is_business_day=True,
            previous_business_day=dt.date(2026, 10, 20),
            next_business_day=dt.date(2026, 10, 22),
        ),
    )
    assert cli.resolve_backfill_sessions(paths, 1, today) == (dt.date(2026, 10, 20),)
    save_trading_day_cache(
        paths.calendar_cache,
        TradingDay(
            date=dt.date(2026, 10, 16),
            is_business_day=True,
            previous_business_day=dt.date(2026, 10, 15),
            next_business_day=dt.date(2026, 10, 19),
        ),
    )
    assert cli.resolve_backfill_sessions(paths, 1, dt.date(2026, 10, 19)) == (dt.date(2026, 10, 16),)


def test_sessions_lag_one_session_when_cache_rolled_past_upper(tmp_path: pathlib.Path) -> None:
    from src.core.paths import DataPaths
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    paths = DataPaths(tmp_path)
    _history(paths, dt.date(2026, 10, 23))
    paths.session_calendar_dir.mkdir(parents=True)
    for day in (dt.date(2026, 10, 22), dt.date(2026, 10, 23), dt.date(2026, 10, 26)):
        (paths.session_calendar_dir / f"dt={day.isoformat()}.json").write_text("{}", encoding="utf-8")
    save_trading_day_cache(
        paths.calendar_cache,
        TradingDay(
            date=dt.date(2026, 10, 24),
            is_business_day=False,
            previous_business_day=dt.date(2026, 10, 23),
            next_business_day=dt.date(2026, 10, 26),
        ),
    )
    sessions = cli.resolve_backfill_sessions(paths, 5, dt.date(2026, 10, 25))
    assert sessions[-1] == dt.date(2026, 10, 23)
    assert len(sessions) == 5


def test_sessions_stay_incomplete_when_a_session_is_missing_before_upper(tmp_path: pathlib.Path) -> None:
    from src.core.paths import DataPaths
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    paths = DataPaths(tmp_path)
    _history(paths, dt.date(2026, 10, 22))
    paths.session_calendar_dir.mkdir(parents=True)
    for day in (dt.date(2026, 10, 22), dt.date(2026, 10, 23)):
        (paths.session_calendar_dir / f"dt={day.isoformat()}.json").write_text("{}", encoding="utf-8")
    save_trading_day_cache(
        paths.calendar_cache,
        TradingDay(
            date=dt.date(2026, 10, 24),
            is_business_day=False,
            previous_business_day=dt.date(2026, 10, 23),
            next_business_day=dt.date(2026, 10, 26),
        ),
    )
    with pytest.raises(ValueError, match="calendar incomplete"):
        cli.resolve_backfill_sessions(paths, 5, dt.date(2026, 10, 25))


def test_parse_deadline_variants() -> None:
    settings = TickTapeBackfillSettings()
    now = dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST)
    assert cli._parse_deadline(None, settings, now) == cli._next_window_end(now, settings)
    assert cli._parse_deadline("2026-10-22T05:30:00", settings, now) == dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST)
    aware = dt.datetime(2026, 10, 21, 20, 30, tzinfo=dt.UTC)
    assert cli._parse_deadline(aware.isoformat(), settings, now) == aware.astimezone(_KST)


def test_run_delegates_and_maps_exit_codes(tmp_path: pathlib.Path, monkeypatch) -> None:
    import polars as pl

    from src.core.paths import DataPaths
    from src.marketdata.tick_tape_backfill import TickTapeRunSummary

    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    universe_path = paths.universe_out(dt.date(2026, 10, 16))
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["A"]}, schema={"symbol": pl.String}).write_parquet(universe_path)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    seen: dict = {}

    def _ok(**kwargs):
        seen.update(kwargs)
        return TickTapeRunSummary(
            status="OK", reason=None, run_id="r", symbols=1, symbol_days=1, pages=2, edge_partial=0, failed=0
        )

    monkeypatch.setattr(cli, "run_tick_tape_backfill", _ok)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_pages": 100, "deadline": None})()
    assert cli.run(args) == 0
    assert seen["settings"].max_pages_per_run == 100

    def _fail(**kwargs):
        return TickTapeRunSummary(
            status="FAIL", reason="auth", run_id="r", symbols=1, symbol_days=0, pages=1, edge_partial=0, failed=1
        )

    monkeypatch.setattr(cli, "run_tick_tape_backfill", _fail)
    assert cli.run(args) == 4
    bad = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_pages": 0, "deadline": None})()
    assert cli.run(bad) == 4


def test_disabled_mode_is_inert(tmp_path, monkeypatch):
    monkeypatch.setenv("KRX_ALPHA_TICK_TAPE_ENABLED", "false")
    args = build_parser().parse_args(["backfill-tick-tape", "--data-root", str(tmp_path)])
    assert cli.run(args) == 2
    assert list(tmp_path.iterdir()) == [tmp_path / "kis-token-cache"]


def test_calendar_absence_is_fatal_not_weekday_fallback(tmp_path, monkeypatch):
    from src.core.paths import DataPaths
    import pytest

    _enabled(monkeypatch)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    args = build_parser().parse_args(["backfill-tick-tape", "--data-root", str(tmp_path)])
    assert cli.run(args) == 4
    paths = DataPaths(tmp_path)
    _history(paths)
    for path in paths.bars_daily_dir.glob("*.parquet"):
        path.unlink()
    with pytest.raises(ValueError, match="historical calendar incomplete"):
        cli.resolve_backfill_sessions(paths, 19, dt.date(2026, 10, 21))


def test_window_deadline_and_setup_failures(tmp_path, monkeypatch):
    from src.core.paths import DataPaths
    from src.brokers.kis.rate import HostPacingNotSharedError

    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    paths.universe_dir.mkdir()
    pl.DataFrame({"symbol": ["A"]}).write_parquet(paths.universe_out(dt.date(2026, 10, 19)))
    args = build_parser().parse_args(["backfill-tick-tape", "--data-root", str(tmp_path)])
    monkeypatch.setattr(cli, "_now_kst", lambda: dt.datetime(2026, 10, 21, 12, 0, tzinfo=_KST))
    assert cli.run(args) == 2
    _enabled(monkeypatch)
    args.deadline = "invalid"
    assert cli.run(args) == 4
    args.deadline = "2026-10-23T12:00:00+09:00"
    seen = {}

    def unavailable(**kwargs):
        seen.update(kwargs)
        raise HostPacingNotSharedError("not shared")

    monkeypatch.setattr(cli, "run_tick_tape_backfill", unavailable)
    assert cli.run(args) == 4
    assert seen["deadline"] == dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST)


def test_default_data_root_respects_collector_environment(tmp_path, monkeypatch, capsys):
    from src.core.paths import DataPaths

    assert cli._now_kst().tzinfo is not None
    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    monkeypatch.setenv("KRX_ALPHA_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    args = build_parser().parse_args(["backfill-tick-tape", "--dry-run"])
    assert cli.run(args) == 0
    assert json.loads(capsys.readouterr().out)["symbols"] == 0


def test_post_midnight_run_includes_session_whose_bar_is_pending(tmp_path):
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    _history(paths, today=dt.date(2026, 10, 22))
    sessions = cli.resolve_backfill_sessions(paths, 19, dt.date(2026, 10, 23))
    assert sessions[-1] == dt.date(2026, 10, 22)
    assert sessions[-2] == dt.date(2026, 10, 21)
    assert len(sessions) == 19


def test_two_session_bar_lag_still_fails_closed(tmp_path):
    import pytest

    from src.core.paths import DataPaths
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    paths = DataPaths(tmp_path)
    _history(paths, today=dt.date(2026, 10, 21))
    save_trading_day_cache(
        paths.calendar_cache, TradingDay(dt.date(2026, 10, 22), True, dt.date(2026, 10, 21), dt.date(2026, 10, 23))
    )
    with pytest.raises(ValueError, match="historical calendar incomplete"):
        cli.resolve_backfill_sessions(paths, 19, dt.date(2026, 10, 23))


def _cli_args(tmp_path, **overrides):
    base = {"dry_run": False, "data_root": tmp_path, "max_pages": None, "deadline": None, "only_symbols": None}
    base.update(overrides)
    return type("Args", (), base)()


def test_only_symbols_filters_and_never_adds(tmp_path, monkeypatch, caplog):
    from src.core.paths import DataPaths
    from src.marketdata.tick_tape_backfill import TickTapeTarget, TickTapeRunSummary

    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    universe_path = paths.universe_out(dt.date(2026, 10, 16))
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["A", "B"]}, schema={"symbol": pl.String}).write_parquet(universe_path)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    seen: dict = {}

    def _ok(**kwargs):
        seen.update(kwargs)
        return TickTapeRunSummary(
            status="OK", reason=None, run_id="r", symbols=1, symbol_days=1, pages=1, edge_partial=0, failed=0
        )

    monkeypatch.setattr(cli, "run_tick_tape_backfill", _ok)
    monkeypatch.setattr(
        cli, "plan_tick_tape_targets",
        lambda **kwargs: (TickTapeTarget("00000A", (dt.date(2026, 10, 20),), 1.0),
                          TickTapeTarget("00000B", (dt.date(2026, 10, 20),), 2.0)),
    )
    with caplog.at_level("INFO"):
        assert cli.run(_cli_args(tmp_path, only_symbols="B,Z")) == 0
    assert [t.symbol for t in seen["targets"]] == ["00000B"]
    assert any("symbol_not_planned" in r.getMessage() and "symbol=00000Z" in r.getMessage() for r in caplog.records)
    assert cli.run(_cli_args(tmp_path, only_symbols="ZZZ")) == 2


def test_signal_handlers_are_scoped(tmp_path, monkeypatch):
    import signal

    import pytest

    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    monkeypatch.setattr(
        cli, "plan_tick_tape_targets",
        lambda **kwargs: (cli_tick_target(),),
    )
    before_term = signal.getsignal(signal.SIGTERM)
    before_int = signal.getsignal(signal.SIGINT)

    def _boom(**kwargs):
        raise SystemExit(3)

    monkeypatch.setattr(cli, "run_tick_tape_backfill", _boom)
    with pytest.raises(SystemExit, match="3"):
        cli.run(_cli_args(tmp_path))
    assert signal.getsignal(signal.SIGTERM) == before_term
    assert signal.getsignal(signal.SIGINT) == before_int


def cli_tick_target():
    from src.marketdata.tick_tape_backfill import TickTapeTarget

    return TickTapeTarget("A", (dt.date(2026, 10, 20),), 1.0)


def test_sigterm_maps_to_graceful_stop(tmp_path, monkeypatch):
    import signal

    from src.core.paths import DataPaths
    from src.marketdata.tick_tape_backfill import TickTapeRunSummary

    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    monkeypatch.setattr(cli, "plan_tick_tape_targets", lambda **kwargs: (cli_tick_target(),))
    observed: dict = {}

    def _inspect(**kwargs):
        should_stop = kwargs["should_stop"]
        signal.raise_signal(signal.SIGTERM)
        observed["stopped"] = should_stop()
        return TickTapeRunSummary(
            status="OK", reason="stop_requested", run_id="r", symbols=0, symbol_days=0,
            pages=0, edge_partial=0, failed=0,
        )

    monkeypatch.setattr(cli, "run_tick_tape_backfill", _inspect)
    assert cli.run(_cli_args(tmp_path)) == 0
    assert observed["stopped"] is True


def test_only_symbols_dry_run_respects_ledger_and_plan(tmp_path, monkeypatch, capsys, caplog):
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    day = dt.date(2026, 10, 20)
    monkeypatch.setattr(cli, "resolve_backfill_sessions", lambda *args: (day,))
    paths.universe_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["005930", "000660"]}).write_parquet(paths.universe_out(dt.date(2026, 10, 19)))
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger = json.dumps({"symbol": "005930", "date": day.isoformat(), "status": "complete", "rows": 2}) + "\n"
    paths.tick_tape_ledger.write_text(ledger)

    def unexpected_run(**kwargs):
        raise AssertionError("dry-run must not call runner")

    monkeypatch.setattr(cli, "run_tick_tape_backfill", unexpected_run)
    args = build_parser().parse_args([
        "backfill-tick-tape", "--data-root", str(tmp_path), "--dry-run", "--only-symbols", "5930,660,999999",
    ])
    with caplog.at_level("INFO"):
        assert cli.run(args) == 0
    assert json.loads(capsys.readouterr().out) == {"symbols": 1, "symbol_days": 1, "cost_hint": 0.0}
    assert {record.getMessage().split("symbol=")[-1] for record in caplog.records
            if "symbol_not_planned" in record.getMessage()} == {"005930", "999999"}
    args.only_symbols = "5930"
    assert cli.run(args) == 2
    assert paths.tick_tape_ledger.read_text() == ledger
