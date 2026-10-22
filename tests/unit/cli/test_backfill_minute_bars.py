"""CLI wiring tests for backfill-minute-bars (KIS 1m source)."""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.cli import backfill_minute_bars as cli
from src.cli.main import build_parser
from src.core.paths import DataPaths

_KST = ZoneInfo("Asia/Seoul")
_D = dt.date(2026, 10, 20)
_PREV = dt.date(2026, 10, 17)
_NOW = dt.datetime(2026, 10, 21, 23, 45, tzinfo=_KST)


@pytest.fixture(autouse=True)
def _planning_clock(monkeypatch):
    from src.marketdata import minute_backfill

    monkeypatch.setattr(minute_backfill, "_now_kst", lambda: _NOW)


def _history(paths: DataPaths) -> None:
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache

    frame = pl.DataFrame(
        {"date": [_PREV, _D], "symbol": ["X", "X"]},
        schema={"date": pl.Date, "symbol": pl.String},
    )
    out = paths.bars_daily_dir / f"{_PREV:%Y-%m}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(out)
    save_trading_day_cache(
        paths.calendar_cache,
        TradingDay(_NOW.date(), True, _D, _NOW.date() + dt.timedelta(days=1)),
    )
    paths.universe_out(_PREV).parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["A"]}, schema={"symbol": pl.String}).write_parquet(paths.universe_out(_PREV))


def _enabled(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_MINUTE_BACKFILL_ENABLED", "true")
    monkeypatch.setattr(cli, "_now_kst", lambda: _NOW)


def _creds(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "5")
    monkeypatch.setenv("KIS_DATA_5_APP_KEY", "k5")
    monkeypatch.setenv("KIS_DATA_5_APP_SECRET", "s5")
    monkeypatch.setenv("KIS_DATA_5_HTS_ID", "h5")


def test_subcommand_registered() -> None:
    assert cli._now_kst().tzinfo is not None
    parser = build_parser()
    args = parser.parse_args(["backfill-minute-bars", "--dry-run"])
    assert args.handler is cli.run
    actions = [action for action in parser._actions if action.__class__.__name__ == "_SubParsersAction"]
    assert "backfill-minute-bars" in actions[0].choices


def test_disabled_mode_is_inert(tmp_path: pathlib.Path, monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_MINUTE_BACKFILL_ENABLED", "false")
    args = build_parser().parse_args(["backfill-minute-bars", "--data-root", str(tmp_path)])
    assert cli.run(args) == 2


def test_missing_credentials_exit_4(tmp_path: pathlib.Path, monkeypatch) -> None:
    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    monkeypatch.delenv("KIS_DATA_SLOTS", raising=False)
    for key in ("KIS_DATA_5_APP_KEY", "KIS_DATA_5_APP_SECRET", "KIS_DATA_5_HTS_ID"):
        monkeypatch.delenv(key, raising=False)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 4


def test_missing_slot_exit_4(tmp_path: pathlib.Path, monkeypatch) -> None:
    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    monkeypatch.setenv("KIS_DATA_SLOTS", "1")
    monkeypatch.setenv("KIS_DATA_1_APP_KEY", "k1")
    monkeypatch.setenv("KIS_DATA_1_APP_SECRET", "s1")
    monkeypatch.setenv("KIS_DATA_1_HTS_ID", "h1")
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 4


def test_calendar_absence_is_fatal(tmp_path: pathlib.Path, monkeypatch) -> None:
    _enabled(monkeypatch)
    _creds(monkeypatch)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 4


def test_invalid_date_exit_4(tmp_path: pathlib.Path, monkeypatch) -> None:
    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    _creds(monkeypatch)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": "bogus", "end": None})()
    assert cli.run(args) == 4


def test_end_capped_to_last_completed_session(tmp_path: pathlib.Path, monkeypatch, capsys) -> None:
    _history(DataPaths(tmp_path))
    _creds(monkeypatch)
    monkeypatch.setattr(cli, "_now_kst", lambda: _NOW)
    args = type("Args", (), {"dry_run": True, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": "2026-10-22"})()
    assert cli.run(args) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["symbol_days"] == 1


def test_start_clamped_to_vendor_retention(tmp_path: pathlib.Path, monkeypatch, caplog) -> None:
    _history(DataPaths(tmp_path))
    _creds(monkeypatch)
    monkeypatch.setattr(cli, "_now_kst", lambda: _NOW)
    original = cli.plan_minute_targets
    planned = {}

    def planner(**kwargs):
        planned.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(cli, "plan_minute_targets", planner)
    args = type("Args", (), {"dry_run": True, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": "2025-04-20", "end": None})()
    with caplog.at_level(logging.INFO, logger="src.cli.backfill_minute_bars"):
        assert cli.run(args) == 0
    clamped = [record for record in caplog.records if "clamped" in record.message]
    assert len(clamped) == 1
    assert planned["start"] == _NOW.date() - dt.timedelta(days=364)


def test_plan_failure_exit_4(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    _history(paths)
    _enabled(monkeypatch)
    _creds(monkeypatch)
    ledger = paths.minute_history_ledger
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text("{truncated\n")
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 4


def test_setup_failure_exit_4(tmp_path: pathlib.Path, monkeypatch) -> None:
    from src.core.errors import KrxAlphaError

    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    _creds(monkeypatch)

    def _boom(**kwargs):
        raise KrxAlphaError("setup down")

    monkeypatch.setattr(cli, "run_minute_backfill", _boom)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 4


def test_nothing_to_do_exit_2(tmp_path: pathlib.Path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    _history(paths)
    paths.universe_out(_PREV).unlink()
    _enabled(monkeypatch)
    _creds(monkeypatch)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 2


def test_dry_run_reports_symbol_days(tmp_path: pathlib.Path, monkeypatch, capsys) -> None:
    _history(DataPaths(tmp_path))
    _creds(monkeypatch)
    monkeypatch.setattr(cli, "_now_kst", lambda: _NOW)
    args = type("Args", (), {"dry_run": True, "data_root": tmp_path, "max_calls": None,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["symbols"] == 1
    assert out["symbol_days"] == 1


def test_next_window_end_is_next_0530() -> None:
    assert cli._next_window_end(_NOW) == dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST)
    morning = dt.datetime(2026, 10, 22, 6, 0, tzinfo=_KST)
    assert cli._next_window_end(morning) == dt.datetime(2026, 10, 23, 5, 30, tzinfo=_KST)


def test_parse_deadline_variants() -> None:
    assert cli._parse_deadline(None, _NOW) == cli._next_window_end(_NOW)
    assert cli._parse_deadline("2026-10-22T05:30:00", _NOW) == dt.datetime(2026, 10, 22, 5, 30, tzinfo=_KST)
    aware = dt.datetime(2026, 10, 21, 20, 30, tzinfo=dt.UTC)
    assert cli._parse_deadline(aware.isoformat(), _NOW) == aware.astimezone(_KST)


def test_run_delegates_and_maps_exit_codes(tmp_path: pathlib.Path, monkeypatch) -> None:
    from src.marketdata.minute_backfill import MinuteRunSummary

    _history(DataPaths(tmp_path))
    _enabled(monkeypatch)
    _creds(monkeypatch)
    seen: dict = {}

    def _ok(**kwargs):
        seen.update(kwargs)
        return MinuteRunSummary(status="OK", reason=None, run_id="r", symbols=1,
                                symbol_days=1, calls=2, vendor_empty=0, failed=0, discarded_offhours=0)

    monkeypatch.setattr(cli, "run_minute_backfill", _ok)
    args = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": 100,
                             "deadline": None, "start": None, "end": None})()
    assert cli.run(args) == 0
    assert seen["settings"].max_calls_per_run == 100

    def _fail(**kwargs):
        return MinuteRunSummary(status="FAIL", reason="auth", run_id="r", symbols=1,
                                symbol_days=0, calls=1, vendor_empty=0, failed=1, discarded_offhours=0)

    monkeypatch.setattr(cli, "run_minute_backfill", _fail)
    assert cli.run(args) == 4
    bad = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": 0,
                            "deadline": None, "start": None, "end": None})()
    assert cli.run(bad) == 4
    invalid = type("Args", (), {"dry_run": False, "data_root": tmp_path, "max_calls": None,
                                "deadline": "invalid", "start": None, "end": None})()
    assert cli.run(invalid) == 4


def test_completed_session_includes_today_only_after_close(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    _history(paths)
    assert cli._last_completed_session(_NOW.replace(hour=10), paths) == _D
    assert cli._last_completed_session(_NOW, paths) == _NOW.date()


def test_dry_run_does_not_require_credentials(tmp_path: pathlib.Path, monkeypatch, capsys) -> None:
    _history(DataPaths(tmp_path))
    monkeypatch.setattr(cli, "_now_kst", lambda: _NOW)
    monkeypatch.delenv("KIS_DATA_SLOTS", raising=False)
    args = build_parser().parse_args(["backfill-minute-bars", "--dry-run", "--data-root", str(tmp_path)])
    assert cli.run(args) == 0
    assert json.loads(capsys.readouterr().out)["symbol_days"] == 1
