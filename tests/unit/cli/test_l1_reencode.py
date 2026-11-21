"""l1-reencode CLI gate tests."""

from __future__ import annotations

import argparse
import datetime as dt
import time
from zoneinfo import ZoneInfo


import src.cli.l1_reencode as cli_mod
from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache
from src.core.paths import DataPaths
from src.storage.l1_reencode import ReencodeResult


def test_business_day_refuses_without_archiver_calls(tmp_path, monkeypatch, caplog) -> None:
    today = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    paths = DataPaths(tmp_path)
    save_trading_day_cache(
        paths.calendar_cache,
        TradingDay(date=today, is_business_day=True, previous_business_day=today, next_business_day=today),
    )

    class _Settings:
        def __init__(self) -> None:
            self.paths = paths

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("archiver must not be created on a business day")

    monkeypatch.setattr(cli_mod, "CollectorSettings", _Settings)
    monkeypatch.setattr(cli_mod.GDriveArchiver, "try_from_env", staticmethod(_forbidden))

    with caplog.at_level("INFO", logger=cli_mod.logger.name):
        code = cli_mod.run(argparse.Namespace(budget_minutes=5))

    assert code == 0
    assert "status=SKIP reason=trading_day" in caplog.text


def _paths_with_cache(tmp_path, *, is_business_day: bool | None):
    today = dt.datetime.now(ZoneInfo("Asia/Seoul")).date()
    paths = DataPaths(tmp_path)
    if is_business_day is not None:
        save_trading_day_cache(
            paths.calendar_cache,
            TradingDay(
                date=today, is_business_day=is_business_day, previous_business_day=today, next_business_day=today
            ),
        )

    class _Settings:
        def __init__(self) -> None:
            self.paths = paths

    return paths, _Settings


def test_non_business_day_runs_backlog_with_budget_deadline(tmp_path, monkeypatch) -> None:
    paths, settings = _paths_with_cache(tmp_path, is_business_day=False)
    captured: dict[str, object] = {}
    archiver = object()

    def _run_backlog(arc, *, l1_root, work_dir, deadline_monotonic, ledger_path):
        captured.update(arc=arc, l1_root=l1_root, work_dir=work_dir, deadline=deadline_monotonic, ledger=ledger_path)
        return [ReencodeResult("l1/x.parquet", 1, 10, 9, "reencoded")]

    monkeypatch.setattr(cli_mod, "CollectorSettings", settings)
    monkeypatch.setattr(cli_mod.GDriveArchiver, "try_from_env", staticmethod(lambda: archiver))
    monkeypatch.setattr(cli_mod, "run_backlog", _run_backlog)

    before = time.monotonic()
    assert cli_mod.run(argparse.Namespace(budget_minutes=2)) == 0

    assert captured["arc"] is archiver
    assert captured["l1_root"] == paths.archive_root
    assert captured["work_dir"] == paths.root / "l1_reencode_tmp"
    assert captured["ledger"] == paths.root / "work" / "l1_reencode_done.jsonl"
    assert before + 119 <= captured["deadline"] <= time.monotonic() + 120


def test_missing_rclone_fails_closed(tmp_path, monkeypatch) -> None:
    _, settings = _paths_with_cache(tmp_path, is_business_day=False)
    monkeypatch.setattr(cli_mod, "CollectorSettings", settings)
    monkeypatch.setattr(cli_mod.GDriveArchiver, "try_from_env", staticmethod(lambda: None))

    assert cli_mod.run(argparse.Namespace(budget_minutes=2)) == 1


def test_unknown_calendar_skips(tmp_path, monkeypatch, caplog) -> None:
    _, settings = _paths_with_cache(tmp_path, is_business_day=None)
    monkeypatch.setattr(cli_mod, "CollectorSettings", settings)

    with caplog.at_level("INFO", logger=cli_mod.logger.name):
        assert cli_mod.run(argparse.Namespace(budget_minutes=2)) == 0

    assert "reason=calendar_unknown" in caplog.text
