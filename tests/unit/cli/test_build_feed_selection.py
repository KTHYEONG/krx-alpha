"""build-feed-selection CLI: inert exit without watched groups, and a status-only build with one watched group."""

from __future__ import annotations

import argparse
import datetime as dt
import json

from src.cli import build_feed_selection as cli
from src.core.paths import DataPaths

DAY = dt.date(2026, 11, 16)


def _args(root, day=DAY):
    return argparse.Namespace(deadline=None, day=day, data_root=root)


def test_cli_noop_without_watch_groups(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("KRX_ALPHA_FEED_CONTROL_WATCH_GROUPS", raising=False)
    assert cli.run(_args(tmp_path)) == 2
    assert not (tmp_path / "manifest").exists()


def test_cli_builds_status_only_with_watch_group(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_FEED_CONTROL_WATCH_GROUPS", '["ls:0"]')
    assert cli.run(_args(tmp_path)) == 0
    paths = DataPaths(tmp_path)
    payload = json.loads((paths.manifest_dir / "feed_quality" / f"{DAY.isoformat()}.selection.json").read_text())
    assert payload["status"] == "NO_SECONDARY"
    assert (paths.manifest_dir / "feed_quality" / "rolling.json").exists()


def test_cli_setup_failure_maps_to_exit_four(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_FEED_CONTROL_WATCH_GROUPS", '["ls:0"]')
    assert cli.run(argparse.Namespace(deadline=None, day="not-a-date", data_root=tmp_path)) == 4
