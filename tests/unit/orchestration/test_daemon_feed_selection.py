"""Nightly FEED_SELECTION job: order, command, watch-group gate, and inert EOD helpers without watched groups."""

from __future__ import annotations

import datetime as dt
import logging
from types import SimpleNamespace

from src.core.config import FeedControlSettings
from src.core.paths import DataPaths
from src.orchestration import daemon as dm

DAY = dt.date(2026, 11, 16)
DEADLINE = dt.datetime(2026, 11, 17, 5, 30, tzinfo=dm._KST)


def _runner(watch_groups: tuple[str, ...]) -> object:
    runner = object.__new__(dm.DaemonRunner)
    runner._feed_control = FeedControlSettings(watch_groups=watch_groups)
    return runner


def test_feed_selection_sits_between_repair_and_minute_bars() -> None:
    order = dm._NIGHTLY_JOB_ORDER
    assert order.index(dm.NightlyJob.REPAIR) < order.index(dm.NightlyJob.FEED_SELECTION)
    assert order.index(dm.NightlyJob.FEED_SELECTION) < order.index(dm.NightlyJob.MINUTE_BARS)


def test_feed_selection_command_carries_deadline() -> None:
    cmd = dm._nightly_job_cmd(dm.NightlyJob.FEED_SELECTION, deadline=DEADLINE)
    assert cmd[-3:-1] == ["build-feed-selection", "--deadline"]
    assert cmd[-1] == DEADLINE.isoformat()


def test_feed_selection_gated_on_watch_groups() -> None:
    off = _runner(())
    on = _runner(("ls:0",))
    assert dm.DaemonRunner._nightly_job_enabled(off, dm.NightlyJob.FEED_SELECTION) is False
    assert dm.DaemonRunner._nightly_job_enabled(on, dm.NightlyJob.FEED_SELECTION) is True
    assert dm._feed_watch_groups(SimpleNamespace()) == ()


def test_health_archive_copies_snapshots_and_is_empty_without_them(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path / "data")
    assert dm._archive_feed_health_at_close(paths, DAY) == 0
    status = paths.work_root / "feed_status"
    status.mkdir(parents=True)
    (status / "ls__0.json").write_text('{"feed_id": "ls:0"}', encoding="utf-8")
    with caplog.at_level(logging.INFO, logger="src.orchestration.daemon"):
        assert dm._archive_feed_health_at_close(paths, DAY) == 1
    archived = paths.manifest_dir / "feed_quality" / f"{DAY.isoformat()}.health" / "ls__0.json"
    assert archived.read_text(encoding="utf-8") == '{"feed_id": "ls:0"}'
    assert "stage=feed_health_archive status=OK" in caplog.text


def test_digest_lines_absent_without_watch_groups(tmp_path) -> None:
    paths = DataPaths(tmp_path / "data")
    assert dm._feed_digest_lines(paths, DAY, watch_groups=()) == []
    lines = dm._feed_digest_lines(paths, DAY, watch_groups=("ls:0",))
    assert "feed_quality_status=PENDING" in lines


def test_health_archive_failure_is_logged_not_raised(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path / "data")
    status = paths.work_root / "feed_status"
    status.mkdir(parents=True)
    (status / "ls__0.json").write_text("{}", encoding="utf-8")
    quality_dir = paths.manifest_dir / "feed_quality"
    quality_dir.parent.mkdir(parents=True, exist_ok=True)
    quality_dir.write_text("not a directory", encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="src.orchestration.daemon"):
        assert dm._archive_feed_health_at_close(paths, DAY) == 0
    assert "stage=feed_health_archive status=FAIL" in caplog.text


def test_business_eod_archives_health_only_when_watched(tmp_path, monkeypatch) -> None:
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import standard_session_anchors
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day
    from tests.unit.orchestration.test_daemon_eod import _prearm_daemon_runner

    kst = ZoneInfo("Asia/Seoul")
    now = dt.datetime(2026, 11, 16, 20, 5, tzinfo=kst)
    anchors = standard_session_anchors(DAY)
    shard = AftermarketShard(MarketVenue.KRX, 0, ("000660",), ("H0STCNT0", "H0STASP0"), "1", "id1")
    runner = _prearm_daemon_runner(tmp_path, (shard,))
    runner._run_id = "test"
    runner._eod_progress = lambda: None
    runner._feed_control = FeedControlSettings(watch_groups=("ls:0",))
    status = runner._paths.work_root / "feed_status"
    status.mkdir(parents=True)
    (status / "ls__0.json").write_text('{"feed_id": "ls:0"}', encoding="utf-8")
    monkeypatch.setattr(dm, "_run_eod_housekeeping", lambda cfg, paths, ref_day, progress=None: type(
        "H", (), {"deleted": 0, "uploaded": 0, "purged": 0, "maintenance_ok": True, "offload_ok": True}
    )())
    monkeypatch.setattr(dm.DaemonRunner, "_reconcile_regular_session", lambda self, r, d, a, audit=None: (True, True))
    monkeypatch.setattr(dm.DaemonRunner, "_reconcile_crosscheck_sessions", lambda self, r, d, a: {})
    monkeypatch.setattr(dm, "_check_backup", lambda *a, **k: ([], True, "fresh"))
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(dm, "record_eod_digest", lambda subject, body: digests.append((subject, body)))
    runner._run_business_eod(now, DAY, _business_trading_day(DAY), anchors)
    archived = runner._paths.manifest_dir / "feed_quality" / f"{DAY.isoformat()}.health" / "ls__0.json"
    assert archived.is_file()
    assert "feed_quality_status=PENDING" in digests[0][1].splitlines()
