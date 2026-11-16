"""Feed quality report: field schema, 30-day rolling cap, consecutive-day alert helper, and inert digest without a report."""

from __future__ import annotations

import datetime as dt
import json
import logging

import pytest

from src.core.alerts import PAGE_ATTR, RESOLVE_ATTR, PageKind
from src.core.config import FeedControlSettings
from src.core.paths import DataPaths
from src.storage import feed_quality as fq

DAY = dt.date(2026, 11, 16)


@pytest.fixture
def paths(tmp_path) -> DataPaths:
    return DataPaths(tmp_path / "data")


def _write_summary(paths: DataPaths, day: dt.date, minutes: int) -> None:
    path = paths.manifest_dir / "feed_quality" / f"{day.isoformat()}.selection.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": "OK",
        "vendors": {"ls": {"tick_frames": 10, "book_frames": 20, "minutes_with_frames": minutes}},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_report_fields_and_availability(paths) -> None:
    _write_summary(paths, DAY, minutes=3)
    report = fq.build_feed_quality(DAY, paths=paths)
    body = report.to_json()
    assert set(body) == {"date", "selection_status", "session_minutes", "vendors", "controller", "feeds"}
    assert body["selection_status"] == "OK"
    ls = body["vendors"]["ls"]
    assert ls["minutes_with_frames"] == 3
    assert 0.0 <= ls["availability"] <= 1.0
    assert set(body["controller"]) == {"transitions", "demotions", "admitted_symbols", "recycles"}


def test_missing_selection_reports_no_selection(paths) -> None:
    report = fq.build_feed_quality(DAY, paths=paths)
    assert report.selection_status == "NO_SELECTION"
    assert report.vendors == {}


def test_rolling_capped_at_thirty_days_and_upserts(paths) -> None:
    start = DAY - dt.timedelta(days=40)
    for offset in range(35):
        day = start + dt.timedelta(days=offset)
        _write_summary(paths, day, minutes=offset % 5)
        fq.write_feed_quality(fq.build_feed_quality(day, paths=paths), paths=paths)
    rows = json.loads(fq.rolling_path(paths).read_text(encoding="utf-8"))
    assert len(rows) == fq.ROLLING_DAYS
    assert rows[-1]["date"] == (start + dt.timedelta(days=34)).isoformat()
    fq.write_feed_quality(fq.build_feed_quality(DAY, paths=paths), paths=paths)
    rows = json.loads(fq.rolling_path(paths).read_text(encoding="utf-8"))
    assert len([r for r in rows if r["date"] == DAY.isoformat()]) <= 1


def test_consecutive_days_below_counts_recent_run(paths) -> None:
    for offset, minutes in enumerate([0, 0, 400, 0, 0]):
        day = DAY - dt.timedelta(days=4 - offset)
        _write_summary(paths, day, minutes=minutes)
        fq.write_feed_quality(fq.build_feed_quality(day, paths=paths), paths=paths)
    assert fq.consecutive_days_below(paths, floor=0.5, vendor="ls") == 2
    assert fq.consecutive_days_below(paths, floor=0.5, vendor="kis") == 0


def test_digest_pending_without_report_and_control_lines(paths) -> None:
    lines = fq.digest_lines(paths, DAY)
    assert "feed_quality_status=PENDING" in lines
    assert any(line.startswith("feed_control_transitions=") for line in lines)


def test_report_reads_archived_health_and_controller_outcomes(paths) -> None:
    archive = fq.feed_health_archive_dir(paths, DAY)
    archive.mkdir(parents=True)
    (archive / "ls__0.json").write_text(
        json.dumps({"feed_id": "ls:0", "disconnects_total": 2, "lag_p50_s": 0.4, "lag_p99_s": 3.0}), encoding="utf-8"
    )
    (archive / "ls__0b.json").write_text(
        json.dumps({"feed_id": "ls:0", "disconnects_total": 1, "lag_p50_s": 0.9, "lag_p99_s": None}), encoding="utf-8"
    )
    (archive / "broken.json").write_text("[]", encoding="utf-8")
    log = paths.feed_control_log_path(DAY)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {"kind": "TRANSITION", "new": "DAY_DEMOTED"},
                {"kind": "ADMIT", "mode": "active", "symbols_n": 3},
                {"kind": "RECYCLE"},
            ]
        )
        + "\nnot json\n",
        encoding="utf-8",
    )
    report = fq.build_feed_quality(DAY, paths=paths)
    assert report.feeds["ls:0"] == {"disconnects": 3, "lag_p50_s": 0.9, "lag_p99_s": 3.0}
    assert report.controller == {"transitions": 1, "demotions": 1, "admitted_symbols": 3, "recycles": 1}


def test_digest_reports_existing_quality_and_junk_rolling_is_zero(paths) -> None:
    _write_summary(paths, DAY, minutes=390)
    fq.write_feed_quality(fq.build_feed_quality(DAY, paths=paths), paths=paths)
    lines = fq.digest_lines(paths, DAY)
    assert "feed_quality_status=OK" in lines
    assert any(line.startswith("feed_quality_ls_availability=") for line in lines)
    fq.rolling_path(paths).write_text('{"not": "a list"}', encoding="utf-8")
    assert fq.consecutive_days_below(paths, floor=0.5, vendor="ls") == 0
    log = paths.feed_control_log_path(DAY)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text("[]\n", encoding="utf-8")
    assert fq.digest_lines(paths, DAY)[0] == "feed_control_admitted_symbols=0"


def _run_floor_day(paths: DataPaths, day: dt.date, minutes: int, settings) -> None:
    _write_summary(paths, day, minutes=minutes)
    report = fq.build_feed_quality(day, paths=paths)
    fq.write_feed_quality(report, paths=paths)
    fq.page_quality_floor(report, paths=paths, settings=settings)


def test_quality_floor_pages_once_per_streak_and_resolves(paths, caplog) -> None:
    settings = FeedControlSettings(watch_groups=("ls:0",))
    start = DAY - dt.timedelta(days=5)
    with caplog.at_level(logging.INFO):
        _run_floor_day(paths, start, 0, settings)
        _run_floor_day(paths, start + dt.timedelta(days=1), 0, settings)
        assert _floor_pages(caplog) == []
        _run_floor_day(paths, start + dt.timedelta(days=2), 0, settings)
        assert len(_floor_pages(caplog)) == 1
        _run_floor_day(paths, start + dt.timedelta(days=3), 0, settings)
        assert len(_floor_pages(caplog)) == 1
        _run_floor_day(paths, start + dt.timedelta(days=4), 100_000, settings)
        resolved = [r for r in caplog.records if getattr(r, RESOLVE_ATTR, None) is PageKind.FEED_QUALITY_FLOOR]
    assert len(resolved) == 1
    assert "shard=ls" in resolved[0].getMessage()


def _floor_pages(caplog) -> list:
    return [
        r
        for r in caplog.records
        if getattr(r, PAGE_ATTR, None) is PageKind.FEED_QUALITY_FLOOR and r.levelno == logging.CRITICAL
    ]


def test_consecutive_days_below_before_excludes_day(paths) -> None:
    for offset in range(3):
        _run_floor_day(paths, DAY - dt.timedelta(days=2 - offset), 0, FeedControlSettings(watch_groups=("ls:0",)))
    assert fq.consecutive_days_below(paths, floor=0.5, vendor="ls") == 3
    assert fq.consecutive_days_below(paths, floor=0.5, vendor="ls", before=DAY) == 2
    assert fq.consecutive_days_below(paths, floor=0.5, vendor="ls", before=DAY - dt.timedelta(days=1)) == 1


def test_quality_floor_without_rolling_file_is_silent(paths, caplog) -> None:
    report = fq.build_feed_quality(DAY, paths=paths)
    with caplog.at_level(logging.INFO):
        fq.page_quality_floor(report, paths=paths, settings=FeedControlSettings(watch_groups=("ls:0",)))
    assert not [r for r in caplog.records if getattr(r, PAGE_ATTR, None) or getattr(r, RESOLVE_ATTR, None)]
