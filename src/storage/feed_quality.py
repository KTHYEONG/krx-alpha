"""Daily feed quality report and 30-day rolling history, derived from controller, selection and archived health snapshots."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final
from zoneinfo import ZoneInfo

from src.core.paths import DataPaths
from src.core.session_anchors import resolve_session_anchors
from src.storage.feed_selection import selection_summary_path

logger = logging.getLogger(__name__)

ROLLING_DAYS: Final[int] = 30
_KST = ZoneInfo("Asia/Seoul")
_SECONDS_PER_MINUTE: Final[int] = 60


@dataclass(frozen=True, slots=True)
class FeedQualityReport:
    date: str
    selection_status: str
    session_minutes: int
    vendors: Mapping[str, Mapping[str, Any]]
    controller: Mapping[str, int]
    feeds: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "selection_status": self.selection_status,
            "session_minutes": self.session_minutes,
            "vendors": {vendor: dict(stats) for vendor, stats in sorted(self.vendors.items())},
            "controller": dict(sorted(self.controller.items())),
            "feeds": {feed: dict(stats) for feed, stats in sorted(self.feeds.items())},
        }


def feed_quality_report_path(paths: DataPaths, day: dt.date) -> pathlib.Path:
    return paths.manifest_dir / "feed_quality" / f"{day.isoformat()}.json"


def feed_health_archive_dir(paths: DataPaths, day: dt.date) -> pathlib.Path:
    return paths.manifest_dir / "feed_quality" / f"{day.isoformat()}.health"


def rolling_path(paths: DataPaths) -> pathlib.Path:
    return paths.manifest_dir / "feed_quality" / "rolling.json"


def _read_json(path: pathlib.Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _controller_counts(paths: DataPaths, day: dt.date) -> dict[str, int]:
    counts = {"transitions": 0, "demotions": 0, "admitted_symbols": 0, "recycles": 0}
    path = paths.feed_control_log_path(day)
    if not path.is_file():
        return counts
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        kind = row.get("kind")
        if kind == "TRANSITION":
            counts["transitions"] += 1
            if row.get("new") == "DAY_DEMOTED":
                counts["demotions"] += 1
        elif kind == "ADMIT" and row.get("mode") == "active":
            counts["admitted_symbols"] += int(row.get("symbols_n", 0))
        elif kind == "RECYCLE":
            counts["recycles"] += 1
    return counts


def _feed_health(paths: DataPaths, day: dt.date) -> dict[str, dict[str, Any]]:
    """Disconnects summed and lag maxima taken across the close-time health snapshots of each feed."""
    feeds: dict[str, dict[str, Any]] = {}
    directory = feed_health_archive_dir(paths, day)
    if not directory.is_dir():
        return feeds
    for path in sorted(directory.glob("*.json")):
        payload = _read_json(path)
        if not isinstance(payload, dict) or not isinstance(payload.get("feed_id"), str):
            continue
        entry = feeds.setdefault(payload["feed_id"], {"disconnects": 0, "lag_p50_s": None, "lag_p99_s": None})
        entry["disconnects"] += int(payload.get("disconnects_total") or 0)
        for key in ("lag_p50_s", "lag_p99_s"):
            value = payload.get(key)
            if isinstance(value, int | float) and not isinstance(value, bool):
                entry[key] = value if entry[key] is None else max(entry[key], value)
    return feeds


def build_feed_quality(day: dt.date, *, paths: DataPaths) -> FeedQualityReport:
    """Assemble the day's quality report from the selection summary, controller log and archived health; read-only."""
    summary = _read_json(selection_summary_path(paths, day))
    summary = summary if isinstance(summary, dict) else {}
    anchors = resolve_session_anchors(paths.session_calendar_dir, day)
    open_wall = dt.datetime.combine(day, anchors.regular_open, tzinfo=_KST).timestamp()
    close_wall = dt.datetime.combine(day, anchors.regular_close, tzinfo=_KST).timestamp()
    session_minutes = max(1, int((close_wall - open_wall) // _SECONDS_PER_MINUTE))
    vendors: dict[str, dict[str, Any]] = {}
    for vendor, stats in dict(summary.get("vendors", {})).items():
        minutes = int(stats.get("minutes_with_frames", 0))
        vendors[vendor] = {
            "availability": round(min(1.0, minutes / session_minutes), 4),
            "minutes_with_frames": minutes,
            "tick_frames": int(stats.get("tick_frames", 0)),
            "book_frames": int(stats.get("book_frames", 0)),
        }
    return FeedQualityReport(
        date=day.isoformat(),
        selection_status=str(summary.get("status", "NO_SELECTION")),
        session_minutes=session_minutes,
        vendors=vendors,
        controller=_controller_counts(paths, day),
        feeds=_feed_health(paths, day),
    )


def _atomic_json(path: pathlib.Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def write_feed_quality(report: FeedQualityReport, *, paths: DataPaths) -> None:
    """Write the day's report and upsert it into the last-30-days rolling file (atomic)."""
    _atomic_json(feed_quality_report_path(paths, dt.date.fromisoformat(report.date)), report.to_json())
    existing = _read_json(rolling_path(paths))
    days = [row for row in existing if isinstance(row, dict)] if isinstance(existing, list) else []
    days = [row for row in days if row.get("date") != report.date]
    days.append({"date": report.date, "selection_status": report.selection_status, "vendors": report.to_json()["vendors"]})
    days.sort(key=lambda row: str(row["date"]))
    _atomic_json(rolling_path(paths), days[-ROLLING_DAYS:])


def consecutive_days_below(paths: DataPaths, *, floor: float, vendor: str) -> int:
    """Count of most recent consecutive rolling days where the vendor's availability is below `floor` (days without it break the run)."""
    existing = _read_json(rolling_path(paths))
    if not isinstance(existing, list):
        return 0
    run = 0
    for row in sorted((r for r in existing if isinstance(r, dict)), key=lambda r: str(r.get("date")), reverse=True):
        stats = dict(row.get("vendors", {})).get(vendor)
        if not isinstance(stats, dict) or float(stats.get("availability", 0.0)) >= floor:
            break
        run += 1
    return run


def digest_lines(paths: DataPaths, day: dt.date) -> Sequence[str]:
    """key=value EOD digest lines: controller counts from the same-day journal, and quality from the nightly report if present."""
    lines = [f"feed_control_{key}={value}" for key, value in sorted(_controller_counts(paths, day).items())]
    report = _read_json(feed_quality_report_path(paths, day))
    if not isinstance(report, dict):
        return (*lines, "feed_quality_status=PENDING")
    lines.append(f"feed_quality_status={report.get('selection_status', 'NO_SELECTION')}")
    for vendor, stats in sorted(dict(report.get("vendors", {})).items()):
        lines.append(f"feed_quality_{vendor}_availability={stats.get('availability')}")
    return tuple(lines)
