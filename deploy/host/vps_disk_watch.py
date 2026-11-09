"""Host disk forecast watcher for the shared VPS.

Measures the used bytes of ``/`` and the tracked data roots once per KST day,
forecasts the days until the disk reaches the X2 target at the net growth over
a trailing window, and reports an alert only when a human needs to act.
Never deletes or modifies anything outside its own state directory.
Stdlib only; runs on the VPS host, not in a container.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

_KST: Final = ZoneInfo("Asia/Seoul")
_GIB: Final[int] = 1024**3
_NICE_LEVEL: Final[int] = 19
_DU_TIMEOUT_S: Final[int] = 240
_DEFAULT_STATE_DIR: Final = Path(".local/state/vps-disk-watch")
_SAMPLES_NAME: Final = "samples.jsonl"
_ALERT_NAME: Final = "alert.json"

TRACKED_PATHS: Final[tuple[tuple[str, str], ...]] = (
    ("krx_l0", "krx-alpha/data/l0"),
    ("krx_l1", "krx-alpha/data/l1"),
    ("kca_capture", "k-closing-alpha/data/history/capture"),
    ("kca_ledger", "k-closing-alpha/data/history/capture/offsite/ledger"),
    ("kca_history", "k-closing-alpha/data/history"),
    ("kse_data", "k-stock-engine/data"),
)
# Non-nested tracked roots; their sum with ``other`` covers used bytes without double counting.
TOP_LEVEL_PARTS: Final[frozenset[str]] = frozenset({"krx_l0", "krx_l1", "kca_history", "kse_data"})


@dataclass(frozen=True, slots=True)
class Thresholds:
    """Alert and forecast limits; the defaults are the operating contract."""

    warn_used_pct: float = 70.0
    crit_used_pct: float = 80.0
    crit_free_gb: float = 25.0
    target_pct: float = 85.0
    warn_days: int = 180
    crit_days: int = 60
    window_days: int = 28
    min_span_days: int = 7
    warn_realert_days: int = 7
    keep_samples: int = 400


@dataclass(frozen=True, slots=True)
class Sample:
    """One day's disk usage in bytes; ``parts`` maps tracked names to their size."""

    day: str
    used_b: int
    total_b: int
    parts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Measurement:
    sample: Sample
    degraded: bool


class MeasurementError(Exception):
    """The root filesystem could not be measured; no sample is recorded."""


def _days_between(later: str, earlier: str) -> int:
    return (dt.date.fromisoformat(later) - dt.date.fromisoformat(earlier)).days


def _window(samples: Sequence[Sample], *, today: str, thresholds: Thresholds) -> list[Sample]:
    """Samples from the trailing window ending at ``today``, oldest first."""
    inside = [s for s in samples if 0 <= _days_between(today, s.day) <= thresholds.window_days]
    return sorted(inside, key=lambda s: s.day)


def forecast_days_to_target(
    samples: Sequence[Sample], *, thresholds: Thresholds, today: str
) -> float | None:
    """Days until used bytes reach the target share, at net window growth.

    Returns None when the window spans fewer than ``min_span_days`` (warm-up) and
    ``inf`` when usage is flat or shrinking over the window.
    """
    window = _window(samples, today=today, thresholds=thresholds)
    if len(window) < 2:
        return None
    oldest, newest = window[0], window[-1]
    span = _days_between(newest.day, oldest.day)
    if span < thresholds.min_span_days:
        return None
    growth = newest.used_b - oldest.used_b
    if growth <= 0:
        return float("inf")
    target_b = thresholds.target_pct / 100.0 * newest.total_b
    gap = target_b - newest.used_b
    if gap <= 0:
        return 0.0
    return gap / (growth / span)


def classify(
    sample: Sample, days: float | None, *, thresholds: Thresholds
) -> tuple[str, tuple[str, ...]]:
    """Return the alert level (OK, WARN or CRIT) and every reason that triggered it."""
    used_pct = sample.used_b * 100.0 / sample.total_b
    free_gb = (sample.total_b - sample.used_b) / _GIB
    crit: list[str] = []
    warn: list[str] = []
    if used_pct >= thresholds.crit_used_pct:
        crit.append("used_crit")
    if free_gb < thresholds.crit_free_gb:
        crit.append("free_crit")
    if days is not None and days < thresholds.crit_days:
        crit.append("days_crit")
    if used_pct >= thresholds.warn_used_pct:
        warn.append("used_warn")
    if days is not None and days < thresholds.warn_days:
        warn.append("days_warn")
    reasons = tuple(crit + warn)
    if crit:
        return "CRIT", reasons
    if warn:
        return "WARN", reasons
    return "OK", ()


def _top_level_used(sample: Sample) -> int:
    return sample.used_b - sum(sample.parts.get(name, 0) for name in TOP_LEVEL_PARTS)


def top_growth(old: Sample, new: Sample, n: int = 3) -> list[tuple[str, int]]:
    """Largest positive byte growths between two samples, including the untracked ``other`` remainder."""
    growth = {
        name: new.parts.get(name, 0) - old.parts.get(name, 0)
        for name in old.parts.keys() | new.parts.keys()
    }
    growth["other"] = _top_level_used(new) - _top_level_used(old)
    declared = {name: index for index, (name, _) in enumerate(TRACKED_PATHS)}
    ranked = sorted(
        ((name, delta) for name, delta in growth.items() if delta > 0),
        key=lambda kv: (-kv[1], declared.get(kv[0], len(declared))),
    )
    return ranked[:n]


def should_alert(
    level: str,
    last_alert: Mapping[str, str] | None,
    *,
    today: str,
    thresholds: Thresholds,
) -> bool:
    """Decide whether a due level should page now, deduplicating repeats of the same alert."""
    if level == "OK":
        return False
    if last_alert is None:
        return True
    last_level = last_alert.get("level")
    last_day = last_alert.get("day")
    if level == "CRIT":
        return not (last_level == "CRIT" and last_day == today)
    if last_day is None:
        return True
    try:
        age = _days_between(today, last_day)
    except ValueError:
        return True
    return not (0 <= age < thresholds.warn_realert_days)


def _du_bytes(path: Path) -> tuple[int, bool]:
    if not path.exists():
        return 0, False
    try:
        proc = subprocess.run(  # noqa: S603 - fixed argv without shell
            ["nice", "-n", str(_NICE_LEVEL), "du", "-sx", "--block-size=1", str(path)],  # noqa: S607 - host PATH nice/du
            capture_output=True,
            text=True,
            timeout=_DU_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 0, False
    if proc.returncode != 0:
        return 0, False
    try:
        return int(proc.stdout.split()[0]), True
    except (IndexError, ValueError):
        return 0, False


def measure_host(home: Path, *, day: str) -> Measurement:
    """Sample root filesystem usage and the tracked roots under ``home``."""
    try:
        usage = shutil.disk_usage("/")
    except OSError as exc:
        raise MeasurementError("disk usage unavailable") from exc
    parts: dict[str, int] = {}
    degraded = False
    for name, rel in TRACKED_PATHS:
        size, ok = _du_bytes(home / rel)
        parts[name] = size
        degraded = degraded or not ok
    return Measurement(
        sample=Sample(day=day, used_b=usage.used, total_b=usage.used + usage.free, parts=parts),
        degraded=degraded,
    )


def _kst_today() -> str:
    return dt.datetime.now(_KST).date().isoformat()


def _sample_from_json(raw: object) -> Sample | None:
    if not isinstance(raw, dict):
        return None
    day, used_b, total_b, parts = raw.get("day"), raw.get("used_b"), raw.get("total_b"), raw.get("parts")
    if not isinstance(day, str) or not isinstance(parts, dict):
        return None
    if not isinstance(used_b, int) or not isinstance(total_b, int) or isinstance(used_b, bool) or isinstance(total_b, bool):
        return None
    if not all(isinstance(k, str) and isinstance(v, int) and not isinstance(v, bool) for k, v in parts.items()):
        return None
    try:
        dt.date.fromisoformat(day)
    except ValueError:
        return None
    return Sample(day=day, used_b=used_b, total_b=total_b, parts=dict(parts))


def _load_samples(path: Path) -> list[Sample]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    samples: list[Sample] = []
    for line in lines:
        try:
            sample = _sample_from_json(json.loads(line))
        except json.JSONDecodeError:
            continue
        if sample is not None:
            samples.append(sample)
    return samples


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _upsert(samples: Sequence[Sample], new: Sample, *, keep: int) -> list[Sample]:
    by_day = {sample.day: sample for sample in samples}
    by_day[new.day] = new
    return sorted(by_day.values(), key=lambda s: s.day)[-keep:]


def _save_samples(path: Path, samples: Sequence[Sample]) -> None:
    lines = [
        json.dumps(
            {"day": s.day, "used_b": s.used_b, "total_b": s.total_b, "parts": s.parts},
            sort_keys=True,
        )
        for s in samples
    ]
    _write_atomic(path, "".join(line + "\n" for line in lines))


def _read_alert(path: Path) -> dict[str, str] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get("level"), str) or not isinstance(raw.get("day"), str):
        return None
    return {"level": raw["level"], "day": raw["day"]}


def _status(level: str, days: float | None, degraded: bool) -> str:
    if level != "OK":
        return level
    if days is None:
        return "WARMUP"
    return "DEGRADED" if degraded else "OK"


def _format_days(days: float | None) -> str:
    if days is None:
        return "na"
    if days == float("inf"):
        return "inf"
    return f"{days:.0f}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VPS disk forecast watcher (measure and alert only).")
    parser.add_argument("--state-dir", type=Path, default=None)
    parser.add_argument("--home", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    thresholds = Thresholds()
    state_dir = args.state_dir or (Path.home() / _DEFAULT_STATE_DIR)
    home = args.home or Path.home()
    today = _kst_today()
    try:
        measurement = measure_host(home, day=today)
    except MeasurementError as exc:
        print(f"[SYS] stage=disk_watch status=UNMEASURED reasons={exc}", file=sys.stderr)  # noqa: T201 - runner stderr protocol
        return 2

    samples_path = state_dir / _SAMPLES_NAME
    alert_path = state_dir / _ALERT_NAME
    samples = _upsert(_load_samples(samples_path), measurement.sample, keep=thresholds.keep_samples)
    _save_samples(samples_path, samples)

    days = forecast_days_to_target(samples, thresholds=thresholds, today=today)
    level, reasons = classify(measurement.sample, days, thresholds=thresholds)
    if measurement.degraded:
        reasons = (*reasons, "degraded")

    alerted = should_alert(level, _read_alert(alert_path), today=today, thresholds=thresholds)
    if alerted:
        _write_atomic(alert_path, json.dumps({"level": level, "day": today}, sort_keys=True))

    window = _window(samples, today=today, thresholds=thresholds)
    net_growth_gb = 0.0
    top: list[tuple[str, int]] = []
    if len(window) >= 2:
        net_growth_gb = (window[-1].used_b - window[0].used_b) / _GIB
        top = top_growth(window[0], window[-1])
    top_text = ",".join(f"{name}:+{delta / _GIB:.1f}" for name, delta in top) or "-"

    current = measurement.sample
    print(  # noqa: T201 - runner stdout protocol
        f"[SYS] stage=disk_watch status={_status(level, days, measurement.degraded)}"
        f" used_pct={current.used_b * 100.0 / current.total_b:.1f}"
        f" free_gb={(current.total_b - current.used_b) / _GIB:.0f}"
        f" net_growth_gb_window={net_growth_gb:.1f}"
        f" days_to_target={_format_days(days)}"
        f" top={top_text}"
        f" reasons={','.join(reasons) or '-'}"
        f" alerted={int(alerted)}"
    )
    return 1 if alerted else 0


if __name__ == "__main__":
    raise SystemExit(main())
