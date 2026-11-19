"""Host disk forecast watcher for the shared VPS.

Measures the used bytes of ``/`` and the tracked data roots once per KST day,
forecasts the days until the disk reaches the X2 target at the net growth after
every retention-bound root has reached its horizon, and pages only on CRIT.
Never deletes or modifies anything outside its own state directory.
Stdlib only; runs on the VPS host, not in a container.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
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
_DEFAULT_BACKLOG_REL: Final = Path("krx-alpha/data/prune_backlog.json")
_DOCKER_DF_ARGV: Final[tuple[str, ...]] = ("docker", "system", "df", "--format", "{{json .}}")
_DOCKER_TIMEOUT_S: Final[int] = 60
_OLDEST_MAX_DEPTH: Final[int] = 6
_DATE_NAME_RE: Final = re.compile(r"(?:dt=)?(\d{4}-\d{2}-\d{2})(?:\.[A-Za-z0-9.]+)?")
_DOCKER_UNITS: Final[Mapping[str, int]] = {"B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4}
_DOCKER_SIZE_RE: Final = re.compile(r"\s*([\d.]+)\s*([A-Za-z]+)")

TRACKED_PATHS: Final[tuple[tuple[str, str], ...]] = (
    ("krx_l0", "krx-alpha/data/l0"),
    ("krx_l1", "krx-alpha/data/l1"),
    ("krx_data", "krx-alpha/data"),
    ("kca_capture", "k-closing-alpha/data/history/capture"),
    ("kca_ledger", "k-closing-alpha/data/history/capture/offsite/ledger"),
    ("kca_history", "k-closing-alpha/data/history"),
    ("kse_data", "k-stock-engine/data"),
    ("home_cache", ".cache"),
    ("vscode_server", ".vscode-server"),
    ("crypto_data", "crypto-pilot/data"),
    ("repair_preserve", "krx-repair-preserve"),
    ("ops_scratch", "ops-scratch"),
)
SYSTEM_PATHS: Final[tuple[tuple[str, Path], ...]] = (("journal", Path("/var/log/journal")),)
# Non-nested roots; their sum with ``other`` covers used bytes without double counting.
TOP_LEVEL_PARTS: Final[frozenset[str]] = frozenset(
    {
        "krx_data",
        "kca_history",
        "kse_data",
        "home_cache",
        "vscode_server",
        "crypto_data",
        "repair_preserve",
        "ops_scratch",
        "journal",
        "docker_images",
    }
)
# Parts contained in a top-level part; excluded from the ``other`` remainder.
NESTED_PARTS: Final[frozenset[str]] = frozenset(
    {"krx_l0", "krx_l1", "krx_rest", "kca_capture", "kca_ledger", "docker_reclaimable"}
)
# Dated roots whose oldest partition is recorded, with subtrees that outlive the root's retention.
OLDEST_ROOTS: Final[frozenset[str]] = frozenset({"krx_l0", "krx_l1", "kca_capture", "kca_history"})
_OLDEST_EXCLUDED: Final[Mapping[str, frozenset[str]]] = {"krx_l1": frozenset({"history/minute"})}


@dataclass(frozen=True, slots=True)
class Thresholds:
    """Alert and forecast limits; the defaults are the operating contract."""

    warn_used_pct: float = 70.0
    crit_used_pct: float = 80.0
    crit_free_gb: float = 25.0
    target_pct: float = 85.0
    warn_days: int = 60
    crit_days: int = 30
    window_days: int = 28
    min_span_days: int = 14
    keep_samples: int = 400
    other_report_gb: float = 10.0
    retain_days: Mapping[str, int] = field(default_factory=lambda: {"krx_l1": 30})


@dataclass(frozen=True, slots=True)
class Sample:
    """One day's disk usage in bytes; ``parts`` maps tracked names to their size."""

    day: str
    used_b: int
    total_b: int
    parts: dict[str, int] = field(default_factory=dict)
    oldest: dict[str, str] = field(default_factory=dict)


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


def _at_horizon(sample: Sample, thresholds: Thresholds) -> bool:
    for root, retain in thresholds.retain_days.items():
        oldest = sample.oldest.get(root)
        if oldest is None or _days_between(sample.day, oldest) < retain:
            return False
    return True


def _slope_bytes_per_day(samples: Sequence[Sample]) -> float:
    xs = [_days_between(s.day, samples[0].day) for s in samples]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(s.used_b for s in samples) / len(samples)
    var_x = sum((x - mean_x) ** 2 for x in xs)
    return sum((x - mean_x) * (s.used_b - mean_y) for x, s in zip(xs, samples, strict=True)) / var_x


def forecast_days_to_target(
    samples: Sequence[Sample], *, thresholds: Thresholds, today: str
) -> float | None:
    """Days until used bytes reach the target share at the net least-squares growth after the retention horizon.

    Returns None (warm-up) until every retention-bound root has reached its horizon and the
    post-horizon samples span ``min_span_days``; ``inf`` when the net slope is not positive.
    """
    window = _window(samples, today=today, thresholds=thresholds)
    start = next((i for i, s in enumerate(window) if _at_horizon(s, thresholds)), None)
    if start is None:
        return None
    usable = window[start:]
    if len(usable) < 2 or _days_between(usable[-1].day, usable[0].day) < thresholds.min_span_days:
        return None
    slope = _slope_bytes_per_day(usable)
    if slope <= 0:
        return float("inf")
    newest = usable[-1]
    gap = thresholds.target_pct / 100.0 * newest.total_b - newest.used_b
    if gap <= 0:
        return 0.0
    return gap / slope


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
    """Used bytes outside every tracked top-level root (the ``other`` remainder)."""
    tracked = sum(sample.parts.get(name, 0) for name in TOP_LEVEL_PARTS)
    if "krx_data" not in sample.parts:
        tracked += sample.parts.get("krx_l0", 0) + sample.parts.get("krx_l1", 0)
    return sample.used_b - tracked


def top_growth(old: Sample, new: Sample, n: int = 3) -> list[tuple[str, int]]:
    """Largest positive byte growths between two samples, including the untracked ``other`` remainder."""
    growth = {
        name: new.parts.get(name, 0) - old.parts.get(name, 0)
        for name in new.parts
        if name in old.parts
    }
    growth["other"] = _top_level_used(new) - _top_level_used(old)
    declared = {name: index for index, (name, _) in enumerate(TRACKED_PATHS)}
    ranked = sorted(
        ((name, delta) for name, delta in growth.items() if delta > 0),
        key=lambda kv: (-kv[1], declared.get(kv[0], len(declared))),
    )
    return ranked[:n]


def should_alert(level: str, last_alert: Mapping[str, str] | None, *, today: str) -> bool:
    """Page only on CRIT, at most once per KST day."""
    if level != "CRIT":
        return False
    return not (last_alert is not None and last_alert.get("level") == "CRIT" and last_alert.get("day") == today)


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


def _parse_docker_size(text: object) -> int | None:
    """Parse docker's decimal size text such as ``9.2GB`` or ``4.43GB (48%)``."""
    if not isinstance(text, str):
        return None
    match = _DOCKER_SIZE_RE.match(text)
    unit = _DOCKER_UNITS.get(match.group(2).upper()) if match else None
    if match is None or unit is None:
        return None
    try:
        return int(float(match.group(1)) * unit)
    except ValueError:
        return None


def _docker_image_bytes() -> tuple[int, int, bool]:
    """Return (images size, reclaimable size, ok) from one read-only ``docker system df``."""
    try:
        proc = subprocess.run(  # noqa: S603 - fixed argv without shell
            _DOCKER_DF_ARGV,
            capture_output=True,
            text=True,
            timeout=_DOCKER_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 0, 0, False
    if proc.returncode != 0:
        return 0, 0, False
    for line in proc.stdout.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and row.get("Type") == "Images":
            size = _parse_docker_size(row.get("Size"))
            reclaimable = _parse_docker_size(row.get("Reclaimable"))
            if size is not None and reclaimable is not None:
                return size, reclaimable, True
    return 0, 0, False


def _date_of(name: str) -> dt.date | None:
    match = _DATE_NAME_RE.fullmatch(name)
    if match is None:
        return None
    try:
        return dt.date.fromisoformat(match.group(1))
    except ValueError:
        return None


def oldest_partition(root: Path, *, excluded: frozenset[str] = frozenset()) -> str | None:
    """ISO date of the oldest ``dt=YYYY-MM-DD``/``YYYY-MM-DD`` entry; dated entries are never descended."""
    oldest: dt.date | None = None
    pending: list[tuple[Path, int]] = [(root, 0)]
    while pending:
        directory, depth = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue
        for entry in entries:
            day = _date_of(entry.name)
            if day is not None:
                oldest = day if oldest is None else min(oldest, day)
            elif (
                depth < _OLDEST_MAX_DEPTH
                and entry.is_dir(follow_symlinks=False)
                and Path(entry.path).relative_to(root).as_posix() not in excluded
            ):
                pending.append((Path(entry.path), depth + 1))
    return None if oldest is None else oldest.isoformat()


def measure_host(
    home: Path, *, day: str, system_paths: Sequence[tuple[str, Path]] = SYSTEM_PATHS
) -> Measurement:
    """Sample root filesystem usage, the tracked roots under ``home`` and the system-level parts."""
    try:
        usage = shutil.disk_usage("/")
    except OSError as exc:
        raise MeasurementError("disk usage unavailable") from exc
    parts: dict[str, int] = {}
    oldest: dict[str, str] = {}
    degraded = False
    roots = [(name, home / rel) for name, rel in TRACKED_PATHS] + [(name, path) for name, path in system_paths]
    for name, path in roots:
        size, ok = _du_bytes(path)
        parts[name] = size
        degraded = degraded or not ok
        if ok and name in OLDEST_ROOTS:
            found = oldest_partition(path, excluded=_OLDEST_EXCLUDED.get(name, frozenset()))
            if found is not None:
                oldest[name] = found
    if "krx_data" in parts:
        parts["krx_rest"] = max(0, parts["krx_data"] - parts.get("krx_l0", 0) - parts.get("krx_l1", 0))
    images, reclaimable, docker_ok = _docker_image_bytes()
    parts["docker_images"] = images
    parts["docker_reclaimable"] = reclaimable
    return Measurement(
        sample=Sample(day=day, used_b=usage.used, total_b=usage.used + usage.free, parts=parts, oldest=oldest),
        degraded=degraded or not docker_ok,
    )


def _kst_today() -> str:
    return dt.datetime.now(_KST).date().isoformat()


def _oldest_from_json(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    valid: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, str):
            try:
                dt.date.fromisoformat(value)
            except ValueError:
                continue
            valid[key] = value
    return valid


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
    return Sample(day=day, used_b=used_b, total_b=total_b, parts=dict(parts), oldest=_oldest_from_json(raw.get("oldest")))


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
            {"day": s.day, "used_b": s.used_b, "total_b": s.total_b, "parts": s.parts, "oldest": s.oldest},
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


def read_prune_backlog(path: Path) -> int | None:
    """Unconfirmed expired L1 partitions written by krx EOD; None when absent or unreadable."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    count = raw.get("l1_partitions_unconfirmed") if isinstance(raw, dict) else None
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        return None
    return count


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
    parser.add_argument("--backlog-path", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    thresholds = Thresholds()
    state_dir = args.state_dir or (Path.home() / _DEFAULT_STATE_DIR)
    home = args.home or Path.home()
    backlog_path = args.backlog_path or (home / _DEFAULT_BACKLOG_REL)
    today = _kst_today()
    started = time.monotonic()
    try:
        measurement = measure_host(home, day=today)
    except MeasurementError as exc:
        print(f"[SYS] stage=disk_watch status=UNMEASURED reasons={exc}", file=sys.stderr)  # noqa: T201 - runner stderr protocol
        return 2
    elapsed_s = time.monotonic() - started

    samples_path = state_dir / _SAMPLES_NAME
    alert_path = state_dir / _ALERT_NAME
    samples = _upsert(_load_samples(samples_path), measurement.sample, keep=thresholds.keep_samples)
    _save_samples(samples_path, samples)

    current = measurement.sample
    days = forecast_days_to_target(samples, thresholds=thresholds, today=today)
    level, reasons = classify(current, days, thresholds=thresholds)
    other_gb = _top_level_used(current) / _GIB
    if other_gb >= thresholds.other_report_gb:
        reasons = (*reasons, "other_high")
    if measurement.degraded:
        reasons = (*reasons, "degraded")

    alerted = should_alert(level, _read_alert(alert_path), today=today)
    if level != "OK":
        _write_atomic(alert_path, json.dumps({"level": level, "day": today}, sort_keys=True))

    window = _window(samples, today=today, thresholds=thresholds)
    net_growth_gb = 0.0
    top: list[tuple[str, int]] = []
    if len(window) >= 2:
        net_growth_gb = (window[-1].used_b - window[0].used_b) / _GIB
        top = top_growth(window[0], window[-1])
    top_text = ",".join(f"{name}:+{delta / _GIB:.1f}" for name, delta in top) or "-"
    backlog = read_prune_backlog(backlog_path)

    print(  # noqa: T201 - runner stdout protocol
        f"[SYS] stage=disk_watch status={_status(level, days, measurement.degraded)}"
        f" used_pct={current.used_b * 100.0 / current.total_b:.1f}"
        f" free_gb={(current.total_b - current.used_b) / _GIB:.0f}"
        f" net_growth_gb_window={net_growth_gb:.1f}"
        f" days_to_target={_format_days(days)}"
        f" other_gb={other_gb:.1f}"
        f" elapsed_s={elapsed_s:.1f}"
        f" prune_backlog={'na' if backlog is None else backlog}"
        f" oldest_l1={current.oldest.get('krx_l1', 'na')}"
        f" top={top_text}"
        f" reasons={','.join(reasons) or '-'}"
        f" alerted={int(alerted)}"
    )
    return 1 if alerted else 0


if __name__ == "__main__":
    raise SystemExit(main())
