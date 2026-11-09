"""Invariant guards for the host disk forecast watcher."""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

GIB = 1024**3
TODAY = dt.date(2026, 11, 10)


def _load_watch() -> Any:
    spec = importlib.util.spec_from_file_location("vps_disk_watch", "deploy/host/vps_disk_watch.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["vps_disk_watch"] = module
    spec.loader.exec_module(module)
    return module


watch = _load_watch()
TH = watch.Thresholds()


def _day(offset: int) -> str:
    return (TODAY - dt.timedelta(days=28 - offset)).isoformat()


def _series(used_gb: list[float], *, total_gb: float = 100.0) -> list:
    return [
        watch.Sample(day=_day(i), used_b=int(u * GIB), total_b=int(total_gb * GIB), parts={"krx_l0": int(u * GIB) // 2})
        for i, u in enumerate(used_gb)
    ]


def test_flat_growth_is_ok() -> None:
    samples = _series([50.0] * 29)

    days = watch.forecast_days_to_target(samples, thresholds=TH, today=TODAY.isoformat())
    level, _ = watch.classify(samples[-1], days, thresholds=TH)

    assert days == float("inf")
    assert level == "OK"


def test_linear_growth_projects_days() -> None:
    # Target 85% of 100 GB is 85 GB; newest sits at 65 GB, so 20 GB remain at +1 GB/day.
    used = [37.0 + i for i in range(29)]
    samples = _series(used)

    days = watch.forecast_days_to_target(samples, thresholds=TH, today=TODAY.isoformat())

    assert days == pytest.approx(20.0, abs=1.0)
    assert watch.classify(samples[-1], days, thresholds=TH)[0] == "CRIT"


def test_linear_growth_warns_when_runway_is_long() -> None:
    # +0.1 GB/day with 20 GB remaining projects ~200 days, above the 180-day WARN line.
    used = [45.0 + 0.1 * i for i in range(29)]
    samples = _series(used)

    days = watch.forecast_days_to_target(samples, thresholds=TH, today=TODAY.isoformat())
    level, _ = watch.classify(samples[-1], days, thresholds=TH)

    assert days > TH.warn_days
    assert level == "OK"


def test_sawtooth_net_growth_cancels() -> None:
    used = [50.0, 57.0, 50.0, 57.0, 50.0, 57.0, 50.0] * 4 + [50.0]
    samples = _series(used[:29])

    days = watch.forecast_days_to_target(samples, thresholds=TH, today=TODAY.isoformat())

    assert days == float("inf")


def test_warmup_under_seven_days() -> None:
    samples = [
        watch.Sample(day=_day(26 + i), used_b=50 * GIB, total_b=100 * GIB, parts={})
        for i in range(3)
    ]

    days = watch.forecast_days_to_target(samples, thresholds=TH, today=TODAY.isoformat())
    level, _ = watch.classify(samples[-1], days, thresholds=TH)

    assert days is None
    assert level == "OK"
    assert watch._status(level, days, degraded=False) == "WARMUP"


def test_used_pct_and_free_thresholds() -> None:
    warn = watch.Sample(day=TODAY.isoformat(), used_b=71 * GIB, total_b=100 * GIB)
    crit = watch.Sample(day=TODAY.isoformat(), used_b=81 * GIB, total_b=100 * GIB)
    free_crit = watch.Sample(day=TODAY.isoformat(), used_b=60 * GIB, total_b=80 * GIB)

    assert watch.classify(warn, None, thresholds=TH)[0] == "WARN"
    assert watch.classify(crit, None, thresholds=TH)[0] == "CRIT"
    assert watch.classify(free_crit, None, thresholds=TH)[0] == "CRIT"


def test_top_growth_names_leaker() -> None:
    # kca_ledger lives inside kca_history, so both grow by 3 GB; the nested path is declared first and wins the tie.
    old = watch.Sample(
        day=_day(0),
        used_b=10 * GIB,
        total_b=100 * GIB,
        parts={"kca_ledger": 1 * GIB, "krx_l1": 2 * GIB, "krx_l0": 1 * GIB, "kca_history": 1 * GIB, "kse_data": 0, "kca_capture": 0},
    )
    new = watch.Sample(
        day=_day(28),
        used_b=13 * GIB + int(0.1 * GIB),
        total_b=100 * GIB,
        parts={"kca_ledger": 4 * GIB, "krx_l1": 2 * GIB + int(0.1 * GIB), "krx_l0": 1 * GIB, "kca_history": 4 * GIB, "kse_data": 0, "kca_capture": 0},
    )

    growth = watch.top_growth(old, new)
    by_name = dict(growth)

    assert growth[0][0] == "kca_ledger"
    assert by_name["krx_l1"] == pytest.approx(0.1 * GIB, rel=1e-6)


def test_top_growth_includes_other_when_untracked_dominates() -> None:
    old = watch.Sample(day=_day(0), used_b=10 * GIB, total_b=100 * GIB, parts={"krx_l0": 1 * GIB})
    new = watch.Sample(day=_day(28), used_b=20 * GIB, total_b=100 * GIB, parts={"krx_l0": 1 * GIB + 1})

    growth = dict(watch.top_growth(old, new))

    assert growth["other"] == pytest.approx(10 * GIB, rel=1e-6)


def test_alert_dedup() -> None:
    today = TODAY.isoformat()
    yesterday = (TODAY - dt.timedelta(days=1)).isoformat()
    eight_ago = (TODAY - dt.timedelta(days=8)).isoformat()

    assert watch.should_alert("WARN", {"level": "WARN", "day": yesterday}, today=today, thresholds=TH) is False
    assert watch.should_alert("WARN", {"level": "WARN", "day": eight_ago}, today=today, thresholds=TH) is True
    assert watch.should_alert("CRIT", {"level": "CRIT", "day": yesterday}, today=today, thresholds=TH) is True
    assert watch.should_alert("CRIT", {"level": "CRIT", "day": today}, today=today, thresholds=TH) is False
    assert watch.should_alert("CRIT", {"level": "WARN", "day": yesterday}, today=today, thresholds=TH) is True
    assert watch.should_alert("OK", None, today=today, thresholds=TH) is False


def test_same_day_rerun_replaces_sample() -> None:
    first = watch.Sample(day=TODAY.isoformat(), used_b=50, total_b=100, parts={})
    second = watch.Sample(day=TODAY.isoformat(), used_b=60, total_b=100, parts={})

    merged = watch._upsert([first], second, keep=400)

    assert merged == [second]


def test_corrupt_state_tolerated(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir()
    good = {"day": TODAY.isoformat(), "used_b": 5, "total_b": 10, "parts": {"krx_l0": 1}}
    (state / "samples.jsonl").write_text("garbage\n{\"day\": 3}\n" + json.dumps(good) + "\n", encoding="utf-8")
    (state / "alert.json").write_text("{not json", encoding="utf-8")

    assert watch._load_samples(state / "samples.jsonl") == [watch.Sample(**good)]
    assert watch._read_alert(state / "alert.json") is None


def test_missing_tracked_path_degraded_not_fatal(tmp_path: Path) -> None:
    (tmp_path / "krx-alpha/data/l0").mkdir(parents=True)

    measurement = watch.measure_host(tmp_path, day=TODAY.isoformat())

    assert measurement.degraded is True
    assert measurement.sample.parts["krx_l1"] == 0
    assert measurement.sample.total_b > 0


def _stub_measure(used_pct: float, *, degraded: bool = False):
    def _measure(home: Path, *, day: str):
        total = 100 * GIB
        sample = watch.Sample(day=day, used_b=int(total * used_pct / 100), total_b=total, parts={})
        return watch.Measurement(sample=sample, degraded=degraded)

    return _measure


def test_main_exit_codes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(watch, "_kst_today", lambda: TODAY.isoformat())
    state = tmp_path / "state"
    argv = ["--state-dir", str(state), "--home", str(tmp_path)]

    monkeypatch.setattr(watch, "measure_host", _stub_measure(50.0))
    assert watch.main(argv) == 0
    assert "status=WARMUP" in capsys.readouterr().out

    monkeypatch.setattr(watch, "measure_host", _stub_measure(85.0))
    assert watch.main(argv) == 1
    out = capsys.readouterr().out
    assert "status=CRIT" in out
    assert "alerted=1" in out

    monkeypatch.setattr(watch, "measure_host", _stub_measure(85.0))
    assert watch.main(argv) == 0
    assert "alerted=0" in capsys.readouterr().out

    def _unmeasurable(home: Path, *, day: str):
        raise watch.MeasurementError("disk usage unavailable")

    monkeypatch.setattr(watch, "measure_host", _unmeasurable)
    assert watch.main(argv) == 2


def test_main_output_is_one_stdout_line_with_tracked_names_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(watch, "_kst_today", lambda: TODAY.isoformat())
    monkeypatch.setattr(watch, "measure_host", _stub_measure(50.0))

    watch.main(["--state-dir", str(tmp_path / "state"), "--home", str(tmp_path)])

    out = capsys.readouterr().out.strip()
    assert out.count("\n") == 0
    assert out.startswith("[SYS] stage=disk_watch status=")
    assert str(tmp_path) not in out


def test_no_destructive_calls() -> None:
    source = Path("deploy/host/vps_disk_watch.py").read_text(encoding="utf-8")

    for forbidden in ("os.remove(", ".unlink(", "rmtree", "docker", "rclone", "urllib", "socket", "requests"):
        assert forbidden not in source
    assert source.count("subprocess.run(") == 1
    assert '["nice", "-n", str(_NICE_LEVEL), "du"' in source
