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


def _oldest(day: str, *, age_days: int) -> dict[str, str]:
    return {"krx_l1": (dt.date.fromisoformat(day) - dt.timedelta(days=age_days)).isoformat()}


def _series(used_gb: list[float], *, total_gb: float = 100.0, age_days: int | None = 30) -> list:
    """Daily samples ending at TODAY; ``age_days`` is the oldest krx_l1 partition age (None = old format)."""
    return [
        watch.Sample(
            day=_day(i),
            used_b=int(u * GIB),
            total_b=int(total_gb * GIB),
            parts={"krx_l0": int(u * GIB) // 2},
            oldest={} if age_days is None else _oldest(_day(i), age_days=age_days),
        )
        for i, u in enumerate(used_gb)
    ]


def _forecast(samples: list) -> float | None:
    return watch.forecast_days_to_target(samples, thresholds=TH, today=TODAY.isoformat())


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
    # +0.1 GB/day with 20 GB remaining projects ~200 days, above the 60-day WARN line.
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


def test_warmup_under_min_span() -> None:
    samples = _series([50.0] * 29)[-10:]

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

    assert watch.should_alert("WARN", None, today=today) is False
    assert watch.should_alert("CRIT", {"level": "CRIT", "day": yesterday}, today=today) is True
    assert watch.should_alert("CRIT", {"level": "CRIT", "day": today}, today=today) is False
    assert watch.should_alert("CRIT", {"level": "WARN", "day": today}, today=today) is True
    assert watch.should_alert("CRIT", None, today=today) is True
    assert watch.should_alert("OK", None, today=today) is False


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


def test_missing_tracked_path_degraded_not_fatal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "krx-alpha/data/l0").mkdir(parents=True)

    _fake_docker(monkeypatch, stdout=_DOCKER_DF_OK)

    measurement = watch.measure_host(tmp_path, day=TODAY.isoformat(), system_paths=())

    assert measurement.degraded is True
    assert measurement.sample.parts["krx_l1"] == 0
    assert measurement.sample.total_b > 0


def test_absent_transient_root_is_zero_not_degraded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name, rel in watch.TRACKED_PATHS:
        if name not in watch.TRANSIENT_PARTS:
            (tmp_path / rel).mkdir(parents=True, exist_ok=True)
    _fake_docker(monkeypatch, stdout=_DOCKER_DF_OK)

    measurement = watch.measure_host(tmp_path, day=TODAY.isoformat(), system_paths=())

    assert measurement.degraded is False
    assert all(measurement.sample.parts[name] == 0 for name in watch.TRANSIENT_PARTS)
    assert watch.TRANSIENT_PARTS <= watch.TOP_LEVEL_PARTS


def _stub_measure(used_pct: float, *, degraded: bool = False, oldest: dict[str, str] | None = None):
    def _measure(home: Path, *, day: str):
        total = 100 * GIB
        sample = watch.Sample(
            day=day, used_b=int(total * used_pct / 100), total_b=total, parts={}, oldest=oldest or {}
        )
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


def test_warn_exit_zero_crit_exit_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(watch, "_kst_today", lambda: TODAY.isoformat())
    state = tmp_path / "state"
    argv = ["--state-dir", str(state), "--home", str(tmp_path)]

    monkeypatch.setattr(watch, "measure_host", _stub_measure(75.0))
    assert watch.main(argv) == 0
    assert "status=WARN" in capsys.readouterr().out
    assert json.loads((state / "alert.json").read_text(encoding="utf-8")) == {"level": "WARN", "day": TODAY.isoformat()}

    monkeypatch.setattr(watch, "measure_host", _stub_measure(85.0))
    assert watch.main(argv) == 1
    assert json.loads((state / "alert.json").read_text(encoding="utf-8")) == {"level": "CRIT", "day": TODAY.isoformat()}


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


_DOCKER_DF_OK = "\n".join(
    [
        json.dumps({"Type": "Images", "Size": "9.5GB", "Reclaimable": "4.43GB (46%)"}),
        json.dumps({"Type": "Containers", "Size": "1GB", "Reclaimable": "0B (0%)"}),
    ]
)


def _fake_docker(monkeypatch: pytest.MonkeyPatch, *, stdout: str = "", exc: Exception | None = None, returncode: int = 0):
    calls: list[list[str]] = []
    real_run = watch.subprocess.run

    def _run(argv, **kwargs):
        if argv[0] != "docker":
            return real_run(argv, **kwargs)
        calls.append(list(argv))
        if exc is not None:
            raise exc
        return watch.subprocess.CompletedProcess(argv, returncode, stdout, "")

    monkeypatch.setattr(watch.subprocess, "run", _run)
    return calls


def test_docker_df_parsed_into_images_and_reclaimable(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _fake_docker(monkeypatch, stdout=_DOCKER_DF_OK)

    assert watch._docker_image_bytes() == (9_500_000_000, 4_430_000_000, True)
    assert calls == [["docker", "system", "df", "--format", "{{json .}}"]]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"exc": watch.subprocess.TimeoutExpired("docker", 60)},
        {"exc": FileNotFoundError("docker")},
        {"returncode": 1},
        {"stdout": "not json"},
    ],
)
def test_docker_df_failure_degraded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kwargs: dict
) -> None:
    _fake_docker(monkeypatch, **kwargs)

    measurement = watch.measure_host(tmp_path, day=TODAY.isoformat(), system_paths=())

    assert measurement.sample.parts["docker_images"] == 0
    assert measurement.degraded is True


def test_allowlisted_subprocess_only() -> None:
    source = Path("deploy/host/vps_disk_watch.py").read_text(encoding="utf-8")

    for forbidden in ("os.remove(", ".unlink(", "rmtree", "rclone", "urllib", "socket", "requests", "shell=True"):
        assert forbidden not in source
    assert source.count("subprocess.run(") == 2
    assert '["nice", "-n", str(_NICE_LEVEL), "du", "-sx", "--block-size=1", str(path)]' in source
    assert '_DOCKER_DF_ARGV: Final[tuple[str, ...]] = ("docker", "system", "df", "--format", "{{json .}}")' in source


# --- forecast evidence ---------------------------------------------------------------------


def _ramp_with_prune(age_days: int) -> list:
    used = [40.0 + 0.8 * i for i in range(9)]
    used[6:] = [u - 4.8 for u in used[6:]]
    samples = _series([0.0] * 29, age_days=age_days)[-9:]
    return [
        watch.Sample(day=s.day, used_b=int(u * GIB), total_b=100 * GIB, parts=s.parts, oldest=s.oldest)
        for s, u in zip(samples, used, strict=True)
    ]


def test_ramp_up_without_horizon_is_warmup() -> None:
    samples = _ramp_with_prune(age_days=20)

    days = _forecast(samples)
    level, reasons = watch.classify(samples[-1], days, thresholds=TH)

    assert days is None
    assert level == "OK"
    assert "days_warn" not in reasons


def test_prune_does_not_end_warmup() -> None:
    samples = _ramp_with_prune(age_days=29)

    assert _forecast(samples) is None


def test_old_samples_backward_compatible(tmp_path: Path) -> None:
    state = tmp_path / "samples.jsonl"
    old = {"day": TODAY.isoformat(), "used_b": 5, "total_b": 10, "parts": {"krx_l0": 1}}
    state.write_text(json.dumps(old) + "\n", encoding="utf-8")

    loaded = watch._load_samples(state)
    samples = _series([50.0 + i for i in range(29)], age_days=None)

    assert loaded == [watch.Sample(**old)]
    assert loaded[0].oldest == {}
    assert _forecast(samples) is None


def test_oldest_roundtrips_and_malformed_oldest_is_dropped(tmp_path: Path) -> None:
    path = tmp_path / "samples.jsonl"
    sample = watch.Sample(day=TODAY.isoformat(), used_b=5, total_b=10, parts={}, oldest={"krx_l1": "2026-10-01"})
    watch._save_samples(path, [sample])
    assert watch._load_samples(path) == [sample]

    bad = {"day": TODAY.isoformat(), "used_b": 5, "total_b": 10, "parts": {}, "oldest": {"krx_l1": "nope", "x": 3}}
    path.write_text(json.dumps(bad) + "\n", encoding="utf-8")
    assert watch._load_samples(path)[0].oldest == {}


def test_steady_state_flat_is_inf() -> None:
    samples = _series([50.0] * 29)

    days = _forecast(samples)

    assert days == float("inf")
    assert watch.classify(samples[-1], days, thresholds=TH)[0] == "OK"


def test_real_growth_after_horizon_warns_then_crits() -> None:
    warn = _series([20.0 + 0.9 * i for i in range(29)], total_gb=100.0)
    # newest 45.2 GB of an 85 GB target: 39.8 GB runway at 0.9 GB/day.
    days_warn = _forecast(warn)
    assert days_warn == pytest.approx(44.2, abs=0.5)
    assert "days_warn" in watch.classify(warn[-1], days_warn, thresholds=TH)[1]

    crit = _series([40.0 + 0.9 * i for i in range(29)], total_gb=100.0)
    # newest 65.2 GB: 19.8 GB runway.
    days_crit = _forecast(crit)
    assert days_crit == pytest.approx(22.0, abs=0.5)
    assert "days_crit" in watch.classify(crit[-1], days_crit, thresholds=TH)[1]


def test_sawtooth_with_underlying_growth_is_seen() -> None:
    # +0.5 GB/day underlying growth plus a weekly-pruned component that drops 3.0 GB (> 1 GB) every 7th day.
    used = [40.0 + 0.5 * i + 0.5 * (i % 7) for i in range(29)]
    samples = _series(used)

    slope_gb_day = watch._slope_bytes_per_day(samples) / GIB
    days = _forecast(samples)

    assert 0.4 <= slope_gb_day <= 0.6
    assert days == pytest.approx((85.0 - used[-1]) / slope_gb_day, rel=1e-6)


def test_pure_sawtooth_is_inf() -> None:
    used = [50.0, 57.0, 50.0, 57.0, 50.0, 57.0, 50.0] * 4 + [50.0]

    assert _forecast(_series(used[:29])) == float("inf")


def test_horizon_gates_on_every_retention_bound_root() -> None:
    th = watch.Thresholds(retain_days={"krx_l1": 30, "kca_x": 40})
    samples = _series([50.0 + i for i in range(29)], age_days=30)

    assert watch.forecast_days_to_target(samples, thresholds=th, today=TODAY.isoformat()) is None


# --- coverage ------------------------------------------------------------------------------


def test_new_roots_explain_other(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_docker(monkeypatch, stdout=_DOCKER_DF_OK)
    for rel, size in {
        "krx-alpha/data/l0/a.bin": 1000,
        "krx-alpha/data/l1/b.bin": 2000,
        "krx-alpha/data/manifest/c.bin": 3000,
        "k-closing-alpha/data/history/capture/d.bin": 4000,
        ".cache/e.bin": 5000,
        ".vscode-server/f.bin": 6000,
        "crypto-pilot/data/g.bin": 7000,
        "krx-repair-preserve/h.bin": 8000,
        "ops-scratch/i.bin": 9000,
    }.items():
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"x" * size)

    sample = watch.measure_host(tmp_path, day=TODAY.isoformat(), system_paths=()).sample
    parts = sample.parts

    assert parts["krx_rest"] >= 3000
    assert parts["krx_data"] >= parts["krx_l0"] + parts["krx_l1"] + parts["krx_rest"] - 4096
    assert watch.NESTED_PARTS.isdisjoint(watch.TOP_LEVEL_PARTS)
    other = watch._top_level_used(sample)
    assert other == sample.used_b - sum(parts.get(name, 0) for name in watch.TOP_LEVEL_PARTS)
    assert parts["docker_images"] == 9_500_000_000


def test_oldest_partition_scans_bounded_and_skips_excluded(tmp_path: Path) -> None:
    (tmp_path / "kis/H0/dt=2026-10-05.parquet").parent.mkdir(parents=True)
    (tmp_path / "kis/H0/dt=2026-10-05.parquet").write_bytes(b"x")
    (tmp_path / "kis/H1/dt=2026-10-02").mkdir(parents=True)
    (tmp_path / "kis/H1/dt=2026-10-02/deep/dt=2000-01-01").mkdir(parents=True)
    (tmp_path / "history/minute/dt=2025-01-01.parquet").parent.mkdir(parents=True)
    (tmp_path / "history/minute/dt=2025-01-01.parquet").write_bytes(b"x")
    (tmp_path / "kis/H1/not-a-date").write_bytes(b"x")

    assert watch.oldest_partition(tmp_path, excluded=frozenset({"history/minute"})) == "2026-10-02"
    assert watch.oldest_partition(tmp_path) == "2025-01-01"
    assert watch.oldest_partition(tmp_path / "missing") is None


def test_prune_backlog_read_only(tmp_path: Path) -> None:
    good = tmp_path / "prune_backlog.json"
    good.write_text(
        json.dumps({"day": "2026-11-19", "l1_partitions_unconfirmed": 7, "oldest_unconfirmed_date": None, "purged": 2}),
        encoding="utf-8",
    )
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{nope", encoding="utf-8")
    wrong = tmp_path / "wrong.json"
    wrong.write_text(json.dumps({"l1_partitions_unconfirmed": "7"}), encoding="utf-8")

    assert watch.read_prune_backlog(good) == 7
    assert watch.read_prune_backlog(tmp_path / "missing.json") is None
    assert watch.read_prune_backlog(corrupt) is None
    assert watch.read_prune_backlog(wrong) is None


def test_main_reports_backlog_and_other_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(watch, "_kst_today", lambda: TODAY.isoformat())
    monkeypatch.setattr(watch, "measure_host", _stub_measure(50.0, oldest={"krx_l1": "2026-10-20"}))
    backlog = tmp_path / "backlog.json"
    backlog.write_text(json.dumps({"l1_partitions_unconfirmed": 3}), encoding="utf-8")
    argv = ["--state-dir", str(tmp_path / "state"), "--home", str(tmp_path), "--backlog-path", str(backlog)]

    watch.main(argv)
    out = capsys.readouterr().out

    assert "prune_backlog=3" in out
    assert "oldest_l1=2026-10-20" in out
    assert "other_gb=" in out
    assert "elapsed_s=" in out
    assert "other_high" in out  # stubbed sample has no tracked parts, so other = used 50 GB

    backlog.unlink()
    watch.main(argv)
    assert "prune_backlog=na" in capsys.readouterr().out
