from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_PROBES_DIR = _REPO_ROOT / "ops" / "probes"
_RUN_WINDOW = _PROBES_DIR / "run_window.sh"
_BASH = shutil.which("bash") or "/bin/bash"
_DEPLOY_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "deploy.yml"
_EXPECTED_PLANS = {
    "MORNING": ["p_host.sh", "p_am.sh"],
    "OPEN": ["p_live.sh"],
    "CLOSE": ["p_live.sh"],
    "EOD": ["p_eod.sh", "p_host.sh"],
    "WEEKLY": ["p_week.sh", "p_host.sh"],
}


def _hermetic_env(**extra: str) -> dict[str, str]:
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    env.update(extra)
    return env


def _run_window(window: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv, hermetic probe
        [_BASH, str(_RUN_WINDOW), window],
        capture_output=True,
        text=True,
        env=_hermetic_env(PROBE_DRY_RUN="1"),
        timeout=30,
        check=False,
    )


def test_scripts_parse() -> None:
    scripts = sorted(_PROBES_DIR.glob("*.sh"))
    assert {p.name for p in scripts} >= {"run_window.sh", *(n for ns in _EXPECTED_PLANS.values() for n in ns)}
    for script in scripts:
        result = subprocess.run(  # noqa: S603 - fixed argv, hermetic probe
            [_BASH, "-n", str(script)], capture_output=True, text=True, check=False
        )
        assert result.returncode == 0, f"{script.name}: {result.stderr}"


def test_run_window_unknown_window_exits_2() -> None:
    result = _run_window("NOPE")
    assert result.returncode == 2


def test_run_window_without_argument_exits_2() -> None:
    result = subprocess.run(  # noqa: S603 - fixed argv, hermetic probe
        [_BASH, str(_RUN_WINDOW)],
        capture_output=True,
        text=True,
        env=_hermetic_env(PROBE_DRY_RUN="1"),
        check=False,
    )
    assert result.returncode == 2


@pytest.mark.parametrize("window", sorted(_EXPECTED_PLANS))
def test_run_window_plan_lists_expected_scripts(window: str) -> None:
    result = _run_window(window)
    assert result.returncode == 0
    planned = [
        line.split("script=")[1].split()[0]
        for line in result.stdout.splitlines()
        if line.startswith("krx PROBE_PLAN ")
    ]
    assert planned == [name.removesuffix(".sh") for name in _EXPECTED_PLANS[window]]


def test_deploy_ships_probes() -> None:
    workflow = _DEPLOY_WORKFLOW.read_text(encoding="utf-8")
    assert "ops/probes/run_window.sh" in workflow
    assert 'install -m 755 "\\$STAGING_DIR/\\$probe"' in workflow
    assert "~/krx-alpha/ops/probes/" in workflow


def test_p_eod_has_11_23_fixes() -> None:
    body = (_PROBES_DIR / "p_eod.sh").read_text(encoding="utf-8")
    assert "groups.get(g)" in body
    assert 'kind == "ranking"' in body
    assert "judge polls" in body
    assert 'if vendor == "db": print(f"Q1 SKIP(db_format)' in body


def _eod_fixture(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    home = tmp_path / "home"
    data = home / "krx-alpha" / "data"
    for directory in ("calendar", "logs", "l1/kis/nxt/regular/H0STCNT0"):
        (data / directory).mkdir(parents=True, exist_ok=True)
    for day in ("2026-11-19", "2026-11-20", "2026-11-24", "2026-11-25"):
        (data / "calendar" / f"dt={day}.json").write_text("{}", encoding="utf-8")
    (data / "logs" / "events-daemon.jsonl").write_text("", encoding="utf-8")
    (data / "l1/kis/nxt/regular/H0STCNT0/dt=2026-11-19.parquet").touch()
    bindir = tmp_path / "bin"
    bindir.mkdir()
    rclone = home / ".local/bin/rclone"
    rclone.parent.mkdir(parents=True)
    rclone.write_text(
        '#!/bin/sh\nif [ "$1" = lsf ] && [ "${RCLONE_FAIL:-0}" = 1 ]; then echo partial; exit 7; fi\nexit 0\n',
        encoding="utf-8",
    )
    rclone.chmod(0o755)
    docker = bindir / "docker"
    docker.write_text(
        '#!/bin/sh\ncase "$1" in\n'
        'logs) [ -n "${LOG_TEXT:-}" ] && printf \'%s\\n\' "$LOG_TEXT"; exit "${LOG_RC:-0}" ;;\n'
        'exec) echo \'{"day":"2026-11-24","sent_today":0,"keys":[]}\' ;;\n'
        'run) echo "Q3 PASS first"; seq 1 70; exit "${PQ_RC:-0}" ;;\n'
        'esac\n',
        encoding="utf-8",
    )
    docker.chmod(0o755)
    zstdcat = bindir / "zstdcat"
    zstdcat.write_text('#!/bin/sh\ncat "$1"\nexit "${ZSTD_RC:-0}"\n', encoding="utf-8")
    zstdcat.chmod(0o755)
    env = _hermetic_env(HOME=str(home), PATH=f"{bindir}:/usr/bin:/bin", D="2026-11-24", PQRUN="0")
    return data, env


def _run_eod(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv, hermetic probe
        [_BASH, str(_PROBES_DIR / "p_eod.sh")],
        capture_output=True, text=True, env=env, timeout=30, check=False,
    )


def _record(sequence: object, conn_id: object = "a") -> str:
    import json

    record = {
        "raw": "000001^090000^100",
        "conn_seq": sequence,
        "recv_wall_ns": 1763942400000000000,
    }
    if conn_id is not None:
        record["conn_id"] = conn_id
    return json.dumps(record)


def _journal(data: Path, stream: str, hour: str, records: list[str]) -> None:
    directory = data / f"l0/kis/krx/regular/{stream}/dt=2026-11-24"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{hour}.zst").write_text("\n".join(records) + "\n", encoding="utf-8")


def test_sequence_bitmap_matches_exact_membership_at_cap() -> None:
    import ast

    source = (_PROBES_DIR / "p_eod.sh").read_text(encoding="utf-8").split("PY=$(cat <<'EOF'\n", 1)[1].split("\nEOF", 1)[0]
    tree = ast.parse(source)
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name in {"_SequenceBitmap", "_SequenceBitmapLimitError"}]
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=classes, type_ignores=[]), "<bitmap>", "exec"), namespace)  # noqa: S102 - repository-owned code
    bitmap_type = namespace["_SequenceBitmap"]
    limit_error = namespace["_SequenceBitmapLimitError"]
    tracker = bitmap_type(1)  # type: ignore[operator]
    seen: set[tuple[str, int]] = set()
    for number in (0, 4095, 1, 4095, 0, 7):
        assert tracker.test_and_set("a", number) == (("a", number) in seen)
        seen.add(("a", number))
    with pytest.raises(limit_error):  # type: ignore[arg-type]
        tracker.test_and_set("a", 4096)
    assert tracker.test_and_set("a", 0)
    tracker = bitmap_type(3)  # type: ignore[operator]
    seen.clear()
    for number in (-1, -(10**15), 10**15, -1):
        assert tracker.test_and_set("a", number) == (("a", number) in seen)
        seen.add(("a", number))


def test_eod_q1_counts_cross_file_duplicates_and_regressions(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    for stream in ("H0STCNT0", "H0STCNT1"):
        _journal(data, stream, "09", [_record(1), _record(2), _record(2)])
        _journal(data, stream, "10", [_record(1), _record(1, "b")])
    result = _run_eod(env)
    assert result.returncode == 0, result.stderr
    rows = [line for line in result.stdout.splitlines() if line.startswith("Q1 ")]
    assert len(rows) == 2
    assert all("rec=5" in line and "dup_seq=2" in line and "seq_regress=1" in line for line in rows)


@pytest.mark.parametrize("record", [_record(1, None), _record(True)])
def test_eod_q1_fails_on_invalid_sequence_key(tmp_path: Path, record: str) -> None:
    data, env = _eod_fixture(tmp_path)
    _journal(data, "H0STCNT0", "09", [record])
    result = _run_eod(env)
    assert result.returncode == 1
    assert "reason=invalid_sequence_key" in result.stdout
    assert "C6 INFO" not in result.stdout


def test_eod_q1_fails_on_partial_decompression(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _journal(data, "H0STCNT0", "09", [_record(1)])
    env["ZSTD_RC"] = "1"
    result = _run_eod(env)
    assert result.returncode == 1
    assert "reason=zstd_exit rc=1" in result.stdout
    assert "C6 INFO" not in result.stdout


def test_eod_keeps_q2_fail_when_pq_execution_fails(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    manifests = data / "manifest" / "regular_kis"
    manifests.mkdir(parents=True)
    (manifests / "2026-11-20.nxt.shard-00.json").write_text("{}", encoding="utf-8")
    env.update(PQRUN="1", PQ_RC="137", L0C="0")
    result = _run_eod(env)
    assert result.returncode == 137, result.stderr
    assert "Q2 FAIL" in result.stdout
    assert "Q3 PASS first" in result.stdout


def test_eod_preserves_host_failure_when_pq_succeeds(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    (data / "logs/events-daemon.jsonl").write_text('{"fields": null}\n', encoding="utf-8")
    env["PQRUN"] = "1"
    result = _run_eod(env)
    assert result.returncode == 1
    assert "Q3 PASS first" in result.stdout


def test_eod_distinguishes_empty_log_matches_from_docker_failure(tmp_path: Path) -> None:
    _, env = _eod_fixture(tmp_path)
    assert _run_eod(env).returncode == 0
    env["LOG_RC"] = "42"
    assert _run_eod(env).returncode == 42


def test_eod_rejects_incomplete_b5_listing(tmp_path: Path) -> None:
    _, env = _eod_fixture(tmp_path)
    env["RCLONE_FAIL"] = "1"
    result = _run_eod(env)
    assert result.returncode != 0
    assert "B5 PASS" not in result.stdout


def test_eod_missing_data_root_fails(tmp_path: Path) -> None:
    result = _run_eod(_hermetic_env(HOME=str(tmp_path), PATH="/usr/bin:/bin"))
    assert result.returncode == 1
    assert "reason=no_data_root" in result.stdout


def test_eod_wrapper_retains_timeout_and_runs_host_probe(tmp_path: Path) -> None:
    home = tmp_path / "home"
    (home / "krx-alpha").mkdir(parents=True)
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copy(_RUN_WINDOW, scripts / "run_window.sh")
    for name in ("p_eod.sh", "p_host.sh"):
        (scripts / name).write_text("#!/bin/sh\necho host-pass\n", encoding="utf-8")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    timeout = bindir / "timeout"
    timeout.write_text('#!/bin/sh\ncase "$3" in *p_eod.sh) echo partial; exit 124 ;; *) shift; exec "$@" ;; esac\n', encoding="utf-8")
    timeout.chmod(0o755)
    result = subprocess.run(  # noqa: S603 - fixed argv, hermetic probe
        [_BASH, str(scripts / "run_window.sh"), "EOD"],
        env=_hermetic_env(HOME=str(home), PATH=f"{bindir}:/usr/bin:/bin"),
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode == 124
    assert "krx partial" in result.stdout
    assert "script=p_eod rc=124" in result.stdout
    assert "script=p_host rc=0" in result.stdout


def _calendar(data: Path, day: str, payload: dict[str, object]) -> None:
    import json

    (data / "calendar" / f"dt={day}.json").write_text(json.dumps(payload), encoding="utf-8")


def _l1(data: Path, group: str, stream: str, day: str) -> None:
    directory = data / "l1" / group / stream
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"dt={day}.parquet").touch()


def test_q2_ignores_unplanned_day(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-18", {})
    _l1(data, "kis/nxt/regular", "H0NXCNT0", "2026-11-18")
    _l1(data, "kis/nxt/regular", "H0NXCNT0", "2026-11-20")
    result = _run_eod(env)
    assert result.returncode == 0, result.stderr
    assert "Q2 PASS" in result.stdout


def test_q2_fails_planned_day_without_l1(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-18", {})
    _l1(data, "kis/nxt/regular", "H0NXCNT0", "2026-11-18")
    _l1(data, "kis/nxt/regular", "H0NXCNT0", "2026-11-20")
    manifests = data / "manifest" / "regular_kis"
    manifests.mkdir(parents=True)
    (manifests / "2026-11-19.nxt.shard-00.json").write_text("{}", encoding="utf-8")
    result = _run_eod(env)
    assert "Q2 FAIL" in result.stdout
    assert "kis/nxt/regular/H0NXCNT0@2026-11-19" in result.stdout


def test_q2_unmapped_group_keeps_calendar_rule(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-18", {})
    _l1(data, "db/krx/regular", "H0STCNT0", "2026-11-18")
    _l1(data, "db/krx/regular", "H0STCNT0", "2026-11-20")
    result = _run_eod(env)
    assert "Q2 FAIL" in result.stdout
    assert "db/krx/regular/H0STCNT0@2026-11-19" in result.stdout


def test_c3_no_premarket_day_skips(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-24", {"regular_open": "09:00:00", "nxt_premarket": None})
    result = _run_eod(env)
    assert "C3 SKIP(no_premarket) group=kis/nxt/nxt_pre" in result.stdout


def test_c3_shifted_day_nxt_regular_missing_fails_once(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-24", {"regular_open": "10:00:00", "nxt_premarket": ["09:00:00", "09:50:00"]})
    result = _run_eod(env)
    assert result.stdout.count("C3 FAIL group=kis/nxt/regular manifest=MISSING") == 1


def test_c3_legacy_shifted_calendar_skips(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-24", {"regular_open": "10:00:00"})
    result = _run_eod(env)
    assert "C3 SKIP(no_premarket) group=kis/nxt/nxt_pre" in result.stdout
    assert "C3 SKIP(shifted_legacy) group=kis/nxt/regular" in result.stdout


def test_r8_no_premarket_next_day_skips(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _calendar(data, "2026-11-25", {"nxt_premarket": None})
    result = _run_eod(env)
    assert "R8 SKIP(no_premarket_next_day) pool=2026-11-25" in result.stdout


_SNAPSHOT_SUMMARY = "2026-11-24T08:00:00.000+09:00 stage=snapshot_summary executed=5 expired=0 failed_jobs=0"


def _snapshot_dirs(data: Path, dirs: tuple[str, ...]) -> None:
    for name in dirs:
        (data / "l1" / "snapshot" / name).mkdir(parents=True, exist_ok=True)


def test_c5_investor_estimate_covered_by_market_snapshot(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _snapshot_dirs(data, ("investor_estimate", "investor_estimate_market", "ohlcv"))
    for name in ("investor_estimate_market", "ohlcv"):
        (data / "l1" / "snapshot" / name / "dt=2026-11-24.parquet").touch()
    env["LOG_TEXT"] = _SNAPSHOT_SUMMARY
    result = _run_eod(env)
    c5 = [line for line in result.stdout.splitlines() if line.startswith("C5 ")]
    assert len(c5) == 1
    assert c5[0].startswith("C5 PASS ")
    assert "missing=-" in c5[0]


def test_c5_investor_estimate_without_any_snapshot_fails(tmp_path: Path) -> None:
    data, env = _eod_fixture(tmp_path)
    _snapshot_dirs(data, ("investor_estimate", "ohlcv"))
    (data / "l1" / "snapshot" / "ohlcv" / "dt=2026-11-24.parquet").touch()
    env["LOG_TEXT"] = _SNAPSHOT_SUMMARY
    result = _run_eod(env)
    assert "C5 FAIL" in result.stdout
    assert "missing=['investor_estimate']" in result.stdout


def _host_env(tmp_path: Path, user_failed: str) -> dict[str, str]:
    home = tmp_path / "home"
    home.mkdir()
    bindir = tmp_path / "bin"
    bindir.mkdir()
    systemctl = bindir / "systemctl"
    systemctl.write_text(
        '#!/bin/sh\n'
        'if [ "$1" = --user ] && [ "$2" = --failed ] && [ -n "${USER_FAILED:-}" ]; then printf \'%s\\n\' $USER_FAILED; fi\n'
        'exit 0\n',
        encoding="utf-8",
    )
    systemctl.chmod(0o755)
    for name in ("chronyc", "docker", "journalctl"):
        stub = bindir / name
        stub.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        stub.chmod(0o755)
    return _hermetic_env(HOME=str(home), PATH=f"{bindir}:/usr/bin:/bin", USER_FAILED=user_failed)


def _run_host(env: dict[str, str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv, hermetic probe
        [_BASH, str(_PROBES_DIR / "p_host.sh")],
        capture_output=True, text=True, env=env, cwd=cwd, timeout=30, check=False,
    )


def _x3_line(stdout: str) -> str:
    return next(line for line in stdout.splitlines() if line.startswith("X3 "))


def test_x3_ignores_quant_ops_probe_unit_failure(tmp_path: Path) -> None:
    env = _host_env(tmp_path, "quant-ops-probe@EOD.service")
    x3 = _x3_line(_run_host(env, tmp_path).stdout)
    assert x3.startswith("X3 PASS ")
    assert "user_failed=-" in x3


def test_x3_warns_only_on_non_probe_unit_failure(tmp_path: Path) -> None:
    env = _host_env(tmp_path, "quant-ops-probe@EOD.service foo.service")
    x3 = _x3_line(_run_host(env, tmp_path).stdout)
    assert x3.startswith("X3 WARN ")
    assert "user_failed=foo.service," in x3
    assert "quant-ops-probe" not in x3
