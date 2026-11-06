"""Hermetic replay guards for the collector ownership fixup, with fake id/find/docker on PATH."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path("deploy/host/krx-ownership-fixup.sh")
TARGET = "1001"

_FAKE_ID = """#!/usr/bin/env bash
case "$1" in
  -u) printf '%s\\n' "$FAKE_UID" ;;
  -g) printf '%s\\n' "$FAKE_GID" ;;
esac
"""

# Each find call consumes one plan character: "P" prints the searched path (foreign entry), "-" prints nothing.
_FAKE_FIND = """#!/usr/bin/env bash
printf '%s\\n' "$*" >>"$FAKE_FIND_LOG"
plan="$(cat "$FAKE_FIND_PLAN" 2>/dev/null || true)"
step="${plan:0:1}"
printf '%s' "${plan:1}" >"$FAKE_FIND_PLAN"
if [ "$step" = "P" ]; then printf '%s\\n' "$1"; fi
exit 0
"""

_FAKE_DOCKER = """#!/usr/bin/env bash
printf '%s\\n' "$*" >>"$FAKE_DOCKER_LOG"
case "$1" in
  stop)
    if [ -n "${FAKE_STOP_MISSING:-}" ]; then echo "Error response from daemon: No such container: $3" >&2; exit 1; fi ;;
esac
exit 0
"""


def _setup(tmp_path: Path, *, plan: str = "", uid: str = TARGET, with_logs: bool = True, stop_missing: bool = False):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("id", _FAKE_ID), ("find", _FAKE_FIND), ("docker", _FAKE_DOCKER)):
        (bin_dir / name).write_text(body, encoding="utf-8")
        (bin_dir / name).chmod(0o755)
    root = tmp_path / "root"
    (root / "data").mkdir(parents=True)
    if with_logs:
        (root / "logs").mkdir()
    cache = tmp_path / "cache"
    cache.mkdir()
    plan_file = tmp_path / "find.plan"
    plan_file.write_text(plan, encoding="utf-8")
    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "KRX_FIXUP_ROOT": str(root),
        "KRX_FIXUP_TOKEN_CACHE": str(cache),
        "FAKE_UID": uid,
        "FAKE_GID": TARGET,
        "FAKE_FIND_PLAN": str(plan_file),
        "FAKE_FIND_LOG": str(tmp_path / "find.log"),
        "FAKE_DOCKER_LOG": str(tmp_path / "docker.log"),
    }
    if stop_missing:
        env["FAKE_STOP_MISSING"] = "1"
    return env, tmp_path / "docker.log", tmp_path / "find.log"


def _run(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv: repo script under fake id/find/docker
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, check=False  # noqa: S607
    )


def _lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def test_clean_tree_is_noop(tmp_path: Path) -> None:
    env, docker_log, _ = _setup(tmp_path, plan="")

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert "krx-ownership-fixup status=clean stopped=0" in result.stdout
    assert _lines(docker_log) == []


def test_foreign_owner_stops_collector_before_reown(tmp_path: Path) -> None:
    env, docker_log, _ = _setup(tmp_path, plan="P")

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert "krx-ownership-fixup status=fixed stopped=1" in result.stdout
    calls = _lines(docker_log)
    stop_index = next(i for i, line in enumerate(calls) if line.startswith("stop -t 95 krx-collector"))
    run_index = next(i for i, line in enumerate(calls) if line.startswith("run ") and "chown -h" in line)
    assert stop_index < run_index


def test_reown_command_is_scoped_to_one_path_without_recursive_chown(tmp_path: Path) -> None:
    env, docker_log, _ = _setup(tmp_path, plan="P")

    result = _run(env)

    assert result.returncode == 0, result.stderr
    runs = [line for line in _lines(docker_log) if line.startswith("run ")]
    assert runs
    for line in runs:
        assert "--pull never" in line
        assert "--network none" in line
        assert "--user 0:0" in line
        assert f"-exec chown -h {TARGET}:{TARGET}" in line
        assert " -R " not in line


def test_residual_foreign_entry_fails_closed(tmp_path: Path) -> None:
    env, _, _ = _setup(tmp_path, plan="PP")

    result = _run(env)

    assert result.returncode != 0
    assert "foreign-owned entries remain" in result.stderr
    assert "status=" not in result.stdout


def test_root_caller_is_refused_before_any_docker_call(tmp_path: Path) -> None:
    env, docker_log, _ = _setup(tmp_path, plan="P", uid="0")

    result = _run(env)

    assert result.returncode != 0
    assert "refusing to run as root" in result.stderr
    assert _lines(docker_log) == []


def test_missing_logs_path_is_skipped_and_other_paths_still_checked(tmp_path: Path) -> None:
    env, docker_log, find_log = _setup(tmp_path, plan="-P", with_logs=False)

    result = _run(env)

    assert result.returncode == 0, result.stderr
    searched = _lines(find_log)
    assert not any("/logs" in line.split()[0] for line in searched)
    assert any(str(Path(env["KRX_FIXUP_TOKEN_CACHE"])) in line for line in searched)
    assert "status=fixed stopped=1" in result.stdout


def test_absent_collector_container_does_not_block_reown(tmp_path: Path) -> None:
    env, docker_log, _ = _setup(tmp_path, plan="P", stop_missing=True)

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert "krx-ownership-fixup status=fixed stopped=0" in result.stdout
