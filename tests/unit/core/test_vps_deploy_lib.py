"""Hermetic bash-replay guards for the shared VPS deploy library."""

from __future__ import annotations

import fcntl
import os
import stat
import subprocess
from pathlib import Path

SHA = "abcdef1234567890abcdef1234567890abcdef12"
SHA12 = SHA[:12]

_FAKE_DOCKER = """#!/usr/bin/env python3
import os
import sys

with open(os.environ["FAKE_DOCKER_LOG"], "a") as handle:
    handle.write("docker " + " ".join(sys.argv[1:]) + "\\n")

args = sys.argv[1:]
state = os.environ.get("FAKE_STATE_DIR", "")


def _bump(name: str) -> int:
    path = os.path.join(state, name + ".cnt") if state else ""
    count = 0
    if path and os.path.exists(path):
        with open(path) as handle:
            count = int((handle.read().strip() or "0"))
    count += 1
    if path:
        with open(path, "w") as handle:
            handle.write(str(count))
    return count


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


command = args[0] if args else ""
if command == "pull":
    seen = _bump("pull")
    fail_until = int(_get("FAKE_PULL_FAIL_UNTIL", "0"))
    if _get("FAKE_PULL_FAIL_ALWAYS", "0") == "1" or seen <= fail_until:
        print("pull failed", file=sys.stderr)
        sys.exit(1)
    sys.exit(0)
if command == "image" and len(args) > 1 and args[1] == "inspect":
    print(_get("FAKE_LABEL", ""))
    sys.exit(0)
if command == "tag":
    sys.exit(0)
if command == "ps":
    if "--format" in args:
        print(_get("FAKE_PS_NAMES", "c1"))
    else:
        print("CONTAINER ID   IMAGE   COMMAND   CREATED   STATUS   NAMES")
        print("deadbeef   img   run   now   Up 1 min   c1")
    sys.exit(0)
if command == "inspect":
    if "--format" not in args:
        print("SECRET_SENTINEL unformatted")
        sys.exit(0)
    seen = _bump("inspect")
    seq = [part for part in _get("FAKE_RESTART_SEQ", "0").split(",") if part != ""]
    restarts = seq[min(seen - 1, len(seq) - 1)] if seq else "0"
    status = _get("FAKE_STATUS", "running")
    oom = _get("FAKE_OOM", "false")
    print(f"{status}|0|{oom}||2026-09-30T00:00:00Z||{restarts}")
    sys.exit(0)
if command == "logs":
    print("SECRET_SENTINEL log line")
    sys.exit(0)
if command == "system":
    print("TYPE   TOTAL   ACTIVE   SIZE")
    sys.exit(0)
sys.exit(0)
"""

_FAKE_JOURNALCTL = """#!/bin/bash
echo "journal line"
exit ${FAKE_JOURNALCTL_RC:-0}
"""


def _install_fakes(tmp_path: Path, monkey_env: dict[str, str]) -> dict[str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    state_dir = tmp_path / "fake-state"
    state_dir.mkdir(parents=True, exist_ok=True)
    log = tmp_path / "docker.log"
    log.write_text("", encoding="utf-8")
    docker = bin_dir / "docker"
    docker.write_text(_FAKE_DOCKER, encoding="utf-8")
    docker.chmod(0o755)
    journalctl = bin_dir / "journalctl"
    journalctl.write_text(_FAKE_JOURNALCTL, encoding="utf-8")
    journalctl.chmod(0o755)
    env = dict(os.environ)
    env.update(
        {
            "HOME": str(tmp_path),
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "FAKE_DOCKER_LOG": str(log),
            "FAKE_STATE_DIR": str(state_dir),
            "VPS_STATE_ROOT": str(tmp_path / "state"),
            "FAKE_LABEL": SHA,
            "FAKE_PS_NAMES": "c1",
        }
    )
    env.update(monkey_env)
    return env


def _replay(script: str, env: dict[str, str], timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - hermetic bash replay without shell
        ["bash", "-c", script],  # noqa: S607 - PATH-resolved bash for hermetic replay
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _pull_count(tmp_path: Path) -> int:
    lines = (tmp_path / "docker.log").read_text(encoding="utf-8").splitlines()
    return sum(1 for line in lines if line.startswith("docker pull"))


def _error_lines(stdout: str) -> list[str]:
    return [line for line in stdout.splitlines() if "::error title=vps-deploy/" in line]


def test_pull_retries_then_succeeds(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"FAKE_PULL_FAIL_UNTIL": "2", "VPS_PULL_RETRY_DELAYS": "0 0"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage deploy\n"
        f"vps_pull_verified ghcr.io/example/repo {SHA}\n",
        env,
    )

    assert proc.returncode == 0
    assert _pull_count(tmp_path) == 3


def test_pull_exhausts_retries_with_self_describing_annotation(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"FAKE_PULL_FAIL_ALWAYS": "1", "VPS_PULL_RETRY_DELAYS": "0 0"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage deploy\n"
        f"vps_pull_verified ghcr.io/example/repo {SHA}\n",
        env,
    )
    errors = _error_lines(proc.stdout)

    assert proc.returncode != 0
    assert len(errors) == 1
    assert "rc=" in errors[0]
    assert f"sha={SHA12}" in errors[0]
    assert "evidence=" in errors[0]


def test_revision_mismatch_fails_closed(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"FAKE_LABEL": "0" * 40, "VPS_PULL_RETRY_DELAYS": "0 0"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage deploy\n"
        f"vps_pull_verified ghcr.io/example/repo {SHA}\n",
        env,
    )
    recorded = (tmp_path / "docker.log").read_text(encoding="utf-8")

    assert proc.returncode != 0
    assert "reason=revision_mismatch" in proc.stdout
    assert not any(line.startswith("docker tag") for line in recorded.splitlines())


def test_shared_lock_does_not_block_second_deploy(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"VPS_PULL_RETRY_DELAYS": "0 0"})
    lock = tmp_path / "state" / "image.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.touch()
    handle = lock.open("a")
    fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
    try:
        proc = _replay(
            f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage deploy\n"
            f"vps_pull_verified ghcr.io/example/repo {SHA}\n",
            env,
            timeout=30,
        )
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()

    assert proc.returncode == 0


def test_exclusive_gc_lock_blocks_deploy_until_timeout(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"VPS_IMAGE_LOCK_WAIT_S": "1", "VPS_PULL_RETRY_DELAYS": "0 0"})
    lock = tmp_path / "state" / "image.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.touch()
    handle = lock.open("a")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    try:
        proc = _replay(
            f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage deploy\n"
            f"vps_pull_verified ghcr.io/example/repo {SHA}\n",
            env,
            timeout=30,
        )
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()

    assert proc.returncode != 0
    assert "reason=image_lock_timeout" in proc.stdout


def test_stability_detects_restart_loop(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"FAKE_RESTART_SEQ": "0,1", "VPS_STABLE_POLL_S": "0"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage serve\n"
        "vps_verify_stable 2 c1\n",
        env,
        timeout=30,
    )

    assert proc.returncode != 0
    assert "reason=unstable" in proc.stdout


def test_stability_detects_oom_kill(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"FAKE_OOM": "true", "VPS_STABLE_POLL_S": "0"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage serve\n"
        "vps_verify_stable 1 c1\n",
        env,
        timeout=30,
    )

    assert proc.returncode != 0


def test_stable_container_passes(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"VPS_STABLE_POLL_S": "1"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage serve\n"
        "vps_verify_stable 1 c1\n",
        env,
        timeout=30,
    )

    assert proc.returncode == 0


def test_evidence_never_leaks_env_or_logs_to_stdout(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage serve\n"
        "vps_fail boom\n",
        env,
    )
    evidence = list((tmp_path / "state" / "evidence" / "proj").glob("*.log"))
    recorded = (tmp_path / "docker.log").read_text(encoding="utf-8").splitlines()

    assert proc.returncode != 0
    assert "SECRET_SENTINEL" not in proc.stdout
    assert len(evidence) == 1
    assert "SECRET_SENTINEL" in evidence[0].read_text(encoding="utf-8")
    assert stat.S_IMODE(evidence[0].stat().st_mode) == 0o600
    assert not [line for line in recorded if "inspect" in line and "--format" not in line]


def test_original_rc_preserved_when_evidence_collection_fails(tmp_path: Path) -> None:
    env = _install_fakes(tmp_path, {"FAKE_JOURNALCTL_RC": "1"})
    proc = _replay(
        f"source deploy/host/vps-deploy-lib.sh\nvps_deploy_init proj {SHA}\nvps_stage serve\n"
        "exit 7\n",
        env,
    )

    assert proc.returncode == 7


def test_library_never_prunes() -> None:
    text = Path("deploy/host/vps-deploy-lib.sh").read_text(encoding="utf-8")

    assert "image prune" not in text
    assert "rmi" not in text
