"""Hermetic replay guards for the 22:00 KST deferred-recreate verifier."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path("deploy/host/krx-deferred-verify.sh")
LIB = Path("deploy/host/vps-deploy-lib.sh")
SHA = "abcdef1234567890abcdef1234567890abcdef12"

_FAKE_DOCKER = """#!/usr/bin/env bash
printf 'docker %s\\n' "$*" >>"$FAKE_DOCKER_LOG"
case "$1" in
  image) printf '%s\\n' "$FAKE_IMAGE_REVISION" ;;
  inspect)
    if [[ "$*" == *revision* ]]; then printf '%s\\n' "$FAKE_RUNNING_REVISION"
    else printf '%s|0|false||2026-09-30T13:00:00Z||0\\n' "$FAKE_STATUS"; fi ;;
  ps) [[ "$*" == *--format* ]] && echo krx-collector ;;
esac
exit 0
"""


def _run(tmp_path: Path, *, image_rev: str, running_rev: str, status: str = "running") -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("docker", _FAKE_DOCKER), ("journalctl", "#!/bin/sh\nexit 0\n")):
        (bin_dir / name).write_text(body, encoding="utf-8")
        (bin_dir / name).chmod(0o755)
    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "VPS_DEPLOY_LIB": str(LIB.resolve()),
        "KRX_DEFERRED_STABLE_WINDOW_S": "1",
        "VPS_STABLE_POLL_S": "1",
        "FAKE_DOCKER_LOG": str(tmp_path / "docker.log"),
        "FAKE_IMAGE_REVISION": image_rev,
        "FAKE_RUNNING_REVISION": running_rev,
        "FAKE_STATUS": status,
    }
    return subprocess.run(  # noqa: S603 - fixed argv: repo script under a fake docker
        ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, check=False  # noqa: S607
    )


def test_deferred_verify_passes_when_running_revision_matches_latest(tmp_path: Path) -> None:
    result = _run(tmp_path, image_rev=SHA, running_rev=SHA)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "check=stable" in result.stdout
    assert "::error" not in result.stdout


def test_deferred_verify_fails_closed_on_stale_running_revision(tmp_path: Path) -> None:
    result = _run(tmp_path, image_rev=SHA, running_rev="0" * 40)

    assert result.returncode != 0
    assert "::error title=vps-deploy/krx-alpha/deferred_verify::" in result.stdout
    assert "reason=running_revision_mismatch" in result.stdout


def test_deferred_verify_fails_when_collector_not_running(tmp_path: Path) -> None:
    result = _run(tmp_path, image_rev=SHA, running_rev=SHA, status="restarting")

    assert result.returncode != 0
    assert "reason=unstable" in result.stdout


def test_deferred_service_runs_verifier_after_recreate() -> None:
    service = Path("deploy/host/krx-deferred-recreate.service").read_text(encoding="utf-8")
    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert "ExecStartPost=/bin/bash %h/krx-alpha/deploy/host/krx-deferred-verify.sh" in service
    assert service.index("ExecStart=") < service.index("ExecStartPost=")
    assert "deploy/host/krx-deferred-verify.sh" in workflow
    assert 'install -m 755 "\\$STAGING_DIR/krx-deferred-verify.sh"' in workflow
