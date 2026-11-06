"""Hermetic guards for the CI ops-key forced command and deploy SSH hardening."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

GATE = Path("deploy/host/ci-ops-gate.sh")
WORKFLOW = Path(".github/workflows/deploy.yml")
INBOX = Path("k-stock-engine/data/state/kr_swing_2019_v1/kind_relay/inbox")


def _gate(home: Path, command: str | None, stdin: bytes = b"") -> subprocess.CompletedProcess[bytes]:
    env = {"PATH": os.environ["PATH"], "HOME": str(home)}
    if command is not None:
        env["SSH_ORIGINAL_COMMAND"] = command
    return subprocess.run(  # noqa: S603 - fixed argv: repo script in a temp HOME
        ["bash", str(GATE)], env=env, input=stdin, capture_output=True, check=False  # noqa: S607
    )


def test_gate_refuses_anything_outside_the_allowlist(tmp_path: Path) -> None:
    for command in (None, "", "bash", "kca-watchdog-probe; id", "cat ~/.ssh/authorized_keys", "kse-kind-relay-put x"):
        result = _gate(tmp_path, command)

        assert result.returncode == 126, command
        assert b"command not allowed" in result.stderr


def test_gate_delivers_kind_bundle_atomically(tmp_path: Path) -> None:
    payload = b'{"bundle": 1}'

    result = _gate(tmp_path, "kse-kind-relay-put", payload)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / INBOX / "bundle.json").read_bytes() == payload
    assert not (tmp_path / INBOX / "bundle.json.incoming").exists()


def test_gate_rejects_empty_bundle_and_keeps_previous(tmp_path: Path) -> None:
    (tmp_path / INBOX).mkdir(parents=True)
    (tmp_path / INBOX / "bundle.json").write_bytes(b"previous")

    result = _gate(tmp_path, "kse-kind-relay-put", b"")

    assert result.returncode == 65
    assert (tmp_path / INBOX / "bundle.json").read_bytes() == b"previous"
    assert not (tmp_path / INBOX / "bundle.json.incoming").exists()


def test_deploy_job_requires_approved_environment_and_pinned_host_key() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    deploy = workflow["jobs"]["deploy"]
    script = "\n".join(step.get("run", "") for step in deploy["steps"])

    assert deploy["environment"] == "production"
    assert "StrictHostKeyChecking=no" not in script
    assert "UserKnownHostsFile=/dev/null" not in script
    assert "StrictHostKeyChecking=yes" in script


def test_every_action_is_pinned_to_a_commit_sha() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    uses = [step["uses"] for job in workflow["jobs"].values() for step in job["steps"] if "uses" in step]

    assert uses
    for ref in uses:
        _, _, version = ref.partition("@")
        assert len(version) == 40, ref
        assert set(version) <= set("0123456789abcdef"), ref


def test_deploy_installs_the_ops_gate_from_staging() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert 'deploy/host/ci-ops-gate.sh "$REMOTE_USER@$HOST:~/krx-alpha/$STAGING_DIR/ci-ops-gate.sh"' in text
    assert 'install -m 755 "\\$STAGING_DIR/ci-ops-gate.sh" ~/bin/ci-ops-gate.new' in text
