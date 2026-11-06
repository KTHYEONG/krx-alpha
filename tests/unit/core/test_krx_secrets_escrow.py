"""Hermetic replay guards for the secrets escrow script (fake rclone, generated age identities, tmp dirs)."""

from __future__ import annotations

import datetime as dt
import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path("deploy/host/krx-secrets-escrow.sh")
SECRET_VALUES = (b"alpha-secret-111", b"beta-secret-222")

_FAKE_RCLONE = """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_RCLONE_LOG"
cmd="$1"
shift
case "$cmd" in
  copyto)
    if [ "${FAKE_COPY_RC:-0}" != 0 ]; then exit "$FAKE_COPY_RC"; fi
    src="$1"
    printf '%s\\n' "$(dirname "$src")" >> "$FAKE_SRC_DIR_LOG"
    ls -A "$(dirname "$src")" >> "$FAKE_SRC_LIST_LOG"
    cp "$src" "$FAKE_REMOTE_DIR/${2##*/}"
    ;;
  lsf)
    find "$FAKE_REMOTE_DIR" -mindepth 1 -maxdepth 1 -type f -printf '%f\\n'
    ;;
  deletefile)
    rm -f -- "$FAKE_REMOTE_DIR/${1##*/}"
    ;;
  *)
    exit 9
    ;;
esac
"""


def _find_on_path(tool: str) -> str | None:
    # shutil.which is monkeypatched to None by the hermetic conftest, so resolve PATH directly.
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(directory) / tool
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _require(tool: str) -> str:
    found = _find_on_path(tool)
    if found is None:
        pytest.skip(f"{tool} not on PATH")
    return found


class _Harness:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.src = root / "quant-secrets"
        self.src.mkdir()
        self.home = root / "home"
        self.home.mkdir()
        self.tmpdir = root / "tmp"
        self.tmpdir.mkdir()
        self.remote = root / "remote"
        self.remote.mkdir()
        self.recipients = root / "recipients.txt"
        self.state = root / "state" / "secrets_escrow.json"
        self.rclone_log = root / "rclone.log"
        self.src_dir_log = root / "src_dir.log"
        self.src_list_log = root / "src_list.log"
        self.fake = root / "fake-rclone.sh"
        self.fake.write_text(_FAKE_RCLONE, encoding="utf-8")
        self.fake.chmod(self.fake.stat().st_mode | stat.S_IEXEC)

    def env(self, **overrides: str) -> dict[str, str]:
        env = {
            "PATH": os.environ["PATH"],
            "HOME": str(self.home),
            "TMPDIR": str(self.tmpdir),
            "ESCROW_SRC_DIR": str(self.src),
            "ESCROW_RECIPIENTS": str(self.recipients),
            "ESCROW_STATE": str(self.state),
            "ESCROW_REMOTE": "gdrive:test-escrow",
            "ESCROW_KEEP": "12",
            "ESCROW_MAX_AGE_DAYS": "30",
            "RCLONE_BIN": str(self.fake),
            "AGE_BIN": _find_on_path("age") or "age",
            "FAKE_RCLONE_LOG": str(self.rclone_log),
            "FAKE_SRC_DIR_LOG": str(self.src_dir_log),
            "FAKE_SRC_LIST_LOG": str(self.src_list_log),
            "FAKE_REMOTE_DIR": str(self.remote),
        }
        env.update(overrides)
        return env

    def run(self, **overrides: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - hermetic bash harness with fixed argv
            ["bash", str(SCRIPT)],  # noqa: S607
            env=self.env(**overrides),
            capture_output=True,
            text=True,
            check=False,
        )

    def write_sources(self) -> None:
        (self.src / "kis-data.env").write_text("KIS_APP_KEY=alpha-secret-111\n", encoding="utf-8")
        (self.src / "kiwoom.env").write_text("KIWOOM_TOKEN=beta-secret-222\n", encoding="utf-8")

    def make_identity(self) -> Path:
        keygen = _require("age-keygen")
        identity = self.root / "identity.txt"
        subprocess.run([keygen, "-o", str(identity)], check=True, capture_output=True)  # noqa: S603 - fixed argv
        public = subprocess.run(  # noqa: S603 - fixed argv
            [keygen, "-y", str(identity)], check=True, capture_output=True, text=True
        ).stdout
        self.recipients.write_text(public, encoding="utf-8")
        return identity

    def remote_names(self) -> list[str]:
        return sorted(p.name for p in self.remote.iterdir())

    def copy_calls(self) -> list[str]:
        if not self.rclone_log.exists():
            return []
        return [line for line in self.rclone_log.read_text(encoding="utf-8").splitlines() if line.startswith("copyto ")]


@pytest.fixture
def harness(tmp_path: Path) -> _Harness:
    return _Harness(tmp_path)


def test_not_configured_is_noop(harness: _Harness) -> None:
    harness.write_sources()

    result = harness.run()

    assert result.returncode == 0
    assert "status=not_configured" in result.stdout
    assert not harness.rclone_log.exists()


def test_first_run_uploads_encrypted(harness: _Harness) -> None:
    _require("age")
    harness.write_sources()
    harness.make_identity()

    result = harness.run()

    assert result.returncode == 0, result.stderr
    assert "status=uploaded" in result.stdout
    (name,) = harness.remote_names()
    assert name.startswith("secrets-")
    assert name.endswith(".tar.age")
    uploaded = (harness.remote / name).read_bytes()
    for secret in SECRET_VALUES:
        assert secret not in uploaded
    state = json.loads(harness.state.read_text(encoding="utf-8"))
    assert state["object"] == name
    assert len(state["fingerprint"]) == 64
    assert len(harness.copy_calls()) == 1


def test_round_trip_restores_exact_files(harness: _Harness) -> None:
    age = _require("age")
    harness.write_sources()
    identity = harness.make_identity()
    assert harness.run().returncode == 0
    (name,) = harness.remote_names()
    restore = harness.root / "restore"
    restore.mkdir()

    decrypted = subprocess.run(  # noqa: S603 - fixed argv
        [age, "-d", "-i", str(identity), str(harness.remote / name)], check=True, capture_output=True
    )
    subprocess.run(["tar", "-x", "-C", str(restore)], input=decrypted.stdout, check=True)  # noqa: S603, S607

    for env_name in ("kis-data.env", "kiwoom.env"):
        assert (restore / "quant-secrets" / env_name).read_bytes() == (harness.src / env_name).read_bytes()


def test_unchanged_skips(harness: _Harness) -> None:
    _require("age")
    harness.write_sources()
    harness.make_identity()
    assert harness.run().returncode == 0
    copies_before = harness.copy_calls()

    result = harness.run()

    assert result.returncode == 0
    assert "status=unchanged" in result.stdout
    assert harness.copy_calls() == copies_before


def test_stale_reuploads(harness: _Harness) -> None:
    _require("age")
    harness.write_sources()
    harness.make_identity()
    assert harness.run().returncode == 0
    state = json.loads(harness.state.read_text(encoding="utf-8"))
    stale = dt.datetime.now(dt.UTC) - dt.timedelta(days=31)
    state["uploaded_at"] = stale.isoformat(timespec="seconds")
    harness.state.write_text(json.dumps(state), encoding="utf-8")
    copies_before = len(harness.copy_calls())

    result = harness.run()

    assert result.returncode == 0
    assert "status=uploaded" in result.stdout
    assert len(harness.copy_calls()) == copies_before + 1


def test_prune_keeps_newest_only_matching(harness: _Harness) -> None:
    _require("age")
    harness.write_sources()
    harness.make_identity()
    for i in range(14):
        (harness.remote / f"secrets-2025010100{i:04d}Z.tar.age").write_bytes(b"old")
    (harness.remote / "other.txt").write_text("keep me", encoding="utf-8")

    # 14 pre-existing objects plus the one uploaded now: keeping 13 deletes exactly the 2 oldest.
    result = harness.run(ESCROW_KEEP="13")

    assert result.returncode == 0, result.stderr
    assert "kept=13" in result.stdout
    remaining = harness.remote_names()
    assert "other.txt" in remaining
    assert "secrets-20250101000000Z.tar.age" not in remaining
    assert "secrets-20250101000001Z.tar.age" not in remaining
    assert "secrets-20250101000002Z.tar.age" in remaining
    assert len([n for n in remaining if n.startswith("secrets-")]) == 13


def test_upload_failure_fails_and_keeps_state(harness: _Harness) -> None:
    _require("age")
    harness.write_sources()
    harness.make_identity()

    result = harness.run(FAKE_COPY_RC="1")

    assert result.returncode != 0
    assert "status=failed" in result.stdout
    assert not harness.state.exists()
    assert list(harness.tmpdir.iterdir()) == []


def test_no_plaintext_on_disk(harness: _Harness) -> None:
    _require("age")
    harness.write_sources()
    harness.make_identity()

    result = harness.run()

    assert result.returncode == 0, result.stderr
    upload_dir = harness.src_dir_log.read_text(encoding="utf-8").strip()
    assert upload_dir.startswith(str(harness.tmpdir))
    listing = harness.src_list_log.read_text(encoding="utf-8").split()
    assert listing
    assert all(name.endswith(".age") for name in listing)
    assert list(harness.tmpdir.iterdir()) == []
    for path in harness.root.rglob("*"):
        if path.is_file() and harness.src not in path.parents:
            content = path.read_bytes()
            for secret in SECRET_VALUES:
                assert secret not in content, path


def test_missing_age_binary_exits_69(harness: _Harness) -> None:
    harness.write_sources()
    harness.recipients.write_text("age1placeholder\n", encoding="utf-8")

    result = harness.run(AGE_BIN="/nonexistent/age-binary")

    assert result.returncode == 69
    assert "status=failed" in result.stdout
    assert not harness.rclone_log.exists()
