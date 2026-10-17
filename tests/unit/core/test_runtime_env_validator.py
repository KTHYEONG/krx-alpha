"""Invariant guards for the runtime-env key manifest and the VPS validator script."""

from __future__ import annotations

import grp
import os
import pwd
import subprocess
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO_ROOT / "deploy/host/krx-validate-runtime-env.sh"


def test_manifest_matches_python_declarations() -> None:
    from src.core.runtime_env_provisioning import (
        OPTIONAL_RUNTIME_ENV_KEYS,
        RUNTIME_ENV_SPEC,
        render_key_manifest,
    )

    manifest = render_key_manifest()
    lines = manifest.splitlines()
    required = [line.split(" ", 1)[1] for line in lines if line.startswith("required ")]
    optional = [line.split(" ", 1)[1] for line in lines if line.startswith("optional ")]

    assert required == [key.target for key in RUNTIME_ENV_SPEC]
    assert optional == list(OPTIONAL_RUNTIME_ENV_KEYS)
    assert manifest.endswith("\n")


def test_manifest_cli_writes_names_only(tmp_path) -> None:
    from src.core.runtime_env_provisioning import main, render_key_manifest

    out = tmp_path / "runtime-env-keys.txt"
    assert main(["emit-key-manifest", "--out", str(out)]) == 0

    text = out.read_text(encoding="utf-8")
    assert text == render_key_manifest()
    assert "=" not in text
    assert out.stat().st_mode & 0o777 == 0o644


def test_module_import_closure_stdlib_only() -> None:
    cmd = [
        "python3",
        "-I",
        "-S",
        "-c",
        "import sys; sys.path.insert(0, sys.argv[1]); import src.core.runtime_env_provisioning",
        str(_REPO_ROOT),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603, S607 - fixed argv, hermetic probe
    assert result.returncode == 0, result.stderr


def _current_owner() -> str:
    user = pwd.getpwuid(os.getuid()).pw_name
    group = grp.getgrgid(os.getgid()).gr_name
    return f"{user}:{group}"


def _base_values() -> dict[str, str]:
    from src.core.runtime_env_provisioning import OPTIONAL_RUNTIME_ENV_KEYS, RUNTIME_ENV_SPEC

    values = {key.target: f"value-{key.target}" for key in RUNTIME_ENV_SPEC}
    values.update({name: f"value-{name}" for name in OPTIONAL_RUNTIME_ENV_KEYS})
    return values


def _write_env(tmp_path: Path, values: dict[str, str], *, mode: int = 0o600, name: str = "krx-alpha.env") -> Path:
    path = tmp_path / name
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
    path.chmod(mode)
    return path


def _manifest(tmp_path: Path, *, name: str = "runtime-env-keys.txt") -> Path:
    from src.core.runtime_env_provisioning import render_key_manifest

    path = tmp_path / name
    path.write_text(render_key_manifest(), encoding="utf-8")
    return path


def _run(env_path: Path, manifest_path: Path, *, expected_owner: str | None = None) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    if expected_owner is not None:
        env["EXPECTED_OWNER"] = expected_owner
    cmd = ["bash", str(_SCRIPT), str(env_path), str(manifest_path)]
    return subprocess.run(cmd, capture_output=True, text=True, env=env)  # noqa: S603, S607 - fixed argv, hermetic probe


def test_validator_accepts_complete_env(tmp_path) -> None:
    result = _run(_write_env(tmp_path, _base_values()), _manifest(tmp_path), expected_owner=_current_owner())

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "Runtime env validated: 14 keys"


def test_validator_optional_keys_not_required(tmp_path) -> None:
    from src.core.runtime_env_provisioning import OPTIONAL_RUNTIME_ENV_KEYS

    values = {key: value for key, value in _base_values().items() if key not in OPTIONAL_RUNTIME_ENV_KEYS}
    result = _run(_write_env(tmp_path, values), _manifest(tmp_path), expected_owner=_current_owner())

    assert result.returncode == 0, result.stderr


def test_validator_rejects_unexpected_key_without_value_leak(tmp_path) -> None:
    result = _run(
        _write_env(tmp_path, _base_values() | {"FOO": "supersecret"}),
        _manifest(tmp_path),
        expected_owner=_current_owner(),
    )

    assert result.returncode == 1
    assert "FOO" in result.stderr
    assert "supersecret" not in result.stdout + result.stderr


def test_validator_rejects_empty_required(tmp_path) -> None:
    values = _base_values()
    values["LS_APP_KEY"] = ""
    result = _run(_write_env(tmp_path, values), _manifest(tmp_path), expected_owner=_current_owner())

    assert result.returncode == 1
    assert "Missing required runtime env key: LS_APP_KEY" in result.stderr


def test_validator_rejects_bad_mode(tmp_path) -> None:
    result = _run(
        _write_env(tmp_path, _base_values(), mode=0o644),
        _manifest(tmp_path),
        expected_owner=_current_owner(),
    )

    assert result.returncode == 1
    assert "Runtime env has unexpected mode" in result.stderr


def test_validator_rejects_bad_owner(tmp_path) -> None:
    result = _run(_write_env(tmp_path, _base_values()), _manifest(tmp_path), expected_owner="invalid-owner:invalid-group")

    assert result.returncode == 1
    assert "Runtime env has unexpected owner" in result.stderr


def test_validator_rejects_corrupt_manifest(tmp_path) -> None:
    manifest = tmp_path / "runtime-env-keys.txt"
    manifest.write_text("required lower_case\n", encoding="utf-8")
    result = _run(_write_env(tmp_path, _base_values()), manifest, expected_owner=_current_owner())

    assert result.returncode == 1
    assert "Runtime env key manifest is invalid" in result.stderr


@pytest.mark.parametrize("assignment", ["LS_APP_KEY", "LS_APP_KEY=\nLS_APP_KEY=sentinel-later"])
def test_validator_requires_first_assignment_to_have_a_value(tmp_path, assignment) -> None:
    env_path = _write_env(tmp_path, _base_values())
    text = env_path.read_text(encoding="utf-8")
    env_path.write_text(text.replace("LS_APP_KEY=value-LS_APP_KEY", assignment), encoding="utf-8")

    result = _run(env_path, _manifest(tmp_path), expected_owner=_current_owner())

    assert result.returncode == 1
    assert "Missing required runtime env key: LS_APP_KEY" in result.stderr
    assert "sentinel-later" not in result.stdout + result.stderr


def test_validator_checks_final_manifest_line_without_newline(tmp_path) -> None:
    manifest = tmp_path / "runtime-env-keys.txt"
    manifest.write_text("required LS_APP_KEY", encoding="utf-8")
    env_path = _write_env(tmp_path, {"LS_APP_KEY": ""})

    result = _run(env_path, manifest, expected_owner=_current_owner())

    assert result.returncode == 1
    assert "Missing required runtime env key: LS_APP_KEY" in result.stderr


@pytest.mark.parametrize("target", ["env", "manifest"])
@pytest.mark.parametrize("exists", [False, True])
def test_validator_rejects_missing_or_empty_input(tmp_path, target, exists) -> None:
    env_path = _write_env(tmp_path, _base_values())
    manifest = _manifest(tmp_path)
    path = env_path if target == "env" else manifest
    if exists:
        path.write_text("", encoding="utf-8")
    else:
        path.unlink()

    result = _run(env_path, manifest, expected_owner=_current_owner())

    assert result.returncode == 1
    expected = "Missing runtime env" if target == "env" else "Runtime env key manifest is invalid"
    assert expected in result.stderr


def test_no_value_ever_printed(tmp_path) -> None:
    from src.core.runtime_env_provisioning import RUNTIME_ENV_SPEC

    sentinel_values = {key.target: f"sentinel-{key.target}" for key in RUNTIME_ENV_SPEC}
    sentinels = list(sentinel_values.values())
    manifest = _manifest(tmp_path)

    accepted = _run(_write_env(tmp_path, sentinel_values), manifest, expected_owner=_current_owner())

    unexpected_values = sentinel_values | {"UNEXPECTED_KEY": "sentinel-UNEXPECTED_KEY"}
    sentinels.append("sentinel-UNEXPECTED_KEY")
    rejected_unexpected = _run(_write_env(tmp_path, unexpected_values), manifest, expected_owner=_current_owner())

    empty_values = sentinel_values | {"LS_APP_KEY": ""}
    rejected_empty = _run(_write_env(tmp_path, empty_values), manifest, expected_owner=_current_owner())

    assert accepted.returncode == 0
    assert rejected_unexpected.returncode == 1
    assert rejected_empty.returncode == 1

    combined = "".join(
        result.stdout + result.stderr for result in (accepted, rejected_unexpected, rejected_empty)
    )
    assert [sentinel for sentinel in sentinels if sentinel in combined] == []
