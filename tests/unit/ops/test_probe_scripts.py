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
