from __future__ import annotations

import importlib.util
import io
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GUARDS_PATH = REPO_ROOT / "tools" / "guards" / "guards.py"
SPEC_PATH = REPO_ROOT / "tools" / "guards" / "guards.toml"
MARKET_HOURS = datetime(2026, 11, 3, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))


def _load_guards() -> ModuleType:
    spec = importlib.util.spec_from_file_location("guards_pretool_under_test", str(GUARDS_PATH))
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _bash_payload(command: str) -> str:
    return json.dumps({"tool_name": "Bash", "tool_input": {"command": command}})


@pytest.fixture(scope="module")
def guards() -> ModuleType:
    return _load_guards()


@pytest.fixture(scope="module")
def spec(guards: ModuleType) -> Any:
    return guards.load_guard_spec(SPEC_PATH)


def _run(guards: ModuleType, spec: Any, stdin_text: str, project: Path) -> tuple[int, str]:
    stderr = io.StringIO()
    code = guards.main_pretool(io.StringIO(stdin_text), stderr, MARKET_HOURS, project, spec)
    return code, stderr.getvalue()


def test_non_bash_tool_ignored(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    payload = json.dumps({"tool_name": "Edit", "tool_input": {"file_path": "rm -rf ~/x"}})
    assert _run(guards, spec, payload, tmp_path) == (0, "")


def test_deny_exits_2_with_reason(
    guards: ModuleType, spec: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, stderr = _run(guards, spec, _bash_payload("rm -rf ~/x"), tmp_path)
    assert code == 2
    assert "recursive rm" in stderr
    assert capsys.readouterr().out == ""


def test_allow_exits_0_silently(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    assert _run(guards, spec, _bash_payload("echo hi"), tmp_path) == (0, "")


def test_invalid_json_fails_closed(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    code, _ = _run(guards, spec, "not json", tmp_path)
    assert code == 2


def test_non_object_payload_fails_closed(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    code, _ = _run(guards, spec, "[]", tmp_path)
    assert code == 2


def test_missing_command_fails_closed(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    code, _ = _run(guards, spec, json.dumps({"tool_name": "Bash", "tool_input": {}}), tmp_path)
    assert code == 2


def test_internal_error_on_risky_command_blocks(
    guards: ModuleType, spec: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(guards, "evaluate_bash", boom)
    assert _run(guards, spec, _bash_payload("rm -rf x"), tmp_path) == (
        2,
        "guard internal error (fail-closed): RuntimeError\n",
    )
    assert _run(guards, spec, _bash_payload("echo hi"), tmp_path) == (0, "")


def test_oversized_risky_command_fails_closed(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    code, _ = _run(guards, spec, _bash_payload("rm -rf " + "a" * 100_000), tmp_path)
    assert code == 2


def test_oversized_benign_command_allowed(guards: ModuleType, spec: Any, tmp_path: Path) -> None:
    code, _ = _run(guards, spec, _bash_payload("echo " + "a" * 100_000), tmp_path)
    assert code == 0


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (_bash_payload("echo hi"), 0),
        (_bash_payload("rm -rf ~/x"), 2),
        ("not json", 2),
    ],
)
def test_subprocess_isolated_mode_works(payload: str, expected: int, tmp_path: Path) -> None:
    completed = subprocess.run(  # noqa: S603
        [sys.executable, "-I", str(GUARDS_PATH), "pretool"],
        input=payload,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={"CLAUDE_PROJECT_DIR": str(tmp_path)},
        timeout=60,
        check=False,
    )
    assert completed.returncode == expected
    assert completed.stdout == ""


def test_main_rejects_unknown_subcommand(guards: ModuleType, capsys: pytest.CaptureFixture[str]) -> None:
    assert guards.main([]) == 2
    assert guards.main(["bogus"]) == 2
    assert capsys.readouterr().out == ""
