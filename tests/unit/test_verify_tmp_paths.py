from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_verify():
    spec = importlib.util.spec_from_file_location("verify_under_test", str(REPO_ROOT / "tools" / "verify.py"))
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_invocation_dirs_unique(tmp_path: Path) -> None:
    verify = _load_verify()
    base = tmp_path / "verify"
    first = verify._make_invocation_dir(base)
    second = verify._make_invocation_dir(base)
    assert first != second
    assert first.is_dir()
    assert second.is_dir()
    assert first.parent == base
    assert second.parent == base


def test_extra_env_forwarded() -> None:
    verify = _load_verify()
    res = verify.run_cmd(
        [sys.executable, "-c", "import os;print(os.environ['COVERAGE_FILE'])"],
        extra_env={"COVERAGE_FILE": "x"},
    )
    assert res.returncode == 0
    assert res.stdout.strip() == "x"


def test_compose_change_selects_deployment_contract_test() -> None:
    verify = _load_verify()
    assert verify._contract_tests(["docker-compose.yml"]) == ["tests/unit/core/test_deployment_config.py"]


def test_config_change_selects_deployment_and_architecture_contracts() -> None:
    verify = _load_verify()
    selected = verify._contract_tests(["src/core/config.py"])
    assert "tests/unit/core/test_deployment_config.py" in selected
    assert "tests/architecture" in selected


def test_unrelated_change_selects_no_contract_tests() -> None:
    verify = _load_verify()
    assert verify._contract_tests(["README.md", "tests/unit/core/test_paths.py"]) == []
