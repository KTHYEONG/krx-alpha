"""lean_check 단위 검증 및 회귀 가드 테스트."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.agent_skills import lean_check


def test_scaffolding_leak_guard_detects_recipe_and_todo(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    dirty_py = src_dir / "dirty.py"
    dirty_py.write_text(
        'def target():\n    """\n    [STEP-BY-STEP RECIPE FOR IMPLEMENTER]:\n    Step 1. Do something\n    """\n    # TODO: fix this later\n    return 42\n',
        encoding="utf-8",
    )
    clean_py = src_dir / "clean.py"
    clean_py.write_text(
        'def clean_func() -> int:\n    """Standard Google-style docstring."""\n    return 42\n',
        encoding="utf-8",
    )

    leaks = lean_check._check_scaffolding_leaks([str(dirty_py), str(clean_py)])
    assert len(leaks) >= 2
    leak_msgs = [d["error"] for d in leaks]
    assert any("Recipe directive leaked" in m for m in leak_msgs)
    assert any("TODO/FIXME placeholder" in m for m in leak_msgs)

    # Clean file produces 0 leaks
    assert len(lean_check._check_scaffolding_leaks([str(clean_py)])) == 0


def test_find_test_files_direct_convention(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    src_dir = tmp_path / "src" / "mhs" / "engine"
    src_dir.mkdir(parents=True)
    source_file = src_dir / "matcher.py"
    source_file.write_text("def match(): pass\n", encoding="utf-8")

    tests_dir = tmp_path / "tests" / "unit" / "mhs" / "engine"
    tests_dir.mkdir(parents=True)
    test_file = tests_dir / "test_matcher.py"
    test_file.write_text("def test_match(): pass\n", encoding="utf-8")

    matched = lean_check._find_test_files(["src/mhs/engine/matcher.py"])
    assert "tests/unit/mhs/engine/test_matcher.py" in matched


def test_find_test_files_spec_markdown(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    tests_dir = tmp_path / "tests" / "unit"
    tests_dir.mkdir(parents=True)
    test_file = tests_dir / "test_spec_feature.py"
    test_file.write_text("def test_it(): pass\n", encoding="utf-8")

    spec_file = tmp_path / "feature_spec.md"
    spec_file.write_text(
        "# Feature Spec\n\n## Test Suite: tests/unit/test_spec_feature.py\n",
        encoding="utf-8",
    )

    matched = lean_check._find_test_files([], spec_path=str(spec_file))
    assert "tests/unit/test_spec_feature.py" in matched


def test_available_memory_gb_returns_positive_float() -> None:
    mem = lean_check._available_memory_gb()
    assert isinstance(mem, float)
    assert mem > 0.0


def test_emit_json_format() -> None:
    pass_json = lean_check._emit_json("PASS", "all", [], coverage=100)
    data = json.loads(pass_json)
    assert data["status"] == "PASS"
    assert data["exit_code"] == 0
    assert data["coverage"] == 100
    assert data["diagnostics"] == []

    fail_json = lean_check._emit_json("FAIL", "pytest", [{"error": "fail"}])
    fail_data = json.loads(fail_json)
    assert fail_data["status"] == "FAIL"
    assert fail_data["exit_code"] == 1


def test_find_test_files_spec_markdown_invariant_scenarios(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    tests_dir = tmp_path / "tests" / "unit"
    tests_dir.mkdir(parents=True)
    test_file = tests_dir / "test_invariant_feature.py"
    test_file.write_text("def test_invariant(): pass\n", encoding="utf-8")

    spec_file = tmp_path / "feature_spec.md"
    spec_file.write_text(
        "# Feature Spec\n\n## Invariant Scenarios: tests/unit/test_invariant_feature.py\n",
        encoding="utf-8",
    )

    matched = lean_check._find_test_files([], spec_path=str(spec_file))
    assert "tests/unit/test_invariant_feature.py" in matched


def test_check_pre_impl_spec_valid(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    src_dir = tmp_path / "src" / "pkg"
    src_dir.mkdir(parents=True)
    caller_file = src_dir / "caller.py"
    caller_file.write_text("def run():\n    anchor_symbol()\n", encoding="utf-8")

    tests_dir = tmp_path / "tests" / "unit"
    tests_dir.mkdir(parents=True)

    spec_file = tmp_path / "valid_spec.md"
    spec_file.write_text(
        "## Target: src/pkg/target.py\n\n"
        "## Wiring: src/pkg/caller.py\n"
        "- Anchor: anchor_symbol\n\n"
        "## Invariant Scenarios: tests/unit/test_target.py\n",
        encoding="utf-8",
    )

    code, diags = lean_check._check_pre_impl_spec(str(spec_file))
    assert code == 0
    assert diags == []


def test_check_pre_impl_spec_invalid_anchor_and_missing_caller(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    src_dir = tmp_path / "src" / "pkg"
    src_dir.mkdir(parents=True)
    caller_file = src_dir / "caller.py"
    caller_file.write_text("def run():\n    pass\n", encoding="utf-8")

    spec_file = tmp_path / "invalid_spec.md"
    spec_file.write_text(
        "## Target: non_existent_dir/sub/target.py\n\n"
        "## Wiring: src/pkg/caller.py\n"
        "- Anchor: missing_anchor\n\n"
        "## Invariant Scenarios: bad_dir/test_target.py\n",
        encoding="utf-8",
    )

    code, diags = lean_check._check_pre_impl_spec(str(spec_file))
    assert code == 1
    assert len(diags) == 3
    errors = [d["error"] for d in diags]
    assert any("Target parent directory does not exist" in e for e in errors)
    assert any("Wiring anchor 'missing_anchor' not found" in e for e in errors)
    assert any("Test suite directory does not exist" in e for e in errors)


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def test_repository_test_files_discovers_only_active_suites(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "tests/unit/test_a.py": "def test_a(): pass\n",
            "tests/contract/test_c.py": "def test_c(): pass\n",
            "tests/integration/test_i.py": "def test_i(): pass\n",
            "tests/e2e/test_e.py": "def test_e(): pass\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/conftest.py": "import pytest\n",
            "tests/unit/__init__.py": "",
            "tests/unit/helper.py": "x = 1\n",
            "tests/unit/__pycache__/test_cached.py": "def test_cached(): pass\n",
            "legacy/test_old.py": "def test_old(): pass\n",
        },
    )

    assert lean_check._repository_test_files() == [
        "tests/architecture/test_layering.py",
        "tests/contract/test_c.py",
        "tests/e2e/test_e.py",
        "tests/integration/test_i.py",
        "tests/unit/test_a.py",
    ]


def test_repository_test_files_missing_directory_raises(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    with pytest.raises(FileNotFoundError, match="tests"):
        lean_check._repository_test_files()


def test_find_test_files_unions_mirrored_caller_and_architecture(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/target.py": "VALUE = 1\n",
            "src/pkg/caller.py": "from src.pkg.target import VALUE\nRESULT = VALUE\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/pkg/test_target.py": "import src.pkg.target\n",
            "tests/unit/test_uses_caller.py": "import src.pkg.caller\n",
            "tests/unit/test_unrelated.py": "x = 1\n",
        },
    )

    selected = lean_check._find_test_files(["src/pkg/target.py"])

    assert "tests/unit/pkg/test_target.py" in selected
    assert "tests/unit/test_uses_caller.py" in selected
    assert "tests/architecture/test_layering.py" in selected
    assert "tests/unit/test_unrelated.py" not in selected


def test_find_test_files_selects_facade_tests_for_extracted_module(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/new_mod.py": "VALUE = 1\n",
            "src/pkg/facade.py": "from src.pkg.new_mod import VALUE\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_facade.py": "import src.pkg.facade\n",
        },
    )

    selected = lean_check._find_test_files(["src/pkg/new_mod.py"])

    assert "tests/unit/test_facade.py" in selected
    assert "tests/architecture/test_layering.py" in selected


def test_references_source_matches_literal_patch_targets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/target.py": "VALUE = 1\n",
            "tests/unit/test_patch.py": (
                'from unittest.mock import patch\n'
                '@patch("src.pkg.target.fetch")\n'
                "def test_x(m): pass\n"
            ),
            "tests/unit/test_monkeypatch.py": (
                "def test_y(monkeypatch):\n"
                '    monkeypatch.setattr("src.pkg.target.VALUE", 1)\n'
            ),
            "tests/unit/test_noise.py": (
                '"""src.pkg.target is only mentioned in prose."""\nNOTE = "src.pkg.target"\nimport os\n'
            ),
        },
    )

    assert lean_check._test_references_source("tests/unit/test_patch.py", "src/pkg/target.py") is True
    assert lean_check._test_references_source("tests/unit/test_monkeypatch.py", "src/pkg/target.py") is True
    assert lean_check._test_references_source("tests/unit/test_noise.py", "src/pkg/target.py") is False


def test_find_test_files_resolves_relative_and_lazy_test_imports(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/target.py": "VALUE = 1\n",
            "src/pkg/caller.py": "from . import target\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_lazy.py": "def test_x():\n    from src.pkg import target\n    assert target.VALUE == 1\n",
            "tests/unit/test_caller.py": "import src.pkg.caller\n",
        },
    )

    selected = lean_check._find_test_files(["src/pkg/target.py"])

    assert "tests/unit/test_lazy.py" in selected
    assert "tests/unit/test_caller.py" in selected


def test_find_test_files_retains_explicit_and_spec_suites(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "tests/unit/test_x.py": "def test_x(): pass\n",
            "tests/unit/test_declared.py": "def test_declared(): pass\n",
        },
    )
    spec_file = tmp_path / "feature_spec.md"
    spec_file.write_text(
        "# Feature Spec\n\n## Invariant Scenarios: tests/unit/test_declared.py\n",
        encoding="utf-8",
    )

    selected = lean_check._find_test_files(["tests/unit/test_x.py"], spec_path=str(spec_file))

    assert selected == ["tests/unit/test_declared.py", "tests/unit/test_x.py"]


def test_find_test_files_fails_closed_for_uncovered_source(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/lonely.py": "VALUE = 1\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_other.py": "x = 1\n",
        },
    )

    with pytest.raises(ValueError, match=r"src/pkg/lonely\.py"):
        lean_check._find_test_files(["src/pkg/lonely.py"])


def test_find_test_files_initializer_and_test_only_selection(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "x = 1\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_x.py": "def test_x(): pass\n",
        },
    )

    assert lean_check._find_test_files(["src/pkg/__init__.py"]) == ["tests/architecture/test_layering.py"]
    assert lean_check._find_test_files(["tests/unit/test_x.py"]) == ["tests/unit/test_x.py"]


def test_find_test_files_surfaces_graph_and_file_errors(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/good.py": "VALUE = 1\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_good.py": "import src.pkg.good\n",
        },
    )

    with pytest.raises(ValueError, match=r"src/does/not_exist\.py"):
        lean_check._find_test_files(["src/does/not_exist.py"])

    (tmp_path / "src" / "pkg" / "broken.py").write_text("def broken(:\n", encoding="utf-8")
    with pytest.raises(SyntaxError, match=r"src/pkg/broken\.py"):
        lean_check._find_test_files(["src/pkg/good.py"])

    (tmp_path / "src" / "pkg" / "broken.py").unlink()
    (tmp_path / "src" / "pkg" / "bad_ref.py").write_text(
        "from src.missing import thing\nVALUE = thing\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match=r"src/pkg/bad_ref\.py"):
        lean_check._find_test_files(["src/pkg/good.py"])


def test_main_selection_failure_exits_nonzero(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/bad.py": "from src.missing import thing\nVALUE = thing\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_other.py": "x = 1\n",
        },
    )
    monkeypatch.setattr("sys.argv", ["lean_check", "--files", "src/pkg/bad.py"])

    with pytest.raises(SystemExit) as exc_info:
        lean_check.main()

    assert exc_info.value.code != 0
    capsys.readouterr()


def test_find_test_files_is_deterministic_and_reflects_edits(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/pkg/target.py": "VALUE = 1\n",
            "tests/architecture/test_layering.py": "def test_arch(): pass\n",
            "tests/unit/test_one.py": "import src.pkg.target\n",
            "tests/unit/test_other.py": "x = 1\n",
        },
    )

    first = lean_check._find_test_files(["src/pkg/target.py", "tests/unit/test_other.py"])
    second = lean_check._find_test_files(["tests/unit/test_other.py", "src/pkg/target.py"])

    assert first == second
    assert "tests/unit/test_other.py" in first
    assert first == sorted(first)

    (tmp_path / "tests" / "unit" / "test_other.py").write_text("import src.pkg.target\n", encoding="utf-8")
    third = lean_check._find_test_files(["src/pkg/target.py"])

    assert "tests/unit/test_other.py" in third

