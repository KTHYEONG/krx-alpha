"""gen_code_map matcher union and artifact invariant guards."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.agent_skills import gen_code_map
from tools.agent_skills import lean_check


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def _scaffold() -> dict[str, str]:
    return {
        "src/__init__.py": "",
        "src/pkg/__init__.py": "",
        "src/pkg/target.py": "VALUE = 1\n",
        "src/pkg/caller.py": "from src.pkg.target import VALUE\nRESULT = VALUE\n",
        "tests/architecture/test_layering.py": "def test_arch(): pass\n",
        "tests/unit/pkg/test_target.py": "import src.pkg.target\n",
        "tests/unit/test_caller_suite.py": "import src.pkg.caller\n",
    }


def test_matcher_unions_mirrored_and_caller_suites(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())

    test_files = lean_check._repository_test_files()
    matched = gen_code_map._matching_tests("src/pkg/target.py", test_files)

    assert matched == ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"]


def test_generation_preserves_schema_and_exclusions(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/pkg/__init__.py": "",
            "src/a.py": "VALUE = 1\n",
            "src/b.py": "OTHER = 2\n",
            "src/c.py": "THING = 3\n",
            "legacy/old.py": "x = 1\n",
            "tests/architecture/test_layering.py": "import src.a\n",
            "tests/unit/test_a.py": "import src.a\n",
            "tests/unit/test_b_one.py": "import src.b\n",
            "tests/unit/test_b_two.py": "from src.b import OTHER\n",
        },
    )

    gen_code_map.main()
    capsys.readouterr()

    raw = (tmp_path / "docs" / "code_map.json").read_bytes()
    assert raw.endswith(b"\n")
    code_map = json.loads(raw.decode("utf-8"))

    assert sorted(code_map) == ["src/a.py", "src/b.py", "src/c.py"]
    assert code_map["src/a.py"] == {"testing": "tests/unit/test_a.py"}
    assert code_map["src/b.py"] == {"testing": ["tests/unit/test_b_one.py", "tests/unit/test_b_two.py"]}
    assert code_map["src/c.py"] == {}
    for entry in code_map.values():
        for suite in ([entry["testing"]] if isinstance(entry.get("testing"), str) else entry.get("testing", [])):
            assert suite.startswith("tests/unit/")
            assert (tmp_path / suite).is_file()


def test_generation_preserves_existing_metadata(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    docs_map = tmp_path / "docs" / "code_map.json"
    docs_map.parent.mkdir(parents=True, exist_ok=True)
    docs_map.write_text(
        json.dumps(
            {
                "src/pkg/target.py": {
                    "deps": ["src/pkg/dependency.py"],
                    "layer": 3,
                    "testing": "tests/unit/old.py",
                },
                "src/pkg/removed.py": {"deps": ["src/old.py"], "layer": 9},
            }
        ),
        encoding="utf-8",
    )

    gen_code_map.main()
    capsys.readouterr()

    result = json.loads(docs_map.read_text(encoding="utf-8"))
    assert result["src/pkg/target.py"] == {
        "deps": ["src/pkg/dependency.py"],
        "layer": 3,
        "testing": ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"],
    }
    assert "src/pkg/removed.py" not in result


def test_generation_failure_preserves_existing_map(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    docs_map = tmp_path / "docs" / "code_map.json"
    docs_map.parent.mkdir(parents=True, exist_ok=True)
    sentinel = b'{"sentinel": true}\n'
    docs_map.write_bytes(sentinel)
    _write_tree(tmp_path, {"src/pkg/bad.py": "from src.missing import thing\nVALUE = thing\n"})

    with pytest.raises(ValueError, match=r"src/pkg/bad\.py"):
        gen_code_map.main()

    capsys.readouterr()
    assert docs_map.read_bytes() == sentinel


def test_generation_output_is_reproducible(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    archived = tmp_path / "legacy" / "docs" / "code_map.json"
    archived.parent.mkdir(parents=True, exist_ok=True)
    archived.write_bytes(b'{"archived": true}\n')

    gen_code_map.main()
    capsys.readouterr()
    first = (tmp_path / "docs" / "code_map.json").read_bytes()

    gen_code_map.main()
    capsys.readouterr()
    second = (tmp_path / "docs" / "code_map.json").read_bytes()

    assert first == second
    assert first.endswith(b"\n")
    assert archived.read_bytes() == b'{"archived": true}\n'


def _forbid_pairwise_predicate(test_file: str, source_file: str) -> bool:
    raise AssertionError("generation must reuse the shared selection state")


def test_generation_builds_selection_graph_once(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    builds = 0
    original_build = lean_check._build_selection_state

    def counting_build() -> dict:
        nonlocal builds
        builds += 1
        return original_build()

    monkeypatch.setattr(lean_check, "_build_selection_state", counting_build)
    monkeypatch.setattr(lean_check, "_test_references_source", _forbid_pairwise_predicate)

    gen_code_map.main()
    capsys.readouterr()

    assert builds == 1


def test_matcher_builds_selection_graph_once(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    builds = 0
    original_build = lean_check._build_selection_state

    def counting_build() -> dict:
        nonlocal builds
        builds += 1
        return original_build()

    monkeypatch.setattr(lean_check, "_build_selection_state", counting_build)
    monkeypatch.setattr(lean_check, "_test_references_source", _forbid_pairwise_predicate)

    test_files = lean_check._repository_test_files()
    matched = gen_code_map._matching_tests("src/pkg/target.py", test_files)

    assert builds == 1
    assert matched == ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"]
