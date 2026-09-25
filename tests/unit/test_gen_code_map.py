"""build_code_map layer/deps/testing invariant guards."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.architecture.layers import LAYER_RANK
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


def _scaffold_sources() -> list[str]:
    return ["src/pkg/target.py", "src/pkg/caller.py"]


def _scaffold_tests() -> list[str]:
    return ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"]


def _scaffold_ranks() -> dict[str, int]:
    return {"src/pkg/target.py": 1, "src/pkg/caller.py": 2}


def test_build_code_map_registers_layer_deps_and_tests(tmp_path: Path, monkeypatch) -> None:
    """New module fully mapped: layer, deps, and testing all appear."""
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())

    code_map = gen_code_map.build_code_map(_scaffold_sources(), _scaffold_tests(), _scaffold_ranks())

    assert code_map["src/pkg/target.py"] == {
        "deps": [],
        "layer": 1,
        "testing": ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"],
    }
    assert code_map["src/pkg/caller.py"] == {
        "deps": ["src/pkg/target.py"],
        "layer": 2,
        "testing": "tests/unit/test_caller_suite.py",
    }


def test_build_code_map_assigns_empty_testing_list_without_tests(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())

    code_map = gen_code_map.build_code_map(["src/pkg/target.py"], [], {"src/pkg/target.py": 1})

    assert code_map["src/pkg/target.py"] == {"deps": [], "layer": 1, "testing": []}


def test_build_code_map_rejects_unregistered_source(tmp_path: Path, monkeypatch) -> None:
    """Unregistered source rejected: fail instead of emitting a partial map."""
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())

    with pytest.raises(ValueError, match=r"src/pkg/target\.py"):
        gen_code_map.build_code_map(_scaffold_sources(), _scaffold_tests(), {"src/pkg/caller.py": 2})


def test_build_code_map_matches_patch_targets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            **_scaffold(),
            "tests/unit/test_patcher.py": (
                "from unittest.mock import patch\n"
                "import src.pkg.caller\n"
                "def test_x():\n"
                "    with patch('src.pkg.target.VALUE', 2):\n"
                "        assert src.pkg.caller.RESULT\n"
            ),
        },
    )

    code_map = gen_code_map.build_code_map(
        _scaffold_sources(), [*_scaffold_tests(), "tests/unit/test_patcher.py"], _scaffold_ranks()
    )

    assert "tests/unit/test_patcher.py" in code_map["src/pkg/target.py"]["testing"]


def test_build_code_map_lists_split_daemon_tests(tmp_path: Path, monkeypatch) -> None:
    """Split daemon tests discoverable: all related modules listed for daemon.py."""
    monkeypatch.chdir(tmp_path)
    _write_tree(
        tmp_path,
        {
            "src/__init__.py": "",
            "src/orchestration/__init__.py": "",
            "src/orchestration/daemon.py": "STATE = 'run'\n",
            "tests/unit/orchestration/test_daemon.py": "import src.orchestration.daemon\n",
            "tests/unit/orchestration/test_daemon_eod.py": "import src.orchestration.daemon\n",
            "tests/unit/orchestration/test_daemon_shutdown.py": (
                "from unittest.mock import patch\n"
                "def test_x():\n"
                "    with patch('src.orchestration.daemon.STATE', 'stop'):\n"
                "        pass\n"
            ),
        },
    )
    tests = [
        "tests/unit/orchestration/test_daemon.py",
        "tests/unit/orchestration/test_daemon_eod.py",
        "tests/unit/orchestration/test_daemon_shutdown.py",
    ]

    code_map = gen_code_map.build_code_map(["src/orchestration/daemon.py"], tests, {"src/orchestration/daemon.py": 6})

    assert code_map["src/orchestration/daemon.py"]["testing"] == sorted(tests)


def test_generation_removes_stale_metadata(tmp_path: Path, monkeypatch, capsys) -> None:
    """Stale previous metadata removed: the obsolete edge does not survive."""
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    for source, rank in _scaffold_ranks().items():
        monkeypatch.setitem(LAYER_RANK, source, rank)
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
    assert sorted(result) == ["src/pkg/caller.py", "src/pkg/target.py"]
    assert result["src/pkg/target.py"] == {
        "deps": [],
        "layer": 1,
        "testing": ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"],
    }
    assert result["src/pkg/caller.py"]["layer"] == 2


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
    for source in ("src/a.py", "src/b.py", "src/c.py"):
        monkeypatch.setitem(LAYER_RANK, source, 1)

    gen_code_map.main()
    capsys.readouterr()

    raw = (tmp_path / "docs" / "code_map.json").read_bytes()
    assert raw.endswith(b"\n")
    code_map = json.loads(raw.decode("utf-8"))

    assert sorted(code_map) == ["src/a.py", "src/b.py", "src/c.py"]
    assert code_map["src/a.py"] == {"deps": [], "layer": 1, "testing": "tests/unit/test_a.py"}
    assert code_map["src/b.py"] == {
        "deps": [],
        "layer": 1,
        "testing": ["tests/unit/test_b_one.py", "tests/unit/test_b_two.py"],
    }
    assert code_map["src/c.py"] == {"deps": [], "layer": 1, "testing": []}
    for entry in code_map.values():
        suites = [entry["testing"]] if isinstance(entry.get("testing"), str) else entry.get("testing", [])
        for suite in suites:
            assert suite.startswith("tests/unit/")
            assert (tmp_path / suite).is_file()


def test_generation_failure_preserves_existing_map(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    for source, rank in _scaffold_ranks().items():
        monkeypatch.setitem(LAYER_RANK, source, rank)
    monkeypatch.setitem(LAYER_RANK, "src/pkg/bad.py", 1)
    docs_map = tmp_path / "docs" / "code_map.json"
    docs_map.parent.mkdir(parents=True, exist_ok=True)
    sentinel = b'{"sentinel": true}\n'
    docs_map.write_bytes(sentinel)
    _write_tree(tmp_path, {"src/pkg/bad.py": "from src.missing import thing\nVALUE = thing\n"})

    with pytest.raises(ValueError, match=r"src/pkg/bad\.py"):
        gen_code_map.main()

    capsys.readouterr()
    assert docs_map.read_bytes() == sentinel


def test_generation_rejects_unregistered_active_source(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    docs_map = tmp_path / "docs" / "code_map.json"
    docs_map.parent.mkdir(parents=True, exist_ok=True)
    sentinel = b'{"sentinel": true}\n'
    docs_map.write_bytes(sentinel)

    with pytest.raises(ValueError, match=r"src/pkg/caller\.py"):
        gen_code_map.main()

    capsys.readouterr()
    assert docs_map.read_bytes() == sentinel


def test_generation_output_is_reproducible(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())
    for source, rank in _scaffold_ranks().items():
        monkeypatch.setitem(LAYER_RANK, source, rank)

    gen_code_map.main()
    capsys.readouterr()
    first = (tmp_path / "docs" / "code_map.json").read_bytes()

    gen_code_map.main()
    capsys.readouterr()
    second = (tmp_path / "docs" / "code_map.json").read_bytes()

    assert first == second
    assert first.endswith(b"\n")


def test_matcher_unions_mirrored_and_caller_suites(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_tree(tmp_path, _scaffold())

    test_files = lean_check._repository_test_files()
    matched = gen_code_map._matching_tests("src/pkg/target.py", test_files)

    assert matched == ["tests/unit/pkg/test_target.py", "tests/unit/test_caller_suite.py"]
