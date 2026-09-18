"""Resolved dependency graph invariant guards on isolated source trees."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.agent_skills.dependency_graph import (
    dependency_cycles,
    internal_dependencies,
    repository_source_files,
)


def _write(root: Path, rel: str, content: str = "x = 1\n") -> None:
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def test_repository_discovers_only_sorted_src_paths(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/a/__init__.py")
    _write(tmp_path, "src/a/b.py")
    _write(tmp_path, "src/top.py")
    _write(tmp_path, "src/a/__pycache__/cached.py")
    _write(tmp_path, "legacy/old.py")
    _write(tmp_path, "scratch/note.py")

    assert repository_source_files(tmp_path) == [
        "src/__init__.py",
        "src/a/__init__.py",
        "src/a/b.py",
        "src/top.py",
    ]


def test_repository_missing_source_tree_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        repository_source_files(tmp_path)


def test_resolves_absolute_module_and_symbol_imports(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/a/__init__.py")
    _write(tmp_path, "src/a/b.py", "VALUE = 1\n")
    _write(tmp_path, "src/a/consumer.py", "import src.a.b\nfrom src.a.b import VALUE\n")

    sources = ["src/__init__.py", "src/a/__init__.py", "src/a/b.py", "src/a/consumer.py"]
    deps = internal_dependencies("src/a/consumer.py", sources, root=tmp_path)

    assert "src/a/b.py" in deps
    assert "src/a/__init__.py" in deps
    assert "src/__init__.py" in deps
    assert not any(p.endswith("VALUE.py") for p in deps)


def test_resolves_relative_imports_at_multiple_levels(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/pkg/__init__.py")
    _write(tmp_path, "src/pkg/sibling.py")
    _write(tmp_path, "src/pkg/sub/__init__.py")
    _write(tmp_path, "src/pkg/sub/leaf.py", "from .. import sibling\n")
    _write(tmp_path, "src/pkg/sub/cousin.py", "from . import leaf\n")
    _write(tmp_path, "src/pkg/top.py", "from .sub import leaf\n")

    sources = repository_source_files(tmp_path)
    assert internal_dependencies("src/pkg/sub/leaf.py", sources, root=tmp_path) == {
        "src/pkg/sibling.py",
        "src/pkg/__init__.py",
        "src/__init__.py",
    }
    assert "src/pkg/sub/leaf.py" in internal_dependencies("src/pkg/sub/cousin.py", sources, root=tmp_path)
    assert "src/pkg/sub/leaf.py" in internal_dependencies("src/pkg/top.py", sources, root=tmp_path)


def test_initializer_resolves_sibling_package_context(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/pkg/__init__.py", "from . import sibling\n")
    _write(tmp_path, "src/pkg/sibling.py")

    sources = repository_source_files(tmp_path)
    deps = internal_dependencies("src/pkg/__init__.py", sources, root=tmp_path)

    assert "src/pkg/sibling.py" in deps


def test_includes_wildcard_and_lazy_imports(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/a.py", "VALUE = 1\n")
    _write(tmp_path, "src/b.py", "OTHER = 2\n")
    _write(tmp_path, "src/c.py", "THING = 3\n")
    _write(
        tmp_path,
        "src/consumer.py",
        "from src.a import *\n"
        "def run():\n"
        "    import src.b\n"
        "    return src.b\n"
        "try:\n"
        "    from src.c import THING\n"
        "except ImportError:\n"
        "    THING = None\n",
    )

    sources = repository_source_files(tmp_path)
    deps = internal_dependencies("src/consumer.py", sources, root=tmp_path)

    assert {"src/a.py", "src/b.py", "src/c.py"} <= deps


def test_excludes_external_and_test_local_imports(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/a.py")
    _write(tmp_path, "tests/unit/__init__.py")
    _write(
        tmp_path,
        "tests/unit/test_a.py",
        "import requests\nfrom . import helper\nimport src.a\n",
    )
    _write(tmp_path, "tests/unit/helper.py")

    sources = ["src/__init__.py", "src/a.py"]
    deps = internal_dependencies("tests/unit/test_a.py", sources, root=tmp_path)

    assert deps == {"src/a.py", "src/__init__.py"}


def test_unresolved_source_imports_raise_with_path(tmp_path: Path) -> None:
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/a.py", "import src.missing\n")
    _write(tmp_path, "src/b.py", "from src.missing import thing\n")
    _write(tmp_path, "src/pkg/__init__.py")
    _write(tmp_path, "src/pkg/mod.py", "from ..nonexistent import thing\n")
    _write(tmp_path, "src/escape.py", "from .. import outside\n")

    sources = repository_source_files(tmp_path)
    for bad in ("src/a.py", "src/b.py", "src/pkg/mod.py", "src/escape.py"):
        with pytest.raises(ValueError, match=bad):
            internal_dependencies(bad, sources, root=tmp_path)


def test_invalid_python_raises_without_executing(tmp_path: Path) -> None:
    marker = tmp_path / "executed.txt"
    _write(tmp_path, "src/__init__.py")
    _write(tmp_path, "src/bad.py", "import os\nos.write_text = 1\ndef broken(:\n    pass\n")

    with pytest.raises(SyntaxError):
        internal_dependencies("src/bad.py", ["src/__init__.py", "src/bad.py"], root=tmp_path)
    assert not marker.exists()


def test_diamond_and_empty_graphs_are_acyclic() -> None:
    diamond = {
        "src/a.py": {"src/b.py", "src/c.py"},
        "src/b.py": {"src/d.py"},
        "src/c.py": {"src/d.py"},
        "src/d.py": set(),
    }
    assert dependency_cycles(diamond) == []
    assert dependency_cycles({}) == []
    assert dependency_cycles({"src/a.py": set()}) == []


def test_reports_same_package_and_initializer_cycles() -> None:
    graph = {
        "src/pkg/a.py": {"src/pkg/b.py"},
        "src/pkg/b.py": {"src/pkg/a.py"},
        "src/pkg/__init__.py": {"src/pkg/mod.py"},
        "src/pkg/mod.py": {"src/pkg/__init__.py"},
        "src/other.py": set(),
    }
    cycles = dependency_cycles(graph)

    assert ("src/pkg/a.py", "src/pkg/b.py") in cycles
    assert ("src/pkg/__init__.py", "src/pkg/mod.py") in cycles
    assert len(cycles) == 2


def test_self_edge_is_cycle_and_unknown_target_raises() -> None:
    assert dependency_cycles({"src/a.py": {"src/a.py"}}) == [("src/a.py",)]
    with pytest.raises(ValueError, match="unknown dependency"):
        dependency_cycles({"src/a.py": {"src/missing.py"}})


def test_cyclic_components_are_deterministic() -> None:
    first = {
        "src/b.py": {"src/a.py"},
        "src/a.py": {"src/b.py"},
        "src/d.py": {"src/c.py"},
        "src/c.py": {"src/d.py"},
        "src/z.py": set(),
    }
    second = {
        "src/z.py": set(),
        "src/d.py": {"src/c.py"},
        "src/c.py": {"src/d.py"},
        "src/a.py": {"src/b.py"},
        "src/b.py": {"src/a.py"},
    }
    assert dependency_cycles(first) == dependency_cycles(second) == [
        ("src/a.py", "src/b.py"),
        ("src/c.py", "src/d.py"),
    ]
