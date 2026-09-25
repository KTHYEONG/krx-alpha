#!/usr/bin/env python3
"""Deterministic code-map generation from the layer registry and import graph."""

from __future__ import annotations

import json
import os
import pathlib
import sys
from collections.abc import Mapping, Sequence
from typing import Any

if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from tests.architecture.layers import LAYER_RANK  # noqa: E402
from tools.agent_skills import lean_check  # noqa: E402
from tools.agent_skills.dependency_graph import internal_dependencies  # noqa: E402


def _match_with_state(source_file: str, test_files: list[str], state: dict[str, Any]) -> list[str]:
    """Map one source module against the generation's shared selection state."""
    if source_file not in state["source_set"]:
        raise ValueError(f"{source_file}: selection requires a current source path")
    parts = source_file.split("/")
    module_name = parts[-1]
    test_name = f"test_{module_name}"
    exact = {
        f"tests/{category}/{'/'.join(parts[1:-1])}/{test_name}" if parts[1:-1] else f"tests/{category}/{test_name}"
        for category in ("unit", "integration", "e2e")
    }
    matched = [tp for tp in test_files if tp in exact]
    if source_file == "src/orchestration/daemon.py":
        # The daemon suite is split across focused modules; mirror them all
        # explicitly instead of assuming the single historical test path.
        for test_file in test_files:
            if test_file.startswith("tests/architecture/"):
                continue
            segs = test_file.split("/")
            if (
                len(segs) == 4
                and segs[2] == "orchestration"
                and segs[3].startswith("test_daemon")
                and test_file not in matched
            ):
                matched.append(test_file)
    known_tests: set[str] = set(state["test_deps"])
    related: list[str] = []
    for test_file in test_files:
        if test_file.startswith("tests/architecture/"):
            continue
        if test_file in known_tests:
            hit = lean_check._references_with_state(state, test_file, source_file)
        else:
            hit = lean_check._references_ondemand(state, test_file, source_file)
        if hit:
            related.append(test_file)
    return sorted(set(matched) | set(related))


def _matching_tests(source_file: str, test_files: list[str]) -> list[str]:
    """Map an active source module to mirrored and dependency-related repository suites.

    Args:
        source_file: Existing non-initializer source path.
        test_files: Repository test module paths to consider.

    Returns:
        Sorted unique non-architecture suite paths related to the source module.

    Raises:
        SyntaxError: If dependency analysis encounters invalid Python.
        ValueError: If an explicit source dependency cannot resolve.
        OSError: If required files cannot be read.
    """
    state = lean_check._build_selection_state()
    return _match_with_state(source_file, test_files, state)


def _build_matching_state(source_files: Sequence[str], test_files: Sequence[str]) -> dict[str, Any]:
    """Build one shared dependency graph for the given generation inputs."""
    root = pathlib.Path(".")
    sources = sorted(source_files)
    tests = sorted(test_files)
    source_set = set(sources)
    graph: dict[str, set[str]] = {}
    for source in sources:
        graph[source] = internal_dependencies(source, sources, root=root)
    reachable: dict[str, set[str]] = {}
    for source in sources:
        seen = {source}
        stack = sorted(graph[source])
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            stack.extend(sorted(graph[current] - seen))
        reachable[source] = seen
    test_deps: dict[str, set[str]] = {}
    test_patches: dict[str, set[str]] = {}
    for test in tests:
        test_deps[test] = internal_dependencies(test, sources, root=root)
        test_patches[test] = lean_check._patch_target_modules(test, source_set)
    return {
        "source_files": sources,
        "source_set": source_set,
        "test_files": tests,
        "graph": graph,
        "reachable": reachable,
        "test_deps": test_deps,
        "test_patches": test_patches,
    }


def build_code_map(
    source_files: Sequence[str],
    test_files: Sequence[str],
    layer_rank: Mapping[str, int],
) -> dict[str, dict[str, object]]:
    """Build complete source, layer, dependency, and test entries from current files.

    Args:
        source_files: Active non-initializer Python source paths.
        test_files: Active Python test paths.
        layer_rank: Current architecture layer registry.

    Returns:
        A deterministic map with layer, deps, and testing for every source.

    Raises:
        ValueError: A source is unregistered or dependency resolution fails.
    """
    for source in sorted(source_files):
        if source not in layer_rank:
            raise ValueError(f"{source}: source is missing from the layer registry")
    state = _build_matching_state(source_files, test_files)
    ordered_tests = sorted(test_files)
    code_map: dict[str, dict[str, object]] = {}
    for source in sorted(source_files):
        deps = sorted(internal_dependencies(source, sorted(source_files), root=pathlib.Path(".")))
        matched = _match_with_state(source, ordered_tests, state)
        testing: object = matched[0] if len(matched) == 1 else matched
        code_map[source] = {"deps": deps, "layer": layer_rank[source], "testing": testing}
    return code_map


def main() -> None:
    py_files: list[str] = []
    for root, dirs, files in os.walk("src"):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        py_files.extend(os.path.join(root, filename) for filename in sorted(files) if filename.endswith(".py"))
    sources = sorted(path for path in py_files if not path.endswith("__init__.py"))
    test_files = lean_check._repository_test_files()

    code_map = build_code_map(sources, test_files, LAYER_RANK)

    docs_path = pathlib.Path("docs/code_map.json")
    if not docs_path.parent.exists():
        docs_path.parent.mkdir(parents=True, exist_ok=True)
    with open(docs_path, "w", encoding="utf-8") as handle:
        json.dump(code_map, handle, indent=2, sort_keys=True)
        handle.write("\n")
    sys.stdout.write(f"regenerated docs/code_map.json with {len(code_map)} canonical sources\n")


if __name__ == "__main__":
    main()
