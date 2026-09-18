#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import os
import sys
from typing import Any

if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from tools.agent_skills import lean_check  # noqa: E402


def _match_with_state(source_file: str, test_files: list[str], state: dict[str, Any]) -> list[str]:
    """Map one source module against the generation's shared selection state."""
    if source_file not in state["source_set"]:
        raise ValueError(f"{source_file}: selection requires a current source path")
    parts = source_file.split("/")
    module_name = parts[-1]
    test_name = f"test_{module_name}"
    exact = {
        f"tests/{category}/{'/'.join(parts[1:-1])}/{test_name}" if parts[1:-1]
        else f"tests/{category}/{test_name}"
        for category in ("unit", "integration", "e2e")
    }
    matched = [tp for tp in test_files if tp in exact]
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


def main() -> None:
    py_files: list[str] = []
    for root, dirs, files in os.walk("src"):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        py_files.extend(
            os.path.join(root, filename)
            for filename in sorted(files)
            if filename.endswith(".py")
        )
    py_files = sorted(py_files)
    test_files = lean_check._repository_test_files()
    state = lean_check._build_selection_state()

    docs_path = pathlib.Path("docs/code_map.json")
    existing_map: dict[str, object] = {}
    if docs_path.is_file():
        with docs_path.open(encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, dict):
            raise ValueError("docs/code_map.json: expected an object at the root")
        existing_map = loaded

    code_map: dict[str, object] = {}
    for source_file in py_files:
        if source_file.endswith("__init__.py"):
            continue
        matched = _match_with_state(source_file, test_files, state)
        previous = existing_map.get(source_file)
        entry = dict(previous) if isinstance(previous, dict) else {}
        entry.pop("testing", None)
        if matched:
            entry["testing"] = matched[0] if len(matched) == 1 else matched
        code_map[source_file] = entry

    # Tolerate absent active code_map.json; do not recreate archived records under docs/
    # Only active src files are mapped; legacy sources remain in legacy/docs/code_map.json
    import contextlib
    # If active docs/code_map.json is absent, still generate active-only map without archived entries
    with contextlib.suppress(FileNotFoundError):
        if not docs_path.parent.exists():
            docs_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(docs_path, "w", encoding="utf-8") as handle:
            json.dump(code_map, handle, indent=2, sort_keys=True)
            handle.write("\n")
        print(f"regenerated docs/code_map.json with {len(code_map)} canonical sources")
    except FileNotFoundError:
        print("active docs/code_map.json absent, skipped regeneration (archived map remains in legacy/docs)")


if __name__ == "__main__":
    main()
