#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import pathlib
import os
import sys

if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from tools.agent_skills import lean_check  # noqa: E402


def _module_layer(source_file: str) -> int | None:
    """Return the LAYER_RANK layer for ``source_file`` (None when unlisted)."""
    from tests.architecture.layers import LAYER_RANK

    return LAYER_RANK.get(source_file)


def _module_deps(source_file: str, known_files: set[str]) -> list[str]:
    """Return sorted internal direct dependencies of ``source_file``."""
    try:
        tree = ast.parse(pathlib.Path(source_file).read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return []
    dotted: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("src"):
            dotted.add(node.module)
        elif isinstance(node, ast.Import):
            dotted.update(alias.name for alias in node.names if alias.name.startswith("src"))
    deps: set[str] = set()
    for mod in dotted:
        parts = mod.split(".")
        for i in range(len(parts), 1, -1):
            candidate = "/".join(parts[:i]) + ".py"
            if candidate in known_files and candidate != source_file:
                deps.add(candidate)
                break
    return sorted(deps)


def _matching_tests(source_file: str, test_files: list[str]) -> list[str]:
    """Return every repository test that covers ``source_file``.

    Exact mirrored ``tests/<category>/<dir>/test_<module>.py`` paths are the fast
    path; otherwise the lean-check AST semantic reference matcher is reused so
    feature-named CLI/workflow tests remain linked.
    """
    parts = source_file.split("/")
    module_name = parts[-1]
    test_name = f"test_{module_name}"
    exact = {
        f"tests/{category}/{'/'.join(parts[1:-1])}/{test_name}" if parts[1:-1]
        else f"tests/{category}/{test_name}"
        for category in ("unit", "integration", "e2e")
    }
    matched = [tp for tp in test_files if tp in exact]
    if matched:
        return matched
    return [tp for tp in test_files if lean_check._test_references_source(tp, source_file)]


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

    code_map: dict[str, object] = {}
    known = set(py_files)
    for source_file in py_files:
        if source_file.endswith("__init__.py"):
            continue
        matched = _matching_tests(source_file, test_files)
        entry: dict[str, object] = {}
        if matched:
            entry["testing"] = matched[0] if len(matched) == 1 else matched
        layer = _module_layer(source_file)
        if layer is not None:
            entry["layer"] = layer
        deps = _module_deps(source_file, known)
        if deps:
            entry["deps"] = deps
        code_map[source_file] = entry

    # Tolerate absent active code_map.json; do not recreate archived records under docs/
    # Only active src files are mapped; legacy sources remain in legacy/docs/code_map.json
    import contextlib
    docs_path = pathlib.Path("docs/code_map.json")
    # If active docs/code_map.json is absent, still generate active-only map without archived entries
    with contextlib.suppress(FileNotFoundError):
        if not docs_path.parent.exists():
            docs_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(docs_path, "w", encoding="utf-8") as handle:
            json.dump(code_map, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        print(f"regenerated docs/code_map.json with {len(code_map)} canonical sources")
    except FileNotFoundError:
        print("active docs/code_map.json absent, skipped regeneration (archived map remains in legacy/docs)")


if __name__ == "__main__":
    main()
