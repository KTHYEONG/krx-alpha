#!/usr/bin/env python3
"""Smart Selective Lean Check: Fast, token-efficient mechanical audit gate."""

from __future__ import annotations

import argparse
import ast
import contextlib
import json
import os
import pathlib
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

JsonDiag = dict[str, Any]

if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

from tools.agent_skills.dependency_graph import internal_dependencies, repository_source_files  # noqa: E402


def _emit_json(
    status: str,
    phase: str,
    diagnostics: list[JsonDiag],
    coverage: int | None = None,
) -> str:
    return json.dumps(
        {
            "status": status,
            "phase": phase,
            "exit_code": 0 if status == "PASS" else 1,
            "coverage": coverage,
            "diagnostics": diagnostics,
        }
    )


def _exit_with_diags(phase: str, header: str, diags: list[JsonDiag], exit_code: int = 1) -> None:
    print(header)
    for d in diags:
        err = d.get("error", "")
        if err:
            print(f"FAIL | {err}")
    print(_emit_json("FAIL", phase, diags), file=sys.stderr)
    sys.exit(exit_code)


def run_cmd(cmd: list[str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    # Strip unnecessary 'uv run' prefix when already running inside virtualenv
    if len(cmd) >= 3 and cmd[0] == "uv" and cmd[1] == "run" and os.environ.get("VIRTUAL_ENV"):
        cmd = cmd[2:]
    env = os.environ.copy()
    env["COVERAGE_NO_CTRACE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["POLARS_MAX_THREADS"] = "2"
    env["OMP_NUM_THREADS"] = "2"
    env["OPENBLAS_NUM_THREADS"] = "2"
    env["MKL_NUM_THREADS"] = "2"
    try:
        return subprocess.run(  # noqa: S603
            cmd, capture_output=True, text=True, shell=False, timeout=timeout, env=env
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=124,
            stdout="",
            stderr=f"Error: timed out after {timeout}s.",
        )


def _available_memory_gb() -> float:
    """Return available system RAM in gigabytes using Linux procfs or sysconf."""
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 * 1024)
    except (OSError, ValueError):
        pass
    try:
        return (os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_AVPHYS_PAGES")) / (1024**3)
    except (ValueError, OSError, AttributeError):
        return 8.0


# ---------------------------------------------------------------------------
# Scaffolding Leak Guard: Block temporary spec/recipe text from production code
# ---------------------------------------------------------------------------

_SCAFFOLDING_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"\[(?:STEP-BY-STEP\s+)?RECIPE", re.IGNORECASE),
        "Recipe directive leaked into code/docstring",
    ),
    (
        re.compile(r"\[ALGORITHM\s+RECIPE\]", re.IGNORECASE),
        "Algorithm recipe placeholder leaked into code",
    ),
    (
        re.compile(r"^\s*(?:#|/{2})?\s*Step\s+\d+\.\s+[A-Z]", re.MULTILINE),
        "Spec Step-by-step numbering leaked into code/comment",
    ),
    (
        re.compile(r"\b(?:TODO|FIXME)\b\s*:", re.IGNORECASE),
        "TODO/FIXME placeholder found in modified code",
    ),
)


def _check_scaffolding_leaks(py_files: list[str]) -> list[JsonDiag]:
    """Verify that no temporary spec recipes or placeholders remain in production code."""
    diags: list[JsonDiag] = []
    src_files = [f for f in py_files if (f.startswith("src/") or "/src/" in f) and os.path.isfile(f)]

    for fpath in src_files:
        try:
            with open(fpath, encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
        except OSError:
            continue

        for idx, line in enumerate(lines, start=1):
            for pat, desc in _SCAFFOLDING_PATTERNS:
                if pat.search(line):
                    diags.append(
                        {
                            "file": fpath,
                            "line": idx,
                            "error": f"Scaffolding Leak: {desc} -> '{line.strip()}'",
                            "fix_hint": "Remove temporary spec/recipe directives and write clean production code/docstring",
                        }
                    )
                    break
    return diags


# ---------------------------------------------------------------------------
# Affected-Test Selection (deterministic semantic matching over resolved graph)
# ---------------------------------------------------------------------------

_ARCH_SUITE = "tests/architecture/test_layering.py"


def _repository_test_files() -> list[str]:
    """Discover active test modules for deterministic affected-suite selection.

    Returns:
        Sorted repository-relative POSIX paths matching tests/**/test_*.py.

    Raises:
        FileNotFoundError: If the tests directory is absent.
        OSError: If discovery cannot complete.
    """
    tests_dir = pathlib.Path("tests")
    if not tests_dir.is_dir():
        raise FileNotFoundError(f"{tests_dir.as_posix()}: tests directory is absent")
    try:
        paths = {
            p.as_posix()
            for p in tests_dir.rglob("test_*.py")
            if "__pycache__" not in p.parts and p.name not in {"__init__.py", "conftest.py"}
        }
    except OSError as exc:
        raise OSError(f"{tests_dir.as_posix()}: test discovery cannot complete: {exc}") from exc
    return sorted(paths)


def _resolve_literal_module(literal: str, source_set: set[str]) -> str | None:
    parts = literal.split(".")
    for width in range(len(parts), 0, -1):
        prefix = "/".join(parts[:width])
        if f"{prefix}.py" in source_set:
            return f"{prefix}.py"
        if f"{prefix}/__init__.py" in source_set:
            return f"{prefix}/__init__.py"
    return None


def _patch_target_modules(test_file: str, source_set: set[str]) -> set[str]:
    try:
        text = pathlib.Path(test_file).read_text(encoding="utf-8")
    except OSError as exc:
        raise OSError(f"{test_file}: cannot read inspected file: {exc}") from exc
    try:
        tree = ast.parse(text, filename=test_file)
    except SyntaxError as exc:
        raise SyntaxError(f"{test_file}: invalid Python at line {exc.lineno}: {exc.msg}") from exc
    targets: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        is_target_call = False
        if isinstance(func, ast.Attribute) and func.attr in {"setattr", "delattr"}:
            is_target_call = isinstance(func.value, ast.Name) and func.value.id == "monkeypatch"
        elif isinstance(func, ast.Name) and func.id == "patch":
            is_target_call = True
        elif isinstance(func, ast.Attribute) and func.attr == "patch":
            is_target_call = True
        elif isinstance(func, ast.Attribute) and func.attr == "object":
            value = func.value
            is_target_call = (isinstance(value, ast.Name) and value.id == "patch") or (
                isinstance(value, ast.Attribute) and value.attr == "patch"
            )
        if not is_target_call or not node.args:
            continue
        first = node.args[0]
        if not isinstance(first, ast.Constant) or not isinstance(first.value, str):
            continue
        resolved = _resolve_literal_module(first.value, source_set)
        if resolved is not None:
            targets.add(resolved)
    return targets


def _build_selection_state() -> dict[str, Any]:
    """Build a fresh selection graph; the state stays local to one selection operation."""
    root = pathlib.Path(".")
    source_files = repository_source_files(root)
    repository_tests = _repository_test_files()
    source_set = set(source_files)
    graph: dict[str, set[str]] = {}
    for source in source_files:
        graph[source] = internal_dependencies(source, source_files, root=root)
    reachable: dict[str, set[str]] = {}
    for source in source_files:
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
    for test in repository_tests:
        test_deps[test] = internal_dependencies(test, source_files, root=root)
        test_patches[test] = _patch_target_modules(test, source_set)
    state: dict[str, Any] = {
        "source_files": source_files,
        "source_set": source_set,
        "test_files": repository_tests,
        "graph": graph,
        "reachable": reachable,
        "test_deps": test_deps,
        "test_patches": test_patches,
    }
    return state


def _references_with_state(state: dict[str, Any], test_file: str, source_file: str) -> bool:
    reachable: dict[str, set[str]] = state["reachable"]
    test_deps: dict[str, set[str]] = state["test_deps"]
    test_patches: dict[str, set[str]] = state["test_patches"]
    direct = test_deps.get(test_file)
    if direct is None:
        return False
    if source_file in direct:
        return True
    for dep in direct:
        if source_file in reachable.get(dep, set()):
            return True
    for patched in test_patches.get(test_file, set()):
        if patched == source_file or source_file in reachable.get(patched, set()):
            return True
    return False


def _references_ondemand(state: dict[str, Any], test_file: str, source_file: str) -> bool:
    """Evaluate one test file absent from the shared state without rebuilding the graph."""
    if not os.path.isfile(test_file):
        raise OSError(f"{test_file}: cannot read inspected test file")
    root = pathlib.Path(".")
    source_files: list[str] = state["source_files"]
    source_set: set[str] = state["source_set"]
    reachable: dict[str, set[str]] = state["reachable"]
    direct = internal_dependencies(test_file, source_files, root=root)
    if source_file in direct:
        return True
    for dep in direct:
        if source_file in reachable.get(dep, set()):
            return True
    for patched in _patch_target_modules(test_file, source_set):
        if patched == source_file or source_file in reachable.get(patched, set()):
            return True
    return False


def _test_references_source(test_file: str, source_file: str) -> bool:
    """Determine whether a test statically reaches a source module or its source callers.

    Args:
        test_file: Repository-relative test module path.
        source_file: Existing active source module path.

    Returns:
        Whether imports or literal patch targets reach the module through the source graph.

    Raises:
        SyntaxError: If a required Python file cannot be parsed.
        ValueError: If an explicit source import or source path cannot resolve.
        OSError: If a required Python file cannot be read.
    """
    state = _build_selection_state()
    source_set: set[str] = state["source_set"]
    if source_file not in source_set:
        raise ValueError(f"{source_file}: selection requires a current source path")
    test_deps: dict[str, set[str]] = state["test_deps"]
    if test_file in test_deps:
        return _references_with_state(state, test_file, source_file)
    return _references_ondemand(state, test_file, source_file)


def _mirrored_candidates(source_file: str) -> list[str]:
    rel = source_file[4:]
    parts = rel.split("/")
    test_name = f"test_{parts[-1]}"
    sub_path = "/".join(parts[:-1])
    return [
        f"tests/unit/{sub_path}/{test_name}" if sub_path else f"tests/unit/{test_name}",
        f"tests/unit/{test_name}",
        f"tests/contract/{sub_path}/{test_name}" if sub_path else f"tests/contract/{test_name}",
    ]


def _is_mirrored(source_file: str, test_file: str) -> bool:
    return test_file in _mirrored_candidates(source_file)


def _find_test_files(py_files: list[str], spec_path: str | None = None) -> list[str]:
    """Select affected suites without losing tests when source modules are extracted.

    Args:
        py_files: Changed source and explicitly selected test paths.
        spec_path: Optional spec supplying additional invariant suites.

    Returns:
        Sorted unique paths for explicit, mirrored and dependency-related suites.

    Raises:
        ValueError: If a changed source file has no discoverable or explicitly declared suite.
        SyntaxError: If dependency analysis encounters invalid Python.
        OSError: If required files cannot be read.
    """
    selected: set[str] = set()
    for candidate in py_files:
        if candidate.startswith("tests/") or "test_" in candidate:
            selected.add(candidate)
    changed_sources = [candidate for candidate in py_files if candidate.startswith("src/")]

    # Spec-declared suites are unioned with discovered tests.
    if spec_path and os.path.isfile(spec_path):
        with contextlib.suppress(OSError):
            with open(spec_path, encoding="utf-8", errors="ignore") as handle:
                content = handle.read()
            matches = re.findall(
                r"(?m)^##\s+(?:Test\s+Suite|Invariant\s+Scenarios):\s*`?([^\n`]+)`?",
                content,
            )
            for match in matches:
                suite = match.strip().strip("`").strip()
                if os.path.isfile(suite):
                    selected.add(suite)

    if not changed_sources:
        return sorted(selected)

    repository_tests = _repository_test_files()
    state = _build_selection_state()
    source_set: set[str] = state["source_set"]
    for changed in changed_sources:
        if changed not in source_set:
            raise ValueError(
                f"{changed}: selection requires a current source path "
                "or explicit deletion handling in a separate change"
            )
    if os.path.isfile(_ARCH_SUITE):
        selected.add(_ARCH_SUITE)

    # Mirrored unit/contract path conventions are preserved.
    for changed in changed_sources:
        if changed.endswith("__init__.py"):
            continue
        for candidate in _mirrored_candidates(changed):
            if candidate in selected:
                break
            if os.path.isfile(candidate):
                selected.add(candidate)
                break

    # Semantic matches from every active test module; never stop at a mirrored match.
    for changed in changed_sources:
        if changed.endswith("__init__.py"):
            continue
        for test in repository_tests:
            if test == _ARCH_SUITE:
                continue
            if _references_with_state(state, test, changed):
                selected.add(test)

    # Fail closed: each changed non-initializer source needs a non-architecture suite.
    non_arch = {item for item in selected if item != _ARCH_SUITE}
    for changed in changed_sources:
        if changed.endswith("__init__.py"):
            continue
        covered = any(
            _is_mirrored(changed, item) or _references_with_state(state, item, changed) for item in non_arch
        )
        if not covered:
            raise ValueError(
                f"{changed}: no discoverable or explicitly declared non-architecture suite covers this source"
            )

    return sorted(selected)


def _check_pre_impl_spec(spec_path: str) -> tuple[int, list[JsonDiag]]:
    """Validate spec blueprint paths, targets, caller files, and anchors before implementation."""
    diags: list[JsonDiag] = []
    if not os.path.isfile(spec_path):
        return 1, [
            {
                "file": spec_path,
                "line": 0,
                "error": f"Spec file not found: {spec_path}",
                "fix_hint": "Check spec file path",
            }
        ]

    try:
        with open(spec_path, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except OSError as e:
        return 1, [
            {
                "file": spec_path,
                "line": 0,
                "error": f"Cannot read spec file: {e}",
                "fix_hint": "Check file permissions",
            }
        ]

    current_caller: str | None = None
    target_found = False

    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()

        # Check Target
        m_target = re.match(r"^##\s+Target:\s*`?([^\n`]+)`?", stripped)
        if m_target:
            target_file = m_target.group(1).strip().strip("`").strip()
            target_found = True
            parent = os.path.dirname(target_file)
            if parent and not os.path.isdir(parent):
                diags.append(
                    {
                        "file": spec_path,
                        "line": idx,
                        "error": f"Target parent directory does not exist: '{parent}' for target '{target_file}'",
                        "fix_hint": f"Create directory {parent} or fix path in spec",
                    }
                )

        # Check Wiring caller file
        m_wiring = re.match(r"^##\s+Wiring:\s*`?([^\n`]+)`?", stripped)
        if m_wiring:
            current_caller = m_wiring.group(1).strip().strip("`").strip()
            if not os.path.isfile(current_caller):
                diags.append(
                    {
                        "file": spec_path,
                        "line": idx,
                        "error": f"Wiring caller file does not exist: '{current_caller}'",
                        "fix_hint": f"Verify caller file path in {spec_path}",
                    }
                )

        # Check Anchor in caller file
        m_anchor = re.match(r"^-\s*(?:Anchor|anchor):\s*`?([^\n`]+)`?", stripped)
        if m_anchor and current_caller and os.path.isfile(current_caller):
            anchor = m_anchor.group(1).strip().strip("`").strip()
            try:
                with open(current_caller, encoding="utf-8", errors="ignore") as cf:
                    caller_content = cf.read()
                if anchor not in caller_content:
                    diags.append(
                        {
                            "file": current_caller,
                            "line": 0,
                            "error": f"Wiring anchor '{anchor}' not found in caller file '{current_caller}'",
                            "fix_hint": f"Ensure anchor '{anchor}' matches an existing symbol or line in {current_caller}",
                        }
                    )
            except OSError:
                pass

        # Check Invariant Scenarios / Test Suite test file
        m_test = re.match(r"^##\s+(?:Invariant\s+Scenarios|Test\s+Suite):\s*`?([^\n`]+)`?", stripped)
        if m_test:
            test_file = m_test.group(1).strip().strip("`").strip()
            parent = os.path.dirname(test_file)
            if parent and not os.path.isdir(parent):
                diags.append(
                    {
                        "file": spec_path,
                        "line": idx,
                        "error": f"Test suite directory does not exist: '{parent}' for '{test_file}'",
                        "fix_hint": f"Create directory {parent} or fix path in spec",
                    }
                )

    if not target_found:
        diags.append(
            {
                "file": spec_path,
                "line": 0,
                "error": "Spec missing mandatory '## Target: <path>' section",
                "fix_hint": "Add '## Target: <relative_path>' section to spec",
            }
        )

    return (1 if diags else 0), diags


# ---------------------------------------------------------------------------
# Diff Coverage Gate
# ---------------------------------------------------------------------------


def _git_diff_added_lines(file: str) -> set[int] | None:
    """1-indexed line numbers this working-tree diff adds to `file`."""
    status_res = subprocess.run(  # noqa: S603
        ["git", "status", "--porcelain", "--", file],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if status_res.stdout.strip().startswith("??"):
        return None
    diff_res = subprocess.run(  # noqa: S603
        ["git", "diff", "--unified=0", "HEAD", "--", file],
        capture_output=True,
        text=True,
        timeout=10,
    )
    added: set[int] = set()
    cur_line = 0
    for line in diff_res.stdout.splitlines():
        if line.startswith("@@"):
            m = re.search(r"\+(\d+)", line)
            if m:
                cur_line = int(m.group(1))
            continue
        if line.startswith("+") and not line.startswith("+++"):
            added.add(cur_line)
            cur_line += 1
        elif not line.startswith("-"):
            cur_line += 1
    return added


def _check_diff_coverage(src_files: list[str], cov_json_path: str) -> tuple[list[JsonDiag], int | None]:
    """Verify that every line added to touched src/ files is executed by tests."""
    if not os.path.exists(cov_json_path):
        return [], None
    try:
        with open(cov_json_path, encoding="utf-8") as f:
            cov_data = json.load(f)
    except Exception:
        return [], None

    files_data = cov_data.get("files", {})
    diags: list[JsonDiag] = []
    total_added = 0
    total_covered = 0
    for sf in src_files:
        entry = files_data.get(sf) or files_data.get(sf.replace("/", os.sep))
        if not entry:
            continue
        missing = set(entry.get("missing_lines", []))
        executed = set(entry.get("executed_lines", []))
        added = _git_diff_added_lines(sf)
        if added is None:
            added = executed | missing
        added &= executed | missing
        if not added:
            continue
        total_added += len(added)
        total_covered += len(added - missing)
        uncovered_new = sorted(added & missing)
        if uncovered_new:
            shown = uncovered_new[:10]
            diags.append(
                {
                    "file": sf,
                    "line": shown[0],
                    "error": f"{len(uncovered_new)} newly-added line(s) not executed by tests: {shown}",
                    "fix_hint": "Add or update tests exercising these lines",
                }
            )
    pct = round(100 * total_covered / total_added) if total_added else None
    return diags, pct


# ---------------------------------------------------------------------------
# Main CLI Entry Point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Smart Selective Lean Check: Tier 1 Mechanical Gate.")
    parser.add_argument("--files", nargs="*", default=[], help="Explicit files to check")
    parser.add_argument("--spec", default=None, help="Path to markdown/JSON spec file")
    parser.add_argument("--fast", action="store_true", help="Run static checks only (scaffolding, ruff, mypy)")
    parser.add_argument("--skip-lint", action="store_true", help="Skip Ruff linting")
    parser.add_argument("--skip-mypy", action="store_true", help="Skip Mypy static check")
    parser.add_argument("--no-cov", action="store_true", help="Disable diff-coverage gate")
    parser.add_argument("--no-xdist", action="store_true", help="Force serial pytest execution (-n 0)")
    parser.add_argument("--timeout", type=int, default=None, help="Pytest timeout in seconds")
    parser.add_argument(
        "--pre-impl",
        action="store_true",
        help="Validate spec blueprint paths and wiring anchors before implementation",
    )
    args = parser.parse_args()

    # 0. Pre-implementation Spec Blueprint Gate
    if args.pre_impl:
        if not args.spec:
            _exit_with_diags(
                "pre-impl",
                "FAIL | --pre-impl requires --spec <spec_file>",
                [
                    {
                        "file": "",
                        "line": 0,
                        "error": "--pre-impl requires --spec argument",
                        "fix_hint": "Pass --spec docs/specs/<feature>_spec.md",
                    }
                ],
            )
        code, diags = _check_pre_impl_spec(args.spec)
        if code != 0:
            _exit_with_diags(
                "pre-impl",
                f"FAIL | Pre-impl spec validation failed ({len(diags)} error(s))",
                diags,
            )
        print("PASS | Spec blueprint paths and wiring anchors verified (pre-impl)")
        print(_emit_json("PASS", "pre-impl", []), file=sys.stderr)
        return

    # 1. File discovery from git if not explicitly passed
    if not args.files:
        try:
            diff_res = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            git_files = [
                line[3:].strip()
                for line in diff_res.stdout.splitlines()
                if "D" not in line[:2]
                and line[3:].strip().endswith(".py")
                and not line[3:].strip().startswith("tools/")
                and not line[3:].strip().endswith("conftest.py")
                and os.path.exists(line[3:].strip())
            ]
            args.files = git_files
        except Exception:
            args.files = []

    py_files = [f for f in args.files if f.endswith(".py")]
    if not py_files:
        print("ALLCHECKS:PASS | No modified .py files detected")
        sys.exit(0)

    # 2. Scaffolding Leak Guard
    scaffolding_diags = _check_scaffolding_leaks(py_files)
    if scaffolding_diags:
        _exit_with_diags(
            "scaffolding-guard",
            f"FAIL | Scaffolding Leak: {len(scaffolding_diags)} temporary spec/recipe artifact(s) found in code",
            scaffolding_diags,
        )

    # 3. Parallel Static Checks (Ruff, Mypy)
    def check_ruff() -> tuple[str, int, list[JsonDiag], str]:
        if args.skip_lint or not py_files:
            return "ruff", 0, [], ""
        res = run_cmd(["uv", "run", "ruff", "check", *py_files, "--quiet"])
        if res.returncode != 0:
            out = "\n".join((res.stdout or res.stderr).strip().splitlines()[:10])
            return "ruff", 1, [{"file": py_files[0], "line": 0, "error": out, "fix_hint": "Fix ruff lint errors"}], "FAIL | Ruff Lint Failed"
        return "ruff", 0, [], ""

    def check_mypy() -> tuple[str, int, list[JsonDiag], str]:
        if args.skip_mypy or not py_files:
            return "mypy", 0, [], ""
        target_mypy = [f for f in py_files if f.startswith("src/")] or py_files
        res = run_cmd(["uv", "run", "mypy", *target_mypy, "--ignore-missing-imports"])
        if res.returncode != 0:
            out = "\n".join((res.stdout or res.stderr).strip().splitlines()[:10])
            return "mypy", 1, [{"file": target_mypy[0], "line": 0, "error": out, "fix_hint": "Fix mypy type errors"}], "FAIL | Mypy Type Check Failed"
        return "mypy", 0, [], ""

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_ruff = executor.submit(check_ruff)
        f_mypy = executor.submit(check_mypy)
        for f in [f_ruff, f_mypy]:
            phase, code, diags, msg = f.result()
            if code != 0:
                _exit_with_diags(phase, msg, diags)

    if args.fast:
        print("PASS | Fast Check Passed (Scaffolding, Ruff, Mypy verified)")
        print(_emit_json("PASS", "fast-check", [], None), file=sys.stderr)
        return

    # 4. Direct Test Discovery
    try:
        test_files = _find_test_files(py_files, spec_path=args.spec)
    except (OSError, SyntaxError, ValueError) as exc:
        _exit_with_diags(
            "test-selection",
            f"FAIL | Test selection failed: {exc}",
            [
                {
                    "file": "",
                    "line": 0,
                    "error": str(exc),
                    "fix_hint": "Add a mirrored or dependency-related suite for the changed source, or declare it in the spec",
                }
            ],
        )
        raise AssertionError("unreachable: selection failure always exits") from exc
    if not test_files:
        print("PASS | Lint & Type check passed (no tests to run)")
        print(_emit_json("PASS", "all", [], None), file=sys.stderr)
        return

    # 5. Smart Pytest Execution (Resource Safety Guard)
    env_workers = os.environ.get("LEAN_CHECK_WORKERS")
    avail_mem_gb = _available_memory_gb()

    if (
        args.no_xdist
        or len(test_files) <= 5
        or (env_workers and env_workers in ("0", "1"))
        or avail_mem_gb < 2.0
    ):
        xdist_args = ["-p", "no:cacheprovider", "-n", "0"]
    else:
        target_workers = int(env_workers) if env_workers and env_workers.isdigit() else 2
        worker_count = min(target_workers, os.cpu_count() or 2, len(test_files))
        xdist_args = ["-p", "no:cacheprovider", "-n", str(worker_count)]

    src_files = [f for f in py_files if f.startswith("src/")]
    cov_json_path = "tmp/lean_check_coverage.json"
    cov_args: list[str] = []

    if src_files and not args.no_cov:
        os.makedirs("tmp", exist_ok=True)
        with contextlib.suppress(OSError):
            os.remove(cov_json_path)
        pkgs = {f.split("/")[1] for f in src_files if len(f.split("/")) >= 2}
        cov_pkgs = [f"--cov=src/{p}" for p in sorted(pkgs)] if pkgs else ["--cov=src"]
        cov_args = [*cov_pkgs, f"--cov-report=json:{cov_json_path}"]

    pytest_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        "not slow",
        *test_files,
        *xdist_args,
        *cov_args,
        "-q",
        "--tb=line",
    ]
    pytest_timeout = args.timeout or max(60, min(240, 20 * len(test_files)))
    pt_res = run_cmd(pytest_cmd, timeout=pytest_timeout)

    if pt_res.returncode == 124:
        _exit_with_diags(
            "pytest-timeout",
            f"FAIL | Pytest Timed Out ({pytest_timeout}s)",
            [{
                "file": "",
                "line": 0,
                "error": f"pytest timed out after {pytest_timeout}s across {len(test_files)} file(s).",
                "fix_hint": "Use --files to scope checks, investigate slow tests, or pass --timeout with a larger value.",
            }],
        )

    if pt_res.returncode == 0:
        cov_diags, cov_pct = _check_diff_coverage(src_files, cov_json_path) if cov_args else ([], None)
        if cov_diags:
            _exit_with_diags(
                "coverage",
                f"FAIL | Diff Coverage: {len(cov_diags)} file(s) with untested new lines",
                cov_diags,
            )
        cov_suffix = f", Diff-Coverage {cov_pct}%" if cov_pct is not None else ""
        print(f"PASS | All checks passed (Scaffolding-Clean, Lint, Type, Tests{cov_suffix})")
        print(_emit_json("PASS", "all", [], cov_pct), file=sys.stderr)
    else:
        last_err = [
            line
            for line in (pt_res.stdout or "").splitlines()
            if any(x in line for x in ("FAIL", "Error", "AssertionError"))
        ]
        cause = last_err[-1] if last_err else (pt_res.stderr or "Check pytest output.").strip()
        cause_sliced = "\n".join(cause.splitlines()[:10])
        _exit_with_diags(
            "pytest",
            f"FAIL | Pytest Failed: {cause_sliced}",
            [{"file": "", "line": 0, "error": cause_sliced, "fix_hint": "Fix failing pytest assertions"}],
        )


if __name__ == "__main__":
    main()
