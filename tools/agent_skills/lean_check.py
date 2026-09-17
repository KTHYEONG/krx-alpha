#!/usr/bin/env python3
"""Smart Selective Lean Check: Fast, token-efficient static checks and tests."""

from __future__ import annotations

import argparse
import ast
import contextlib
import functools
import json
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any

JsonDiag = dict[str, Any]

if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())


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


def _fail_exit_many(phase: str, header: str, diags: list[JsonDiag]) -> None:
    print(header)
    for d in diags:
        print(f"FAIL | {d.get('error', '')}")
    print(_emit_json("FAIL", phase, diags), file=sys.stderr)
    sys.exit(1)


def _fail_exit(phase: str, msg: str, diag: JsonDiag) -> None:
    print(msg)
    print(_emit_json("FAIL", phase, [diag]), file=sys.stderr)
    sys.exit(1)


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


def _repo_relative(path: str) -> str:
    if not os.path.isabs(path):
        return path
    try:
        return os.path.relpath(path, os.getcwd())
    except ValueError:
        return path


# ---------------------------------------------------------------------------
# AST Test-to-Source Matching (Used by both lean_check and gen_code_map)
# ---------------------------------------------------------------------------


@functools.cache
def _repository_test_files() -> list[str]:
    """Return test modules in deterministic order for semantic source matching."""
    test_files: list[str] = []
    for root, _dirs, files in os.walk("tests"):
        test_files.extend(
            os.path.join(root, filename)
            for filename in sorted(files)
            if filename.startswith("test_") and filename.endswith(".py")
        )
    return sorted(test_files)


@functools.cache
def _load_test_ast(test_file: str) -> ast.AST | None:
    """Parse a test file once for repeated semantic source checks."""
    try:
        with open(test_file, encoding="utf-8") as handle:
            return ast.parse(handle.read(), filename=test_file)
    except (OSError, SyntaxError):
        return None


@functools.cache
def _imported_source_modules(test_file: str) -> frozenset[str]:
    """Return imported module paths from a cached test AST."""
    tree = _load_test_ast(test_file)
    if tree is None:
        return frozenset()
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
            modules.update(f"{node.module}.{alias.name}" for alias in node.names if alias.name != "*")
    return frozenset(modules)


def _test_references_source(test_file: str, source_file: str) -> bool:
    """Match a test to a source module through its imports."""
    source_module = source_file[:-3].replace("/", ".")
    return source_module in _imported_source_modules(test_file)


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
    # Check modified src/ files only (allow arbitrary test fixtures if needed)
    src_files = [
        f
        for f in py_files
        if (_repo_relative(f).startswith("src/") or "/src/" in f or f.startswith("src/")) and os.path.isfile(f)
    ]

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
                    break  # report first leak on this line
    return diags


# ---------------------------------------------------------------------------
# Spec Parsing & Compliance (Lightweight Compatibility Layer)
# ---------------------------------------------------------------------------


def _extract_tests_from_spec(spec_path: str) -> list[str]:
    """Lightweight extraction of target test files from spec file."""
    if not os.path.isfile(spec_path):
        return []
    try:
        with open(spec_path, encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except OSError:
        return []

    if spec_path.endswith(".json"):
        with contextlib.suppress(Exception):
            data = json.loads(content)
            tests: list[str] = []
            for sc in data.get("scenarios", []) or data.get("tests", []):
                ttf = _repo_relative(sc.get("target_test_file", ""))
                if ttf and os.path.exists(ttf):
                    tests.append(ttf)
            return tests

    # Markdown spec: extract ## Test Suite: <path>
    matches = re.findall(r"(?m)^##\s+Test\s+Suite:\s*`?([^\n`]+)`?", content)
    return [m.strip() for m in matches if os.path.exists(m.strip())]


def _check_spec_compliance(spec_path: str, pre_impl: bool = False) -> tuple[int, list[JsonDiag]]:
    """Lightweight compatibility stub: validates spec file existence and target directories."""
    if not os.path.isfile(spec_path):
        return 1, [{"file": spec_path, "line": 0, "error": f"Spec file not found: {spec_path}", "fix_hint": ""}]

    if pre_impl:
        # Check target directories referenced in spec
        try:
            with open(spec_path, encoding="utf-8", errors="ignore") as f:
                content = f.read()
            targets = re.findall(r"(?m)^##\s+Target:\s*`?([^\n`]+)`?", content)
            for t in targets:
                target_path = t.strip()
                parent_dir = os.path.dirname(target_path)
                if parent_dir and not os.path.exists(parent_dir):
                    return 1, [
                        {
                            "file": target_path,
                            "line": 0,
                            "error": f"Target parent directory not found: {parent_dir}",
                            "fix_hint": f"Ensure valid directory path for {target_path}",
                        }
                    ]
        except Exception as e:
            return 1, [{"file": spec_path, "line": 0, "error": f"Spec check error: {e}", "fix_hint": ""}]

    return 0, []


# ---------------------------------------------------------------------------
# Test Discovery & Change Impact
# ---------------------------------------------------------------------------


def _analyze_impact_level(py_files: list[str]) -> tuple[int, str]:
    """Analyze change scope and return (impact_level, reason)."""
    if not py_files:
        return 1, "No python files modified"

    core_keywords = ("config", "base", "core", "schema", "contract")
    is_core_modified = any(any(kw in f.lower() for kw in core_keywords) for f in py_files)
    if is_core_modified or len(py_files) >= 5:
        return 3, "Core module or large multi-file change detected"

    src_files = [f for f in py_files if f.startswith("src/")]
    if not src_files:
        return 1, "Only test or tool files modified"

    return 1, "Standard module change"


def _find_test_files(py_files: list[str], impact_level: int = 1) -> list[str]:
    """Find relevant pytest files for modified python files."""
    test_files = [f for f in py_files if f.startswith("tests/") or "test_" in f]
    source_files = [f for f in py_files if not (f.startswith("tests/") or "test_" in f)]
    repository_files = _repository_test_files()

    for sf in source_files:
        if sf.startswith("src/") and not sf.endswith("__init__.py"):
            parts = sf.split("/")
            module_name = parts[-1]
            test_name = f"test_{module_name}"
            found_direct = False
            for category in ["unit", "integration", "e2e", "contract"]:
                sub_path = "/".join(parts[1:-1])
                td = f"tests/{category}/{sub_path}" if sub_path else f"tests/{category}"
                tp = f"{td}/{test_name}"
                if tp in test_files:
                    found_direct = True
                    break
                if os.path.exists(tp):
                    test_files.append(tp)
                    found_direct = True
                    break

            if not found_direct or impact_level >= 2:
                for tp in repository_files:
                    if tp not in test_files and _test_references_source(tp, sf):
                        test_files.append(tp)
    return test_files


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
# CLI Entry Point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Smart Selective Lean Check.")
    parser.add_argument("--files", nargs="*", default=[])
    parser.add_argument("--spec", default=None, help="Path to spec file")
    parser.add_argument("--skip-lint", action="store_true", help="Skip Ruff linting")
    parser.add_argument("--skip-mypy", action="store_true", help="Skip Mypy static check")
    parser.add_argument("--fast", action="store_true", help="Skip pytest and run static checks only")
    parser.add_argument("--pre-impl", action="store_true", help="Pre-implementation spec validation")
    parser.add_argument("--deselect", nargs="*", default=[], help="Pytest node ids to deselect")
    parser.add_argument("--pytest-timeout", type=int, default=None, help="Seconds for pytest step")
    parser.add_argument("--test-timeout", type=int, default=120, help="Per-test wall-clock limit in seconds")
    parser.add_argument("--no-cov", action="store_true", help="Disable diff-coverage gate")
    parser.add_argument("--no-xdist", action="store_true", help="Force serial pytest execution")
    args = parser.parse_args()

    # 1. Pre-implementation check
    if args.pre_impl:
        if not args.spec:
            print("FAIL | --pre-impl requires --spec")
            sys.exit(2)
        ec, diags = _check_spec_compliance(args.spec, pre_impl=True)
        if ec != 0:
            _fail_exit_many("spec-compliance", "FAIL | Spec pre-impl validation failed", diags)
        print("PASS | Spec validation verified (pre-impl)")
        print(_emit_json("PASS", "spec-compliance", []), file=sys.stderr)
        sys.exit(0)

    # 2. File discovery from git if not explicitly passed
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

    # 3. Regenerate code map self-healing
    try:
        from tools.agent_skills import gen_code_map

        gen_code_map.main()
    except Exception as e:
        print(f"INFO | code_map self-heal skipped: {e}")

    # 4. Scaffolding Leak Guard: Stop spec metadata from entering production code
    scaffolding_diags = _check_scaffolding_leaks(py_files)
    if scaffolding_diags:
        _fail_exit_many(
            "scaffolding-guard",
            f"FAIL | Scaffolding Leak: {len(scaffolding_diags)} temporary spec/recipe artifact(s) found in code",
            scaffolding_diags,
        )

    # 5. Test Discovery
    impact_level, impact_reason = _analyze_impact_level(py_files)
    print(f"INFO | Impact Level: {impact_level} ({impact_reason})")
    test_files = _find_test_files(py_files, impact_level=impact_level)

    if args.spec:
        spec_tests = _extract_tests_from_spec(args.spec)
        for st in spec_tests:
            if st not in test_files:
                test_files.append(st)

    # 6. Parallel Static Checks (Ruff, Mypy)
    def check_ruff_task() -> tuple[str, int, list[JsonDiag], str]:
        if args.skip_lint or not py_files:
            return "ruff", 0, [], ""
        ruff_res = run_cmd(["uv", "run", "ruff", "check", *py_files, "--quiet"])
        if ruff_res.returncode != 0:
            out_sliced = "\n".join((ruff_res.stdout or ruff_res.stderr).strip().splitlines()[:10])
            d = {"file": py_files[0], "line": 0, "error": out_sliced, "fix_hint": "Fix ruff lint errors"}
            return "ruff", 1, [d], "FAIL | Ruff Lint Failed"
        return "ruff", 0, [], ""

    def check_mypy_task() -> tuple[str, int, list[JsonDiag], str]:
        if args.skip_mypy or not py_files:
            return "mypy", 0, [], ""
        target_mypy = [f for f in py_files if f.startswith("src/")] or py_files
        mypy_res = run_cmd(["uv", "run", "mypy", *target_mypy, "--ignore-missing-imports"])
        if mypy_res.returncode != 0:
            out_sliced = "\n".join((mypy_res.stdout or mypy_res.stderr).strip().splitlines()[:10])
            d = {"file": target_mypy[0], "line": 0, "error": out_sliced, "fix_hint": "Fix mypy type errors"}
            return "mypy", 1, [d], "FAIL | Mypy Type Check Failed"
        return "mypy", 0, [], ""

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_ruff = executor.submit(check_ruff_task)
        f_mypy = executor.submit(check_mypy_task)

        for f in [f_ruff, f_mypy]:
            phase, code, diags, msg = f.result()
            if code != 0:
                if len(diags) > 1:
                    _fail_exit_many(phase, msg, diags)
                else:
                    _fail_exit(phase, msg, diags[0] if diags else {})

    if args.fast:
        print("PASS | Fast Check Passed (Scaffolding, Ruff, Mypy verified)")
        print(_emit_json("PASS", "fast-check", [], None), file=sys.stderr)
        return

    # 7. Pytest & Diff Coverage
    if not test_files:
        print("PASS | Lint & Type check passed (no tests to run)")
        print(_emit_json("PASS", "all", [], None), file=sys.stderr)
        return

    deselect_args = [f"--deselect={node}" for node in args.deselect]
    timeout_args = [f"--timeout={args.test_timeout}", "--timeout-method=thread"] if args.test_timeout > 0 else []
    xdist_args = ["-p", "no:cacheprovider", "-n", "0"] if args.no_xdist else []
    src_files = [f for f in py_files if f.startswith("src/")]
    cov_json_path = "tmp/lean_check_coverage.json"
    cov_args: list[str] = []

    if src_files and not args.no_cov:
        os.makedirs("tmp", exist_ok=True)
        with contextlib.suppress(OSError):
            os.remove(cov_json_path)
        cov_args = ["--cov=src", f"--cov-report=json:{cov_json_path}"]

    core_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        "not slow",
        *test_files,
        *deselect_args,
        *timeout_args,
        *xdist_args,
        *cov_args,
        "-q",
        "--tb=line",
    ]
    pytest_timeout = args.pytest_timeout or max(120, min(600, 120 * len(test_files)))
    pt_res = run_cmd(core_cmd, timeout=pytest_timeout)

    if pt_res.returncode == 124 and not args.no_xdist:
        print("INFO | pytest timed out under xdist; retrying serially with -n0")
        serial_cmd = [*core_cmd, "-p", "no:cacheprovider", "-n", "0"]
        pt_res = run_cmd(serial_cmd, timeout=pytest_timeout)

    if pt_res.returncode == 0:
        cov_diags, cov_pct = _check_diff_coverage(src_files, cov_json_path) if cov_args else ([], None)
        if cov_diags:
            _fail_exit_many(
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
        d = {"file": "", "line": 0, "error": cause_sliced, "fix_hint": "Fix failing pytest assertions"}
        _fail_exit("pytest", f"FAIL | Pytest Failed: {cause_sliced}", d)


if __name__ == "__main__":
    main()
