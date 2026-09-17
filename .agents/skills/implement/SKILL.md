---
name: implement
description: Implement an approved spec mechanically with focused TDD and integration verification.
---

# Implement Protocol

Fast-execution protocol for mechanical code implementation based strictly on frozen specs (`_spec.md` or `contract.json`).

## Execution Principles

Operate as a deterministic translator turning the specification into code and passing tests:
1. Append the test suite directly into target test files.
2. Implement clean production logic satisfying the spec's invariants and docstring.
3. Wire the caller at the specified anchor.
4. Verify with `lean_check.py`.

## Directives

1. **Scaffolding Exclusion**:
   - Production code must contain only finalized code and docstrings.
   - Do not paste or leave temporary spec directives, step numbers, or placeholder comments in code or docstrings.

2. **Fidelity & Anti-Defensive Sprawl**:
   - Treat the spec as truth. Do not invent unrequested parameters or speculative abstraction layers.
   - Do not leave stubs (`pass`, `...`, `NotImplementedError`, placeholder returns).
   - Avoid speculative `try-except` blocks or unrequested null checks that are not required by spec invariants.

3. **Phased Workflow**:
   - **Phase A (Tests - Red)**:
     - Place the test functions from `## Test Suite: <target_test_file>`.
     - Run: `uv run pytest <target_test_file> -q --tb=short` and confirm failure.
   - **Phase B (Logic & Wiring - Green)**:
     - Implement the target functions to satisfy invariants and pass tests.
     - Apply wiring snippet at `- Anchor: <anchor>`.
     - Run: `uv run ruff check <target_file> <caller_file>`.
   - **Phase C (Verification)**:
     - Run: `uv run python tools/agent_skills/lean_check.py --spec <spec_file>`
     - Confirm all checks pass with 100% diff coverage.

4. **Diff Coverage Resolution**:
   - If diff coverage reports untested lines:
     1. Simplify or remove unrequested defensive branches.
     2. If required domain logic lacks coverage, add the missing scenario.

## Output

### 🔨 [IMPLEMENT] <Task Title>

- **Status**: ✅ COMPLETE (or ❌ ESCALATED)
- **Modified**: <Count> files
- **Verification**:
  - 🧪 Pytest: <Passed>/<Total> passed
  - 🧹 Ruff / Mypy: <PASS/FAIL>
  - 🛡️ Scaffolding & Diff Coverage: <PASS/FAIL>
