---
name: implement
description: Implement an approved spec mechanically with focused invariant guards and integration verification.
---

# Implement Protocol

Fast-execution protocol for mechanical code implementation based strictly on frozen specs (`_spec.md` or `contract.json`).

## Execution Principles

Operate as a deterministic translator turning the specification into code and passing tests:
1. Implement clean production logic satisfying the spec's invariants and docstring.
2. Implement targeted invariant guard tests satisfying the spec's Invariant Scenarios.
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

3. **Clean Test Naming Rule**:
   - Do NOT hardcode temporary spec/ticket IDs (e.g. `POLICY-01`, `SCENARIO-02`) into test function names.
   - Use idiomatic Pythonic names reflecting the target and behavior: `def test_<target_function>_<invariant_behavior>():`.
   - If scenario traceability is desired, add it optionally to the first line of the test docstring, not the function identifier.

4. **Direct Implementation Workflow (Invariant-Driven)**:
   - **Small Scope (≤ 1 target file)**:
     - **Phase 1 (Production Logic & Wiring)**: Implement clean logic and wire at `- Anchor: <anchor>`. Check with `uv run ruff check <target_file> <caller_file>`.
     - **Phase 2 (Invariant Guard Tests)**: Implement guard tests in `<target_test_file>` verifying Invariant Scenarios. Run `uv run pytest <target_test_file> -q`.
     - **Phase 3 (Verification & Pruning)**: Run `uv run python tools/agent_skills/lean_check.py --spec <spec_file>`.
   - **Multi-Component Scope (> 1 target file)**:
     - Execute sequentially per component unit across all $N$ targets (Unit Chaining):
       - For each Unit $i \in \{1, \dots, N\}$:
         1. Implement Unit $i$ target logic & wiring.
         2. Write Unit $i$ invariant guard tests in `<test_file_i>`.
         3. Confirm `uv run pytest <test_file_i> -q` passes before advancing to Unit $i+1$.
       - After all units pass individual verification:
         - Run `uv run python tools/agent_skills/lean_check.py --spec <spec_file>` once across the whole scope.
     - Do NOT run redundant Red-check test runs (e.g. executing pytest before production logic is written) to conserve token and process overhead.

5. **Diff Coverage Resolution (Pruning Over Bloat)**:
   - If diff coverage reports untested lines:
     1. Evaluate if it is speculative defensive code (unrequested `try-except`, unreachable branches): **Prune and delete the code**.
     2. If required domain logic lacks coverage, add the missing boundary scenario.

## Output

### 🔨 [IMPLEMENT] <Task Title>

- **Status**: ✅ COMPLETE (or ❌ ESCALATED)
- **Modified**: <Count> files
- **Verification**:
  - 🧪 Pytest: <Passed>/<Total> passed
  - 🧹 Ruff / Mypy: <PASS/FAIL>
  - 🛡️ Scaffolding & Diff Coverage: <PASS/FAIL>
