---
name: implement
description: Implement an approved spec mechanically with focused TDD and integration verification.
---

# Implement Protocol

Fast-execution protocol for mechanical code implementation based strictly on frozen specs (`_spec.md` or `contract.json`), optimized for zero-invention, low-reasoning execution.

## Low-Reasoning Execution Philosophy

Operate as a deterministic compiler translating the specification into code and passing tests.
When provided with a Markdown Blueprint (`_spec.md`), your execution is purely mechanical:
1. Translate the docstring `[STEP-BY-STEP RECIPE FOR IMPLEMENTER]` directly into clean Python code without guessing.
2. Paste the provided Test Suite directly into the target test files.
3. Wire the caller at the specified Anchor.
4. Run `lean_check.py` to confirm everything is green.

## Directives

1. **Zero Guesswork & Anti-Defensive Sprawl**:
   - Treat the spec as absolute truth. Do not invent new parameters, change signatures, or create speculative abstraction layers.
   - **Anti-Stub Rule**: Never leave `pass`, `...`, `NotImplementedError`, `TODO`, or placeholder return values.
   - **Recipe Translation Rule**: In `_spec.md`, follow the numbered `Step 1, Step 2, ...` inside the function docstring sequentially. Translate each step 1:1 into Python statements.
   - **Anti-Defensive-Sprawl Rule (CRITICAL)**: Do NOT add speculative `try-except` blocks, silent `except Exception: return None`, or unrequested null checks unless explicitly specified in the recipe. Untested defensive branches will fail diff coverage.

2. **Phased Mechanical Workflow**:
   - **Phase A (TDD Scenarios Placement - Red)**:
     - Open the specified test files from `## Test Suite: <target_test_file>`.
     - Append the exact test functions provided in the spec. Do NOT alter assertions or invent new mock structures.
     - Run: `uv run pytest <target_test_file> -q --tb=short`. Confirm the new tests fail (Red).
   - **Phase B (Core Logic & Wiring - Green)**:
     - Open `## Target: <target_file>`. Implement the function by executing its step-by-step recipe.
     - Open `## Wiring: <caller_file>`. Apply the wiring snippet at `- Anchor: <anchor>`.
     - Run: `uv run ruff check <target_file> <caller_file>`.
   - **Phase C (Final Verification)**:
     - Run: `uv run python tools/agent_skills/lean_check.py --spec <spec_file>`
     - Ensure exit status is `PASS` (0 errors).

3. **Diff Coverage & Self-Healing**:
   - If `lean_check.py` reports untested new lines in diff coverage:
     1. Are they unrequested defensive code (e.g. speculative `try-except`)? -> **Remove dead defensive code and simplify.**
     2. If required logic is missing a test, fix cleanly.

## Output

### 🔨 [IMPLEMENT] <Task Title>

- **Status**: ✅ COMPLETE (or ❌ ESCALATED)
- **Modified**: <Count> files
- **Verification**:
  - 🧪 Pytest: <Passed>/<Total> passed
  - 🧹 Ruff / Mypy: <PASS/FAIL>
  - 📐 Spec Compliance: <PASS/FAIL>
