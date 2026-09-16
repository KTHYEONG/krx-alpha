---
name: check
description: Independently audit contract compliance, typing, regressions, coverage, and test validity.
---

# Check Protocol

Independent audit gate completing the development loop (`probe` -> `spec` -> `implement` -> `check`). Performs deterministic verification followed by rigorous semantic code review.

## Directives

1. **Identify Modified Scope & Active Spec**:
   - Inspect modified files using `git status --short`.
   - Identify active spec under `docs/specs/*_spec.md` or `docs/specs/*_contract.json`.

2. **Tier 1: Deterministic Audit Gate (Fast Script)**:
   - Run Smart Selective Verification runner:
     ```bash
     uv run python tools/agent_skills/lean_check.py
     ```
     (Pass `--spec docs/specs/<feature>_spec.md` if not auto-detected).
   - This gate deterministically verifies: Ruff linting, Mypy types, impact-scoped tests, and **100% diff coverage** on newly-added lines.
   - **Immediate Stop on Tier 1 Failure**: If `lean_check.py` fails, immediately report `FAIL` with root-cause diagnostics without proceeding to Tier 2.

3. **Tier 2: Semantic Defect Scan (Universal Code Review)**:
   Tier 1 proves mechanical compliance (lines were executed). Tier 2 proves **semantic truth**. Review the `git diff` against 3 universal engineering lenses without anchoring to a single domain:
   1) **Test Efficacy & Vacuity**: Verify tests are not vacuous. Assertions must validate concrete return values and state transformations, not merely assert trivial truths (`assert True`) or mask real logic with over-mocking.
   2) **Contract & State Integrity**: Verify pre/post conditions and domain invariants hold. Ensure edge conditions, boundary values, error branches, and resource lifecycles are properly handled rather than silently ignored.
   3) **Clean Wiring & Anti-Sprawl**: Ensure new logic is actively wired into its production entry-point (no dead/ghost paths), and confirm that no speculative defensive try-except blocks were added solely to skirt coverage.

4. **Surgical Remediation Authority (Zero Ping-Pong)**:
   - The auditor (`check`) has full authority to perform pinpoint surgical patches when the diagnosis is deterministic:
     1) Fix minor typing/import inconsistencies or formatting in target files.
     2) Strengthen loose test assertions or fix contradictory test fixtures directly.
     3) Clean up artificial test contortions or dead code.
   - Immediately re-run `lean_check.py` to confirm the patch is green.
   - Output final verdict with concise audit evidence.

## Output

Avoid conversational fluff. Present concise, verifiable audit evidence:

### 🛡️ [CHECK] <Audit Target>

- **Tier 1 (Mechanical Gate)**: ✅ PASS (Lint, Type, Tests, Diff-Coverage)
- **Tier 2 (Semantic Audit)**:
  - 🧪 **Test Efficacy**: <1 line: Evidence that tests genuinely validate state/behavior rather than just executing lines>
  - 🧩 **Contract & State**: <1 line: Verification of pre/post conditions, invariants, and boundary safety>
  - 🔌 **Wiring & Cleanliness**: <1 line: Confirmation of active entry-point hookup and absence of dead defensive code>
- **Verdict**: ✅ PASS [Optional: (Surgical Fix: <1-line summary of patch applied>)]

*(On Failure)*:
❌ FAIL: <Audit Target> | Root: <Cause> | Impact: <Scope> | Fix: <Action> → `/implement`, `/spec`, or `/probe`
