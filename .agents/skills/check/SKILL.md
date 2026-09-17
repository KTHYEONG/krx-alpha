---
name: check
description: Independently audit contract compliance, typing, regressions, coverage, and test validity.
---

# Check Protocol

Independent audit gate completing the development loop (`probe` -> `spec` -> `implement` -> `check`). Performs deterministic mechanical verification followed by semantic review.

## Directives

1. **Scope Identification**:
   - Inspect modified files using `git status --short`.
   - Identify active spec under `docs/specs/*_spec.md` or `docs/specs/*_contract.json`.

2. **Tier 1: Deterministic Verification**:
   - Run the lean check runner:
     ```bash
     uv run python tools/agent_skills/lean_check.py
     ```
     (Pass `--spec docs/specs/<feature>_spec.md` if needed).
   - Verifies: scaffolding exclusion, Ruff linting, Mypy types, pytest suite, and 100% diff coverage on new `src/` lines.
   - Stop immediately on Tier 1 failure and report diagnostics.

3. **Tier 2: Semantic Review**:
   Review the `git diff` across three engineering lenses:
   1) **Test Efficacy**: Assertions validate real state transformations and domain calculations rather than vacuous checks or over-mocked logic.
   2) **Contract & Invariants**: Pre/post conditions, domain invariants, edge conditions, and error branches are handled safely.
   3) **Clean Wiring & Code Health**: New logic connects to production entry points without ghost paths, dead defensive try-except blocks, or temporary scaffolding.

4. **Surgical Remediation**:
   - The auditor may apply pinpoint fixes for deterministic issues:
     1) Minor typing, import, or format inconsistencies.
     2) Test fixture adjustments or assertion strengthening.
     3) Removal of dead code or temporary comments.
   - Re-run `lean_check.py` to confirm the fix is green.

## Output

### 🛡️ [CHECK] <Audit Target>

- **Tier 1 (Mechanical Gate)**: ✅ PASS (Scaffolding-Clean, Lint, Type, Tests, Diff-Coverage)
- **Tier 2 (Semantic Audit)**:
  - 🧪 **Test Efficacy**: <Evidence that tests validate behavior rather than trivial lines>
  - 🧩 **Contract & Invariants**: <Verification of domain invariants and boundary safety>
  - 🔌 **Wiring & Cleanliness**: <Confirmation of entry-point connection and clean code>
- **Verdict**: ✅ PASS [Optional: (Surgical Fix: <summary of applied fix>)]

*(On Failure)*:
❌ FAIL: <Audit Target> | Root: <Cause> | Impact: <Scope> | Fix: <Action> → `/implement`, `/spec`, or `/probe`
