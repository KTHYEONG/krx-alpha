---
name: spec
description: Produce a machine-readable, zero-invention implementation blueprint (Markdown Spec) from probed architecture rationale.
---

# Spec Protocol

Produce an unambiguous, AI-native implementation blueprint (`docs/specs/<feature>_spec.md`) optimized for mechanical, zero-search, zero-invention downstream execution by low-reasoning worker models (`implement`).

## High-Reasoning Allocation Philosophy

The design rationale and empirical proof have already been resolved in `/probe`.
As the contract architect, your cognitive budget is 100% dedicated to:
1. **Concrete Step-by-Step Recipe in Docstrings/Comments**: An unambiguous, sequential pseudocode recipe (`Step 1`, `Step 2`, ...) inside each target symbol's definition so low-reasoning models translate directly to code without guessing.
2. **Explicit Wiring Snippets**: Ready-to-paste caller wiring blocks (`Anchor` + clean import/call snippet).
3. **Uncompromising Concrete Test Suites**: Raw, unescaped test functions with strict assertions and exact exception/output matches.

## Directives

1. **Prerequisite & Context Alignment (Auto-Inference)**:
   - When `/spec` is called without arguments, automatically infer the feature name, domain, and technical context directly from the preceding `/probe` step in the active session.
   - Carry over the invariants, trade-offs, failure modes, and architectural decisions directly from conversation memory.
   - Inspect target files and immediate 1-depth callers to ensure exact imports and AST anchors. Do NOT scan unrelated repository paths.

2. **Blueprint Structure (`docs/specs/<feature>_spec.md`)**:
   Create a pure Markdown specification containing three mandatory sections:

   ### A. Target Blueprint (`## Target: <relative_path>`)
   Write the exact function/class signature with a comprehensive docstring containing the algorithmic execution steps:
   ```python
   def target_function(param1: Type1, param2: Type2) -> ReturnType:
       """
       [STEP-BY-STEP RECIPE FOR IMPLEMENTER]:
       Step 1. Precondition / Fail-closed check:
          Validate inputs. Raise DomainError if invalid.
       Step 2. Core Transformation:
          Execute logic in exact mathematical/domain order.
       Step 3. Failure Mode Defense:
          Handle boundary edge case X explicitly.
       Step 4. Postcondition Guarantee:
          Return formatted result.
       """
   ```

   ### B. Wiring Blueprint (`## Wiring: <caller_file>`)
   Specify the exact anchor and ready-to-insert code:
   - `- Anchor: <function_name_or_class>`
   ```python
   from path.to.module import target_function
   # Replace / Insert at anchor:
   result = target_function(...)
   ```

   ### C. Test Suite (`## Test Suite: <target_test_file>`)
   Write raw, 100% executable Python test functions. NEVER use JSON string escaping (`\n`, `\"`). Write clean pytest code directly:
   ```python
   def test_target_function_normal():
       ...
   def test_target_function_boundary():
       ...
   ```

3. **Self-Validation Gate**:
   Validate the markdown spec with `lean_check.py`:
   ```bash
   uv run python tools/agent_skills/lean_check.py --spec docs/specs/<feature>_spec.md --pre-impl
   ```

## Chat Output Format

Keep chat output ultra-compact. Detailed recipes and test code already reside inside `_spec.md`. Output only the minimal hand-off card below:

### 📐 [SPEC] <기능명>
> 📄 **청사진**: [`docs/specs/<feature>_spec.md`](file:///docs/specs/<feature>_spec.md)  
> 🚦 **하네스 검증**: `lean_check --pre-impl` **PASS**

- 🎯 **작업 요약**: <구현할 핵심 기능 1줄 요약>
- 📦 **작업 규모**: <N>개 파일 수정 · <N>개 테스트 작성 완료

---
👉 **다음 단계 (OpenCode)**: 
`/implement docs/specs/<feature>_spec.md`
