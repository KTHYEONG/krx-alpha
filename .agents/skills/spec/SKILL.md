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

1. **Prerequisite & Context Alignment**:
   - Check if `scratch/probe_<feature>.json` exists. Carry over invariants, trade-offs, and failure modes directly.
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

Keep chat response clear, intuitive for humans, and contract-focused. Avoid cryptic jargon dumps or robotic abbreviation walls.

---

### 📐 [SPEC] <기능명>
> 📄 **청사진 문서**: [`docs/specs/<feature>_spec.md`](file:///docs/specs/<feature>_spec.md)  
> 📊 **작업 규모**: <N>개 파일 · <N>개 변경점 · <N>개 배선 · <N>개 시나리오  
> 🚦 **게이트 검증**: `lean_check --pre-impl` **PASS**

#### 1. 한눈에 보는 변경 요약 (Summary Briefing)
- 🔍 **해결할 문제**: <전문 용어 난사 대신, 기존에 어떤 결함/증상이 발생하고 있었는지 직관적 설명 1-2줄>
- 🛠️ **해결 방식**: <어떤 구조적 개선이나 알고리즘 레시피로 해결하는지 1-2줄>
- 🎯 **기대 효과 및 영향**: <사용자/시스템 입장에서 무엇이 정상화되는지 1-2줄>

#### 2. 작업 범위 및 연계 조치 (Scope & Follow-ups)
- 📦 **이번 작업에 포함 (In-Scope)**: <직접 해결 대상 및 함께 수정할 연관 변경점 일체>
- 🛡️ **설계상 금지 (Guardrails)**: <나중에 할 일이 아니라, 의도적으로 배제한 안티패턴/원칙>
- ⏭️ **후속 연계 과제 (Next Action)**: <독립적인 대형 작업 등으로 분리가 필요한 경우, 원클릭 실행 명령어 제시. 없을 시 "없음">

#### 3. 구현 및 배선 대상 (Blueprint Matrix)
| 파일 경로 | 구분 | 대상 심볼 / 앵커 |
| :--- | :--- | :--- |
| `[<target_file>](file:///<target_file>)` | Target | `<symbol_1>` (Step-by-step Recipe 제공) |
| `[<caller_file>](file:///<caller_file>)` | Wiring | `<anchor_symbol>` (호출부 주입) |
| `[<test_file>](file:///<test_file>)` | Tests | `<N>개 시나리오 테스트 스켈레톤` |

---
👉 다음 단계: `/implement docs/specs/<feature>_spec.md`
