---
name: probe
description: Explore hypotheses, conduct empirical scratch experiments, and establish high-reasoning design rationale before contracting.
---

# Probe Protocol

High-reasoning exploratory protocol to formulate hypotheses, uncover failure modes, and empirically validate solutions via scratch experiments before freezing contracts.

## High-Reasoning Exploration Philosophy

As the high-reasoning architect, your cognitive budget is 100% dedicated to **causal root-cause discovery, autonomous alternative generation, and falsification-driven stress testing**.
Do NOT write `contract.json` or full test suite skeletons in this phase.
Focus on: *What is the fundamental causality behind this state? What competing architectures could resolve it? Under what realistic boundary conditions does each approach break?*

## Directives

1. **Focused Scope & Context Alignment (No Over-Exploration)**:
   - Do NOT run broad, unbounded repository scans. Confine code inspection to the target module and its immediate 1-depth callers or fixtures using targeted `rg`/`view_file`.
   - Collect domain references and historical decisions when needed:
     ```bash
     uv run python tools/agent_skills/spec_init.py --feature <feature_name> --domain <domain> --query <keyword>
     ```
   - **Challenge Prior Assumptions (No Sacred Cows)**: Historical ADRs and caveats are past context, not infallible dogma. If current empirical evidence contradicts a past decision, actively challenge and falsify the old assumption rather than anchoring to it.

2. **Autonomous Alternative Generation (No Anchoring Bias)**:
   - Do NOT settle on the first plausible fix. Formulate competing architectural approaches driven strictly by the inherent nature of the problem.
   - Evaluate engineering trade-offs (complexity vs. safety vs. blast radius vs. maintainability) without forcing predetermined dichotomies.

3. **Falsification-Driven Empirical Probing**:
   - Do NOT merely verify that a hypothesis works under happy paths; actively identify under what domain-specific conditions it **fails**.
   - Interrogate implicit assumptions regarding data distribution, state transitions, timing, concurrency, and boundary constraints specific to this system.
   - Run lightweight scratch probes:
     - Create a temporary probe script: `scratch/probe_<topic>.py`
     - Run via `uv run python scratch/probe_<topic>.py`
     - Measure real values, state transformations, or execution bottlenecks directly on actual or synthetic workloads.
     - Never rely on speculative assumptions when an empirical command can prove or falsify them.

4. **Invariants & Performance Budget Formulation**:
   - Define strict Fail-Closed invariants and domain boundaries (.agents/rules/quant.md, performance.md, code-style.md).
   - If touching backtest, training, or bulk I/O, draft a realistic `performance_budget`:
     - `{ expected_data_scale, memory_target_mb, storage_format, dtype_precision, chunking_strategy }`
     - Do NOT introduce artificial truncation or shortened windows.

5. **Probe Summary Persistence & Transition Gate to `/spec`**:
   - Always persist probe conclusions to `scratch/probe_<topic>.json`:
     ```json
     {
       "feature": "<feature_name>",
       "domain": "<domain>",
       "hypothesis": "<validated core hypothesis>",
       "empirical_proof": {
         "script": "scratch/probe_<topic>.py",
         "summary": "<real measurement/benchmark result>"
       },
       "alternatives_considered": ["<alt 1>", "<alt 2>"],
       "chosen_reason": "<why the selected architecture was chosen>",
       "failure_modes": ["<failure mode 1>", "<failure mode 2>"],
       "invariants": ["<fail-closed rule 1>", "<invariant rule 2>"],
       "performance_budget": null
     }
     ```
   - Once persisted, hand off directly to `/spec` which will consume `scratch/probe_<topic>.json` as input.

## Chat Output Format

Keep chat response clear, intuitive for humans, and evidence-focused. Avoid cryptic jargon dumps or robotic abbreviation walls.
Detailed logs, benchmark payloads, and raw traces MUST be dumped to `scratch/probe_<topic>.json` and referenced via link, not pasted into chat.

**Output Directives:**
- **Human-Readable Context First**: Always explain the core diagnosis, chosen direction, and user-facing impact in plain Korean before presenting tables.
- **Terminal-Safe Tables**: Never put multiline descriptions or long code snippets inside Markdown tables. Keep columns short.
- **Language Requirement**: All output rendered to the user MUST be written in Korean (한국어).

---

### 🔬 [PROBE] <기능/토픽 제목>

#### 1. 한눈에 보는 진단 요약 (Triage Briefing)
- 🔍 **근본 원인 (Root Cause)**: <표면적 에러가 아닌, 데이터/상태 전이 상의 근본 원인 1-2줄>
- 🛠️ **채택된 접근법**: <경쟁 대안 중 왜 이 방식을 선택했는지 1-2줄>
- 🎯 **영향 및 기대 효과**: <이 변경이 시스템과 사용자에게 주는 실질적 영향 1줄>

#### 2. 실증 검증 매트릭스 (Empirical Matrix)
> 📁 상세 로그/페이로드: [`scratch/probe_<topic>.json`](file:///scratch/probe_<topic>.json)

| 검증 항목 / 대상 | 실증 측정치 / 근거 | 판정 (Verdict) |
| :--- | :--- | :--- |
| `<모듈 or 이슈>` | `<측정값, 실패 라인, exit code 등 컴팩트한 근거>` | `CONFIRMED / REJECTED / BUG` |

#### 3. 핵심 불변식 (Invariants & Boundaries)
- 🛡️ **<INV-NAME>**: <Fail-Closed 조건 또는 경계 규칙 1줄 요약>
- 🧩 **<STATE-RULE>**: <상태 전이 또는 스키마 규약 1줄 요약>

#### 4. 구현 시 주의점 및 위험 (Critical Traps)
*필요한 경우에만 최대 2개 이하의 콜아웃 박스 사용. 중복 설명 금지.*
> [!CRITICAL]
> **<핵심 위험 제목>**: <구현 시 주의할 엣지 조건 또는 회귀 경고 1줄 요약>

---
👉 다음 단계: `/spec --feature <feature_name> --domain <domain>`
