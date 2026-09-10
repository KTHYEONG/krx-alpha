---
name: probe
description: Explore hypotheses, conduct empirical scratch experiments, and establish high-reasoning design rationale before contracting.
---

# Probe Protocol

High-reasoning exploratory protocol to formulate hypotheses, uncover failure modes, and empirically validate solutions via scratch experiments before freezing contracts.

## High-Reasoning Exploration Philosophy

As the high-reasoning architect, your cognitive budget here is 100% dedicated to **problem formulation, empirical validation, edge case discovery, and structural design choices**.
Do NOT write `contract.json` or full test suite skeletons in this phase.
Focus on: *Is the hypothesis sound? What does the real data/runtime look like? Which architecture handles failure modes best?*

## Directives

1. **Context & Prior ADR Lookup**:
   - Collect domain references and historical decisions:
     ```bash
     uv run python tools/agent_skills/spec_init.py --feature <feature_name> --domain <domain> --query <keyword>
     ```
   - Inspect existing schemas, calling pipelines, and invariant contracts using targeted `rg`/`view_file`.

2. **Autonomous Hypothesis & Exploration**:
   - Explore the solution space with full autonomy. Formulate hypotheses and compare design alternatives based strictly on problem complexity — whether a single verified hypothesis or multiple competing models.
   - Actively identify failure modes (e.g. division by zero, lookahead leak, missing corporate action adjustment, numerical instability, OOM on full window) without artificial ceilings.

3. **Mandatory Empirical Verification (Scratch Probing)**:
   - When numeric stability, data availability, schema compatibility, or runtime performance is non-trivial, run empirical probes:
     - Create a temporary probe script: `scratch/probe_<topic>.py`
     - Run via `uv run python scratch/probe_<topic>.py`
     - Measure real values, shape transformations, or execution bottlenecks directly on actual or synthetic datasets.
     - Never rely on speculative assumptions when an empirical command can prove or falsify them.

4. **Invariants & Performance Budget Formulation**:
   - Define strict Fail-Closed invariants and domain boundaries (.agents/rules/quant.md, performance.md, code-style.md).
   - If touching backtest, training, or bulk I/O, draft a realistic `performance_budget`:
     - `{ expected_data_scale, memory_target_mb, storage_format, dtype_precision, chunking_strategy }`
     - Do NOT introduce artificial truncation or shortened windows.

5. **Probe Summary Persistence & Transition Gate to `/spec`**:
   - To prevent context loss across agent sessions or token budget limits, **always persist probe conclusions** to `scratch/probe_<topic>.json`:
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

Keep chat response ultra-compact, scannable, and evidence-focused. Strictly avoid conversational prose and narrative walls of text.
Detailed logs, benchmark payloads, and raw traces MUST be dumped to `scratch/probe_<topic>.json` and referenced via link, not pasted into chat.

**Output Directives:**
- **Terminal-Safe Tables**: Never put multiline descriptions or long code snippets inside Markdown tables. Keep columns short (`Item / Target`, `Evidence / Metric`, `Verdict`).
- **Concise Bullet Points**: Use concise, telegraphic bullets (verb-first or keyword-first, max 1-2 lines per bullet).
- **Zero Redundancy**: Do not repeat explanations across Summary, Matrix, and Traps.
- **Language Requirement**: All output rendered to the user MUST be written in English.

---

### 🔬 [PROBE] <Feature/Topic Title>

#### 1. Triage Summary
- 🎯 **Core Finding**: <Root cause or empirical defect in 1 line>
- 📦 **Scope**: <In-scope targets vs out-of-scope / backlogged debt>

#### 2. Empirical Verification Matrix
> 📁 Trace/Payload Details: [`scratch/probe_<topic>.json`](file:///scratch/probe_<topic>.json)

| Target / Item | Empirical Evidence / Metric | Verdict |
| :--- | :--- | :--- |
| `<Target module/issue>` | `<Compact measurement, failing line, exit code>` | `CONFIRMED / REJECTED / BUG` |

#### 3. Core Invariants & Boundaries
- 🛡️ **<INV-NAME>**: <Fail-Closed condition or boundary rule in 1 line>
- 🧩 **<STATE-RULE>**: <State transition or schema contract in 1 line>

#### 4. Implementation Traps
*Only use when critical. Maximum 2 alert callouts. Never repeat info from Summary or Matrix.*
> [!CRITICAL]
> **<Trap / Risk Title>**: <Specific implementation caution or regression warning>

---
👉 Next Step: `/spec --feature <feature_name> --domain <domain>`
