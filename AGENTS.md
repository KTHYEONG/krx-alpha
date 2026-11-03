# Operating Manual: KRX-Alpha

> **Domain Identity:** KRX high-frequency tick (execution & 10-level order book) lossless ingestion pipeline & real-account shadow OMS engine.

## 1. Domain Ground Truth & Hard Boundaries
- **Lossless Ingestion & Append-Only:** Zero tick loss. Raw L0 market data must be written to append-only zstd journals before parsing.
- **Trading Session Freeze:** Strictly defer deployments and heavy maintenance during market hours (08:10~22:00 KST).
- **Clock Drift & Fail-Closed:** NTP clock drift must stay $\le 2.0s$. On drift violation, abort safely (Fail-Closed, `ClockUnsyncedError`). Paper mode sockets must never reach real broker gateways.
- **Workspace Hygiene:** Keep exploratory experiments and temporary files strictly isolated under `scratch/`. Never commit raw cache or scratch files.

## 2. Autonomy & Execution Contract
- **Bias Toward Action & Diagnostic Autonomy:** For data queries, exploratory scratch diagnostics, and empirical root-cause isolation, execute immediately without asking for permission. When asked open-ended questions about bugs or data anomalies, proactively run scratch experiments under `scratch/` to discover truth. Never modify production code or commit in response to open-ended diagnostic queries.
- **Skills as On-Demand Tools:** Skills (`probe`, `spec`, `implement`, `check`, `refactor`, `commit`, `ship`) are user-level (`~/.claude/skills`) and project-agnostic; this project's policy is the Workflow Profile in §6. Individual slash invocations (`/probe`, `/spec`, etc.) execute only that skill and halt for user review. `/ship` is the explicit autonomy opt-in for the approved implement → check → commit chain. Specs live under `docs/specs/` (gitignored for model/tool handoffs without repo bloat).

## 3. Project Toolchain & Verification
Verify code changes against the project's native toolchains before concluding tasks:
- **Quality Gate:** `uv run python tools/verify.py`
- **Test Runner:** `uv run pytest`
- **Git Commits:** Run the project's `commit` skill.

## 4. Communication & Language
- **Natural Korean:** Converse, explain rationales, and report findings in Korean (한국어). Inside structured output cards, retain English keys/badges while writing descriptions in Korean.
- **Technical English:** System instructions, rules, specifications (`docs/specs/`), code, and docstrings are written in English.

## 5. Domain Rule Routing
- **Domain Invariants:** [domain.md](.agents/rules/domain.md) — *Financial invariants, temporal causality, market frictions, and conservation laws.*
- **Testing & Quality:** [testing.md](.agents/rules/testing.md) — *Invariant-driven testing, boundary conditions, failure isolation, and diff-coverage.*
- **Architecture & Standards:** [code-style.md](.agents/rules/code-style.md) — *Module boundaries, strong static typing contracts, and toolchain alignment.*
- **Documentation & Comments:** [documentation.md](.agents/rules/documentation.md) — *Production docstrings, architecture specs, and non-obvious rationale.*
- **Performance & Optimization:** [performance.md](.agents/rules/performance.md) — *Vectorized panel builds, hot-loop profiling, and resource budgets.*
- **Logging & Diagnostics:** [logging.md](.agents/rules/logging.md) — *Operational logging, 6 fixed category taxonomy, and credential redaction.*

## 6. Workflow Profile
- verify: `uv run python tools/verify.py`
- test_scoped: `uv run pytest {files}`
- spec_dir: `docs/specs` (untracked; archive finished specs to `docs/specs/_done/`)
- scratch_dir: `scratch/` (never commit)
- high_risk_paths: `src/data/**`, ingestion/journal writers, OMS and broker gateway code, clock/NTP guards, ledger/accounting code
- hard_stops: deploy or heavy maintenance during market hours (08:10~22:00 KST); any order TR on live keys (`KIS_APP_KEY`); changes to append-only journal semantics; weakening `ClockUnsyncedError` fail-closed or paper/real socket separation; any `rm` without previewing exact targets
- commit_style: Korean `<type>: <summary <=50 chars>` + `- **Why:** <rationale>`; types feat/fix/refactor/chore/docs; no AI or co-author trailers; every commit via `/commit`
- report_language: Korean (card keys stay English)
- rules: `.agents/rules/*.md`
