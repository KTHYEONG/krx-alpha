# Operating Manual: KRX-Alpha

> **Domain Identity:** KRX high-frequency tick (execution & 10-level order book) lossless ingestion pipeline & real-account shadow OMS engine.

## 1. Domain Ground Truth & Hard Boundaries
- **Lossless Ingestion & Append-Only:** Zero tick loss. Raw L0 market data must be written to append-only zstd journals before parsing.
- **Trading Session Freeze:** Strictly defer deployments and heavy maintenance during market hours (08:10~22:00 KST).
- **Clock Drift & Fail-Closed:** NTP clock drift must stay $\le 2.0s$. On drift violation, abort safely (Fail-Closed, `ClockUnsyncedError`). Paper mode sockets must never reach real broker gateways.
- **Workspace Hygiene:** Keep exploratory experiments and temporary files strictly isolated under `scratch/`. Never commit raw cache or scratch files.
- **Core Quant Invariants (details: `.agents/rules/domain.md`):** Point-in-time data only, no lookahead; historical universe including delisted names; results net of friction with capacity limits; no magic numbers (typed `Spec`/`Config`); ledgers conserve exactly (integer KRW/`Decimal`); on anomalies fail closed to `NO_TRADE`.
- **Config & Secrets:** Runtime settings are externalized and validated at application boundaries; never hardcode environment-dependent secrets or endpoints.

## 2. Autonomy & Execution Contract
- **Bias Toward Action & Diagnostic Autonomy:** Run data queries and scratch diagnostics without asking permission; for open-ended bug or anomaly questions, run experiments under `scratch/` to find the truth. Never modify production code or commit in response to open-ended diagnostic queries.
- **Skills as On-Demand Tools:** Skills (`probe`, `spec`, `implement`, `check`, `refactor`, `commit`, `ship`) live in `~/.agents/skills`; project policy is §6. Slash invocations run only their skill and halt for review; `/ship` is the explicit autonomy opt-in. Specs live under `docs/specs/` (gitignored for model/tool handoffs).

## 3. Project Toolchain & Verification
Verify code changes against the project's native toolchains before concluding tasks:
- **Quality Gate:** `uv run python tools/verify.py`
- **Test Runner:** `uv run pytest`
- **Git Commits:** Run the project's `commit` skill.
- Instruction budget: `uv run python tools/instruction_budget.py`
- Commit guard: git config core.hooksPath .githooks (one-time per clone)

## 4. Communication & Language
- **Natural Korean:** Converse, explain rationales, and report findings in Korean (한국어). In structured output cards, keep English keys/badges; write descriptions in Korean.
- **Technical English:** System instructions, rules, specifications (`docs/specs/`), code, and docstrings are written in English.

## 5. Domain Rule Routing
- Before touching `src/**`, `tests/**`, or strategy/universe/accounting logic: read `.agents/rules/domain.md`
- Before touching `tests/**`, test tooling, or fixtures: read `.agents/rules/testing.md`
- Before touching any logging call, log format, or credential handling: read `.agents/rules/logging.md`
- Before touching hot loops, bulk I/O, or parallelism: read `.agents/rules/performance.md`
- Before touching docstrings, comments, or `docs/architecture/**`: read `.agents/rules/documentation.md`

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
