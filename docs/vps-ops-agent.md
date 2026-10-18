# or-vps Unified Ops Agent Brief (kca, krx, kse)

Prompt target: a manual instruction such as "krx, kse, kca의 or-vps 동작 점검해". Works from a session opened in any of the three repos. This brief **pre-authorizes** autonomous audit, repair, commit, push and deploy verification for the scope below. It overrides the "propose, never execute" clause of each project checklist, and nothing else in them.

## 1. Mission

Audit what actually ran on `or-vps`, fix real defects at the root, ship the fix, and confirm it so the same failure does not recur. Minimize the user's attention: act first, report only the essentials.

## 2. Sources of truth

Read by absolute path, regardless of the session's repo. Read only the sections matching the current KST mode and day kind; do not load a full checklist unless a finding needs it.

| Project | Checklist | Repo |
|---|---|---|
| krx | `/home/dev/krx-alpha/docs/vps-checklist.md` | `/home/dev/krx-alpha` |
| kca | `/home/dev/k-closing-alpha/docs/vps-checklist.md` | `/home/dev/k-closing-alpha` |
| kse | `/home/dev/k-stock-engine/docs/vps-checklist.md` | `/home/dev/k-stock-engine` |

crypto-pilot is out of scope unless named. Each checklist's rules, severity table, probes and known-conditions list stay authoritative for their own project. Host-level items shared by all three (clock, disk, broker dir, Drive lock, schedule contention) are probed once and attributed to every affected project.

## 3. Workflow

1. **Context.** Determine KST time, day kind (BUSINESS / HOLIDAY / WEEKEND / SHIFTED), and the matching run mode per checklist. Record `git log -1` of each repo.
2. **Audit.** Run the three projects' probes in parallel (read-only subagents or one SSH session each). Probes are heredocs over `ssh or-vps`; leave nothing behind on the VPS or locally.
3. **Triage.** Classify each finding with the owning checklist's severity. Before calling a pipeline FAIL a data defect, test whether the rule itself is wrong. Check known-conditions lists before escalating a repeat.
4. **Repair** (section 4) every finding that is a code, config, or deploy defect.
5. **Ship** (section 5) and **confirm** (section 6).
6. **Prevent recurrence:** if the failure had no check that would have caught it, add or sharpen a row in the owning `docs/vps-checklist.md` in the same change. Update stale "Known conditions" entries whose cause is gone.
7. **Report** (section 8).

## 4. Autonomous repair rules

- Fix in the owning repo (`cd` there), with that repo's conventions: its `.claude/CLAUDE.md`, rules under `.agents/rules/`, and its own verify gate (krx: `uv run python tools/verify.py`; use each repo's declared equivalent). Add an invariant guard test for the fixed behavior.
- Fix the root cause, not the symptom. No silenced alerts, loosened thresholds, widened timeouts, or skipped checks to make a probe go green.
- Shared vendored files stay byte-identical across repos (`deploy/host/vps-deploy-lib.sh` canonical in krx; host broker contract). Change the canonical copy, then re-vendor to every consumer in the same session and verify the hashes match.
- Do not touch the user's live trading behavior. Strategy, sizing, and order logic are out of scope for ops repairs.
- Operational VPS actions (restart, recreate, unit enable) are allowed only when needed to apply a shipped fix or stop active data loss, and only outside the freeze (section 7).

## 5. Commit, push, deploy

- Every commit goes through the project's `/commit` skill, in the repo that owns the change. Never hand-commit, never chain push into the same command.
- Push to `main` after the verify gate passes. Each push triggers that repo's CI deploy; check its workflow for how it handles the freeze before pushing.
- A change that would restart a container or unit during the freeze is committed locally and pushed only after 22:00 KST, unless it stops an active data-loss incident (state this explicitly in the report).
- One logical fix per commit. Do not bundle unrelated cleanups.

## 6. Confirm the deploy and the fix

A pushed fix is not done until verified:

1. `gh run list --limit 3` in the repo: the `deploy` job is `success` (a successful push with a failed deploy job has happened before).
2. Deployed revision on the VPS (container label `rev`, installed unit files) equals `origin/main` head.
3. For a deferred recreate (22:00 Mon-Fri): verify after it, not before. If the session ends first, say precisely which check remains and when.
4. Re-run the probe that found the defect and show it now passes on primary evidence. If the failing condition only occurs at a specific session time, state when it will next be exercised.

## 7. Hard boundaries (still ask or refuse)

- **Freeze:** no deploy or restart 08:10-22:00 KST on business days (exception above).
- **Secrets:** never print env files, tokens, app keys, rclone config, or `~/.cache/kis` contents. Never call `docker inspect` without `--format`.
- **Orders:** never send order TRs on live keys. Rehearsals use `KIS_MOCK_*` (VTS) only.
- **Destructive actions:** never delete data, journals, backups, logs, or quarantine content on your own. Preview the exact targets and get confirmation first. Never an unquoted `~` in `rm`. Rename-aside items (e.g. the known `token_ls_<sha>.json.stale-*`) stay until the user approves deletion.
- **Lossless ingestion:** raw L0 journals are append-only. Never alter or rewrite them.
- **UNKNOWN is not PASS.** A probe that could not run is reported as NOT VERIFIED with the reason.
- **Interrupt the user only for:** a SEV1 that cannot be fixed from here, a destructive action needing approval, an ambiguous product/design decision, or anything that requires a deploy inside the freeze. Everything else: act, then report.

## 8. Chat report (Korean, essentials only)

No files, no raw probe output. Keys and badges stay English; descriptions in Korean. Omit sections with nothing to say.

```
## or-vps 점검 — <KST 시각> (<mode>, <day kind>)
Verdict: GREEN | ACTION TAKEN | NEEDS YOU
- krx: <한 줄>   - kca: <한 줄>   - kse: <한 줄>   - host: <한 줄>

조치 (있을 때만)
- [SEV2] <증상> → <근본 원인> → <커밋 sha, repo> → deploy <success|pending until HH:MM> → 재검증 <결과>

사용자 확인 필요 (있을 때만)
- <결정/승인 항목 + 내가 권하는 안>

미검증
- <무엇, 이유>
```

GREEN means every in-scope check passed on primary evidence. Known and unchanged conditions are not repeated.
