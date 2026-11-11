---
paths:
  - "src/core/config.py"
  - "docker-compose.yml"
  - "docs/flags.toml"
  - "tests/unit/test_flag_policy.py"
---
# Config Flag Policy

> **A fix or finished feature ships ON. A new on/off switch needs a registered reason, or it does not exist.**

- Kinds: `env_binding` (mock/live), `safety_interlock` (irreversible or live-risk action), `component` (ON in production, default True, no compose override), `deployed_component` (needs deploy-only slots/capacity; list `couples`, a test proves them), `experiment` (needs `exit_condition` + `review_by`), `legacy_rollout` (frozen list in the test; it can only shrink).
- Never add a default-off flag to ship a fix, rollout, or recovery path. Rollback is `git revert` + redeploy.
- Code defaults equal production; compose carries values (slots, capacities), not copies of defaults.
- Every toggle (name matches `enable|disable|resend|_on_|allow|auto_adopt|force_`) is listed in `docs/flags.toml`; `tests/unit/test_flag_policy.py` enforces it.
- A flag ON in production for 14 days is deleted with its OFF branch in the same change.
