# VPS Daily Automation Audit Runbook (krx-alpha)

Agent-executable runbook. An AI auditor runs it on a schedule, verifies that every automated routine of the day ran **and that the data it produced is correct**, then reports. It is feature-agnostic: new automation is added as a check row (section 12), not as a new document.

- Host `or-vps` (SSH alias, tailnet), container `krx-collector`, data root host `~/krx-alpha/data` = container `/app/data`.
- Last verified against production: 2026-10-08 (third business day, M0–M3 exercised: LS token reissue generation 3 at 08:20:36, dashboard closing-auction fix confirmed at 15:27, dashboard `krx.data_quality` WARN cleared as predicted); 2026-10-07 (second business day with the hardened streamer: M0–M3 exercised, first LS token reissue at 08:20:31 observed, EOD heartbeat continuity observed); 2026-10-06 (first full business day: M0, M1, M2, M3 exercised; S2, S3, S6, S7, C1–C4, C7, Q1–Q6, H8, A1–A2, A5, NP2–NP6, X1 all ran against live data). M4 exercised 2026-10-06 (H7 still in progress at 23:47) and 2026-10-07 (backup finished 23:31:44, all green). Still unexercised: A3/A4 failure paths, the 22:00 deferred recreate with pending image changes, W-only rows beyond X1. Baselines in section 9 are dated; refresh them, never hard-code new ones.
- Output language: report in Korean; keys, IDs, badges stay English (section 10). This file stays English.

## 1. Operating rules (non-negotiable)

1. **Read-only.** No restart, deploy, file write, `rclone` write, or config edit on the VPS. Analysis output goes to the local scratchpad / `scratch/`. Remediation is proposed, never executed, unless the user says so.
2. **No secrets.** Never print env files, tokens, app keys, rclone config, or `~/.cache/kis` contents (file names and mtimes only).
3. **UNKNOWN is not PASS.** A missing file, unreachable host, expired evidence, or a probe error is `UNKNOWN` with the reason. A silent check is a defect of the audit.
4. **Day kind first.** Decide `BUSINESS` / `HOLIDAY` / `WEEKEND` / `SHIFTED` (section 4) before judging anything; the same silence is PASS on a holiday and SEV1 on a business day.
5. **Baselines are relative.** Judge row counts and sizes against the trailing median of the previous 5 comparable days (probes print the ratio). Absolute numbers in section 9 are seeds for sanity only.
6. **Do not trust the dashboard or the logs' own verdicts.** Confirm each claimed OK from primary evidence (files, manifests, parquet content). Audit the dashboard itself (A5).
7. **A FAIL verdict from the pipeline is a finding, not a conclusion.** Before calling it a data defect, test whether the rule is wrong (2026-10-05: `price_band_violation` FAILs were new-listing days under a fixed ±30% band).
8. **Evidence expires.** Collect volatile evidence first (section 5). Run M3 before 22:00.
9. **Load discipline.** Heavy probes only at/after 20:35 KST on business days, wrapped in `nice -n 19`; parquet reads use column pruning. Never run content probes inside the container during 08:10–20:30. Deep re-derivations run locally on copied partitions.
10. **Freeze.** Never recommend or perform a deploy/restart on a business day 08:10–22:00 KST except to stop an active data-loss incident (state this explicitly).

## 2. Severity and escalation

| Sev | Definition | Examples | Action |
|---|---|---|---|
| SEV1 | Data is being lost or will be lost before the next run; fail-closed guard tripped | no ticks during a session window; streamer down >5 min in session; `ClockUnsyncedError`; disk >90%; EOD not finished by 21:00; offload size mismatch with L0 already deleted; calendar says business day but daemon skipped | Notify the user immediately (push/notification tool if available, else first line of the reply) before finishing the rest of the audit |
| SEV2 | Data present but suspect, redundancy or monitoring degraded | DQ FAIL confirmed as real; snapshot kind missing/zero; host backup stale >26h; remote auth expired; unexplained CRITICAL; dashboard OK while ground truth FAIL | In the report headline; propose fix and deadline |
| SEV3 | Anomaly with no data impact | known-benign WARN drifting; one gap ≤30 s; failed unit of another project | In report body |
| INFO | Context | baselines, counts, next run | Body only |

A finding keeps its severity until its evidence is gone; re-report only on change (section 10).

## 3. Run modes and cadence

Run the mode that matches the KST time. Every run starts with C0 (section 4) and ends with the report (section 10).

| Mode | When (KST) | Purpose | Checks |
|---|---|---|---|
| M0 pre-open | business day 07:45–08:15 | Is the day armed? | C0, H1–H6, S1, S7 (stale-by-one is expected), C7, A1, NP1 (if premarket enabled) |
| M1 open | business day 09:05–09:30 | Did collection actually start? | C0, S2–S4, S6–S8, C1 (current hour), C2 (regular), H2 |
| M2 close | business day 15:45–16:15 | Regular session closed clean, aftermarket armed | C0, S2, C2, C4, C7 (aftermarket universe) |
| **M3 EOD review** | business day 20:35–21:45 (**finish before 22:00**) | Full audit of the day incl. content | everything in sections 6–8 except weekly rows |
| M4 night | every day 23:45–00:15 | Host backup, next-day readiness | H5–H8, C7 (next premarket pool), S1 for tomorrow |
| W weekly | Saturday 10:00 | Trends, deep content cross-checks, capacity | all `W` rows, X1, H3 trend |
| X incident | on demand | Triage | section 11, then affected rows |

Holiday / weekend: run M0-lite (H1–H6, S1) and M4; expected state is idle. Cadence is set by whoever schedules the run (loop/schedule skill or the user); this runbook only defines coverage per mode.

## 4. C0 context bootstrap (every run)

Run P-CTX (section 8). Derive:

- `NOW` KST, `KIND` of today and of tomorrow. Sources that must agree: `calendar_cache.json` (`is_business_day`, `previous_business_day` = `P`, `next_business_day` = `N`; valid only when `date == today`), dashboard `[market]` `closed_dates` / `shifted_dates`, weekday. Disagreement = S1 finding. `SHIFTED` days move session anchors: read `calendar/dt=<day>.json` (written only for business days; ABSENT on a holiday/weekend is normal) (`regular_open`, `regular_close`, `after_market_end`, `source`); every time in this runbook shifts accordingly. `source != vendor` means the vendor calendar failed and defaults are in use (SEV3).
- `D` = session date under audit: today if business day and `NOW` ≥ 08:20, else `P`.
- Deployed revision = container label `rev`; compare with `origin/main` head / last successful CI run. Mismatch or a start time inside 08:10–22:00 = S4 finding.

## 5. Evidence volatility

| Evidence | Lifetime | Consequence |
|---|---|---|
| `docker logs krx-collector` (INFO heartbeat, `stage=normalize`, digest, `stage=prune ... REUSED`) | until next container recreate (22:00 Mon–Fri, any deploy) and 30 MB cap | Collect in M3; export `docker logs --since 14h` to the scratchpad first |
| `data/logs/events-*.jsonl` | persistent, rotating 5 MiB x5; **WARNING+ and flagged INFO only** | Absence of INFO is not evidence of absence; `state_change`, `start`, `streamer` INFO are retained |
| `manifest/**`, `universe/**`, `candidates.json`, `calendar/**` | persistent | Primary evidence for plans and session boundaries |
| L0 `l0/**` | until offload-verified and older than `journal_retain_days`=3 | Only source for same-day raw checks |
| L1 `l1/**` | 30 days local, permanent on remote; created for dates < today-3 | DQ verdict of day D exists only from D+4 calendar days; same-day content is checked on L0 |
| Dashboard `public/status.json` | refreshed every minute | Snapshot, not history |
| Host timers / backup status | systemd journal, `host_backup_status.json` | Persistent |

## 6. Moments that must not be missed (business day timeline)

Times are standard anchors; shift per C0. "By" = latest acceptable time; missing it is the stated severity.

| Time | Event | Verify with | If missed |
|---|---|---|---|
| 07:05 | Shared KIS token warm-up (host timer of the sibling project); krx issues a token only if the cache is empty | no `kis_token_preflight` CRITICAL in events at 08:20 | SEV1 for the premarket/aftermarket (KIS) paths |
| 07:58 | Premarket pool effective (if `PREMARKET_ENABLED`) | NP1 | SEV3 (that day's premarket skipped) |
| 08:00–08:50 | NXT premarket stream (if enabled) | NP2–NP6 | SEV2 |
| 08:10 | Freeze starts | no deploy/restart after this | n/a |
| 08:20 | Daemon `STREAMER_ACTIVE`: calendar gate, bars refresh, candidates | S2, S6, S7 | `candidates_not_ready` still failing at 08:50 = **SEV1** (2026-10-02: `selected 96 exceeds slot_budget 90`, collection started late, 97 gaps) |
| by 08:50 | `FULL_ACTIVE`, regular streamer up | S3, C1 | SEV1 |
| 09:00 | Open: first ticks | newest `l0/ls/krx/regular/*/dt=D/HH.jsonl.zst` mtime < 120 s | SEV1 |
| 15:20–15:30 | Closing auction (densest data) | C2 gaps in window | SEV2 |
| by 15:31 | Aftermarket universe written | `universe/aftermarket/D.json` | SEV1 for aftermarket collection |
| 15:39 | Last snapshot REST job; 15:40 `AFTER_MARKET_ACTIVE`, NXT aftermarket starts (KRX aftermarket 16:00) | C4, C2 | SEV2 |
| 20:00 | Aftermarket ends, EOD starts; writers close within ~10 s | C2 `closed` | SEV2 |
| by 20:30 | EOD maintenance done (normalize, offload, reconcile); 20:20 program-trade sync; 20:30 next-day pool | S5, H8, C5, C7 | WARN at 20:30, **SEV1 at 21:00** |
| 22:00 Mon-Fri | Deferred recreate applies pending deploys | M4: container start ~22:00, `rev` as intended | SEV2 |
| 23:30 | Host Drive backup | H7 | SEV2 if stale >26 h |

## 7. Check catalogue

Columns: ID, modes, what, how (probe in section 8 or command), PASS / WARN / FAIL. Severity per section 2 unless stated. Commands run on the host; `cd ~/krx-alpha/data` first. Inside table cells `\|` is markdown escaping of a shell pipe `|`; type it as `|`.

### H: Host and container

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| H1 | M0 M3 M4 | Container health | P-CTX | Up; `restarts=0`; `oom=false`; start = a recreate after 22:00 (CI deploys land any time after the freeze lifts: 2026-10-06 recreates at 22:00, 22:17, 22:20, 23:28) and `rev` = `git rev-parse origin/main` (docs/chore commits rebuild the image too) | any restart/OOM = SEV2; start inside 08:10–22:00 unexplained = SEV2 |
| H2 | M0 M3 W | Memory | `docker stats --no-stream krx-collector` | idle <200 MiB; EOD peak <900 MiB of 1 GiB | >800 MiB WARN; OOM = SEV2 |
| H3 | M0 M3 W | Disk | `df -h /home/ubuntu`; `du -sh data/l0 data/l1` | <70% | 70–85% WARN; >85% FAIL SEV1 (>90%); weekly: project 30-day growth |
| H4 | M0 M3 | Clock | `chronyc tracking` (System time) | abs offset <0.5 s | >0.5 s WARN; >2.0 s FAIL SEV1 (`ClockUnsyncedError`); also per-manifest `clock_offset_ns` (C2) |
| H5 | M0 M4 | Failed units | `systemctl --user --failed --no-legend; systemctl --failed --no-legend` | none for `krx-*` / `quant-dashboard-*` | krx unit failed = SEV2; other project's failed unit = SEV3 INFO (2026-10-05: `kca-tape-sweep.service`; 2026-10-06 23:30: `crypto-pilot-backup.service` status 75 = shared Drive lock busy while `krx-host-backup` held it) |
| H6 | M0 M4 | Timers armed | `systemctl --user list-timers --no-pager \| grep -E "krx-\|quant-dashboard\|vps-image"` | `krx-host-backup` (23:30 KST daily), `krx-deferred-recreate` (Mon-Fri 22:00 KST) have a future trigger; while `krx-host-backup` is running its NEXT column is `-` (not a missing trigger) | missing/stale trigger with the service not running = SEV2 |
| H7 | M0 M4 | Host backup | `cat ~/.local/state/krx-alpha/host_backup_status.json` (+ `journalctl --user -u krx-host-backup --since "2 days ago"`) | `rc=0`, `data_rc=0`, `prune_rc=0`, `last_ok_at` <26 h. The 23:30 run lasts up to ~45 min (2026-10-05: lock_wait 2666 s), so at M4 (23:45–00:15) the file still shows the previous run: PASS if the journal shows `Starting` at 23:30 and `last_ok_at` <26 h; confirm the new attempt at the next M0 | stale/non-zero = SEV2 |
| H8 | M3 W | Offload integrity | P-REMOTE | every newest-3 local L1 has identical remote size; rclone reachable | mismatch = SEV2 (SEV1 if its L0 is already deleted); rclone error = UNKNOWN then SEV2 (`auth_expired`) |
| H9 | M3 | Other containers present | `docker ps -a --format "{{.Names}} {{.Status}}"` | `krx-collector` Up | others: INFO only |
| H10 | M0 M1 + 5-min | LS token age and ownership | P-HEALTH `ls_token_age_h` (file mtime of `~/.cache/kis/token_ls_*.json`; never read its content) and `ls_token_owner_bad` | Tokens issued since `fc22b36` carry the vendor `expires_in` (86400 s) and are reissued 10 min before it, so the age ceiling is 23 h; the 12 h ceiling applies only to legacy tokens without expiry. M0 (before 08:20) age is informational (a night-old token is expected); M1 (08:21–09:20 on a business day) requires `ls_token_age_h` <1 and one `shared_token status=PUBLISHED` at ~08:20 (reissue); afterwards `ls_token_age_h` <23. Verified 2026-10-07: reissue at 08:20:31 (generation 2); `ls_token_owner_bad=0` (store files must be owned by the container's uid; the container currently runs as root, uid 0, which can read and write any owner, so the probe reports 0 then, and a root-owned `token_ls_*.json` is normal; it matters only if the image is changed to a non-root user, where a foreign-owned file makes the store raise `PermissionError`) | no reissue at 08:20 (age ≥1 h at M1) or ≥23 h on a business day after 08:21 = SEV2; on weekends and holidays no streamer connects, so the age keeps growing (2026-10-09 holiday: 28 h) and is informational (stale token: LS closes the socket at subscribe; 2026-10-06 08:20 crash loop); remediation: rename the token file aside and restart the container. `ls_token_owner_bad>0` = SEV2; remediation: `docker exec krx-collector chown 1001:1001 /run/kis-token-cache/token_ls_*` (and the lock files), then confirm `ls_token_age_h` and the next `stream_connect` |
| H11 | 5-min (09:05–15:30 regular, 16:05–20:00 aftermarket) | Live L0 freshness | P-HEALTH `l0 <stream> newest_age_s` | regular ≤180 s until 15:30, KIS aftermarket ≤300 s; after 15:30 regular ticks legitimately stop (last tick 15:30:29); after 20:00 all streams idle | stale in window = SEV1; streamer_alive=False in session = SEV1 |

### S: Scheduling and daemon

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| S1 | M0 M3 M4 | Calendar agreement | C0 sources + events `stage=session status=SKIP reason=market_holiday`; `calendar/dt=<N>.json` exists with `source: vendor` for the next business day (M4) | daemon SKIP exists iff day is closed; anchors file exists only for business days (ABSENT on a closed day is normal) | business day skipped = SEV1; holiday collected = SEV3; sources disagree = SEV2 |
| S2 | M1 M2 M3 | State timeline | P-EVENTS section state_change: `grep '"state_change"' logs/events-daemon.jsonl` for D | `PRE_MARKET_SLEEP→STREAMER_ACTIVE(08:20)→FULL_ACTIVE(08:50)→AFTER_MARKET_ACTIVE(15:40)→POST_MARKET_EOD(20:00)→NIGHT_SLEEP`, each within ±90 s of anchor (holiday: same states, no collection). Cross-check the crash-loop window: `streamer RESTARTED`/`circuit_open` events and `STREAMER_ACTIVE` re-entries must be absent or explained, because manifest gaps do NOT record periods in which the streamer was not running (2026-10-06 08:20–08:41 outage left `gaps=0`) | late/missing transition = SEV2; skipped state = SEV1; crash loop with `circuit_open` = SEV1 |
| S3 | M1 M2 | Heartbeat | `docker logs --since 30m krx-collector 2>&1 \| grep stage=heartbeat \| tail -3` | every ~10 min; `streamer_alive=True` in `FULL_ACTIVE` | stale >15 min or `alive=False` in session = SEV1; `streamer_restarts>0` = SEV2 |
| S4 | M0 M3 | Starts and deploys | events `stage=start status=ONLINE` for D; `work/daemon_lifecycle.json` (`clean_exit`, `crash_error`) | starts only at/after 22:00 (deferred recreate, CI deploys after the freeze); no start between 08:10 and 22:00 | start in 08:10–22:00 = SEV2 (hotfix or crash; read `shutdown` event: `signal`, `graceful`); several starts after 22:00 = INFO (deploys) |
| S5 | M3 | Event audit | P-EVENTS for D | every non-INFO event is explained by an open item or the benign list (section 9); no `stage=eod_*`, `prune`, `quarantine`, `host_backup_freshness` CRITICAL | unexplained CRITICAL/ERROR = SEV2; `eod_maintenance DEGRADED` = SEV1 |
| S6 | M1 | Candidates armed | `candidates.json` (written by the 08:20 orchestration, not before): `rev` = P as YYYYMMDD; count; no `candidates_not_ready` / `orchestration_error` since 08:20. Before 08:20 the file legitimately holds the previous session's rev | rev matches P, count within trailing range and ≤ configured slot budget | stale rev or errors = SEV1 after 08:50 |
| S7 | M0 M1 M3 | Bars refresh | P-BARS `latest_date` | M0: = the business day before P (refresh runs ~08:20); from 08:50: = P | older than P after 08:50 = SEV3; KRX timeout WARN is known (retry) |
| S8 | M1 | First-frame watch (08:36–09:05) | events `stage=ingest_watchdog status=NO_FIRST_FRAME` for D | absent | present = SEV1 (regular streamer produced no frame after the 08:30 print was due); same remediation chain as H11 |

### C: Collection completeness

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| C1 | M1 M3 | L0 inventory | P-L0 with `D` | expected streams (section 9) present; hourly files cover the session window; `zero_size=0`; each file within 0.3–3x the same-hour median of the previous 5 days; M1: newest file mtime <120 s | missing stream/hour = SEV1 in session, SEV2 after; zero-size = SEV1 |
| C2 | M2 M3 | Manifests | P-MANIFEST with `D` | aftermarket: every shard `closed` within 10 s of 20:00, `acks_ok == planned` (40/shard), `gaps=0`, `degraded=None`; regular: `boots=1`, no `restart` gap, max gap ≤30 s; `clock` measured, |offset| <50 ms | ack shortfall / `degraded` = SEV2; `boots>1` or restart gap in session = SEV2; gap >30 s = SEV3 (SEV2 if in 15:20–15:30). Regular manifest has no `writer_closed` by design |
| C3 | M3 | Symbol coverage | KIS aftermarket: `zstdcat l0/kis/<venue>/<session>/H0xxCNT0/dt=D/*.zst \| python3 -c "import sys,json,collections;c=collections.Counter(json.loads(l)['raw'].split('^')[0] for l in sys.stdin);print(len(c),c.most_common(3),c.most_common()[-3:])"` (L0 `symbol` is empty for KIS) | KRX: all 40 pool symbols have trades; NXT: the pool symbols that trade are ~half (thin venue); compare with the baseline row (section 9) instead of an absolute 90% rule | KRX dead symbols (<100 trades) above 12, or NXT symbols with trades below 15 = SEV3 (tune selection, not during freeze) |
| C4 | M2 M3 | Snapshots present | P-SNAP | 9 kinds for D; `ratio` 0.5–2.0 of median **and** consistent with candidate count (see Q6); `session_date` mismatch 0; key nulls 0 | missing kind or 0 rows = SEV2; ratio outside = SEV3 until explained by candidate count |
| C5 | M3 | Program trades | P-BARS-style: `pl.scan_parquet('/app/data/bars/program_trades/*.parquet').group_by('date').len().sort('date').tail(3)` | the sync lags one business day: the latest date at M3 equals P (2026-10-06 M3 still showed 2026-10-02 while `stage=program_trades_sync status=STARTED date=D` ran at 20:28); count within ±5% of median (~2456) | latest date older than P = SEV3; no `STARTED` log by 21:00 = SEV2 |
| C6 | M0 M3 | Daily bars | P-BARS | latest date = P (or D after refresh), count within ±3% of median (~2763) | see Q5 |
| C7 | M2 M4 | Universe artifacts | aftermarket: `universe/aftermarket/D.json`; premarket: `universe/premarket/N.json` | exist; `selected_count` ≤ `capacity`; `effective_from` ≤ start of its session; fields `schema_version rev session_date candidates` present | missing = SEV1 for that session (premarket: SEV3) |

### Q: Data content (is the data itself right)

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| Q1 | M3 W | DQ verdicts | P-DQ | newest partition per active stream: no FAIL; WARN only with the benign counters in section 9 and ratios ≤3x baseline | FAIL: reproduce locally first (X2); real defect = SEV2, rule false positive = SEV3 fix the rule; unknown counter = SEV2 |
| Q2 | M3 | L1 volume | P-DQ `ratio_vs_median` | 0.6–1.6 | outside = SEV3, explain via universe size |
| Q3 | M3 | Conservation | docker logs `stage=normalize part=`: `raw_records == l1_rows + dedup_dropped`; `conn_seq_conflict=0`; footer `num_rows` (P-DQ `rows`) == `l1_rows` | exact | any inequality = SEV1 (loss or duplication) |
| Q4 | M3 W | Phase labels | P-DQ `unclassified` | <1% of rows | ≥1% = SEV3 (time-field assumption broken) |
| Q5 | M0 M3 | Daily bar invariants | P-BARS | all violation columns 0 for the last 10 dates; `duplicate_date_symbol=0`; `chg_mismatch=0` | any >0 = SEV2 |
| Q6 | M3 | Snapshot invariants | P-SNAP2 | OHLC consistent, no duplicate keys/news ids, `net_qty == buy_qty - sell_qty`, ranking keys unique; `program_trade`/`security_status`/`investor_estimate` distinct symbols ≈ candidate count (±5%) | violation = SEV2; symbol/candidate mismatch = SEV3 |
| Q7 | M3 | Candidate content | `candidates.json` via P-SNAP2 line: unique symbols matching `^[0-9A-Z]{6}$`, each present in daily bars for P | all hold | any miss = SEV3 |
| Q8 | M3 | Retention | `ls l0/*/*/*/*/ \| sort`; `ls quarantine` | L0 dates ≤ 4 newest per stream (3 retained + current); L0 older than that exists only when offload is unverified (H8 FAIL); `quarantine/` must contain no files (`find quarantine -type f` returns 0; empty subfolders are fine) | stale L0 without cause = SEV3; any new quarantine entry (mtime after 2026-09-15) = SEV2 (data moved out) |
| X1 | W | Tick vs bar volume | X-VOL (local) | for every symbol subscribed for the whole session (final candidate set), max cumulative tick `volume` == daily bar `volume` (42/42 exact on 2026-10-01; 95/95 on 2026-10-02). Symbols present in L1 with only a few pre-open ticks come from a replaced candidate list after a mid-morning restart (2026-10-02: 24 such symbols) and are excluded, explained by the S4 restart | ≤2 final-set symbols within 1% = SEV3; otherwise SEV2 (tick loss or bar error) |
| X2 | on Q1 FAIL | Reproduce a verdict | copy the L1 partition to `scratch/`, `PYTHONPATH=. uv run python` with `src.storage.quality.decode_tick_raw_fields`, group violations by `shcode`/`sign`/`ref` | root cause class named (feed, decoder, or rule) | unresolved = SEV2 |

### A: Alerting and observability pipeline

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| A1 | M0 M3 | Alert path armed | `docker logs ... \| grep "stage=alert"`; `data/work/alert_ledger.json` after the first CRITICAL | `status=ENABLED` after start; no `status=FAIL` (send failure) | `DISABLED` = SEV1 (alerts off); `FAIL` = SEV2 |
| A2 | M3 | CRITICAL vs mail budget | P-EVENTS CRITICAL count for D vs `alert_ledger.json` (`sent_today`, per-key `count`) and `stage=alert status=SUPPRESSED` lines | each distinct CRITICAL key mailed or logged as suppressed; read the ledger with `docker exec krx-collector cat /app/data/work/alert_ledger.json` (host file is root 0600; fields `sent_today`, per-key `count`/`confirmed_count`); `sent_today` < daily cap 20; no key at cap 3 unexplained (2026-10-06: 1 key, `circuit_open`, mailed once) | CRITICAL with neither mail record nor SUPPRESSED = SEV2 (ask the user to confirm mailbox) |
| A3 | M3 | Digest | `docker logs ... \| grep "stage=digest"` | `status=SENT` after EOD | `FAIL`/missing = SEV3 |
| A4 | M0 M3 | External liveness | `grep "stage=healthcheck"` | none or `RECOVERED` | `status=FAIL` = SEV2 (dead-man's switch blind) |
| A5 | M3 | Dashboard truth audit | P-DASH vs this run's findings | every krx.* level agrees with primary evidence; `generated` <3 min old; `curl -s -o /dev/null -w "%{http_code}" http://100.81.197.26:8765/` = 200 | dashboard OK while a SEV1/SEV2 exists = SEV2 monitoring gap (name the missing check); dashboard FAIL with healthy evidence = SEV3 |

## 8. Probes (tested 2026-10-05)

Write each block to the scratchpad and run as shown. `D` = audit date. Host probes: `ssh or-vps "D=$D python3 -" < file`. Container probes: `ssh or-vps "nice -n 19 docker exec -i krx-collector /app/.venv/bin/python -" < file`.

**P-CTX** (`ssh or-vps "bash -s" < p_ctx.sh`)

```bash
cd ~/krx-alpha/data
echo "now_kst=$(TZ=Asia/Seoul date '+%F %T %a')"
echo "calendar_cache=$(cat calendar_cache.json)"
echo "anchors_today=$(cat calendar/dt=$(TZ=Asia/Seoul date +%F).json 2>/dev/null || echo ABSENT)"
echo "dashboard_market=$(sed -n '/^\[market\]/,/^$/p' ~/quant-dashboard/config.toml | tr '\n' ' ')"
docker inspect krx-collector --format 'container started={{.State.StartedAt}} restarts={{.RestartCount}} oom={{.State.OOMKilled}} rev={{index .Config.Labels "org.opencontainers.image.revision"}}'
echo "latest_l0_days=$(ls -d l0/ls/krx/regular/H0STCNT0/dt=* 2>/dev/null | tail -3 | xargs -n1 basename | tr '\n' ' ')"
echo "latest_l1_days=$(ls l1/ls/krx/regular/H0STCNT0 | tail -2 | tr '\n' ' ')"
```

**P-L0** (host)

```python
import glob, os, collections
D = os.environ["D"]
root = os.path.expanduser("~/krx-alpha/data/l0")
rows = collections.defaultdict(list)
for f in sorted(glob.glob(f"{root}/**/dt={D}/*.zst", recursive=True)):
    rows[f.split("/l0/")[1].split("/dt=")[0]].append((os.path.basename(f).split(".")[0], os.path.getsize(f)))
for s, v in sorted(rows.items()):
    print(f"{s:42s} files={len(v):2d} hours={v[0][0]}-{v[-1][0]} total_mb={sum(x[1] for x in v)/1e6:8.1f} zero_size={sum(1 for x in v if x[1]==0)}")
print("streams_found", len(rows))
```

**P-MANIFEST** (host)

```python
import datetime as dt, glob, json, os
D = os.environ["D"]
K = dt.timezone(dt.timedelta(hours=9))
hm = lambda ns: dt.datetime.fromtimestamp(ns / 1e9, K).strftime("%H:%M:%S")
for f in sorted(glob.glob(os.path.expanduser(f"~/krx-alpha/data/manifest/**/{D}*.json"), recursive=True)):
    m = json.load(open(f)); acks = m["subscription_acks"]; gaps = m["gaps"]
    dur = [(g["gap_end_ns"] - g["gap_start_ns"]) / 1e9 for g in gaps]
    reasons = {}
    for g in gaps: reasons[g.get("reason")] = reasons.get(g.get("reason"), 0) + 1
    print(os.path.basename(f), m["venue"], m["session"], "shard", m["shard_index"],
          "closed", None if m["writer_closed_at_ns"] is None else hm(m["writer_closed_at_ns"]),
          "acks_ok", sum(1 for a in acks if a.get("accepted")), "/", len(acks), "planned", len(m["planned_pairs"]),
          "gaps", len(gaps), "max_gap_s", round(max(dur), 1) if dur else 0, reasons, "boots", len(m["boots"]),
          "clock", m["clock_status"], round(m["clock_offset_ns"] / 1e6, 3), "ms", "degraded", m["degraded_reason"])
```

**P-EVENTS** (host; non-INFO events of `D` per events file, plus daemon state transitions)

```python
import collections, json, os
D = os.environ["D"]; base = os.path.expanduser("~/krx-alpha/data/logs")
for name in sorted(os.listdir(base)):
    if not name.startswith("events-") or not name.endswith(".jsonl"): continue
    c = collections.Counter(); last = {}; states = []
    for l in open(f"{base}/{name}", errors="replace"):
        try: r = json.loads(l)
        except ValueError: continue
        if not r["ts"].startswith(D): continue
        f = r["fields"]
        if f.get("stage") == "state_change": states.append(r["ts"][11:19] + " " + f.get("to", "?"))
        if r["level"] == "INFO": continue
        k = (r["level"], f.get("stage"), f.get("status"), f.get("reason")); c[k] += 1; last[k] = r["ts"][11:19]
    print("==", name, "non-INFO:", sum(c.values()))
    for k, v in c.most_common(15): print("  ", v, k, "last", last[k])
    if states: print("   states:", states)
```

**P-DQ** (container; L1 verdicts, ratios, phase labels; legacy layouts and partitions older than 14 days are not judged)

```python
import collections, datetime as dt, glob, json, statistics
import pyarrow.compute as pc
import pyarrow.parquet as pq
cut = (dt.date.today() - dt.timedelta(days=14)).isoformat()
by = collections.defaultdict(list)
for f in sorted(glob.glob("/app/data/l1/**/dt=*.parquet", recursive=True)):
    if "/snapshot/" in f or f[-18:-8] < cut: continue
    pf = pq.ParquetFile(f); raw = (pf.metadata.metadata or {}).get(b"krx_alpha.dq"); dq = json.loads(raw) if raw else {}
    by[f.split("/l1/")[1].rsplit("/", 1)[0]].append((f, pf.metadata.num_rows, dq.get("status", "NONE"), dq.get("tick") or dq.get("quote") or {}))
for stream, v in sorted(by.items()):
    f, rows, st, t = v[-1]; prev = [r for _, r, _, _ in v[:-1]]; med = statistics.median(prev) if prev else None
    ph = pq.read_table(f, columns=["market_phase"]).column(0)
    unc = pc.sum(pc.equal(ph, "unclassified").cast("int64")).as_py() or 0
    print(f"{stream:30s} {f[-18:-8]} rows={rows} ratio_vs_median={None if not med else round(rows/med,2)} dq={st} hist={[s for _,_,s,_ in v][-6:]} "
          f"unclassified={unc/rows:.4%} nonzero={ {k: x for k, x in t.items() if k != 'rows' and x} }")
```

**P-BARS** (container)

```python
import statistics
import polars as pl
b = pl.scan_parquet("/app/data/bars/daily/*.parquet").with_columns(
    chg_gap=(pl.col("daily_change_pct") - (pl.col("close") / pl.col("base_price") - 1) * 100).abs())
g = b.group_by("date").agg(
    pl.len().alias("n"), (pl.col("close") <= 0).sum().alias("close_le0"), (pl.col("high") < pl.col("low")).sum().alias("hi_lt_lo"),
    ((pl.col("close") > pl.col("high")) | (pl.col("close") < pl.col("low"))).sum().alias("close_out_hl"),
    (pl.col("volume") < 0).sum().alias("vol_neg"), pl.col("close").null_count().alias("nulls"),
    (pl.col("chg_gap") > 0.1).sum().alias("chg_mismatch")).sort("date").tail(10).collect()
print(g)
base = statistics.median(g["n"].to_list()[:-1])
print("latest_date", g["date"][-1], "latest_n", g["n"][-1], "median_prev_n", base, "ratio", round(g["n"][-1] / base, 4))
print("duplicate_date_symbol", b.group_by(["date", "symbol"]).len().filter(pl.col("len") > 1).select(pl.len()).collect().item())
```

**P-SNAP** (container; presence, volume, key integrity per snapshot kind)

```python
import glob, statistics
import polars as pl
for k in sorted(glob.glob("/app/data/l1/snapshot/*")):
    fs = sorted(glob.glob(k + "/dt=*.parquet"))
    rows = [(f[-18:-8], pl.scan_parquet(f).select(pl.len()).collect().item()) for f in fs[-6:]]
    last = rows[-1]; med = statistics.median([n for _, n in rows[:-1]]) if len(rows) > 1 else None
    df = pl.read_parquet(fs[-1])
    bad = int((df["session_date"].cast(pl.String) != last[0]).sum())
    nulls = {c: int(df[c].null_count()) for c in ("symbol", "observed_at_ns", "session_date") if c in df.columns and df[c].null_count() > 0}
    syms = df["symbol"].n_unique() if "symbol" in df.columns else None
    print(k.split("/")[-1], "latest", last, "median_prev", med, "ratio", None if not med else round(last[1] / med, 3), "symbols", syms, "session_date_mismatch", bad, "key_nulls", nulls)
```

**P-SNAP2** (container; content invariants and candidates)

```python
import glob, json
import polars as pl
c = pl.col
def last(kind): return sorted(glob.glob(f"/app/data/l1/snapshot/{kind}/dt=*.parquet"))[-1]
def n(df, e): return int(df.select(e.sum()).item())
d = pl.read_parquet(last("stock_minute_bar"))
print("stock_minute_bar rows", d.height, "bad_ohlc", n(d, (c("high") < c("low")) | (c("open") > c("high")) | (c("open") < c("low")) | (c("close") > c("high")) | (c("close") < c("low"))),
      "dup", d.height - d.select("symbol", "bar_time").unique().height, "symbols", d["symbol"].n_unique(), "bar_time", d["bar_time"].min(), d["bar_time"].max())
d = pl.read_parquet(last("index_minute_bar"))
print("index_minute_bar bad_ohlc", n(d, (c("high") < c("low")) | (c("close") > c("high")) | (c("close") < c("low"))), "dup", d.height - d.select("index_code", "bar_time").unique().height)
d = pl.read_parquet(last("ranking")); print("ranking dup", d.height - d.select("list_kind", "rank", "observed_at_ns", "market_div_code").unique().height)
d = pl.read_parquet(last("news_title")); print("news_title dup_news_id", d.height - d["news_id"].n_unique(), "null_title", int(d["title"].null_count()))
d = pl.read_parquet(last("program_trade")); print("program_trade net_mismatch", n(d, (c("net_qty") - (c("buy_qty") - c("sell_qty"))).abs() > 0), "of", d.height)
cand = json.load(open("/app/data/candidates.json")); syms = [x["symbol"] for x in cand["candidates"]]
print("candidates rev", cand["rev"], "n", len(syms), "unique", len(set(syms)))
```

**P-REMOTE** (host; local vs remote L1 sizes for the newest 3 partitions per stream)

```python
import glob, json, os, subprocess
R = os.path.expanduser("~/.local/bin/rclone"); ROOT = os.path.expanduser("~/krx-alpha/data/l1")
REMOTE = "gdrive:quant-lake/live/krx-alpha/data/l1"; bad = 0
streams = sorted({os.path.dirname(f) for f in glob.glob(f"{ROOT}/**/dt=*.parquet", recursive=True) if "/snapshot/" not in f})
for s in streams:
    rel = os.path.relpath(s, ROOT)
    local = {os.path.basename(f): os.path.getsize(f) for f in sorted(glob.glob(f"{s}/dt=*.parquet"))[-3:]}
    out = subprocess.run([R, "lsjson", f"{REMOTE}/{rel}"], capture_output=True, text=True, timeout=120)
    remote = {x["Name"]: x["Size"] for x in json.loads(out.stdout or "[]")} if out.returncode == 0 else None
    for name, size in local.items():
        state = "UNKNOWN(rclone_error)" if remote is None else ("OK" if remote.get(name) == size else f"MISMATCH remote={remote.get(name)}")
        bad += state != "OK"; print(f"{rel}/{name} local={size} {state}")
print("not_ok", bad)
```

**P-HEALTH** (`ssh or-vps "bash -s" < p_health.sh`; run every 5 minutes during sessions. Silent when healthy, remediate and report only on a violation)

```bash
cd ~/krx-alpha/data
K=$(TZ=Asia/Seoul date +%T); D=$(TZ=Asia/Seoul date +%F)
echo "now=$K"
docker inspect krx-collector --format 'container restarts={{.RestartCount}} oom={{.State.OOMKilled}} started={{.State.StartedAt}} rev={{slice (index .Config.Labels "org.opencontainers.image.revision") 0 7}}'
docker logs --since 12m krx-collector 2>&1 | grep -E "stage=heartbeat" | tail -1 | sed -E 's/.*(state=[A-Z_]+ cycle=[0-9]+ streamer_alive=[A-Za-z]+ streamer_restarts=[0-9]+).*/hb: \1/'
echo "last_state: $(grep '"state_change"' logs/events-daemon.jsonl | tail -1 | sed -E 's/.*"ts": "([^"]+)".*to=([A-Z_]+).*/\1 -> \2/')"
for s in ls/krx/regular kis/krx/krx_after kis/nxt/nxt_after kis/nxt/nxt_pre; do n=$(find l0/$s -path "*dt=$D*" -name '*.zst' -printf '%T@\n' 2>/dev/null | sort -n | tail -1); [ -n "$n" ] && echo "l0 $s newest_age_s=$(( $(date +%s) - ${n%.*} ))" || echo "l0 $s none"; done
echo "crit_6m: $(docker logs --since 6m krx-collector 2>&1 | grep -cE 'level=CRITICAL|level=ERROR|Traceback')  restarts_6m: $(docker logs --since 6m krx-collector 2>&1 | grep -c 'status=RESTARTED')"
docker logs --since 6m krx-collector 2>&1 | grep -E 'level=CRITICAL|level=ERROR|status=RESTARTED|stream_disconnect' | cut -c1-230 | tail -4
t=$(ls -l --time-style=+%s ~/.cache/kis/token_ls_*.json 2>/dev/null | awk '{print $6}' | head -1); [ -n "$t" ] && echo "ls_token_age_h=$(( ($(date +%s)-t)/3600 ))"
cu=$(docker exec krx-collector id -u); o=$([ "$cu" = 0 ] || stat -c '%u %n' ~/.cache/kis/token_ls_* 2>/dev/null | awk -v u="$cu" '$1!=u'); echo "ls_token_owner_bad=$(printf '%s' "$o" | grep -c '^')${o:+ files: $o}"
echo "disk=$(df -h /home/ubuntu | awk 'NR==2{print $5}') mem=$(docker stats --no-stream --format '{{.MemUsage}}' krx-collector)"
```

Live monitoring pipelines must be unbuffered: end with `awk '{print substr($0,1,260); fflush()}'`, never `cut`/`sed` (they hold output until exit and silence the monitor; 2026-10-06). Prove a watcher fires once on a real event before trusting its silence.

**P-DASH** (host)

```python
import json, os
d = json.load(open(os.path.expanduser("~/quant-dashboard/public/status.json")))
def walk(x):
    if isinstance(x, dict):
        if "id" in x and "level" in x: yield x
        for v in x.values(): yield from walk(v)
    elif isinstance(x, list):
        for v in x: yield from walk(v)
cs = [c for c in walk(d) if str(c["id"]).startswith("krx.")]
print("generated", d.get("generated_at") or d.get("collected_at"), "krx_checks", len(cs))
for c in cs: print(c["level"], c["id"], "|", c["detail"][:90])
```

**X-VOL** (local, weekly; `D` must have an L1 partition, i.e. at least 4 calendar days old)

```bash
mkdir -p scratch/vps_probe && scp -q or-vps:~/krx-alpha/data/l1/ls/krx/regular/H0STCNT0/dt=$D.parquet scratch/vps_probe/ticks.parquet \
  && scp -q or-vps:~/krx-alpha/data/bars/daily/${D%-??}.parquet scratch/vps_probe/bars.parquet
D=$D PYTHONPATH=. uv run python - <<'PY'
import os, polars as pl
from src.storage.quality import decode_tick_raw_fields
D = os.environ["D"]
f = decode_tick_raw_fields(pl.read_parquet("scratch/vps_probe/ticks.parquet"))
g = f.with_columns(volume=pl.col("volume_raw").cast(pl.Int64, strict=False)).group_by("shcode").agg(pl.col("volume").max().alias("tick_vol"))
b = pl.read_parquet("scratch/vps_probe/bars.parquet").filter(pl.col("date").cast(pl.String) == D).select(pl.col("symbol").alias("shcode"), "volume")
j = g.join(b, on="shcode", how="left").with_columns(ratio=pl.col("tick_vol") / pl.col("volume"))
print("symbols", j.height, "unmatched", j["volume"].null_count(), "exact", int((j["ratio"] == 1.0).sum()))
print(j.filter(pl.col("ratio") != 1.0).sort("ratio").head(10))
PY
```

## 9. Baselines and benign patterns (2026-10-05 measurements)

Seeds for sanity and for "is this WARN the usual one". Refresh monthly or when the universe/policy changes; record the date.

| Item | Baseline |
|---|---|
| Daily bars per date | 2760–2766 rows; `market` null on legacy rows only |
| Program trades per date | ~2456–2461 |
| Regular candidates / subscribed symbols | 37–54 per day (pairs = 2x symbols); the 2026-10-01 selection produced 96 and `candidates.json` still holds that rev (>90 budget, pre-fix) until the 2026-10-06 08:20 orchestration rewrites it; expect ≤ budget afterwards (verify in S6, first run after the `c6bbfb5` fix on a business day) |
| Aftermarket plan | 2 venues x 2 shards x 40 pairs (20 symbols x 2 streams), `capacity` 40, all acks accepted, gaps 0 |
| Expected L0 streams on a business day | `ls/krx/regular/{H0STCNT0,H0STASP0}` hours 08–20; `kis/krx/krx_after/{H0STCNT0,H0STASP0}` hours 16–20; `kis/nxt/nxt_after/{H0NXCNT0,H0NXASP0}` hours 15–20; premarket `kis/nxt/nxt_pre/{H0NXCNT0,H0NXASP0}` hours 07–08 when enabled |
| L0 daily size | regular quote ~455 MB, regular tick ~165 MB, KRX aftermarket 52/20 MB, NXT aftermarket 27/15 MB at the 2026-10-05 seed. Regular-stream size scales with the candidate count, so compare per symbol: quote 3.7–5.1 MB/symbol, tick 1.6–2.0 MB/symbol (2026-10-06 59 symbols 261/96 MB, 10-07 79 symbols 401/155 MB, 10-08 37 symbols 138/59 MB). Hourly file counts differ by shard suffix and by quiet hours (regular tick has no `20` file when no print lands at 20:00); judge hour coverage, not file count |
| Snapshot kinds (9) | `stock_minute_bar` 23460 rows (60 symbols x 391), `ranking` ~23400, `index_minute_bar` 1173, `index_snapshot` 234, `news_title` 4700–5800; `program_trade`, `security_status`, `investor_estimate`, `auction_book` scale with candidate count (2026-10-02: x2.2 because candidates were 96 vs ~42) |
| NXT premarket (2026-10-06 first run) | 20 symbols all with data; ~188K ticks and ~211K quotes; manifest 40/40 pairs; event time 08:00:00–08:50:08 |
| Aftermarket L1 (2026-10-02) | `krx_after` ASP 508K / CNT 127K rows, `nxt_after` ASP 244K / CNT 99K; DQ PASS except NXT CNT WARN (`tick_loss` 7) |
| Benign DQ WARN | `ls regular H0STASP0`: `decode_fail` ≤0.01%, `total_remain_short` ≤2.5%; `ls regular H0STCNT0`: `cum_volume_regression` ≤5 rows, `tick_loss` ≤2; `nxt_after H0NXCNT0`: `tick_loss` ≤0.02% of rows, `lost_volume` ≤200. Everything else PASS. FAIL is never benign |
| Known false FAIL footers | `kis/krx/krx_after/H0STCNT0` `dt=2026-09-29` and `dt=2026-10-01` (new-listing days misjudged by the old fixed ±30% band; rule fixed in `717225b`). Decided 2026-10-06 NOT to re-judge: the data is intact and only the footer label is wrong. Ignore these two verdicts permanently in Q1 and in weekly scans; the dashboard `krx.data_quality` WARN for them clears on 2026-10-08 |
| Premarket pool timing | `universe/premarket/N.json` is written on the preceding run, which can be well before 20:30 (2026-10-06 pool was generated 2026-10-03 00:24 across the weekend) |
| Aftermarket symbols with trades (C3, 2026-10-06 / 10-07) | `krx_after` CNT 40/40 symbols, dead (<100 trades) 8 / 10; `nxt_after` CNT 19 / 21 of 40 pool symbols, dead 0 (illiquid NXT symbols print no trades) |
| Known quarantine content | Deleted 2026-10-10 by user decision: the 14 legacy L0 files (`quarantine/ls/H0STCNT0` and `H0STASP0`, `dt=2026-09-09`, ~185 MB, quarantined by retention 2026-09-15). `quarantine/` is expected empty; any file in it is a new quarantine event (SEV2) |
| Dashboard `krx.daemon_heartbeat` on closed days | the daemon walks STREAMER_ACTIVE/FULL_ACTIVE on holidays with no streamer (`session SKIP reason=market_holiday`); `FULL_ACTIVE` + `streamer_alive=False` showed FAIL on 2026-10-09 until quant-dashboard `d124ad3` limited that rule to TRADING/SHIFTED days. Closed days expect every `krx.*` check OK or "휴장일 건너뜀" |
| Dashboard `krx.regular_stream` | a WARN at 15:22–15:25 on 2026-10-06 and 10-07 was the closing auction (no trades 15:20–15:30) judged as a stale tick file; fixed in quant-dashboard `28ccb6f` (deployed 2026-10-07 20:4x); expect no WARN from 2026-10-08 |
| Host backup duration (H7) | 2026-10-05 run 45 min (lock_wait 2666 s), 10-06 run 1 h 45 min (lock_wait 6174 s, shared Drive lock held by another project's backup), 10-07 run 1 min 40 s (lock_wait 0). Run time is dominated by the lock wait; judge `rc`/`last_ok_at`, and treat lock_wait >2 h or a run still active at the next 23:30 as SEV3 |
| Offload | local L1 size == remote size for every partition |
| Clock | manifest `clock_offset_ns` within ±1 ms; chrony offset <1 ms |
| Memory | idle ~80–100 MiB; EOD normalizer child ≤500 MiB |

## 10. Reporting

Write the report in Korean, English keys, as the chat reply; do not write report files. Report only changes since the previous run plus the standing SEV1/SEV2 list.

```text
VPS-AUDIT <D> <mode>  run=<NOW KST>  rev=<short>  kind=<BUSINESS|HOLIDAY|WEEKEND|SHIFTED>
VERDICT: GREEN | AMBER (SEV3 only) | RED (SEV2) | CRITICAL (SEV1)
SEV1/SEV2: <ID> <한 줄 요약> | evidence: <command/value> | impact | proposed action (미실행)
CHANGED since <prev run>: <new / resolved / worsened>
CHECKED: H n/n · S n/n · C n/n · Q n/n · X n/n · A n/n   (PASS / WARN / FAIL / UNKNOWN counts)
UNKNOWN: <ID> <사유> (what is needed to resolve)
DATA CONTENT: <the content checks that ran and their key numbers, e.g. bars 2766 rows ratio 1.00; ticks==bars 42/42>
NEXT: <next mode and time; pending known events, e.g. 22:00 recreate, 내일 첫 프리마켓>
```

SEV1: notify immediately (section 2). A report is not complete until every ID of the run's modes has a status; list skipped IDs with the reason.

## 11. Failure triage (stop after reporting; no deploy 08:10–22:00)

| Symptom | Look at |
|---|---|
| `candidates_not_ready` / `orchestration_error` | events message (`selected N exceeds slot_budget M`, bars refresh timeout `data-dbg.krx.co.kr`), `candidates.json` rev, `universe/` for P; recurring KRX timeouts are retried every 5 min |
| `streamer RESTARTED` x5 then `circuit_open` right after `STREAMER_ACTIVE`; traceback `ClientConnectionResetError: Cannot write to closing transport` in `ls.py subscribe` | stale LS token (H10); rename `~/.cache/kis/token_ls_*.json` aside, restart the container, confirm `stage=shared_token status=PUBLISHED` and `stream_connect accepted=<pairs>`; keep file ownership (uid 1001) |
| `ls_token_owner_bad>0` in P-HEALTH | token cache files owned by another uid while the container runs non-root (H10; with the current root container this never fires); the in-container store raises `PermissionError` before `stream_connect` | `docker exec krx-collector chown <container uid>:<container gid> /run/kis-token-cache/token_ls_*` (and the lock files), then confirm `ls_token_age_h` and the next `stream_connect` |
| `stage=ingest_watchdog status=NO_FIRST_FRAME` on a business day 08:36–09:05 (S8) | regular streamer produced no frame after the 08:30 print was due (all subscriptions rejected, wrong token, silent socket) | same remediation chain as H11 (`L0 file stalled / zero size` row); if the 08:20–08:41 window is affected, the 08:30–08:40 prints are lost |
| Daemon restarted in session | `shutdown` event (`signal`, `graceful`), `daemon_lifecycle.json`, CI/deploy times vs freeze |
| Manifest `degraded_reason`, ack shortfall | `subscription_acks[].code`; KIS 41-pair/connection cap; key lease `work/kis_ws_leases/<key_id>.lock` (`key_lease_busy`) |
| L0 file stalled / zero size | `docker logs --since 30m`, `pgrep -af collect-` in container, disk (H3), clock (H4) |
| `StorageExhaustedError` | disk below floor; free space only after 22:00 |
| `ClockUnsyncedError` | `chronyc tracking`, NTP reachability |
| DQ FAIL | X2; check universe for new listings, vendor format change (decode_fail), then session clock |
| EOD not done by 21:00 | `stage=eod_*` CRITICAL, `rclone` auth (`auth_expired`), disk, normalize timeout `L1WorkerCrashError` (memory) |
| Offload mismatch | `rclone lsjson` sizes, partition re-normalized after verification (H8), remote auth |
| Alert silence | A1/A2, `alert_ledger.json`, SMTP failure `reason=` in logs |
| Dashboard disagrees | P-DASH vs evidence; the adapter's allow-list (`_VERIFIED_STAGES`) hides stages it does not list |

## 12. Maintaining this runbook

- Every new automated routine (timer, session, stream, snapshot kind, store) gets: a timeline row (section 6), a presence check (C), a content check (Q), and an expected-stream baseline (section 9). A routine without a content check is incomplete.
- Every incident adds one of: a new check, a new benign pattern, or a triage row. Date the baseline change.
- Prefer a new column/row over prose. Keep probes tested; re-run them against production after any schema change and update "tested" date.
- Feature modules are appended as `## NP`-style sections with a lifecycle line (`trial until <date>` or `stable`) and merge into the main tables once stable.

## NP. Module: NXT premarket (lifecycle: trial; first session 2026-10-06)

Enabled by `KRX_ALPHA_PREMARKET_ENABLED` and `PREMARKET_CREDENTIAL_SLOT=5` in `docker-compose.yml`. Window 08:00–08:50, pool file `universe/premarket/D.json` (`capacity` 20), manifest `manifest/premarket/D.nxt.shard-00.json`, L0 `l0/kis/nxt/nxt_pre/{H0NXCNT0,H0NXASP0}/dt=D/`. Run in M0 (arming) and M3 (results).

| ID | Check | How | PASS |
|---|---|---|---|
| NP1 | Pool armed (M0, previous evening M3) | `ls -l universe/premarket/D.json`; fields `selected_count`, `effective_from` ≤ 07:58 | file exists; `selected_count == capacity` or `eligible_count` shortfall explained |
| NP2 | Manifest closed, ACKs complete (M3) | P-MANIFEST filtered to `session == nxt_pre` | `closed` set, `acks_ok == planned == 40`, `degraded=None` |
| NP3 | Gaps (M3) | P-MANIFEST `max_gap_s`, `reasons` | max gap ≤30 s; no `restart` gaps; compare `watchdog` gaps with `manifest/aftermarket/D.nxt.shard-00.json` |
| NP4 | L0 present, both streams (M3) | P-L0 | files `07`/`08.s0.jsonl.zst` for both streams, sizes >0 |
| NP5 | Symbols with data (M3) | C3 command on `nxt/nxt_pre` | 15–20 symbols with trades; record counts as baseline |
| NP6 | Event time sane (M3) | first/last `exchange_event_time` of the stream | HHMMSS, first ≥ 075800, last ≤ 085500 (else V2 time-field assumption) |
| NP7 | EOD result (M3) | events `eod_maintenance`; `docker logs --since 14h \| grep premarket_eod` | `status=OK`, `accepted=40 planned=40`; premarket never degrades EOD |
| NP8 | Normalization (W; L1 exists from D+4) | P-DQ stream `kis/nxt/nxt_pre/*`; `market_phase` | both streams L1 present; `OK`/WARN with `tick_loss` <0.1%; `premarket` dominates, `unclassified` <1%; nothing in `quarantine/` |
| NP9 | Offload (W) | P-REMOTE | remote has `l1/kis/nxt/nxt_pre/<stream>/dt=<D>.parquet` |
| NP10 | Next pool (M3) | `ls -l universe/premarket/N.json`; `docker logs --since 14h \| grep premarket_pool` | `status=OK target=N symbols=20`; else tomorrow's premarket is skipped (fix after 22:00 only) |
| NP11 | Quick post-run (M1 at 08:55) | `docker logs --since 2h \| grep -E "premarket_(stream\|plan\|supervise)"`; `tail -3 logs/events-cli-collect-premarket.jsonl` | `status=START` then `STOP`; no `SKIP`, `RESTARTED`, `circuit_open`, `FAIL`; `rejected=0`; no `stream_gap`/`stream_disconnect` bursts |
| NP12 | Calibration (W) | max in-window gap of the week; `grep key_lease_busy`; slot env names only | set `KRX_ALPHA_PREMARKET_SILENCE_LIMIT_S` ≈2.6x largest normal gap; no `key_lease_busy`; `KIS_DECISION_SHARD_SLOTS` / `KIS_HOST_DATA_SLOTS` unchanged |

Disable switch: `KRX_ALPHA_PREMARKET_ENABLED` in `docker-compose.yml`; change only outside 08:10–22:00, then confirm CI deploy green and the container Up.
