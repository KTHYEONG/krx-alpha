# VPS Daily Automation Audit Runbook (krx-alpha)

Agent-executable. Verify that every automated routine ran **and that what it wrote is correct**, then report. New automation is added as rows (section 12), never as a new document.

- Host `or-vps` (SSH alias; host clock is UTC, always print KST with `TZ=Asia/Seoul`), container `krx-collector`, data root host `~/krx-alpha/data` = container `/app/data`.
- Probes last run against production 2026-10-18 (rev `40ad278`): L0 of 2026-10-13..16, L1 of 2026-10-12, snapshots/bars/backup/offload as of 2026-10-18. Never exercised: A3/A4 failure paths, deferred recreate with a failed verify, `program_trades_sync` after fix `c8288e7` (first run 2026-10-19 ~20:40).
- Report in Korean; keys, IDs, badges in English (section 10). This file stays English.
- Retired IDs (do not reuse): H9 (merged into H1), C3 (merged into C8), Q7 (merged into C7).

## 1. Operating rules (non-negotiable)

1. **Read-only, leave nothing behind.** No restart, deploy, file write, `rclone` write (`copy/sync/move/purge/delete/mkdir`), or config edit on the VPS; `rclone lsjson/lsf/size/check` are read-only and allowed. Pipe every probe as a heredoc over `ssh` stdin; write no files on the VPS or locally. Remediation is proposed, never executed, unless the user (or `docs/vps-ops-agent.md`) authorizes it; deploys still obey rule 10.
2. **No secrets.** Never print env files, tokens, app keys, rclone config (`~/.config/rclone/*`, `~/krx-alpha/rclone.conf`), or `~/.cache/kis` contents (names and mtimes only). `docker inspect` only with `--format`.
3. **UNKNOWN is not PASS.** Missing file, unreachable host, expired evidence, or probe error = `UNKNOWN` with reason.
4. **Day kind first** (section 4). The same silence is PASS on a closed day and SEV1 on a business day.
5. **Baselines are relative.** Judge counts against the trailing median of the previous 5 comparable days, normalized per subscribed symbol where the volume scales with the universe (regular streams, candidate-scaled snapshots). Section 9 numbers are seeds.
6. **Primary evidence only.** Never accept a dashboard, log verdict, or DQ footer as proof; confirm from files, manifests, parquet content, remote listings. Audit the dashboard itself (A5).
7. **A pipeline FAIL is a finding, not a conclusion.** Test whether the rule is wrong before calling a data defect.
8. **Evidence expires** (section 5). Run M3 before 22:00.
9. **Load discipline.** Content probes on business days only at/after 20:35 KST (or on closed days), wrapped in `nice -n 19`, parquet reads column-pruned. Never inside 08:10–20:30 on a business day.
10. **Freeze.** Never recommend or perform a deploy/restart on a business day 08:10–22:00 KST except to stop an active data-loss incident (say so explicitly).

## 2. Severity

| Sev | Definition | Examples | Action |
|---|---|---|---|
| SEV1 | Data is being or will be lost; fail-closed guard tripped | no ticks in a session window; streamer down >5 min in session; `ClockUnsyncedError`; disk >90%; EOD not done by 21:00; offload mismatch with L0 already deleted; business day skipped; conservation inequality | Notify immediately (first line of the reply), then finish the audit |
| SEV2 | Data present but suspect; redundancy or monitoring degraded | content invariant violated; snapshot kind missing; host backup stale >26 h; remote auth expired; unexplained CRITICAL; dashboard OK while ground truth FAIL | Headline; propose fix and deadline |
| SEV3 | Anomaly without data impact | benign WARN drifting; one gap ≤30 s; other project's failed unit | Body |
| INFO | Context | baselines, next run | Body |

A finding keeps its severity until its evidence is gone; re-report only on change.

## 3. Run modes

Every run starts with C0 and ends with the report.

| Mode | When (KST) | Checks |
|---|---|---|
| M0 pre-open | business day 07:45–08:15 | C0, H1–H8, H10, H12, H14, H15, S1, S7, C5, C6, C7 (premarket D), A1, DB1, X1 (on P) |
| M1 open | business day 09:05–09:30 | C0, H2, H10, H11, S2–S4, S6–S8, C1 (current hour), C2 (regular) |
| M2 close | business day 15:45–16:15 | C0, H11, S2, C2, C4, C7 (aftermarket D) |
| **M3 EOD** | business day 20:35–21:45 (**finish before 22:00**) | all rows of sections 7, NP, DB except W-only rows |
| M4 night | every day 23:45–00:15 | H5–H7, H12–H15, C5, C7 (premarket N), S1 (N) |
| W weekly | Saturday 10:00 | W rows, X1 (oldest local L0 day), H3 trend, NP8, NP12 |
| X incident | on demand | section 11, then affected rows |

Closed day (holiday/weekend): run M0-lite (H1–H7, H12, S1) and M4; expected state idle. Weekend: daemon sits in `WEEKEND_SLEEP` (heartbeat every ~10 min), runs **no** EOD, does not rewrite `calendar_cache.json`, and launches no program sync. Holidays on weekdays walk the normal states with `session SKIP reason=market_holiday` and run a holiday EOD (`eod_maintenance status=HOLIDAY`).

## 4. C0 context bootstrap (every run)

Run P-CTX. Derive:

- `NOW` KST and `KIND` of today and tomorrow. Sources: weekday; dashboard `[market]` `closed_dates`/`shifted_dates`; `calendar_cache.json` (`is_business_day`, `previous_business_day` = `P`, `next_business_day` = `N`) — valid only when its `date == today`; on weekends and before 08:20 it legitimately holds the last business day, so derive `P`/`N` from it as "last business day" and its `next_business_day`. Disagreement = S1 finding.
- `calendar/dt=<day>.json` exists only for business days (`regular_open`, `closing_auction_start`, `regular_close`, `after_market_end`, `source`). `SHIFTED` days move every time in this runbook. `source != vendor` = defaults in use (SEV3).
- `D` = session under audit: today if business day and `NOW` ≥ 08:20, else `P`.
- Deployed `rev` (container label) must equal `git rev-parse origin/main` locally; mismatch or a container start inside 08:10–22:00 on a business day = H1/S4 finding.

## 5. Evidence volatility

| Evidence | Lifetime | Consequence |
|---|---|---|
| `docker logs krx-collector` (heartbeat, `stage=normalize`, `snapshot_summary`, `digest`, `program_trades_sync STARTED/OK`) | until next container recreate (22:00 Mon–Fri when an image is pending, any CI deploy) and 30 MB cap | read in M3 before 22:00 with `docker logs --since 14h \| grep ...` |
| `data/logs/events-*.jsonl` | persistent, 5 MiB x5 rotation; **WARNING+ plus flagged INFO only** (`state_change`, `start`, `eod_maintenance`, `session SKIP`, `streamer_stop`) | absence of other INFO proves nothing |
| `manifest/**`, `universe/**`, `calendar/**`, `candidates.json` (overwritten 08:20 each business day) | persistent | primary evidence for plans and boundaries; the regular manifest `manifest/D.json` acked symbols is the persistent record of D's candidate set |
| L0 `l0/<vendor>/<venue>/<session>/<stream>/dt=D/*.jsonl.zst` | 4 newest dates per stream (current + `journal_retain_days`=3) | only source of same-day content |
| L1 `l1/<vendor>/<venue>/<session>/<stream>/dt=D.parquet`, `l1/snapshot/<kind>/dt=D.parquet` | tick L1 for D created at the first business/holiday EOD with D < today−3 (no EOD on weekends), then L0 deleted after remote verification; local 30 days, remote permanent. Snapshots written same day | tick DQ for D exists from ~D+4 |
| Dashboard `~/quant-dashboard/public/status.json` | refreshed every minute | snapshot only |
| Host units, `~/.local/state/krx-alpha/host_backup_status.json`, `~/logs/krx-host-backup-<UTC date>.log` | persistent | — |

## 6. Business day timeline

Standard anchors; shift per C0. "By" = latest acceptable time.

| Time | Event | Verify | If missed |
|---|---|---|---|
| 05:30 daily | `vps-image-gc` (krx-owned host unit) | H15 | SEV3 |
| 07:05 | Shared KIS token warm-up (kca timer); krx issues only if the cache is empty | no `kis_token_preflight` CRITICAL | SEV1 for KIS paths |
| 07:58–08:50 | NXT premarket pool effective, stream 08:00–08:50 | NP rows | SEV2 |
| 08:20 | `STREAMER_ACTIVE`: calendar gate, bars refresh, `market_map.json`, `candidates.json`, LS token reissue | S2, S6, S7, H10 | `candidates_not_ready` still failing at 08:50 = SEV1 |
| by 08:50 | `FULL_ACTIVE` | S2, S3 | SEV1 |
| 09:00 | first regular ticks | H11, C1 | SEV1 |
| 08:00–15:39 | Snapshot REST jobs (9 kinds) | C4 | SEV2 |
| 15:20–15:30 | Closing auction (no trades in tick stream) | C2 gaps | SEV2 |
| by 15:31 | `universe/aftermarket/D.json` (KRX, J screens) and `D.nxt.json` (NXT, NX screens) | C7 | SEV1 for aftermarket |
| 15:40 | `AFTER_MARKET_ACTIVE`; NXT aftermarket 15:40–20:00, KRX aftermarket 16:00–20:00 (KIS shards); LS regular stream keeps carrying KRX aftermarket prints until 20:00 | H11, C2 | SEV2 |
| 20:00 | `POST_MARKET_EOD`: writers close within ~2 s, EOD normalize → offload → verified L0 delete → remote L0 purge → reconcile → backup freshness → digest | C2, S5 | SEV2 |
| by 20:45 (Monday and post-holiday batch with `deleted_partitions` ≥200: by 21:00, 2026-10-12 20:51, 10-19 20:58 — tracked slowdown, see S5) | `eod_maintenance status=OK` (2026-10-12..16: 20:27–20:51); then `program_trades_sync STARTED` (timeout 2400 s from launch); `premarket_pool status=OK target=N`; `NIGHT_SLEEP` | S5, C5, C7 | WARN 20:45, **SEV1 21:00**; sync FAIL = SEV2 |
| 22:00 Mon–Fri | `krx-deferred-recreate` (recreates only if a pulled image is pending) + `deferred_verify` | H14 | SEV2 |
| 23:30 daily | `krx-host-backup` to Drive (shared lock) | H7 | SEV2 if `last_ok_at` >26 h |

Neighbors sharing `~/.cache/kis`, the LS key, DB금융투자 key, Toss key, KIS data slots (`deploy/shared/kis_slot_assignments.toml`) and the Drive lock. Attribute H12 contention by time; their own failures are SEV3 here (audited by `k-closing-alpha/docs/vps-checklist.md`, `k-stock-engine/docs/vps-checklist.md`).

| Time KST | Job | Shared resource |
|---|---|---|
| 07:05 / 07:10 | `kca-kis-token-warmup` / `kca-kiwoom-token-rotate` | KIS token files |
| 08:30, 11:30, 21:30 | `kca-price-ingest` | Kiwoom, Toss |
| 08:32 / 08:39:30 | `kse-morning` / `kse-auction-open` | KIS slot 5 REST (krx premarket WS uses slot 5) |
| 08:39:55–09:01 | `kca-auction-open`, `kca-order-exit`, `kca-order-reconcile-exit`, `kca-paper-exit` | KIS main account |
| 15:20–15:45 | `kca-collect/predict/auction-close/order-entry/finalize-close/aftermarket-book/archive-intraday-regular/auction-path` | KIS slots 1·5, Toss `STOCK_TRADING_TREND` (shared with krx snapshots), Kiwoom, DB금융투자 |
| 18:30–19:40 | `kse-collect-eve` | LS REST (`admission_ls_*`), KIS slot 5 |
| 20:05 / 20:35 / 20:40 | `kca-archive-intraday` / `kca-tape-sweep` / `kse-collect-dbsec` | LS, DB금융투자 (1 token/min/key), Kiwoom |
| 22:15 / 23:05 / 02:30 / 21:30 | `kca-backup` / `kca-extended-backfill` / `kse-backup` / `crypto-pilot-backup` | Drive lock with `krx-host-backup` 23:30 |

## 7. Check catalogue

Commands run on the host after `cd ~/krx-alpha/data` unless the probe says container. In cells `\|` is a markdown-escaped shell pipe; type `|`.

### H: Host, units, storage

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| H1 | M0 M3 M4 | Container | P-CTX | `krx-collector` Up, `restarts=0`, `oom=false`, `rev` = `origin/main`, start outside business-day 08:10–22:00; latest `gh run list --limit 3` (local) deploy job `success` | restart/OOM = SEV2; unexplained in-freeze start = SEV2; deploy job not `success` = SEV2 (pushes can succeed while deploy fails). Other containers (`mhs-live-daemon`, `market-normalizer`, `market-capture-blue`) INFO |
| H2 | M0 M1 M3 | Memory | `docker stats --no-stream --format '{{.MemUsage}}' krx-collector` | ≤ section 9 phase band; limit 1 GiB | >800 MiB WARN; OOM SEV2 |
| H3 | M0 M3 W | Disk | `df -h /`; `du -sh l0 l1` | <70% | 70–85% WARN; >85% FAIL (SEV1 >90%); W: 30-day growth |
| H4 | M0 M3 | Clock | `chronyc tracking \| grep -E 'System time\|Leap'` | \|offset\| <0.5 s, Leap Normal | >0.5 s WARN; >2.0 s SEV1 |
| H5 | M0 M4 | Failed units + alert path | `systemctl --user --failed --no-legend; systemctl --failed --no-legend; systemctl --user cat kca-alert@.service >/dev/null && echo alert_ok` | no `krx-*`/`vps-image-gc` failed; `alert_ok` (krx units' `OnFailure=` uses kca's template) | krx unit failed = SEV2; template missing = SEV2 (krx unit failures silent); other project = SEV3 |
| H6 | M0 M4 | Timers armed | `systemctl --user list-timers --all --no-pager \| grep -E 'krx-\|vps-image-gc'`; `systemctl --user is-enabled krx-host-backup.timer krx-deferred-recreate.timer vps-image-gc.timer` | 3 timers `enabled` with a future NEXT: backup 23:30 daily, recreate Mon–Fri 22:00, image-gc 05:30 daily (list-timers prints UTC). NEXT `-` while the service runs is normal | missing/stale = SEV2 |
| H7 | M0 M4 | Host backup ran and is complete | `cat ~/.local/state/krx-alpha/host_backup_status.json`; P-STORE `backup:` line | `rc=data_rc=prune_rc=0`, `last_ok_at` <26 h; P-STORE `backup` has 0 `+`/`*`/`!` entries older than `attempt_started_at` (newer ones = written since, normal on business days). At M4 the 23:30 run may still hold the lock (lock_wait up to 2 h): PASS if `journalctl --user -u krx-host-backup --since today` shows `Starting` and `last_ok_at` <26 h | non-zero rc or stale = SEV2; `STALE_DIFF` = SEV2 (file never reached Drive); lock_wait >2 h or still running at next 23:30 = SEV3 |
| H8 | M3 W | Offload completeness | P-STORE `offload l1` and `offload manifest` | every local L1 partition (ticks and snapshots) and manifest present remotely with identical size: only `=` lines, `rc=0` (2026-10-18: 247 L1 `=`, 103 manifests `=`) | `+` or `*` = SEV2 (SEV1 if its L0 is already deleted); `!`/rc≠0 = UNKNOWN then SEV2 (`auth_expired`) |
| H10 | M0 M1 | LS token age | P-HEALTH `ls_token_age_h`, `ls_token_owner_bad` | M1 (08:21–09:20): age <1 (reissued ~08:20); later on business days <23. Before 08:20 and on closed days informational (2026-10-18: 57 h over the weekend) | no reissue by M1 or ≥23 h in session = SEV2 (LS drops the socket at subscribe; 2026-10-06 crash loop). `ls_token_owner_bad>0` = SEV2 (only possible if the container stops running as uid 0) |
| H11 | 5-min in session | Live L0 freshness | P-HEALTH `l0 <stream> newest_age_s` | `ls/krx/regular` ≤180 s during 09:05–15:20 and 16:05–20:00 (exempt 15:20–16:05: closing auction and post-close quiet, max gap ~570 s); `kis/krx/krx_after` ≤120 s 16:05–20:00; `kis/nxt/nxt_after` ≤120 s 15:45–20:00; `kis/nxt/nxt_pre` ≤120 s 08:02–08:50. Observed max inter-arrival 2026-10-15/16: regular 17 s (session), 33 s (16–20h); KIS after ≤3.4 s | stale in window = SEV1 unless a market-wide halt (section 11) |
| H12 | M0 M4 W | Shared broker dir (contract v1) | P-BROKER | `marker=present`; `unexpected=` only `token_ls_<sha>.json.stale-20261006` (awaits user deletion); `canonical_sha` krx = kca = kse; `contract_errors_24h` 0/0 | marker missing = SEV1; SHA mismatch = SEV2; contract errors = SEV2; new unexpected file = SEV3 |
| H13 | M4 | Stop grace | P-BROKER `stop_timeout` | 95 | lower = SEV3 |
| H14 | M0 (Tue–Sat) M4 | Deferred recreate verified | `journalctl --user -u krx-deferred-recreate --since "26 hours ago" --no-pager \| grep -E 'deferred_verify\|Recreate\|Failed'`; `systemctl --user show krx-deferred-recreate.service -p Result` | `Result=success`; if a recreate happened: `deferred_verify status=ok check=stable` (its `sha` is the image pending at 22:00; a later CI deploy may supersede it, H1 judges the final `rev`) | `Result≠success` or `running_revision_mismatch` = SEV2 |
| H15 | M0 W | Image GC | `journalctl --user -u vps-image-gc --since "26 hours ago" --no-pager \| grep stage=image_gc` | `status=OK` (2026-10-16/17: removed 1/6, kept 13, protected 4) | missing/FAIL = SEV3 (disk creep) |

### S: Scheduling and daemon

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| S1 | M0 M3 M4 | Calendar | C0 sources; events `stage=session status=SKIP reason=market_holiday`; M4: `calendar/dt=<N>.json` with `source: vendor` | SKIP iff weekday holiday; anchors file only for business days | business day skipped = SEV1; holiday collected = SEV3; sources disagree = SEV2 |
| S2 | M1 M2 M3 | State timeline | P-EVENTS `states` for D | `PRE_MARKET_SLEEP 00:00 → STREAMER_ACTIVE 08:20 → FULL_ACTIVE 08:50 → AFTER_MARKET_ACTIVE 15:40 → POST_MARKET_EOD 20:00 → NIGHT_SLEEP` (~20:30–20:52, after EOD) each ±90 s; extra `NIGHT_SLEEP` entries after 22:00 = restarts (S4). No `streamer RESTARTED`/`circuit_open`: manifest gaps do not record time the streamer was down | late/missing transition = SEV2; skipped state = SEV1; `circuit_open` = SEV1 |
| S3 | M1 M2 | Heartbeat | `docker logs --since 30m krx-collector 2>&1 \| grep stage=heartbeat \| tail -3` | every ~10 min; `streamer_alive=True` in `FULL_ACTIVE`/`AFTER_MARKET_ACTIVE` | stale >15 min or `alive=False` in session = SEV1; `streamer_restarts>0` = SEV2 |
| S4 | M0 M3 | Starts | events `stage=start status=ONLINE` for D; `work/daemon_lifecycle.json` | starts only after 22:00 or on closed days | business-day start 08:10–22:00 = SEV2 (read `shutdown` `signal`/`graceful`); `daemon_restart status=UNCLEAN` = SEV2 |
| S5 | M3 | Event audit | P-EVENTS for D; `docker logs --since 14h krx-collector 2>&1 \| grep -E 'stage=eod_\|stage=session_reconciliation\|stage=backup_freshness\|stage=host_backup_freshness'` | `eod_maintenance ... status=OK` by 20:45 (Monday/post-holiday batch: by 21:00; record `deleted_partitions`/`purged` and the finish time each Monday — 10-12 20:51, 10-19 20:58 and rising); every non-INFO event explained by section 9 benign list or an open item | unexplained ERROR/CRITICAL = SEV2; `eod_maintenance DEGRADED`/`FAIL`, `session_reconciliation FAIL`, `backup_freshness STALE` = SEV1 |
| S6 | M1 | Candidates armed | P-UNIV `candidates` line (after 08:20) | `rev` = P (YYYYMMDD); n ≤ `universe_slot_budget` 100 (= LS 200 pairs / 2 streams); unique; no `candidates_not_ready`/`orchestration_error` since 08:20; `market_map.json` mtime today ~08:20 with ~2760 entries | stale rev/errors after 08:50 = SEV1; n=0 = SEV1 |
| S7 | M0 M1 M3 | Bars refresh | P-BARS `latest_date` | M0: business day before P; from 08:50: = P | older than P after 08:50 = SEV3 (KRX timeout WARN retried every 5 min) |
| S8 | M1 | First frame | events `stage=ingest_watchdog status=NO_FIRST_FRAME` for D (08:36–09:05) | absent | present = SEV1 |

### C: Collection completeness

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| C1 | M1 M3 | L0 inventory | P-L0 with D | 8 streams (section 9) with hour coverage: ls regular 08–20, krx_after 16–20, nxt_after 15–20, nxt_pre 08; `zero_size=0`; per-symbol MB within 0.5–2x section 9 band | missing stream/hour = SEV1 in session, SEV2 after; zero-size = SEV1 |
| C2 | M2 M3 | Manifests | P-MANIFEST with D | regular: `unacked=0`, `boots=1`, `max` gap ≤30 s, `clock measured` \|offset\| <5 ms, `cand_rev` = P; aftermarket 4 shards and premarket 1: `closed` within 2 s of session end, `acked_pairs == planned == 40`, `unacked=0`, `degraded None`. `ack_rows` > `acked_pairs` = resubscribe after a watchdog reconnect (benign if a matching gap span exists) | unacked/degraded = SEV2; `boots>1` or `restart` gap = SEV2; gap >30 s = SEV3 (SEV2 inside 15:20–15:30) |
| C4 | M2 M3 | Snapshots (9 kinds) | P-SNAP; `docker logs --since 14h krx-collector 2>&1 \| grep -E 'snapshot_summary\|snapshot_dq\|NO_SUCCESS\|STORE_FAIL'` | every kind present for D, `bad_date=0`, `nulls=0`; candidate-scaled kinds `symdiff=0` and rows/expected: `security_status`, `auction_book` = 1.000, `program_trade` ≥0.99, `investor_estimate` ≥0.95; fixed kinds: `stock_minute_bar` 23460 and `index_minute_bar` 1173 exact, `index_snapshot` ≥0.99 of 234, `ranking` ≥0.96 of 23400, `news_title` 0.90–1.10 of median; `snapshot_summary failed_jobs=0 expired=0`; no `snapshot_dq status=FAIL` | missing kind/0 rows = SEV2; below threshold or `symdiff>0` = SEV2; `snapshot_dq WARN` = SEV3 |
| C5 | M0 M4 | Program trades (`bars/program_trades/YYYY-MM.parquet`) | P-BARS `program_trades` lines; M3/M4: `docker logs --since 14h krx-collector 2>&1 \| grep program_trades_sync` | sync covers the session just closed: after `status=OK` latest date = D, else ≥ P; rows/date within ±2% of median (2449–2458); `program_trades_business_days_missing` empty except D before its sync logs OK | `STARTED` absent by 21:00 = SEV2; `FAIL`/`STALE` = SEV2; missing business day at next M0 = SEV2 (backfill lookback 120 d heals it on the next OK run; confirm) |
| C6 | M0 M3 | Daily bars (`bars/daily/YYYY-MM.parquet`) | P-BARS | latest = P after 08:20 refresh; n within ±0.5% of median (2761–2766) and ≈ `market_map.json` entries; `bars_business_days_missing` ⊆ {D, P before 08:20} (weekend: last business day, normal) | missing older day = SEV2; see Q5 |
| C7 | M0 M2 M3 M4 | Universe + plan consistency | P-UNIV with D (and N) | aftermarket D (by 15:31) and premarket D/N exist; per venue file (`D.json` KRX J screens, `D.nxt.json` NXT NX screens) `selected == n == unique == min(capacity, eligible)` (after 40, pre 20); `bad_sym=0`; per-venue shard `planned==universe True` (NXT shards checked against `D.nxt.json`), `acked==planned True`; premarket N `generated` ≤ 20:52 of the previous business day, `effective_from` 07:58; regular `same_set True` | missing = SEV1 for that session (premarket SEV3); inconsistency = SEV2 |
| C8 | M3 | Symbol coverage on L0 | P-L0C with D | ls regular `no_data=0`, `unplanned=0`; krx_after `no_data` ≤2, `thin_lt100` ≤10 of 40; nxt_pre `no_data=0`; nxt_after `no_data` ≤ open item OI-2 baseline (17–18 of 40) | worse than band = SEV3 (selection, change after freeze); regular `no_data>0` = SEV2 |

### Q: Data content

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| Q1 | M3 W | DQ footers | P-DQ | newest partition per stream: no FAIL; WARN only with section 9 benign counters | FAIL: reproduce (X2) before judging; real = SEV2, rule false positive = SEV3; unknown counter = SEV2 |
| Q2 | M3 | L1 volume | P-DQ `*_ratio_vs_median` (regular streams per acked symbol) | 0.6–1.6 | outside = SEV3, explain (universe mix, halt) |
| Q3 | M3 | Conservation (normalize) | `docker logs --since 14h krx-collector 2>&1 \| grep 'stage=normalize part='`; P-DQ `rows`/`footer_rows` | each part `raw_records == l1_rows + dedup_dropped`, `conn_seq_conflict=0`; next run: L1 `rows == footer_rows` | any inequality = SEV1 |
| Q4 | M3 W | Phase labels | P-DQ `unclassified`, P-L1C `phases` | <1% (observed ≤0.22% nxt_pre ASP); ls regular phases include `regular`, `aftermarket`, both auctions | ≥1% = SEV3 |
| Q5 | M0 M3 | Daily bar invariants | P-BARS | violation columns 0 for last 10 dates; `duplicate_date_symbol=0` | any >0 = SEV2 |
| Q6 | M3 | Snapshot invariants | P-SNAP2 | `bad_ohlc=0`, `dup=0`, `dup_news_id=0`, `null_title=0`, `net_mismatch=0`; `stock_minute_bar` `bar_time` 090000–153000 | violation = SEV2 |
| Q8 | M3 | Retention | `for s in l0/*/*/*/*; do echo "$s $(ls $s \| wc -l)"; done`; `find quarantine -type f \| wc -l` | ≤4 dates per stream; `quarantine` 0 files (empty dirs `quarantine/ls/*/dt=2026-09-09` are leftovers) | >4 dates without H8 failure = SEV3; any quarantine file = SEV2 |
| Q9 | M3 | L0 content invariants | P-L0C with D | `parse_fail=0`, `dup_seq=0`, `seq_regress=0`, `hour_mismatch=0`, `out_win=0`, `px_le0=0`, `crossed=0`; `evt_empty` 0 except ls ASP ≤0.01% of rec (observed 7–33); `B_per_rec` within section 9 band | any violation = SEV2 (corrupt/misfiled journal or decoder drift); `dup_seq`/`seq_regress` = SEV1 (writer replay) |
| Q10 | M3 W | L1 content invariants | P-L1C | per newest partition: `dup_seq=0`, `wrong_day=0`, `recv_regress=0`, `evt_empty` ≤ footer `decode_fail`; `evt` range inside the session window of section 9 | violation = SEV2 |
| X1 | M0 M3 W | Tick vs bar volume | X-VOL-L0 on P (bars for P exist from 08:20 of the next business day; L0 kept 3 days); W on L1 via X-VOL | ls regular: `exact` ≥ symbols−2, every ratio ≥0.98 (2026-10-12 43/43, 10-13 21/21, 10-14 21/22 min 0.988, 10-15 21/23 min 0.938 with a 09:01:14 disconnect); `unmatched=0` | ratio <0.98 not explained by a C2 gap = SEV2; KIS continuity variant: P-DQ `tick.tick_loss == 0` (and `lost_volume == 0`) for `kis/*/krx_after` and `kis/*/nxt_after` L1 partitions except counts explained by C2 manifest gap spans, EOD log `stage=aftermarket_prearm status=OK`; report-only: for LS-overlap symbols, first KIS tick `acc - cntg` vs LS cumulative at the venue open, and final KIS cumulative == LS at 19:59:59 |
| X2 | on Q1 FAIL | Reproduce verdict | in container after 20:35 or closed day: read the L1 partition in place with `src.storage.quality.decode_tick_raw_fields`, group violations by `shcode`/`sign`; copy nothing | root cause class named (feed, decoder, rule) | unresolved = SEV2 |

### A: Alerting and observability

| ID | Modes | Check | How | PASS | WARN / FAIL |
|---|---|---|---|---|---|
| A1 | M0 M3 | Alert path armed | `docker logs krx-collector 2>&1 \| grep -E 'stage=alert\|stage=healthcheck' \| tail -3` | `stage=alert status=ENABLED` after last start; no `healthcheck status=DISABLED` | alert `DISABLED` = SEV1; `FAIL` = SEV2 |
| A2 | M3 | CRITICAL vs mail | P-EVENTS CRITICAL keys for D vs `docker exec krx-collector cat /app/data/work/alert_ledger.json` (`sent_today`, per-key `count`) and `stage=alert status=SUPPRESSED` | each CRITICAL key mailed or suppressed; `sent_today` <20; no key at cap 3 unexplained | unaccounted CRITICAL = SEV2 |
| A3 | M3 | Digest | `docker logs --since 14h krx-collector 2>&1 \| grep stage=digest` | `status=SENT` after EOD | FAIL/missing = SEV3 |
| A4 | M0 M3 | External liveness | `docker logs --since 24h krx-collector 2>&1 \| grep stage=healthcheck` | none or `RECOVERED` | `FAIL` = SEV2 |
| A5 | M3 | Dashboard truth | P-DASH vs this run | 6 `krx.*` checks (`container`, `daemon_heartbeat`, `data_quality`, `host_backup`, `unit.krx-deferred-recreate`, `unit.krx-host-backup`) agree with evidence; `generated` <3 min; HTTP 200 | dashboard OK while SEV1/SEV2 open = SEV2 (name the uncovered check: it has no snapshot, offload, universe, content or image-gc check); dashboard FAIL with healthy evidence = SEV3 |

## 8. Probes (all executed 2026-10-18)

Host Python: `ssh or-vps "D=$D N=$N nice -n 19 python3 -" <<'PY' … PY`. Container: `ssh or-vps "nice -n 19 docker exec -i -e D=$D krx-collector /app/.venv/bin/python -" <<'PY' … PY`. Shell: `ssh or-vps 'bash -s' <<'SH' … SH`. Never write a probe to a file.

**P-CTX** (shell)

```bash
cd ~/krx-alpha/data
echo "now_kst=$(TZ=Asia/Seoul date '+%F %T %a')"
echo "calendar_cache=$(cat calendar_cache.json)"
for d in $(TZ=Asia/Seoul date +%F) $(TZ=Asia/Seoul date -d tomorrow +%F); do echo "anchors_$d=$(cat calendar/dt=$d.json 2>/dev/null || echo ABSENT)"; done
echo "dashboard_market=$(sed -n '/^\[market\]/,/^$/p' ~/quant-dashboard/config.toml | tr '\n' ' ')"
docker inspect krx-collector --format 'container started={{.State.StartedAt}} restarts={{.RestartCount}} oom={{.State.OOMKilled}} rev={{index .Config.Labels "org.opencontainers.image.revision"}}'
docker ps -a --format '{{.Names}} {{.Status}}'
echo "latest_l0=$(ls l0/ls/krx/regular/H0STCNT0 | tail -4 | tr '\n' ' ') latest_l1=$(ls l1/ls/krx/regular/H0STCNT0 | tail -2 | tr '\n' ' ')"
cat work/daemon_lifecycle.json; echo
```

**P-L0** (host; inventory)

```python
import glob, os, collections
D = os.environ["D"]; root = os.path.expanduser("~/krx-alpha/data/l0"); rows = collections.defaultdict(list)
for f in sorted(glob.glob(f"{root}/*/*/*/*/dt={D}/*.zst")):
    rows[f.split("/l0/")[1].split("/dt=")[0]].append((os.path.basename(f)[:2], os.path.getsize(f)))
for s, v in sorted(rows.items()):
    print(f"{s:28s} files={len(v):2d} hours={sorted({h for h, _ in v})} total_mb={sum(x for _, x in v) / 1e6:7.1f} zero_size={sum(1 for _, x in v if x == 0)}")
print("streams_found", len(rows))
```

**P-L0C** (host; L0 content of D, streams every record; ~3 min at 2.5M records)

```python
import collections, datetime as dt, glob, json, os, subprocess
D = os.environ["D"]; base = os.path.expanduser("~/krx-alpha/data"); K = dt.timezone(dt.timedelta(hours=9))
WIN = {"regular": ("083000", "200059"), "krx_after": ("154000", "200059"), "nxt_after": ("153000", "200059"), "nxt_pre": ("075800", "085059")}
def planned(session, venue):
    fs = {"regular": [f"{base}/manifest/{D}.json"], "nxt_pre": [f"{base}/manifest/premarket/{D}.nxt.shard-00.json"]}.get(
        session, glob.glob(f"{base}/manifest/aftermarket/{D}.{venue}.shard-*.json"))
    return {a["symbol"] for f in fs if os.path.exists(f) for a in json.load(open(f))["subscription_acks"] if a.get("accepted")}
for sdir in sorted(glob.glob(f"{base}/l0/*/*/*/*/dt={D}")):
    vendor, venue, session, stream = sdir.split("/l0/")[1].split("/")[:4]; quote = stream.endswith("ASP0"); lo, hi = WIN[session]
    n = bad = dup = regress = hour_mis = evt_empty = out_win = px_le0 = crossed = size = 0
    sym = collections.Counter(); last = {}; seen = set(); tmin, tmax = "999999", "000000"
    for f in sorted(glob.glob(f"{sdir}/*.zst")):
        size += os.path.getsize(f); fh = os.path.basename(f)[:2]
        for line in subprocess.Popen(["zstdcat", f], stdout=subprocess.PIPE).stdout:
            n += 1
            try:
                r = json.loads(line)
                if vendor == "ls":
                    j = json.loads(r["raw"]); b = j["body"]; s = j["header"]["tr_key"]; t = b["hotime" if quote else "chetime"]
                    a1, b1, px = (int(b["offerho1"]), int(b["bidho1"]), 1) if quote else (0, 0, int(b["price"]))
                else:
                    p = r["raw"].split("^"); s, t = p[0], p[1]
                    a1, b1, px = (int(p[3]), int(p[13]), 1) if quote else (0, 0, int(p[2]))
            except (ValueError, KeyError, IndexError, TypeError):
                bad += 1; continue
            sym[s] += 1; k = (r["conn_id"], r["conn_seq"]); dup += k in seen; seen.add(k)
            regress += r["conn_seq"] < last.get(r["conn_id"], -1); last[r["conn_id"]] = r["conn_seq"]
            hour_mis += dt.datetime.fromtimestamp(r["recv_wall_ns"] / 1e9, K).strftime("%H") != fh
            if not t: evt_empty += 1
            else: tmin, tmax = min(tmin, t), max(tmax, t); out_win += not lo <= t <= hi
            px_le0 += px <= 0; crossed += a1 > 0 and b1 > 0 and b1 >= a1
    pl = planned(session, venue)
    print(f"{vendor}/{venue}/{session}/{stream} rec={n} B_per_rec={size // max(n, 1)} parse_fail={bad} dup_seq={dup} seq_regress={regress} "
          f"hour_mismatch={hour_mis} evt={tmin}-{tmax} evt_empty={evt_empty} out_win={out_win} px_le0={px_le0} crossed={crossed} "
          f"syms={len(sym)} planned={len(pl)} no_data={len(pl - set(sym))} unplanned={len(set(sym) - pl)} thin_lt100={sum(1 for x in pl if 0 < sym[x] < 100)}")
```

**P-MANIFEST** (host)

```python
import datetime as dt, glob, json, os
D = os.environ["D"]; K = dt.timezone(dt.timedelta(hours=9))
hm = lambda ns: dt.datetime.fromtimestamp(ns / 1e9, K).strftime("%H:%M:%S")
for f in sorted(glob.glob(os.path.expanduser(f"~/krx-alpha/data/manifest/**/{D}*.json"), recursive=True)):
    m = json.load(open(f)); acc = {(a["symbol"], a["tr_id"]) for a in m["subscription_acks"] if a.get("accepted")}
    plan = {(p["symbol"], p["tr_id"]) for p in m["planned_pairs"]}
    spans = sorted({(g["gap_start_ns"], g["gap_end_ns"], g.get("reason")) for g in m["gaps"]})
    big = max(spans, key=lambda s: s[1] - s[0], default=None)
    print(os.path.basename(f), m["session"], "closed", m["writer_closed_at_ns"] and hm(m["writer_closed_at_ns"]),
          "acked_pairs", len(acc), "planned", len(plan) or "-", "unacked", len(plan - acc), "ack_rows", len(m["subscription_acks"]),
          "gap_spans", len(spans), "max", None if not big else f"{(big[1] - big[0]) / 1e9:.1f}s@{hm(big[0])}/{big[2]}",
          "boots", len(m["boots"]), "clock", m["clock_status"], f"{m['clock_offset_ns'] / 1e6:.3f}ms", "degraded", m["degraded_reason"], "cand_rev", m.get("candidates_rev"))
```

The regular manifest has no `planned_pairs` and no `writer_closed_at_ns` by design; its acked pairs = 2 x candidates.

**P-UNIV** (host; universe artifacts vs plans; set `N` for the next premarket pool)

```python
import glob, json, os, re
D, N = os.environ["D"], os.environ.get("N", "")
base = os.path.expanduser("~/krx-alpha/data"); ok = re.compile(r"^[0-9A-Z]{6}$")
def acked(fs): return {a["symbol"] for f in fs for a in json.load(open(f))["subscription_acks"] if a.get("accepted")}
def planned(fs): return {p["symbol"] for f in fs for p in json.load(open(f))["planned_pairs"]}
def uni(kind, day, venue_suffix=""):
    f = f"{base}/universe/{kind}/{day}{venue_suffix}.json"
    if not os.path.exists(f): print(kind, day, venue_suffix or "-", "MISSING"); return None
    u = json.load(open(f)); s = [c["symbol"] for c in u["candidates"]]
    print(kind, day, venue_suffix or "-", "selected", u["selected_count"], "n", len(s), "unique", len(set(s)), "capacity", u["capacity"], "eligible", u["eligible_count"],
          "bad_sym", sum(not ok.match(x) for x in s), "effective_from", u["effective_from"][11:19], "generated", u["generated_at"][:19])
    return set(s)
a = uni("aftermarket", D, "")
a_nxt = uni("aftermarket", D, ".nxt")
if a is not None:
    fs = sorted(glob.glob(f"{base}/manifest/aftermarket/{D}.krx.shard-*.json"))
    print(f"  krx shards", len(fs), "planned==universe", planned(fs) == a, "acked==planned", acked(fs) == planned(fs))
if a_nxt is not None:
    fs = sorted(glob.glob(f"{base}/manifest/aftermarket/{D}.nxt.shard-*.json"))
    print(f"  nxt shards", len(fs), "planned==universe", planned(fs) == a_nxt, "acked==planned", acked(fs) == planned(fs))
p = uni("premarket", D)
if p is not None:
    fs = glob.glob(f"{base}/manifest/premarket/{D}.nxt.shard-*.json"); print("  premarket planned==universe", planned(fs) == p if fs else "NO_MANIFEST")
if N: uni("premarket", N)
c = json.load(open(f"{base}/candidates.json")); cs = [x["symbol"] for x in c["candidates"]]
reg = acked([f"{base}/manifest/{D}.json"]) if os.path.exists(f"{base}/manifest/{D}.json") else set()
m = json.load(open(f"{base}/manifest/{D}.json")) if reg else {}
print("candidates rev", c["rev"], "n", len(cs), "unique", len(set(cs)), "bad_sym", sum(not ok.match(x) for x in cs), "dropped", c.get("dropped_count"),
      "| regular manifest candidates_rev", m.get("candidates_rev"), "acked_syms", len(reg), "same_set", set(cs) == reg)
```

`same_set` is meaningful only while `candidates.json` rev = `cand_rev` (until 08:20 of the next business day).

**P-EVENTS** (host; non-INFO events of D per events file plus state transitions)

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

**P-DQ** (container; L1 footers, ratios, phases; current layout only, partitions ≤14 days)

```python
import collections, datetime as dt, glob, json, statistics
import pyarrow.compute as pc
import pyarrow.parquet as pq
cut = (dt.date.today() - dt.timedelta(days=14)).isoformat()
def syms(d):
    try: return len({a["symbol"] for a in json.load(open(f"/app/data/manifest/{d}.json"))["subscription_acks"] if a.get("accepted")}) or None
    except OSError: return None
by = collections.defaultdict(list)
for f in sorted(glob.glob("/app/data/l1/*/*/*/*/dt=*.parquet")):
    if "/snapshot/" in f or f[-18:-8] < cut: continue
    pf = pq.ParquetFile(f); raw = (pf.metadata.metadata or {}).get(b"krx_alpha.dq"); dq = json.loads(raw) if raw else {}
    n = pf.metadata.num_rows; d = f[-18:-8]; per = syms(d) if "/ls/krx/regular/" in f else 1
    by[f.split("/l1/")[1].rsplit("/", 1)[0]].append((f, n, n / per if per else None, dq.get("status", "NONE"), dq.get("tick") or dq.get("quote") or {}))
for stream, v in sorted(by.items()):
    f, rows, norm, st, t = v[-1]; prev = [x[2] for x in v[:-1] if x[2]]; med = statistics.median(prev) if prev else None
    ph = pq.read_table(f, columns=["market_phase"]).column(0)
    unc = pc.sum(pc.equal(ph, "unclassified").cast("int64")).as_py() or 0
    print(f"{stream:28s} {f[-18:-8]} rows={rows} footer_rows={t.get('rows')} {'rows_per_sym' if '/regular/' in f else 'rows'}_ratio_vs_median={None if not (med and norm) else round(norm / med, 2)} "
          f"dq={st} hist={[x[3] for x in v][-6:]} unclassified={unc / rows:.4%} nonzero={ {k: x for k, x in t.items() if k != 'rows' and x} }")
```

**P-L1C** (container; independent L1 content, column-pruned)

```python
import glob
import polars as pl
for s in sorted({f.rsplit("/", 1)[0] for f in glob.glob("/app/data/l1/*/*/*/*/dt=*.parquet")}):
    f = sorted(glob.glob(f"{s}/dt=*.parquet"))[-1]; D = f[-18:-8]
    d = pl.read_parquet(f, columns=["conn_id", "conn_seq", "recv_wall_ns", "exchange_event_time", "market_phase"])
    day = pl.from_epoch(pl.col("recv_wall_ns"), time_unit="ns").dt.convert_time_zone("Asia/Seoul").dt.date().cast(pl.String)
    e = pl.col("exchange_event_time")
    r = d.select(pl.len().alias("rows"), (pl.len() - pl.struct("conn_id", "conn_seq").n_unique()).alias("dup_seq"),
                 (day != D).sum().alias("wrong_day"), (e.is_null() | (e == "")).sum().alias("evt_empty"),
                 e.filter(e != "").min().alias("evt_min"), e.max().alias("evt_max"),
                 (pl.col("recv_wall_ns").diff().over("conn_id") < 0).sum().alias("recv_regress")).row(0, named=True)
    ph = dict(d["market_phase"].value_counts(sort=True).iter_rows())
    print(s.split("/l1/")[1], D, r, "phases", {k: round(v / r["rows"], 4) for k, v in ph.items()})
```

**P-BARS** (container; daily bars and program trades)

```python
import datetime as dt, glob, statistics
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
p = pl.scan_parquet("/app/data/bars/program_trades/*.parquet").group_by("date").len().sort("date").tail(10).collect()
print("program_trades", p.rows())
cal = sorted(f[-15:-5] for f in glob.glob("/app/data/calendar/dt=*.json"))
today = dt.date.today().isoformat()
for name, have in (("bars", g["date"]), ("program_trades", p["date"])):
    lo = str(have[0])
    print(f"{name}_business_days_missing", sorted(set(d for d in cal if lo <= d < today) - {str(x) for x in have.to_list()}))
```

**P-SNAP** (container; presence, expected rows, key integrity for snapshot day D)

```python
import glob, json, os, statistics
import polars as pl
PER_SYM = {"security_status": 1, "auction_book": 9, "program_trade": 14, "investor_estimate": 20}
FIXED = {"stock_minute_bar": 23460, "index_minute_bar": 1173, "index_snapshot": 234, "ranking": 23400}
D = os.environ["D"]
try: plan = {a["symbol"] for a in json.load(open(f"/app/data/manifest/{D}.json"))["subscription_acks"] if a.get("accepted")}
except OSError: plan = set()
print(D, "regular_syms", len(plan))
for k in sorted(glob.glob("/app/data/l1/snapshot/*")):
    kind = k.rsplit("/", 1)[1]; f = f"{k}/dt={D}.parquet"
    if not os.path.exists(f): print("  ", kind, "MISSING"); continue
    df = pl.read_parquet(f); n = df.height
    exp = PER_SYM[kind] * len(plan) if kind in PER_SYM else FIXED.get(kind)
    if exp is None:
        prev = sorted(x for x in glob.glob(f"{k}/dt=*.parquet") if x < f)[-5:]
        exp = statistics.median([pl.scan_parquet(x).select(pl.len()).collect().item() for x in prev]) if prev else n
    bad_date = int((df["session_date"].cast(pl.String) != D).sum())
    nulls = sum(int(df[c].null_count()) for c in ("symbol", "observed_at_ns", "session_date") if c in df.columns)
    symdiff = len(set(df["symbol"].unique().to_list()) ^ plan) if kind in PER_SYM else "-"
    print("  ", f"{kind}={n}/{exp}({n / exp if exp else 0:.3f}) symdiff={symdiff} bad_date={bad_date} nulls={nulls}")
```

**P-SNAP2** (container; content invariants of the newest snapshot partitions)

```python
import glob
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
```

**P-STORE** (shell; offload and host-backup completeness, read-only `rclone check`, ~30 s)

```bash
R=~/.local/bin/rclone; RR=gdrive:quant-lake/live/krx-alpha/data; cd ~/krx-alpha/data
st=$(python3 -c "import json,os;print(json.load(open(os.path.expanduser('~/.local/state/krx-alpha/host_backup_status.json')))['attempt_started_at'])")
cutoff=$(date -d "$st" +%s)
chk() { nice -n 19 $R check "$@" --one-way --size-only --combined - 2>/dev/null; echo "rc=$?" >&2; }
for t in "l1 $RR/l1" "manifest $RR/manifests"; do
  set -- $t; out=$(chk "$1" "$2" 2>&1); echo "offload $1: $(printf '%s\n' "$out" | grep -v '^rc=' | cut -c1 | sort | uniq -c | tr -s ' \n' ' ') $(printf '%s\n' "$out" | grep '^rc=')"
  printf '%s\n' "$out" | grep -E '^[+*!] ' | head -5
done
out=$(chk . $RR --filter-from ../deploy/host/krx-alpha.rclone-filter --exclude '.env*' --exclude '*.key' --exclude '*_key.txt' 2>&1)
echo "backup: $(printf '%s\n' "$out" | grep -v '^rc=' | cut -c1 | sort | uniq -c | tr -s ' \n' ' ') $(printf '%s\n' "$out" | grep '^rc=') last_attempt=$st"
printf '%s\n' "$out" | grep -E '^[+*!] ' | while read -r k p; do m=$(stat -c %Y "$p" 2>/dev/null || echo 0); [ "$m" -gt "$cutoff" ] && echo "  newer_than_backup $k $p" || echo "  STALE_DIFF $k $p"; done | sort | uniq -c | sort -rn | head -8
```

`=` match, `+` local only, `*` size differs, `!` error. Remote `data/manifest/` and `data/work/` are legacy trees; the container uploads manifests to `manifests/`.

**P-HEALTH** (shell; every 5 min in session; silent when healthy)

```bash
cd ~/krx-alpha/data
D=$(TZ=Asia/Seoul date +%F); echo "now=$(TZ=Asia/Seoul date +%T)"
docker inspect krx-collector --format 'container restarts={{.RestartCount}} oom={{.State.OOMKilled}} started={{.State.StartedAt}} rev={{slice (index .Config.Labels "org.opencontainers.image.revision") 0 7}}'
docker logs --since 12m krx-collector 2>&1 | grep -E "stage=heartbeat" | tail -1 | sed -E 's/.*(state=[A-Z_]+ cycle=[0-9]+ streamer_alive=[A-Za-z]+ streamer_restarts=[0-9]+).*/hb: \1/'
echo "last_state: $(grep '"state_change"' logs/events-daemon.jsonl | tail -1 | sed -E 's/.*"ts": "([^"]+)".*to=([A-Z_]+).*/\1 -> \2/')"
for s in ls/krx/regular kis/krx/krx_after kis/nxt/nxt_after kis/nxt/nxt_pre; do n=$(find l0/$s -path "*dt=$D*" -name '*.zst' -printf '%T@\n' 2>/dev/null | sort -n | tail -1); [ -n "$n" ] && echo "l0 $s newest_age_s=$(( $(date +%s) - ${n%.*} ))" || echo "l0 $s none"; done
echo "crit_6m: $(docker logs --since 6m krx-collector 2>&1 | grep -cE 'level=CRITICAL|level=ERROR|Traceback')  restarts_6m: $(docker logs --since 6m krx-collector 2>&1 | grep -c 'status=RESTARTED')"
docker logs --since 6m krx-collector 2>&1 | grep -E 'level=CRITICAL|level=ERROR|status=RESTARTED|stream_disconnect' | cut -c1-230 | tail -4
t=$(ls -l --time-style=+%s ~/.cache/kis/token_ls_*.json 2>/dev/null | awk '{print $6}' | head -1); [ -n "$t" ] && echo "ls_token_age_h=$(( ($(date +%s)-t)/3600 ))"
cu=$(docker exec krx-collector id -u); o=$([ "$cu" = 0 ] || stat -c '%u %n' ~/.cache/kis/token_ls_* 2>/dev/null | awk -v u="$cu" '$1!=u'); echo "ls_token_owner_bad=$(printf '%s' "$o" | grep -c '^')${o:+ files: $o}"
echo "disk=$(df -h / | awk 'NR==2{print $5}') mem=$(docker stats --no-stream --format '{{.MemUsage}}' krx-collector)"
```

Live monitors must be unbuffered (`awk '{print substr($0,1,260); fflush()}'`, never `cut`/`sed` in a follow pipe). Prove a watcher fires once before trusting its silence.

**P-BROKER** (shell; names only)

```bash
d=~/.cache/kis
echo "marker=$([ -f $d/.host-admission ] && echo present || echo MISSING)"
echo "unexpected=$(ls -A $d | grep -vE '^(\.host-admission|token_(ls|toss|dbsec|kiwoom)_[0-9a-f]{12}\.json(\.lock)?|token_issue_(ls|toss|dbsec|kiwoom)_[0-9a-f]{12}\.state(\.lock)?|admission_(ls|toss|dbsec|kiwoom)_[0-9a-f]{12}(_[A-Za-z0-9_-]+)?\.state|token_[0-9a-f]{12}\.json(\.lock)?|tps_[0-9a-f]{12}\.state)$' | sed -E 's/[0-9a-f]{12}/<sha>/g' | tr '\n' ' ')"
echo "canonical_sha krx=$(docker exec krx-collector sha256sum /app/deploy/shared/host_broker_contract.py | cut -c1-12) kca=$(sha256sum ~/k-closing-alpha/src/api/host_broker_contract.py | cut -c1-12) kse=$(docker run --rm --entrypoint sha256sum ghcr.io/kthyeong/k-stock-engine:latest /app/src/integrations/host_broker_contract.py | cut -c1-12)"
echo "contract_errors_24h krx=$(docker logs --since 24h krx-collector 2>&1 | grep -cE 'IGW00201|EGW00201|TokenIssueThrottled|HostBrokerDirNotShared|SCHEMA_INVALID') units=$(journalctl --user --since '24 hours ago' --no-pager 2>/dev/null | grep -cE 'IGW00201|EGW00201|TokenIssueThrottled|HostBrokerDirNotShared|SCHEMA_INVALID')"
echo "stop_timeout=$(docker inspect krx-collector --format '{{.Config.StopTimeout}}') db_crosscheck_env=$(docker exec krx-collector sh -c 'env | grep -c KRX_ALPHA_DB_CROSSCHECK')"
```

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

Plus `curl -s -o /dev/null -w "%{http_code}" http://100.81.197.26:8765/`.

**X-VOL-L0** (container; D = a business day whose bars exist and whose L0 is still local; ~1 min)

```python
import glob, io, json, os, zstandard, polars as pl
D = os.environ["D"]; vol = {}; last = {}
for f in sorted(glob.glob(f"/app/data/l0/ls/krx/regular/H0STCNT0/dt={D}/*.zst")):
    with open(f, "rb") as fh:
        for line in io.TextIOWrapper(zstandard.ZstdDecompressor().stream_reader(fh), encoding="utf-8"):
            b = json.loads(json.loads(line)["raw"])["body"]; s = b["shcode"]; v = int(b["volume"])
            if v >= vol.get(s, 0): vol[s] = v; last[s] = b["chetime"]
t = pl.DataFrame({"symbol": list(vol), "tick_vol": list(vol.values()), "last_t": [last[s] for s in vol]})
b = pl.read_parquet(f"/app/data/bars/daily/{D[:7]}.parquet", columns=["date", "symbol", "volume"]).filter(pl.col("date").cast(pl.String) == D)
j = t.join(b, on="symbol", how="left").with_columns(ratio=pl.col("tick_vol") / pl.col("volume"))
print(D, "symbols", j.height, "unmatched", j["volume"].null_count(), "exact", int((j["ratio"] == 1.0).sum()), "within_1pct", int(((j["ratio"] - 1).abs() <= 0.01).sum()))
print(j.filter(pl.col("ratio") != 1.0).sort("ratio").head(10))
```

KIS variant (OI-3): glob `l0/kis/krx/krx_after/H0STCNT0`, `p = json.loads(line)["raw"].split("^")`, symbol `p[0]`, time `p[1]`, cumulative volume `p[13]`.

**X-VOL** (container, W; D with an L1 partition)

```python
import os, polars as pl
from src.storage.quality import decode_tick_raw_fields
D = os.environ["D"]
f = decode_tick_raw_fields(pl.read_parquet(f"/app/data/l1/ls/krx/regular/H0STCNT0/dt={D}.parquet"))
g = f.with_columns(volume=pl.col("volume_raw").cast(pl.Int64, strict=False)).group_by("shcode").agg(pl.col("volume").max().alias("tick_vol"))
b = pl.read_parquet(f"/app/data/bars/daily/{D[:7]}.parquet").filter(pl.col("date").cast(pl.String) == D).select(pl.col("symbol").alias("shcode"), "volume")
j = g.join(b, on="shcode", how="left").with_columns(ratio=pl.col("tick_vol") / pl.col("volume"))
print("symbols", j.height, "unmatched", j["volume"].null_count(), "exact", int((j["ratio"] == 1.0).sum()))
print(j.filter(pl.col("ratio") != 1.0).sort("ratio").head(10))
```

## 9. Baselines and benign patterns (measured 2026-10-12..16)

| Item | Baseline |
|---|---|
| Regular candidates / symbols | 7–43 per day (10-12 43, 10-13 21, 10-14 22, 10-15 23, 10-16 7); budget 100. Count follows the selection reasons (surge/limit-up/new-high) and drops on weak tapes |
| Regular L0 per symbol | tick 2.8–3.8 MB, 29K–55K records; quote 5.4–7.0 MB; `B_per_rec` tick 69–116, quote 149–185 |
| Aftermarket L0 (10-16) | krx_after ASP 35.9 MB / CNT 13.8 MB (`B_per_rec` 103/159); nxt_after ASP 28.2 / CNT 16.7 (106/126); hour `20` files ≤10 KB |
| Premarket L0 (10-16) | nxt_pre ASP 14.5 MB / CNT 16.5 MB, ~200K/300K records (`B_per_rec` 73/54), 20/20 symbols, evt 080000–085009 |
| L1 rows (10-06..12) | krx_after ASP 392–504K / CNT 91–145K; nxt_after ASP 337–385K / CNT 144–224K; nxt_pre ASP 185–215K / CNT 177–234K; ls regular scales with symbols (10-12: 1.07M ASP / 0.81M CNT for 43 symbols) |
| L0 symbol coverage (C8) | krx_after CNT 39/40 with data, 7–8 thin (<100); nxt_after 22–23 of 40 (OI-2); nxt_pre 20/20; regular all |
| Event-time windows | ls regular 083000–200000; krx_after 160000–200001; nxt_after 154007–200000; nxt_pre 080000–085009 |
| Manifest | regular: one `disconnect` span ≤17 s per day typical, clock \|offset\| ≤0.6 ms; aftermarket 4 shards x 40 pairs, gaps 0 except an occasional NXT `watchdog` span ≤13 s (10-16 shard-01 16:36:42 12.8 s, ack_rows 80 after resubscribe) |
| Snapshots per D | per regular symbol: `security_status` 1, `auction_book` 9, `program_trade` 14, `investor_estimate` 19–20; fixed: `stock_minute_bar` 23460 (60 symbols x 391, 090000–153000), `index_minute_bar` 1173, `index_snapshot` 232–234, `ranking` 22638–23400, `news_title` 4854–5236 |
| Daily bars | 2761–2766 rows/date; `market_map.json` 2762 entries |
| Program trades | 2449–2458 rows/date |
| Benign DQ WARN | ls regular ASP: `decode_fail` ≤0.01% (empty `hotime`), `total_remain_short` ≤2.5%; ls regular CNT: `cum_volume_regression` ≤5, `tick_loss` ≤2; krx_after CNT `cum_volume_regression` ≤1; nxt_after CNT `tick_loss` ≤0.02% of rows, `lost_volume` ≤200. FAIL is never benign. Ignore the old FAIL footers `kis/krx/krx_after/H0STCNT0` `dt=2026-09-29`, `dt=2026-10-01` (old ±30% band, data intact) |
| Benign events | `stream_disconnect`/`stream_gap reason=disconnect` (LS, ≤30 s) and `reason=watchdog` (KIS NXT) WARNINGs; `quality status=WARN` from the normalize worker with benign counters |
| EOD | `eod_maintenance status=OK` 20:21–20:51; `deleted_partitions` 0 on most days, ~70–210 when a 3-day-old batch ages out; `uploaded` 9 per normalized day |
| Host backup | lock_wait 0 on quiet nights, up to 6174 s when another project holds the Drive lock; run 20 s–2 min after the lock; P-STORE backup 400 `=` (10-18) |
| Clock | chrony offset <1 ms; manifest offsets ≤0.6 ms |
| Memory | weekend/idle 80–100 MiB; regular session 380–420 MiB; aftermarket with 4 KIS children ~610 MiB; EOD normalizer child ≤500 MiB |
| Dashboard on closed days | every `krx.*` OK; `daemon_heartbeat` shows `WEEKEND_SLEEP`/"휴장일 건너뜀" |

### Open items (re-check each run; remove when closed)

| ID | Since | Condition | Expected resolution / check |
|---|---|---|---|
| OI-2 | ≥2026-10-13 | NXT aftermarket shards subscribe the KRX-ranked aftermarket universe without an NXT-listing filter: 17–18 of 40 NXT symbols produce no frame at all (10-16: shard-00 5, shard-01 12, incl. `900270`, `900300`). Premarket uses NX-only rankings and has 20/20 | code defect (selection policy `src/universe/aftermarket.py`); track C8 `no_data` until fixed |
| OI-3 | ≥2026-10-14 | KIS `krx_after` max cumulative tick volume < daily bar volume for many symbols (10-14: exact 26/40, min 0.914 `005930`; 10-15: 29/39, min 0.961), last tick 19:59:59; LS regular mostly exact | closed as a definition difference: bars include off-book volume while tick cumulative does not (no tick loss); verify with the X1 KIS continuity variant |
| OI-4 | 2026-10-20 | LS regular open delivery lag grows from the first second of 09:00: recv-minus-chetime max 0 s (10-16) → 14 s (10-19) → 53 s (10-20, linear ~0.5 s/s). On 10-20 LS dropped the socket at 09:01:48 (manifest gap 4.8 s) and the undelivered ~55 s was never replayed: session_audit `confirmed_missing=15873` of 795893 trades (2.0 %, all 42 symbols, `chetime_prev` 09:00:5x → `chetime_next` 09:01:5x). Collector was not the bottleneck (10-19 sustained 1586 frames/s vs 630 peak on 10-20); quality status of the retained L0 stays PASS because the check has no count-continuity rule. Gap-fill: `data/gapfill/ls/regular/2026-10-20.{parquet,report.json}` holds 15 713 of the 15 873 lost trades (volume 99.5 % of the audited deficit) from the kca Kiwoom tick tape plus KIS `FHPST01060000` for symbols absent from that tape; rows carry `vendor`/`provenance`, L0 untouched; rerun with `gapfill-ticks` (heavy: run off the collector container, which OOM-kills a 5.5 M-row tape load) | each business day compare per-minute latency of 09:00–09:03 (`ingest_latency` lines, `manifest/audit/<D>.ls.regular.audit.json`); latency >10 s or `confirmed_missing` >0 inside 09:00–09:05 = SEV2 until a vendor-side cause or a second feed/gap-fill is decided |

## 10. Reporting

Korean, English keys, chat only; report changes since the previous run plus standing SEV1/SEV2.

```text
VPS-AUDIT <D> <mode>  run=<NOW KST>  rev=<short>  kind=<BUSINESS|HOLIDAY|WEEKEND|SHIFTED>
VERDICT: GREEN | AMBER (SEV3 only) | RED (SEV2) | CRITICAL (SEV1)
SEV1/SEV2: <ID> <한 줄 요약> | evidence: <command/value> | impact | proposed action (미실행)
CHANGED since <prev run>: <new / resolved / worsened>
CHECKED: H n/n · S n/n · C n/n · Q n/n · X n/n · A n/n   (PASS / WARN / FAIL / UNKNOWN)
UNKNOWN: <ID> <사유> (what resolves it)
DATA CONTENT: <key numbers, e.g. L0 dup_seq=0 crossed=0; snapshots 9/9 exact; bars 2762 ratio 1.00; ticks==bars 21/21>
NEXT: <next mode/time; pending events, e.g. 22:00 recreate>
```

Every ID of the run's modes needs a status; list skipped IDs with the reason.

## 11. Failure triage (report first; no deploy 08:10–22:00)

| Symptom | Look at |
|---|---|
| `candidates_not_ready` / `orchestration_error` | events message (`selected N exceeds slot_budget M`, KRX bars timeout), `candidates.json` rev; KRX timeouts retry every 5 min |
| `streamer RESTARTED` x5 then `circuit_open` after 08:20; `ClientConnectionResetError` in LS subscribe | stale LS token (H10): rename `~/.cache/kis/token_ls_*.json` aside, restart, confirm `shared_token status=PUBLISHED` and `stream_connect` |
| `NO_FIRST_FRAME` 08:36–09:05 | all subscriptions rejected / wrong token / silent socket; same chain as the row above; 08:30–08:40 prints are lost |
| Daemon restarted in session | `shutdown` event (`signal`, `graceful`), `daemon_lifecycle.json`, CI run times vs freeze |
| Manifest `degraded_reason`, unacked pairs | `subscription_acks[].rt_cd`; KIS 41-pair/connection cap; `work/kis_ws_leases/<key_id>.lock` (`key_lease_busy`) |
| L0 stalled / zero size | `docker logs --since 30m`, `docker exec krx-collector pgrep -af collect-`, H3, H4 |
| All streams silent at once in session | KRX circuit breaker / halt first (2026-10-15 CB: ~900 s regular gap, streams connected); watchdog is not halt-aware |
| Q9/Q10 violation | which stream/hour; `dup_seq`/`seq_regress` → writer replay after restart (S4); `hour_mismatch` → clock (H4); `crossed`/`px_le0` → decoder field drift (vendor format change) |
| `StorageExhaustedError` | disk floor; free space only after 22:00 |
| `ClockUnsyncedError` | `chronyc tracking`, NTP reachability |
| DQ FAIL | X2; new listings, vendor format change, session clock |
| EOD not done by 21:00 | `stage=eod_*` CRITICAL, `rclone` `auth_expired`, disk, `L1WorkerCrashError` (memory) |
| Offload / backup mismatch (H7/H8) | `rclone lsjson` on the path, partition re-normalized after verification, remote auth, Drive lock holders in `host_backup_status.json` |
| Alert silence | A1/A2, `alert_ledger.json`, SMTP `reason=` |
| Dashboard disagrees | P-DASH vs evidence; the adapter's `_VERIFIED_STAGES` allow-list hides unlisted stages |

## 12. Maintaining this runbook

- Every new automated routine (timer, session, stream, snapshot kind, store) gets a timeline row (section 6), a presence check (C/H), a content check on its actual output (Q), and a section 9 baseline. A routine without a content check is incomplete.
- Every incident adds a check, a benign pattern, an open item, or a triage row. Date baselines; drop history that no longer drives a decision.
- Re-run probes after any schema/layout change and update the "Probes last run" line.
- Feature modules are appended as `## NP`-style sections with a lifecycle line and merge into the main tables once stable.

## NP. Module: NXT premarket (lifecycle: trial since 2026-10-06; clean 2026-10-12..16)

Enabled by `KRX_ALPHA_PREMARKET_ENABLED=true`, `KRX_ALPHA_PREMARKET_CREDENTIAL_SLOT=5` (`docker-compose.yml`). Window 08:00–08:50, pool `universe/premarket/D.json` (capacity 20, NX-only rankings), manifest `manifest/premarket/D.nxt.shard-00.json`, L0 `l0/kis/nxt/nxt_pre/{H0NXCNT0,H0NXASP0}/dt=D/08.s0.jsonl.zst`.

| ID | Modes | Check | How | PASS |
|---|---|---|---|---|
| NP1 | M0 | Pool armed | P-UNIV premarket D | exists, `selected == capacity` (or eligible shortfall), `effective_from` 07:58 |
| NP2 | M3 | Manifest | P-MANIFEST `nxt_pre` | `closed` ≤08:50:10, `acked_pairs == planned == 40`, `unacked=0`, no gap span >30 s, `degraded None` |
| NP4 | M3 | L0 present | P-L0 | both streams, hour `08` (a `07` file only if a frame lands before 08:00), sizes >0 |
| NP5 | M3 | Content | P-L0C `nxt_pre` | `no_data=0`, `thin_lt100=0`, `evt` 080000–0850xx, all Q9 invariants 0 |
| NP7 | M3 | EOD result | `docker logs --since 14h krx-collector 2>&1 \| grep premarket_eod` | `status=OK accepted=40 planned=40`; premarket never degrades EOD |
| NP8 | W | Normalization | P-DQ / P-L1C `kis/nxt/nxt_pre/*` | PASS or benign WARN; `premarket` ≥99.5% of rows |
| NP10 | M3 | Next pool | P-UNIV with N; `docker logs --since 14h krx-collector 2>&1 \| grep premarket_pool` | `status=OK target=N symbols=20` |
| NP11 | M1 08:55 | Quick post-run | `docker logs --since 2h krx-collector 2>&1 \| grep -E "premarket_(stream\|plan\|supervise)"` | START then STOP; no `SKIP`, `RESTARTED`, `circuit_open`, `FAIL` |
| NP12 | W | Calibration | max in-window gap of the week; `grep key_lease_busy` | `KRX_ALPHA_PREMARKET_SILENCE_LIMIT_S` ≈2.6x largest normal gap; no `key_lease_busy` |

Disable switch: `KRX_ALPHA_PREMARKET_ENABLED` in `docker-compose.yml`, changed only outside the freeze; then H1 and the CI deploy job.

## DB. Module: DB금융투자 crosscheck stream (lifecycle: dormant since 2026-10-17)

One stream-child spec row (`role=crosscheck`) gated by `KRX_ALPHA_DB_CROSSCHECK_ENABLED`, absent from `docker-compose.yml`. DB S00 delivers ~70–80% of LS trades, so it is a quote crosscheck, never a tick backup. Re-enabling is a user decision.

| ID | Modes | Check | How | PASS |
|---|---|---|---|---|
| DB1 | M0 | Stays dormant | P-BROKER `db_crosscheck_env`; `docker logs --since 14h krx-collector 2>&1 \| grep -c db_crosscheck`; `ls -d l0/db manifest/*.db.json 2>/dev/null` | `0`, `0`, nothing listed; any DB activity = SEV2 (config drift) |
