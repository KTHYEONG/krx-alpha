# VPS Checklist (krx-collector, NXT premarket)

Host `or-vps`, container `krx-collector`, data root `/home/ubuntu/krx-alpha/data` (container `/app/data`). Read-only checks only.
Set `D` = KST session date (YYYY-MM-DD), `N` = next business day. Report each item as PASS / WARN / FAIL with the value found.

## A. Daily, after 20:30 KST (business days; first 2 weeks, then weekly)

| # | Check | Command | PASS |
|---|---|---|---|
| A1 | Manifest closed, ACKs complete | `python3 -c "import json;m=json.load(open('manifest/premarket/D.nxt.shard-00.json'));print(m['venue'],m['session'],m['writer_closed_at_ns'] is not None,len([a for a in m['subscription_acks'] if a['accepted']]),len(m['planned_pairs']),len(m['gaps']),m['degraded_reason'])"` | `nxt nxt_pre True 40 40 <gaps> None` |
| A2 | Gaps | list `m['gaps']` entries inside 08:00–08:50: `reason`, duration `(gap_end_ns-gap_start_ns)/1e9` | max gap <= 30 s; `restart` gaps = 0. Baseline: aftermarket also logs `watchdog` gaps, compare with `manifest/aftermarket/D.nxt.shard-00.json` |
| A3 | L0 present, both streams | `ls -l l0/kis/nxt/nxt_pre/H0NXCNT0/dt=D l0/kis/nxt/nxt_pre/H0NXASP0/dt=D` | files `08.s0.jsonl.zst` (and `07.s0` if frames before 08:00) in both; sizes > 0 |
| A4 | Symbols with data | `zstdcat l0/kis/nxt/nxt_pre/H0NXCNT0/dt=D/*.zst \| python3 -c "import sys,json,collections;c=collections.Counter(json.loads(l)['raw'].split('^')[0] for l in sys.stdin);print(len(c),c.most_common(3),c.most_common()[-3:])"` (L0 `symbol` field is empty for KIS; use first `raw` field) | 15–20 symbols with trades; record counts as baseline |
| A5 | Event time sane | first/last `exchange_event_time` of the same stream | HHMMSS, first >= 075800, last <= 085500 (WARN otherwise: V2 time-field assumption) |
| A6 | EOD result | `grep eod_maintenance data/logs/events-daemon.jsonl \| tail -1` | `status=OK` (premarket never degrades it) |
| A7 | Premarket EOD line | `docker logs --since 14h krx-collector 2>&1 \| grep premarket_eod` | `status=OK accepted=40 planned=40` |
| A8 | Normalization quality (L1 is produced when a day leaves the 3-day L0 window: check day D-3, first valid on the 4th day) | `ls l1/kis/nxt/nxt_pre/*/` and `grep "stage=quality" data/logs/events-normalize-worker.jsonl \| grep "tr_id=H0NX" \| tail -6` | L1 parquet exists for both streams for D-3; `status=OK` or WARN with `tick_loss` < 0.1% of rows; `ladder_disorder`/`crossed_book` small (premarket quotes are continuous, not auction); no file under `quarantine/` dated D |
| A9 | Phase labels (D-3; polars exists only in the container) | `docker exec krx-collector /app/.venv/bin/python -c "import polars as pl;print(pl.read_parquet('data/l1/kis/nxt/nxt_pre/H0NXCNT0/dt=<D-3>.parquet',columns=['market_phase']).to_series().value_counts())"` | `premarket` dominates; `unclassified` < 1% |
| A10 | Offload | EOD `uploaded=` > 0; remote has `l1/kis/nxt/nxt_pre/<stream>/dt=<D-3>.parquet` (`rclone lsjson` on the configured remote) | present remotely |
| A11 | Next pool (also on dashboard) | `ls -l universe/premarket/N.json` and `docker logs --since 14h krx-collector 2>&1 \| grep premarket_pool` | file exists; `status=OK target=N symbols=20`; otherwise tomorrow's premarket is skipped (fix after 22:00 only) |

## B. Daily, after 08:55 KST (quick)

| # | Check | PASS |
|---|---|---|
| B1 | Dashboard `프리마켓 종목`=OK, `프리마켓 수집`=DONE with `구독 40/40` | as stated |
| B2 | `docker logs --since 2h krx-collector 2>&1 \| grep -E "premarket_(stream\|plan\|supervise)"` | `status=START` then `STOP`; no `SKIP`, `RESTARTED`, `circuit_open`, `FAIL` |
| B3 | `tail -3 data/logs/events-cli-collect-premarket.jsonl` | `stream_connect ... rejected=0`; no `stream_gap`/`stream_disconnect` bursts |

## C. Weekly

| # | Check | PASS |
|---|---|---|
| C1 | Disk | `df -h /home/ubuntu` < 70%; `du -sh l0/kis/nxt/nxt_pre l1/kis/nxt/nxt_pre`: record bytes/day; projected 30-day growth acceptable |
| C2 | Retention | `ls l0/kis/nxt/nxt_pre/H0NXCNT0` holds <= `journal_retain_days` (3) newest days after offload |
| C3 | Pool quality | for each D: symbols with < 100 trades in A4 (dead symbols). If > 5 per day, tune `max_symbols` or selection; do not change during 08:10–22:00 |
| C4 | Silence limit calibration (V3) | max in-window gap across the week (A2); set `KRX_ALPHA_PREMARKET_SILENCE_LIMIT_S` ~2.6x the largest normal gap |
| C5 | Slot isolation (V5) | no `key_lease_busy` in premarket events; `KIS_DECISION_SHARD_SLOTS` and `KIS_HOST_DATA_SLOTS` unchanged vs `quant-secrets/*.env` (names only, never print keys) |
| C6 | Container | `docker ps` Up; `docker stats --no-stream krx-collector` memory < 400 MiB at 08:30 and EOD; restarts = 0 |

## D. Failure triage (stop after reporting; do not deploy 08:10–22:00)

| Symptom | Look at |
|---|---|
| `premarket_plan status=SKIP` | `universe/premarket/D.json` missing/invalid → A11 of previous evening, `premarket_pool` WARN lines |
| ACK accepted < planned | `subscription_acks[].code` in manifest (NX subscription rejected → V2) |
| `key_lease_busy` | another process holds slot lease `work/kis_ws_leases/<key_id>.lock` |
| `StorageExhaustedError` | disk below premarket floor (collector floor + `extra_free_disk_gb`) |
| `ClockUnsyncedError` | NTP offset > 2.0 s; chrony status on host |
| manifest unclosed after 08:55 | child alive? `docker exec krx-collector pgrep -af collect-premarket`; daemon log `premarket_stream` |

## E. Enable / disable switch

`docker-compose.yml` `KRX_ALPHA_PREMARKET_ENABLED` (true/false). Change only outside 08:10–22:00, then confirm CI deploy green and container Up.
