# Feed resilience architecture

Stable boundaries of the live feed failover: data plane, feed groups, controller state machine, file contracts, capacity, and alert policy. Parameters are named `FeedControlSettings` fields (`src/core/config.py`); this document does not restate their values.

## 1. Data plane

- Every source writes raw L0 per vendor, append-only, before any parsing. Secondary feeds never mutate primary vendor partitions.
- Canonical selection per symbol-window is derived after the fact (`l1/derived/feed_selection`, nightly `build-feed-selection`). Collection-time switching only decides which symbols a secondary additionally subscribes.

## 2. Feed groups and ids

- A watched group is one primary subscription unit: `ls:<n>` (LS regular shard), `kis-krx:<n>` (KIS KRX slot), `kis-nxt:<n>` (KIS NXT slot). The group id is also the primary feed id.
- Secondary feed ids are the allow-listed set in `FeedControlSettings` (`db:*`, `toss:*`, `kis:5`). Secondaries are admitted only to groups of the same venue and never to a feed locked to another venue for the day.
- Health files are keyed by feed id with `:` replaced by `__` (`work/feed_status/<feed>.json`).

## 3. Controller state machine

Per group: `PRIMARY_OK -> DEGRADED -> SECONDARY_ACTIVE -> FAILBACK_HOLD -> PRIMARY_OK`, with `DAY_DEMOTED` as a terminal state for the session day.

- Source classification is from the health file: stale, circuit-open, auth-rejected, disconnected, silent, lagging, or healthy (`classify_source`).
- Hysteresis: degradation requires the abnormal status to persist for `degrade_after_s`; failover after `failover_after_s` or immediately on `DEAD`; recovery to `PRIMARY_OK` requires `heal_after_s` of health.
- Failback is held for `failback_hold_s` and never occurs during high-risk windows (session standby and closing auction). A relapse during `FAILBACK_HOLD` returns to `SECONDARY_ACTIVE`.
- A group is demoted for the day after `demote_relapses` relapses or `demote_degraded_s` of cumulative degradation. A demoted group does not fail back until the next session day.
- Each controller step is pure (`src/orchestration/feed_control/controller.py`); the supervisor (`src/orchestration/feed_failover.py`) applies its actions and persists state.

## 4. Subscription constraint and capacity recycle

- Running child processes cannot change subscriptions. The only live path is add-only: admitted symbols are appended to the day's dynamic admission plan and picked up by the secondary child.
- Because subscriptions are never removed during the session, a failback does not unsubscribe. Secondary capacity is recycled (secondary child restarted) only after the regular close, once the group is no longer admitting.
- Venue lock: a secondary feed is bound to the venue of its first admission for the day; admissions from the other venue in the same step are refused and logged.

## 5. File contracts

| Path (under the krx data root) | Writer | Schema or shape | Reader |
|---|---|---|---|
| `work/feed_status/<feed>.json` | each child | `feed-health.v1`, same-session only | supervisor, EOD archive |
| `work/feed_controller.json` | supervisor | controller state, `schema_version` checked on load; corrupt files are renamed aside, never deleted | supervisor on start |
| `manifest/feed_control/<day>.jsonl` | supervisor | append-only journal: `TRANSITION`, action, and `group_plan` rows | EOD digest, quality report |
| `work/status/feed_control.json` | supervisor | `status-checks.v1` (producer `krx-feed-controller`) | dashboard status source |
| `l1/derived/feed_selection/dt=<day>.parquet`, `manifest/feed_quality/<day>.selection.json` | nightly `build-feed-selection` | derived selection and summary | quality report |
| `manifest/feed_quality/<day>.json`, `rolling.json`, `<day>.health/` | nightly `build-feed-selection`, EOD archive | quality report and rolling window | alert floor, EOD digest |

The status file is written at most once per `_STATUS_EVERY_S` and on every controller transition. Its checks carry `stale_after_s` 120, so a stopped controller reads as FAIL at the dashboard without any push from the controller.

## 6. Capacity (ceilings, existing accounts only)

| Session | Primary | Secondaries (priority order) |
|---|---|---|
| KRX regular | LS shards (two shards) | KIS slots 3 and 4 (static duplicates), DB, Toss, KIS slot 5 |
| NXT regular | KIS slots 1 and 2 | KIS slot 5 (same broker, protects slot death only), Toss when probed |
| NXT pre/after, KRX after | existing shards | none |

Capacity is a configured ceiling per secondary (pair capacity per connection), not a boolean switch. An empty `secondary_feeds` keeps the controller in observe-only mode.

## 7. Alert matrix

| Event | Channel |
|---|---|
| Failover, failback, day demotion, secondary lost (`protection_lost`) | digest counters and status WARN; never a page |
| Group without any healthy cover (`FEED_GROUP_DOWN`) | page |
| Controller tick failing for `STUCK_AFTER_TICKS` consecutive ticks (`FEED_CONTROLLER_STUCK`) | page once per day |
| Availability below `quality_floor_availability` for `quality_floor_days` consecutive days (`FEED_QUALITY_FLOOR`) | page once per vendor per streak |
| `REGULAR_FEED_DOWN`, `STREAM_OUTAGE`, `REGULAR_SHARD_STALE` | unchanged |

## 8. Failure behavior

- The supervisor never raises into the daemon. Tick failures are logged and counted; the primary watchdogs keep full authority.
- A failed status write is isolated (`OSError` only) and logged at most once per minute.
- No secondary is admitted unless its health classifies as `HEALTHY`; unknown or missing health admits nothing (fail-closed).
