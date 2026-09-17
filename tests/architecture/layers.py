"""레이어 랭크 단일 계약 (tests/architecture/test_layering 의 fail-closed 기준)."""

from __future__ import annotations

LAYER_RANK: dict[str, int] = {'src/core/config.py': 0, 'src/core/calendar.py': 0, 'src/core/errors.py': 0, 'src/marketdata/schema.py': 0, 'src/marketdata/krx_bars.py': 1, 'src/universe/policy.py': 1, 'src/realtime/contracts.py': 1, 'src/realtime/subscription.py': 1, 'src/realtime/clock.py': 1, 'src/execution/contracts.py': 1, 'src/execution/ticks.py': 1, 'src/storage/journal.py': 2, 'src/storage/retention.py': 2, 'src/storage/remote.py': 2, 'src/storage/quality.py': 2, 'src/universe/ipc.py': 2, 'src/realtime/manifest.py': 2, 'src/execution/risk.py': 2, 'src/execution/ledger.py': 2, 'src/execution/journal.py': 2, 'src/realtime/adapters/ls.py': 3, 'src/realtime/adapters/kis.py': 4, 'src/execution/kis_client.py': 3, 'src/marketdata/service.py': 4, 'src/universe/service.py': 4, 'src/realtime/session.py': 4, 'src/realtime/streamer.py': 4, 'src/execution/gateways.py': 4, 'src/orchestration/supervisor.py': 5, 'src/orchestration/eod.py': 5, 'src/execution/oms.py': 5, 'src/orchestration/daemon.py': 6, 'src/execution/service.py': 6, 'src/cli/main.py': 7, 'src/cli/bars_refresh.py': 7, 'src/cli/collect_init.py': 7, 'src/cli/collect_status.py': 7, 'src/cli/collect_stream.py': 7, 'src/cli/collect_aftermarket.py': 7, 'src/cli/universe_plan.py': 7, 'src/cli/order.py': 7}
LAYER_RANK['src/marketdata/toss_calendar.py'] = 1
LAYER_RANK['src/realtime/kis_sharding.py'] = 1
LAYER_RANK['src/realtime/kis_lease.py'] = 1
LAYER_RANK['src/storage/normalize_worker.py'] = 3
LAYER_RANK['src/core/observability.py'] = 0
LAYER_RANK['src/core/kis_keypool_provisioning.py'] = 0
LAYER_RANK['src/core/runtime_env_provisioning.py'] = 0
LAYER_RANK['src/cli/provision_kis_keypool.py'] = 7
LAYER_RANK['src/universe/aftermarket.py'] = 4
