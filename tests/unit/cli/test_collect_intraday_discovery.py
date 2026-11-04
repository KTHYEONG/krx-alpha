from __future__ import annotations

import argparse
import datetime as dt
import logging
from collections.abc import Mapping
from typing import ClassVar
from zoneinfo import ZoneInfo

import pytest

from src.cli import collect_intraday_discovery as discovery_cli
from src.core.config import IntradayAdmissionSettings
from src.core.errors import KrxAlphaError
from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.marketdata.kiwoom_ticks import KiwoomTickTapeError, KiwoomTransientError
from src.universe.intraday import (
    AdmissionState,
    DynamicEntry,
    DynamicPlan,
    read_dynamic_admissions,
    state_from_ledger,
    write_dynamic_admissions,
)
from src.universe.membership import MembershipRow, read_membership

_KST = ZoneInfo("Asia/Seoul")
_DAY = dt.date(2026, 11, 3)
_TV = 20_000_000_000


class _Clock:
    def __init__(self, start: dt.datetime) -> None:
        self.current = start

    def now(self) -> dt.datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += dt.timedelta(seconds=seconds)


def _at(hour: int, minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime.combine(_DAY, dt.time(hour, minute, second), tzinfo=_KST)


def _row(rank: int, symbol: str, change_pct: float = 6.0, trade_value: int = _TV) -> KiwoomRankingRow:
    return KiwoomRankingRow(rank=rank, symbol=symbol, last_price=10_000, change_pct=change_pct, trade_value_krw=trade_value)


def _cfg(**overrides: object) -> IntradayAdmissionSettings:
    base: dict[str, object] = {
        "enabled": True,
        "start_after_open_s": 0.0,
        "poll_interval_s": 60.0,
        "admission_cutoff": dt.time(9, 1),
        "post_close_screen_at": dt.time(9, 2),
        "min_abs_change_pct": 5.0,
        "min_trade_value_krw": 10_000_000_000,
        "status_check": False,
    }
    base.update(overrides)
    return IntradayAdmissionSettings(**base)  # type: ignore[arg-type]


def _build(
    tmp_path,
    clock: _Clock,
    rounds: list[object],
    *,
    cfg: IntradayAdmissionSettings | None = None,
    state: AdmissionState | None = None,
    status_of=None,
    persisted: list[tuple[int, tuple[KiwoomRankingRow, ...]]] | None = None,
) -> tuple[discovery_cli.IntradayDiscovery, object]:
    script = list(rounds)
    counter = {"wall": 1_000}

    def fetch() -> tuple[KiwoomRankingRow, ...]:
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item  # type: ignore[return-value]

    def persist_ranking(observed: int, rows: tuple[KiwoomRankingRow, ...]) -> None:
        if persisted is not None:
            persisted.append((observed, rows))

    def wall_ns() -> int:
        counter["wall"] += 1
        return counter["wall"]

    instance = discovery_cli.IntradayDiscovery(
        cfg=cfg or _cfg(),
        session_date=_DAY,
        open_time=dt.time(9, 0),
        membership_path=tmp_path / "membership.jsonl",
        ipc_path=tmp_path / "dynamic.json",
        fetch=fetch,
        persist_ranking=persist_ranking,
        status_of=status_of,
        persist_status=lambda status, observed: None,
        initial_state=state or AdmissionState(collected=frozenset(), ineligible=frozenset(), capacity_left={"ls:1": 2}),
        now=clock.now,
        wall_ns=wall_ns,
        sleep=clock.sleep,
    )
    return instance, script


def test_ledger_row_is_written_before_ipc_entry(tmp_path, monkeypatch) -> None:
    clock = _Clock(_at(8, 59))
    order: list[str] = []
    real_append = discovery_cli.append_membership

    def recording_append(path, rows):
        order.append("ledger")
        real_append(path, rows)

    def failing_write(path, plan):
        order.append("ipc")
        raise OSError("injected ipc failure")

    monkeypatch.setattr(discovery_cli, "append_membership", recording_append)
    monkeypatch.setattr(discovery_cli, "write_dynamic_admissions", failing_write)
    run, _ = _build(tmp_path, clock, [[_row(1, "005930")], [_row(1, "005930")], []])

    assert run.run() == 0

    assert order[:2] == ["ledger", "ipc"]
    rows = read_membership(tmp_path / "membership.jsonl")
    assert [(r.symbol, r.tier, r.source) for r in rows if r.source == "dynamic"] == [("005930", "B", "dynamic")]
    assert not (tmp_path / "dynamic.json").exists()


def test_restart_rebuilds_capacity_from_ledger_without_duplicates(tmp_path) -> None:
    ledger = tmp_path / "membership.jsonl"
    for index in range(5):
        symbol = f"00020{index}"
        append_row = MembershipRow(
            symbol=symbol, tier="B", source="dynamic", decided_at_ns=1, direction="up", warning_level=None,
            ls_shard=1, kis_slot=None, trade_value_krw=_TV, reason=None,
        )
        discovery_cli.append_membership(ledger, [append_row])
    write_dynamic_admissions(tmp_path / "dynamic.json", DynamicPlan(rev=1, entries=tuple(
        DynamicEntry(f"00020{i}", "ls:1", 1) for i in range(5)
    )))

    state = state_from_ledger(read_membership(ledger), ineligible=frozenset(), capacity={"ls:1": 40})
    clock = _Clock(_at(8, 59))
    run, _ = _build(
        tmp_path,
        clock,
        [[_row(1, "000200"), _row(2, "000300")], [_row(1, "000200")], [_row(1, "000200")]],
        state=state,
    )

    assert run.run() == 0

    dynamic = [r.symbol for r in read_membership(ledger) if r.source == "dynamic"]
    assert dynamic.count("000200") == 1
    assert dynamic.count("000300") == 1
    assert read_dynamic_admissions(tmp_path / "dynamic.json").rev == 2


def test_cutoff_respected_and_post_close_screen_writes_tier_c_only(tmp_path) -> None:
    clock = _Clock(_at(15, 16))
    cfg = _cfg(admission_cutoff=dt.time(15, 15), post_close_screen_at=dt.time(15, 31), start_after_open_s=0.0)
    run, script = _build(tmp_path, clock, [[_row(1, "000400"), _row(2, "000500")]], cfg=cfg)
    fetches: list[int] = []
    original = run._fetch

    def counting_fetch():
        fetches.append(1)
        return original()

    run._fetch = counting_fetch  # type: ignore[attr-defined]

    assert run.run() == 0

    assert len(fetches) == 1
    assert script == []
    rows = read_membership(tmp_path / "membership.jsonl")
    assert {(r.symbol, r.tier, r.source) for r in rows} == {
        ("000400", "C", "post_close_screen"),
        ("000500", "C", "post_close_screen"),
    }
    assert not (tmp_path / "dynamic.json").exists()


def test_poll_failure_is_isolated_and_logged(tmp_path, caplog) -> None:
    caplog.set_level(logging.WARNING)
    clock = _Clock(_at(8, 59))
    run, _ = _build(
        tmp_path,
        clock,
        [KiwoomTransientError("vendor blip"), [_row(1, "600001")], [_row(1, "600001")]],
    )

    assert run.run() == 0

    assert "status=POLL_FAIL" in caplog.text
    rows = read_membership(tmp_path / "membership.jsonl")
    assert ("600001", "B", "dynamic") in {(r.symbol, r.tier, r.source) for r in rows}


def test_every_poll_is_persisted_with_distinct_observation_time(tmp_path) -> None:
    clock = _Clock(_at(8, 59))
    persisted: list[tuple[int, tuple[KiwoomRankingRow, ...]]] = []
    run, _ = _build(
        tmp_path,
        clock,
        [[_row(1, "000600")], [_row(1, "000700")], [_row(1, "000800")]],
        persisted=persisted,
    )

    assert run.run() == 0

    assert len(persisted) == 3
    assert len({observed for observed, _ in persisted}) == 3


def test_status_excluded_rows_are_rejected_and_warning_level_recorded(tmp_path) -> None:
    clock = _Clock(_at(8, 59))
    statuses: Mapping[str, Mapping[str, object]] = {
        "000900": {"managed": True, "market_warning_code": "00"},
        "001000": {"market_warning_code": "00"},
    }
    run, _ = _build(
        tmp_path,
        clock,
        [[_row(1, "000900"), _row(2, "001000")], [_row(1, "000900")], [_row(1, "000900")]],
        cfg=_cfg(status_check=True),
        status_of=lambda symbol: statuses[symbol],
    )

    assert run.run() == 0

    rows = read_membership(tmp_path / "membership.jsonl")
    dynamic = [(r.symbol, r.warning_level) for r in rows if r.source == "dynamic"]
    assert dynamic == [("001000", "none")]


def test_feed_fields_map_ls_and_kis_feeds() -> None:
    assert discovery_cli._feed_fields("ls:1") == (1, None)
    assert discovery_cli._feed_fields("kis:5") == (None, "5")


@pytest.mark.parametrize(("change", "direction"), [(6.0, "up"), (-6.0, "down")])
def test_tier_c_direction_follows_sign(change: float, direction: str) -> None:
    row = _row(1, "002000", change_pct=change)

    assert discovery_cli._tier_c(row, source="intraday_capacity", decided=1).direction == direction


def test_five_consecutive_poll_failures_raise_critical_once(tmp_path, caplog) -> None:
    caplog.set_level(logging.WARNING)
    clock = _Clock(_at(8, 59))
    failures = [KiwoomTransientError("down") for _ in range(6)]
    cfg = _cfg(admission_cutoff=dt.time(9, 5), post_close_screen_at=dt.time(9, 6))
    run, _ = _build(tmp_path, clock, [*failures, []], cfg=cfg)

    assert run.run() == 0

    assert caplog.text.count("reason=consecutive_poll_failures") == 1
    assert caplog.text.count("status=POLL_FAIL") == 6


def test_bar_ineligible_symbol_is_rejected_once_per_day(tmp_path, caplog) -> None:
    caplog.set_level(logging.INFO)
    clock = _Clock(_at(8, 59))
    state = AdmissionState(collected=frozenset(), ineligible=frozenset({"000800"}), capacity_left={"ls:1": 2})
    run, _ = _build(tmp_path, clock, [[_row(1, "000800")], [_row(1, "000800")], []], state=state)

    assert run.run() == 0

    rows = read_membership(tmp_path / "membership.jsonl")
    assert not [r for r in rows if r.source == "dynamic"]


def test_status_lookup_failure_admits_with_unknown_warning_level(tmp_path) -> None:
    from src.execution.contracts import KisApiError

    clock = _Clock(_at(8, 59))

    def status_of(symbol: str) -> dict:
        raise KisApiError("EGW00201", "rate limited")

    run, _ = _build(
        tmp_path,
        clock,
        [[_row(1, "000950")], [], []],
        cfg=_cfg(status_check=True),
        status_of=status_of,
    )

    assert run.run() == 0

    dynamic = [(r.symbol, r.warning_level) for r in read_membership(tmp_path / "membership.jsonl") if r.source == "dynamic"]
    assert dynamic == [("000950", "unknown")]


def test_membership_write_failure_admits_nothing_and_writes_no_ipc(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.ERROR)
    clock = _Clock(_at(8, 59))

    def failing_append(path, rows):
        raise OSError("disk full")

    monkeypatch.setattr(discovery_cli, "append_membership", failing_append)
    run, _ = _build(tmp_path, clock, [[_row(1, "000960")], [], []])

    assert run.run() == 0

    assert "reason=membership_write" in caplog.text
    assert not (tmp_path / "dynamic.json").exists()


def test_post_close_membership_failure_is_logged_not_raised(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.ERROR)
    clock = _Clock(_at(15, 31))
    cfg = _cfg(admission_cutoff=dt.time(15, 15), post_close_screen_at=dt.time(15, 31))

    def failing_append(path, rows):
        raise OSError("disk full")

    monkeypatch.setattr(discovery_cli, "append_membership", failing_append)
    run, _ = _build(tmp_path, clock, [[_row(1, "000970")]], cfg=cfg)

    assert run.run() == 0
    assert "stage=intraday_post_close status=FAIL" in caplog.text


def test_ipc_write_failure_keeps_session_state_and_logs(tmp_path, monkeypatch, caplog) -> None:
    caplog.set_level(logging.ERROR)
    clock = _Clock(_at(8, 59))

    def failing_write(path, plan):
        raise OSError("ipc unavailable")

    monkeypatch.setattr(discovery_cli, "write_dynamic_admissions", failing_write)
    run, _ = _build(tmp_path, clock, [[_row(1, "000980")], [_row(1, "000980")], []])

    assert run.run() == 0

    assert "reason=ipc_write" in caplog.text
    dynamic = [r.symbol for r in read_membership(tmp_path / "membership.jsonl") if r.source == "dynamic"]
    assert dynamic == ["000980"]


def test_ranking_rows_carry_point_in_time_metadata() -> None:
    rows = discovery_cli._ranking_rows(
        observed=77, session_date=_DAY, rows=(_row(3, "000990", change_pct=-5.5, trade_value=123),)
    )

    assert rows == [
        {
            "session_date": _DAY,
            "observed_at_ns": 77,
            "source_tr": "ka10032",
            "market_div_code": "J",
            "list_kind": "kiwoom_trade_amount",
            "rank": 3,
            "symbol": "000990",
            "change_pct": -5.5,
            "trade_value_krw": 123,
        }
    ]


def test_bars_ineligible_degrades_to_empty_on_unreadable_store(tmp_path, monkeypatch) -> None:
    from src.core.paths import DataPaths
    from src.marketdata.partitioned_store import PartitionedStoreError

    def unreadable(root, **kwargs):
        raise PartitionedStoreError("no partitions")

    monkeypatch.setattr(discovery_cli, "scan_month_partitions", unreadable)

    assert discovery_cli._bars_ineligible(DataPaths(tmp_path), _DAY) == frozenset()


def test_bars_ineligible_reads_eligibility_columns(tmp_path, monkeypatch) -> None:
    import polars as pl

    from src.core.paths import DataPaths

    empty = pl.LazyFrame(
        schema={"date": pl.Date, "symbol": pl.String, "stock_cert_kind": pl.String, "section": pl.String}
    )
    monkeypatch.setattr(discovery_cli, "scan_month_partitions", lambda root, **kwargs: empty)

    assert discovery_cli._bars_ineligible(DataPaths(tmp_path), _DAY) == frozenset()


def test_today_status_excluded_uses_classification() -> None:
    import polars as pl

    class _Store:
        def __init__(self, frame: pl.DataFrame) -> None:
            self._frame = frame

        def frame(self, dataset):
            return self._frame

    frame = pl.DataFrame(
        {"symbol": ["001000", "001100"], "managed": [True, None], "market_warning_code": ["00", "00"]}
    )

    assert discovery_cli._today_status_excluded(_Store(frame)) == frozenset({"001000"})  # type: ignore[arg-type]
    assert discovery_cli._today_status_excluded(_Store(pl.DataFrame())) == frozenset()  # type: ignore[arg-type]


def test_slot_credential_requires_configured_slot() -> None:
    from src.core.errors import MissingCredentialsError
    from src.realtime.kis_sharding import KisDataCredential

    cred = KisDataCredential("5", "app", "secret", "hts", "kid")

    assert discovery_cli._slot_credential("5", (cred,)) is cred
    with pytest.raises(MissingCredentialsError):
        discovery_cli._slot_credential("4", (cred,))


class _FakeClock:
    def __init__(self, start: dt.datetime) -> None:
        self.current = start

    def now(self) -> dt.datetime:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += dt.timedelta(seconds=seconds)


def _wire_env(monkeypatch, tmp_path, **overrides: str) -> None:
    env = {
        "KRX_ALPHA_INTRADAY_ENABLED": "true",
        "KRX_ALPHA_INTRADAY_START_AFTER_OPEN_S": "0",
        "KRX_ALPHA_INTRADAY_POLL_INTERVAL_S": "60",
        "KRX_ALPHA_INTRADAY_ADMISSION_CUTOFF": "09:01:00",
        "KRX_ALPHA_INTRADAY_POST_CLOSE_SCREEN_AT": "09:02:00",
        "KRX_ALPHA_INTRADAY_MIN_TRADE_VALUE_KRW": "10000000000",
        "KRX_ALPHA_INTRADAY_STATUS_CHECK": "false",
        "KRX_ALPHA_DATA_ROOT": str(tmp_path / "data"),
        "KRX_ALPHA_REGULAR_KIS_ENABLED": "true",
        "KRX_ALPHA_REGULAR_KIS_CREDENTIAL_SLOTS": '["1","2","3","4"]',
        "KRX_ALPHA_REGULAR_KIS_PAIR_CAPACITY_PER_CONNECTION": "41",
    }
    env.update(overrides)
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def _wire_boundaries(monkeypatch, tmp_path, rounds: list[object]) -> list[int]:
    from types import SimpleNamespace

    script = list(rounds)
    fetches: list[int] = []

    def fetch(**kwargs):
        fetches.append(1)
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(discovery_cli, "load_credentials", lambda model: SimpleNamespace(kiwoom_app_key="k", kiwoom_secret_key="s"))
    monkeypatch.setattr(discovery_cli, "issue_kiwoom_token", lambda **kwargs: "TOKEN")
    monkeypatch.setattr(discovery_cli, "fetch_trade_amount_ranking", fetch)
    monkeypatch.setattr(discovery_cli, "KisTokenSettings", lambda: SimpleNamespace(token_cache_dir=tmp_path / "tokens", allow_issue=False))
    monkeypatch.setattr(discovery_cli, "SnapshotStore", _StubStore)
    return fetches


class _StubStore:
    appended: ClassVar[list[tuple[object, int]]] = []

    def __init__(self, *, paths, session_date) -> None:
        self._session_date = session_date

    def append(self, dataset, rows) -> int:
        type(self).appended.append((dataset, len(rows)))
        return len(rows)

    def frame(self, dataset):
        import polars as pl

        return pl.DataFrame()


def test_build_discovery_wires_real_boundaries_end_to_end(tmp_path, monkeypatch) -> None:
    _wire_env(monkeypatch, tmp_path)
    _StubStore.appended = []
    fetches = _wire_boundaries(monkeypatch, tmp_path, [[_row(1, "001200")], [], []])
    clock = _FakeClock(_at(8, 59))
    args = argparse.Namespace(session_date=_DAY)

    discovery = discovery_cli.build_discovery(args, now=clock.now, sleep=clock.sleep)

    assert discovery.run() == 0
    assert len(fetches) == 3
    assert any(dataset.value == "ranking" for dataset, _ in _StubStore.appended)
    assert (tmp_path / "data" / "manifest" / "membership" / f"{_DAY}.dynamic.json").exists()


def test_build_discovery_fails_closed_on_token_store_error(tmp_path, monkeypatch) -> None:
    from deploy.shared.host_broker_contract import TokenIssueThrottled

    _wire_env(monkeypatch, tmp_path)
    _wire_boundaries(monkeypatch, tmp_path, [])

    class _BrokenStore:
        def get_or_issue(self, issue):
            raise TokenIssueThrottled("host throttle")

    monkeypatch.setattr(discovery_cli, "make_kiwoom_store", lambda *args, **kwargs: _BrokenStore())
    with pytest.raises(KiwoomTickTapeError, match="token store failed"):
        discovery_cli.build_discovery(
            argparse.Namespace(session_date=_DAY), now=_FakeClock(_at(8, 59)).now, sleep=lambda _: None
        )


def test_kiwoom_callers_reuse_one_token_record(tmp_path, monkeypatch) -> None:
    import asyncio
    from types import SimpleNamespace

    from src.core.config import CollectorSettings
    from src.core.paths import DataPaths
    from src.marketdata import kiwoom_ticks
    from src.realtime.adapters.registry import AppCredentials, RegularAdapterContext, _build_kiwoom
    from tests.unit.marketdata.test_tick_tape_backfill_regressions import _Session, _run

    cache_dir = tmp_path / "tokens"
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(cache_dir))
    session = _Session()
    backfill = _run(DataPaths(tmp_path / "tape"), session, run_id="shared-cache")
    assert backfill.status == "OK"
    assert session.token_calls == 1

    def unexpected_issue(**kwargs):
        pytest.fail("shared Kiwoom token cache unexpectedly missed")

    monkeypatch.setattr(kiwoom_ticks, "issue_kiwoom_token", unexpected_issue)
    registry_adapter = _build_kiwoom(
        AppCredentials("k", "s"),
        RegularAdapterContext(
            http=object(),
            settings=CollectorSettings(),
            token_cache_dir=cache_dir,
            market_of=None,
        ),
    )
    token_from_realtime = asyncio.run(registry_adapter._issue_token())
    assert token_from_realtime == "tok-1"

    _wire_env(monkeypatch, tmp_path)
    _wire_boundaries(monkeypatch, tmp_path, [[], [], []])
    monkeypatch.setattr(discovery_cli, "KisTokenSettings", lambda: SimpleNamespace(token_cache_dir=cache_dir))
    monkeypatch.setattr(discovery_cli, "issue_kiwoom_token", unexpected_issue)
    ranking_tokens: list[str] = []

    def capture_ranking(**kwargs):
        ranking_tokens.append(kwargs["access_token"])
        return ()

    monkeypatch.setattr(discovery_cli, "fetch_trade_amount_ranking", capture_ranking)
    clock = _FakeClock(_at(8, 59))
    discovery = discovery_cli.build_discovery(argparse.Namespace(session_date=_DAY), now=clock.now, sleep=clock.sleep)
    assert discovery.run() == 0
    assert ranking_tokens
    assert set(ranking_tokens) == {"tok-1"}


def test_run_refuses_when_disabled(tmp_path, monkeypatch) -> None:
    _wire_env(monkeypatch, tmp_path, KRX_ALPHA_INTRADAY_ENABLED="false")

    with pytest.raises(KrxAlphaError, match="disabled"):
        discovery_cli.run(argparse.Namespace(session_date=_DAY))


def test_persistence_failures_are_logged_not_raised(tmp_path, monkeypatch, caplog) -> None:
    from src.storage.snapshot_store import SnapshotStoreError

    class _FailingStore(_StubStore):
        def append(self, dataset, rows) -> int:
            raise SnapshotStoreError("snapshot disk full")

    _wire_env(monkeypatch, tmp_path)
    caplog.set_level(logging.ERROR)
    _wire_boundaries(monkeypatch, tmp_path, [[], [], []])
    monkeypatch.setattr(discovery_cli, "SnapshotStore", _FailingStore)
    clock = _FakeClock(_at(8, 59))

    assert discovery_cli.build_discovery(argparse.Namespace(session_date=_DAY), now=clock.now, sleep=clock.sleep).run() == 0
    assert "status=PERSIST_FAIL" in caplog.text


def test_non_qualifying_rows_are_ignored_silently(tmp_path) -> None:
    clock = _Clock(_at(8, 59))
    run, _ = _build(tmp_path, clock, [[_row(1, "001300", change_pct=1.0)], [], []])

    assert run.run() == 0

    assert read_membership(tmp_path / "membership.jsonl") == ()


def test_post_close_fetch_failure_writes_nothing_and_exits_clean(tmp_path, caplog) -> None:
    caplog.set_level(logging.WARNING)
    clock = _Clock(_at(15, 31))
    cfg = _cfg(admission_cutoff=dt.time(15, 15), post_close_screen_at=dt.time(15, 31))
    run, _ = _build(tmp_path, clock, [KiwoomTransientError("vendor down")], cfg=cfg)

    assert run.run() == 0

    assert "status=POLL_FAIL" in caplog.text
    assert read_membership(tmp_path / "membership.jsonl") == ()


def test_status_path_wires_slot_client_and_persists_observations(tmp_path, monkeypatch, caplog) -> None:
    from types import SimpleNamespace

    from src.realtime.kis_sharding import KisDataCredential

    _wire_env(monkeypatch, tmp_path, KRX_ALPHA_INTRADAY_STATUS_CHECK="true")
    caplog.set_level(logging.INFO)
    _StubStore.appended = []
    _wire_boundaries(monkeypatch, tmp_path, [[_row(1, "001400")], [], []])
    monkeypatch.setattr(
        discovery_cli,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", "app-5", "secret-5", "hts-5", "kid5"),),
    )
    built: list[dict] = []

    def stack(**kwargs):
        built.append(kwargs)
        return SimpleNamespace(data=SimpleNamespace(get_security_status=lambda symbol: {"market_warning_code": "00"}))

    monkeypatch.setattr(discovery_cli, "build_kis_rest_stack", stack)
    clock = _FakeClock(_at(8, 59))

    assert discovery_cli.build_discovery(argparse.Namespace(session_date=_DAY), now=clock.now, sleep=clock.sleep).run() == 0

    assert len(built) == 1
    assert built[0]["auth"].app_key == "app-5"
    assert any(dataset.value == "security_status" for dataset, _ in _StubStore.appended)
    dynamic = [(r.symbol, r.warning_level) for r in read_membership(tmp_path / "data" / "manifest" / "membership" / f"{_DAY}.jsonl") if r.source == "dynamic"]
    assert dynamic == [("001400", "none")]


def test_status_persistence_failure_is_logged_not_raised(tmp_path, monkeypatch, caplog) -> None:
    from types import SimpleNamespace

    from src.realtime.kis_sharding import KisDataCredential
    from src.storage.snapshot_store import SnapshotStoreError

    class _FailingStore(_StubStore):
        def append(self, dataset, rows) -> int:
            raise SnapshotStoreError("snapshot disk full")

    _wire_env(monkeypatch, tmp_path, KRX_ALPHA_INTRADAY_STATUS_CHECK="true")
    caplog.set_level(logging.ERROR)
    _wire_boundaries(monkeypatch, tmp_path, [[_row(1, "001500")], [], []])
    monkeypatch.setattr(discovery_cli, "SnapshotStore", _FailingStore)
    monkeypatch.setattr(
        discovery_cli,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", "app-5", "secret-5", "hts-5", "kid5"),),
    )
    monkeypatch.setattr(
        discovery_cli,
        "build_kis_rest_stack",
        lambda **kwargs: SimpleNamespace(data=SimpleNamespace(get_security_status=lambda symbol: {"market_warning_code": "00"})),
    )
    clock = _FakeClock(_at(8, 59))

    assert discovery_cli.build_discovery(argparse.Namespace(session_date=_DAY), now=clock.now, sleep=clock.sleep).run() == 0
    assert "stage=intraday_status status=PERSIST_FAIL" in caplog.text
