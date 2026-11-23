"""MarketSweepSettings contract and runtime coupling."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pytest
import yaml

from src.core.config import MarketSweepSettings, SnapshotSettings, resolve_collector_runtime


@pytest.fixture(autouse=True)
def _hermetic_krx_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("KRX_ALPHA_"):
            monkeypatch.delenv(key, raising=False)


def test_defaults_are_disabled_with_spec_schedule() -> None:
    settings = MarketSweepSettings()

    assert settings.enabled is False
    assert settings.kis_data_slot is None
    assert settings.rest_rate_per_s == pytest.approx(18.0)
    assert settings.markets == ("KOSPI", "KOSDAQ")
    assert settings.auction_open_times[0] == dt.time(8, 40)
    assert settings.auction_open_deadline == dt.time(9, 0)
    assert settings.auction_close_times == (
        dt.time(15, 20, 30),
        dt.time(15, 23),
        dt.time(15, 25, 30),
        dt.time(15, 28),
    )
    assert settings.investor_estimate_times == SnapshotSettings().investor_estimate_times
    assert settings.investor_estimate_close_time == SnapshotSettings().eod_collect_time
    assert settings.run_end == SnapshotSettings().run_end


def test_compose_enables_sweep_on_slot_6(monkeypatch: pytest.MonkeyPatch) -> None:
    document = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    for entry in document["services"]["krx-collector"]["environment"]:
        key, _, value = str(entry).partition("=")
        if key.startswith("KRX_ALPHA_MARKET_SWEEP_") or key == "KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT":
            monkeypatch.setenv(key, value)

    runtime = resolve_collector_runtime()

    assert runtime.market_sweep.enabled is True
    assert runtime.market_sweep.kis_data_slot == "6"
    assert runtime.snapshot.kis_data_slot == "2"


@pytest.mark.parametrize("slot", ["3", "4", "5"])
def test_reserved_auction_window_slots_are_refused(slot: str) -> None:
    with pytest.raises(ValueError, match=f"kis_data_slot {slot} is reserved"):
        MarketSweepSettings(enabled=True, kis_data_slot=slot)


def test_enabled_requires_slot_and_markets() -> None:
    with pytest.raises(ValueError, match="kis_data_slot must be set when enabled"):
        MarketSweepSettings(enabled=True)
    with pytest.raises(ValueError, match="markets must not be empty"):
        MarketSweepSettings(markets=())


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"auction_open_times": ()}, "auction_open_times must not be empty"),
        ({"auction_close_times": (dt.time(15, 23), dt.time(15, 23))}, "auction_close_times must be strictly ascending"),
        ({"auction_open_deadline": dt.time(9, 40)}, "market sweep order"),
        ({"investor_estimate_times": (dt.time(15, 21),)}, "market sweep order"),
        ({"auction_close_deadline": dt.time(15, 36)}, "market sweep order"),
        ({"run_end": dt.time(15, 40)}, "market sweep order"),
    ],
)
def test_schedule_order_violations_are_rejected(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        MarketSweepSettings(**overrides)  # type: ignore[arg-type]


def test_runtime_rejects_sweep_on_snapshot_slot() -> None:
    with pytest.raises(ValueError, match="market_sweep kis_data_slot must not equal snapshot kis_data_slot"):
        resolve_collector_runtime(
            snapshot=SnapshotSettings(kis_data_slot="2"),
            market_sweep=MarketSweepSettings(enabled=True, kis_data_slot="2"),
        )
    runtime = resolve_collector_runtime(market_sweep=MarketSweepSettings(enabled=True, kis_data_slot="6"))

    assert runtime.market_sweep.kis_data_slot == "6"
