"""collect-market-sweep CLI unit tests (hermetic: fake credentials, no vendor calls)."""

from __future__ import annotations

import argparse
import json
import logging
import os

import pytest

import src.cli.collect_market_sweep as sweep_cli
from src.realtime.kis_sharding import KisDataCredential


@pytest.fixture(autouse=True)
def _sweep_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    for key in list(os.environ):
        if key.startswith("KRX_ALPHA_"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("KRX_ALPHA_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("KRX_ALPHA_MARKET_SWEEP_ENABLED", "true")
    monkeypatch.setenv("KRX_ALPHA_MARKET_SWEEP_KIS_DATA_SLOT", "6")


def _args(tmp_path) -> argparse.Namespace:
    return argparse.Namespace(
        session_date="2026-11-24", market_map_path=str(tmp_path / "market_map.json"), max_iterations=1
    )


def test_run_sweeps_full_listing_on_slot_6_client(tmp_path, monkeypatch) -> None:
    (tmp_path / "market_map.json").write_text(
        json.dumps({"005930": "KOSPI", "000660": "KOSPI", "900110": "KONEX"}), encoding="utf-8"
    )
    monkeypatch.setattr(
        sweep_cli,
        "load_kis_data_credentials",
        lambda: (
            KisDataCredential("2", "snapshot-key", "s2", "hts", "kid2"),
            KisDataCredential("6", "sweep-key", "s6", "hts", "kid6"),
        ),
    )
    seen: dict = {}
    monkeypatch.setattr(sweep_cli, "run_market_sweep_session", lambda **kwargs: seen.update(kwargs))

    assert sweep_cli.run(_args(tmp_path)) == 0

    assert seen["source"]._transport._auth.app_key == "sweep-key"
    assert seen["symbols"] == ("000660", "005930")
    assert seen["settings"].kis_data_slot == "6"


def test_run_idles_without_calls_when_market_map_missing(tmp_path, monkeypatch, caplog) -> None:
    def _no_credentials():
        raise AssertionError("credentials must not be loaded without a symbol source")

    monkeypatch.setattr(sweep_cli, "load_kis_data_credentials", _no_credentials)
    idled: dict = {}
    monkeypatch.setattr(sweep_cli, "idle_until_run_end", lambda *args, **kwargs: idled.update(kwargs))

    with caplog.at_level(logging.WARNING, logger="src.cli.collect_market_sweep"):
        assert sweep_cli.run(_args(tmp_path)) == 0

    assert idled["max_iterations"] == 1
    assert "[DATA] stage=market_sweep status=DEGRADED reason=market_map_unreadable:FileNotFoundError" in caplog.text
