"""Conflating-peer gapfill rule guard tests (part 3/4 follow-up)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.core.config import RepairSettings
from src.storage.l0_gapfill import PeerTrade, derive_cross_vendor_fill


def _interval(missing=3, **overrides):
    base = {
        "missing_trades": missing,
        "chetime_prev": "090000",
        "chetime_next": "090002",
        "cum_prev": 10,
        "cum_next": 14,
        "volume_deficit": 250,
        "acml_prev": 1000,
        "acml_next_before": 1250,
    }
    base.update(overrides)
    return base


def _audit(*intervals, symbol="000001"):
    return {"symbols": [{"symbol": symbol, "intervals": list(intervals)}]}


def test_conflating_peer_needs_exact_interval_match() -> None:
    audit = _audit(_interval())
    peers = [("kiwoom", True, {"000001": [PeerTrade(1250, 90002, 5200, 250)]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}


def test_conflating_peer_fills_exact_interval() -> None:
    audit = _audit(_interval())
    peers = [
        (
            "kiwoom",
            True,
            {
                "000001": [
                    PeerTrade(1100, 90001, 5000, 100),
                    PeerTrade(1200, 90001, 5100, 100),
                    PeerTrade(1250, 90002, 5200, 50),
                ]
            },
        )
    ]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 3
    assert recovered == {0: 3}
    assert rows["provenance"].to_list() == ["gapfill:xvendor_kiwoom"] * 3
    assert residual["symbols"][0]["intervals"][0]["missing_trades"] == 0


def test_non_conflating_peer_wins_interval() -> None:
    audit = _audit(_interval())
    peers = [
        ("kis", {"000001": [PeerTrade(1100, 90001, 100, 100), PeerTrade(1200, 90001, 101, 100)]}),
        (
            "kiwoom",
            True,
            {
                "000001": [
                    PeerTrade(1100, 90001, 5000, 100),
                    PeerTrade(1200, 90001, 5100, 100),
                    PeerTrade(1250, 90002, 5200, 50),
                ]
            },
        ),
    ]
    rows, recovered, _ = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 2
    assert set(rows["vendor"].to_list()) == {"kis"}


def test_conflating_volume_mismatch_fills_nothing() -> None:
    audit = _audit(_interval())
    peers = [("kiwoom", True, {"000001": [PeerTrade(1100, 90001, 1, 10), PeerTrade(1200, 90001, 1, 10), PeerTrade(1250, 90002, 1, 10)]})]
    rows, recovered, _ = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}


def test_legacy_two_tuple_peers_stay_non_conflating() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [PeerTrade(1100, 90001, 100, 100)]})]
    rows, recovered, _ = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 1
    assert recovered == {0: 1}


def test_plain_peer_overflow_rejects_interval() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [PeerTrade(1100, 90001, 1, 200), PeerTrade(1200, 90001, 1, 200)]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}
    assert residual.get("xvendor_rejected_intervals") == 1


def test_plain_peer_count_overflow_rejects_interval() -> None:
    audit = _audit(_interval())
    peers = [("kis", {"000001": [
        PeerTrade(1100, 90001, 1, 10), PeerTrade(1150, 90001, 1, 10),
        PeerTrade(1200, 90001, 1, 10), PeerTrade(1250, 90002, 1, 10),
    ]})]
    rows, recovered, residual = derive_cross_vendor_fill(audit, peers)
    assert rows.height == 0
    assert recovered == {}
    assert residual.get("xvendor_rejected_intervals") == 1


def test_conflating_peers_default_and_validated() -> None:
    assert RepairSettings().xvendor_conflating_peers == ("kiwoom",)
    with pytest.raises(ValidationError):
        RepairSettings(xvendor_conflating_peers=("KIWOOM",))
    with pytest.raises(ValidationError):
        RepairSettings(xvendor_conflating_peers=("kiwoom", "kiwoom"))
