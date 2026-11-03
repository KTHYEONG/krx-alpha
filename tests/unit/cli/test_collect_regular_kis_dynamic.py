from __future__ import annotations

import argparse
import asyncio
import json
import pathlib

import pytest

import src.cli.collect_regular_kis as crk
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.realtime.kis_sharding import KisDataCredential
from src.universe.intraday import DynamicEntry, DynamicPlan, write_dynamic_admissions

_DAY = "2026-11-03"
_FAR_FUTURE_NS = 4_102_444_800_000_000_000


class _Manifest:
    def __init__(self, expected_close_ns: int) -> None:
        self.expected_close_ns = expected_close_ns


class _Session:
    def __init__(self, expected_close_ns: int) -> None:
        self.manifest = _Manifest(expected_close_ns)


def _env(
    monkeypatch, *, intraday: bool = True, slot: str = "5", regular_slots=("1", "2", "3", "4"), regular: bool = True
) -> None:
    monkeypatch.setenv("KRX_ALPHA_INTRADAY_ENABLED", "true" if intraday else "false")
    monkeypatch.setenv("KRX_ALPHA_INTRADAY_KIS_DYNAMIC_SLOT", slot)
    monkeypatch.setenv("KRX_ALPHA_REGULAR_KIS_ENABLED", "true" if regular else "false")
    monkeypatch.setenv("KRX_ALPHA_REGULAR_KIS_CREDENTIAL_SLOTS", json.dumps(list(regular_slots)))
    monkeypatch.setenv("KRX_ALPHA_REGULAR_KIS_PAIR_CAPACITY_PER_CONNECTION", "41")
    monkeypatch.setattr(
        crk,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", "app-5", "secret-5", "hts-5", "kid5"),),
    )


def _args(tmp_path: pathlib.Path, *, slot: str = "5", key_id: str = "kid5") -> argparse.Namespace:
    data = tmp_path / "data"
    return argparse.Namespace(
        session_date=_DAY,
        journal_root=str(data / "journal"),
        manifest_path=str(data / "manifest" / "dyn.json"),
        credential_slot=slot,
        credential_key_id=key_id,
        ntp_host=None,
        max_clock_offset_ns=None,
        max_cycles=None,
    )


def _run(args: argparse.Namespace) -> int:
    return asyncio.run(crk._run_dynamic_stream(args))


def test_disabled_intraday_is_refused(monkeypatch, tmp_path) -> None:
    _env(monkeypatch, intraday=False)

    with pytest.raises(KrxAlphaError, match="intraday admission is disabled"):
        _run(_args(tmp_path))


def test_slot_other_than_configured_dynamic_slot_is_refused(monkeypatch, tmp_path) -> None:
    _env(monkeypatch, slot="5")

    with pytest.raises(KrxAlphaError, match="not the configured kis_dynamic_slot"):
        _run(_args(tmp_path, slot="4"))


def test_dynamic_slot_inside_regular_slots_is_refused(monkeypatch, tmp_path) -> None:
    _env(monkeypatch, slot="5", regular_slots=("1", "2", "3", "5"))

    with pytest.raises(KrxAlphaError, match="must not be a regular_kis credential slot"):
        _run(_args(tmp_path))


def test_credential_fingerprint_mismatch_is_refused(monkeypatch, tmp_path) -> None:
    _env(monkeypatch)

    with pytest.raises(MissingCredentialsError, match="fingerprint mismatch"):
        _run(_args(tmp_path, key_id="other"))


def test_closed_window_returns_without_connecting(monkeypatch, tmp_path) -> None:
    _env(monkeypatch)
    monkeypatch.setattr(crk, "bootstrap_session", lambda cfg: _Session(expected_close_ns=0))
    called: list[object] = []
    monkeypatch.setattr(crk, "run_streaming_child", lambda **kwargs: called.append(kwargs))

    assert _run(_args(tmp_path)) == 0
    assert called == []


def test_open_window_streams_dynamic_pairs_with_live_watch(monkeypatch, tmp_path) -> None:
    _env(monkeypatch)
    data = tmp_path / "data"
    write_dynamic_admissions(
        data / "manifest" / "membership" / f"{_DAY}.dynamic.json",
        DynamicPlan(rev=1, entries=(DynamicEntry("005930", "kis:5", 1), DynamicEntry("000660", "kis:5", 1))),
    )
    monkeypatch.setattr(crk, "bootstrap_session", lambda cfg: _Session(expected_close_ns=_FAR_FUTURE_NS))
    captured: dict[str, object] = {}

    async def fake_child(**kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(crk, "run_streaming_child", fake_child)

    assert _run(_args(tmp_path)) == 0

    watch = captured["live_watch"]
    assert captured["replay_pairs"] == watch.pairs()  # type: ignore[attr-defined]
    assert ("005930", "H0STCNT0") in captured["replay_pairs"]  # type: ignore[operator]
    assert ("000660", "H0STASP0") in captured["replay_pairs"]  # type: ignore[operator]
    adapter = captured["adapter_factory"](object())  # type: ignore[operator]
    assert adapter.name == "kis"
    assert adapter.capacity_pairs == 41


def test_regular_kis_disabled_is_refused(monkeypatch, tmp_path) -> None:
    _env(monkeypatch, regular=False)

    with pytest.raises(KrxAlphaError, match="regular_kis collection is disabled"):
        _run(_args(tmp_path))


def test_invalid_clock_offset_is_refused(monkeypatch, tmp_path) -> None:
    _env(monkeypatch)
    args = _args(tmp_path)
    args.max_clock_offset_ns = 3_000_000_000

    with pytest.raises(ValueError, match="at most 2 seconds"):
        _run(args)
