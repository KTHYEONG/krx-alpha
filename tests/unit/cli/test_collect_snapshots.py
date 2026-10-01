"""collect-snapshots CLI unit tests."""

from __future__ import annotations


def _slot_credential(slot: str = "1"):
    from src.realtime.kis_sharding import KisDataCredential

    return KisDataCredential(
        slot=slot, app_key="data-key", app_secret="data-secret", hts_id="hts", key_id="kid"
    )


def _args(tmp_path, **overrides):
    import argparse

    params = {
        "session_date": "2026-09-17",
        "candidates_path": str(tmp_path / "candidates.json"),
        "max_iterations": 1,
    }
    params.update(overrides)
    return argparse.Namespace(**params)


def test_run_builds_account_free_data_slot_client(tmp_path, monkeypatch) -> None:
    import hashlib
    import json

    import src.cli.collect_snapshots as collect_mod
    from src.brokers.kis.trading import KIS_LIVE_BASE_URL
    from src.cli.collect_snapshots import run
    from src.core.config import KisTokenSettings

    (tmp_path / "candidates.json").write_text(
        json.dumps({"candidates": [{"symbol": "005930"}, {"symbol": "000660"}]}), encoding="utf-8"
    )
    monkeypatch.setattr(collect_mod, "load_kis_data_credentials", lambda: (_slot_credential("1"),))
    seen: dict = {}
    monkeypatch.setattr(collect_mod, "run_snapshot_session", lambda **kwargs: seen.setdefault("snapshot_call", kwargs))

    exit_code = run(_args(tmp_path))

    assert exit_code == 0
    source = seen["snapshot_call"]["source"]
    transport = source._transport
    assert transport._auth.app_key == "data-key"
    assert not hasattr(source, "_creds")
    expected = f"token_{hashlib.sha256(b'data-key').hexdigest()[:12]}.json"
    assert transport._tokens._token_cache_path.name == expected
    assert transport._timeout_s == 5.0
    assert transport._base_url == KIS_LIVE_BASE_URL
    assert transport._tokens._allow_token_issue is KisTokenSettings().allow_issue
    assert seen["snapshot_call"]["symbols"] == ("005930", "000660")


def test_run_raises_when_configured_slot_missing(tmp_path, monkeypatch) -> None:
    import pytest

    import src.cli.collect_snapshots as collect_mod
    from src.cli.collect_snapshots import run
    from src.core.errors import MissingCredentialsError

    monkeypatch.setattr(collect_mod, "load_kis_data_credentials", lambda: (_slot_credential("2"),))

    with pytest.raises(MissingCredentialsError):
        run(_args(tmp_path))


def test_run_continues_with_empty_symbols_when_candidates_missing(tmp_path, monkeypatch, caplog) -> None:
    import logging

    import src.cli.collect_snapshots as collect_mod
    from src.cli.collect_snapshots import run

    monkeypatch.setattr(collect_mod, "load_kis_data_credentials", lambda: (_slot_credential("1"),))
    seen: dict = {}
    monkeypatch.setattr(collect_mod, "run_snapshot_session", lambda **kwargs: seen.update(kwargs))

    with caplog.at_level(logging.WARNING, logger="src.cli.collect_snapshots"):
        exit_code = run(_args(tmp_path))

    assert exit_code == 0
    assert seen["symbols"] == ()
    assert any("no_candidates" in rec.message for rec in caplog.records)


def test_run_continues_with_empty_symbols_when_candidates_corrupt(tmp_path, monkeypatch, caplog) -> None:
    import logging

    import src.cli.collect_snapshots as collect_mod
    from src.cli.collect_snapshots import run

    (tmp_path / "candidates.json").write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(collect_mod, "load_kis_data_credentials", lambda: (_slot_credential("1"),))
    seen: dict = {}
    monkeypatch.setattr(collect_mod, "run_snapshot_session", lambda **kwargs: seen.update(kwargs))

    with caplog.at_level(logging.WARNING, logger="src.cli.collect_snapshots"):
        exit_code = run(_args(tmp_path))

    assert exit_code == 0
    assert seen["symbols"] == ()
    assert any("corrupt candidates file" in rec.message for rec in caplog.records)
