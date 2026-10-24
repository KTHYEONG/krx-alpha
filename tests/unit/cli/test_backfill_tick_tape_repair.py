from __future__ import annotations

import datetime as dt
import pathlib


def test_repair_order_fallback_never_aborts(tmp_path: pathlib.Path, monkeypatch) -> None:
    import src.cli.backfill_tick_tape as cli

    def _boom(*args, **kwargs):
        raise RuntimeError("order down")

    monkeypatch.setattr(cli, "pending_repair_symbol_days", _boom)
    monkeypatch.setattr(cli, "resolve_backfill_sessions", lambda *a: (dt.date(2026, 10, 21),))
    monkeypatch.setattr(cli, "read_ledger_done", lambda *a: frozenset())
    observed = []

    def plan(**kwargs):
        observed.append(kwargs["repair_symbol_days"])
        return ()

    monkeypatch.setattr(cli, "plan_tick_tape_targets", plan)
    import argparse

    paths_root = tmp_path
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    monkeypatch.setenv("KRX_ALPHA_TICK_TAPE_ENABLED", "true")
    args = argparse.Namespace(
        dry_run=False, data_root=paths_root, max_pages=None, deadline=None, only_symbols=None
    )
    assert cli.run(args) == 2
    assert observed == [frozenset()]
