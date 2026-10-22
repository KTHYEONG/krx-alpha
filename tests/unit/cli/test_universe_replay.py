"""universe-replay CLI exit-code guards."""

from __future__ import annotations


def test_universe_replay_cli_exit_codes(monkeypatch) -> None:
    import argparse

    import src.cli.universe_replay as cli_mod

    args = argparse.Namespace(start="2026-09-01", end="2026-09-02", overwrite=False)
    monkeypatch.setattr(
        cli_mod, "replay_universe_history",
        lambda **kw: type("S", (), {"written": 1, "skipped": 0})(),
    )
    assert cli_mod.run(args) == 0

    def _fail(**kw):
        raise ValueError("boom")

    monkeypatch.setattr(cli_mod, "replay_universe_history", _fail)
    assert cli_mod.run(args) == 4


def test_universe_replay_cli_invalid_dates(monkeypatch) -> None:
    import argparse

    import src.cli.universe_replay as cli_mod

    monkeypatch.setenv("KRX_ALPHA_DATA_ROOT", "scratch/unused-replay")
    assert cli_mod.run(argparse.Namespace(start="bad-date", end="2026-09-02", overwrite=False)) == 4
