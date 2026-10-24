from __future__ import annotations

import argparse
import datetime as dt
import json
import signal

import pytest

from src.cli import repair_ticks as cli
from src.core.paths import DataPaths
from src.storage.l0_repair_job import RepairRunSummary

KST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime(2026, 10, 24, 0, 10, tzinfo=KST)


@pytest.fixture
def args(tmp_path, monkeypatch):
    monkeypatch.setenv("KRX_ALPHA_REPAIR_JOB_ENABLED", "true")
    monkeypatch.setenv("KRX_ALPHA_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(cli, "_now_kst", lambda: NOW)
    return argparse.Namespace(data_root=tmp_path, journal_root=tmp_path / "l0", day=None, deadline=None, dry_run=False)


def candidate(paths, day):
    (paths.journal_root / "ls/krx/regular/H0STCNT0" / f"dt={day}").mkdir(parents=True)


def snapshot(root):
    return {p.relative_to(root): (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()}


def test_disabled_skips_without_writing(args, monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_REPAIR_JOB_ENABLED", "false")
    before = set(args.data_root.rglob("*"))
    assert cli.run(args) == 2
    assert set(args.data_root.rglob("*")) == before


def test_dry_run_reports_states_without_writing_even_disabled(args, monkeypatch, capsys) -> None:
    monkeypatch.setenv("KRX_ALPHA_REPAIR_JOB_ENABLED", "false")
    paths = DataPaths(args.data_root)
    candidate(paths, NOW.date() - dt.timedelta(days=1))
    candidate(paths, NOW.date())
    paths.root.joinpath("sentinel").write_bytes(b"unchanged")
    before = snapshot(paths.root)
    dirs = set(paths.root.rglob("*"))
    args.dry_run = True
    assert cli.run(args) == 0
    assert json.loads(capsys.readouterr().out) == {"days": [{"day": "2026-10-23", "vendor": "ls", "state": "unaudited"}]}
    assert snapshot(paths.root) == before
    assert set(paths.root.rglob("*")) == dirs


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_signal_stops_remaining_days_and_restores_handlers(args, monkeypatch, signum) -> None:
    import zstandard as zstd

    from src.storage import l0_repair_job as job

    paths = DataPaths(args.data_root)
    days = [NOW.date() - dt.timedelta(days=i) for i in (3, 2, 1)]
    for day in days:
        candidate(paths, day)
        raw = {"raw": json.dumps({"body": {"shcode": "000001", "chetime": "090000", "mdchecnt": "1",
                                         "mschecnt": "0", "price": "100", "volume": "1", "cvolume": "1"}}),
               "recv_wall_ns": int(dt.datetime.combine(day, dt.time(9), KST).timestamp() * 1e9)}
        part = paths.journal_root / "ls/krx/regular/H0STCNT0" / f"dt={day}"
        (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress((json.dumps(raw) + "\n").encode()))
    original = job.write_session_audit

    def write(audit, path):
        original(audit, path)
        signal.raise_signal(signum)

    monkeypatch.setattr(job, "write_session_audit", write)
    handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    assert cli.run(args) == 0
    assert paths.session_audit_path(days[0], "ls").exists()
    assert all(not paths.session_audit_path(day, "ls").exists() for day in days[1:])
    assert handlers == {s: signal.getsignal(s) for s in handlers}


@pytest.mark.parametrize(("status", "code"), [("OK", 0), ("DEGRADED", 0), ("SKIP", 2), ("FAIL", 4), ("bad", 4)])
def test_exit_codes_and_default_journal_root(args, monkeypatch, status, code) -> None:
    args.data_root = args.data_root / "output"
    args.journal_root = None

    def run(**kwargs):
        assert kwargs["journal_root"] == args.data_root.parent / "l0"
        assert kwargs["window_date"] == dt.date(2026, 10, 23)
        assert kwargs["deadline"] == NOW.replace(hour=5, minute=30)
        return RepairRunSummary(status, 0, 0, 0, 0, 0, "test")

    monkeypatch.setattr(cli, "run_repair", run)
    assert cli.run(args) == code


def test_invalid_deadline_and_settings_map_to_setup_failure(args, monkeypatch) -> None:
    args.deadline = "bad"
    assert cli.run(args) == 4
    args.deadline = None
    monkeypatch.setenv("KRX_ALPHA_REPAIR_HOLD_MAX_EXTRA_DAYS", "bad")
    assert cli.run(args) == 4


def test_job_exception_restores_handlers(args, monkeypatch) -> None:
    def fail(**kwargs):
        raise OSError("job failed")

    monkeypatch.setattr(cli, "run_repair", fail)
    before = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
    assert cli.run(args) == 4
    assert before == (signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT))


def test_deadline_timezone_and_window_boundaries() -> None:
    from src.core.config import TickTapeBackfillSettings

    cfg = TickTapeBackfillSettings()
    assert cli._parse_deadline("2026-10-24T03:00", cfg, NOW) == NOW.replace(hour=3, minute=0)
    assert cli._parse_deadline("2026-10-23T18:00:00Z", cfg, NOW) == NOW.replace(hour=3, minute=0)
    assert cli._next_window_end(NOW.replace(hour=6), cfg).date() == NOW.date() + dt.timedelta(days=1)
    assert cli._window_identity(NOW.replace(hour=23, minute=30), cfg) == NOW.date()
    assert cli._window_identity(NOW, cfg.model_copy(update={"window_start_kst": dt.time(0)})) == NOW.date()
    assert cli._now_kst().tzinfo is not None


def test_parser_registration_and_dry_run_no_candidates(args, capsys) -> None:
    from src.cli.main import build_parser

    parsed = build_parser().parse_args(["repair-ticks", "--day", "2026-10-23", "--dry-run"])
    assert parsed.handler is cli.run
    assert parsed.day == dt.date(2026, 10, 23)
    args.dry_run = True
    assert cli.run(args) == 0
    assert json.loads(capsys.readouterr().out) == {"days": []}


def test_main_dry_run_disables_persistent_log_writes(args, monkeypatch, capsys) -> None:
    from src.cli import main

    monkeypatch.setenv("KRX_ALPHA_PERSISTENT_LOGS", "true")
    observed = []
    monkeypatch.setattr(main, "configure_logging", lambda *a, **kw: observed.append(kw["log_dir"]))
    assert main.main(["repair-ticks", "--dry-run"]) == 0
    assert observed == [None]
