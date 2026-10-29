from __future__ import annotations

import datetime as dt
import logging

from src.orchestration.eod import check_session_reconciliation


def _ns(day: dt.date, hour: int, minute: int, second: int = 0) -> int:
    from zoneinfo import ZoneInfo

    return int(dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=ZoneInfo("Asia/Seoul")).timestamp()) * 1_000_000_000


def _write_journal(root, stream: str, day: dt.date) -> None:
    part = root / "ls" / "krx" / "regular" / stream / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "10.jsonl.zst").write_bytes(b"x")


def _write_manifest(path, day: dt.date, gaps=()) -> None:
    from src.realtime.manifest import SessionManifest

    path.parent.mkdir(parents=True, exist_ok=True)
    manifest = SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0)
    for symbol, start, end in gaps:
        manifest.record_gap(symbol=symbol, gap_start_ns=start, gap_end_ns=end, reason="disconnect")
    manifest.save(path)


def _session(tmp_path, day: dt.date, gaps=()):
    journal_root = tmp_path / "l0"
    _write_journal(journal_root, "H0STCNT0", day)
    _write_journal(journal_root, "H0STASP0", day)
    manifest_path = tmp_path / "manifest" / f"{day.isoformat()}.json"
    _write_manifest(manifest_path, day, gaps)
    return journal_root, manifest_path


def _audit(day: dt.date, *, missing: int, trades: int = 1_600_000, tail: float = 0.0):
    from src.storage.l0_audit import AuditStatus, LedgerTotals, LossInterval, SessionAudit, SymbolLedger

    start = _ns(day, 9, 1)
    interval = LossInterval(
        symbol="005930",
        phase="regular",
        kind="confirmed",
        missing_trades=10,
        volume_deficit=5,
        cum_prev=1,
        cum_next=12,
        chetime_prev="090100",
        chetime_next="090101",
        recv_prev_ns=start,
        recv_next_ns=start + 2_000_000_000,
        conn_changed=False,
    )
    symbol = SymbolLedger(
        symbol="005930",
        status="OK",
        frames=max(trades, 1),
        trades_observed=trades,
        confirmed_missing=missing,
        count_only_missing=0,
        net_missing=0,
        volume_deficit=0,
        duplicates=0,
        regressions=0,
        resets=0,
        decode_fail=0,
        intervals=(interval,) if missing else (),
        intervals_truncated=0,
        manifest_frames=None,
    )
    totals = LedgerTotals(
        symbols=1,
        symbols_with_loss=int(missing > 0),
        symbols_without_counter=0,
        trades_observed=trades,
        confirmed_missing=missing,
        count_only_missing=0,
        net_missing_positive=0,
        volume_deficit=0,
        loss_ratio=(missing / trades) if trades else 0.0,
    )
    return SessionAudit(
        schema_version=1,
        session_date=day,
        vendor="ls",
        venue="krx",
        session="regular",
        status=AuditStatus.OK,
        reason="ok",
        generated_at_ns=1,
        sources=(),
        latency=(),
        symbols=(symbol,),
        totals=totals,
        tail_silence_s=tail,
        last_regular_tick_recv_ns=1,
    )


def _enforce(**overrides):
    from src.core.config import AuditMode, AuditSettings

    return AuditSettings(mode=AuditMode.ENFORCE, **overrides)


def test_loss_only_fail_becomes_pending_repair(tmp_path, caplog) -> None:
    day = dt.date(2026, 10, 28)
    journal_root, manifest_path = _session(tmp_path, day)
    audit = _audit(day, missing=23_731, trades=3_877_222)
    with caplog.at_level(logging.INFO):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=_enforce(),
            repair_pending=True,
        )
    assert ok is True
    pending = [r for r in caplog.records if "PENDING_REPAIR" in r.getMessage()]
    assert len(pending) == 1
    assert pending[0].levelno == logging.WARNING
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]


def test_tail_silence_still_fails_with_repair_pending(tmp_path, caplog) -> None:
    day = dt.date(2026, 10, 28)
    journal_root, manifest_path = _session(tmp_path, day)
    audit = _audit(day, missing=23_731, trades=3_877_222, tail=900.0)
    with caplog.at_level(logging.INFO):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=_enforce(),
            repair_pending=True,
            failure_level=logging.CRITICAL,
        )
    assert ok is False
    assert any(r.levelno == logging.CRITICAL and "tail_silence_exceeded" in r.getMessage() for r in caplog.records)


def test_structural_still_fails_with_repair_pending(tmp_path, caplog) -> None:
    day = dt.date(2026, 10, 28)
    journal_root, manifest_path = _session(tmp_path, day)
    audit = _audit(day, missing=23_731, trades=3_877_222)
    with caplog.at_level(logging.INFO):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0", "MISSING"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=_enforce(),
            repair_pending=True,
        )
    assert ok is False


def test_repair_disabled_keeps_old_behaviour(tmp_path, caplog) -> None:
    day = dt.date(2026, 10, 28)
    journal_root, manifest_path = _session(tmp_path, day)
    audit = _audit(day, missing=23_731, trades=3_877_222)
    with caplog.at_level(logging.INFO):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STCNT0", "H0STASP0"),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            audit=audit,
            audit_settings=_enforce(),
            repair_pending=False,
        )
    assert ok is False
    assert any(r.levelno == logging.CRITICAL and "trade_loss_exceeded" in r.getMessage() for r in caplog.records)
