from __future__ import annotations

from src.core.config import AuditSettings
from src.storage.l0_gapfill import GapfillInterval, GapfillStatus
from src.storage.l0_repair import evaluate_repaired_loss


def _settings(**overrides) -> AuditSettings:
    return AuditSettings(**overrides)


def _audit(symbols, *, trades: int, confirmed: int = 0):
    return {
        "totals": {"trades_observed": trades, "confirmed_missing": confirmed},
        "symbols": list(symbols),
    }


def _ledger(symbol="005930", *, trades=1_600_000, confirmed=0, status="OK", intervals=()):
    return {
        "symbol": symbol,
        "status": status,
        "trades_observed": trades,
        "confirmed_missing": confirmed,
        "intervals": list(intervals),
    }


def _interval(missing, *, kind="confirmed", prev="090000", nxt="090002"):
    return {
        "missing_trades": missing,
        "chetime_prev": prev,
        "chetime_next": nxt,
        "kind": kind,
        "cum_prev": 1,
        "cum_next": missing + 2,
    }


def _outcome(interval_id=0, *, symbol="005930", missing=1, recovered=0):
    return GapfillInterval(
        interval_id=interval_id,
        symbol=symbol,
        chetime_prev="090000",
        chetime_next="090002",
        cum_prev=1,
        cum_next=missing + 2,
        missing_trades=missing,
        recovered=recovered,
        residual=max(0, missing - recovered),
        status=GapfillStatus.FULL if recovered >= missing else GapfillStatus.NONE,
    )


def test_full_recovery_passes() -> None:
    audit = _audit(
        [_ledger(trades=3_877_222, confirmed=23_731, intervals=[_interval(23_731)])],
        trades=3_877_222,
        confirmed=23_731,
    )
    verdict = evaluate_repaired_loss(audit, [_outcome(missing=23_731, recovered=23_731)], _settings())
    assert verdict.status == "PASS"
    assert verdict.residual_confirmed == 0
    assert verdict.reasons == ()


def test_recovery_is_limited_to_matching_interval_and_symbol() -> None:
    audit = _audit(
        [_ledger(confirmed=20, intervals=[_interval(10), _interval(10)])], trades=1_600_000, confirmed=20
    )
    outcomes = [_outcome(missing=10, recovered=100), _outcome(interval_id=1, symbol="000001", missing=10, recovered=10)]
    assert evaluate_repaired_loss(audit, outcomes, _settings()).residual_confirmed == 10


def test_residual_over_budget_fails() -> None:
    audit = _audit(
        [_ledger(trades=1_000_000, confirmed=6_000, intervals=[_interval(6_000)])],
        trades=1_000_000,
        confirmed=6_000,
    )
    verdict = evaluate_repaired_loss(audit, [_outcome(missing=6_000, recovered=0)], _settings())
    assert verdict.status == "FAIL"
    assert verdict.residual_confirmed == 6_000
    assert verdict.reasons == ("residual_loss_exceeded:6000/1000000",)


def test_count_only_recovery_does_not_reduce_confirmed() -> None:
    audit = _audit(
        [
            _ledger(
                trades=1_000_000,
                confirmed=10,
                intervals=[_interval(10, kind="confirmed"), _interval(5, kind="count_only")],
            )
        ],
        trades=1_000_000,
        confirmed=10,
    )
    outcomes = [
        _outcome(interval_id=0, missing=10, recovered=0),
        _outcome(interval_id=1, missing=5, recovered=5),
    ]
    verdict = evaluate_repaired_loss(audit, outcomes, _settings())
    assert verdict.residual_confirmed == 10
    assert verdict.status == "PASS"


def test_symbol_residual_breach_names_symbol() -> None:
    audit = _audit(
        [_ledger(symbol="000001", trades=1_000, confirmed=70, intervals=[_interval(70)])],
        trades=1_000_000,
        confirmed=70,
    )
    verdict = evaluate_repaired_loss(audit, [_outcome(missing=70, recovered=0)], _settings())
    assert verdict.status == "FAIL"
    assert any(r.startswith("residual_symbol_loss_exceeded:000001:70/1000") for r in verdict.reasons)


def test_zero_trades_passes() -> None:
    audit = _audit([_ledger(trades=0, confirmed=0, intervals=[])], trades=0)
    verdict = evaluate_repaired_loss(audit, [], _settings())
    assert verdict.status == "PASS"
    assert verdict.ratio == 0.0


def test_warn_when_above_warn_ratio() -> None:
    settings = _settings(loss_warn_ratio=1e-4, loss_fail_ratio=5e-3)
    audit = _audit(
        [_ledger(trades=1_000_000, confirmed=200, intervals=[_interval(200)])],
        trades=1_000_000,
        confirmed=200,
    )
    verdict = evaluate_repaired_loss(audit, [_outcome(missing=200, recovered=0)], settings)
    assert verdict.status == "WARN"
    assert verdict.reasons == ("residual_loss_warn:200/1000000",)


def test_tolerates_malformed_audit_and_outcomes() -> None:
    verdict = evaluate_repaired_loss({"totals": None, "symbols": None}, [], _settings())
    assert verdict.status == "PASS"
    assert verdict.trades_observed == 0
    audit = _audit([{"symbol": "", "intervals": "bad"}, "bad"], trades=0)
    verdict = evaluate_repaired_loss(audit, [object()], _settings())
    assert verdict.status == "PASS"


def test_defensive_branches_floor_and_skip() -> None:
    bad_totals = {"totals": {"trades_observed": "bad"}, "symbols": []}
    assert evaluate_repaired_loss(bad_totals, [], _settings()).status == "PASS"
    neg_totals = {"totals": {"trades_observed": -5}, "symbols": []}
    assert evaluate_repaired_loss(neg_totals, [], _settings()).trades_observed == 0
    audit = _audit(
        [
            {
                "symbol": "000001",
                "status": "OK",
                "trades_observed": "bad",
                "confirmed_missing": "bad",
                "intervals": [_interval(5)],
            }
        ],
        trades=1_000_000,
        confirmed=0,
    )
    assert evaluate_repaired_loss(audit, [], _settings()).residual_confirmed == 0
    audit = _audit(
        [_ledger(trades=1_000_000, confirmed=5, intervals=[_interval(5)])],
        trades=1_000_000,
        confirmed=5,
    )
    assert evaluate_repaired_loss(audit, [], _settings()).residual_confirmed == 5
    bad_recovered = _outcome(missing=5, recovered=0)
    object.__setattr__(bad_recovered, "recovered", "bad")
    verdict = evaluate_repaired_loss(audit, [bad_recovered], _settings())
    assert verdict.residual_confirmed == 5


def test_repair_job_emits_repaired_reconciliation_once(tmp_path, caplog) -> None:
    import datetime as dt
    import json

    import polars as pl
    import zstandard as zstd

    from src.core.paths import DataPaths
    from src.storage import l0_repair_job as job

    day = dt.date(2026, 10, 23)
    paths = DataPaths(tmp_path)
    part = paths.journal_root / "ls/krx/regular/H0STCNT0" / f"dt={day}"
    part.mkdir(parents=True, exist_ok=True)
    kst = dt.timezone(dt.timedelta(hours=9))
    records = []
    for seq, (hms, cum, volume) in enumerate((("090000", 1, 1), ("090002", 3, 3))):
        body = {
            "shcode": "000001",
            "chetime": hms,
            "mdchecnt": "0",
            "mschecnt": str(cum),
            "price": "100",
            "cvolume": "1",
            "volume": str(volume),
        }
        records.append(
            {
                "raw": json.dumps({"body": body}),
                "conn_id": "c",
                "conn_seq": seq,
                "recv_wall_ns": int(dt.datetime.combine(day, dt.time(9, 0, seq * 2), kst).timestamp() * 1e9),
            }
        )
    out = part / "09.jsonl.zst"
    out.write_bytes(zstd.ZstdCompressor().compress("".join(json.dumps(r) + "\n" for r in records).encode()))
    audit_path = paths.session_audit_path(day, "ls")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "session_date": day.isoformat(),
                "vendor": "ls",
                "venue": "krx",
                "session": "regular",
                "status": "OK",
                "totals": {"confirmed_missing": 1, "trades_observed": 2, "trades_received": 2},
                "symbols": [
                    {
                        "symbol": "000001",
                        "confirmed_missing": 1,
                        "intervals": [
                            {
                                "missing_trades": 1,
                                "chetime_prev": "090000",
                                "chetime_next": "090002",
                                "cum_prev": 1,
                                "cum_next": 3,
                            }
                        ],
                    }
                ],
            }
        )
    )
    tape_path = paths.tick_tape_partition(day, "test")
    tape_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": ["000001"] * 3, "ts_hms": [90000, 90001, 90002], "price": [100] * 3, "volume": [1] * 3}
    ).write_parquet(tape_path)
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    with paths.tick_tape_ledger.open("a") as handle:
        handle.write(json.dumps({"symbol": "000001", "date": day.isoformat(), "status": "complete"}) + "\n")
    deadline = dt.datetime(2026, 10, 24, 5, 30, tzinfo=kst)
    with caplog.at_level("INFO"):
        summary = job.run_repair(
            paths=paths,
            journal_root=paths.journal_root,
            audit_settings=_settings(),
            repair_settings=__import__("src.core.config", fromlist=["RepairSettings"]).RepairSettings(),
            retain_days=3,
            window_date=day,
            deadline=deadline,
            now=lambda: dt.datetime(2026, 10, 24, 0, 10, tzinfo=kst),
            run_id="test",
        )
    assert summary.gapfilled == 1
    _, report_path = paths.gapfill_paths(day, "ls")
    doc = json.loads(report_path.read_bytes())
    assert doc["notes"]["verdict"]["status"] in ("PASS", "WARN", "FAIL")
    assert "repaired_reconciliation" in caplog.text
