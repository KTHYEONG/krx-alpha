from __future__ import annotations

import datetime as dt
import pathlib

import polars as pl

from src.core.paths import DataPaths
from src.storage.l0_gapfill import load_tape_for_symbols, received_trades_for_gaps


def test_load_tape_filters_and_zero_pads(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 20)
    part = paths.tick_tape_root / f"dt={day.isoformat()}"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": [day, day, day],
            "symbol": ["000001", "000002", "000001"],
            "tape_seq": [0, 1, 2],
            "ts_hms": [90000, 90001, 90002],
            "price": [10, 20, 30],
            "volume": [1, 2, 3],
            "vendor": ["kiwoom"] * 3,
            "run_id": ["r"] * 3,
        }
    ).write_parquet(part / "run-a.parquet")
    out = load_tape_for_symbols(paths, day, {"1", "000002"})
    assert sorted(out.get_column("symbol").to_list()) == ["000001", "000001", "000002"]
    assert out.schema["symbol"] == pl.String


def test_load_tape_missing_day_is_empty(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    out = load_tape_for_symbols(paths, dt.date(2026, 10, 21), {"000001"})
    assert out.height == 0
    assert out.columns == ["symbol", "ts_hms", "price", "volume"]
    assert load_tape_for_symbols(paths, dt.date(2026, 10, 21), set()).height == 0
    assert load_tape_for_symbols(paths, dt.date(2026, 10, 21), {""}).height == 0


def test_load_tape_glob_error_is_empty(tmp_path: pathlib.Path, monkeypatch) -> None:
    import pathlib as _pathlib

    paths = DataPaths(tmp_path)
    day = dt.date(2026, 10, 21)
    (paths.tick_tape_root / f"dt={day.isoformat()}").mkdir(parents=True)
    monkeypatch.setattr(_pathlib.Path, "glob", lambda self, pattern: (_ for _ in ()).throw(OSError("down")))
    out = load_tape_for_symbols(paths, day, {"000001"})
    assert out.height == 0


def test_received_trades_matches_cli_contract(tmp_path: pathlib.Path) -> None:
    import json

    import zstandard as zstd

    day = dt.date(2026, 10, 20)
    audit = {
        "symbols": [
            "not-a-mapping",
            {"symbol": 123, "intervals": []},
            {"symbol": "000001", "intervals": ["not-a-mapping", {"chetime_prev": None}]},
            {"symbol": "000001", "intervals": [{"chetime_prev": "bad", "chetime_next": "alsobad"}]},
            {
                "symbol": "000001",
                "intervals": [
                    {"chetime_prev": "090058", "chetime_next": "090056"},
                    {"chetime_prev": "090056", "chetime_next": "090057"},
                    {
                        "missing_trades": 1,
                        "chetime_prev": "090056",
                        "chetime_next": "090058",
                        "cum_prev": 10,
                        "cum_next": 12,
                    }
                ],
            },
        ]
    }
    part = tmp_path / "l0" / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True)

    def _record(chetime: str, cum: int, price: int, volume: int) -> dict[str, object]:
        body = {
            "shcode": "000001",
            "chetime": chetime,
            "mdchecnt": "0",
            "mschecnt": str(cum),
            "price": str(price),
            "cvolume": str(volume),
        }
        return {"raw": json.dumps({"body": body}), "recv_wall_ns": 1, "conn_id": "c", "conn_seq": cum}

    lines = [_record("090056", 10, 100, 1), _record("090058", 12, 103, 4)]
    payload = "".join(json.dumps(item) + "\n" for item in lines).encode()
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress(payload))
    found = received_trades_for_gaps(audit, tmp_path / "l0", vendor="ls", day=day)
    assert found[("000001", 90056)] == [(10, 100, 1)]
    assert found[("000001", 90058)] == [(12, 103, 4)]
