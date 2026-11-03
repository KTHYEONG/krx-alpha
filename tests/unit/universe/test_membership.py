from __future__ import annotations

import json
import multiprocessing
import pathlib
from typing import Literal

import pytest

from src.universe.membership import (
    MembershipFileError,
    MembershipRow,
    append_membership,
    effective_tiers,
    read_membership,
)

_WORKERS = 4
_ROWS_PER_WORKER = 100


def _row(
    symbol: str,
    tier: Literal["A", "B", "C"] = "B",
    *,
    source: str = "static",
    decided_at_ns: int = 0,
    reason: str | None = None,
) -> MembershipRow:
    return MembershipRow(
        symbol=symbol,
        tier=tier,
        source=source,
        decided_at_ns=decided_at_ns,
        direction=None,
        warning_level=None,
        ls_shard=None,
        kis_slot=None,
        trade_value_krw=None,
        reason=reason,
    )


def _append_worker(path: str, worker: int) -> None:
    for i in range(_ROWS_PER_WORKER):
        padding = "x" * 200
        append_membership(
            pathlib.Path(path),
            [_row(f"W{worker}-{i:03d}", reason=f"accepted:{padding}")],
        )


def test_append_then_read_round_trips_every_field(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "manifest" / "membership" / "2026-11-02.jsonl"
    full = MembershipRow(
        symbol="005930",
        tier="A",
        source="static",
        decided_at_ns=1_700_000_000_000_000_000,
        direction="up",
        warning_level="none",
        ls_shard=0,
        kis_slot="1",
        trade_value_krw=123_456_789,
        reason="plan",
    )
    append_membership(path, [full, _row("000660", "C", decided_at_ns=5)])

    assert read_membership(path) == (full, _row("000660", "C", decided_at_ns=5))


def test_append_empty_sequence_is_noop_and_creates_nothing(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "membership" / "day.jsonl"

    append_membership(path, [])

    assert not path.exists()
    assert not path.parent.exists()


def test_read_missing_file_returns_empty(tmp_path: pathlib.Path) -> None:
    assert read_membership(tmp_path / "absent.jsonl") == ()


def test_concurrent_appends_produce_parseable_lines_without_interleaving(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "day.jsonl"
    procs = [multiprocessing.Process(target=_append_worker, args=(str(path), w)) for w in range(_WORKERS)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=120)
    assert all(proc.exitcode == 0 for proc in procs)

    lines = path.read_bytes().splitlines()
    assert len(lines) == _WORKERS * _ROWS_PER_WORKER
    for line in lines:
        assert isinstance(json.loads(line), dict)

    rows = read_membership(path)
    assert len({row.symbol for row in rows}) == _WORKERS * _ROWS_PER_WORKER


def test_promotion_only_c_then_b_then_c_yields_b() -> None:
    rows = [
        _row("005930", "C", decided_at_ns=1),
        _row("005930", "B", decided_at_ns=2),
        _row("005930", "C", decided_at_ns=3),
    ]

    assert effective_tiers(rows)["005930"] == rows[1]


def test_later_c_never_demotes_a() -> None:
    rows = [_row("005930", "A", decided_at_ns=1), _row("005930", "C", decided_at_ns=2)]

    assert effective_tiers(rows)["005930"] == rows[0]


def test_equal_tier_latest_row_wins() -> None:
    rows = [_row("005930", "B", decided_at_ns=1, reason="first"), _row("005930", "B", decided_at_ns=2, reason="second")]

    assert effective_tiers(rows)["005930"] == rows[1]


def test_ack_rows_never_change_tier() -> None:
    b_row = _row("005930", "B", decided_at_ns=1)
    ack = _row("005930", "A", source="ack", decided_at_ns=2, reason="accepted:00000")

    assert effective_tiers([b_row, ack])["005930"] == b_row


def test_ack_only_symbol_is_absent_from_effective_tiers() -> None:
    assert "005930" not in effective_tiers([_row("005930", "A", source="ack")])


def test_malformed_truncated_line_names_line_number(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "day.jsonl"
    good = json.dumps(
        {
            "symbol": "005930",
            "tier": "A",
            "source": "static",
            "decided_at_ns": 1,
            "direction": None,
            "warning_level": None,
            "ls_shard": 0,
            "kis_slot": None,
            "trade_value_krw": None,
            "reason": None,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    path.write_text(good + "\n" + good[:40] + "\n", encoding="utf-8")

    with pytest.raises(MembershipFileError, match="line 2"):
        read_membership(path)


def test_blank_line_is_malformed(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "day.jsonl"
    append_membership(path, [_row("005930")])
    with path.open("ab") as handle:
        handle.write(b"\n")

    with pytest.raises(MembershipFileError, match="line 2"):
        read_membership(path)


@pytest.mark.parametrize(
    "line",
    [
        '["005930", "A"]',
        '{"symbol": "005930"}',
        '{"symbol": "005930", "tier": "D", "source": "static", "decided_at_ns": 1, "direction": null, '
        '"warning_level": null, "ls_shard": null, "kis_slot": null, "trade_value_krw": null, "reason": null}',
    ],
)
def test_wrong_shape_or_tier_is_malformed(tmp_path: pathlib.Path, line: str) -> None:
    path = tmp_path / "day.jsonl"
    path.write_text(line + "\n", encoding="utf-8")

    with pytest.raises(MembershipFileError, match="line 1"):
        read_membership(path)


def test_append_rejects_invalid_row_without_writing(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "day.jsonl"
    append_membership(path, [_row("005930")])
    before = path.read_bytes()

    with pytest.raises(MembershipFileError):
        append_membership(path, [_row("000660"), _row("035720", "D")])  # type: ignore[arg-type]

    assert path.read_bytes() == before


def test_duplicate_static_rows_collapse_to_one_entry_per_symbol() -> None:
    plan = [_row("005930", "A", decided_at_ns=1), _row("000660", "B", decided_at_ns=1)]

    tiers = effective_tiers(plan + plan)

    assert set(tiers) == {"005930", "000660"}
    assert tiers["005930"] == plan[0]
    assert tiers["000660"] == plan[1]
