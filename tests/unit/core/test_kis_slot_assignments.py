from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml
import pytest

from src.core.kis_keypool_provisioning import CANONICAL_SLOTS_LINE, DATA_SLOTS

TABLE_PATH = Path("deploy/shared/kis_slot_assignments.toml")
COMPOSE_PATH = Path("docker-compose.yml")

_OWNERS = frozenset({"krx-alpha", "k-closing-alpha", "k-stock-engine"})
_CHANNELS = frozenset({"rest", "ws"})
_DERIVATIONS = frozenset({"explicit", "kis_data_slots_prefix"})
_PURPOSE_RE = re.compile(r"[a-z0-9_]+")
_CLOCK_RE = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")


def _load_rows() -> list[dict[str, object]]:
    return tomllib.loads(TABLE_PATH.read_text(encoding="utf-8"))["assignment"]


def _krx_rows() -> dict[str, dict[str, object]]:
    return {row["purpose"]: row for row in _load_rows() if row["owner"] == "krx-alpha"}


def _compose_env() -> dict[str, str]:
    raw = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    return dict(item.split("=", 1) for item in raw["services"]["krx-collector"]["environment"])


def _window_overlaps(left: list[str], right: list[str]) -> bool:
    return max(left[0], right[0]) < min(left[1], right[1])


def test_table_schema_valid() -> None:
    document = tomllib.loads(TABLE_PATH.read_text(encoding="utf-8"))
    assert type(document["schema_version"]) is int
    assert document["schema_version"] == 1
    rows = document["assignment"]
    assert isinstance(rows, list)
    assert rows
    canonical_slots = {str(slot) for slot in DATA_SLOTS}
    seen: set[tuple[object, object]] = set()
    for row in rows:
        assert row["owner"] in _OWNERS
        assert isinstance(row["purpose"], str)
        assert _PURPOSE_RE.fullmatch(row["purpose"])
        assert (row["owner"], row["purpose"]) not in seen
        seen.add((row["owner"], row["purpose"]))
        assert row["channel"] in _CHANNELS
        slots = row["slots"]
        assert isinstance(slots, list)
        assert slots
        assert len(set(slots)) == len(slots)
        assert set(slots) <= canonical_slots
        window = row["window_kst"]
        assert isinstance(window, list)
        assert len(window) == 2
        assert all(isinstance(bound, str) for bound in window)
        assert all(_CLOCK_RE.fullmatch(bound) for bound in window)
        assert window[0] < window[1]
        assert isinstance(row["config_key"], str)
        assert row["config_key"].strip()
        assert row["derivation"] in _DERIVATIONS


def test_table_has_no_conflicts() -> None:
    rows = _load_rows()
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            if left["channel"] != right["channel"]:
                continue
            if not set(left["slots"]) & set(right["slots"]):
                continue
            assert not _window_overlaps(left["window_kst"], right["window_kst"])


@pytest.mark.parametrize(
    ("left", "right", "overlaps"),
    [
        (["08:00", "09:00"], ["09:00", "10:00"], False),
        (["09:00", "10:00"], ["08:00", "09:00"], False),
        (["08:00", "09:01"], ["09:00", "10:00"], True),
        (["08:00", "10:00"], ["08:30", "09:00"], True),
        (["08:00", "09:00"], ["08:00", "09:00"], True),
    ],
)
def test_window_overlap_boundaries(left: list[str], right: list[str], overlaps: bool) -> None:
    assert _window_overlaps(left, right) is overlaps


def test_krx_rows_exact() -> None:
    krx = _krx_rows()
    assert set(krx) == {"snapshot_rest", "premarket_ws", "aftermarket_ws"}

    assert krx["snapshot_rest"]["channel"] == "rest"
    assert krx["snapshot_rest"]["slots"] == ["2"]
    assert krx["snapshot_rest"]["window_kst"] == ["08:00", "15:39"]
    assert krx["snapshot_rest"]["config_key"] == "KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"
    assert krx["snapshot_rest"]["derivation"] == "explicit"

    assert krx["premarket_ws"]["channel"] == "ws"
    assert krx["premarket_ws"]["slots"] == ["5"]
    assert krx["premarket_ws"]["window_kst"] == ["07:58", "08:50"]
    assert krx["premarket_ws"]["config_key"] == "KRX_ALPHA_PREMARKET_CREDENTIAL_SLOT"
    assert krx["premarket_ws"]["derivation"] == "explicit"

    assert krx["aftermarket_ws"]["channel"] == "ws"
    assert krx["aftermarket_ws"]["slots"] == ["1", "2", "3", "4"]
    assert krx["aftermarket_ws"]["window_kst"] == ["15:40", "20:00"]
    assert krx["aftermarket_ws"]["config_key"] == "KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION"
    assert krx["aftermarket_ws"]["derivation"] == "kis_data_slots_prefix"


def test_compose_snapshot_slot_matches_table() -> None:
    env = _compose_env()
    assert [env["KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"]] == _krx_rows()["snapshot_rest"]["slots"]


def test_compose_premarket_slot_matches_table() -> None:
    env = _compose_env()
    assert env["KRX_ALPHA_PREMARKET_ENABLED"] == "true"
    assert [env["KRX_ALPHA_PREMARKET_CREDENTIAL_SLOT"]] == _krx_rows()["premarket_ws"]["slots"]


def test_aftermarket_derived_slots_match_table(caplog, monkeypatch) -> None:
    import logging
    import os

    from src.core.config import AftermarketSettings
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    for key in os.environ:
        if key.startswith("KRX_ALPHA_AFTERMARKET_"):
            monkeypatch.delenv(key)
    settings = AftermarketSettings()
    env = _compose_env()
    slots = CANONICAL_SLOTS_LINE.split("=", 1)[1].split(",")
    credentials = tuple(KisDataCredential(slot, f"key-{slot}", "secret", f"hts-{slot}", f"id-{slot}") for slot in slots)
    symbols = tuple(f"{index:06d}" for index in range(settings.max_symbols))
    pair_capacity = int(env["KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION"])

    with caplog.at_level(logging.WARNING, logger="src.realtime.kis_sharding"):
        plan = plan_aftermarket_shards(
            symbols=symbols,
            credentials=credentials,
            pair_capacity_per_connection=pair_capacity,
            krx_streams=settings.krx_streams,
            nxt_streams=settings.nxt_streams,
        )

    assert sorted({shard.credential_slot for shard in plan}) == _krx_rows()["aftermarket_ws"]["slots"]
    assert not any("TRUNCATED" in record.message for record in caplog.records)


def test_premarket_and_snapshot_slots_differ() -> None:
    krx = _krx_rows()
    assert krx["premarket_ws"]["slots"] != krx["snapshot_rest"]["slots"]


def test_assignment_table_excluded_from_image() -> None:
    patterns = Path(".dockerignore").read_text(encoding="utf-8").splitlines()
    assert TABLE_PATH.as_posix() in patterns
