from __future__ import annotations

import hashlib
import re
import tomllib
from pathlib import Path

import yaml
import pytest

from src.core.kis_keypool_provisioning import CANONICAL_SLOTS_LINE, DATA_SLOTS

TABLE_PATH = Path("deploy/shared/kis_slot_assignments.toml")
COMPOSE_PATH = Path("docker-compose.yml")

_OWNERS = frozenset({"krx", "kca", "kse"})
_CHANNELS = frozenset({"rest", "ws"})
_CLOCK_RE = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")


def _load_rows() -> list[dict[str, object]]:
    return tomllib.loads(TABLE_PATH.read_text(encoding="utf-8"))["assignment"]


def _krx_rows() -> dict[str, dict[str, object]]:
    document = tomllib.loads(TABLE_PATH.read_text(encoding="utf-8"))
    selectors = document["selectors"]["krx"]
    groups: dict[str, dict[str, object]] = {}
    for row in _load_rows():
        if row["project"] != "krx":
            continue
        if row["channel"] == "rest":
            name = "snapshot_rest"
            assert row["slot"] == selectors["KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"]
        elif row["purpose"] == "regular_kis_ws":
            name = "regular_kis_ws"
            assert str(row["slot"]) in [str(slot) for slot in selectors["KRX_ALPHA_REGULAR_KIS_CREDENTIAL_SLOTS"]]
        elif row["slot"] == selectors["KRX_ALPHA_PREMARKET_CREDENTIAL_SLOT"]:
            name = "premarket_ws"
        else:
            name = "aftermarket_ws"
        group = groups.setdefault(name, {"channel": row["channel"], "slots": [], "window_kst": row["windows"][0].split("-")})
        assert row["windows"] == ["-".join(group["window_kst"])]
        group["slots"].append(str(row["slot"]))
    return groups


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
    canonical_slots = set(DATA_SLOTS)
    assert set(document["pool"]) == canonical_slots
    assert set(document["host_data_slots"]) <= canonical_slots
    for selectors in document["selectors"].values():
        for value in selectors.values():
            assert set(value if isinstance(value, list) else [value]) <= canonical_slots
    seen: set[tuple[object, ...]] = set()
    for row in rows:
        assert row["project"] in _OWNERS
        assert isinstance(row["purpose"], str)
        assert row["purpose"].strip()
        identity = (row["slot"], row["project"], row["channel"], tuple(row["windows"]))
        assert identity not in seen
        seen.add(identity)
        assert row["channel"] in _CHANNELS
        assert type(row["slot"]) is int
        assert row["slot"] in canonical_slots
        assert row["windows"]
        for raw in row["windows"]:
            window = raw.split("-")
            assert len(window) == 2
            assert _CLOCK_RE.fullmatch(window[0])
            assert _CLOCK_RE.fullmatch(window[1]) or window[1] == "24:00"
            assert window[0] < window[1]


def test_table_has_no_conflicts() -> None:
    rows = _load_rows()
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            if left["channel"] != right["channel"]:
                continue
            if left["slot"] != right["slot"]:
                continue
            if left["channel"] == "rest" and left["project"] == right["project"]:
                continue
            for left_window in left["windows"]:
                for right_window in right["windows"]:
                    assert not _window_overlaps(left_window.split("-"), right_window.split("-"))


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
    assert set(krx) == {"snapshot_rest", "premarket_ws", "regular_kis_ws", "aftermarket_ws"}

    assert krx["snapshot_rest"]["channel"] == "rest"
    assert krx["snapshot_rest"]["slots"] == ["2"]
    assert krx["snapshot_rest"]["window_kst"] == ["08:00", "15:39"]

    assert krx["premarket_ws"]["channel"] == "ws"
    assert krx["premarket_ws"]["slots"] == ["5"]
    assert krx["premarket_ws"]["window_kst"] == ["07:58", "08:50"]

    assert krx["regular_kis_ws"]["channel"] == "ws"
    assert krx["regular_kis_ws"]["slots"] == ["1", "2", "3", "4"]
    assert krx["regular_kis_ws"]["window_kst"] == ["08:28", "15:32"]

    assert krx["aftermarket_ws"]["channel"] == "ws"
    assert krx["aftermarket_ws"]["slots"] == ["1", "2", "3", "4"]
    assert krx["aftermarket_ws"]["window_kst"] == ["15:40", "20:00"]


def test_compose_snapshot_slot_matches_table() -> None:
    env = _compose_env()
    assert [env["KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"]] == _krx_rows()["snapshot_rest"]["slots"]


def test_compose_premarket_slot_matches_table() -> None:
    env = _compose_env()
    assert env["KRX_ALPHA_PREMARKET_ENABLED"] == "true"
    assert [env["KRX_ALPHA_PREMARKET_CREDENTIAL_SLOT"]] == _krx_rows()["premarket_ws"]["slots"]


def test_compose_regular_kis_slots_match_table(monkeypatch) -> None:
    import json

    from src.core.config import RegularKisSettings

    env = _compose_env()
    assert env["KRX_ALPHA_REGULAR_KIS_ENABLED"] == "true"
    assert json.loads(env["KRX_ALPHA_REGULAR_KIS_CREDENTIAL_SLOTS"]) == _krx_rows()["regular_kis_ws"]["slots"]
    assert env["KRX_ALPHA_REGULAR_KIS_PAIR_CAPACITY_PER_CONNECTION"] == "41"
    for key, value in env.items():
        if key.startswith("KRX_ALPHA_REGULAR_KIS_"):
            monkeypatch.setenv(key, value)
    resolved = RegularKisSettings()
    assert resolved.enabled is True
    assert resolved.credential_slots == ("1", "2", "3", "4")


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
            krx_symbols=symbols,
            nxt_symbols=symbols,
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


def test_canonical_table_hash_pinned() -> None:
    raw = TABLE_PATH.read_bytes()
    assert raw.splitlines()[0] == b"# canonical: krx-alpha/deploy/shared/kis_slot_assignments.toml schema=1"
    assert hashlib.sha256(raw).hexdigest() == "4c0b40679733c885e16c0846893f3f4022b4fe252ed8ecf7faa75c4e20260ca3"
