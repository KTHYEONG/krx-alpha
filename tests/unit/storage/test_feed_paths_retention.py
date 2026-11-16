"""New feed paths are covered by the existing L1 retention mapping: derived selection never supersedes L0; db/toss partitions do."""

from __future__ import annotations

import datetime as dt
import pathlib

import pytest

from src.storage.layout import l0_partition_for_l1


@pytest.mark.parametrize(
    ("repo_path", "expected"),
    [
        ("l1/derived/feed_selection/dt=2026-11-16.parquet", None),
        ("l1/db/krx/regular/H0STCNT0/dt=2026-11-16.parquet", "l0/db/krx/regular/H0STCNT0/dt=2026-11-16"),
        ("l1/toss/krx/regular/H0STCNT0/dt=2026-11-16.parquet", "l0/toss/krx/regular/H0STCNT0/dt=2026-11-16"),
    ],
)
def test_l1_repo_path_mapping_covers_new_paths(repo_path: str, expected: str | None) -> None:
    assert l0_partition_for_l1(repo_path) == expected


def test_derived_selection_relpath_is_outside_vendor_partitions() -> None:
    rel = pathlib.PurePath("derived/feed_selection/dt=2026-11-16.parquet")
    assert rel.parts[0] not in {"ls", "kis", "db", "toss", "kiwoom"}
    assert dt.date.fromisoformat("2026-11-16")
