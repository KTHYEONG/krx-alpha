def test_session_calendar_dir_derives_from_root(tmp_path) -> None:
    import pathlib

    from src.core.paths import DataPaths

    assert DataPaths(pathlib.Path(tmp_path) / "data").session_calendar_dir == pathlib.Path(tmp_path) / "data" / "calendar"


def test_premarket_paths_are_date_prefixed_and_isolated(tmp_path) -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")
    day = dt.date(2026, 10, 2)

    candidates = paths.premarket_candidates(day)
    manifest = paths.premarket_manifest_path(day)

    assert candidates.name.split(".")[0] == day.isoformat()
    assert manifest.name.split(".")[0] == day.isoformat()
    assert candidates != paths.aftermarket_candidates(day, "krx")
    assert manifest != paths.aftermarket_manifest_path(day, "nxt", 0)
    assert "aftermarket" not in manifest.parts


def test_aftermarket_candidates_path_per_venue(tmp_path) -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths
    from src.realtime.contracts import MarketVenue

    paths = DataPaths(pathlib.Path(tmp_path) / "data")
    day = dt.date(2026, 10, 19)

    assert paths.aftermarket_candidates(day, MarketVenue.KRX) == paths.universe_dir / "aftermarket" / "2026-10-19.json"
    assert paths.aftermarket_candidates(day, "nxt") == paths.universe_dir / "aftermarket" / "2026-10-19.nxt.json"


def test_aftermarket_candidates_rejects_unknown_venue(tmp_path) -> None:
    import datetime as dt
    import pathlib

    import pytest

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")

    with pytest.raises(ValueError, match="unknown aftermarket venue"):
        paths.aftermarket_candidates(dt.date(2026, 10, 19), "xyz")


def test_session_audit_path_layout_creates_nothing(tmp_path) -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")

    audit = paths.session_audit_path(dt.date(2026, 10, 15), "ls")

    assert audit == pathlib.Path(tmp_path) / "data" / "manifest" / "audit" / "2026-10-15.ls.regular.audit.json"
    assert not audit.exists()
    assert not audit.parent.exists()


def test_session_audit_path_name_is_date_first(tmp_path) -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")
    day = dt.date(2026, 10, 15)

    assert dt.date.fromisoformat(paths.session_audit_path(day, "ls").name.split(".")[0]) == day


def test_session_audit_path_rejects_empty_vendor(tmp_path) -> None:
    import datetime as dt
    import pathlib

    import pytest

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")

    with pytest.raises(ValueError, match="vendor"):
        paths.session_audit_path(dt.date(2026, 10, 15), "")


def test_regular_kis_manifest_path_layout() -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path("var/krx"))
    assert paths.regular_kis_manifest_path(dt.date(2026, 10, 22), 3) == pathlib.Path(
        "var/krx/manifest/regular_kis/2026-10-22.krx.shard-03.json"
    )


def test_universe_replay_out_partitions_by_policy_version(tmp_path) -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")
    day = dt.date(2026, 11, 2)

    assert paths.universe_replay_out(day) == paths.universe_replay_dir / "2026-11-02.parquet"
    assert paths.universe_replay_out(day, "tiered_v2_symmetric") == (
        paths.universe_replay_dir / "tiered_v2_symmetric" / "2026-11-02.parquet"
    )


def test_ls_manifest_paths_shard_zero_is_legacy_and_lists_existing_shards(tmp_path) -> None:
    import datetime as dt
    import pathlib

    import pytest

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")
    day = dt.date(2026, 11, 3)

    assert paths.ls_manifest_path(day, 0) == paths.manifest_path(day)
    assert paths.ls_manifest_path(day, 2) == paths.manifest_dir / "ls" / "2026-11-03.krx.shard-02.json"
    with pytest.raises(ValueError, match="shard_index"):
        paths.ls_manifest_path(day, -1)

    assert paths.ls_manifest_paths(day) == (paths.manifest_path(day),)

    ls_dir = paths.manifest_dir / "ls"
    ls_dir.mkdir(parents=True)
    shard_two = paths.ls_manifest_path(day, 2)
    shard_one = paths.ls_manifest_path(day, 1)
    shard_two.write_text("{}")
    shard_one.write_text("{}")
    (ls_dir / "2026-11-02.krx.shard-01.json").write_text("{}")
    (ls_dir / "2026-11-03.krx.shard-xx.json").write_text("{}")

    assert paths.ls_manifest_paths(day) == (paths.manifest_path(day), shard_one, shard_two)


def test_membership_and_universe_dropped_paths_layout(tmp_path) -> None:
    import datetime as dt
    import pathlib

    from src.core.paths import DataPaths

    paths = DataPaths(pathlib.Path(tmp_path) / "data")
    day = dt.date(2026, 11, 3)

    assert paths.membership_path(day) == paths.manifest_dir / "membership" / "2026-11-03.jsonl"
    assert paths.universe_dropped_dir == paths.root / "universe_dropped"
    assert paths.universe_dropped_out(day) == paths.root / "universe_dropped" / "2026-11-03.parquet"
