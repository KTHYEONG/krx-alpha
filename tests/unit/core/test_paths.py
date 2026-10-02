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
    assert candidates != paths.aftermarket_candidates(day)
    assert manifest != paths.aftermarket_manifest_path(day, "nxt", 0)
    assert "aftermarket" not in manifest.parts
