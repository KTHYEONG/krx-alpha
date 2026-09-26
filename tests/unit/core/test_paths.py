def test_session_calendar_dir_derives_from_root(tmp_path) -> None:
    import pathlib

    from src.core.paths import DataPaths

    assert DataPaths(pathlib.Path(tmp_path) / "data").session_calendar_dir == pathlib.Path(tmp_path) / "data" / "calendar"
