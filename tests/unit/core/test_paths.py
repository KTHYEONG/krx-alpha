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
