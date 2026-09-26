def _record(day=None, sent=0):
    import datetime as dt

    from src.core.lifecycle import DaemonLifecycleRecord

    return DaemonLifecycleRecord(
        run_id="daemon-1",
        started_at=dt.datetime(2026, 9, 25, 20, 0, tzinfo=dt.UTC),
        clean_exit=False,
        crash_error=None,
        crash_alert_day=day,
        crash_alerts_sent=sent,
    )


def test_crash_alert_budget_persists_across_restarts() -> None:
    import datetime as dt

    from src.core.lifecycle import crash_alert_allowed

    today = dt.date(2026, 9, 25)

    assert crash_alert_allowed(None, today, 3) is True
    assert crash_alert_allowed(_record(today, 2), today, 3) is True
    assert crash_alert_allowed(_record(today, 3), today, 3) is False
    assert crash_alert_allowed(_record(today, 3), today + dt.timedelta(days=1), 3) is True


def test_invalid_lifecycle_file_reads_as_none(tmp_path) -> None:
    from src.core.lifecycle import read_lifecycle

    missing = tmp_path / "daemon_lifecycle.json"
    assert read_lifecycle(missing) is None

    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    assert read_lifecycle(corrupt) is None

    wrong_shape = tmp_path / "shape.json"
    wrong_shape.write_text('{"run_id": "x"}', encoding="utf-8")
    assert read_lifecycle(wrong_shape) is None

    non_object = tmp_path / "list.json"
    non_object.write_text("[1, 2]", encoding="utf-8")
    assert read_lifecycle(non_object) is None


def test_lifecycle_write_then_read_round_trips(tmp_path) -> None:
    import datetime as dt

    from src.core.lifecycle import read_lifecycle, write_lifecycle

    record = _record(dt.date(2026, 9, 25), 2)
    path = tmp_path / "work" / "daemon_lifecycle.json"

    write_lifecycle(path, record)

    assert read_lifecycle(path) == record
    assert list(path.parent.glob("*.tmp")) == []
