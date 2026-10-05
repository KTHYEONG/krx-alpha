import datetime as dt
import logging

import polars as pl
import pytest


def _write_bars(path, rows: list[tuple[dt.date, str]], *, extra: dict | None = None) -> None:
    frame = pl.DataFrame({"date": [d for d, _ in rows], "symbol": [s for _, s in rows]})
    if extra:
        for key, values in extra.items():
            frame = frame.with_columns(pl.Series(key, values))
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)


def _store(tmp_path, files: dict[str, list[tuple[dt.date, str]]], **kwargs):
    root = tmp_path / "bars" / "daily"
    for name, rows in files.items():
        _write_bars(root / name, rows, **kwargs)
    return root


def test_first_bar_on_partition_date_is_listing(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(tmp_path, {
        "2026-09.parquet": [(dt.date(2026, 9, 26), "BBB"), (day, "BBB"), (day, "AAA")],
    })

    assert "AAA" in listing_day_symbols(root, day)


def test_symbol_with_earlier_history_is_not_listing(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(tmp_path, {
        "2026-09.parquet": [(dt.date(2026, 9, 26), "BBB"), (day, "BBB")],
    })

    assert "BBB" not in listing_day_symbols(root, day)


def test_store_start_is_not_listing(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 1)
    root = _store(tmp_path, {"2026-09.parquet": [(day, "CCC")]})

    assert listing_day_symbols(root, day) == frozenset()


def test_future_bars_do_not_change_answer(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(tmp_path, {
        "2026-09.parquet": [(dt.date(2026, 9, 26), "BBB"), (day, "AAA")],
    })
    before = listing_day_symbols(root, day)
    _write_bars(
        root / "2026-10.parquet",
        [(dt.date(2026, 10, 2), "AAA"), (dt.date(2026, 10, 2), "ZZZ")],
    )

    assert listing_day_symbols(root, day) == before == frozenset({"AAA"})


def test_missing_or_corrupt_store_degrades_to_empty(tmp_path, caplog) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    with caplog.at_level(logging.WARNING):
        assert listing_day_symbols(tmp_path / "no-such-dir", day) == frozenset()
    assert "stage=listing_days status=DEGRADED" in caplog.text
    caplog.clear()

    root = tmp_path / "bars" / "daily"
    root.mkdir(parents=True)
    (root / "2026-09.parquet").write_bytes(b"not-a-parquet")
    with caplog.at_level(logging.WARNING):
        assert listing_day_symbols(root, day) == frozenset()
    assert "stage=listing_days status=DEGRADED" in caplog.text


def test_only_date_and_symbol_are_read(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(
        tmp_path,
        {"2026-09.parquet": [(dt.date(2026, 9, 26), "BBB"), (day, "AAA")]},
        extra={"close": [1.0, 2.0], "volume": ["junk", "junk"]},
    )

    assert listing_day_symbols(root, day) == frozenset({"AAA"})


def test_legacy_daily_file_is_ignored(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = tmp_path / "bars" / "daily"
    root.mkdir(parents=True)
    pl.DataFrame({"date": [day], "symbol": ["LEG"]}).write_parquet(tmp_path / "bars" / "daily.parquet")

    assert listing_day_symbols(root, day) == frozenset()


def test_no_bar_for_day_returns_empty(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    root = _store(tmp_path, {
        "2026-09.parquet": [(dt.date(2026, 9, 26), "BBB"), (dt.date(2026, 9, 27), "BBB")],
    })

    assert listing_day_symbols(root, dt.date(2026, 9, 29)) == frozenset()


def test_only_future_bars_returns_empty(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    root = _store(tmp_path, {
        "2026-10.parquet": [(dt.date(2026, 10, 2), "AAA")],
    })

    assert listing_day_symbols(root, dt.date(2026, 9, 29)) == frozenset()


def test_all_null_rows_return_empty(tmp_path) -> None:
    import polars as pl

    from src.storage.listing_days import listing_day_symbols

    root = tmp_path / "bars" / "daily"
    root.mkdir(parents=True)
    pl.DataFrame({"date": [dt.date(2026, 9, 26), dt.date(2026, 9, 26)], "symbol": [None, None]}).write_parquet(root / "2026-09.parquet")

    assert listing_day_symbols(root, dt.date(2026, 9, 29)) == frozenset()


def test_corrupt_future_month_does_not_change_listing(tmp_path, caplog) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(tmp_path, {"2026-09.parquet": [(day - dt.timedelta(days=1), "OLD"), (day, "NEW")]})
    before = listing_day_symbols(root, day)
    (root / "2026-10.parquet").write_bytes(b"corrupt future partition")

    with caplog.at_level(logging.WARNING):
        assert listing_day_symbols(root, day) == before == frozenset({"NEW"})
    assert "status=DEGRADED" not in caplog.text


@pytest.mark.parametrize("bad_key", ["date", "symbol"])
def test_partial_unknown_history_degrades_without_listing(tmp_path, caplog, bad_key) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = tmp_path / "bars" / "daily"
    root.mkdir(parents=True)
    dates = [day - dt.timedelta(days=1), day - dt.timedelta(days=1), day]
    symbols = ["OLD", "NEW", "NEW"]
    if bad_key == "date":
        dates[1] = None
    else:
        symbols[1] = None
    pl.DataFrame({"date": dates, "symbol": symbols}).write_parquet(root / "2026-09.parquet")

    with caplog.at_level(logging.WARNING):
        assert listing_day_symbols(root, day) == frozenset()
    assert "stage=listing_days status=DEGRADED reason=ValueError" in caplog.text


def test_same_month_future_unknown_symbol_is_ignored(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(tmp_path, {"2026-09.parquet": [(day - dt.timedelta(days=1), "OLD"), (day, "NEW")]})
    before = listing_day_symbols(root, day)
    pl.DataFrame({"date": [day - dt.timedelta(days=1), day, day + dt.timedelta(days=1)],
                  "symbol": ["OLD", "NEW", None]}).write_parquet(root / "2026-09.parquet")

    assert listing_day_symbols(root, day) == before == frozenset({"NEW"})


def test_history_across_months_prevents_listing(tmp_path) -> None:
    from src.storage.listing_days import listing_day_symbols

    day = dt.date(2026, 9, 29)
    root = _store(tmp_path, {
        "2026-08.parquet": [(dt.date(2026, 8, 31), "OLD")],
        "2026-09.parquet": [(day, "OLD"), (day, "NEW")],
    })

    assert listing_day_symbols(root, day) == frozenset({"NEW"})
