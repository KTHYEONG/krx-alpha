"""Drive lake catalog: template normalization, aggregation, duplicate classification, CLI I/O."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.cli import lake_catalog as cat


@pytest.mark.parametrize(
    ("path", "template", "date"),
    [
        ("data/l1/ls/krx/regular/H0STCNT0/dt=2026-10-23.parquet", "data/l1/ls/krx/regular/H0STCNT0/dt=*.parquet", "2026-10-23"),
        ("data/l0/kis/krx/regular/H0STASP0/dt=2026-10-27/15.s0.jsonl.zst", "data/l0/kis/krx/regular/H0STASP0/dt=*/{hh}.s0.jsonl.zst", "2026-10-27"),
        ("data/public_archive/metrics/SEIUSDT/202403.parquet", "data/public_archive/metrics/{sym}/*.parquet", "2024-03"),
        ("data/live_capture/book_ticker/20261025/03.parquet", "data/live_capture/book_ticker/*/{hh}.parquet", "2026-10-25"),
        ("capture_sealed/raw/2026-10/2026-10-16/seg-0ef46e86c1fae555.tar.zst", "capture_sealed/raw/*/*/seg-*.tar.zst", "2026-10"),
        ("data/history/capture/quarantine/shrink/2026-10-01/regular/417030-2f8f3eb4c455.parquet",
         "data/history/capture/quarantine/shrink/*/regular/{sym}-*.parquet", "2026-10-01"),
        ("data/bars/daily.parquet", "data/bars/daily.parquet", None),
        ("lake/state/session=bogus/x.json", "lake/state/session=*/x.json", None),
    ],
)
def test_template_of_replaces_partition_tokens(path: str, template: str, date: str | None) -> None:
    assert cat.template_of(path) == (template, date)


def test_project_of_tiers() -> None:
    assert cat.project_of("live/krx-alpha/data/x") == "live/krx-alpha"
    assert cat.project_of("research/crypto-pilot-full/futures/a") == "research/crypto-pilot-full"
    assert cat.project_of("Stock.xlsx") == "Stock.xlsx"


def _entry(path: str, size: int, md5: str | None, mtime: str = "2026-10-30T00:00:00Z") -> dict[str, object]:
    entry: dict[str, object] = {"Path": path, "Size": size, "IsDir": False, "ModTime": mtime}
    if md5 is not None:
        entry["Hashes"] = {"md5": md5}
    return entry


def test_build_catalog_aggregates_and_classifies_duplicates() -> None:
    entries = [
        {"Path": "live/krx-alpha/data", "IsDir": True},
        _entry("live/krx-alpha/data/l1/a/dt=2026-10-01.parquet", 100, "aa", "2026-10-02T00:00:00Z"),
        _entry("live/krx-alpha/data/l1/a/dt=2026-10-03.parquet", 200_000, "bb", "2026-10-04T00:00:00Z"),
        _entry("live/krx-alpha/data/l1/a/dt=2026-10-03.parquet", 200_000, "bb", "2026-10-04T00:00:00Z"),
        _entry("live/kse/lake/state/seg-abcdef01.tar.zst", 5, "cc"),
        _entry("live/kse/lake/state/seg-abcdef01.tar.zst", 6, "dd"),
        _entry("live/kse/lake/ops/MANIFEST.json", 7, None),
        _entry("live/kse/lake/ops/MANIFEST.json", 7, None),
    ]
    doc = cat.build_catalog(entries, root="gdrive:quant-lake", generated_at=datetime(2026, 11, 2, tzinfo=UTC)).to_json()

    assert doc["schema"] == "quant-lake-catalog-v1"
    assert doc["generated_at"] == "2026-11-02T00:00:00+00:00"
    assert doc["files"] == 7
    (krx,) = doc["projects"]["live/krx-alpha"]["datasets"]
    assert krx == {
        "template": "data/l1/a/dt=*.parquet", "files": 3, "bytes": 400_100, "small_files": 1,
        "first": "2026-10-01", "last": "2026-10-03", "latest_mtime": "2026-10-04T00:00:00Z",
    }
    assert doc["duplicates"]["identical"] == ["live/krx-alpha/data/l1/a/dt=2026-10-03.parquet"]
    # differing content and missing hashes can never be proven identical
    assert doc["duplicates"]["conflicting"] == ["live/kse/lake/ops/MANIFEST.json", "live/kse/lake/state/seg-abcdef01.tar.zst"]


def _runner_for(listing: list[dict[str, object]], calls: list[list[str]], *, lsjson_rc: int = 0, copy_rc: int = 0, stdout: str | None = None):
    def _runner(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        if cmd[1] == "lsjson":
            return SimpleNamespace(returncode=lsjson_rc, stdout=json.dumps(listing) if stdout is None else stdout, stderr="boom")
        return SimpleNamespace(returncode=copy_rc, stdout="", stderr="quota")

    return _runner


def test_main_lists_remote_writes_and_publishes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[list[str]] = []
    listing = [_entry("live/krx-alpha/data/bars/daily.parquet", 10, "aa")]
    out = tmp_path / "out" / "catalog.json"
    rc = cat.main(["--out", str(out), "--publish", "--work", str(tmp_path / "w")], runner=_runner_for(listing, calls))

    assert rc == 0
    assert json.loads(out.read_text(encoding="utf-8"))["files"] == 1
    assert calls[0][-3:] == ["--hash", "--hash-type", "md5"]
    copy = calls[1]
    assert copy[:2] == ["rclone", "copyto"]
    assert copy[3] == "gdrive:quant-lake/_catalog/index.json"
    assert copy[-1].startswith("gdrive:quant-lake/_catalog/_versions/")
    assert json.loads((tmp_path / "w" / "index.json").read_text(encoding="utf-8"))["files"] == 1
    assert "project=live/krx-alpha files=1" in capsys.readouterr().out


def test_main_publishes_from_private_temp_dir_by_default() -> None:
    calls: list[list[str]] = []
    assert cat.main(["--publish"], runner=_runner_for([_entry("live/x/a.json", 1, "aa")], calls)) == 0
    staged = Path(calls[1][2])
    assert staged.name == "index.json"
    assert not staged.parent.exists()


def test_main_exit_2_on_conflicting_duplicates_from_saved_listing(tmp_path: Path) -> None:
    listing = tmp_path / "listing.json"
    listing.write_text(json.dumps([_entry("live/x/a.json", 1, "aa"), _entry("live/x/a.json", 2, "bb")]), encoding="utf-8")
    assert cat.main(["--from-json", str(listing)]) == 2


def test_rclone_failures_raise(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="lsjson failed rc=1"):
        cat.list_remote("gdrive:q", _runner_for([], [], lsjson_rc=1))
    with pytest.raises(RuntimeError, match="invalid JSON"):
        cat.list_remote("gdrive:q", _runner_for([], [], stdout="nope"))
    doc = cat.build_catalog([], root="gdrive:q", generated_at=datetime(2026, 11, 2, tzinfo=UTC)).to_json()
    with pytest.raises(RuntimeError, match="publish failed rc=1"):
        cat.publish(doc, "gdrive:q", tmp_path, _runner_for([], [], copy_rc=1))
