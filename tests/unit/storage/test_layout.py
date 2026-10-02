"""Storage layout single-source invariant guards (P5)."""

from __future__ import annotations

import datetime as dt
import pathlib

from src.realtime.contracts import MarketSession, MarketVenue

DAY = dt.date(2026, 9, 18)


def test_round_trip_all_enum_combos(tmp_path) -> None:
    from src.storage.layout import (
        L0PartitionKey,
        L0PartitionRoute,
        l0_partition_relpath,
        route_for_l0_partition,
    )

    for venue in MarketVenue:
        for session in MarketSession:
            key = L0PartitionKey(
                vendor="kis", venue=venue, session=session, stream="H0STASP0", day=DAY
            )
            rel = l0_partition_relpath(key)
            assert rel.as_posix() == f"kis/{venue.value}/{session.value}/H0STASP0/dt=2026-09-18"
            root = tmp_path / "j"
            assert route_for_l0_partition(root / rel) == L0PartitionRoute(
                venue.value, session, "H0STASP0"
            )
            assert route_for_l0_partition(rel) == L0PartitionRoute(venue.value, session, "H0STASP0")


def test_session_set_pinned() -> None:
    assert {s.value for s in MarketSession} == {"regular", "krx_after", "nxt_after", "nxt_pre"}


def test_legacy_shapes_classify_as_default() -> None:
    from src.realtime.contracts import MarketSession
    from src.storage.layout import route_for_l0_partition

    for rel, stream in (
        ("ls/H0STASP0/dt=2026-09-18", "H0STASP0"),
        ("kis/krx/regular/dt=2026-09-18", "regular"),
        ("ls/krx/weird/S/dt=2026-09-18", "S"),
    ):
        route = route_for_l0_partition(pathlib.PurePosixPath(rel))
        assert route.venue == "krx"
        assert route.session is MarketSession.REGULAR
        assert route.stream == stream


def test_raw_venue_preserved() -> None:
    from src.realtime.contracts import MarketSession
    from src.storage.layout import route_for_l0_partition

    route = route_for_l0_partition(pathlib.PurePosixPath("ls/zzz/nxt_after/S/dt=2026-09-18"))
    assert route.venue == "zzz"
    assert route.session is MarketSession.NXT_AFTER


def _legacy_route(part: pathlib.PurePath) -> tuple[str, str, str]:
    stream_name = part.parent.name
    session_name = part.parent.parent.name
    is_routed = session_name in {"regular", "krx_after", "nxt_after", "nxt_pre"}
    legacy_venue = "krx" if not is_routed else part.parent.parent.parent.name
    legacy_session = "regular" if not is_routed else session_name
    return (legacy_venue, legacy_session, stream_name)


def _legacy_l1_out(
    journal_root: pathlib.Path, archive_base: pathlib.Path, part: pathlib.Path
) -> pathlib.Path:
    rel_parent = part.relative_to(journal_root).parent
    return archive_base / rel_parent / f"{part.name}.parquet"


def _legacy_rel(archive_base: pathlib.Path, out_path: pathlib.Path) -> str:
    return "l1/" + out_path.relative_to(archive_base).as_posix()


def _legacy_l0_for_l1(repo_path: str) -> str | None:
    import re

    pattern = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})\.parquet$")
    if not repo_path.startswith("l1/"):
        return None
    rest = repo_path[len("l1/") :]
    if rest == "snapshot" or rest.startswith("snapshot/"):
        return None
    if "/" not in rest:
        return None
    parent, _, name = rest.rpartition("/")
    if not parent:
        return None
    match = pattern.match(name)
    if match is None:
        return None
    try:
        dt.date.fromisoformat(match.group(1))
    except ValueError:
        return None
    return f"l0/{parent}/{name[: -len('.parquet')]}"


_L0_CASES = (
    "kis/krx/regular/H0STASP0/dt=2026-09-18",
    "kis/nxt/nxt_after/H0NXCNT0/dt=2026-09-18",
    "ls/unknown/krx_after/H0STACN0/dt=2026-09-18",
    "ls/H0STASP0/dt=2026-09-18",
    "kis/krx/regular/dt=2026-09-18",
    "dt=2026-09-18",
    "foo/bar/dt=2026-09-18",
)

_L1_CASES = (
    "l1/kis/krx/regular/H0STASP0/dt=2026-09-18.parquet",
    "l1/ls/H0STASP0/dt=2026-09-18.parquet",
    "l1/snapshot/ranking/dt=2026-09-18.parquet",
    "l1/kis/bad.parquet",
    "l1//dt=2026-09-18.parquet",
    "l1/kis/krx/regular/S/dt=2026-13-45.parquet",
    "l1/kis/krx/regular/S/dt=2026-09-18.parquet",
    "l0/kis/krx/regular/S/dt=2026-09-18",
    "manifests/a.json",
)


def test_characterization_tree_matches_legacy_oracles(tmp_path) -> None:
    from src.storage.layout import (
        l0_partition_for_l1,
        l1_relpath_for_l0_partition,
        l1_repo_path,
        route_for_l0_partition,
    )

    journal_root = tmp_path / "j"
    archive_base = tmp_path / "l1"
    for rel in _L0_CASES:
        part = journal_root / rel
        route = route_for_l0_partition(part)
        legacy_venue, legacy_session, legacy_stream = _legacy_route(part)
        assert (route.venue, route.session.value, route.stream) == (
            legacy_venue,
            legacy_session,
            legacy_stream,
        )
        assert (archive_base / l1_relpath_for_l0_partition(part.relative_to(journal_root))).as_posix() == _legacy_l1_out(
            journal_root, archive_base, part
        ).as_posix()
        out_path = archive_base / l1_relpath_for_l0_partition(part.relative_to(journal_root))
        assert l1_repo_path(out_path.relative_to(archive_base)) == _legacy_rel(archive_base, out_path)
    for repo_path in _L1_CASES:
        assert l0_partition_for_l1(repo_path) == _legacy_l0_for_l1(repo_path)


def test_empty_stream_collapse() -> None:
    from src.storage.layout import L0PartitionKey, l0_partition_relpath

    key = L0PartitionKey(
        vendor="kis",
        venue=MarketVenue.KRX,
        session=MarketSession.REGULAR,
        stream="",
        day=DAY,
    )
    assert l0_partition_relpath(key).as_posix() == "kis/krx/regular/dt=2026-09-18"


def test_key_from_strings_fails_closed() -> None:
    from src.storage.layout import l0_partition_key

    assert l0_partition_key("kis", "bogus", "regular", "S", DAY) is None
    assert l0_partition_key("kis", "krx", "after", "S", DAY) is None
    key = l0_partition_key("kis", "krx", "regular", "S", DAY)
    assert key is not None
    assert isinstance(key.venue, MarketVenue)
    assert isinstance(key.session, MarketSession)


def test_journal_file_naming() -> None:
    import fnmatch

    from src.storage.layout import l0_journal_file_glob, l0_journal_file_name

    assert l0_journal_file_name(9, None) == "09.jsonl.zst"
    assert l0_journal_file_name(9, "s1") == "09.s1.jsonl.zst"
    untagged = l0_journal_file_glob(None)
    assert fnmatch.fnmatch("09.jsonl.zst", untagged)
    assert not fnmatch.fnmatch("09.s1.jsonl.zst", untagged)
    tagged = l0_journal_file_glob("s1")
    assert fnmatch.fnmatch("09.s1.jsonl.zst", tagged)
    assert not fnmatch.fnmatch("09.s0.jsonl.zst", tagged)


def test_pure_module_no_io(tmp_path) -> None:
    import ast

    from src.storage.layout import (
        L0PartitionKey,
        l0_journal_file_glob,
        l0_journal_file_name,
        l0_partition_key,
        l0_partition_relpath,
        l1_relpath_for_l0_partition,
        l1_repo_path,
        route_for_l0_partition,
    )

    tree = ast.parse((pathlib.Path("src/storage/layout.py")).read_text(encoding="utf-8"))
    allowed_stdlib = {"dataclasses", "datetime", "pathlib", "re", "typing"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] in allowed_stdlib
        elif isinstance(node, ast.ImportFrom):
            assert node.module in allowed_stdlib | {"__future__", "src.realtime.contracts"}
    ghost = tmp_path / "ghost"
    key = L0PartitionKey(
        vendor="kis",
        venue=MarketVenue.KRX,
        session=MarketSession.REGULAR,
        stream="S",
        day=DAY,
    )
    assert l0_partition_relpath(key).as_posix().startswith("kis/")
    assert route_for_l0_partition(ghost / "kis/krx/regular/S/dt=2026-09-18").stream == "S"
    assert l1_relpath_for_l0_partition(pathlib.PurePosixPath("a/b/dt=2026-09-18")).as_posix().endswith(
        ".parquet"
    )
    assert l1_repo_path(pathlib.PurePosixPath("a/dt=2026-09-18.parquet")).startswith("l1/")
    assert l0_partition_key("kis", "nope", "regular", "S", DAY) is None
    assert l0_journal_file_name(1, None) == "01.jsonl.zst"
    assert l0_journal_file_glob(None) == "[0-9][0-9].jsonl.zst"
    assert not ghost.exists()


def _expired_tree(journal_root: pathlib.Path, archive_root: pathlib.Path, day: str) -> dict[str, pathlib.Path]:
    parts = {
        "routed": journal_root / "kis/krx/regular/H0STASP0" / f"dt={day}",
        "tagged": journal_root / "kis/krx/regular/H0SHARD" / f"dt={day}",
        "legacy": journal_root / "ls/H0STASP0" / f"dt={day}",
        "empty": journal_root / "kis/krx/regular" / f"dt={day}",
        "root": journal_root / f"dt={day}",
    }
    for name, part in parts.items():
        part.mkdir(parents=True, exist_ok=True)
        if name == "tagged":
            (part / "09.s1.jsonl.zst").write_bytes(b"x")
            (part / "10.s1.jsonl.zst").write_bytes(b"y")
        else:
            (part / "09.jsonl.zst").write_bytes(b"x")
    return parts


def _oracle_rels(journal_root: pathlib.Path, archive_base: pathlib.Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in (p for p in journal_root.rglob("dt=*") if p.is_dir()):
        rel_parent = part.relative_to(journal_root).parent
        out_path = archive_base / rel_parent / f"{part.name}.parquet"
        out[str(part)] = "l1/" + out_path.relative_to(archive_base).as_posix()
    return out


def test_deletion_gate_parity_no_normalize(tmp_path) -> None:
    from src.storage.retention import prune_old_journals

    journal_root = tmp_path / "l0"
    archive_root = tmp_path / "l1"
    parts = _expired_tree(journal_root, archive_root, "2026-09-01")
    oracle = _oracle_rels(journal_root, archive_root)
    for part in parts.values():
        rel_parent = part.relative_to(journal_root).parent
        out_path = archive_root / rel_parent / f"{part.name}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"pq")
    names = sorted(parts)
    verified = {oracle[str(parts[n])] for n in names[:2]}

    stats = prune_old_journals(
        journal_root,
        archive_root=archive_root,
        retain_days=3,
        reference_date=dt.date(2026, 9, 10),
        verified_remote_l1=verified,
        normalize=False,
    )

    for n in names[:2]:
        assert not parts[n].exists()
    for n in names[2:]:
        assert (parts[n] / "09.jsonl.zst").exists() or n == "tagged"
    if "tagged" in names[2:]:
        assert (parts["tagged"] / "09.s1.jsonl.zst").exists()
    assert stats.deleted == 2
    assert stats.failed == 0


def test_deletion_gate_parity_normalize(tmp_path) -> None:
    from src.storage.retention import prune_old_journals

    journal_root = tmp_path / "l0"
    archive_root = tmp_path / "l1"
    parts = _expired_tree(journal_root, archive_root, "2026-09-01")
    recorded: list[tuple[str, str]] = []

    def _stub(part: pathlib.Path, out_path: pathlib.Path) -> int:
        recorded.append((part.as_posix(), out_path.as_posix()))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"pq")
        return 3

    names = sorted(parts)
    stats = prune_old_journals(
        journal_root,
        archive_root=archive_root,
        retain_days=3,
        reference_date=dt.date(2026, 9, 10),
        normalizer=_stub,
        verified_remote_l1=set(),
        normalize=True,
    )

    expected = {
        _legacy_l1_out(journal_root, archive_root, part).as_posix() for part in parts.values()
    }
    assert {out for _, out in recorded} == expected
    assert stats.normalized == len(parts)
    assert stats.deleted == 0
    for part in parts.values():
        assert part.exists()


def test_local_l1_prune_parity(tmp_path) -> None:
    from src.storage.retention import prune_local_l1

    archive_root = tmp_path / "l1"
    files = [
        archive_root / "kis/krx/regular/S/dt=2026-07-01.parquet",
        archive_root / "kis/S/dt=2026-07-01.parquet",
        archive_root / "snapshot/ranking/dt=2026-07-01.parquet",
    ]
    for path in files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"pq")
    confirmed = {
        "l1/" + files[0].relative_to(archive_root).as_posix(),
        "l1/" + files[2].relative_to(archive_root).as_posix(),
    }

    purged = prune_local_l1(
        archive_root,
        retain_days=30,
        reference_date=dt.date(2026, 9, 8),
        confirmed_remote=confirmed,
    )

    assert purged == 2
    assert not files[0].exists()
    assert files[1].exists()
    assert not files[2].exists()


def test_mapper_reexport_identity_and_repo_path_parity(tmp_path) -> None:
    import src.storage.layout as layout_mod
    import src.storage.remote as remote_mod

    assert remote_mod.l0_partition_for_l1 is layout_mod.l0_partition_for_l1
    archiver = remote_mod.GDriveArchiver(remote_name="g", remote_path="p", runner=lambda *a, **k: None)
    for rel in (
        "kis/krx/regular/S/dt=2026-09-18.parquet",
        "kis/S/dt=2026-09-18.parquet",
        "snapshot/ranking/dt=2026-09-18.parquet",
    ):
        local = tmp_path / rel
        assert archiver.repo_path_for(tmp_path, local) == f"l1/{rel}"


def test_offload_verified_set_parity(tmp_path) -> None:
    from src.orchestration.eod import run_eod_offload
    from src.storage.remote import SyncStats

    archive_root = tmp_path / "l1"
    rels = [
        "kis/krx/regular/S/dt=2026-09-18.parquet",
        "kis/S/dt=2026-09-18.parquet",
        "snapshot/ranking/dt=2026-09-18.parquet",
    ]
    for rel in rels:
        path = archive_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"12345678")

    class _Arc:
        def sync_l1_tree(self, root, *, progress=None):
            return SyncStats(uploaded=3)

        def sync_manifest_tree(self, root):
            return SyncStats()

        def remote_file_sizes(self, prefix):
            assert prefix == "l1/"
            return {f"l1/{rel}": (archive_root / rel).stat().st_size for rel in rels}

        def remote_files(self, prefix):
            return set()

    result = run_eod_offload(archive_root, archiver=_Arc())

    assert result.verified_remote_l1 == frozenset(f"l1/{rel}" for rel in rels)


def _manifest(path: pathlib.Path, day: dt.date) -> None:
    from src.realtime.manifest import SessionManifest

    path.parent.mkdir(parents=True, exist_ok=True)
    SessionManifest(session_date=day, clock_offset_ns=0, started_at_ns=0).save(path)


def test_reconciliation_uses_writer_layout(tmp_path) -> None:
    from zoneinfo import ZoneInfo

    from src.orchestration.eod import check_session_reconciliation
    from src.storage.journal import L0JournalWriter

    day = dt.date(2026, 9, 14)
    journal_root = tmp_path / "l0"
    wall_ns = int(
        dt.datetime(2026, 9, 14, 10, 0, tzinfo=ZoneInfo("Asia/Seoul")).timestamp() * 1_000_000_000
    )
    writer = L0JournalWriter(
        journal_root, "ls", venue="krx", session="regular", stream="H0STASP0", file_tag="s0"
    )
    writer.append(
        raw="a",
        recv_mono_ns=1,
        recv_wall_ns=wall_ns,
        conn_id="c1",
        conn_seq=1,
    )
    assert writer.flush() == 1
    assert writer.owned_files(wall_ns) == [writer.partition_path(wall_ns)]
    assert writer.partition_path(wall_ns).name == "10.s0.jsonl.zst"
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _manifest(manifest_path, day)

    assert (
        check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=journal_root,
            streams=("H0STASP0",),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )
        is True
    )


def test_reconciliation_unknown_route_fails_closed(tmp_path, caplog) -> None:
    import logging

    from src.orchestration.eod import check_session_reconciliation

    day = dt.date(2026, 9, 14)
    manifest_path = tmp_path / "manifest" / "2026-09-14.json"
    _manifest(manifest_path, day)

    with caplog.at_level(logging.CRITICAL):
        ok = check_session_reconciliation(
            manifest_path=manifest_path,
            date=day,
            journal_root=tmp_path / "l0",
            streams=("H0STASP0",),
            vendor="ls",
            venue="bogus",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
        )

    assert ok is False
    assert "journal_missing:H0STASP0" in caplog.text


def _write_l0_records(part: pathlib.Path) -> None:
    import json

    import zstandard as zstd

    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "a",
        "recv_mono_ns": 1,
        "recv_wall_ns": 100,
        "conn_id": "c1",
        "conn_seq": 1,
        "vendor": "kis",
        "tr_id": "H0STCNT0",
    }
    (part / "09.jsonl.zst").write_bytes(
        zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8"))
    )


def test_null_fill_parity(tmp_path) -> None:
    import polars as pl

    from src.storage.normalization import normalize_l0_partition

    journal_root = tmp_path / "l0"
    cases = {
        "legacy": (journal_root / "ls/H0STCNT0/dt=2026-09-01", "krx", "regular"),
        "routed": (journal_root / "kis/nxt/nxt_after/H0NXCNT0/dt=2026-09-01", "nxt", "nxt_after"),
        "unknown": (journal_root / "ls/krx/weird/S/dt=2026-09-01", "krx", "regular"),
    }
    for name, (part, venue, session) in cases.items():
        _write_l0_records(part)
        out_path = tmp_path / "l1" / f"{name}.parquet"
        assert normalize_l0_partition(part, out_path) == 1
        frame = pl.read_parquet(out_path)
        assert frame.height == 1
        assert frame["venue"].to_list() == [venue]
        assert frame["session"].to_list() == [session]


def test_day_glob_byte_identical() -> None:
    from src.storage.layout import l0_day_journal_glob

    assert l0_day_journal_glob("ls", dt.date(2026, 9, 14)) == "ls/**/dt=2026-09-14/*.jsonl.zst"


def test_quarantine_mapping() -> None:
    import pytest

    from src.storage.layout import quarantine_repo_path_for_l0

    assert (
        quarantine_repo_path_for_l0("l0/ls/H0STASP0/dt=2026-09-18/09.jsonl.zst")
        == "quarantine/ls/H0STASP0/dt=2026-09-18/09.jsonl.zst"
    )
    with pytest.raises(ValueError, match="does not start with"):
        quarantine_repo_path_for_l0("l1/x")


def test_deletion_gate_parity_all_shapes_deleted(tmp_path) -> None:
    from src.storage.retention import prune_old_journals

    journal_root = tmp_path / "l0"
    archive_root = tmp_path / "l1"
    parts = _expired_tree(journal_root, archive_root, "2026-09-01")
    oracle = _oracle_rels(journal_root, archive_root)
    assert oracle[str(parts["root"])] == "l1/dt=2026-09-01.parquet"
    for part in parts.values():
        rel_parent = part.relative_to(journal_root).parent
        out_path = archive_root / rel_parent / f"{part.name}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"pq")

    stats = prune_old_journals(
        journal_root,
        archive_root=archive_root,
        retain_days=3,
        reference_date=dt.date(2026, 9, 10),
        verified_remote_l1=set(oracle.values()),
        normalize=False,
    )

    for part in parts.values():
        assert not part.exists()
    assert stats.deleted == 6
    assert stats.failed == 0


def test_premarket_partition_key_is_valid() -> None:
    from src.storage.layout import l0_partition_key, l0_partition_relpath

    key = l0_partition_key("kis", "nxt", "nxt_pre", "H0NXCNT0", DAY)

    assert key is not None
    assert l0_partition_relpath(key).as_posix().endswith("kis/nxt/nxt_pre/H0NXCNT0/dt=2026-09-18")


def test_premarket_l1_path_mirrors_l0_shape() -> None:
    from src.storage.layout import (
        l0_partition_for_l1,
        l0_partition_key,
        l0_partition_relpath,
        l1_relpath_for_l0_partition,
        l1_repo_path,
    )

    key = l0_partition_key("kis", "nxt", "nxt_pre", "H0NXCNT0", DAY)
    assert key is not None
    l0_rel = l0_partition_relpath(key)
    l1_rel = l1_relpath_for_l0_partition(l0_rel)

    assert l1_rel.as_posix() == "kis/nxt/nxt_pre/H0NXCNT0/dt=2026-09-18.parquet"
    assert l1_repo_path(l1_rel) == "l1/kis/nxt/nxt_pre/H0NXCNT0/dt=2026-09-18.parquet"
    assert l0_partition_for_l1(l1_repo_path(l1_rel)) == f"l0/{l0_rel.as_posix()}"


def test_premarket_route_is_inferred_from_directory() -> None:
    from src.realtime.contracts import MarketSession
    from src.storage.layout import route_for_l0_partition

    route = route_for_l0_partition(pathlib.PurePosixPath("l0/kis/nxt/nxt_pre/H0NXASP0/dt=2026-09-18"))

    assert route.venue == "nxt"
    assert route.session is MarketSession.NXT_PRE
    assert route.stream == "H0NXASP0"
