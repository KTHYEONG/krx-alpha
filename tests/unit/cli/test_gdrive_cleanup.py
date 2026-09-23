"""gdrive_cleanup plan unit tests."""

from __future__ import annotations


def _obj(path: str, size: int):
    from src.cli.gdrive_cleanup import RemoteObject

    return RemoteObject(path=path, size=size)


def test_verified_l1_supersedes_its_l0_partition() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    objects = [
        _obj("l1/ls/krx/regular/H0STASP0/dt=2026-09-18.parquet", 150),
        _obj("l0/ls/krx/regular/H0STASP0/dt=2026-09-18/08.jsonl.zst", 100),
        _obj("l0/ls/krx/regular/H0STASP0/dt=2026-09-18/09.jsonl.zst", 50),
    ]

    candidates, _kept = build_cleanup_plan(objects)

    assert len(candidates) == 1
    item = candidates[0]
    assert item.rule == "l0_superseded_by_l1"
    assert item.target == "l0/ls/krx/regular/H0STASP0/dt=2026-09-18"
    assert item.bytes == 150


def test_legacy_layout_is_handled() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    objects = [
        _obj("l1/ls/H0STASP0/dt=2026-09-11.parquet", 10),
        _obj("l0/ls/H0STASP0/dt=2026-09-11/08.jsonl.zst", 7),
    ]

    candidates, _kept = build_cleanup_plan(objects)

    assert any(item.target == "l0/ls/H0STASP0/dt=2026-09-11" for item in candidates)


def test_pending_l0_without_l1_is_kept() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    objects = [_obj("l0/ls/krx/regular/H0STASP0/dt=2026-09-22/08.jsonl.zst", 12)]

    candidates, kept = build_cleanup_plan(objects)

    assert candidates == []
    assert ("l0/ls/krx/regular/H0STASP0/dt=2026-09-22/08.jsonl.zst", "no_l1") in kept


def test_zero_size_l1_does_not_justify_deletion() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    objects = [
        _obj("l1/ls/H0STASP0/dt=2026-09-11.parquet", 0),
        _obj("l0/ls/H0STASP0/dt=2026-09-11/08.jsonl.zst", 7),
    ]

    candidates, kept = build_cleanup_plan(objects)

    assert candidates == []
    assert any(path == "l0/ls/H0STASP0/dt=2026-09-11/08.jsonl.zst" for path, _reason in kept)


def test_quarantine_duplicate_removes_l0_copy_only() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    l0_path = "l0/ls/H0STASP0/dt=2026-09-09/x.jsonl.zst"
    q_path = "quarantine/ls/H0STASP0/dt=2026-09-09/x.jsonl.zst"
    candidates, _kept = build_cleanup_plan([_obj(l0_path, 9), _obj(q_path, 9)])

    assert len(candidates) == 1
    assert candidates[0].rule == "l0_quarantine_duplicate"
    assert candidates[0].target == l0_path
    assert all(item.target != q_path for item in candidates)

    candidates2, kept2 = build_cleanup_plan([_obj(l0_path, 9), _obj(q_path, 8)])

    assert candidates2 == []
    assert any(path == l0_path for path, _reason in kept2)


def test_manifest_duplicate_requires_equal_manifests_copy() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    candidates, _kept = build_cleanup_plan(
        [_obj("manifest/2026-09-11.json", 5), _obj("manifests/2026-09-11.json", 5)]
    )

    assert len(candidates) == 1
    assert candidates[0].rule == "manifest_duplicate"

    candidates2, _kept2 = build_cleanup_plan(
        [_obj("manifest/2026-09-11.json", 5), _obj("manifests/2026-09-11.json", 6)]
    )

    assert candidates2 == []


def test_protected_trees_are_immune() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    objects = [
        _obj("l1/ls/H0STASP0/dt=2026-09-11.parquet", 10),
        _obj("manifests/2026-09-11.json", 5),
        _obj("quarantine/ls/H0STASP0/dt=2026-09-09/x.jsonl.zst", 9),
        _obj("bars/daily.parquet", 3),
    ]

    candidates, _kept = build_cleanup_plan(objects)

    assert all(
        not item.target.startswith(prefix)
        for item in candidates
        for prefix in ("l1/", "manifests/", "quarantine/", "bars/")
    )


def test_listing_failure_aborts_before_mutation(monkeypatch) -> None:
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod

    calls: list[list[str]] = []

    def _runner(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=1, stdout="", stderr="boom")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)

    assert cleanup_mod.main(["--apply"]) != 0
    assert all("purge" not in args and "deletefile" not in args for args in calls)


def test_dry_run_is_non_mutating(monkeypatch, capsys) -> None:
    import json
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod

    calls: list[list[str]] = []
    entries = [{"Path": "l1/ls/H0STASP0/dt=2026-09-11.parquet", "Size": 10, "IsDir": False}]

    def _runner(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout=json.dumps(entries), stderr="")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)

    assert cleanup_mod.main([]) == 0
    assert calls
    assert all(args[1] == "lsjson" for args in calls)
    out = capsys.readouterr().out
    assert "rule=l0_superseded_by_l1" in out


def test_apply_relists_and_purges_l0_dirs(monkeypatch) -> None:
    import json
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod

    calls: list[list[str]] = []
    listing = [
        {"Path": "l1/ls/H0STASP0/dt=2026-09-11.parquet", "Size": 10, "IsDir": False},
        {"Path": "l0/ls/H0STASP0/dt=2026-09-11/08.jsonl.zst", "Size": 7, "IsDir": False},
        {"Path": "work/tmp.json", "Size": 2, "IsDir": False},
    ]

    def _runner(args, **kwargs):
        calls.append(args)
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout=json.dumps(listing), stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)

    assert cleanup_mod.main(["--apply"]) == 0
    kinds = [args[1] for args in calls]
    assert kinds.count("lsjson") == 2
    assert any(args[1] == "purge" for args in calls)
    assert any(args[1] == "deletefile" for args in calls)
    assert sum(1 for args in calls if args[1] == "rmdirs") == 3


def test_apply_second_listing_failure_aborts_without_mutation(monkeypatch) -> None:
    import json
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod

    calls: list[list[str]] = []
    listing = [{"Path": "l1/ls/H0STASP0/dt=2026-09-11.parquet", "Size": 10, "IsDir": False}]
    state = {"n": 0}

    def _runner(args, **kwargs):
        calls.append(args)
        if args[1] == "lsjson":
            state["n"] += 1
            if state["n"] == 1:
                return SimpleNamespace(returncode=0, stdout=json.dumps(listing), stderr="")
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)

    assert cleanup_mod.main(["--apply"]) != 0
    assert all(args[1] not in ("purge", "deletefile") for args in calls)


def test_apply_mutation_failure_returns_nonzero(monkeypatch) -> None:
    import json
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod

    listing = [
        {"Path": "l1/ls/H0STASP0/dt=2026-09-11.parquet", "Size": 10, "IsDir": False},
        {"Path": "l0/ls/H0STASP0/dt=2026-09-11/08.jsonl.zst", "Size": 7, "IsDir": False},
    ]

    def _runner(args, **kwargs):
        if args[1] == "lsjson":
            return SimpleNamespace(returncode=0, stdout=json.dumps(listing), stderr="")
        if args[1] == "purge":
            return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)

    assert cleanup_mod.main(["--apply"]) == 1


def test_superseded_without_files_yields_no_candidate() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    candidates, kept = build_cleanup_plan([_obj("l1/ls/H0STASP0/dt=2026-09-11.parquet", 10)])

    assert candidates == []
    assert kept == []


def test_snapshot_l1_never_supersedes_l0() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    objects = [
        _obj("l1/snapshot/ranking/dt=2026-09-18.parquet", 20),
        _obj("l0/snapshot/ranking/dt=2026-09-18/08.jsonl.zst", 5),
    ]

    candidates, kept = build_cleanup_plan(objects)

    assert all(item.rule != "l0_superseded_by_l1" for item in candidates)


def test_manifest_without_counterpart_is_kept() -> None:
    from src.cli.gdrive_cleanup import build_cleanup_plan

    candidates, kept = build_cleanup_plan([_obj("manifest/2026-09-12.json", 5)])

    assert candidates == []
    assert ("manifest/2026-09-12.json", "no_manifests_match") in kept


def test_invalid_listing_json_aborts(monkeypatch) -> None:
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod

    def _runner(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="not json", stderr="")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)

    assert cleanup_mod.main([]) != 0


def test_apply_skips_targets_outside_data(monkeypatch) -> None:
    from types import SimpleNamespace

    import src.cli.gdrive_cleanup as cleanup_mod
    from src.cli.gdrive_cleanup import CleanupCandidate

    calls: list[list[str]] = []
    listing: list[dict[str, object]] = []

    def _runner(args, **kwargs):
        calls.append(args)
        if args[1] == "lsjson":
            import json

            return SimpleNamespace(returncode=0, stdout=json.dumps(listing), stderr="")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cleanup_mod.subprocess, "run", _runner)
    monkeypatch.setattr(
        cleanup_mod,
        "build_cleanup_plan",
        lambda _objs: (
            [CleanupCandidate(rule="work_transient", target="bars/evil.parquet", bytes=1, evidence="x")],
            [],
        ),
    )

    assert cleanup_mod.main(["--apply"]) == 0
    assert all(args[1] not in ("purge", "deletefile") for args in calls)
