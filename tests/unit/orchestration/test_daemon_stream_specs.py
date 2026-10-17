"""Stream child spec table and generic install/supervise/stop invariant tests."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import pathlib
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, resolve_collector_runtime

_KST = ZoneInfo("Asia/Seoul")
_GOLDEN_PATH = pathlib.Path(__file__).parent / "golden" / "daemon_step_timeline.json"


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def _runner(tmp_path, monkeypatch, **settings_kw):
    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", **settings_kw)
    runtime = resolve_collector_runtime(collector=settings)
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def _session_kwargs(**overrides):
    kwargs = {
        "key": daemon_mod.StreamChildKey.SNAPSHOT,
        "role": daemon_mod.StreamRole.SNAPSHOT,
        "lifecycle": daemon_mod.ChildLifecycle.SESSION,
        "enabled": lambda runtime: True,
        "cmd": lambda today, paths, reason: ["cmd"],
        "active": lambda runtime, now, anchors: True,
        "logs": daemon_mod.SupervisionLogs(
            restarted="r",
            circuit_open="c",
            circuit_level=logging.CRITICAL,
        ),
    }
    kwargs.update(overrides)
    return kwargs


def test_spec_table_order_and_roles() -> None:
    keys = [spec.key for spec in daemon_mod.DaemonRunner._STREAM_SPECS]
    assert keys == [
        daemon_mod.StreamChildKey.REGULAR,
        daemon_mod.StreamChildKey.SNAPSHOT,
        daemon_mod.StreamChildKey.DB_CROSSCHECK,
        daemon_mod.StreamChildKey.AFTERMARKET,
        daemon_mod.StreamChildKey.PREMARKET,
    ]
    primaries = [s for s in daemon_mod.DaemonRunner._STREAM_SPECS if s.role is daemon_mod.StreamRole.PRIMARY]
    assert len(primaries) == 1
    sessions = [
        s.key for s in daemon_mod.DaemonRunner._STREAM_SPECS if s.lifecycle is daemon_mod.ChildLifecycle.SESSION
    ]
    assert sessions == [
        daemon_mod.StreamChildKey.REGULAR,
        daemon_mod.StreamChildKey.SNAPSHOT,
        daemon_mod.StreamChildKey.DB_CROSSCHECK,
    ]


def test_stream_spec_missing_key_raises() -> None:
    with pytest.raises(KeyError, match="nope"):
        daemon_mod.DaemonRunner._stream_spec("nope")  # type: ignore[arg-type]


def test_session_spec_requires_callables() -> None:
    with pytest.raises(ValueError, match="requires cmd, active and logs"):
        daemon_mod.StreamChildSpec(**_session_kwargs(cmd=None))
    with pytest.raises(ValueError, match="must not define cmd, active or logs"):
        daemon_mod.StreamChildSpec(
            key=daemon_mod.StreamChildKey.AFTERMARKET,
            role=daemon_mod.StreamRole.AFTERMARKET,
            lifecycle=daemon_mod.ChildLifecycle.SHARDED,
            enabled=lambda runtime: True,
            logs=daemon_mod.SupervisionLogs(
                restarted="r", circuit_open="c", circuit_level=logging.CRITICAL
            ),
        )


def test_crosscheck_spec_cannot_page() -> None:
    with pytest.raises(ValueError, match="must not alert above WARNING"):
        daemon_mod.StreamChildSpec(
            **_session_kwargs(
                key=daemon_mod.StreamChildKey.DB_CROSSCHECK,
                role=daemon_mod.StreamRole.CROSSCHECK,
                logs=daemon_mod.SupervisionLogs(
                    restarted="r", circuit_open="c", circuit_level=logging.CRITICAL
                ),
                requires_primary=True,
            )
        )


def test_crosscheck_spec_requires_primary_flag() -> None:
    with pytest.raises(ValueError, match="requires a primary child"):
        daemon_mod.StreamChildSpec(
            **_session_kwargs(
                key=daemon_mod.StreamChildKey.DB_CROSSCHECK,
                role=daemon_mod.StreamRole.CROSSCHECK,
                logs=daemon_mod.SupervisionLogs(
                    restarted="r", circuit_open="c", circuit_level=logging.WARNING
                ),
            )
        )


@pytest.mark.parametrize("lifecycle", [daemon_mod.ChildLifecycle.SHARDED, daemon_mod.ChildLifecycle.WINDOWED])
def test_crosscheck_without_session_logs_raises_value_error(lifecycle) -> None:
    with pytest.raises(ValueError, match="CROSSCHECK child db_crosscheck requires session supervision logs"):
        daemon_mod.StreamChildSpec(
            key=daemon_mod.StreamChildKey.DB_CROSSCHECK,
            role=daemon_mod.StreamRole.CROSSCHECK,
            lifecycle=lifecycle,
            enabled=lambda runtime: True,
        )


def test_degraded_start_uses_primary_spec_command(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    day = dt.date(2026, 9, 14)
    calls: list = []

    def command(today, paths, reason):
        calls.append((today, paths, reason))
        return ["custom-primary", reason]

    primary = dataclasses.replace(
        runner._stream_spec(daemon_mod.StreamChildKey.REGULAR), cmd=command
    )
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_STREAM_SPECS", (primary,))
    monkeypatch.setattr(daemon_mod, "_degraded_candidates_rev", lambda *args, **kwargs: 20260914)
    runner._handle_orchestration_failure(dt.datetime(2026, 9, 14, 8, 25, tzinfo=_KST), day)
    assert calls == [(day, runner._paths, "orchestration_failed")]
    assert runner.children.regular_is_degraded
    assert runner.state.degraded_active


def test_started_log_only_for_primary() -> None:
    with pytest.raises(ValueError, match="only PRIMARY children may log a started event"):
        daemon_mod.StreamChildSpec(
            **_session_kwargs(
                logs=daemon_mod.SupervisionLogs(
                    started="s", restarted="r", circuit_open="c", circuit_level=logging.CRITICAL
                )
            )
        )


def test_primary_only_flags_rejected_on_non_primary() -> None:
    with pytest.raises(ValueError, match="must not set primary-only supervision flags"):
        daemon_mod.StreamChildSpec(
            key=daemon_mod.StreamChildKey.PREMARKET,
            role=daemon_mod.StreamRole.PREMARKET,
            lifecycle=daemon_mod.ChildLifecycle.WINDOWED,
            enabled=lambda runtime: True,
            starts_in_degraded=True,
        )


def test_daemon_state_fields_frozen() -> None:
    golden = json.loads(_GOLDEN_PATH.read_text(encoding="utf-8"))
    names = {f.name for f in dataclasses.fields(daemon_mod.DaemonState)}
    assert names == set(golden["steps"][0]["state"].keys())
    assert "_CHILD_RESULT_FIELDS" not in names


def test_child_result_routes_to_named_fields() -> None:
    state = daemon_mod.DaemonState()
    state.set_child_result(daemon_mod.StreamChildKey.DB_CROSSCHECK, "running")
    assert state.last_db_crosscheck_result == "running"
    assert state.child_result(daemon_mod.StreamChildKey.DB_CROSSCHECK) == "running"
    with pytest.raises(KeyError, match="PREMARKET"):
        state.child_result(daemon_mod.StreamChildKey.PREMARKET)


def test_children_properties_alias_keyed_store() -> None:
    children = daemon_mod.DaemonChildren()
    sup = object()
    children.snapshot = sup
    assert children.get(daemon_mod.StreamChildKey.SNAPSHOT) is sup
    assert children.snapshot is sup
    children.snapshot = None
    assert children.get(daemon_mod.StreamChildKey.SNAPSHOT) is None
    assert daemon_mod.StreamChildKey.SNAPSHOT not in children.streams
    for key, attribute in (
        (daemon_mod.StreamChildKey.REGULAR, "regular"),
        (daemon_mod.StreamChildKey.DB_CROSSCHECK, "db_crosscheck"),
        (daemon_mod.StreamChildKey.PREMARKET, "premarket"),
    ):
        setattr(children, attribute, sup)
        assert getattr(children, attribute) is sup
        assert children.get(key) is sup
    with pytest.raises(ValueError, match="aftermarket shards live in the aftermarket mapping"):
        children.set(daemon_mod.StreamChildKey.AFTERMARKET, sup)  # type: ignore[arg-type]


def test_stop_absent_child_is_pure_noop(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    runner.state.degraded_active = True
    assert runner._stop_stream_child(daemon_mod.StreamChildKey.REGULAR) is None
    assert runner.state.degraded_active is True


def test_install_stream_child_rejects_non_session_and_degraded(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="cannot install a non-session child"):
        runner._install_stream_child(daemon_mod.StreamChildKey.AFTERMARKET, ["cmd"])
    with pytest.raises(ValueError, match="only the PRIMARY child may be installed degraded"):
        runner._install_stream_child(daemon_mod.StreamChildKey.SNAPSHOT, ["cmd"], degraded=True)


def test_stop_stream_child_rejects_aftermarket(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="aftermarket shards are stopped"):
        runner._stop_stream_child(daemon_mod.StreamChildKey.AFTERMARKET)


def test_crosscheck_supervise_requires_primary_and_window(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    day = dt.date(2026, 9, 14)
    calls: list[str] = []

    class _Sup:
        last_exit_code = None

        def ensure_running(self) -> str:
            calls.append("ensure")
            return "running"

    runner.children.db_crosscheck = _Sup()  # type: ignore[assignment]
    runner.children.regular = None
    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.DB_CROSSCHECK, dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST), _anchors(day)
    )
    assert calls == []
    runner.children.regular = object()  # type: ignore[assignment]
    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.DB_CROSSCHECK, dt.datetime(2026, 9, 14, 16, 0, tzinfo=_KST), _anchors(day)
    )
    assert calls == []
    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.DB_CROSSCHECK, dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST), _anchors(day)
    )
    assert calls == ["ensure"]
    assert runner.state.last_db_crosscheck_result == "running"


def test_supervise_started_logs_event(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch)

    class _Sup:
        last_exit_code = None

        def ensure_running(self) -> str:
            return "started"

    runner.children.regular = _Sup()  # type: ignore[assignment]
    now = dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._supervise_stream_child(daemon_mod.StreamChildKey.REGULAR, now, _anchors(now.date()))
    started = [r for r in caplog.records if "stage=streamer status=STARTED" in r.getMessage()]
    assert len(started) == 1
    assert getattr(started[0], "krx_event", False) is True
    assert runner.state.last_supervisor_result == "started"


def test_replace_constructs_after_stop(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch)
    calls: list[list] = []

    class _Old:
        def stop(self, *, timeout_s: float = 15.0) -> str:
            calls.append(["stop", timeout_s])
            return "graceful"

    class _New:
        def __init__(self, *, cmd, breaker=None):
            calls.append(["new", list(cmd)])

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _New)
    runner.children.snapshot = _Old()  # type: ignore[assignment]
    with caplog.at_level(logging.CRITICAL, logger=daemon_mod.logger.name):
        runner._install_stream_child(daemon_mod.StreamChildKey.SNAPSHOT, ["cmd"])
    assert calls == [["stop", 15.0], ["new", ["cmd"]]]
    live = [r for r in caplog.records if "REPLACE_LIVE" in r.getMessage()]
    assert len(live) == 1
    assert live[0].levelno == logging.CRITICAL
    assert "slot=snapshot" in live[0].getMessage()


def test_degraded_upgrade_silences_snapshot_only(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)

    class _Fake:
        def stop(self, *, timeout_s: float = 15.0) -> str:
            return "graceful"

    runner._install_stream_child(daemon_mod.StreamChildKey.REGULAR, ["cmd"], degraded=True)
    runner.children.snapshot = _Fake()  # type: ignore[assignment]
    runner.children.db_crosscheck = _Fake()  # type: ignore[assignment]
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._install_stream_child(daemon_mod.StreamChildKey.REGULAR, ["cmd"], replaces_degraded=True)
        runner._install_stream_child(daemon_mod.StreamChildKey.SNAPSHOT, ["cmd"], replaces_degraded=True)
        runner._install_stream_child(daemon_mod.StreamChildKey.DB_CROSSCHECK, ["cmd"], replaces_degraded=True)
    degraded = [r for r in caplog.records if "REPLACE_DEGRADED" in r.getMessage()]
    assert len(degraded) == 1
    assert degraded[0].levelno == logging.INFO
    assert getattr(degraded[0], "krx_event", False) is True
    assert not any("slot=snapshot" in r.getMessage() for r in caplog.records)
    crosscheck = [r for r in caplog.records if "slot=db_crosscheck" in r.getMessage()]
    assert len(crosscheck) == 1
    assert crosscheck[0].levelno == logging.CRITICAL


def test_stop_children_order_preserved(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)

    class _Obj:
        def __init__(self, name: str) -> None:
            self.name = name

    runner.children.regular = _Obj("regular")  # type: ignore[assignment]
    runner.children.aftermarket["shard0"] = _Obj("shard0")  # type: ignore[assignment]
    runner.children.aftermarket["shard1"] = _Obj("shard1")  # type: ignore[assignment]
    runner.children.snapshot = _Obj("snapshot")  # type: ignore[assignment]
    runner.children.db_crosscheck = _Obj("db_crosscheck")  # type: ignore[assignment]
    runner.children.premarket = _Obj("premarket")  # type: ignore[assignment]
    selected: list = []
    monkeypatch.setattr(
        daemon_mod,
        "stop_supervisors",
        lambda children, *, deadline_s: selected.extend(children) or {"graceful": 0, "killed": 0, "not_running": 0},
    )
    runner.stop_children()
    assert [obj.name for obj in selected] == [
        "regular",
        "shard0",
        "shard1",
        "snapshot",
        "db_crosscheck",
        "premarket",
    ]
