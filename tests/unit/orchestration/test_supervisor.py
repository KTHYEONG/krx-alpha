def test_restart_circuit_breaker_allows_within_limit() -> None:
    from src.orchestration.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=3, window_s=60.0)

    assert breaker.allow_restart(now=0.0) is True
    breaker.record_restart(now=0.0)
    assert breaker.allow_restart(now=1.0) is True
    breaker.record_restart(now=1.0)
    assert breaker.allow_restart(now=2.0) is True
    breaker.record_restart(now=2.0)
    assert breaker.allow_restart(now=3.0) is False


def test_restart_circuit_breaker_prunes_old_restarts_outside_window() -> None:
    from src.orchestration.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=1, window_s=10.0)
    breaker.record_restart(now=0.0)

    assert breaker.allow_restart(now=5.0) is False
    assert breaker.allow_restart(now=11.0) is True


def test_process_supervisor_starts_process_when_not_running() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    class _Proc:
        def poll(self):
            self.checked = True
            return None

    started_cmds: list[list[str]] = []

    def _fake_popen(cmd):
        started_cmds.append(cmd)
        return _Proc()

    sup = ProcessSupervisor(cmd=["echo", "hi"], popen=_fake_popen)

    result = sup.ensure_running()

    assert result == "started"
    assert started_cmds == [["echo", "hi"]]
    assert sup.is_running() is True


def test_process_supervisor_returns_running_when_alive() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    class _Proc:
        def poll(self):
            self.checked = True
            return None

    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: _Proc())
    sup.ensure_running()

    result = sup.ensure_running()

    assert result == "running"


def test_process_supervisor_restarts_dead_process_and_records_breaker() -> None:
    from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker

    class _DeadProc:
        def poll(self):
            self.checked = True
            return 1

    class _AliveProc:
        def poll(self):
            self.checked = True
            return None

    procs = iter([_DeadProc(), _AliveProc()])
    spawned: list[list[str]] = []

    def _fake_popen(cmd):
        spawned.append(cmd)
        return next(procs)

    breaker = RestartCircuitBreaker(max_restarts=5, window_s=60.0)
    sup = ProcessSupervisor(cmd=["x"], popen=_fake_popen, breaker=breaker)

    first = sup.ensure_running()
    second = sup.ensure_running()

    assert first == "started"
    assert second == "restarted"
    assert len(spawned) == 2


def test_process_supervisor_returns_circuit_open_when_breaker_denies(monkeypatch) -> None:
    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker

    class _DeadProc:
        def poll(self):
            self.checked = True
            return 1

    def _fake_popen(cmd):
        return _DeadProc()

    breaker = RestartCircuitBreaker(max_restarts=1, window_s=60.0)
    sup = ProcessSupervisor(cmd=["x"], popen=_fake_popen, breaker=breaker)

    clock = {"t": 0.0}
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: clock["t"])
    breaker.record_restart(now=0.0)

    first = sup.ensure_running()
    second = sup.ensure_running()

    assert first == "started"
    assert second == "circuit_open"


def test_process_supervisor_stop_terminates_gracefully() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    class _Proc:
        def __init__(self):
            self.terminated = False
            self.killed = False

        def poll(self):
            self.checked = True
            return None

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

        def wait(self, timeout=None):
            self.waited = True
            return 0

    proc = _Proc()
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: proc)
    sup.ensure_running()

    result = sup.stop(timeout_s=5.0)

    assert result == "graceful"
    assert proc.terminated is True
    assert proc.killed is False


def test_process_supervisor_stop_kills_on_timeout() -> None:
    import subprocess
    from src.orchestration.supervisor import ProcessSupervisor

    class _Proc:
        def __init__(self):
            self.terminated = False
            self.killed = False
            self.wait_calls = 0

        def poll(self):
            self.checked = True
            return None

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True

        def wait(self, timeout=None):
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise subprocess.TimeoutExpired(cmd="x", timeout=timeout)
            return -9

    proc = _Proc()
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: proc)
    sup.ensure_running()

    result = sup.stop(timeout_s=0.01)

    assert result == "killed"
    assert proc.killed is True


def test_process_supervisor_stop_returns_not_running_when_no_process() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: None)

    result = sup.stop()

    assert result == "not_running"


def test_process_supervisor_records_last_exit_code_on_restart() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    class _Proc:
        def __init__(self, code):
            self.code = code

        def poll(self):
            return self.code

    procs = iter([_Proc(-9), _Proc(None)])
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: next(procs))

    assert sup.ensure_running() == "started"
    assert sup.last_exit_code is None
    assert sup.ensure_running() == "restarted"
    assert sup.last_exit_code == -9


def test_stop_supervisors_signals_all_before_waiting() -> None:
    from src.orchestration.supervisor import ProcessSupervisor, stop_supervisors

    order: list[str] = []

    class _Proc:
        def __init__(self, name: str) -> None:
            self.name = name

        def poll(self):
            return None

        def terminate(self):
            order.append(f"terminate-{self.name}")

        def wait(self, timeout=None):
            order.append(f"wait-{self.name}")
            return 0

        def kill(self):
            order.append(f"kill-{self.name}")

    sups = []
    for name in ("a", "b", "c"):
        proc = _Proc(name)
        sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd, _p=proc: _p)
        sup.ensure_running()
        sups.append(sup)

    counts = stop_supervisors(sups, deadline_s=5.0)

    assert counts == {"graceful": 3, "killed": 0, "not_running": 0}
    first_wait = min(order.index(f"wait-{n}") for n in ("a", "b", "c"))
    last_terminate = max(order.index(f"terminate-{n}") for n in ("a", "b", "c"))
    assert last_terminate < first_wait


def test_stop_supervisors_shared_deadline_kills_straggler(monkeypatch) -> None:
    import subprocess

    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, stop_supervisors

    class _QuickProc:
        def poll(self):
            return None

        def terminate(self):
            return None

        def wait(self, timeout=None):
            return 0

        def kill(self):
            raise AssertionError("quick child must not be killed")

    class _StuckProc:
        def __init__(self) -> None:
            self.calls = 0

        def poll(self):
            return None

        def terminate(self):
            return None

        def wait(self, timeout=None):
            self.calls += 1
            if timeout is not None:
                raise subprocess.TimeoutExpired(cmd="x", timeout=timeout)
            return -9

        def kill(self):
            return None

    now = {"t": 100.0}
    timeouts: list[float] = []
    real_wait = ProcessSupervisor.wait_stopped

    def _tracking_wait(self, timeout_s: float = 0.0):
        timeouts.append(timeout_s)
        out = real_wait(self, timeout_s)
        now["t"] += 1.0
        return out

    monkeypatch.setattr(ProcessSupervisor, "wait_stopped", _tracking_wait)
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: now["t"])

    sups = [
        ProcessSupervisor(cmd=["x"], popen=lambda cmd: _QuickProc()),
        ProcessSupervisor(cmd=["x"], popen=lambda cmd: _QuickProc()),
        ProcessSupervisor(cmd=["x"], popen=lambda cmd: _StuckProc()),
    ]
    for sup in sups:
        sup.ensure_running()

    counts = stop_supervisors(sups, deadline_s=5.0)

    assert counts == {"graceful": 2, "killed": 1, "not_running": 0}
    assert timeouts == [5.0, 4.0, 3.0]
    assert all(t <= 5.0 for t in timeouts)


def test_stop_supervisors_counts_not_running_without_signal() -> None:
    from src.orchestration.supervisor import ProcessSupervisor, stop_supervisors

    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: None)

    counts = stop_supervisors([sup], deadline_s=5.0)

    assert counts == {"graceful": 0, "killed": 0, "not_running": 1}


def test_wait_stopped_records_last_exit_code() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    class _Proc:
        def __init__(self) -> None:
            self.code = None

        def poll(self):
            return self.code

        def terminate(self):
            return None

        def wait(self, timeout=None):
            self.code = 3
            return 3

    proc = _Proc()
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: proc)
    sup.ensure_running()

    assert sup.wait_stopped(5.0) == "graceful"
    assert sup.last_exit_code == 3


def test_stop_supervisors_clamps_negative_remaining_to_immediate_kill(monkeypatch) -> None:
    import subprocess

    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, stop_supervisors

    class _StuckProc:
        def __init__(self) -> None:
            self.calls = 0

        def poll(self):
            return None

        def terminate(self):
            return None

        def wait(self, timeout=None):
            self.calls += 1
            if timeout is not None:
                raise subprocess.TimeoutExpired(cmd="x", timeout=timeout)
            return -9

        def kill(self):
            return None

    now = {"t": 50.0}
    timeouts: list[float] = []
    real_wait = ProcessSupervisor.wait_stopped

    def _tracking_wait(self, timeout_s: float = 0.0):
        timeouts.append(timeout_s)
        out = real_wait(self, timeout_s)
        now["t"] += 10.0
        return out

    monkeypatch.setattr(ProcessSupervisor, "wait_stopped", _tracking_wait)
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: now["t"])
    sups = [ProcessSupervisor(cmd=["x"], popen=lambda cmd: _StuckProc()) for _ in range(2)]
    for sup in sups:
        sup.ensure_running()

    counts = stop_supervisors(sups, deadline_s=5.0)

    assert counts == {"graceful": 0, "killed": 2, "not_running": 0}
    assert timeouts[0] == 5.0
    assert timeouts[1] == 0.0


def test_breaker_budget_spent_opens() -> None:
    from src.orchestration.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=5, window_s=1800.0)
    assert breaker.allow_probe(now=0.0) is False
    for i in range(5):
        assert breaker.allow_restart(now=float(i * 10)) is True
        breaker.record_restart(now=float(i * 10))
    assert breaker.allow_restart(now=50.0) is False
    assert breaker.is_open(now=50.0) is True


def test_breaker_probe_not_due_immediately(monkeypatch) -> None:
    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker

    class _DeadProc:
        def poll(self):
            return 1

    clock = {"t": 40.0}
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: clock["t"])
    spawns: list = []
    breaker = RestartCircuitBreaker(max_restarts=5, window_s=1800.0)
    for i in range(5):
        breaker.record_restart(now=float(i * 10))
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: spawns.append(cmd) or _DeadProc(), breaker=breaker)
    assert sup.ensure_running() == "started"
    clock["t"] = 40.0 + 59.0
    assert breaker.allow_probe(now=clock["t"]) is False
    assert sup.ensure_running() == "circuit_open"
    assert len(spawns) == 1


def test_breaker_probe_due_after_base_interval(monkeypatch) -> None:
    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker

    class _DeadProc:
        def poll(self):
            return 1

    clock = {"t": 40.0}
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: clock["t"])
    spawns: list = []
    breaker = RestartCircuitBreaker(max_restarts=5, window_s=1800.0)
    for i in range(5):
        breaker.record_restart(now=float(i * 10))
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: spawns.append(cmd) or _DeadProc(), breaker=breaker)
    assert sup.ensure_running() == "started"
    clock["t"] = 40.0 + 60.0
    assert breaker.allow_probe(now=clock["t"]) is True
    assert breaker.allow_restart(now=clock["t"]) is False
    assert sup.ensure_running() == "restarted"
    assert len(spawns) == 2


def test_breaker_probe_spacing_doubles_then_caps() -> None:
    from src.orchestration.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=5, window_s=100000.0)
    base = 0.0
    for i in range(5):
        breaker.record_restart(now=base + float(i))
    expected = [60.0, 120.0, 240.0, 300.0, 300.0]
    last = 4.0
    for spacing in expected:
        assert breaker.allow_probe(now=last + spacing - 1.0) is False
        assert breaker.allow_probe(now=last + spacing) is True
        breaker.record_restart(now=last + spacing)
        last = last + spacing


def test_breaker_transient_failure_recovers_within_one_probe(monkeypatch) -> None:
    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker

    clock = {"t": 0.0}
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: clock["t"])
    failing_until = 65.0

    def _popen(cmd):
        class _P:
            def __init__(self, dead: bool) -> None:
                self._dead = dead

            def poll(self):
                return 1 if self._dead else None

        return _P(clock["t"] < failing_until)

    breaker = RestartCircuitBreaker(max_restarts=5, window_s=1800.0)
    sup = ProcessSupervisor(cmd=["x"], popen=_popen, breaker=breaker)
    assert sup.ensure_running() == "started"
    resumed_at: float | None = None
    for step in range(1, 40):
        clock["t"] = float(step * 10)
        result = sup.ensure_running()
        if clock["t"] >= 50.0 and result == "restarted" and clock["t"] >= failing_until:
            resumed_at = clock["t"]
            break
    assert resumed_at is not None
    assert resumed_at <= 50.0 + 60.0 + 10.0


def test_breaker_healthy_child_drains_window() -> None:
    from src.orchestration.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=5, window_s=1800.0)
    for i in range(5):
        breaker.record_restart(now=float(i * 10))
    assert breaker.is_open(now=50.0) is True
    assert breaker.is_open(now=40.0 + 1800.0 + 1.0) is False
    assert breaker.allow_restart(now=40.0 + 1800.0 + 1.0) is True


def test_supervisor_alert_latch_once_per_episode(monkeypatch) -> None:
    import src.orchestration.supervisor as sup_mod
    from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker

    clock = {"t": 0.0}
    monkeypatch.setattr(sup_mod.time, "monotonic", lambda: clock["t"])
    breaker = RestartCircuitBreaker(max_restarts=1, window_s=1800.0)
    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: type("P", (), {"poll": lambda self: 1})(), breaker=breaker)
    assert sup.take_circuit_alert() is False
    assert sup.ensure_running() == "started"
    clock["t"] = 1.0
    assert sup.ensure_running() == "restarted"
    clock["t"] = 2.0
    assert sup.ensure_running() == "circuit_open"
    assert sup.take_circuit_alert() is True
    assert sup.take_circuit_alert() is False
    clock["t"] = 61.0
    assert sup.ensure_running() == "restarted"
    clock["t"] = 62.0
    assert sup.ensure_running() == "circuit_open"
    assert sup.take_circuit_alert() is False
    clock["t"] = 1900.0
    assert sup.ensure_running() == "restarted"
    clock["t"] = 1901.0
    assert sup.ensure_running() == "circuit_open"
    clock["t"] = 1902.0
    assert sup.ensure_running() == "circuit_open"
    assert sup.take_circuit_alert() is True


def test_supervisor_take_alert_always_false_without_breaker() -> None:
    from src.orchestration.supervisor import ProcessSupervisor

    sup = ProcessSupervisor(cmd=["x"], popen=lambda cmd: None)
    assert sup.take_circuit_alert() is False


def test_breaker_constructor_validation() -> None:
    import pytest

    from src.orchestration.supervisor import RestartCircuitBreaker

    with pytest.raises(ValueError, match="max_restarts"):
        RestartCircuitBreaker(max_restarts=0)
    with pytest.raises(ValueError, match="window_s"):
        RestartCircuitBreaker(window_s=0.0)
    with pytest.raises(ValueError, match="probe_base_interval_s"):
        RestartCircuitBreaker(probe_base_interval_s=0.0)
    with pytest.raises(ValueError, match="probe_max_interval_s"):
        RestartCircuitBreaker(probe_max_interval_s=0.0)
    with pytest.raises(ValueError, match="probe_max_interval_s"):
        RestartCircuitBreaker(probe_base_interval_s=300.0, probe_max_interval_s=60.0)
