
def test_restart_circuit_breaker_allows_within_limit() -> None:
    from src.collector.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=3, window_s=60.0)

    assert breaker.allow_restart(now=0.0) is True
    breaker.record_restart(now=0.0)
    assert breaker.allow_restart(now=1.0) is True
    breaker.record_restart(now=1.0)
    assert breaker.allow_restart(now=2.0) is True
    breaker.record_restart(now=2.0)
    assert breaker.allow_restart(now=3.0) is False

def test_restart_circuit_breaker_prunes_old_restarts_outside_window() -> None:
    from src.collector.supervisor import RestartCircuitBreaker

    breaker = RestartCircuitBreaker(max_restarts=1, window_s=10.0)
    breaker.record_restart(now=0.0)

    assert breaker.allow_restart(now=5.0) is False
    assert breaker.allow_restart(now=11.0) is True

def test_process_supervisor_starts_process_when_not_running() -> None:
    from src.collector.supervisor import ProcessSupervisor

    class _Proc:
        def poll(self):
            self.checked = True
            return None

    started_cmds: list[list[str]] = []

    def _fake_popen(cmd):
        started_cmds.append(cmd)
        return _Proc()

    sup = ProcessSupervisor(cmd=['echo', 'hi'], popen=_fake_popen)

    result = sup.ensure_running()

    assert result == 'started'
    assert started_cmds == [['echo', 'hi']]
    assert sup.is_running() is True

def test_process_supervisor_returns_running_when_alive() -> None:
    from src.collector.supervisor import ProcessSupervisor

    class _Proc:
        def poll(self):
            self.checked = True
            return None

    sup = ProcessSupervisor(cmd=['x'], popen=lambda cmd: _Proc())
    sup.ensure_running()

    result = sup.ensure_running()

    assert result == 'running'

def test_process_supervisor_restarts_dead_process_and_records_breaker() -> None:
    from src.collector.supervisor import ProcessSupervisor, RestartCircuitBreaker

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
    sup = ProcessSupervisor(cmd=['x'], popen=_fake_popen, breaker=breaker)

    first = sup.ensure_running()
    second = sup.ensure_running()

    assert first == 'started'
    assert second == 'restarted'
    assert len(spawned) == 2

def test_process_supervisor_returns_circuit_open_when_breaker_denies() -> None:
    from src.collector.supervisor import ProcessSupervisor, RestartCircuitBreaker

    class _DeadProc:
        def poll(self):
            self.checked = True
            return 1

    def _fake_popen(cmd):
        return _DeadProc()

    breaker = RestartCircuitBreaker(max_restarts=0, window_s=60.0)
    sup = ProcessSupervisor(cmd=['x'], popen=_fake_popen, breaker=breaker)

    first = sup.ensure_running()
    second = sup.ensure_running()

    assert first == 'started'
    assert second == 'circuit_open'

def test_process_supervisor_stop_terminates_gracefully() -> None:
    from src.collector.supervisor import ProcessSupervisor

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
    sup = ProcessSupervisor(cmd=['x'], popen=lambda cmd: proc)
    sup.ensure_running()

    result = sup.stop(timeout_s=5.0)

    assert result == 'graceful'
    assert proc.terminated is True
    assert proc.killed is False

def test_process_supervisor_stop_kills_on_timeout() -> None:
    import subprocess
    from src.collector.supervisor import ProcessSupervisor

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
                raise subprocess.TimeoutExpired(cmd='x', timeout=timeout)
            return -9

    proc = _Proc()
    sup = ProcessSupervisor(cmd=['x'], popen=lambda cmd: proc)
    sup.ensure_running()

    result = sup.stop(timeout_s=0.01)

    assert result == 'killed'
    assert proc.killed is True

def test_process_supervisor_stop_returns_not_running_when_no_process() -> None:
    from src.collector.supervisor import ProcessSupervisor

    sup = ProcessSupervisor(cmd=['x'], popen=lambda cmd: None)

    result = sup.stop()

    assert result == 'not_running'
