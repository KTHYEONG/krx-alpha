def test_keypool_runbook_limits_shared_fragment_to_data_keys() -> None:
    from pathlib import Path

    runbook = Path('docs/architecture/kis-aftermarket-keypool-deployment.md').read_text(encoding='utf-8')
    assert 'KIS_DATA_SLOTS=1,2,3,4,5' in runbook
    assert 'KIS_HOST_DATA_SLOTS=1,2,3,4' in runbook
    assert 'KIS_TRADE_*' in runbook
    assert 'KIS_APP_*' in runbook
    assert 'uv run python -m src.cli.provision_kis_keypool' in runbook
    assert '/home/ubuntu/quant-secrets/kis-data.env' in runbook


def test_deploy_workflow_validates_shared_env_and_wires_kca() -> None:
    from pathlib import Path

    workflow = Path('.github/workflows/deploy.yml').read_text(encoding='utf-8')
    assert 'KIS_DATA_ENV_CONTENT' not in workflow
    assert 'kca-kis-token-warmup.service.d' in workflow
    assert 'validate_shared_keypool' in workflow
    assert 'HOST_DATA_SLOTS=1,2,3,4' in workflow
    assert 'systemctl --user daemon-reload' in workflow
    assert 'kca-kis-token-warmup.timer' in workflow
    assert 'systemctl --user restart kca-kis-token-warmup.service' not in workflow


def test_deploy_workflow_validates_existing_vps_keypool_without_github_secret() -> None:
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert "KIS_DATA_ENV_CONTENT" not in workflow
    assert "KIS_DATA_ENV_B64" not in workflow
    assert "validate_shared_keypool()" in workflow
    assert "/home/ubuntu/quant-secrets/kis-data.env" in workflow
    assert "systemctl --user restart kca-kis-token-warmup.service" not in workflow


def test_dockerfile_keeps_uv_cache_out_of_image_and_runs_venv_python() -> None:
    from pathlib import Path

    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")

    assert dockerfile.splitlines()[0].startswith("# syntax=docker/dockerfile:1")
    assert "UV_CACHE_DIR=/root/.cache/uv" in dockerfile
    assert dockerfile.count("--mount=type=cache,target=/root/.cache/uv,sharing=locked") == 2
    assert "libgomp1" in dockerfile
    assert 'CMD ["/app/.venv/bin/python", "-m", "src.orchestration.daemon"]' in dockerfile
    assert '"uv", "run"' not in dockerfile


def test_compose_enables_aftermarket_with_verified_kis_pair_capacity() -> None:
    from pathlib import Path

    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert "KRX_ALPHA_AFTER_MARKET_ENABLED=true" in compose
    # KIS 웹소켓 커넥션당 41스트림쌍 하드캡: docs/architecture/design-decisions.md,
    # overview.md, tests/unit/realtime/test_kis_sharding.py 가 모두 이 값을 검증치로 쓴다.
    assert "KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION=41" in compose


def test_compose_uses_absolute_secret_paths_without_shell_interpolation() -> None:
    from pathlib import Path

    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert "- /home/ubuntu/quant-secrets/krx-alpha.env" in compose
    assert "- /home/ubuntu/quant-secrets/kis-data.env" in compose
    assert compose.index("/home/ubuntu/quant-secrets/krx-alpha.env") < compose.index(
        "/home/ubuntu/quant-secrets/kis-data.env"
    )
    assert "${" not in compose
    assert "- .env" not in compose
    assert "- /home/ubuntu/.cache/kis:/run/kis-token-cache:ro" in compose
    assert "- /home/ubuntu/.config/rclone:/root/.config/rclone:ro" in compose
    assert "KRX_ALPHA_KIS_TOKEN_ALLOW_ISSUE=false" in compose
    assert "KRX_ALPHA_KIS_TOKEN_CACHE_DIR=/run/kis-token-cache" in compose
    assert '["/app/.venv/bin/python", "-m", "src.orchestration.daemon"]' in compose


def test_deploy_workflow_builds_native_arm64_and_tags_commit_sha() -> None:
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert "runs-on: ubuntu-24.04-arm" in workflow
    assert "platforms: linux/arm64" in workflow
    assert "linux/amd64" not in workflow
    assert "setup-qemu-action" not in workflow
    assert "sha-${{ github.sha }}" in workflow
    assert "${{ env.IMAGE }}:latest" in workflow


def test_deploy_workflow_validates_runtime_env_without_mutating_remote_env_file() -> None:
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert "upsert_env" not in workflow
    assert "touch .env" not in workflow
    assert "KIS_DATA_ENV_CONTENT" not in workflow
    assert "validate_runtime_env" in workflow
    assert "validate_shared_keypool" in workflow
    assert "/home/ubuntu/quant-secrets/krx-alpha.env" in workflow
    assert "/home/ubuntu/quant-secrets/kis-data.env" in workflow
    assert "kca-kis-token-warmup.service.d" in workflow
    assert "kca-kis-token-warmup.timer" in workflow
    assert "provision_kis_keypool" in workflow


def test_layer_rank_registers_runtime_env_provisioning() -> None:
    from tests.architecture.layers import LAYER_RANK

    assert LAYER_RANK["src/core/runtime_env_provisioning.py"] == 0
    assert LAYER_RANK["src/core/kis_keypool_provisioning.py"] == 0
    assert LAYER_RANK["src/cli/provision_kis_keypool.py"] == 7


def test_compose_enables_snapshot_collection() -> None:
    from pathlib import Path

    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert "KRX_ALPHA_SNAPSHOT_ENABLED=true" in compose


def test_deploy_installs_host_backup_and_retires_shared_timer_only_when_krx_only() -> None:
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert "deploy/host/krx-host-backup.sh" in workflow
    assert "deploy/host/krx-alpha.rclone-filter" in workflow
    assert "deploy/host/krx-host-backup.service" in workflow
    assert "deploy/host/krx-host-backup.timer" in workflow
    assert "enable --now krx-host-backup.timer" in workflow
    assert "disable --now quant-lake-backup.timer" in workflow


def _run_retire_guard(tmp_path, projects: list[str]) -> tuple[bool, bool]:
    """Execute the workflow's retirement guard as the remote shell would and report (retired, timer_disabled)."""
    import os
    import subprocess
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")
    block = workflow.split("# QUANT_LAKE_RETIRE_GUARD_BEGIN", 1)[1].split("# QUANT_LAKE_RETIRE_GUARD_END", 1)[0]
    # 비인용 heredoc이 러너에서 \$ -> $ 로 풀린 뒤 원격에서 실행되는 형태를 재현한다
    remote_script = block.replace("\\$", "$")
    home = tmp_path / "home"
    units = home / ".config" / "systemd" / "user"
    units.mkdir(parents=True)
    (units / "quant-lake-backup.timer").write_text("[Timer]\n", encoding="utf-8")
    (units / "quant-lake-backup.service").write_text("[Service]\n", encoding="utf-8")
    (home / "bin").mkdir()
    entries = "\n".join(f'  [{name}]="$HOME/{name}/data"' for name in projects)
    (home / "bin" / "quant-lake-backup.sh").write_text(
        "declare -A PROJECTS=(\n" + entries + "\n)\n"
        "declare -A EXTRA_ARGS=()\n"
        'EXTRA_ARGS[crypto-pilot]="--filter-from x"\n',
        encoding="utf-8",
    )
    fakebin = tmp_path / "fakebin"
    fakebin.mkdir()
    record = tmp_path / "systemctl.log"
    stub = fakebin / "systemctl"
    stub.write_text(f'#!/bin/sh\necho "$@" >> {record}\n', encoding="utf-8")
    stub.chmod(0o755)
    env = {**os.environ, "HOME": str(home), "PATH": f"{fakebin}:{os.environ['PATH']}"}
    subprocess.run(["bash", "-ec", remote_script], env=env, check=True, capture_output=True, text=True)  # noqa: S603, S607 - hermetic guard replay
    retired = not (home / "bin" / "quant-lake-backup.sh").exists() and any(
        f.name.startswith("quant-lake-backup.sh.retired.") for f in (home / "bin").iterdir()
    )
    disabled = record.exists() and "disable --now quant-lake-backup.timer" in record.read_text(encoding="utf-8")
    return retired, disabled


def test_retire_guard_keeps_shared_timer_while_other_projects_remain(tmp_path) -> None:
    retired, disabled = _run_retire_guard(tmp_path, ["mt-etf-king-2026", "krx-alpha", "crypto-pilot"])
    assert (retired, disabled) == (False, False)


def test_retire_guard_retires_shared_timer_when_krx_only(tmp_path) -> None:
    retired, disabled = _run_retire_guard(tmp_path, ["krx-alpha"])
    assert (retired, disabled) == (True, True)


def test_timer_keeps_nightly_kst_slot() -> None:
    from pathlib import Path

    timer = Path("deploy/host/krx-host-backup.timer").read_text(encoding="utf-8")

    assert "OnCalendar=*-*-* 23:30:00 Asia/Seoul" in timer
    assert "Persistent=true" in timer


def test_deferred_timer_matches_gate_constant() -> None:
    from pathlib import Path

    from src.orchestration.deploy_gate import DEFERRED_RECREATE_KST

    timer = Path("deploy/host/krx-deferred-recreate.timer").read_text(encoding="utf-8")

    assert "OnCalendar=Mon..Fri 22:00:00 Asia/Seoul" in timer
    assert "Persistent=false" in timer
    assert f"{DEFERRED_RECREATE_KST.hour:02d}:{DEFERRED_RECREATE_KST.minute:02d}" in timer


def test_deferred_service_never_force_recreates() -> None:
    from pathlib import Path

    service = Path("deploy/host/krx-deferred-recreate.service").read_text(encoding="utf-8")

    assert "Type=oneshot" in service
    assert "WorkingDirectory=%h/krx-alpha" in service
    assert "docker compose up -d" in service
    assert "--force-recreate" not in service
    assert "OnFailure=kca-alert@%n.service" in service
    assert "TimeoutStartSec=10min" in service


def test_deploy_workflow_gates_recreate_behind_session_gate() -> None:
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")

    assert "python3 -m src.orchestration.deploy_gate" in workflow
    assert workflow.index("deploy_gate") < workflow.index("up -d --force-recreate")
    assert "RECREATE_NOW" in workflow
    assert "\\$C pull" in workflow
    pull_line = next(line for line in workflow.splitlines() if "\\$C pull" in line)
    assert "RECREATE_NOW" not in pull_line
    assert "krx-deferred-recreate.service" in workflow
    assert "krx-deferred-recreate.timer" in workflow
    assert "enable --now krx-deferred-recreate.timer" in workflow


def test_deferred_slot_precedes_nightly_backup_and_follows_eod() -> None:
    import datetime as dt

    from src.core.calendar import SessionSchedule
    from src.orchestration.deploy_gate import DEFERRED_RECREATE_KST

    backup_slot = dt.time(23, 30)
    assert SessionSchedule().after_market_eod_done < DEFERRED_RECREATE_KST < backup_slot
