def test_resolve_collector_runtime_uses_one_enabled_flag_and_data_root(tmp_path, monkeypatch) -> None:
    import pytest

    from src.core.config import AftermarketSettings, CollectorSettings, SnapshotSettings, resolve_collector_runtime

    monkeypatch.delenv("KRX_ALPHA_AFTERMARKET_ENABLED", raising=False)
    collector = CollectorSettings(data_root=tmp_path, after_market_enabled=False)
    snapshot = SnapshotSettings(enabled=False)

    default = resolve_collector_runtime(collector=collector, snapshot=snapshot)
    assert default.aftermarket.enabled is False
    assert default.paths.root == tmp_path

    explicit = AftermarketSettings(enabled=True, pair_capacity_per_connection=4)
    with pytest.raises(ValueError, match="aftermarket enabled conflicts"):
        resolve_collector_runtime(collector=collector, aftermarket=explicit, snapshot=snapshot)

    matching = resolve_collector_runtime(
        collector=collector, aftermarket=AftermarketSettings(enabled=False), snapshot=snapshot
    )
    assert matching.aftermarket.enabled is False



def test_deploy_workflow_validates_shared_env_and_wires_kca() -> None:
    from pathlib import Path

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")
    assert "KIS_DATA_ENV_CONTENT" not in workflow
    assert "kca-kis-token-warmup.service.d" in workflow
    assert "validate_shared_keypool" in workflow
    assert "HOST_DATA_SLOTS=1,2,3,4" in workflow
    assert "systemctl --user daemon-reload" in workflow
    assert "kca-kis-token-warmup.timer" in workflow
    assert "systemctl --user restart kca-kis-token-warmup.service" not in workflow


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
    # KIS 웹소켓 커넥션당 41스트림쌍 하드캡: docs/architecture/engineering-decisions.md,
    # system-design.md, tests/unit/realtime/test_kis_sharding.py 가 모두 이 값을 검증치로 쓴다.
    assert "KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION=41" in compose


def test_compose_pins_snapshot_rest_to_data_slot_not_shared_with_kca_decision() -> None:
    from pathlib import Path

    import yaml

    from src.core.config import SnapshotSettings

    raw = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    env = dict(item.split("=", 1) for item in raw["services"]["krx-collector"]["environment"])

    # k-closing-alpha가 결정 역할(DATA_1)과 결정창 샤드(DATA_5)로 15:20~15:35를 점유한다.
    assert env["KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"] == "2"
    assert env["KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"] not in {"1", "5"}
    assert SnapshotSettings(kis_data_slot=env["KRX_ALPHA_SNAPSHOT_KIS_DATA_SLOT"]).kis_data_slot == "2"


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
    assert "- /home/ubuntu/.cache/kis:/run/kis-token-cache" in compose
    assert ":/run/kis-token-cache:ro" not in compose
    assert "- /home/ubuntu/.config/rclone:/root/.config/rclone:ro" in compose
    assert "KRX_ALPHA_KIS_TOKEN_ALLOW_ISSUE=true" in compose
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


def _workflow_function_body(workflow: str, name: str) -> str:
    """Extract a shell validator body from the deploy workflow without executing it."""
    start = workflow.index(f"{name}() {{")
    end = workflow.index("\n          }\n", start)
    return workflow[start:end]


def _shell_word_list(body: str, loop_head: str) -> list[str]:
    """Collect the word list of a `for <var> in ... ; do` loop as literal tokens."""
    section = body.split(loop_head, 1)[1].split("; do", 1)[0]
    return [token for token in section.replace("\\", " ").split() if token]


def test_runtime_fragment_keys_match_ci_allowlist_without_values() -> None:
    import re
    from pathlib import Path

    from src.core.runtime_env_provisioning import RUNTIME_ENV_SPEC

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")
    body = _workflow_function_body(workflow, "validate_runtime_env")
    targets = [key.target for key in RUNTIME_ENV_SPEC]

    allowlist = re.search(r"\$1 !~ /\^\(([^)]+)\)\$/", body)
    assert allowlist is not None
    assert sorted(allowlist.group(1).split("|")) == sorted(targets)

    required = _shell_word_list(body, "for required_key in")
    assert sorted(required) == sorted(targets)
    assert len(set(required)) == len(targets)

    echo_lines = [line for line in body.splitlines() if "echo" in line]
    assert echo_lines
    assert not any("credential_value" in line for line in echo_lines)
    assert 'cat "$file"' not in body
    assert "cat $file" not in body


def test_keypool_slot_keys_match_ci_validation_exactly_once() -> None:
    import re
    from pathlib import Path

    from src.core.kis_keypool_provisioning import (
        ACCEPTED_KEYS,
        CANONICAL_HOST_SLOTS_LINE,
        CANONICAL_SLOTS_LINE,
    )

    workflow = Path(".github/workflows/deploy.yml").read_text(encoding="utf-8")
    body = _workflow_function_body(workflow, "validate_shared_keypool")

    required = _shell_word_list(body, "for required_key in")
    assert sorted(required) == sorted(ACCEPTED_KEYS)
    for key in ACCEPTED_KEYS:
        assert body.count(key) == 1

    slot_pattern = re.compile(r"KIS_DATA_[1-5]_(APP_KEY|APP_SECRET|HTS_ID)")
    assert all(slot_pattern.fullmatch(key) for key in ACCEPTED_KEYS)
    assert f'expected_slots_line="{CANONICAL_SLOTS_LINE}"' in body
    assert f'expected_host_slots_line="{CANONICAL_HOST_SLOTS_LINE}"' in body
    assert len(ACCEPTED_KEYS) + 2 == 17

    echo_lines = [line for line in body.splitlines() if "echo" in line]
    assert not any("credential_value" in line for line in echo_lines)


def _accepted_typed_env_names() -> set[str]:
    from src.core import config as config_module

    families = [
        config_module.CollectorSettings,
        config_module.AftermarketSettings,
        config_module.SnapshotSettings,
        config_module.ExecutionSettings,
        config_module.DataQualitySettings,
        config_module.KisTokenSettings,
        config_module.RcloneArchiveSettings,
        config_module.ObservabilitySettings,
        config_module.AlertSettings,
        config_module.TossProgramTradesSettings,
        config_module.KrxCredentials,
        config_module.TossCredentials,
        config_module.LsCredentials,
        config_module.KisCredentials,
    ]
    names: set[str] = set()
    for family in families:
        prefix = str(family.model_config.get("env_prefix") or "").upper()
        for field_name, field in family.model_fields.items():
            names.add(f"{prefix}{field_name}".upper())
            alias = field.validation_alias
            choices = alias.choices if hasattr(alias, "choices") else [alias]
            for choice in choices:
                if isinstance(choice, str):
                    names.add(choice.upper())
    return names


def test_compose_overrides_are_all_recognized_typed_settings() -> None:
    from pathlib import Path

    import yaml

    raw = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    entries = raw["services"]["krx-collector"]["environment"]
    overrides = sorted(item.split("=", 1)[0] for item in entries if item.startswith("KRX_ALPHA_"))

    assert overrides
    accepted = _accepted_typed_env_names()
    assert [name for name in overrides if name not in accepted] == []


def test_host_and_container_remote_roots_share_one_data_subtree() -> None:
    import re
    from pathlib import Path

    from src.core.config import CollectorSettings, RcloneArchiveSettings

    script = Path("deploy/host/krx-host-backup.sh").read_text(encoding="utf-8")
    archiver = RcloneArchiveSettings()

    host_default = re.search(r'REMOTE_ROOT="\$\{REMOTE_ROOT:-([^}]+)\}"', script)
    assert host_default is not None
    host_root = host_default.group(1)
    assert f"{archiver.remote_name}:{archiver.remote_path}" == f"{host_root}/data"
    assert host_root.endswith("/krx-alpha")

    assert "--filter-from" in script
    assert "krx-alpha.rclone-filter" in script
    assert '--exclude ".env*"' in script
    assert "flock -w" in script
    assert "QUANT_GDRIVE_LOCK" in script
    assert "VERSION_RETENTION_DAYS" in script
    assert CollectorSettings().archive_retain_days == 30


def test_host_backup_unit_retries_with_direct_restart_mode() -> None:
    from pathlib import Path

    unit = Path("deploy/host/krx-host-backup.service").read_text(encoding="utf-8")

    assert "Restart=on-failure" in unit
    assert "RestartMode=direct" in unit
    assert "RestartSec=30min" in unit
    assert "StartLimitBurst=3" in unit
    assert "StartLimitIntervalSec=10h" in unit
    assert "OnFailure=kca-alert@%n.service" in unit
    assert "Type=oneshot" in unit
    assert "TimeoutStartSec=4h" in unit
