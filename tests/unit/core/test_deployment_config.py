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
