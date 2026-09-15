def test_compose_requires_shared_data_env_file_after_project_env() -> None:
    from pathlib import Path

    compose = Path('docker-compose.yml').read_text(encoding='utf-8')
    assert '- .env' in compose
    assert '- ${KIS_DATA_ENV_FILE:?KIS_DATA_ENV_FILE is required}' in compose
    assert compose.index('- .env') < compose.index('- ${KIS_DATA_ENV_FILE:?KIS_DATA_ENV_FILE is required}')
    assert '${KIS_SHARED_TOKEN_CACHE_DIR}:/run/kis-token-cache:ro' in compose


def test_keypool_runbook_limits_shared_fragment_to_data_keys() -> None:
    from pathlib import Path

    runbook = Path('docs/architecture/kis-aftermarket-keypool-deployment.md').read_text(encoding='utf-8')
    assert 'KIS_DATA_SLOTS=1,2,3,4,5' in runbook
    assert 'KIS_HOST_DATA_SLOTS=1,2,3,4' in runbook
    assert 'KIS_TRADE_*' in runbook
    assert 'KIS_APP_*' in runbook
    assert 'KIS_DATA_ENV_CONTENT' in runbook


def test_deploy_workflow_provisions_shared_env_and_wires_kca() -> None:
    from pathlib import Path

    workflow = Path('.github/workflows/deploy.yml').read_text(encoding='utf-8')
    assert 'KIS_DATA_ENV_CONTENT: ${{ secrets.KIS_DATA_ENV_CONTENT }}' in workflow
    assert 'KIS_DATA_ENV_FILE' in workflow
    assert 'KIS_SHARED_TOKEN_CACHE_DIR' in workflow
    assert 'kca-kis-token-warmup.service.d' in workflow
    assert 'non-data or account field' in workflow
    assert 'HOST_DATA_SLOTS=1,2,3,4' in workflow
    assert 'canonical_file' in workflow
    assert r'\$1 !~ /^KIS_' in workflow
    assert 'systemctl --user daemon-reload' in workflow
    assert 'kca-kis-token-warmup.timer' in workflow
    assert 'systemctl --user restart kca-kis-token-warmup.service' in workflow
