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
