def test_build_runtime_fragment_emits_declared_keys_in_canonical_order(tmp_path) -> None:
    from src.core.runtime_env_provisioning import RUNTIME_ENV_SPEC, build_runtime_fragment

    source = tmp_path / ".quant.env"
    source.write_text(
        "\n".join(
            [
                "# comment line",
                "",
                "export KRX_OPENAPI_KEY=krx-key",
                'export TOSS_APP_KEY="toss-key"',
                "TOSS_APP_SECRET='toss-secret'",
                "export LS_APP_KEY=ls-key",
                "export LS_APP_SECRET=ls-secret",
                "export KIS_APP_KEY=kis-key",
                "export KIS_APP_SECRET=kis-secret",
                "export KIS_ACCOUNT_NO=12345678",
                "export KIS_ACCOUNT_PRODUCT_CODE=01",
                "export LIVE_ALERT_GMAIL_USER=alert@example.com",
                "export LIVE_ALERT_GMAIL_APP_PASSWORD=alert-pass",
                "export ALERT_GMAIL_TO=ops@example.com",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    fragment = build_runtime_fragment(source)

    lines = fragment.splitlines()
    assert [line.partition("=")[0] for line in lines] == [key.target for key in RUNTIME_ENV_SPEC]
    assert len(lines) == 12
    assert fragment.endswith("\n")
    assert "export " not in fragment
    assert "TOSS_APP_KEY=toss-key" in lines
    assert "TOSS_APP_SECRET=toss-secret" in lines
    assert "ALERT_GMAIL_USER=alert@example.com" in lines
    assert "ALERT_GMAIL_APP_PASSWORD=alert-pass" in lines
    assert "LIVE_ALERT_GMAIL_USER" not in fragment


def test_build_runtime_fragment_prefers_primary_source_over_alias(tmp_path) -> None:
    from src.core.runtime_env_provisioning import build_runtime_fragment

    source = tmp_path / ".quant.env"
    source.write_text(
        "\n".join(
            [
                "KRX_OPENAPI_KEY=krx-key",
                "TOSS_APP_KEY=toss-key",
                "TOSS_APP_SECRET=toss-secret",
                "LS_APP_KEY=ls-key",
                "LS_APP_SECRET=ls-secret",
                "KIS_APP_KEY=kis-key",
                "KIS_APP_SECRET=kis-secret",
                "KIS_ACCOUNT_NO=12345678",
                "KIS_ACCOUNT_PRODUCT_CODE=01",
                "ALERT_GMAIL_USER=primary@example.com",
                "LIVE_ALERT_GMAIL_USER=alias@example.com",
                "ALERT_GMAIL_APP_PASSWORD=primary-pass",
                "LIVE_ALERT_GMAIL_APP_PASSWORD=alias-pass",
                "ALERT_GMAIL_TO=ops@example.com",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    fragment = build_runtime_fragment(source)

    assert "ALERT_GMAIL_USER=primary@example.com" in fragment.splitlines()
    assert "ALERT_GMAIL_APP_PASSWORD=primary-pass" in fragment.splitlines()
    assert "alias@example.com" not in fragment
    assert "alias-pass" not in fragment


def test_build_runtime_fragment_rejects_missing_required_key(tmp_path) -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.core.runtime_env_provisioning import build_runtime_fragment

    source = tmp_path / ".quant.env"
    source.write_text(
        "\n".join(
            [
                "KRX_OPENAPI_KEY=krx-key",
                "TOSS_APP_KEY=toss-key",
                "TOSS_APP_SECRET=toss-secret",
                "LS_APP_KEY=ls-key",
                "LS_APP_SECRET=ls-secret",
                "KIS_APP_KEY=kis-key",
                "KIS_APP_SECRET=kis-secret",
                "KIS_ACCOUNT_NO=12345678",
                "KIS_ACCOUNT_PRODUCT_CODE=01",
                "ALERT_GMAIL_USER=alert@example.com",
                "ALERT_GMAIL_APP_PASSWORD=alert-pass",
                "ALERT_GMAIL_TO=",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(KrxAlphaError, match="ALERT_GMAIL_TO"):
        build_runtime_fragment(source)


def test_build_runtime_fragment_never_leaks_keypool_or_undeclared_keys(tmp_path) -> None:
    from src.core.runtime_env_provisioning import build_runtime_fragment

    source = tmp_path / ".quant.env"
    source.write_text(
        "\n".join(
            [
                "KRX_OPENAPI_KEY=krx-key",
                "TOSS_APP_KEY=toss-key",
                "TOSS_APP_SECRET=toss-secret",
                "LS_APP_KEY=ls-key",
                "LS_APP_SECRET=ls-secret",
                "KIS_APP_KEY=kis-key",
                "KIS_APP_SECRET=kis-secret",
                "KIS_ACCOUNT_NO=12345678",
                "KIS_ACCOUNT_PRODUCT_CODE=01",
                "ALERT_GMAIL_USER=alert@example.com",
                "ALERT_GMAIL_APP_PASSWORD=alert-pass",
                "ALERT_GMAIL_TO=ops@example.com",
                "KIS_DATA_1_APP_SECRET=pool-secret",
                "KIS_TRADE_APP_KEY=trade-key",
                "KIS_HTS_ID=hts-id",
                "BINANCE_SECRET_KEY=binance-secret",
                "KIWOM_SECRET_KEY=kiwoom-secret",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    fragment = build_runtime_fragment(source)

    for forbidden in (
        "KIS_DATA_1_APP_SECRET",
        "pool-secret",
        "KIS_TRADE_APP_KEY",
        "trade-key",
        "KIS_HTS_ID",
        "BINANCE_SECRET_KEY",
        "binance-secret",
        "KIWOM_SECRET_KEY",
        "kiwoom-secret",
    ):
        assert forbidden not in fragment


def test_install_runtime_fragment_sends_values_only_on_ssh_stdin(monkeypatch) -> None:
    import subprocess

    import src.core.runtime_env_provisioning as provisioning

    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(provisioning.subprocess, "run", fake_run)
    fragment = "KIS_APP_SECRET=secret-value\n"

    provisioning.install_runtime_fragment("or-vps", fragment)

    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[0] == "ssh"
    assert args[1] == "or-vps"
    assert args[2] == "bash"
    assert args[3] == "-c"
    assert kwargs["input"] == fragment
    assert kwargs["check"] is True
    assert kwargs["text"] is True
    assert "shell" not in kwargs
    assert all("secret-value" not in part for part in args)
    remote_script = args[4]
    assert provisioning.REMOTE_RUNTIME_ENV_PATH == "/home/ubuntu/quant-secrets/krx-alpha.env"
    assert provisioning.REMOTE_RUNTIME_ENV_PATH in remote_script
    assert "set -euo pipefail" in remote_script
    assert "chmod 0700" in remote_script
    assert "chmod 600" in remote_script
    assert "chown ubuntu:ubuntu" in remote_script
    assert "mv -f" in remote_script
