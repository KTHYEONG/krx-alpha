def test_build_shared_fragment_orders_canonical_keys_and_excludes_accounts(tmp_path) -> None:
    from src.core.kis_keypool_provisioning import build_shared_fragment

    source = tmp_path / ".quant.env"
    lines = ["export KIS_DATA_1_ACCOUNT_NO=account", "export KIS_TRADE_APP_KEY=trade"]
    for slot in range(1, 6):
        lines.extend(
            [
                f"export KIS_DATA_{slot}_APP_KEY=key-{slot}",
                f'KIS_DATA_{slot}_APP_SECRET="secret-{slot}"',
                f"export KIS_DATA_{slot}_HTS_ID=hts-{slot}",
            ]
        )
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    fragment = build_shared_fragment(source)

    expected_names = ["KIS_DATA_SLOTS", "KIS_HOST_DATA_SLOTS"]
    for slot in range(1, 6):
        expected_names.extend(
            [f"KIS_DATA_{slot}_APP_KEY", f"KIS_DATA_{slot}_APP_SECRET", f"KIS_DATA_{slot}_HTS_ID"]
        )
    assert [line.partition("=")[0] for line in fragment.splitlines()] == expected_names
    assert fragment.startswith("KIS_DATA_SLOTS=1,2,3,4,5\nKIS_HOST_DATA_SLOTS=1,2,3,4\n")
    assert "ACCOUNT_NO" not in fragment
    assert "KIS_TRADE_" not in fragment
    assert "secret-1" in fragment

def test_build_shared_fragment_rejects_missing_credential(tmp_path) -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.core.kis_keypool_provisioning import build_shared_fragment

    source = tmp_path / ".quant.env"
    lines: list[str] = []
    for slot in range(1, 6):
        lines.extend([f"KIS_DATA_{slot}_APP_KEY=key-{slot}", f"KIS_DATA_{slot}_HTS_ID=hts-{slot}"])
        if slot != 1:
            lines.append(f"KIS_DATA_{slot}_APP_SECRET=secret-{slot}")
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(KrxAlphaError, match="KIS_DATA_1_APP_SECRET"):
        build_shared_fragment(source)

def test_build_shared_fragment_rejects_duplicate_credential(tmp_path) -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.core.kis_keypool_provisioning import build_shared_fragment

    source = tmp_path / ".quant.env"
    lines: list[str] = []
    for slot in range(1, 6):
        lines.extend(
            [
                f"KIS_DATA_{slot}_APP_KEY=key-{slot}",
                f"KIS_DATA_{slot}_APP_SECRET=secret-{slot}",
                f"KIS_DATA_{slot}_HTS_ID=hts-{slot}",
            ]
        )
    lines.append("KIS_DATA_1_APP_SECRET=duplicate")
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(KrxAlphaError, match=r"duplicate.*KIS_DATA_1_APP_SECRET"):
        build_shared_fragment(source)

def test_install_shared_fragment_sends_credentials_only_on_ssh_stdin(monkeypatch) -> None:
    import subprocess

    import src.core.kis_keypool_provisioning as provisioning

    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(provisioning.subprocess, "run", fake_run)
    fragment = "KIS_DATA_1_APP_SECRET=secret-value\n"

    provisioning.install_shared_fragment("or-vps", fragment)

    args, kwargs = calls[0]
    assert args[:2] == ["ssh", "or-vps"]
    assert kwargs["input"] == fragment
    assert "secret-value" not in " ".join(args)
    assert "mktemp" in args[-1]
    assert "mv -f" in args[-1]

def test_main_dry_run_validates_without_installing_or_logging_secret(monkeypatch, caplog, tmp_path) -> None:
    import src.cli.provision_kis_keypool as cli

    source = tmp_path / ".quant.env"
    source.write_text("placeholder\n", encoding="utf-8")
    monkeypatch.setattr(cli, "build_shared_fragment", lambda path: "KIS_DATA_1_APP_SECRET=secret-value\n")
    monkeypatch.setattr(cli, "build_runtime_fragment", lambda path: "KRX_OPENAPI_KEY=key\n")

    def fail_install(host: str, fragment: str) -> None:
        raise AssertionError("dry-run must not install")

    monkeypatch.setattr(cli, "install_shared_fragment", fail_install)
    monkeypatch.setattr(cli, "install_runtime_fragment", fail_install)

    assert cli.main(["--source", str(source), "--dry-run"]) == 0
    assert "secret-value" not in caplog.text


def test_main_installs_when_not_dry_run(monkeypatch, caplog, tmp_path) -> None:
    import src.cli.provision_kis_keypool as cli

    source = tmp_path / ".quant.env"
    source.write_text("placeholder\n", encoding="utf-8")
    monkeypatch.setattr(cli, "build_shared_fragment", lambda path: "KIS_DATA_1_APP_SECRET=secret-value\n")
    monkeypatch.setattr(cli, "build_runtime_fragment", lambda path: "KRX_OPENAPI_KEY=key\n")

    installed: list[tuple[str, str]] = []

    def fake_install(host: str, fragment: str) -> None:
        installed.append((host, fragment))

    monkeypatch.setattr(cli, "install_shared_fragment", fake_install)
    monkeypatch.setattr(cli, "install_runtime_fragment", lambda host, fragment: None)

    assert cli.main(["--source", str(source), "--host", "or-vps"]) == 0

    assert installed == [("or-vps", "KIS_DATA_1_APP_SECRET=secret-value\n")]
    assert "secret-value" not in caplog.text


def test_parse_workstation_assignments_normalizes_and_rejects_duplicates(tmp_path) -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.core.kis_keypool_provisioning import parse_workstation_assignments

    accepted = frozenset({"ALPHA", "BETA", "GAMMA", "DELTA"})
    source = tmp_path / ".quant.env"
    source.write_text(
        "\n".join(
            [
                "# comment",
                "",
                "no_assignment_line",
                "export ALPHA=one",
                '  BETA = "two"  ',
                "GAMMA='three'",
                "DELTA=",
                "OUT_OF_SCOPE=first",
                "OUT_OF_SCOPE=second",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    parsed = parse_workstation_assignments(source, accepted)

    assert parsed == {"ALPHA": "one", "BETA": "two", "GAMMA": "three"}

    duplicate = tmp_path / "dup.env"
    duplicate.write_text("ALPHA=one\nexport ALPHA=two\n", encoding="utf-8")

    with pytest.raises(KrxAlphaError, match="ALPHA"):
        parse_workstation_assignments(duplicate, accepted)
