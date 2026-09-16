def test_provision_cli_installs_both_fragments_after_building_both(tmp_path, monkeypatch) -> None:
    import src.cli.provision_kis_keypool as cli

    events: list[str] = []

    monkeypatch.setattr(cli, "build_shared_fragment", lambda path: events.append("build_keypool") or "KIS_DATA_SLOTS=1,2,3,4,5\n")
    monkeypatch.setattr(cli, "install_shared_fragment", lambda host, fragment: events.append(f"install_keypool:{host}"))
    monkeypatch.setattr(cli, "build_runtime_fragment", lambda path: events.append("build_runtime") or "KIS_APP_KEY=key\n")
    monkeypatch.setattr(cli, "install_runtime_fragment", lambda host, fragment: events.append(f"install_runtime:{host}"))

    source = tmp_path / ".quant.env"
    source.write_text("KIS_APP_KEY=key\n", encoding="utf-8")

    exit_code = cli.main(["--host", "or-vps", "--source", str(source)])

    assert exit_code == 0
    assert events.index("build_keypool") < events.index("install_keypool:or-vps")
    assert events.index("build_runtime") < events.index("install_runtime:or-vps")
    assert events.index("build_runtime") < events.index("install_keypool:or-vps")
    assert events == [
        "build_keypool",
        "build_runtime",
        "install_keypool:or-vps",
        "install_runtime:or-vps",
    ]


def test_provision_cli_dry_run_installs_nothing(tmp_path, monkeypatch) -> None:
    import src.cli.provision_kis_keypool as cli

    events: list[str] = []

    monkeypatch.setattr(cli, "build_shared_fragment", lambda path: events.append("build_keypool") or "KIS_DATA_SLOTS=1,2,3,4,5\n")
    monkeypatch.setattr(cli, "build_runtime_fragment", lambda path: events.append("build_runtime") or "KIS_APP_KEY=key\n")

    def fail_install(host: str, fragment: str) -> None:
        raise AssertionError("dry-run must not install")

    monkeypatch.setattr(cli, "install_shared_fragment", fail_install)
    monkeypatch.setattr(cli, "install_runtime_fragment", fail_install)

    source = tmp_path / ".quant.env"
    source.write_text("KIS_APP_KEY=key\n", encoding="utf-8")

    exit_code = cli.main(["--host", "or-vps", "--source", str(source), "--dry-run"])

    assert exit_code == 0
    assert events == ["build_keypool", "build_runtime"]
