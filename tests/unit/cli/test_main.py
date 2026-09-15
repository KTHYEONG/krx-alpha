"""CLI main dispatcher unit tests."""

from __future__ import annotations


def test_cli_main_registers_universe_plan_subcommand():
    # Given: 최상위 CLI 파서
    from src.cli.main import build_parser

    parser = build_parser()

    # When: universe-plan 서브커맨드를 인자와 함께 파싱
    args = parser.parse_args([
        "universe-plan",
        "--bars-path", "data/bars.parquet",
        "--decision-date", "2026-09-04",
        "--out-path", "data/universe.parquet",
    ])

    # Then: 서브커맨드가 등록되어 있고 핸들러가 바인딩된다
    assert args.command == "universe-plan"
    assert args.bars_path == "data/bars.parquet"
    assert args.decision_date == "2026-09-04"
    assert callable(args.handler)


def test_cli_main_registers_collect_status_subcommand():
    # Given: 최상위 파서
    from src.cli.main import build_parser

    parser = build_parser()

    # When
    args = parser.parse_args(["collect-status", "--manifest-path", "data/session.json"])

    # Then: 서브커맨드 등록 + 핸들러 바인딩
    assert args.command == "collect-status"
    assert args.manifest_path == "data/session.json"
    assert callable(args.handler)


def test_cli_main_registers_collect_init_subcommand():
    # Given: 최상위 파서
    from src.cli.main import build_parser

    parser = build_parser()

    # When
    args = parser.parse_args([
        "collect-init", "--session-date", "2026-09-08",
        "--journal-root", "data/l0", "--manifest-path", "data/session.json",
        "--candidates-path", "data/candidates.json",
    ])

    # Then
    assert args.command == "collect-init"
    assert args.session_date == "2026-09-08"
    assert callable(args.handler)


def test_bars_refresh_subcommand_registered() -> None:
    from src.cli.main import build_parser

    parser = build_parser()
    args = parser.parse_args(['bars-refresh', '--store-path', 'b.parquet',
                              '--market-map-path', 'm.json', '--ref-date', '2026-09-08'])

    assert args.command == 'bars-refresh'
    assert callable(args.handler)

def test_cli_main_registers_order_subcommand() -> None:
    # Given: 최상위 CLI 파서
    from src.cli.main import build_parser

    parser = build_parser()

    # When
    args = parser.parse_args(["order", "--symbol", "005930", "--side", "buy", "--qty", "3", "--price", "10000"])
    market = parser.parse_args(["order", "--symbol", "005930", "--side", "sell", "--qty", "1", "--type", "market"])

    # Then
    assert args.command == "order"
    assert (args.symbol, args.side, args.qty, args.type, args.price) == ("005930", "buy", 3, "limit", 10_000)
    assert callable(args.handler)
    assert (market.type, market.price) == ("market", None)


def test_cli_main_configures_logging_per_subcommand(tmp_path, monkeypatch) -> None:
    import json

    import src.cli.main as main_mod

    calls: list[dict[str, object]] = []

    def fake_configure(component, *, log_dir=None, level="INFO"):
        calls.append({"component": component, "log_dir": log_dir, "level": level})
        return "r"

    monkeypatch.setattr(main_mod, "configure_logging", fake_configure)
    monkeypatch.delenv("KRX_ALPHA_PERSISTENT_LOGS", raising=False)
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps({"session_date": "2026-09-14", "clock_offset_ns": 0, "started_at_ns": 0, "subscription_acks": [], "gaps": []}), encoding="utf-8")

    code = main_mod.main(["--log-level", "DEBUG", "collect-status", "--manifest-path", str(manifest)])

    assert code == 0
    assert calls == [{"component": "cli-collect-status", "log_dir": None, "level": "DEBUG"}]


def test_main_registers_collect_aftermarket_command():
    from src.cli.main import build_parser
    parser = build_parser()
    action = next(a for a in parser._actions if a.dest == 'command')
    assert 'collect-aftermarket' in action.choices
