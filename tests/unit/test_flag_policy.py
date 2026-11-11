from __future__ import annotations

import inspect
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Iterable, Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings

import src.core.config as config_module
from src.core.config import (
    AftermarketSettings,
    CollectorSettings,
    PremarketSettings,
    RegularKisSettings,
    resolve_collector_runtime,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "docs" / "flags.toml"
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
COMPOSE_SERVICE = "krx-collector"
BUDGET_TOOL = REPO_ROOT / "tools" / "instruction_budget.py"
RULE_FILE = ".agents/rules/config-flags.md"
TOGGLE_NAME_RE = re.compile(r"(?i)(enable|disable|resend|_on_|allow|auto_adopt|force_)")
KINDS = frozenset(
    {"env_binding", "safety_interlock", "component", "deployed_component", "experiment", "legacy_rollout"}
)
LEGACY_ROLLOUT = frozenset(
    {
        "SnapshotSettings.enabled",
        "UniverseTierSettings.core_enabled",
        "UniverseTierSettings.downside_enabled",
        "TickTapeBackfillSettings.enabled",
        "TickTapeOverflowSettings.enabled",
        "RepairSettings.hold_enabled",
        "RepairSettings.job_enabled",
    }
)
REVIEW_GRACE_DAYS = 14
TODAY: date = date.today()

_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})

# Deploy-only values for each deployed_component that is built directly; the coupled
# qualified fields must each be necessary for construction to succeed.
DEPLOYED_COUPLING: dict[str, tuple[Callable[[dict[str, Any]], object], dict[str, Any], tuple[str, ...]]] = {
    "AftermarketSettings.enabled": (
        lambda kwargs: AftermarketSettings(**kwargs),
        {"enabled": True, "pair_capacity_per_connection": 41},
        ("AftermarketSettings.pair_capacity_per_connection",),
    ),
    "PremarketSettings.enabled": (
        lambda kwargs: PremarketSettings(**kwargs),
        {"enabled": True, "credential_slot": "5", "pair_capacity_per_connection": 41},
        ("PremarketSettings.credential_slot", "PremarketSettings.pair_capacity_per_connection"),
    ),
    "RegularKisSettings.enabled": (
        lambda kwargs: RegularKisSettings(**kwargs),
        {"enabled": True, "credential_slots": ("1", "2", "3", "4"), "pair_capacity_per_connection": 41},
        ("RegularKisSettings.credential_slots", "RegularKisSettings.pair_capacity_per_connection"),
    ),
}
COLLECTOR_AFTER_MARKET = "CollectorSettings.after_market_enabled"
MISSING_COUPLING_MESSAGES: dict[str, str] = {
    "AftermarketSettings.pair_capacity_per_connection": "pair_capacity_per_connection must be a positive",
    "PremarketSettings.credential_slot": "credential_slot must be set when enabled",
    "PremarketSettings.pair_capacity_per_connection": "pair_capacity_per_connection must be a positive",
    "RegularKisSettings.credential_slots": "credential_slots must be non-empty",
    "RegularKisSettings.pair_capacity_per_connection": "pair_capacity_per_connection must be a positive",
}
AFTERMARKET_CAPACITY_ENV = "KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION"


@pytest.fixture(autouse=True)
def _hermetic_krx_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("KRX_ALPHA_"):
            monkeypatch.delenv(key, raising=False)


def discover_toggles(classes: Iterable[type[BaseModel]] | None = None) -> dict[str, tuple[bool, str]]:
    """Map qualified toggle name to (code default, env name)."""
    if classes is None:
        classes = [
            cls
            for _, cls in inspect.getmembers(config_module, inspect.isclass)
            if issubclass(cls, BaseSettings) and cls.__module__ == config_module.__name__
        ]
    toggles: dict[str, tuple[bool, str]] = {}
    for cls in classes:
        prefix = str(cls.model_config.get("env_prefix", ""))
        for field_name, field in cls.model_fields.items():
            if field.annotation is bool and TOGGLE_NAME_RE.search(field_name):
                toggles[f"{cls.__name__}.{field_name}"] = (bool(field.default), prefix + field_name.upper())
    return toggles


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, dict[str, Any]]:
    with path.open("rb") as handle:
        rows = tomllib.load(handle).get("flag", [])
    registry: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = row["name"]
        if name in registry:
            raise ValueError(f"duplicate flag registry row: {name}")
        registry[name] = row
    return registry


def parse_compose_env(path: Path = COMPOSE_PATH) -> dict[str, str]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    entries = document["services"][COMPOSE_SERVICE]["environment"]
    if not isinstance(entries, list):
        raise TypeError(f"{path}: {COMPOSE_SERVICE} environment must be a list")
    env: dict[str, str] = {}
    for entry in entries:
        key, sep, value = str(entry).partition("=")
        if not sep:
            raise ValueError(f"{path}: environment entry without '=': {entry!r}")
        if key in env:
            raise ValueError(f"{path}: duplicate environment key {key}")
        env[key] = value
    return env


def _as_bool(value: str) -> bool | None:
    token = value.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    return None


def coverage_violations(
    toggles: Mapping[str, tuple[bool, str]], registry: Mapping[str, Mapping[str, Any]]
) -> list[str]:
    missing = [
        f"{name}: toggle has no row in docs/flags.toml ({RULE_FILE})" for name in toggles if name not in registry
    ]
    stale = [f"{name}: registry row names no existing toggle ({RULE_FILE})" for name in registry if name not in toggles]
    return [*missing, *stale]


def kind_violations(registry: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [
        f"{name}: kind {row.get('kind')!r} is not one of {sorted(KINDS)} ({RULE_FILE})"
        for name, row in registry.items()
        if row.get("kind") not in KINDS
    ]


def default_violations(toggles: Mapping[str, tuple[bool, str]], registry: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [
        f"{name}: registry default {row.get('default')!r} differs from code default {toggles[name][0]!r} ({RULE_FILE})"
        for name, row in registry.items()
        if name in toggles and row.get("default") != toggles[name][0]
    ]


def component_violations(registry: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [
        f"{name}: component must default to True, found {row.get('default')!r} ({RULE_FILE})"
        for name, row in registry.items()
        if row.get("kind") == "component" and row.get("default") is not True
    ]


def _deadline_violations(name: str, row: Mapping[str, Any], today: date) -> list[str]:
    kind = row.get("kind")
    violations: list[str] = []
    if not str(row.get("exit_condition", "")).strip():
        violations.append(f"{name}: {kind} needs a non-empty exit_condition ({RULE_FILE})")
    try:
        review_by = date.fromisoformat(str(row.get("review_by", "")))
    except ValueError:
        violations.append(f"{name}: {kind} review_by must be YYYY-MM-DD ({RULE_FILE})")
        return violations
    if today > review_by + timedelta(days=REVIEW_GRACE_DAYS):
        violations.append(
            f"{name}: {kind} review_by {review_by.isoformat()} is more than {REVIEW_GRACE_DAYS} days past; "
            f"delete the flag and its OFF branch ({RULE_FILE})"
        )
    return violations


def experiment_violations(registry: Mapping[str, Mapping[str, Any]], today: date) -> list[str]:
    return [
        violation
        for name, row in registry.items()
        if row.get("kind") == "experiment"
        for violation in _deadline_violations(name, row, today)
    ]


def legacy_violations(registry: Mapping[str, Mapping[str, Any]], today: date) -> list[str]:
    legacy_rows = {name for name, row in registry.items() if row.get("kind") == "legacy_rollout"}
    violations = [
        f"{name}: legacy_rollout row is not in the frozen LEGACY_ROLLOUT list ({RULE_FILE})"
        for name in sorted(legacy_rows - LEGACY_ROLLOUT)
    ]
    violations += [
        f"{name}: LEGACY_ROLLOUT entry has no legacy_rollout row ({RULE_FILE})"
        for name in sorted(LEGACY_ROLLOUT - legacy_rows)
    ]
    for name in sorted(legacy_rows):
        violations += _deadline_violations(name, registry[name], today)
    return violations


def deployed_violations(registry: Mapping[str, Mapping[str, Any]]) -> list[str]:
    violations: list[str] = []
    for name, row in registry.items():
        if row.get("kind") != "deployed_component":
            continue
        if not row.get("couples"):
            violations.append(f"{name}: deployed_component must list couples ({RULE_FILE})")
        derived = row.get("derived_from")
        if derived is not None and registry.get(derived, {}).get("kind") != "deployed_component":
            violations.append(f"{name}: derived_from {derived!r} is not a deployed_component row ({RULE_FILE})")
    return violations


def duplicate_default_violations(
    toggles: Mapping[str, tuple[bool, str]], registry: Mapping[str, Mapping[str, Any]], compose_env: Mapping[str, str]
) -> list[str]:
    violations: list[str] = []
    for name, (default, env_name) in toggles.items():
        row = registry.get(name)
        if row is None or row.get("kind") == "env_binding" or env_name not in compose_env:
            continue
        parsed = _as_bool(compose_env[env_name])
        if parsed is None:
            violations.append(f"{env_name}: compose value {compose_env[env_name]!r} is not a boolean ({RULE_FILE})")
        elif parsed == default:
            violations.append(f"{env_name}: compose sets the code default {default!r} ({RULE_FILE})")
    return violations


def component_in_compose_violations(
    toggles: Mapping[str, tuple[bool, str]], registry: Mapping[str, Mapping[str, Any]], compose_env: Mapping[str, str]
) -> list[str]:
    return [
        f"{env_name}: component toggle must not be set in compose ({RULE_FILE})"
        for name, (_, env_name) in toggles.items()
        if registry.get(name, {}).get("kind") == "component" and env_name in compose_env
    ]


def required_in_compose_violations(
    toggles: Mapping[str, tuple[bool, str]], registry: Mapping[str, Mapping[str, Any]], compose_env: Mapping[str, str]
) -> list[str]:
    violations: list[str] = []
    for name, row in registry.items():
        kind = row.get("kind")
        if kind not in {"deployed_component", "legacy_rollout"} or name not in toggles:
            continue
        env_name = toggles[name][1]
        derived = row.get("derived_from")
        if derived is not None:
            if env_name in compose_env:
                violations.append(f"{env_name}: derived toggle must not be set in compose ({RULE_FILE})")
            derived_env = toggles[derived][1]
            if _as_bool(compose_env.get(derived_env, "")) is not True:
                violations.append(f"{derived_env}: derived_from source must be set true in compose ({RULE_FILE})")
            continue
        if _as_bool(compose_env.get(env_name, "")) is not True:
            violations.append(f"{env_name}: {kind} must be set true in compose ({RULE_FILE})")
    return violations


def test_registry_covers_every_toggle() -> None:
    assert coverage_violations(discover_toggles(), load_registry()) == []


def test_registry_covers_synthetic_stale_row() -> None:
    registry = {"GONE_ENABLED": {"name": "GONE_ENABLED", "kind": "safety_interlock", "default": True}}

    assert coverage_violations({}, registry) == [f"GONE_ENABLED: registry row names no existing toggle ({RULE_FILE})"]


def test_registry_kind_is_valid() -> None:
    assert kind_violations(load_registry()) == []
    assert kind_violations({"X_ENABLED": {"kind": "rollout", "default": True}}) == [
        f"X_ENABLED: kind 'rollout' is not one of {sorted(KINDS)} ({RULE_FILE})"
    ]


def test_registry_default_matches_code() -> None:
    assert default_violations(discover_toggles(), load_registry()) == []


def test_component_defaults_on_and_not_in_compose() -> None:
    registry = load_registry()
    toggles = discover_toggles()
    compose_env = parse_compose_env()

    assert component_violations(registry) == []
    assert component_in_compose_violations(toggles, registry, compose_env) == []
    assert component_violations({"X_ENABLED": {"kind": "component", "default": False}}) != []


def test_legacy_rollout_list_is_frozen_and_set_in_compose() -> None:
    registry = load_registry()
    toggles = discover_toggles()
    compose_env = parse_compose_env()

    assert legacy_violations(registry, TODAY) == []
    assert required_in_compose_violations(toggles, registry, compose_env) == []
    synthetic = {
        "X_ENABLED": {"kind": "legacy_rollout", "default": False, "exit_condition": "x", "review_by": TODAY.isoformat()}
    }
    assert legacy_violations(synthetic, TODAY) != []


def test_compose_does_not_duplicate_defaults() -> None:
    assert duplicate_default_violations(discover_toggles(), load_registry(), parse_compose_env()) == []


@pytest.mark.parametrize("name", sorted(DEPLOYED_COUPLING))
def test_deployed_component_coupling_is_proven(name: str) -> None:
    build, valid, couples = DEPLOYED_COUPLING[name]
    row = load_registry()[name]

    assert row["couples"] == list(couples)
    build(dict(valid))
    for qualified in couples:
        partial = {key: value for key, value in valid.items() if key != qualified.split(".", 1)[1]}
        with pytest.raises(ValueError, match=re.escape(MISSING_COUPLING_MESSAGES[qualified])):
            build(partial)


def test_after_market_coupling_through_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = CollectorSettings(after_market_enabled=True)

    with pytest.raises(ValueError, match="pair_capacity_per_connection must be a positive"):
        resolve_collector_runtime(collector=collector)
    monkeypatch.setenv(AFTERMARKET_CAPACITY_ENV, "41")
    runtime = resolve_collector_runtime(collector=collector)

    assert runtime.aftermarket.enabled is True


def test_deployed_components_are_all_proven_and_set_in_compose() -> None:
    registry = load_registry()
    toggles = discover_toggles()
    compose_env = parse_compose_env()
    deployed = {name for name, row in registry.items() if row.get("kind") == "deployed_component"}

    assert deployed == {*DEPLOYED_COUPLING, COLLECTOR_AFTER_MARKET, "AftermarketSettings.enabled"}
    assert deployed_violations(registry) == []
    assert required_in_compose_violations(toggles, registry, compose_env) == []
    assert registry["AftermarketSettings.enabled"]["derived_from"] == COLLECTOR_AFTER_MARKET
    assert toggles["AftermarketSettings.enabled"][1] not in compose_env


def test_experiment_requires_deadline_and_exit() -> None:
    base = {"kind": "experiment", "default": False, "reason": "r"}
    missing_exit = {**base, "exit_condition": "", "review_by": TODAY.isoformat()}
    missing_date = {**base, "exit_condition": "metric holds", "review_by": ""}
    stale = {**base, "exit_condition": "metric holds", "review_by": (TODAY - timedelta(days=15)).isoformat()}
    within_grace = {**base, "exit_condition": "metric holds", "review_by": (TODAY - timedelta(days=13)).isoformat()}

    assert experiment_violations({"X_ENABLED": missing_exit}, TODAY) != []
    assert experiment_violations({"X_ENABLED": missing_date}, TODAY) != []
    assert experiment_violations({"X_ENABLED": stale}, TODAY) != []
    assert experiment_violations({"X_ENABLED": within_grace}, TODAY) == []
    assert experiment_violations(load_registry(), TODAY) == []


def test_policy_detects_new_default_off_flag() -> None:
    class _Synthetic(BaseModel):
        NEW_FEATURE_ENABLED: bool = False

    toggles = discover_toggles([_Synthetic])

    assert toggles == {"_Synthetic.NEW_FEATURE_ENABLED": (False, "NEW_FEATURE_ENABLED")}
    assert coverage_violations(toggles, load_registry()) == [
        f"_Synthetic.NEW_FEATURE_ENABLED: toggle has no row in docs/flags.toml ({RULE_FILE})",
        *[
            f"{name}: registry row names no existing toggle ({RULE_FILE})"
            for name in load_registry()
            if name not in toggles
        ],
    ]


def test_config_flags_rule_within_budget() -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(BUDGET_TOOL)], capture_output=True, text=True, check=False, cwd=REPO_ROOT
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "violations=0" in result.stdout
