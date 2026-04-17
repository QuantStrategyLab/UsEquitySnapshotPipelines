from __future__ import annotations

import argparse
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from .artifacts import write_json
from .crisis_response_shadow_plugin import (
    SHADOW_MODE,
    build_crisis_response_shadow_signal,
    write_crisis_response_shadow_outputs,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table
from .taco_panic_rebound_research import DEFAULT_EVENT_SET, resolve_trade_war_event_set
from .taco_rebound_shadow_plugin import (
    TACO_REBOUND_PROFILE,
    build_taco_rebound_shadow_signal,
    write_taco_rebound_shadow_outputs,
)

DEFAULT_RUNNER_OUTPUT_DIR = "data/output/strategy_plugins"
PLUGIN_CRISIS_RESPONSE_SHADOW = "crisis_response_shadow"
PLUGIN_TACO_REBOUND_SHADOW = TACO_REBOUND_PROFILE
PLUGIN_MODE_PAPER = "paper"
PLUGIN_MODE_ADVISORY = "advisory"
PLUGIN_MODE_LIVE = "live"
SUPPORTED_PLUGIN_MODES = (SHADOW_MODE, PLUGIN_MODE_PAPER, PLUGIN_MODE_ADVISORY, PLUGIN_MODE_LIVE)
PLUGIN_COMPATIBLE_STRATEGIES: dict[str, tuple[str, ...]] = {
    PLUGIN_CRISIS_RESPONSE_SHADOW: ("tqqq_growth_income",),
    PLUGIN_TACO_REBOUND_SHADOW: (
        "dynamic_mega_leveraged_pullback",
        "mags_official_leveraged_pullback",
    ),
}


@dataclass(frozen=True)
class PluginRunResult:
    strategy: str
    plugin: str
    enabled: bool
    mode: str
    effective_mode: str | None
    status: str
    output_dir: str | None = None
    latest_signal_path: str | None = None
    message: str = ""


PluginRunner = Callable[[Mapping[str, Any], str], PluginRunResult]


def load_plugin_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"plugin config not found: {config_path}")
    return tomllib.loads(config_path.read_text(encoding="utf-8"))


def _as_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_values = value.split(",")
    else:
        raw_values = list(value)
    return tuple(str(item).strip() for item in raw_values if str(item).strip())


def _as_credit_pairs(value: Any) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for item in _as_str_tuple(value):
        parts = [part.strip().upper() for part in item.replace("/", ":").split(":")]
        if len(parts) != 2 or not all(parts):
            raise ValueError(f"credit pair must use NUMERATOR:DENOMINATOR syntax: {item!r}")
        pair = (parts[0], parts[1])
        if pair not in pairs:
            pairs.append(pair)
    return tuple(pairs)


def _optional_table(path: Any) -> pd.DataFrame | None:
    raw_path = str(path or "").strip()
    if not raw_path:
        return None
    return read_table(raw_path)


def _safe_scope_name(value: Any, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"strategy plugin entry requires {field}")
    safe = "".join(char if char.isalnum() or char in {"_", "-", "."} else "_" for char in text)
    return safe.strip("_") or text.replace("/", "_")


def _plugin_mode(plugin_config: Mapping[str, Any], default_mode: str) -> str:
    return str(plugin_config.get("mode", default_mode)).strip().lower()


def _validate_plugin_mode(plugin_name: str, mode: str) -> None:
    if mode not in SUPPORTED_PLUGIN_MODES:
        modes = ", ".join(SUPPORTED_PLUGIN_MODES)
        raise ValueError(f"{plugin_name} supports only configured modes {modes}; got mode={mode!r}")


def _validate_plugin_strategy(plugin_name: str, strategy: str) -> None:
    compatible = PLUGIN_COMPATIBLE_STRATEGIES.get(plugin_name, ())
    if compatible and strategy not in compatible:
        choices = ", ".join(compatible)
        raise ValueError(
            f"{plugin_name} is strategy-limited and can only be mounted to: {choices}; got strategy={strategy!r}"
        )


def _flatten_strategy_plugin_entry(entry: Mapping[str, Any]) -> dict[str, Any]:
    plugin_config = {
        key: value
        for key, value in entry.items()
        if key not in {"inputs", "outputs", "settings"}
    }
    for nested_key in ("inputs", "outputs", "settings"):
        nested = entry.get(nested_key, {})
        if nested is None:
            continue
        if not isinstance(nested, Mapping):
            raise ValueError(f"{nested_key} must be a table")
        duplicate_keys = sorted(set(plugin_config).intersection(nested))
        if duplicate_keys:
            keys = ", ".join(duplicate_keys)
            raise ValueError(f"duplicate strategy plugin config key(s) in {nested_key}: {keys}")
        plugin_config.update(nested)
    return plugin_config


def _default_plugin_output_dir(strategy: str, plugin: str) -> str:
    return str(Path("data/output") / strategy / "plugins" / plugin)


def _build_crisis_response_kwargs(plugin_config: Mapping[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    string_keys = {
        "as_of",
        "start_date",
        "end_date",
        "benchmark_symbol",
        "attack_symbol",
        "market_symbol",
        "synthetic_attack_from",
        "external_valuation_mode",
    }
    numeric_keys = {
        "synthetic_attack_multiple",
        "synthetic_attack_expense_rate",
        "crisis_drawdown",
        "crisis_risk_multiplier",
        "severe_crisis_risk_multiplier",
        "bubble_fragility_risk_multiplier",
        "bubble_fragility_drawdown",
        "external_trailing_pe_threshold",
        "external_forward_pe_threshold",
        "external_cape_threshold",
        "external_unprofitable_growth_threshold",
        "external_pct_above_200d_threshold",
        "external_pct_above_50d_threshold",
        "external_new_high_new_low_spread_threshold",
        "external_advance_decline_drawdown_threshold",
        "external_negative_earnings_share_threshold",
        "external_earnings_revision_3m_threshold",
        "external_margin_revision_3m_threshold",
    }
    integer_keys = {
        "crisis_confirm_days",
        "bubble_fragility_ma_days",
        "bubble_fragility_ma_slope_days",
        "bubble_fragility_confirm_days",
        "max_price_age_days",
        "max_external_context_age_days",
    }
    for key in string_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = str(plugin_config[key]).strip()
    for key in numeric_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = float(plugin_config[key])
    for key in integer_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = int(plugin_config[key])
    if "financial_symbols" in plugin_config:
        kwargs["financial_symbols"] = _as_str_tuple(plugin_config["financial_symbols"])
    if "rate_symbols" in plugin_config:
        kwargs["rate_symbols"] = _as_str_tuple(plugin_config["rate_symbols"])
    if "credit_pairs" in plugin_config:
        kwargs["credit_pairs"] = _as_credit_pairs(plugin_config["credit_pairs"])
    return kwargs


def _build_taco_rebound_kwargs(plugin_config: Mapping[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    string_keys = {
        "as_of",
        "start_date",
        "end_date",
        "benchmark_symbol",
        "attack_symbol",
    }
    numeric_keys = {
        "tariff_softening_sleeve",
        "geopolitical_deescalation_sleeve",
        "shock_sleeve",
        "max_sleeve",
        "crisis_guard_drawdown",
    }
    integer_keys = {
        "active_signal_days",
        "crisis_guard_ma_days",
        "crisis_guard_ma_slope_days",
        "max_price_age_days",
    }
    bool_keys = {"suppress_when_price_crisis_guard_active"}
    for key in string_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = str(plugin_config[key]).strip()
    for key in numeric_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = float(plugin_config[key])
    for key in integer_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = int(plugin_config[key])
    for key in bool_keys:
        if key in plugin_config and plugin_config[key] is not None:
            kwargs[key] = _as_bool(plugin_config[key])
    return kwargs


def _mode_execution_controls(mode: str) -> dict[str, Any]:
    if mode == SHADOW_MODE:
        return {
            "capital_impact": "none",
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
            "paper_ledger_required": False,
            "human_confirmation_required": False,
            "risk_controls_required": False,
            "notification_profile": "shadow_only",
        }
    if mode == PLUGIN_MODE_PAPER:
        return {
            "capital_impact": "none",
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
            "paper_ledger_required": True,
            "human_confirmation_required": False,
            "risk_controls_required": False,
            "notification_profile": "paper",
        }
    if mode == PLUGIN_MODE_ADVISORY:
        return {
            "capital_impact": "manual_only",
            "broker_order_allowed": False,
            "live_allocation_mutation_allowed": False,
            "paper_ledger_required": False,
            "human_confirmation_required": True,
            "risk_controls_required": False,
            "notification_profile": "advisory",
        }
    if mode == PLUGIN_MODE_LIVE:
        return {
            "capital_impact": "bounded_by_platform_policy",
            "broker_order_allowed": True,
            "live_allocation_mutation_allowed": True,
            "paper_ledger_required": False,
            "human_confirmation_required": False,
            "risk_controls_required": True,
            "notification_profile": "live",
        }
    raise ValueError(f"unsupported plugin mode: {mode!r}")


def run_crisis_response_shadow_plugin(plugin_config: Mapping[str, Any], default_mode: str) -> PluginRunResult:
    strategy = _safe_scope_name(plugin_config.get("strategy"), field="strategy")
    plugin = _safe_scope_name(plugin_config.get("plugin", PLUGIN_CRISIS_RESPONSE_SHADOW), field="plugin")
    mode = _plugin_mode(plugin_config, default_mode)
    output_dir = str(plugin_config.get("output_dir") or _default_plugin_output_dir(strategy, plugin)).strip()
    enabled = _as_bool(plugin_config.get("enabled"), default=True)
    if not enabled:
        return PluginRunResult(
            strategy=strategy,
            plugin=plugin,
            enabled=False,
            mode=mode,
            effective_mode=None,
            status="skipped",
            output_dir=output_dir,
            message="plugin disabled",
        )
    _validate_plugin_mode(plugin, mode)

    prices_path = str(plugin_config.get("prices", "")).strip()
    if not prices_path:
        raise ValueError(f"{plugin} for strategy={strategy} requires a prices path")
    price_history = read_table(prices_path)
    external_context = _optional_table(plugin_config.get("external_context"))
    event_set = str(plugin_config.get("event_set", DEFAULT_EVENT_SET)).strip() or DEFAULT_EVENT_SET

    payload = build_crisis_response_shadow_signal(
        price_history,
        events=resolve_trade_war_event_set(event_set),
        external_context=external_context,
        **_build_crisis_response_kwargs(plugin_config),
    )
    payload["strategy"] = strategy
    payload["plugin"] = plugin
    payload["mode"] = mode
    payload["configured_mode"] = mode
    payload["effective_mode"] = mode
    payload.setdefault("execution_controls", {})
    payload["execution_controls"].update(_mode_execution_controls(mode))
    payload["execution_controls"]["configured_mode"] = mode
    payload["execution_controls"]["effective_mode"] = mode
    payload["execution_controls"]["repository_broker_write_allowed"] = False
    payload["execution_controls"]["repository_allocation_mutation_allowed"] = False
    payload["execution_controls"]["mode_note"] = (
        "Mode is the platform behavior contract; this repository writes artifacts and does not call brokers"
    )
    paths = write_crisis_response_shadow_outputs(payload, output_dir)
    return PluginRunResult(
        strategy=strategy,
        plugin=plugin,
        enabled=True,
        mode=mode,
        effective_mode=mode,
        status="ok",
        output_dir=output_dir,
        latest_signal_path=str(paths["latest_signal"]),
        message=f"route={payload['canonical_route']} action={payload['suggested_action']}",
    )


def run_taco_rebound_shadow_plugin(plugin_config: Mapping[str, Any], default_mode: str) -> PluginRunResult:
    strategy = _safe_scope_name(plugin_config.get("strategy"), field="strategy")
    plugin = _safe_scope_name(plugin_config.get("plugin", PLUGIN_TACO_REBOUND_SHADOW), field="plugin")
    mode = _plugin_mode(plugin_config, default_mode)
    output_dir = str(plugin_config.get("output_dir") or _default_plugin_output_dir(strategy, plugin)).strip()
    enabled = _as_bool(plugin_config.get("enabled"), default=True)
    if not enabled:
        return PluginRunResult(
            strategy=strategy,
            plugin=plugin,
            enabled=False,
            mode=mode,
            effective_mode=None,
            status="skipped",
            output_dir=output_dir,
            message="plugin disabled",
        )
    _validate_plugin_mode(plugin, mode)

    prices_path = str(plugin_config.get("prices", "")).strip()
    if not prices_path:
        raise ValueError(f"{plugin} for strategy={strategy} requires a prices path")
    price_history = read_table(prices_path)
    event_set = str(plugin_config.get("event_set", DEFAULT_EVENT_SET)).strip() or DEFAULT_EVENT_SET

    payload = build_taco_rebound_shadow_signal(
        price_history,
        events=resolve_trade_war_event_set(event_set),
        **_build_taco_rebound_kwargs(plugin_config),
    )
    payload["strategy"] = strategy
    payload["plugin"] = plugin
    payload["mode"] = mode
    payload["configured_mode"] = mode
    payload["effective_mode"] = mode
    payload.setdefault("execution_controls", {})
    payload["execution_controls"].update(_mode_execution_controls(mode))
    payload["execution_controls"]["configured_mode"] = mode
    payload["execution_controls"]["effective_mode"] = mode
    payload["execution_controls"]["repository_broker_write_allowed"] = False
    payload["execution_controls"]["repository_allocation_mutation_allowed"] = False
    payload["execution_controls"]["mode_note"] = (
        "Mode is the platform behavior contract; this repository writes artifacts and does not call brokers"
    )
    paths = write_taco_rebound_shadow_outputs(payload, output_dir)
    return PluginRunResult(
        strategy=strategy,
        plugin=plugin,
        enabled=True,
        mode=mode,
        effective_mode=mode,
        status="ok",
        output_dir=output_dir,
        latest_signal_path=str(paths["latest_signal"]),
        message=f"route={payload['canonical_route']} action={payload['suggested_action']}",
    )


PLUGIN_RUNNERS: dict[str, PluginRunner] = {
    PLUGIN_CRISIS_RESPONSE_SHADOW: run_crisis_response_shadow_plugin,
    PLUGIN_TACO_REBOUND_SHADOW: run_taco_rebound_shadow_plugin,
}


def _strategy_plugin_entries(
    config: Mapping[str, Any],
    *,
    selected_plugins: Sequence[str] | None = None,
    selected_strategies: Sequence[str] | None = None,
) -> tuple[dict[str, Any], ...]:
    entries = config.get("strategy_plugins", ())
    if not isinstance(entries, list):
        raise ValueError("strategy_plugins config must be an array of tables")

    plugin_filter = set(_as_str_tuple(selected_plugins))
    strategy_filter = set(_as_str_tuple(selected_strategies))
    selected: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise ValueError("each strategy_plugins entry must be a table")
        plugin_config = _flatten_strategy_plugin_entry(entry)
        strategy = _safe_scope_name(plugin_config.get("strategy"), field="strategy")
        plugin = _safe_scope_name(plugin_config.get("plugin"), field="plugin")
        if plugin_filter and plugin not in plugin_filter:
            continue
        if strategy_filter and strategy not in strategy_filter:
            continue
        _validate_plugin_strategy(plugin, strategy)
        plugin_config["strategy"] = strategy
        plugin_config["plugin"] = plugin
        selected.append(plugin_config)
    return tuple(selected)


def run_configured_plugins(
    config: Mapping[str, Any],
    *,
    selected_plugins: Sequence[str] | None = None,
    selected_strategies: Sequence[str] | None = None,
) -> dict[str, Any]:
    default_mode = str(config.get("default_mode", SHADOW_MODE)).strip().lower()
    plugin_configs = _strategy_plugin_entries(
        config,
        selected_plugins=selected_plugins,
        selected_strategies=selected_strategies,
    )

    results: list[PluginRunResult] = []
    for plugin_config in plugin_configs:
        plugin = str(plugin_config["plugin"])
        if plugin not in PLUGIN_RUNNERS:
            raise ValueError(f"unsupported plugin: {plugin}")
        results.append(PLUGIN_RUNNERS[plugin](plugin_config, default_mode))

    output_dir = Path(str(config.get("output_dir", DEFAULT_RUNNER_OUTPUT_DIR)).strip())
    summary = {
        "schema_version": "strategy_plugins.v1",
        "default_mode": default_mode,
        "strategy_plugins": [asdict(result) for result in results],
    }
    write_json(output_dir / "latest_run.json", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run configured sidecar strategy plugins.")
    parser.add_argument("--config", required=True, help="TOML config file listing strategy-scoped sidecar plugins")
    parser.add_argument("--plugins", default=None, help="Optional comma-separated plugin allowlist")
    parser.add_argument("--strategies", default=None, help="Optional comma-separated strategy allowlist")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_plugin_config(args.config)
    summary = run_configured_plugins(
        config,
        selected_plugins=_as_str_tuple(args.plugins) or None,
        selected_strategies=_as_str_tuple(args.strategies) or None,
    )
    for result in summary["strategy_plugins"]:
        print(
            f"{result['strategy']}:{result['plugin']} {result['status']} mode={result['mode']} "
            f"latest={result.get('latest_signal_path') or ''} {result.get('message') or ''}".rstrip()
        )
    return 0


__all__ = [
    "PLUGIN_CRISIS_RESPONSE_SHADOW",
    "PLUGIN_TACO_REBOUND_SHADOW",
    "PLUGIN_COMPATIBLE_STRATEGIES",
    "PLUGIN_MODE_ADVISORY",
    "PLUGIN_MODE_LIVE",
    "PLUGIN_MODE_PAPER",
    "PluginRunResult",
    "load_plugin_config",
    "main",
    "run_configured_plugins",
    "run_crisis_response_shadow_plugin",
    "run_taco_rebound_shadow_plugin",
]
