from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from quant_strategy_plugins import strategy_plugin_runner as base_runner

from us_equity_snapshot_pipelines.crisis_response_research import ROUTE_TRUE_CRISIS
from us_equity_snapshot_pipelines.strategy_plugin_runner import (
    GENERAL_MARKET_REGIME_NOTIFICATION_TARGET,
    IBIT_SMART_DCA_STRATEGY,
    IBIT_ZSCORE_EXIT_POLICY,
    PLUGIN_CRISIS_RESPONSE_SHADOW,
    PLUGIN_IBIT_ZSCORE_EXIT,
    PLUGIN_MARKET_REGIME_CONTROL,
    PLUGIN_TACO_REBOUND_SHADOW,
    load_plugin_config,
    main,
    run_configured_plugins,
)

STRATEGY_NAME = "tqqq_growth_income"
SOXL_STRATEGY_NAME = "soxl_soxx_trend_income"
LEFT_SIDE_STRATEGY_NAME = "russell_top50_leader_rotation"
IBIT_STRATEGY_NAME = "ibit_smart_dca"


def test_ibit_zscore_exit_policy_remains_notification_only_until_promotion() -> None:
    assert IBIT_SMART_DCA_STRATEGY == IBIT_STRATEGY_NAME
    assert IBIT_ZSCORE_EXIT_POLICY.notification_allowed is True
    assert IBIT_ZSCORE_EXIT_POLICY.position_control_allowed is False
    assert IBIT_ZSCORE_EXIT_POLICY.evidence_status == base_runner.EVIDENCE_NOTIFICATION_ONLY
    assert (
        base_runner.PLUGIN_CONSUMPTION_POLICY_REGISTRY[(PLUGIN_IBIT_ZSCORE_EXIT, IBIT_STRATEGY_NAME)]
        == IBIT_ZSCORE_EXIT_POLICY
    )


def _quiet_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=230)
    rows: list[dict[str, object]] = []
    for symbol in ("QQQ", "TQQQ", "SPY"):
        for offset, as_of in enumerate(dates):
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of,
                    "close": 100.0 + offset * 0.01,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _soxl_quiet_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=230)
    rows: list[dict[str, object]] = []
    for symbol in ("SOXX", "SOXL", "SPY"):
        for offset, as_of in enumerate(dates):
            multiplier = 3.0 if symbol == "SOXL" else 1.0
            rows.append(
                {
                    "symbol": symbol,
                    "as_of": as_of,
                    "close": 100.0 + offset * 0.01 * multiplier,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _financial_crisis_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2007-01-02", periods=310)
    rows: list[dict[str, object]] = []
    qqq = pd.Series(100.0, index=dates)
    qqq.iloc[245:] = pd.Series(
        [100.0 - idx * (35.0 / (len(dates) - 245 - 1)) for idx in range(len(dates) - 245)],
        index=dates[245:],
    )
    tqqq = pd.Series(100.0, index=dates)
    tqqq.iloc[245:] = pd.Series(
        [100.0 - idx * (70.0 / (len(dates) - 245 - 1)) for idx in range(len(dates) - 245)],
        index=dates[245:],
    )
    xlf = pd.Series(100.0, index=dates)
    xlf.iloc[220:] = pd.Series(
        [100.0 - idx * (55.0 / (len(dates) - 220 - 1)) for idx in range(len(dates) - 220)],
        index=dates[220:],
    )
    hyg = pd.Series(100.0, index=dates)
    hyg.iloc[235:] = pd.Series(
        [100.0 - idx * (18.0 / (len(dates) - 235 - 1)) for idx in range(len(dates) - 235)],
        index=dates[235:],
    )
    prices = {
        "QQQ": qqq,
        "TQQQ": tqqq,
        "SPY": pd.Series(100.0, index=dates),
        "XLF": xlf,
        "HYG": hyg,
        "IEF": pd.Series(100.0, index=dates),
    }
    for symbol, series in prices.items():
        for as_of, close in series.items():
            rows.append({"symbol": symbol, "as_of": as_of, "close": close, "volume": 1_000_000})
    return pd.DataFrame(rows)


def _shadow_plugin_config(tmp_path, *, include_output_dir: bool = True) -> dict[str, object]:
    prices_path = tmp_path / "prices.csv"
    _quiet_prices().to_csv(prices_path, index=False)
    entry: dict[str, object] = {
        "strategy": STRATEGY_NAME,
        "plugin": PLUGIN_CRISIS_RESPONSE_SHADOW,
        "enabled": True,
        "mode": "shadow",
        "inputs": {
            "prices": str(prices_path),
            "as_of": "2025-11-19",
            "start_date": "2025-01-02",
            "financial_symbols": [],
            "credit_pairs": [],
            "rate_symbols": [],
        },
    }
    if include_output_dir:
        entry["outputs"] = {"output_dir": str(tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW)}
    return {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [entry],
    }


def _taco_rebound_prices() -> pd.DataFrame:
    dates = pd.bdate_range("2026-03-20", periods=16)
    qqq_path = [100.0, 99.0, 98.0, 97.0, 96.0, 95.0, 94.0, 92.0, 96.0, 99.0]
    tqqq_path = [100.0, 97.0, 94.0, 91.0, 88.0, 85.0, 82.0, 76.0, 88.0, 96.0]
    rows: list[dict[str, object]] = []
    for idx, as_of in enumerate(dates):
        qqq_close = qqq_path[idx] if idx < len(qqq_path) else 104.0 + idx
        tqqq_close = tqqq_path[idx] if idx < len(tqqq_path) else 110.0 + idx * 2.0
        rows.append({"symbol": "QQQ", "as_of": as_of, "close": qqq_close, "volume": 1_000_000})
        rows.append({"symbol": "TQQQ", "as_of": as_of, "close": tqqq_close, "volume": 1_000_000})
    return pd.DataFrame(rows)


def _ibit_zscore_history(*, latest_zscore: float = 8.5) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=220, freq="D")
    rows = [
        {
            "as_of": as_of,
            "mvrv_zscore": 2.0 + (idx % 20) * 0.05,
            "btc_close": 40_000.0 + idx * 10.0,
        }
        for idx, as_of in enumerate(dates[:-1])
    ]
    rows.append(
        {
            "as_of": dates[-1],
            "mvrv_zscore": latest_zscore,
            "btc_close": 65_000.0,
        }
    )
    return pd.DataFrame(rows)


def test_strategy_plugin_runner_executes_strategy_scoped_shadow_plugin(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    summary = run_configured_plugins(config)

    assert summary["schema_version"] == "strategy_plugins.v1"
    result = summary["strategy_plugins"][0]
    assert result["strategy"] == STRATEGY_NAME
    assert result["plugin"] == PLUGIN_CRISIS_RESPONSE_SHADOW
    assert result["mode"] == "shadow"
    assert result["effective_mode"] == "shadow"
    assert result["status"] == "ok"

    latest_signal = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json"
    latest_run = tmp_path / "runner" / "latest_run.json"
    assert latest_signal.exists()
    assert latest_run.exists()
    payload = json.loads(latest_signal.read_text(encoding="utf-8"))
    assert payload["strategy"] == STRATEGY_NAME
    assert payload["plugin"] == PLUGIN_CRISIS_RESPONSE_SHADOW
    assert payload["mode"] == "shadow"
    assert payload["configured_mode"] == "shadow"
    assert payload["effective_mode"] == "shadow"
    assert payload["execution_controls"]["broker_order_allowed"] is False
    assert payload["execution_controls"]["live_allocation_mutation_allowed"] is False
    assert payload["execution_controls"]["notification_profile"] == "shadow_only"
    assert payload["execution_controls"]["repository_broker_write_allowed"] is False
    assert payload["execution_controls"]["repository_allocation_mutation_allowed"] is False
    assert "platform behavior contract" in payload["execution_controls"]["mode_note"]


def test_strategy_plugin_runner_rehearses_triggered_shadow_artifact_without_execution_permissions(tmp_path) -> None:
    prices = _financial_crisis_prices()
    prices_path = tmp_path / "crisis_prices.csv"
    output_dir = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW
    as_of = str(pd.to_datetime(prices["as_of"]).max().date())
    prices.to_csv(prices_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": STRATEGY_NAME,
                "plugin": PLUGIN_CRISIS_RESPONSE_SHADOW,
                "enabled": True,
                "inputs": {
                    "prices": str(prices_path),
                    "as_of": as_of,
                    "start_date": "2007-01-02",
                    "financial_symbols": ["XLF"],
                    "credit_pairs": ["HYG:IEF"],
                    "rate_symbols": [],
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["status"] == "ok"
    assert result["message"] == f"route={ROUTE_TRUE_CRISIS} action=defend"
    payload = json.loads((output_dir / "latest_signal.json").read_text(encoding="utf-8"))
    assert payload["canonical_route"] == ROUTE_TRUE_CRISIS
    assert payload["suggested_action"] == "defend"
    assert payload["would_trade_if_enabled"] is True
    assert payload["price_scanner_active"] is True
    assert payload["execution_controls"]["broker_order_allowed"] is False
    assert payload["execution_controls"]["live_allocation_mutation_allowed"] is False
    assert payload["execution_controls"]["repository_broker_write_allowed"] is False
    assert payload["execution_controls"]["repository_allocation_mutation_allowed"] is False


def test_strategy_plugin_runner_defaults_output_under_strategy_plugin_scope(tmp_path, monkeypatch) -> None:
    config = _shadow_plugin_config(tmp_path, include_output_dir=False)
    monkeypatch.chdir(tmp_path)

    summary = run_configured_plugins(config)

    expected = tmp_path / "data" / "output" / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW
    assert summary["strategy_plugins"][0]["output_dir"] == str(
        Path("data/output") / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW
    )
    assert (expected / "latest_signal.json").exists()


def test_strategy_plugin_runner_can_skip_disabled_strategy_plugin(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"][0]["enabled"] = False

    summary = run_configured_plugins(config, selected_plugins=[PLUGIN_CRISIS_RESPONSE_SHADOW])

    assert summary["strategy_plugins"][0]["strategy"] == STRATEGY_NAME
    assert summary["strategy_plugins"][0]["status"] == "skipped"
    assert summary["strategy_plugins"][0]["effective_mode"] is None
    assert not (tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json").exists()


def test_strategy_plugin_runner_uses_default_mode_when_entry_mode_is_omitted(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    del config["strategy_plugins"][0]["mode"]

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["mode"] == "shadow"
    assert result["effective_mode"] == "shadow"
    latest_signal = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json"
    payload = json.loads(latest_signal.read_text(encoding="utf-8"))
    assert payload["strategy"] == STRATEGY_NAME
    assert payload["plugin"] == PLUGIN_CRISIS_RESPONSE_SHADOW
    assert payload["configured_mode"] == "shadow"
    assert payload["execution_controls"]["notification_profile"] == "shadow_only"


def test_strategy_plugin_runner_rejects_crisis_shadow_soxl_strategy_mount(tmp_path) -> None:
    prices_path = tmp_path / "soxl_prices.csv"
    output_dir = tmp_path / SOXL_STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW
    _soxl_quiet_prices().to_csv(prices_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": SOXL_STRATEGY_NAME,
                "plugin": PLUGIN_CRISIS_RESPONSE_SHADOW,
                "enabled": True,
                "inputs": {
                    "prices": str(prices_path),
                    "as_of": "2025-11-19",
                    "start_date": "2025-01-02",
                    "benchmark_symbol": "SOXX",
                    "attack_symbol": "SOXL",
                    "financial_symbols": [],
                    "credit_pairs": [],
                    "rate_symbols": [],
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    with pytest.raises(ValueError, match="strategy-limited"):
        run_configured_plugins(config)

    assert not (output_dir / "latest_signal.json").exists()


def test_strategy_plugin_runner_runs_soxl_market_regime_notification_target(tmp_path) -> None:
    prices_path = tmp_path / "market_regime_prices.csv"
    output_dir = tmp_path / GENERAL_MARKET_REGIME_NOTIFICATION_TARGET / "plugins" / PLUGIN_MARKET_REGIME_CONTROL
    _soxl_quiet_prices().to_csv(prices_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "notification_targets": [
            {
                "notification_target": GENERAL_MARKET_REGIME_NOTIFICATION_TARGET,
                "plugin": PLUGIN_MARKET_REGIME_CONTROL,
                "enabled": True,
                "inputs": {
                    "prices": str(prices_path),
                    "as_of": "2025-11-19",
                    "benchmark_symbol": "SOXX",
                    "attack_symbol": "SOXL",
                    "crisis_enabled": False,
                    "macro_enabled": False,
                    "taco_enabled": False,
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    summary = run_configured_plugins(config)

    assert summary["strategy_plugins"] == []
    result = summary["notification_targets"][0]
    assert result["strategy"] == ""
    assert result["target_type"] == "notification_target"
    assert result["notification_target"] == GENERAL_MARKET_REGIME_NOTIFICATION_TARGET
    assert result["plugin"] == PLUGIN_MARKET_REGIME_CONTROL
    assert result["status"] == "ok"
    payload = json.loads((output_dir / "latest_signal.json").read_text(encoding="utf-8"))
    assert "strategy" not in payload
    assert payload["target_type"] == "notification_target"
    assert payload["notification_target"] == GENERAL_MARKET_REGIME_NOTIFICATION_TARGET
    assert payload["execution_controls"]["capital_impact"] == "notification_only"
    assert payload["execution_controls"]["strategy_runtime_metadata_allowed"] is False
    assert payload["execution_controls"]["position_control_allowed"] is False


def test_strategy_plugin_runner_filters_by_strategy(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"].append(
        {
            **config["strategy_plugins"][0],
            "strategy": "soxl_growth_income",
            "outputs": {"output_dir": str(tmp_path / "soxl_growth_income" / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW)},
        }
    )

    summary = run_configured_plugins(config, selected_strategies=[STRATEGY_NAME])

    assert [result["strategy"] for result in summary["strategy_plugins"]] == [STRATEGY_NAME]
    assert (tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json").exists()
    assert not (
        tmp_path / "soxl_growth_income" / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json"
    ).exists()


def test_strategy_plugin_runner_runs_taco_rebound_notification_mount_for_tqqq(tmp_path) -> None:
    prices_path = tmp_path / "taco_prices.csv"
    output_dir = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_TACO_REBOUND_SHADOW
    _taco_rebound_prices().to_csv(prices_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": STRATEGY_NAME,
                "plugin": PLUGIN_TACO_REBOUND_SHADOW,
                "enabled": True,
                "inputs": {
                    "prices": str(prices_path),
                    "event_set": "geopolitical-deescalation",
                    "as_of": "2026-04-02",
                    "start_date": "2026-03-20",
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["strategy"] == STRATEGY_NAME
    assert result["plugin"] == PLUGIN_TACO_REBOUND_SHADOW
    assert result["status"] == "ok"
    assert "route=taco_rebound action=notify_manual_review" in result["message"]
    latest = json.loads((output_dir / "latest_signal.json").read_text(encoding="utf-8"))
    assert latest["manual_review_required"] is True
    assert latest["rebound_confirmation"]["confirmed"] is True
    assert latest["would_trade_if_enabled"] is False
    assert "sleeve_suggestion" not in latest


def test_strategy_plugin_runner_runs_ibit_zscore_exit_position_control(tmp_path) -> None:
    zscore_path = tmp_path / "ibit_zscore.csv"
    output_dir = tmp_path / IBIT_STRATEGY_NAME / "plugins" / PLUGIN_IBIT_ZSCORE_EXIT
    _ibit_zscore_history(latest_zscore=8.5).to_csv(zscore_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": IBIT_STRATEGY_NAME,
                "plugin": PLUGIN_IBIT_ZSCORE_EXIT,
                "enabled": True,
                "inputs": {
                    "zscore_metrics": str(zscore_path),
                    "as_of": "2024-08-07",
                    "dynamic_lookback_days": 180,
                    "dynamic_min_periods": 60,
                    "soft_exit_zscore_floor": 4.0,
                    "hard_exit_zscore_floor": 7.0,
                    "parking_symbol": "BOXX",
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["strategy"] == IBIT_STRATEGY_NAME
    assert result["plugin"] == PLUGIN_IBIT_ZSCORE_EXIT
    assert result["status"] == "ok"
    assert result["message"] == "route=risk_off action=defend"
    latest = json.loads((output_dir / "latest_signal.json").read_text(encoding="utf-8"))
    assert latest["schema_version"] == "ibit_zscore_exit.v1"
    assert latest["canonical_route"] == "risk_off"
    assert latest["suggested_action"] == "defend"
    assert latest["position_control"]["target_allocations"] == {"IBIT": 0.25, "BOXX": 0.75}
    assert latest["thresholds"]["threshold_mode"] == "rolling_percentile_hybrid"
    assert latest["execution_controls"]["position_control_allowed"] is False
    assert latest["execution_controls"]["consumption_evidence_status"] == "notification_only"


def test_strategy_plugin_runner_builds_dynamic_ibit_zscore_risk_reduced_route(tmp_path) -> None:
    zscore_path = tmp_path / "ibit_zscore.csv"
    output_dir = tmp_path / IBIT_STRATEGY_NAME / "plugins" / PLUGIN_IBIT_ZSCORE_EXIT
    _ibit_zscore_history(latest_zscore=5.5).to_csv(zscore_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": IBIT_STRATEGY_NAME,
                "plugin": PLUGIN_IBIT_ZSCORE_EXIT,
                "enabled": True,
                "inputs": {
                    "zscore_metrics": str(zscore_path),
                    "as_of": "2024-08-07",
                    "dynamic_min_periods": 60,
                    "soft_exit_zscore_floor": 5.0,
                    "hard_exit_zscore_floor": 7.0,
                    "parking_symbol": "SGOV",
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    summary = run_configured_plugins(config)

    assert summary["strategy_plugins"][0]["message"] == "route=risk_reduced action=delever"
    latest = json.loads((output_dir / "latest_signal.json").read_text(encoding="utf-8"))
    assert latest["canonical_route"] == "risk_reduced"
    assert latest["position_control"]["target_allocations"] == {"IBIT": 0.5, "SGOV": 0.5}


def test_strategy_plugin_runner_rejects_taco_rebound_for_non_tqqq_strategy(tmp_path) -> None:
    prices_path = tmp_path / "taco_prices.csv"
    output_dir = tmp_path / LEFT_SIDE_STRATEGY_NAME / "plugins" / PLUGIN_TACO_REBOUND_SHADOW
    _taco_rebound_prices().to_csv(prices_path, index=False)
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": LEFT_SIDE_STRATEGY_NAME,
                "plugin": PLUGIN_TACO_REBOUND_SHADOW,
                "enabled": True,
                "inputs": {
                    "prices": str(prices_path),
                    "event_set": "geopolitical-deescalation",
                    "as_of": "2026-04-02",
                    "start_date": "2026-03-20",
                },
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    with pytest.raises(ValueError, match="strategy-limited"):
        run_configured_plugins(config)

    assert not (output_dir / "latest_signal.json").exists()


def test_strategy_plugin_runner_can_skip_disabled_taco_notification_mount(tmp_path) -> None:
    output_dir = tmp_path / LEFT_SIDE_STRATEGY_NAME / "plugins" / PLUGIN_TACO_REBOUND_SHADOW
    config = {
        "output_dir": str(tmp_path / "runner"),
        "default_mode": "shadow",
        "strategy_plugins": [
            {
                "strategy": LEFT_SIDE_STRATEGY_NAME,
                "plugin": PLUGIN_TACO_REBOUND_SHADOW,
                "enabled": False,
                "outputs": {"output_dir": str(output_dir)},
            }
        ],
    }

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["strategy"] == LEFT_SIDE_STRATEGY_NAME
    assert result["plugin"] == PLUGIN_TACO_REBOUND_SHADOW
    assert result["status"] == "skipped"
    assert not (output_dir / "latest_signal.json").exists()


def test_strategy_plugin_runner_rejects_incompatible_plugin_strategy_mount(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"][0]["strategy"] = LEFT_SIDE_STRATEGY_NAME

    with pytest.raises(ValueError, match="strategy-limited"):
        run_configured_plugins(config)


@pytest.mark.parametrize("mode", ["paper", "advisory", "live", "broker_write"])
def test_strategy_plugin_runner_rejects_non_shadow_mode(tmp_path, mode: str) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"][0]["mode"] = mode

    with pytest.raises(ValueError, match="supports only configured modes shadow"):
        run_configured_plugins(config)


def test_strategy_plugin_runner_rejects_duplicate_plugin_config_keys(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"][0]["output_dir"] = str(tmp_path / "top_level")

    with pytest.raises(ValueError, match="duplicate strategy plugin config key.*output_dir"):
        run_configured_plugins(config)


def test_strategy_plugin_runner_cli_loads_toml_config(tmp_path) -> None:
    prices_path = tmp_path / "prices.csv"
    config_path = tmp_path / "plugins.toml"
    output_dir = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW
    _quiet_prices().to_csv(prices_path, index=False)
    config_path.write_text(
        f"""
output_dir = "{tmp_path / 'runner'}"
default_mode = "shadow"

[[strategy_plugins]]
strategy = "{STRATEGY_NAME}"
plugin = "{PLUGIN_CRISIS_RESPONSE_SHADOW}"
enabled = true
mode = "shadow"

[strategy_plugins.inputs]
prices = "{prices_path}"
as_of = "2025-11-19"
start_date = "2025-01-02"
financial_symbols = []
credit_pairs = []
rate_symbols = []

[strategy_plugins.outputs]
output_dir = "{output_dir}"
""".strip(),
        encoding="utf-8",
    )

    loaded = load_plugin_config(config_path)
    exit_code = main(["--config", str(config_path), "--strategies", STRATEGY_NAME])

    assert loaded["default_mode"] == "shadow"
    assert loaded["strategy_plugins"][0]["strategy"] == STRATEGY_NAME
    assert exit_code == 0
    assert (output_dir / "latest_signal.json").exists()


def test_strategy_plugin_runner_example_config_uses_default_mode_without_duplicate_entry_mode() -> None:
    config = load_plugin_config(Path("docs/examples/strategy_plugins.example.toml"))

    assert config["default_mode"] == "shadow"
    assert "mode" not in config["strategy_plugins"][0]
    assert config["strategy_plugins"][0]["outputs"]["output_dir"].endswith(
        "tqqq_growth_income/plugins/crisis_response_shadow"
    )
