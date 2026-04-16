from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from us_equity_snapshot_pipelines.strategy_plugin_runner import (
    PLUGIN_CRISIS_RESPONSE_SHADOW,
    PLUGIN_MODE_ADVISORY,
    PLUGIN_MODE_LIVE,
    PLUGIN_MODE_PAPER,
    load_plugin_config,
    main,
    run_configured_plugins,
)

STRATEGY_NAME = "tqqq_growth_income"


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
    assert payload["execution_controls"]["paper_ledger_required"] is False
    assert payload["execution_controls"]["human_confirmation_required"] is False
    assert payload["execution_controls"]["risk_controls_required"] is False
    assert payload["execution_controls"]["notification_profile"] == "shadow_only"
    assert payload["execution_controls"]["repository_broker_write_allowed"] is False
    assert payload["execution_controls"]["repository_allocation_mutation_allowed"] is False
    assert "platform behavior contract" in payload["execution_controls"]["mode_note"]


def test_strategy_plugin_runner_defaults_output_under_strategy_plugin_scope(tmp_path, monkeypatch) -> None:
    config = _shadow_plugin_config(tmp_path, include_output_dir=False)
    monkeypatch.chdir(tmp_path)

    summary = run_configured_plugins(config)

    expected = tmp_path / "data" / "output" / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW
    assert summary["strategy_plugins"][0]["output_dir"] == str(Path("data/output") / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW)
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
    config["default_mode"] = PLUGIN_MODE_PAPER

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["mode"] == PLUGIN_MODE_PAPER
    assert result["effective_mode"] == PLUGIN_MODE_PAPER
    latest_signal = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json"
    payload = json.loads(latest_signal.read_text(encoding="utf-8"))
    assert payload["strategy"] == STRATEGY_NAME
    assert payload["plugin"] == PLUGIN_CRISIS_RESPONSE_SHADOW
    assert payload["configured_mode"] == PLUGIN_MODE_PAPER
    assert payload["execution_controls"]["paper_ledger_required"] is True


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
    assert not (tmp_path / "soxl_growth_income" / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json").exists()


@pytest.mark.parametrize(
    ("mode", "expected_controls"),
    [
        (
            PLUGIN_MODE_PAPER,
            {
                "capital_impact": "none",
                "broker_order_allowed": False,
                "live_allocation_mutation_allowed": False,
                "paper_ledger_required": True,
                "human_confirmation_required": False,
                "risk_controls_required": False,
                "notification_profile": "paper",
            },
        ),
        (
            PLUGIN_MODE_ADVISORY,
            {
                "capital_impact": "manual_only",
                "broker_order_allowed": False,
                "live_allocation_mutation_allowed": False,
                "paper_ledger_required": False,
                "human_confirmation_required": True,
                "risk_controls_required": False,
                "notification_profile": "advisory",
            },
        ),
        (
            PLUGIN_MODE_LIVE,
            {
                "capital_impact": "bounded_by_platform_policy",
                "broker_order_allowed": True,
                "live_allocation_mutation_allowed": True,
                "paper_ledger_required": False,
                "human_confirmation_required": False,
                "risk_controls_required": True,
                "notification_profile": "live",
            },
        ),
    ],
)
def test_strategy_plugin_runner_modes_set_unified_execution_contract(
    tmp_path, mode: str, expected_controls: dict[str, object]
) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"][0]["mode"] = mode

    summary = run_configured_plugins(config)

    result = summary["strategy_plugins"][0]
    assert result["mode"] == mode
    assert result["effective_mode"] == mode
    latest_signal = tmp_path / STRATEGY_NAME / "plugins" / PLUGIN_CRISIS_RESPONSE_SHADOW / "latest_signal.json"
    payload = json.loads(latest_signal.read_text(encoding="utf-8"))
    assert payload["configured_mode"] == mode
    assert payload["effective_mode"] == mode
    assert payload["mode"] == mode
    for key, expected in expected_controls.items():
        assert payload["execution_controls"][key] == expected
    assert payload["execution_controls"]["repository_broker_write_allowed"] is False
    assert payload["execution_controls"]["repository_allocation_mutation_allowed"] is False
    assert "platform behavior contract" in payload["execution_controls"]["mode_note"]


def test_strategy_plugin_runner_rejects_unknown_mode(tmp_path) -> None:
    config = _shadow_plugin_config(tmp_path)
    config["strategy_plugins"][0]["mode"] = "broker_write"

    with pytest.raises(ValueError, match="supports only configured modes"):
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
