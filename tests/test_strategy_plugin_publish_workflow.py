import re
from pathlib import Path


WORKFLOW = Path(".github/workflows/publish-strategy-plugins.yml")
PYPROJECT = Path("pyproject.toml")
MARKET_REGIME_PLUGIN_REF = "4c186d586238cc3d46b23d0e2a668af1ad44d9a3"


def test_strategy_plugin_publish_workflow_publishes_shadow_artifact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Publish Strategy Plugins" in workflow
    assert "cron: '30 22 * * 1-5'" in workflow
    assert "market-regime-control:" in workflow
    assert "strategy_profile: tqqq_growth_income" in workflow
    assert "strategy_profile: soxl_soxx_trend_income" in workflow
    assert "target_type: notification_target" in workflow
    assert "notification_target: market_regime_notification" in workflow
    assert "PLUGIN_TARGET_TYPE: ${{ matrix.target_type }}" in workflow
    assert "PLUGIN_NOTIFICATION_TARGET: ${{ matrix.notification_target }}" in workflow
    assert "PLUGIN_BENCHMARK_SYMBOL: ${{ matrix.benchmark_symbol }}" in workflow
    assert "PLUGIN_ATTACK_SYMBOL: ${{ matrix.attack_symbol }}" in workflow
    assert (
        "PLUGIN_VOLATILITY_DELEVER_PRICE_REBOUND_ENABLED: "
        "${{ matrix.volatility_delever_price_rebound_enabled }}"
    ) in workflow
    assert "INPUT_MARKET_REGIME_GCS_PREFIX: ${{ inputs.market_regime_gcs_prefix }}" in workflow
    assert "INPUT_MARKET_REGIME_SOXL_GCS_PREFIX: ${{ inputs.market_regime_soxl_gcs_prefix }}" in workflow
    assert (
        "INPUT_MARKET_REGIME_SOXL_STRATEGY_GCS_PREFIX: "
        "${{ inputs.market_regime_soxl_strategy_gcs_prefix }}"
    ) in workflow
    assert "PLUGIN_NAME: market_regime_control" in workflow
    assert "market_regime_control.v1" in workflow
    assert 'notification_target = "${PLUGIN_NOTIFICATION_TARGET}"' in workflow
    assert "--notification-targets" in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
        "tqqq_growth_income/plugins/market_regime_control"
    ) in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
        "market_regime_notification/plugins/market_regime_control"
    ) in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
        "soxl_soxx_trend_income/plugins/market_regime_control"
    ) in workflow
    assert "name: strategy-plugin-market-regime-control-${{ matrix.output_scope }}-${{ github.run_id }}" in workflow
    assert 'realized_vol_threshold = 0.30' in workflow
    assert 'realized_vol_requires_confirmation = true' in workflow
    assert 'delever_risk_asset_scalar = 0.0' in workflow
    assert 'taco_enabled = ${PLUGIN_TACO_ENABLED}' in workflow
    assert "volatility_delever_price_rebound_enabled: 'true'" in workflow
    assert "volatility_delever_price_rebound_enabled = ${PLUGIN_VOLATILITY_DELEVER_PRICE_REBOUND_ENABLED}" in workflow
    assert "position_control" in workflow
    assert "notification" in workflow
    assert "write_strategy_plugin_release_manifest" in workflow
    assert workflow.count("from us_equity_snapshot_pipelines.artifacts import normalize_strategy_plugin_gcs_prefix") == 3
    assert workflow.count("prefix = normalize_strategy_plugin_gcs_prefix") == 3
    assert workflow.count("Validated strategy plugin GCS prefix") == 3
    assert "GITHUB_RUN_ID" in workflow
    assert "GITHUB_SHA" in workflow
    assert "release_manifest.json" in workflow


def test_strategy_plugin_dependency_supports_market_regime_control() -> None:
    pyproject = PYPROJECT.read_text(encoding="utf-8")

    assert f"QuantStrategyPlugins.git@{MARKET_REGIME_PLUGIN_REF}" in pyproject
    assert "QuantStrategyPlugins.git@" + "v0.1.6" not in pyproject


def test_strategy_plugin_publish_workflow_keeps_legacy_artifact_jobs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "crisis-response-shadow:" in workflow
    assert "taco-rebound-shadow:" in workflow
    assert "PLUGIN_NAME: crisis_response_shadow" in workflow
    assert "PLUGIN_NAME: taco_rebound_shadow" in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
        "tqqq_growth_income/plugins/crisis_response_shadow"
    ) in workflow
    assert "INPUT_SOXL_GCS_PREFIX" not in workflow
    assert "soxl_soxx_trend_income/plugins/crisis_response_shadow" not in workflow
    assert 'benchmark_symbol = "${PLUGIN_BENCHMARK_SYMBOL}"' in workflow
    assert 'attack_symbol = "${PLUGIN_ATTACK_SYMBOL}"' in workflow
    assert 'default_mode = "shadow"' in workflow
    assert "python scripts/run_strategy_plugins.py" in workflow
    assert "gcloud storage cp" in workflow


def test_strategy_plugin_publish_workflow_uses_artifact_mode_not_platform_mode() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert re.search(r"^\s+mode = ", workflow, flags=re.MULTILINE) is None
    assert "effective_mode" in workflow
