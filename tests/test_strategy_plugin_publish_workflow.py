import re
from pathlib import Path


WORKFLOW = Path(".github/workflows/publish-strategy-plugins.yml")


def test_strategy_plugin_publish_workflow_publishes_shadow_artifact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Publish Strategy Plugins" in workflow
    assert "cron: '30 22 * * 1-5'" in workflow
    assert "market-regime-control:" in workflow
    assert "strategy_profile: tqqq_growth_income" in workflow
    assert "strategy_profile: soxl_soxx_trend_income" in workflow
    assert "PLUGIN_BENCHMARK_SYMBOL: ${{ matrix.benchmark_symbol }}" in workflow
    assert "PLUGIN_ATTACK_SYMBOL: ${{ matrix.attack_symbol }}" in workflow
    assert "INPUT_MARKET_REGIME_GCS_PREFIX: ${{ inputs.market_regime_gcs_prefix }}" in workflow
    assert "INPUT_MARKET_REGIME_SOXL_GCS_PREFIX: ${{ inputs.market_regime_soxl_gcs_prefix }}" in workflow
    assert "PLUGIN_NAME: market_regime_control" in workflow
    assert "market_regime_control.v1" in workflow
    assert (
        "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
        "tqqq_growth_income/plugins/market_regime_control"
    ) in workflow
    assert (
        "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
        "soxl_soxx_trend_income/plugins/market_regime_control"
    ) in workflow
    assert 'realized_vol_threshold = 0.30' in workflow
    assert 'realized_vol_requires_confirmation = true' in workflow
    assert 'delever_risk_asset_scalar = 0.0' in workflow
    assert 'taco_enabled = ${PLUGIN_TACO_ENABLED}' in workflow
    assert "position_control" in workflow
    assert "notification" in workflow
    assert "write_strategy_plugin_release_manifest" in workflow
    assert "GITHUB_RUN_ID" in workflow
    assert "GITHUB_SHA" in workflow
    assert "release_manifest.json" in workflow


def test_strategy_plugin_publish_workflow_keeps_legacy_artifact_jobs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "crisis-response-shadow:" in workflow
    assert "taco-rebound-shadow:" in workflow
    assert "INPUT_SOXL_GCS_PREFIX: ${{ inputs.soxl_gcs_prefix }}" in workflow
    assert "PLUGIN_NAME: crisis_response_shadow" in workflow
    assert "PLUGIN_NAME: taco_rebound_shadow" in workflow
    assert (
        "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
        "tqqq_growth_income/plugins/crisis_response_shadow"
    ) in workflow
    assert (
        "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
        "soxl_soxx_trend_income/plugins/crisis_response_shadow"
    ) in workflow
    assert 'benchmark_symbol = "${PLUGIN_BENCHMARK_SYMBOL}"' in workflow
    assert 'attack_symbol = "${PLUGIN_ATTACK_SYMBOL}"' in workflow
    assert 'default_mode = "shadow"' in workflow
    assert "python scripts/run_strategy_plugins.py" in workflow
    assert "gcloud storage cp" in workflow


def test_strategy_plugin_publish_workflow_uses_artifact_mode_not_platform_mode() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert re.search(r"^\s+mode = ", workflow, flags=re.MULTILINE) is None
    assert "effective_mode" in workflow
