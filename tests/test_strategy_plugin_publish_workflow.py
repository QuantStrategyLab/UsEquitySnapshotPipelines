import re
from pathlib import Path


WORKFLOW = Path(".github/workflows/publish-strategy-plugins.yml")
PYPROJECT = Path("pyproject.toml")
ALERT_MODULE = Path("src/us_equity_snapshot_pipelines/strategy_plugin_alerts.py")
QUANT_PLATFORM_KIT_REF = "aee8121d530c2e92c72b68aee434bf174b3b9c85"
MARKET_REGIME_PLUGIN_REF = "eedaa71de8472448c4665b8b7b3be679fe7db83d"
US_EQUITY_STRATEGIES_REF = "b2fa659304c02cc19f7c82e86b0ce36ef592846a"


def test_strategy_plugin_publish_workflow_publishes_shadow_artifact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Publish Strategy Plugins" in workflow
    assert "verify-main-ci:" in workflow
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
        "PLUGIN_VOLATILITY_DELEVER_PRICE_REBOUND_ENABLED: ${{ matrix.volatility_delever_price_rebound_enabled }}"
    ) in workflow
    assert "panic_reversal_enabled: 'true'" in workflow
    assert "PLUGIN_PANIC_REVERSAL_ENABLED: ${{ matrix.panic_reversal_enabled }}" in workflow
    assert "INPUT_MARKET_REGIME_GCS_PREFIX: ${{ inputs.market_regime_gcs_prefix }}" in workflow
    assert "INPUT_MARKET_REGIME_SOXL_GCS_PREFIX: ${{ inputs.market_regime_soxl_gcs_prefix }}" in workflow
    assert (
        "INPUT_MARKET_REGIME_SOXL_STRATEGY_GCS_PREFIX: ${{ inputs.market_regime_soxl_strategy_gcs_prefix }}"
    ) in workflow
    assert "PLUGIN_NAME: market_regime_control" in workflow
    assert "market_regime_control.v1" in workflow
    assert 'notification_target = "${PLUGIN_NOTIFICATION_TARGET}"' in workflow
    assert "--notification-targets" in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/tqqq_growth_income/plugins/market_regime_control"
    ) in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
        "market_regime_notification/plugins/market_regime_control"
    ) in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/soxl_soxx_trend_income/plugins/market_regime_control"
    ) in workflow
    assert "name: strategy-plugin-market-regime-control-${{ matrix.output_scope }}-${{ github.run_id }}" in workflow
    assert "realized_vol_threshold = 0.30" in workflow
    assert "realized_vol_requires_confirmation = true" in workflow
    assert "delever_risk_asset_scalar = 0.0" in workflow
    assert "taco_enabled = ${PLUGIN_TACO_ENABLED}" in workflow
    assert "panic_reversal_enabled = ${PLUGIN_PANIC_REVERSAL_ENABLED}" in workflow
    assert "volatility_delever_price_rebound_enabled: 'true'" in workflow
    assert "volatility_delever_price_rebound_enabled = ${PLUGIN_VOLATILITY_DELEVER_PRICE_REBOUND_ENABLED}" in workflow
    assert "position_control" in workflow
    assert "notification" in workflow
    assert "write_strategy_plugin_release_manifest" in workflow
    assert (
        workflow.count("from us_equity_snapshot_pipelines.artifacts import normalize_strategy_plugin_gcs_prefix") == 4
    )
    assert workflow.count("prefix = normalize_strategy_plugin_gcs_prefix") == 4
    assert workflow.count("Validated strategy plugin GCS prefix") == 4
    assert "GITHUB_RUN_ID" in workflow
    assert "GITHUB_SHA" in workflow
    assert "release_manifest.json" in workflow
    assert "Publish unified notification-target alert" not in workflow
    assert "publish-market-regime-alerts:" in workflow
    assert "needs: market-regime-control" in workflow
    assert "Download market-regime artifacts" in workflow
    assert "actions/download-artifact@v7" in workflow
    assert "pattern: strategy-plugin-market-regime-control-*-${{ github.run_id }}" in workflow
    assert "merge-multiple: true" in workflow
    assert "PLUGIN_ALERT_OUTPUT_DIR: data/output/market_regime_alerts" in workflow
    assert "PLUGIN_ALERT_SIGNAL_GLOB: data/output/**/plugins/market_regime_control/latest_signal.json" in workflow
    assert "Publish consolidated market-regime alert" in workflow
    assert "strategy-plugin-market-regime-alerts-${{ github.run_id }}" in workflow
    assert "STRATEGY_PLUGIN_ALERT_LANG: ${{ vars.STRATEGY_PLUGIN_ALERT_LANG || 'zh' }}" in workflow
    assert "STRATEGY_PLUGIN_ALERT_STATE_GCS_URI" in workflow
    assert "python -m us_equity_snapshot_pipelines.strategy_plugin_alerts" in workflow
    alert_module = ALERT_MODULE.read_text(encoding="utf-8")
    assert "unified_alert_result.json" in alert_module
    assert "PLUGIN_ALERT_SIGNAL_GLOB" in alert_module


def test_strategy_plugin_dependency_supports_market_regime_control() -> None:
    pyproject = PYPROJECT.read_text(encoding="utf-8")

    assert f"QuantPlatformKit.git@{QUANT_PLATFORM_KIT_REF}" in pyproject
    assert f"QuantStrategyPlugins.git@{MARKET_REGIME_PLUGIN_REF}" in pyproject
    assert f"UsEquityStrategies.git@{US_EQUITY_STRATEGIES_REF}" in pyproject
    assert "google-cloud-storage>=2.18" in pyproject
    assert "QuantStrategyPlugins.git@" + "v0.1.6" not in pyproject


def test_strategy_plugin_publish_workflow_keeps_legacy_artifact_jobs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "crisis-response-shadow:" in workflow
    assert "taco-rebound-shadow:" in workflow
    assert "PLUGIN_NAME: crisis_response_shadow" in workflow
    assert "PLUGIN_NAME: taco_rebound_shadow" in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/tqqq_growth_income/plugins/crisis_response_shadow"
    ) in workflow
    assert "INPUT_SOXL_GCS_PREFIX" not in workflow
    assert "soxl_soxx_trend_income/plugins/crisis_response_shadow" not in workflow
    assert 'benchmark_symbol = "${PLUGIN_BENCHMARK_SYMBOL}"' in workflow
    assert 'attack_symbol = "${PLUGIN_ATTACK_SYMBOL}"' in workflow
    assert 'default_mode = "shadow"' in workflow
    assert "python scripts/run_strategy_plugins.py" in workflow
    assert "gcloud storage cp" in workflow


def test_strategy_plugin_publish_workflow_publishes_ibit_zscore_exit_artifact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "ibit-zscore-exit:" in workflow
    assert "ibit_zscore_gcs_prefix:" in workflow
    assert "ibit_zscore_metrics_url:" in workflow
    assert "ibit_zscore_metrics_urls:" in workflow
    assert "PLUGIN_NAME: ibit_zscore_exit" in workflow
    assert "STRATEGY_PROFILE: ibit_smart_dca" in workflow
    assert "IBIT_ZSCORE_METRICS_URLS" in workflow
    assert "ZSCORE_METRICS_URLS" in workflow
    assert "IBIT_ZSCORE_METRICS_QUERY_TOKEN" in workflow
    assert "IBIT_ZSCORE_METRICS_BEARER_TOKEN" in workflow
    assert "BGEOMETRICS_API_TOKEN" in workflow
    assert "IBIT_ZSCORE_METRICS_PROXY" in workflow
    assert "IBIT_ZSCORE_METRICS_PUBLIC_PROXIES" in workflow
    assert "IBIT_ZSCORE_METRICS_ALLOW_PUBLIC_PROXY" in workflow
    assert "IBIT_ZSCORE_METRICS_MIN_ROWS" in workflow
    assert "IBIT_ZSCORE_METRICS_MAX_AGE_DAYS" in workflow
    assert "IBIT_ZSCORE_METRICS_MAX_FALLBACK_AGE_DAYS" in workflow
    assert "IBIT_ZSCORE_METRICS_MAX_GAP_DAYS" in workflow
    assert "IBIT_ZSCORE_METRICS_MAX_ABS_ZSCORE" in workflow
    assert "IBIT_ZSCORE_METRICS_MAX_DAILY_ZSCORE_CHANGE" in workflow
    assert "Restore last-good IBIT zscore metrics cache" in workflow
    assert "IBIT_ZSCORE_METRICS_FALLBACK_CSV" in workflow
    assert "ibit_zscore_metrics_download.json" in workflow
    assert "IBIT zscore metrics used last-good fallback cache" in workflow
    assert "YFINANCE_PROXY: ${{ secrets.YFINANCE_PROXY }}" in workflow
    assert "/inputs/ibit_zscore_metrics.csv" in workflow
    assert "https://api.bitcoin-data.com/v1/mvrv-zscore" in workflow
    assert "newhedge.io" not in workflow
    assert "scripts/download_ibit_zscore_metrics.py" in workflow
    assert "Build IBIT DCA research artifact" in workflow
    assert "scripts/build_scheduled_ibit_dca_research.py" in workflow
    assert "--parking-proxy-symbol BIL" in workflow
    assert "--price-field adjusted_close" in workflow
    assert "RESEARCH_OUTPUT_DIR: data/output/ibit_smart_dca/research/ibit_dca" in workflow
    assert "ibit_dca_research_manifest.json" in workflow
    assert "ibit_dca_research_report.md" in workflow
    assert "ibit_dca_live_readiness_summary.csv" in workflow
    assert 'zscore_metrics = "${zscore_path}"' in workflow
    assert "ibit_zscore_exit.v1" in workflow
    assert "notification_only" in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/ibit_smart_dca/plugins/ibit_zscore_exit"
    ) in workflow
    assert "name: strategy-plugin-ibit-zscore-exit-${{ github.run_id }}" in workflow
    assert "${{ env.RESEARCH_OUTPUT_DIR }}" in workflow


def test_strategy_plugin_publish_workflow_uses_artifact_mode_not_platform_mode() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert re.search(r"^\s+mode = ", workflow, flags=re.MULTILINE) is None
    assert "effective_mode" in workflow
