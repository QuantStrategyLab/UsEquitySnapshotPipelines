import json
import re
import sys
from importlib.metadata import distribution
from types import SimpleNamespace

import pandas as pd
import pytest
from pathlib import Path

WORKFLOW = Path(".github/workflows/publish-strategy-plugins.yml")
RUSSELL_WORKFLOW = Path(".github/workflows/run-russell-live-ledger.yml")
PYPROJECT = Path("pyproject.toml")
ALERT_MODULE = Path("src/us_equity_snapshot_pipelines/strategy_plugin_alerts.py")
MARKET_REGIME_PLUGIN_REF = "8e6333f8c829748d7dfea4275dbd4cf963f7ffa0"


def test_strategy_plugin_publish_workflow_publishes_shadow_artifact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Publish Strategy Plugins" in workflow
    assert "verify-main-ci:" in workflow
    assert "cron: '31 22 * * 1-5'" in workflow
    assert "cron: '30 22 * * 1-5'" not in workflow
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
    assert "market regime strategy artifact must remain notification-only until promotion evidence passes" in workflow
    assert "market regime artifact must be mountable as strategy metadata" not in workflow
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

    qpk_refs = re.findall(r"QuantPlatformKit\.git@([0-9a-f]{40})", pyproject)
    assert len(qpk_refs) == 1
    strategy_refs = re.findall(r"UsEquityStrategies\.git@([0-9a-f]{40})", pyproject)
    assert len(strategy_refs) == 1
    assert f"QuantStrategyPlugins.git@{MARKET_REGIME_PLUGIN_REF}" in pyproject
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
    assert "IBIT zscore artifact must remain notification-only until promotion evidence passes" in workflow
    assert "IBIT zscore artifact must be mountable as strategy metadata" not in workflow
    assert (
        "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/ibit_smart_dca/plugins/ibit_zscore_exit"
    ) in workflow
    assert "name: strategy-plugin-ibit-zscore-exit-${{ github.run_id }}" in workflow
    assert "${{ env.RESEARCH_OUTPUT_DIR }}" in workflow


def test_strategy_plugin_publish_workflow_uses_artifact_mode_not_platform_mode() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert re.search(r"^\s+mode = ", workflow, flags=re.MULTILINE) is None
    assert "effective_mode" in workflow


def test_strategy_plugin_alert_state_settings_uses_qpk_project_id_keyword() -> None:
    alert_module = ALERT_MODULE.read_text(encoding="utf-8")

    assert "StrategyPluginAlertStateSettings.from_env(" in alert_module
    assert "project_id=resolved_env.get(\"GCP_PROJECT_ID\")" in alert_module
    assert "gcp_project_id=" not in alert_module


def test_russell_live_ledger_workflow_upload_artifact_guard() -> None:
    workflow = RUSSELL_WORKFLOW.read_text(encoding="utf-8")

    assert "uses: actions/upload-artifact@v4" in workflow
    assert "if-no-files-found: error" in workflow
    assert "retention-days: 7" in workflow


def test_installed_strategy_plugin_revision_matches_manifest() -> None:
    source = json.loads(distribution("quant-strategy-plugins").read_text("direct_url.json"))
    assert source["vcs_info"]["commit_id"] == MARKET_REGIME_PLUGIN_REF


@pytest.mark.parametrize("plugin", ["crisis", "taco"])
@pytest.mark.parametrize("status,confidence", [
    ("ok", "nan"), ("ok", "inf"), ("ok", "-inf"), ("ok", None),
    ("advisory", 0.8), ("ok", 0.8),
])
def test_installed_ai_audit_keeps_consumer_routing_and_feedback_boundaries(monkeypatch, plugin, status, confidence):
    from quant_strategy_plugins import ai_audit
    from us_equity_snapshot_pipelines.research.crisis_response_shadow_plugin import build_crisis_response_shadow_signal
    from us_equity_snapshot_pipelines.research.taco_rebound_shadow_plugin import build_taco_rebound_shadow_signal

    output = json.dumps({
        "verdict": "review", "confidence": confidence, "summary": "synthetic opinion",
        "mode": "live", "final_route_unchanged": False,
        "execution_controls": {"broker_order_allowed": True},
    })
    calls = []

    class Gateway:
        def __init__(self, _config):
            pass

        def analyze(self, *_args, **_kwargs):
            calls.append("analyze")
            return SimpleNamespace(
                success=status == "ok", provider="openai", model="synthetic-model",
                output=output, note="advisory" if status == "advisory" else "", error="",
                raw={"status": status, "policy_verdict": "advisory" if status == "advisory" else "eligible",
                     "output": output},
            )

        def execute(self, *_args, **_kwargs):
            raise AssertionError("research audit must not execute")

    monkeypatch.setitem(sys.modules, "ai_gateway_client", SimpleNamespace(
        AiGatewayClient=Gateway, GatewayConfig=SimpleNamespace(from_env=lambda: object()),
    ))
    monkeypatch.setenv("CODEX_AUDIT_SERVICE_URL", "https://gateway.invalid")
    monkeypatch.setattr(ai_audit, "build_ai_audit_endpoints", lambda **_: (
        ai_audit.AiAuditEndpoint("primary", "", model="synthetic-model"),
    ))
    feedback = []
    monkeypatch.setattr(ai_audit, "_report_shadow_disagreement", lambda **fields: feedback.append(fields))
    dates = pd.bdate_range("2025-01-02", periods=230)
    prices = pd.DataFrame([
        {"symbol": symbol, "as_of": date, "close": 100.0 + offset * 0.01, "volume": 1000}
        for symbol in ("QQQ", "TQQQ", "SPY") for offset, date in enumerate(dates)
    ])
    build = build_crisis_response_shadow_signal if plugin == "crisis" else build_taco_rebound_shadow_signal
    options = {"events": (), "as_of": str(dates[-1].date()), "start_date": "2025-01-02"}
    original = build(prices, **options)
    assert calls == []  # The existing default remains AI-disabled.
    payload = build(prices, **options, ai_audit_enabled=True, ai_audit_codex_enabled=False)
    audit = payload["ai_audit"]
    assert audit["status"] == status
    assert audit["confidence"] == (0.8 if confidence == 0.8 else None)
    assert calls == ["analyze"]
    assert len(feedback) == (1 if status == "ok" and confidence == 0.8 else 0)
    assert audit["final_route_unchanged"] is True
    assert audit["mode"] == "shadow_only"
    for key in ("broker_order_allowed", "live_allocation_mutation_allowed", "allocation_recommendation_allowed"):
        assert audit["execution_controls"][key] is False
    for key in ("canonical_route", "suggested_action", "risk_multiplier_suggestion", "would_trade_if_enabled"):
        assert payload.get(key) == original.get(key)
    for key in ("broker_order_allowed", "live_allocation_mutation_allowed"):
        assert payload["execution_controls"][key] is False
    json.dumps(audit, allow_nan=False)
