"""Workflow config tests for publish CI gating."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLISH_SNAPSHOT = ROOT / ".github/workflows/publish-snapshot-artifacts.yml"
PUBLISH_PLUGINS = ROOT / ".github/workflows/publish-strategy-plugins.yml"
SHARED_CI_CHECK = r"uses: QuantStrategyLab/\.github/actions/verify-main-ci@[0-9a-f]{40}"


def test_publish_snapshot_artifacts_requires_main_ci_before_publish() -> None:
    workflow = PUBLISH_SNAPSHOT.read_text(encoding="utf-8")

    assert "actions: read" in workflow
    assert "Verify main CI succeeded before publish" in workflow
    assert re.search(SHARED_CI_CHECK, workflow)
    assert "github.event_name == 'schedule'" in workflow
    assert "github.event_name == 'workflow_run'" in workflow
    assert "inputs.execute_publish == true" in workflow
    assert '--price-start "2022-01-01"' in workflow
    assert "Upload lifecycle market history" in workflow
    assert "SOXL,SOXX,BOXX" in workflow
    assert workflow.index("Validate lifecycle market history") < workflow.index("Upload generated artifacts")
    assert workflow.index("Validate lifecycle market history") < workflow.index("Publish or print GCS plan")
    assert "us-equity-market-history-${{ github.run_id }}-${{ github.run_attempt }}" in workflow
    assert "downloaded_price_history.csv" in workflow
    assert '--extra-history-symbols "QQQ,AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AVGO,SOXL,SOXX,BOXX"' in workflow


def test_publish_strategy_plugins_uses_shared_ci_gate_job() -> None:
    workflow = PUBLISH_PLUGINS.read_text(encoding="utf-8")

    assert "verify-main-ci:" in workflow
    assert "needs: verify-main-ci" in workflow
    assert "needs.verify-main-ci.result == 'success' || needs.verify-main-ci.result == 'skipped'" in workflow
    assert re.search(SHARED_CI_CHECK, workflow)
