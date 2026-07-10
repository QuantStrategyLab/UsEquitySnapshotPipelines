"""Workflow config tests for publish CI gating."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLISH_SNAPSHOT = ROOT / ".github/workflows/publish-snapshot-artifacts.yml"
PUBLISH_PLUGINS = ROOT / ".github/workflows/publish-strategy-plugins.yml"
VERIFY_SCRIPT = ROOT / ".github/scripts/verify_main_ci_success.sh"


def test_publish_snapshot_artifacts_requires_main_ci_before_publish() -> None:
    workflow = PUBLISH_SNAPSHOT.read_text(encoding="utf-8")

    assert "actions: read" in workflow
    assert "Verify main CI succeeded before publish" in workflow
    assert "bash .github/scripts/verify_main_ci_success.sh" in workflow
    assert "github.event_name == 'schedule'" in workflow
    assert "github.event_name == 'workflow_run'" in workflow
    assert "inputs.execute_publish == true" in workflow
    assert '--price-start "2022-01-01"' in workflow
    assert "Upload lifecycle market history" in workflow
    assert "us-equity-market-history-${{ github.run_id }}" in workflow
    assert "downloaded_price_history.csv" in workflow


def test_publish_strategy_plugins_uses_shared_ci_gate_job() -> None:
    workflow = PUBLISH_PLUGINS.read_text(encoding="utf-8")

    assert "verify-main-ci:" in workflow
    assert "needs: verify-main-ci" in workflow
    assert "needs.verify-main-ci.result == 'success' || needs.verify-main-ci.result == 'skipped'" in workflow
    assert workflow.count("bash .github/scripts/verify_main_ci_success.sh") >= 1


def test_verify_main_ci_success_script_checks_latest_main_run() -> None:
    script = VERIFY_SCRIPT.read_text(encoding="utf-8")

    assert 'workflow="${CI_WORKFLOW:-ci.yml}"' in script
    assert 'branch="${VERIFY_BRANCH:-main}"' in script
    assert "gh run list" in script
    assert 'conclusion}" != "success"' in script
