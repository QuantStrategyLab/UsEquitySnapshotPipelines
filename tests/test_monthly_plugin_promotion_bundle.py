from __future__ import annotations

import json
from pathlib import Path

from scripts.run_monthly_report_bundle import build_bundle, render_ai_review_input, render_job_summary
from us_equity_snapshot_pipelines.contracts import list_scheduled_profile_contracts

from tests.test_monthly_report_bundle import _write_json, _write_profile_artifacts


def test_build_bundle_collects_plugin_promotion_review_manifest(tmp_path: Path) -> None:
    for contract in list_scheduled_profile_contracts():
        _write_profile_artifacts(tmp_path, contract.profile)
    manifest_path = tmp_path / "plugin_review_ibit" / "plugin_promotion_review_manifest.json"
    _write_json(
        manifest_path,
        {
            "manifest_type": "strategy_plugin_promotion_review",
            "artifact_schema_version": "strategy_plugin_promotion_review.v1",
            "strategy": "ibit_smart_dca",
            "plugin": "ibit_zscore_exit",
            "plugin_role": "notification_only",
            "policy_evidence_status": "notification_only",
            "row_count": 1,
            "replace_live_component_now_count": 0,
            "inputs": {
                "source_research_manifest": {"path": "ibit_dca_research_manifest.json"},
            },
            "artifacts": {
                "plugin_promotion_review_csv": {"path": "plugin_promotion_review.csv"},
                "plugin_promotion_review_md": {"path": "plugin_promotion_review.md"},
            },
            "review_rows": [
                {
                    "strategy": "ibit_smart_dca",
                    "plugin": "ibit_zscore_exit",
                    "required_gates_passed": True,
                    "replace_live_component_now": False,
                    "blocking_reason": "policy_still_notification_only",
                    "recommended_action": "prepare_separate_promotion_artifact",
                }
            ],
        },
    )

    bundle = build_bundle(tmp_path, report_month="2026-06")
    markdown = render_ai_review_input(bundle)
    summary = render_job_summary(bundle)

    assert bundle["plugin_promotion_review_count"] == 1
    assert bundle["plugin_promotion_review_problem_count"] == 0
    assert bundle["plugin_promotion_reviews"][0]["plugin"] == "ibit_zscore_exit"
    assert "Plugin Promotion Reviews" in markdown
    assert "prepare_separate_promotion_artifact" in markdown
    assert "Plugin promotion reviews: `1`" in summary
