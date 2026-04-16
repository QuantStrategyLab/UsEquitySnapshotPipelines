import re
from pathlib import Path


WORKFLOW = Path(".github/workflows/publish-strategy-plugins.yml")


def test_strategy_plugin_publish_workflow_publishes_shadow_artifact() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Publish Strategy Plugins" in workflow
    assert "cron: '30 22 * * 1-5'" in workflow
    assert "STRATEGY_PROFILE: tqqq_growth_income" in workflow
    assert "PLUGIN_NAME: crisis_response_shadow" in workflow
    assert (
        "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
        "tqqq_growth_income/plugins/crisis_response_shadow"
    ) in workflow
    assert 'default_mode = "shadow"' in workflow
    assert "python scripts/run_strategy_plugins.py" in workflow
    assert "gcloud storage cp" in workflow


def test_strategy_plugin_publish_workflow_uses_artifact_mode_not_platform_mode() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert re.search(r"^\s+mode = ", workflow, flags=re.MULTILINE) is None
    assert "effective_mode" in workflow
