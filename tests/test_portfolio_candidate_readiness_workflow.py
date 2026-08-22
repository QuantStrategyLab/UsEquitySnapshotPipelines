from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/portfolio-candidate-readiness.yml")


def test_portfolio_readiness_workflow_is_scheduled_and_uses_only_sanitized_terminal_artifacts() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "cron: '35 11 * * 2-6'" in workflow
    assert "workflow_dispatch:" not in workflow
    assert "tqqq-p1-p3-daily-research.yml" in workflow
    assert "soxl-p1-p3-daily-research.yml" in workflow
    assert "--event schedule" in workflow
    assert "tqqq-p1-terminal-*" in workflow
    assert "soxl-p1-terminal-*" in workflow
    assert "tqqq-p3-terminal-*" in workflow
    assert "soxl-p3-terminal-*" in workflow
    assert "build_portfolio_candidate_readiness.py" in workflow
    assert "portfolio-candidate-readiness-${{ github.run_id }}-${{ github.run_attempt }}" in workflow
    assert "AI_RESEARCH_PROPOSAL_READY" in workflow
    assert "qsl-portfolio-candidate-readiness:" in workflow
    assert "ALPACA_API_KEY_ID" not in workflow
    assert "ALPACA_API_SECRET_KEY" not in workflow
    assert "gcloud storage" not in workflow
    assert "broker" not in workflow.lower()
    assert "placeorder" not in workflow.lower()
