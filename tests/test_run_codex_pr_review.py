from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_retired_local_review_script_is_absent() -> None:
    assert not (ROOT / "scripts/run_codex_pr_review.py").exists()


def test_codex_review_workflow_delegates_to_aiauditbridge() -> None:
    workflow = (ROOT / ".github/workflows/codex_pr_review.yml").read_text(encoding="utf-8")

    match = re.search(
        r"uses:\s*QuantStrategyLab/AIAuditBridge/\.github/workflows/codex_pr_review\.yml@([^\s#]+)",
        workflow,
    )
    assert match is not None
    assert match.group(1)
