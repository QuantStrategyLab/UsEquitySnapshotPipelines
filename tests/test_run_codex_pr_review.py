from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_retired_local_review_script_is_absent() -> None:
    assert not (ROOT / "scripts/run_codex_pr_review.py").exists()


def test_custom_codex_review_workflows_are_absent() -> None:
    assert not (ROOT / ".github/workflows/codex_pr_review.yml").exists()
    assert not (ROOT / ".github/workflows/codex_review_gate.yml").exists()
