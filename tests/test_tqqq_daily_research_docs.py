from __future__ import annotations

from pathlib import Path


def test_validation_policy_distinguishes_current_daily_v5_from_legacy_manual_v1() -> None:
    policy = Path("docs/tqqq-validation-and-shadow-policy.md").read_text(encoding="utf-8")
    contract = Path("docs/tqqq-p2-v5-daily-research.md").read_text(encoding="utf-8")

    assert "### Current P2 v5 scheduled research path" in policy
    assert "### Legacy manual v1 compatibility path" in policy
    assert "P4 paper, P5 shadow, broker orders, capital, and" in policy
    assert "P6 live remain unavailable." in policy
    assert "P4--P6 authority" in policy
    assert "current\nmain branch now adds the separately reviewed daily controller" in contract
    assert ".github/workflows/tqqq-p1-p3-daily-research.yml" in contract
