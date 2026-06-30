from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.research.ai_rule_proposal import build_rule_spec_from_ai_proposal


def test_ai_rule_proposal_converts_only_to_transparent_rule_spec() -> None:
    spec = build_rule_spec_from_ai_proposal(
        {
            "proposal_id": "memory_semiconductor_momentum_v1",
            "hypothesis": "test",
            "suggested_hard_gates": {
                "min_trading_days": 252,
                "min_month_end_closes": 13,
            },
            "suggested_metrics": ["momentum_score", "ret_63d", "vol_63"],
        },
        universe_id="memory_semiconductor_watchlist",
    )

    assert spec.rule_id == "memory_semiconductor_momentum_v1"
    assert tuple(gate.metric for gate in spec.hard_gates) == ("trading_days", "month_end_closes")
    assert tuple(term.metric for term in spec.score_terms) == ("momentum_score", "ret_63d", "vol_63")
    assert spec.score_terms[-1].higher_is_better is False


def test_ai_rule_proposal_rejects_unknown_metrics() -> None:
    with pytest.raises(ValueError, match="unsupported metrics"):
        build_rule_spec_from_ai_proposal(
            {
                "proposal_id": "bad_rule",
                "suggested_hard_gates": {"min_trading_days": 252},
                "suggested_metrics": ["future_return_6m"],
            },
            universe_id="memory_semiconductor_watchlist",
        )


def test_ai_rule_proposal_rejects_unknown_hard_gate() -> None:
    with pytest.raises(ValueError, match="unknown AI hard gate"):
        build_rule_spec_from_ai_proposal(
            {
                "proposal_id": "bad_rule",
                "suggested_hard_gates": {"best_recent_winner": True},
                "suggested_metrics": ["momentum_score"],
            },
            universe_id="memory_semiconductor_watchlist",
        )
