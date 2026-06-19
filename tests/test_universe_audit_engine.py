from __future__ import annotations

import pandas as pd

from us_equity_snapshot_pipelines.universe_audit_contracts import (
    ELIGIBLE_FOR_RESEARCH_RANKING,
    OBSERVE_UNTIL_SEASONED,
    ScoreTermSpec,
    SeasoningRule,
    SelectionRuleSpec,
    SymbolSpec,
)
from us_equity_snapshot_pipelines.universe_audit_engine import run_universe_audit


def _prices() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    seasoned_dates = pd.bdate_range("2025-01-02", periods=280)
    young_dates = pd.bdate_range("2026-04-02", periods=45)
    for idx, date in enumerate(seasoned_dates):
        rows.append({"symbol": "SMH", "as_of": date, "close": 100 + idx, "volume": 1_000_000})
    for idx, date in enumerate(young_dates):
        rows.append({"symbol": "DRAM", "as_of": date, "close": 40 + idx, "volume": 1_000_000})
    return pd.DataFrame(rows)


def _rule() -> SelectionRuleSpec:
    return SelectionRuleSpec(
        rule_id="test_rule",
        rule_version="v1",
        universe_id="test_universe",
        hard_gates=SeasoningRule(min_trading_days=252, min_month_end_closes=13).to_hard_gates(),
        score_terms=(ScoreTermSpec("momentum_score", 1.0),),
    )


def test_universe_audit_keeps_unseasoned_etf_out_of_ranking() -> None:
    result = run_universe_audit(
        _prices(),
        specs=(
            SymbolSpec("DRAM", "memory_etf", True),
            SymbolSpec("SMH", "semiconductor_proxy_etf", True),
        ),
        rule_spec=_rule(),
    )

    snapshot = result.candidate_snapshot.set_index("symbol")
    assert snapshot.loc["DRAM", "research_action"] == OBSERVE_UNTIL_SEASONED
    assert snapshot.loc["SMH", "research_action"] == ELIGIBLE_FOR_RESEARCH_RANKING
    assert tuple(result.ranking["symbol"]) == ("SMH",)
    assert set(result.gate_results["gate_name"]) == {"min_trading_days", "min_month_end_closes"}


def test_universe_audit_explains_tracker_only_symbols() -> None:
    result = run_universe_audit(
        _prices(),
        specs=(SymbolSpec("SMH", "memory_stock_tracker", False),),
        rule_spec=_rule(),
    )

    assert result.ranking.empty
    decision = result.promotion_decisions[0]
    assert decision.research_action == "tracker_only"
    assert "eligible_for_trading" in decision.failed_gates
