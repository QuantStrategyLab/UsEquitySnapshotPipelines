from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any


WATCHLIST = "watchlist"
RESEARCH_CANDIDATE = "research_candidate"
BACKTEST_PASSED = "backtest_passed"
PAPER_SHADOW = "paper_shadow"
SMALL_LIVE = "small_live"
LIVE_POOL = "live_pool"
REJECTED = "rejected"
TRACKER_ONLY = "tracker_only"
BENCHMARK_ONLY = "benchmark_only"
MISSING_DATA = "missing_data"
OBSERVE_UNTIL_SEASONED = "observe_until_seasoned"
ELIGIBLE_FOR_RESEARCH_RANKING = "eligible_for_research_ranking"

PROMOTION_STATES = frozenset(
    {
        WATCHLIST,
        RESEARCH_CANDIDATE,
        BACKTEST_PASSED,
        PAPER_SHADOW,
        SMALL_LIVE,
        LIVE_POOL,
        REJECTED,
    }
)


@dataclass(frozen=True)
class SymbolSpec:
    symbol: str
    role: str
    eligible_for_trading: bool

    def normalized(self) -> "SymbolSpec":
        return SymbolSpec(
            symbol=str(self.symbol or "").strip().upper(),
            role=str(self.role or "").strip().lower() or "candidate",
            eligible_for_trading=bool(self.eligible_for_trading),
        )


@dataclass(frozen=True)
class HardGateSpec:
    name: str
    metric: str
    operator: str
    threshold: float | int | bool | str
    failure_action: str = WATCHLIST
    description: str = ""

    def normalized(self) -> "HardGateSpec":
        return HardGateSpec(
            name=str(self.name or "").strip(),
            metric=str(self.metric or "").strip(),
            operator=str(self.operator or "").strip(),
            threshold=self.threshold,
            failure_action=str(self.failure_action or WATCHLIST).strip() or WATCHLIST,
            description=str(self.description or "").strip(),
        )


@dataclass(frozen=True)
class ScoreTermSpec:
    metric: str
    weight: float
    higher_is_better: bool = True
    description: str = ""

    def normalized(self) -> "ScoreTermSpec":
        return ScoreTermSpec(
            metric=str(self.metric or "").strip(),
            weight=float(self.weight),
            higher_is_better=bool(self.higher_is_better),
            description=str(self.description or "").strip(),
        )


@dataclass(frozen=True)
class SelectionRuleSpec:
    rule_id: str
    rule_version: str
    universe_id: str
    hard_gates: tuple[HardGateSpec, ...]
    score_terms: tuple[ScoreTermSpec, ...]
    benchmark_symbols: tuple[str, ...] = ()
    notes: str = ""
    ai_proposal_id: str | None = None

    def normalized(self) -> "SelectionRuleSpec":
        return SelectionRuleSpec(
            rule_id=str(self.rule_id or "").strip(),
            rule_version=str(self.rule_version or "").strip(),
            universe_id=str(self.universe_id or "").strip(),
            hard_gates=tuple(gate.normalized() for gate in self.hard_gates),
            score_terms=tuple(term.normalized() for term in self.score_terms),
            benchmark_symbols=tuple(
                dict.fromkeys(str(symbol or "").strip().upper() for symbol in self.benchmark_symbols if str(symbol or "").strip())
            ),
            notes=str(self.notes or "").strip(),
            ai_proposal_id=str(self.ai_proposal_id).strip() if self.ai_proposal_id else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self.normalized())


@dataclass(frozen=True)
class SeasoningRule:
    min_trading_days: int = 252
    min_month_end_closes: int = 13

    def to_hard_gates(self, *, failure_action: str = WATCHLIST) -> tuple[HardGateSpec, ...]:
        return (
            HardGateSpec(
                name="min_trading_days",
                metric="trading_days",
                operator=">=",
                threshold=int(self.min_trading_days),
                failure_action=failure_action,
                description="Candidate must have enough daily observations before it can enter research ranking.",
            ),
            HardGateSpec(
                name="min_month_end_closes",
                metric="month_end_closes",
                operator=">=",
                threshold=int(self.min_month_end_closes),
                failure_action=failure_action,
                description="Candidate must have enough month-end closes before it can enter research ranking.",
            ),
        )


@dataclass(frozen=True)
class GateResult:
    symbol: str
    gate_name: str
    metric: str
    passed: bool
    actual: Any
    operator: str
    threshold: Any
    action: str
    reason: str

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateDecision:
    symbol: str
    decision: str
    research_action: str
    reason: str
    failed_gates: tuple[str, ...] = ()

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UniverseAuditResult:
    candidate_snapshot: Any
    gate_results: Any
    ranking: Any
    promotion_decisions: tuple[CandidateDecision, ...]
    rule_spec: SelectionRuleSpec
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
