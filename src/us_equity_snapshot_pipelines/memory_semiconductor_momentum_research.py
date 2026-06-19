from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

import pandas as pd

from .ai_rule_proposal import build_rule_spec_from_ai_proposal
from .russell_1000_multi_factor_defensive_snapshot import read_table
from .universe_audit_contracts import (
    ELIGIBLE_FOR_RESEARCH_RANKING,
    ScoreTermSpec,
    SeasoningRule,
    SelectionRuleSpec,
    SymbolSpec,
    WATCHLIST,
)
from .universe_audit_engine import normalize_symbols as _normalize_symbols
from .universe_audit_engine import run_universe_audit
from .yfinance_prices import download_yahoo_chart_price_history

DEFAULT_PRICE_START_DATE = "2024-01-01"
DEFAULT_MEMORY_ETF_SYMBOLS = ("DRAM",)
DEFAULT_SEMICONDUCTOR_PROXY_ETFS = ("SMH", "SOXX", "XSD", "PSI")
DEFAULT_MEMORY_STOCK_TRACKERS = ("MU", "WDC", "STX", "SNDK")
DEFAULT_BENCHMARKS = ("SPY", "QQQ")
DEFAULT_MIN_TRADING_DAYS = 252
DEFAULT_MIN_MONTH_END_CLOSES = 13
DEFAULT_RULE_ID = "memory_semiconductor_momentum"
DEFAULT_RULE_VERSION = "v1"
DEFAULT_UNIVERSE_ID = "memory_semiconductor_watchlist"


# Re-export for existing tests/callers inside this repository.
__all__ = [
    "DEFAULT_BENCHMARKS",
    "DEFAULT_MEMORY_ETF_SYMBOLS",
    "DEFAULT_MEMORY_STOCK_TRACKERS",
    "DEFAULT_SEMICONDUCTOR_PROXY_ETFS",
    "SeasoningRule",
    "SelectionRuleSpec",
    "SymbolSpec",
    "build_default_symbol_specs",
    "build_memory_semiconductor_audit",
    "build_memory_semiconductor_rule_spec",
    "build_memory_semiconductor_snapshot",
    "build_tradeable_ranking",
    "main",
]


def build_default_symbol_specs(
    *,
    memory_etfs: Sequence[str] = DEFAULT_MEMORY_ETF_SYMBOLS,
    semiconductor_proxy_etfs: Sequence[str] = DEFAULT_SEMICONDUCTOR_PROXY_ETFS,
    memory_stock_trackers: Sequence[str] = DEFAULT_MEMORY_STOCK_TRACKERS,
    benchmarks: Sequence[str] = DEFAULT_BENCHMARKS,
) -> tuple[SymbolSpec, ...]:
    specs: list[SymbolSpec] = []
    for symbol in _normalize_symbols(memory_etfs):
        specs.append(SymbolSpec(symbol=symbol, role="memory_etf", eligible_for_trading=True))
    for symbol in _normalize_symbols(semiconductor_proxy_etfs):
        specs.append(SymbolSpec(symbol=symbol, role="semiconductor_proxy_etf", eligible_for_trading=True))
    for symbol in _normalize_symbols(memory_stock_trackers):
        specs.append(SymbolSpec(symbol=symbol, role="memory_stock_tracker", eligible_for_trading=False))
    for symbol in _normalize_symbols(benchmarks):
        specs.append(SymbolSpec(symbol=symbol, role="benchmark", eligible_for_trading=False))
    deduped: dict[str, SymbolSpec] = {}
    for spec in specs:
        normalized = spec.normalized()
        deduped.setdefault(normalized.symbol, normalized)
    return tuple(deduped.values())


def build_memory_semiconductor_rule_spec(
    *,
    seasoning_rule: SeasoningRule = SeasoningRule(DEFAULT_MIN_TRADING_DAYS, DEFAULT_MIN_MONTH_END_CLOSES),
    rule_id: str = DEFAULT_RULE_ID,
    rule_version: str = DEFAULT_RULE_VERSION,
    universe_id: str = DEFAULT_UNIVERSE_ID,
) -> SelectionRuleSpec:
    return SelectionRuleSpec(
        rule_id=rule_id,
        rule_version=rule_version,
        universe_id=universe_id,
        hard_gates=seasoning_rule.to_hard_gates(failure_action=WATCHLIST),
        score_terms=(
            ScoreTermSpec("momentum_score", 0.55, higher_is_better=True),
            ScoreTermSpec("ret_63d", 0.30, higher_is_better=True),
            ScoreTermSpec("vol_63", 0.15, higher_is_better=False),
        ),
        benchmark_symbols=DEFAULT_BENCHMARKS,
        notes=(
            "Transparent research-only rule for memory semiconductor ETF/proxy ranking. "
            "AI may propose this structure, but deterministic hard gates and score terms decide membership."
        ),
    )


def build_memory_semiconductor_audit(
    price_history: pd.DataFrame,
    *,
    specs: Sequence[SymbolSpec] | None = None,
    seasoning_rule: SeasoningRule = SeasoningRule(DEFAULT_MIN_TRADING_DAYS, DEFAULT_MIN_MONTH_END_CLOSES),
    rule_spec: SelectionRuleSpec | None = None,
    as_of_date: str | None = None,
):
    resolved_rule = rule_spec or build_memory_semiconductor_rule_spec(seasoning_rule=seasoning_rule)
    return run_universe_audit(
        price_history,
        specs=tuple(specs or build_default_symbol_specs()),
        rule_spec=resolved_rule,
        as_of_date=as_of_date,
    )


def build_memory_semiconductor_snapshot(
    price_history: pd.DataFrame,
    *,
    specs: Sequence[SymbolSpec] | None = None,
    seasoning_rule: SeasoningRule = SeasoningRule(DEFAULT_MIN_TRADING_DAYS, DEFAULT_MIN_MONTH_END_CLOSES),
    as_of_date: str | None = None,
) -> pd.DataFrame:
    return build_memory_semiconductor_audit(
        price_history,
        specs=specs,
        seasoning_rule=seasoning_rule,
        as_of_date=as_of_date,
    ).candidate_snapshot


def build_tradeable_ranking(snapshot: pd.DataFrame) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot.copy()
    if "research_action" in snapshot.columns:
        ranking = snapshot.loc[snapshot["research_action"].eq(ELIGIBLE_FOR_RESEARCH_RANKING)].copy()
    else:
        ranking = snapshot[
            (snapshot.get("eligible_for_trading") == True)  # noqa: E712 - pandas scalar comparison.
            & (snapshot.get("seasoning_eligible") == True)  # noqa: E712 - pandas scalar comparison.
            & (snapshot.get("has_data") == True)  # noqa: E712 - pandas scalar comparison.
        ].copy()
    if ranking.empty:
        return ranking
    if "score" not in ranking.columns:
        ranking = ranking.copy()
        ranking["score"] = pd.to_numeric(ranking.get("momentum_score"), errors="coerce")
    sort_columns = [column for column in ("score", "momentum_score", "ret_63d") if column in ranking.columns]
    if not sort_columns:
        return ranking.reset_index(drop=True)
    ranking = ranking.sort_values(sort_columns, ascending=[False] * len(sort_columns)).reset_index(drop=True)
    if "rank" not in ranking.columns:
        ranking.insert(0, "rank", range(1, len(ranking) + 1))
    return ranking


def _json_safe(value):
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_audit_report(path: Path, *, result, profile: str) -> None:
    decisions = result.promotion_decisions
    ranking_preview = ", ".join(result.ranking["symbol"].astype(str).head(5).tolist()) if not result.ranking.empty else "none"
    watchlist = ", ".join(
        decision.symbol for decision in decisions if decision.research_action != ELIGIBLE_FOR_RESEARCH_RANKING
    ) or "none"
    path.write_text(
        "\n".join(
            [
                f"# {profile} Audit Report",
                "",
                "This report is generated by deterministic universe-audit rules. AI proposals may design rule candidates, but they do not decide final membership.",
                "",
                f"- Rule: `{result.rule_spec.rule_id}` `{result.rule_spec.rule_version}`",
                f"- Universe: `{result.rule_spec.universe_id}`",
                f"- Candidate count: {result.diagnostics['candidate_count']}",
                f"- Research-ranking count: {result.diagnostics['ranking_count']}",
                f"- Ranking preview: {ranking_preview}",
                f"- Watchlist / tracker / rejected: {watchlist}",
                "",
                "Artifacts:",
                "",
                "- `candidate_snapshot.csv`",
                "- `gate_results.csv`",
                "- `ranking.csv`",
                "- `promotion_decision.json`",
                "- `run_manifest.json`",
                "",
            ]
        ),
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a research-only memory semiconductor momentum universe audit.")
    parser.add_argument("--prices", help="Input price history file (.csv/.json/.jsonl/.parquet).")
    parser.add_argument("--price-start", default=DEFAULT_PRICE_START_DATE)
    parser.add_argument("--price-end", default=None)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--memory-etfs", default=",".join(DEFAULT_MEMORY_ETF_SYMBOLS))
    parser.add_argument("--semiconductor-proxy-etfs", default=",".join(DEFAULT_SEMICONDUCTOR_PROXY_ETFS))
    parser.add_argument("--memory-stock-trackers", default=",".join(DEFAULT_MEMORY_STOCK_TRACKERS))
    parser.add_argument("--benchmarks", default=",".join(DEFAULT_BENCHMARKS))
    parser.add_argument("--min-trading-days", type=int, default=DEFAULT_MIN_TRADING_DAYS)
    parser.add_argument("--min-month-end-closes", type=int, default=DEFAULT_MIN_MONTH_END_CLOSES)
    parser.add_argument("--ai-rule-proposal", help="Optional AI proposal JSON; converted to deterministic rule spec before execution.")
    parser.add_argument("--output-dir", default="data/output/memory_semiconductor_momentum_research")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    specs = build_default_symbol_specs(
        memory_etfs=_normalize_symbols(args.memory_etfs),
        semiconductor_proxy_etfs=_normalize_symbols(args.semiconductor_proxy_etfs),
        memory_stock_trackers=_normalize_symbols(args.memory_stock_trackers),
        benchmarks=_normalize_symbols(args.benchmarks),
    )
    symbols = [spec.symbol for spec in specs]
    if args.prices:
        price_history = read_table(Path(args.prices))
    else:
        price_history = download_yahoo_chart_price_history(
            symbols,
            start=str(args.price_start),
            end=args.price_end,
        )

    seasoning_rule = SeasoningRule(
        min_trading_days=int(args.min_trading_days),
        min_month_end_closes=int(args.min_month_end_closes),
    )
    if args.ai_rule_proposal:
        rule_spec = build_rule_spec_from_ai_proposal(
            args.ai_rule_proposal,
            universe_id=DEFAULT_UNIVERSE_ID,
        )
    else:
        rule_spec = build_memory_semiconductor_rule_spec(seasoning_rule=seasoning_rule)

    result = build_memory_semiconductor_audit(
        price_history,
        specs=specs,
        seasoning_rule=seasoning_rule,
        rule_spec=rule_spec,
        as_of_date=args.as_of_date,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    price_history.to_csv(output_dir / "downloaded_price_history.csv", index=False)
    result.candidate_snapshot.to_csv(output_dir / "candidate_snapshot.csv", index=False)
    result.gate_results.to_csv(output_dir / "gate_results.csv", index=False)
    result.ranking.to_csv(output_dir / "ranking.csv", index=False)
    _write_json(
        output_dir / "promotion_decision.json",
        {
            "rule_spec": result.rule_spec.to_dict(),
            "decisions": [decision.to_row() for decision in result.promotion_decisions],
        },
    )
    _write_json(
        output_dir / "run_manifest.json",
        {
            "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "profile": DEFAULT_RULE_ID,
            "artifact_type": "transparent_universe_audit",
            "price_start": str(args.price_start),
            "price_end": args.price_end,
            "as_of_date": args.as_of_date,
            "seasoning_rule": asdict(seasoning_rule),
            "rule_spec": result.rule_spec.to_dict(),
            "symbols": [asdict(spec) for spec in specs],
            "diagnostics": dict(result.diagnostics),
            "outputs": [
                "downloaded_price_history.csv",
                "candidate_snapshot.csv",
                "gate_results.csv",
                "ranking.csv",
                "promotion_decision.json",
                "audit_report.md",
            ],
        },
    )
    _write_audit_report(output_dir / "audit_report.md", result=result, profile=DEFAULT_RULE_ID)
    print(result.candidate_snapshot.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
