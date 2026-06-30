from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

PROMOTION_REVIEW_COLUMNS = (
    "Run",
    "Candidate Role",
    "Gate Profile",
    "CAGR",
    "Max Drawdown",
    "Sharpe",
    "Turnover/Year",
    "live_gate_passed",
    "live_gate_reason",
    "stress_gate_passed",
    "stress_gate_reason",
    "overfit_gate_passed",
    "overfit_gate_reason",
    "liquidity_gate_passed",
    "liquidity_gate_reason",
    "Portfolio NAV",
    "Max Participation Rate",
    "reality_check_qqq_passed",
    "reality_check_qqq_p_value",
    "reality_check_spy_passed",
    "reality_check_spy_p_value",
    "spa_qqq_passed",
    "spa_qqq_consistent_p_value",
    "spa_spy_passed",
    "spa_spy_consistent_p_value",
    "era_robustness_passed",
    "era_robustness_reason",
    "era_best_cagr_count",
    "era_positive_qqq_excess_rate",
    "era_positive_spy_excess_rate",
    "era_worst_qqq_excess_cagr",
    "era_worst_spy_excess_cagr",
    "era_worst_max_drawdown",
    "era_recommended_action",
    "mcs_style_in_confidence_set",
    "mcs_style_dominated_by_best",
    "mcs_style_p_value_vs_best",
    "mcs_style_annualized_gap_vs_best",
    "mcs_style_recommended_action",
    "required_gates_passed",
    "required_gate_reason",
    "statistical_support_level",
    "promotion_decision",
    "recommended_action",
)


def _bool(value, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return bool(value)
    if pd.isna(value):
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _number(value, *, default: float = float("nan")) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else float(default)


def _candidate_role_from_run(run: str) -> tuple[str, str]:
    if run == "base_top4_cap25" or str(run).startswith("base_top4"):
        return "robust_baseline", "fallback"
    if run == "blend_top2_25_top4_75":
        return "conservative_live_design", "conservative"
    if run == "blend_top2_50_top4_50":
        return "balanced_offensive_live_design", "balanced_offensive"
    if str(run).startswith("panic"):
        return "panic_rebound_guard_research", "research_only"
    if str(run).startswith("base_top2"):
        return "aggressive_research", "research_only"
    return "research_only", "research_only"


def _summary_by_run(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Run", *columns])
    if "Run" not in frame.columns:
        raise ValueError("summary frame must include Run column")
    keep = ["Run", *[column for column in columns if column in frame.columns]]
    return frame.loc[:, keep].drop_duplicates(subset=["Run"], keep="first")


def _reality_by_run(frame: pd.DataFrame | None, *, prefix: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Run", f"{prefix}_passed", f"{prefix}_p_value"])
    if "Run" not in frame.columns:
        raise ValueError("reality check summary must include Run column")
    output = frame.loc[:, [column for column in ["Run", "Reality Check Passed", "Reality Check P Value"] if column in frame.columns]].copy()
    output = output.rename(
        columns={
            "Reality Check Passed": f"{prefix}_passed",
            "Reality Check P Value": f"{prefix}_p_value",
        }
    )
    if f"{prefix}_passed" not in output.columns:
        output[f"{prefix}_passed"] = False
    if f"{prefix}_p_value" not in output.columns:
        output[f"{prefix}_p_value"] = float("nan")
    return output.drop_duplicates(subset=["Run"], keep="first")


def _spa_by_run(frame: pd.DataFrame | None, *, prefix: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Run", f"{prefix}_passed", f"{prefix}_consistent_p_value"])
    if "Run" not in frame.columns:
        raise ValueError("SPA summary must include Run column")
    output = frame.loc[
        :,
        [column for column in ["Run", "SPA Passed", "SPA Consistent P Value"] if column in frame.columns],
    ].copy()
    output = output.rename(
        columns={
            "SPA Passed": f"{prefix}_passed",
            "SPA Consistent P Value": f"{prefix}_consistent_p_value",
        }
    )
    if f"{prefix}_passed" not in output.columns:
        output[f"{prefix}_passed"] = False
    if f"{prefix}_consistent_p_value" not in output.columns:
        output[f"{prefix}_consistent_p_value"] = float("nan")
    return output.drop_duplicates(subset=["Run"], keep="first")


def _era_by_run(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Run"])
    if "Run" not in frame.columns:
        raise ValueError("era split promotion summary must include Run column")
    keep = [
        "Run",
        "era_robustness_passed",
        "era_robustness_reason",
        "Best CAGR Era Count",
        "Positive QQQ Excess Era Rate",
        "Positive SPY Excess Era Rate",
        "Worst QQQ Excess CAGR",
        "Worst SPY Excess CAGR",
        "Worst Max Drawdown",
        "recommended_action",
    ]
    output = frame.loc[:, [column for column in keep if column in frame.columns]].copy()
    output = output.rename(
        columns={
            "Best CAGR Era Count": "era_best_cagr_count",
            "Positive QQQ Excess Era Rate": "era_positive_qqq_excess_rate",
            "Positive SPY Excess Era Rate": "era_positive_spy_excess_rate",
            "Worst QQQ Excess CAGR": "era_worst_qqq_excess_cagr",
            "Worst SPY Excess CAGR": "era_worst_spy_excess_cagr",
            "Worst Max Drawdown": "era_worst_max_drawdown",
            "recommended_action": "era_recommended_action",
        }
    )
    if "era_robustness_passed" not in output.columns:
        output["era_robustness_passed"] = False
    if "era_robustness_reason" not in output.columns:
        output["era_robustness_reason"] = "missing_gate_artifact"
    return output.drop_duplicates(subset=["Run"], keep="first")


def _mcs_by_run(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Run"])
    if "Run" not in frame.columns:
        raise ValueError("MCS-style candidate summary must include Run column")
    keep = [
        "Run",
        "In MCS Style Confidence Set",
        "Dominated By Best Candidate",
        "Pairwise P Value vs Best",
        "Annualized Gap vs Best",
        "recommended_action",
    ]
    output = frame.loc[:, [column for column in keep if column in frame.columns]].copy()
    output = output.rename(
        columns={
            "In MCS Style Confidence Set": "mcs_style_in_confidence_set",
            "Dominated By Best Candidate": "mcs_style_dominated_by_best",
            "Pairwise P Value vs Best": "mcs_style_p_value_vs_best",
            "Annualized Gap vs Best": "mcs_style_annualized_gap_vs_best",
            "recommended_action": "mcs_style_recommended_action",
        }
    )
    if "mcs_style_in_confidence_set" not in output.columns:
        output["mcs_style_in_confidence_set"] = False
    if "mcs_style_dominated_by_best" not in output.columns:
        output["mcs_style_dominated_by_best"] = False
    return output.drop_duplicates(subset=["Run"], keep="first")


def _liquidity_for_nav(frame: pd.DataFrame | None, *, portfolio_nav: float | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["Run"])
    if "Run" not in frame.columns:
        raise ValueError("liquidity summary must include Run column")
    liquidity = frame.copy()
    if portfolio_nav is not None and "Portfolio NAV" in liquidity.columns:
        target = float(portfolio_nav)
        liquidity["_nav_delta"] = (pd.to_numeric(liquidity["Portfolio NAV"], errors="coerce") - target).abs()
        liquidity = liquidity.sort_values(["Run", "_nav_delta"]).drop_duplicates(subset=["Run"], keep="first")
        liquidity = liquidity.drop(columns=["_nav_delta"])
    else:
        liquidity = liquidity.drop_duplicates(subset=["Run"], keep="first")
    keep = [
        "Run",
        "Portfolio NAV",
        "Max Participation Rate",
        "liquidity_gate_passed",
        "liquidity_gate_reason",
    ]
    return liquidity.loc[:, [column for column in keep if column in liquidity.columns]]


def _market_text(markets: list[str]) -> str:
    if markets == ["qqq", "spy"]:
        return "qqq_and_spy"
    return "_and_".join(markets)


def _statistical_support_level(row: pd.Series) -> str:
    reality_markets = [
        market
        for market, column in (
            ("qqq", "reality_check_qqq_passed"),
            ("spy", "reality_check_spy_passed"),
        )
        if _bool(row.get(column), default=False)
    ]
    spa_markets = [
        market
        for market, column in (
            ("qqq", "spa_qqq_passed"),
            ("spy", "spa_spy_passed"),
        )
        if _bool(row.get(column), default=False)
    ]
    if reality_markets and spa_markets and reality_markets == spa_markets:
        return f"{_market_text(reality_markets)}_reality_check_and_spa"
    parts = []
    if reality_markets:
        parts.append(f"{_market_text(reality_markets)}_reality_check")
    if spa_markets:
        parts.append(f"{_market_text(spa_markets)}_spa")
    if parts:
        return "_plus_".join(parts)
    has_spa_artifact = pd.notna(row.get("spa_qqq_consistent_p_value")) or pd.notna(
        row.get("spa_spy_consistent_p_value")
    )
    return "not_reality_check_or_spa_winner" if has_spa_artifact else "not_reality_check_winner"


def _decision(row: pd.Series) -> tuple[bool, str, str, str, str]:
    gate_checks = {
        "live_gate": _bool(row.get("live_gate_passed"), default=False),
        "stress_gate": _bool(row.get("stress_gate_passed"), default=False),
        "overfit_gate": _bool(row.get("overfit_gate_passed"), default=False),
        "liquidity_gate": _bool(row.get("liquidity_gate_passed"), default=False),
    }
    failed = [name for name, passed in gate_checks.items() if not passed]
    required_passed = not failed
    required_reason = "pass" if required_passed else ";".join(failed)

    support = _statistical_support_level(row)

    role = str(row.get("Candidate Role", ""))
    profile = str(row.get("Gate Profile", ""))
    if not required_passed:
        return False, required_reason, support, "research_only", "keep_research_only"
    if role == "balanced_offensive_live_design" or profile == "balanced_offensive":
        action = "promote_aggressive_live_design_review"
        if support.startswith("qqq_and_spy_reality_check"):
            action = "preferred_aggressive_live_design_review"
        return True, "pass", support, "live_design_review_balanced_offensive", action
    if role == "conservative_live_design" or profile == "conservative":
        return True, "pass", support, "live_design_review_conservative", "promote_conservative_live_design_review"
    if role == "robust_baseline" or profile == "fallback":
        return True, "pass", support, "fallback_live_design_review", "keep_as_fallback_live_design"
    return False, "not_promotable_candidate_role", support, "research_only", "keep_research_only"


def build_promotion_review(
    candidate_summary: pd.DataFrame,
    *,
    live_readiness: pd.DataFrame | None = None,
    stress_summary: pd.DataFrame | None = None,
    overfit_promotion: pd.DataFrame | None = None,
    liquidity_summary: pd.DataFrame | None = None,
    reality_check_qqq: pd.DataFrame | None = None,
    reality_check_spy: pd.DataFrame | None = None,
    spa_qqq: pd.DataFrame | None = None,
    spa_spy: pd.DataFrame | None = None,
    era_split_promotion: pd.DataFrame | None = None,
    mcs_style_summary: pd.DataFrame | None = None,
    candidate_runs: Iterable[str] | None = None,
    portfolio_nav: float | None = None,
) -> pd.DataFrame:
    base = _summary_by_run(
        pd.DataFrame(candidate_summary),
        columns=("CAGR", "Max Drawdown", "Sharpe", "Turnover/Year"),
    )
    runs = tuple(str(run) for run in candidate_runs or ())
    if runs:
        base = base.loc[base["Run"].astype(str).isin(runs)].copy()
    if base.empty:
        return pd.DataFrame(columns=PROMOTION_REVIEW_COLUMNS)

    live = _summary_by_run(
        pd.DataFrame(live_readiness) if live_readiness is not None else pd.DataFrame(),
        columns=("Candidate Role", "Gate Profile", "live_gate_passed", "live_gate_reason"),
    )
    stress = _summary_by_run(
        pd.DataFrame(stress_summary) if stress_summary is not None else pd.DataFrame(),
        columns=("all_stress_gates_passed", "stress_gate_reason"),
    ).rename(columns={"all_stress_gates_passed": "stress_gate_passed"})
    overfit = _summary_by_run(
        pd.DataFrame(overfit_promotion) if overfit_promotion is not None else pd.DataFrame(),
        columns=("live_promotion_gate_passed", "live_promotion_gate_reason"),
    ).rename(
        columns={
            "live_promotion_gate_passed": "overfit_gate_passed",
            "live_promotion_gate_reason": "overfit_gate_reason",
        }
    )
    liquidity = _liquidity_for_nav(liquidity_summary, portfolio_nav=portfolio_nav)
    qqq = _reality_by_run(reality_check_qqq, prefix="reality_check_qqq")
    spy = _reality_by_run(reality_check_spy, prefix="reality_check_spy")
    spa_qqq_frame = _spa_by_run(spa_qqq, prefix="spa_qqq")
    spa_spy_frame = _spa_by_run(spa_spy, prefix="spa_spy")
    era = _era_by_run(era_split_promotion)
    mcs = _mcs_by_run(mcs_style_summary)

    output = base.merge(live, on="Run", how="left")
    output = output.merge(stress, on="Run", how="left")
    output = output.merge(overfit, on="Run", how="left")
    output = output.merge(liquidity, on="Run", how="left")
    output = output.merge(qqq, on="Run", how="left")
    output = output.merge(spy, on="Run", how="left")
    output = output.merge(spa_qqq_frame, on="Run", how="left")
    output = output.merge(spa_spy_frame, on="Run", how="left")
    output = output.merge(era, on="Run", how="left")
    output = output.merge(mcs, on="Run", how="left")

    roles = output["Run"].map(lambda run: _candidate_role_from_run(str(run)))
    if "Candidate Role" not in output.columns:
        output["Candidate Role"] = pd.NA
    if "Gate Profile" not in output.columns:
        output["Gate Profile"] = pd.NA
    output["Candidate Role"] = output["Candidate Role"].fillna(roles.map(lambda value: value[0]))
    output["Gate Profile"] = output["Gate Profile"].fillna(roles.map(lambda value: value[1]))
    for column in (
        "live_gate_passed",
        "stress_gate_passed",
        "overfit_gate_passed",
        "liquidity_gate_passed",
        "reality_check_qqq_passed",
        "reality_check_spy_passed",
        "spa_qqq_passed",
        "spa_spy_passed",
        "era_robustness_passed",
        "mcs_style_in_confidence_set",
        "mcs_style_dominated_by_best",
    ):
        if column not in output.columns:
            output[column] = False
        output[column] = output[column].map(lambda value: _bool(value, default=False))
    for column in ("live_gate_reason", "stress_gate_reason", "overfit_gate_reason", "liquidity_gate_reason"):
        if column not in output.columns:
            output[column] = "missing_gate_artifact"
        output[column] = output[column].fillna("missing_gate_artifact")

    decisions = output.apply(_decision, axis=1, result_type="expand")
    output["required_gates_passed"] = decisions[0]
    output["required_gate_reason"] = decisions[1]
    output["statistical_support_level"] = decisions[2]
    output["promotion_decision"] = decisions[3]
    output["recommended_action"] = decisions[4]
    for column in PROMOTION_REVIEW_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA
    output = output.loc[:, list(PROMOTION_REVIEW_COLUMNS)]
    decision_order = {
        "live_design_review_balanced_offensive": 0,
        "live_design_review_conservative": 1,
        "fallback_live_design_review": 2,
        "research_only": 3,
    }
    output = output.assign(_decision_order=output["promotion_decision"].map(decision_order).fillna(99))
    return output.sort_values(["_decision_order", "CAGR"], ascending=[True, False], kind="stable").drop(
        columns=["_decision_order"]
    ).reset_index(drop=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build an integrated Russell live-promotion review artifact.")
    parser.add_argument("--summary", required=True, help="Input concentration_variant_summary.csv")
    parser.add_argument("--live-readiness", required=True, help="Input live_readiness_summary.csv")
    parser.add_argument("--stress-summary", required=True, help="Input stress_live_readiness_summary.csv")
    parser.add_argument("--overfit-promotion", required=True, help="Input overfit_promotion_gate_summary.csv")
    parser.add_argument("--liquidity-summary", required=True, help="Input liquidity_summary.csv")
    parser.add_argument("--reality-check-qqq", help="Optional QQQ reality_check_candidate_summary.csv")
    parser.add_argument("--reality-check-spy", help="Optional SPY reality_check_candidate_summary.csv")
    parser.add_argument("--spa-qqq", help="Optional QQQ spa_candidate_summary.csv")
    parser.add_argument("--spa-spy", help="Optional SPY spa_candidate_summary.csv")
    parser.add_argument("--era-split-promotion", help="Optional era_split_promotion_summary.csv")
    parser.add_argument("--mcs-style-summary", help="Optional mcs_style_candidate_summary.csv")
    parser.add_argument("--candidate-runs", default="")
    parser.add_argument("--portfolio-nav", type=float)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_promotion_review(
        read_table(args.summary),
        live_readiness=read_table(args.live_readiness),
        stress_summary=read_table(args.stress_summary),
        overfit_promotion=read_table(args.overfit_promotion),
        liquidity_summary=read_table(args.liquidity_summary),
        reality_check_qqq=read_table(args.reality_check_qqq) if args.reality_check_qqq else None,
        reality_check_spy=read_table(args.reality_check_spy) if args.reality_check_spy else None,
        spa_qqq=read_table(args.spa_qqq) if args.spa_qqq else None,
        spa_spy=read_table(args.spa_spy) if args.spa_spy else None,
        era_split_promotion=read_table(args.era_split_promotion) if args.era_split_promotion else None,
        mcs_style_summary=read_table(args.mcs_style_summary) if args.mcs_style_summary else None,
        candidate_runs=parse_csv_strings(args.candidate_runs, default=()),
        portfolio_nav=args.portfolio_nav,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "live_promotion_review.csv"
    result.to_csv(output_path, index=False)
    print(result.head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote live promotion review -> {output_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
