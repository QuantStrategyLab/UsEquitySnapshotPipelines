from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_TOP_QUANTILE = 0.25
DEFAULT_MAX_PBO_PROXY_LOSS_RATE = 0.50
DEFAULT_MIN_POSITIVE_QQQ_EXCESS_RATE = 0.50
DEFAULT_MIN_ROLLING_TOP_QUANTILE_RATE = 0.50

RANK_WINDOW_COLUMNS = (
    "Run",
    "Variant Type",
    "Window Years",
    "Window Start Year",
    "Window End Year",
    "Rank Metric",
    "Rank Metric Value",
    "Rolling Rank",
    "Rolling Rank Percentile",
    "Rolling Top Quantile",
    "QQQ Excess CAGR",
    "SPY Excess CAGR",
)
DIAGNOSTIC_COLUMNS = (
    "Run",
    "Variant Type",
    "Candidate Family",
    "Full Sample CAGR",
    "Full Sample Sharpe",
    "Full Sample Max Drawdown",
    "Full Sample Turnover/Year",
    "Full Sample CAGR Rank",
    "Full Sample CAGR Rank Percentile",
    "Full Sample Top Quantile",
    "Rolling Window Count",
    "Rolling Median Rank Percentile",
    "Rolling Worst Rank Percentile",
    "Rolling Top Quantile Rate",
    "Rolling Bottom Half Rate",
    "Positive QQQ Excess Rate",
    "Positive SPY Excess Rate",
    "Median QQQ Excess CAGR",
    "Worst QQQ Excess CAGR",
    "Median SPY Excess CAGR",
    "Worst SPY Excess CAGR",
    "Worst Rolling Max Drawdown",
    "PBO Proxy Loss Rate",
    "Walk Forward Gate Passed",
    "Walk Forward Gate Reason",
    "OOS Baseline CAGR Win Rate",
    "Median OOS Excess CAGR vs Baseline",
    "Worst OOS Excess CAGR vs Baseline",
    "overfit_risk_label",
    "overfit_risk_reason",
    "recommended_action",
)

PROMOTION_GATE_COLUMNS = (
    "Run",
    "Candidate Family",
    "overfit_gate_passed",
    "overfit_gate_reason",
    "live_promotion_gate_passed",
    "live_promotion_gate_reason",
    "gate_scope",
    "overfit_risk_label",
    "overfit_risk_reason",
    "Walk Forward Gate Passed",
    "Walk Forward Gate Reason",
    "recommended_action",
)
PROMOTABLE_CANDIDATE_FAMILIES = {"fixed_blend_live_candidate", "top4_fallback_candidate"}


def _number(value, *, default: float = float("nan")) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else float(default)


def _bool_or_nan(value) -> bool | float:
    if isinstance(value, bool):
        return bool(value)
    if pd.isna(value):
        return float("nan")
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return float("nan")


def _rank_percentile(rank: pd.Series, group_size: pd.Series) -> pd.Series:
    denominator = (group_size.astype(float) - 1.0).replace(0.0, np.nan)
    percentile = (rank.astype(float) - 1.0) / denominator
    return percentile.fillna(0.0)


def _candidate_family(run: str, variant_type: str) -> str:
    run_text = str(run).strip()
    variant_text = str(variant_type).strip()
    if run_text.startswith("panic") or variant_text.startswith("panic_rebound_guard"):
        return "panic_rebound_guard_research"
    if run_text.startswith(("resid", "beta")) or variant_text.startswith("residual_beta"):
        return "residual_beta_research"
    if run_text.startswith("voltarget") or variant_text.startswith("volatility_managed"):
        return "volatility_managed_research"
    if run_text.startswith("sector_cap") or variant_text.startswith("sector_capped"):
        return "sector_capped_research"
    if run_text.startswith("sector_penalty") or variant_text.startswith("sector_soft_penalty"):
        return "sector_soft_penalty_research"
    if run_text.startswith("dynamic") or variant_text.startswith("dynamic"):
        return "dynamic_research"
    if run_text.startswith("base_top2") or run_text.startswith("top2"):
        return "top2_aggressive_research"
    if run_text.startswith("base_top4") or run_text.startswith("top4"):
        return "top4_fallback_candidate"
    if run_text.startswith(("blend_top2_25_top4_75", "blend_top2_50_top4_50")):
        return "fixed_blend_live_candidate"
    if run_text.startswith("blend_top2"):
        return "fixed_blend_research"
    return "research_or_comparator"


def _with_full_sample_ranks(summary: pd.DataFrame, *, top_quantile: float) -> pd.DataFrame:
    frame = pd.DataFrame(summary).copy()
    if frame.empty:
        return frame
    if "Run" not in frame.columns:
        raise ValueError("summary must include a Run column")
    frame["CAGR"] = pd.to_numeric(frame.get("CAGR"), errors="coerce")
    frame["Full Sample CAGR Rank"] = frame["CAGR"].rank(method="min", ascending=False, na_option="bottom")
    group_size = pd.Series(float(len(frame)), index=frame.index)
    frame["Full Sample CAGR Rank Percentile"] = _rank_percentile(frame["Full Sample CAGR Rank"], group_size)
    frame["Full Sample Top Quantile"] = frame["Full Sample CAGR Rank Percentile"] <= float(top_quantile)
    return frame


def build_rank_windows(
    rolling: pd.DataFrame,
    *,
    top_quantile: float = DEFAULT_TOP_QUANTILE,
) -> pd.DataFrame:
    frame = pd.DataFrame(rolling).copy()
    if frame.empty:
        return pd.DataFrame(columns=RANK_WINDOW_COLUMNS)
    required = {"Run", "Window Years", "Window Start Year", "Window End Year", "Strategy CAGR", "QQQ CAGR", "SPY CAGR"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"rolling must include columns: {', '.join(missing)}")
    for column in ("Strategy CAGR", "QQQ CAGR", "SPY CAGR", "Strategy Max Drawdown"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["Rank Metric"] = "strategy_cagr"
    frame["Rank Metric Value"] = frame["Strategy CAGR"]
    frame["QQQ Excess CAGR"] = frame["Strategy CAGR"] - frame["QQQ CAGR"]
    frame["SPY Excess CAGR"] = frame["Strategy CAGR"] - frame["SPY CAGR"]
    group_cols = ["Window Years", "Window Start Year", "Window End Year"]
    frame["Rolling Rank"] = frame.groupby(group_cols)["Rank Metric Value"].rank(
        method="min",
        ascending=False,
        na_option="bottom",
    )
    group_size = frame.groupby(group_cols)["Run"].transform("count")
    frame["Rolling Rank Percentile"] = _rank_percentile(frame["Rolling Rank"], group_size)
    frame["Rolling Top Quantile"] = frame["Rolling Rank Percentile"] <= float(top_quantile)
    columns = [column for column in RANK_WINDOW_COLUMNS if column in frame.columns]
    return frame.loc[:, columns]


def _aggregate_rank_windows(rank_windows: pd.DataFrame) -> pd.DataFrame:
    if rank_windows.empty:
        return pd.DataFrame(
            columns=[
                "Run",
                "Rolling Window Count",
                "Rolling Median Rank Percentile",
                "Rolling Worst Rank Percentile",
                "Rolling Top Quantile Rate",
                "Rolling Bottom Half Rate",
                "Positive QQQ Excess Rate",
                "Positive SPY Excess Rate",
                "Median QQQ Excess CAGR",
                "Worst QQQ Excess CAGR",
                "Median SPY Excess CAGR",
                "Worst SPY Excess CAGR",
            ]
        )
    frame = rank_windows.copy()
    grouped = frame.groupby("Run", dropna=False)
    output = grouped.agg(
        **{
            "Rolling Window Count": ("Rolling Rank Percentile", "count"),
            "Rolling Median Rank Percentile": ("Rolling Rank Percentile", "median"),
            "Rolling Worst Rank Percentile": ("Rolling Rank Percentile", "max"),
            "Rolling Top Quantile Rate": ("Rolling Top Quantile", "mean"),
            "Rolling Bottom Half Rate": ("Rolling Rank Percentile", lambda values: float((values > 0.50).mean())),
            "Positive QQQ Excess Rate": ("QQQ Excess CAGR", lambda values: float((values > 0.0).mean())),
            "Positive SPY Excess Rate": ("SPY Excess CAGR", lambda values: float((values > 0.0).mean())),
            "Median QQQ Excess CAGR": ("QQQ Excess CAGR", "median"),
            "Worst QQQ Excess CAGR": ("QQQ Excess CAGR", "min"),
            "Median SPY Excess CAGR": ("SPY Excess CAGR", "median"),
            "Worst SPY Excess CAGR": ("SPY Excess CAGR", "min"),
        }
    )
    return output.reset_index()


def _rolling_drawdown_summary(rolling: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(rolling).copy()
    if frame.empty or "Strategy Max Drawdown" not in frame.columns:
        return pd.DataFrame(columns=["Run", "Worst Rolling Max Drawdown"])
    frame["Strategy Max Drawdown"] = pd.to_numeric(frame["Strategy Max Drawdown"], errors="coerce")
    return frame.groupby("Run", dropna=False, as_index=False).agg(
        **{"Worst Rolling Max Drawdown": ("Strategy Max Drawdown", "min")}
    )


def _walk_forward_summary_by_run(walk_forward_summary: pd.DataFrame | None) -> pd.DataFrame:
    if walk_forward_summary is None:
        return pd.DataFrame(columns=["Run"])
    frame = pd.DataFrame(walk_forward_summary).copy()
    if frame.empty:
        return pd.DataFrame(columns=["Run"])
    if "Candidate Run" not in frame.columns:
        raise ValueError("walk_forward_summary must include a Candidate Run column")
    frame = frame.rename(columns={"Candidate Run": "Run"})
    keep = [
        "Run",
        "OOS Baseline CAGR Win Rate",
        "Median OOS Excess CAGR vs Baseline",
        "Worst OOS Excess CAGR vs Baseline",
        "walk_forward_gate_passed",
        "walk_forward_gate_reason",
    ]
    for column in keep:
        if column not in frame.columns:
            frame[column] = np.nan
    return frame.loc[:, keep]


def _risk_and_action(
    row: pd.Series,
    *,
    max_pbo_proxy_loss_rate: float,
    min_positive_qqq_excess_rate: float,
    min_rolling_top_quantile_rate: float,
) -> tuple[str, str, str]:
    reasons: list[str] = []
    family = str(row.get("Candidate Family", ""))
    full_top = bool(row.get("Full Sample Top Quantile", False))
    pbo_loss_rate = _number(row.get("PBO Proxy Loss Rate"))
    qqq_positive_rate = _number(row.get("Positive QQQ Excess Rate"))
    top_quantile_rate = _number(row.get("Rolling Top Quantile Rate"))
    wf_gate = _bool_or_nan(row.get("Walk Forward Gate Passed"))

    if wf_gate is False:
        reasons.append("walk_forward_gate_failed")
    if full_top and pd.notna(pbo_loss_rate) and pbo_loss_rate >= float(max_pbo_proxy_loss_rate):
        reasons.append("full_sample_top_quantile_but_rolling_bottom_half_too_often")
    if pd.notna(qqq_positive_rate) and qqq_positive_rate < float(min_positive_qqq_excess_rate):
        reasons.append("rolling_qqq_excess_win_rate_too_low")

    if reasons:
        label = "high"
    elif full_top and pd.notna(top_quantile_rate) and top_quantile_rate < float(min_rolling_top_quantile_rate):
        label = "medium"
        reasons.append("full_sample_top_quantile_but_rolling_top_quantile_rate_low")
    else:
        label = "low"
        reasons.append("pass")

    if wf_gate is False:
        action = "keep_research_only_oos_failed"
    elif label == "high":
        action = "reject_or_keep_research_only_overfit_risk"
    elif family == "fixed_blend_live_candidate":
        action = "live_candidate_stability_review"
    elif family == "top4_fallback_candidate":
        action = "fallback_stability_review"
    else:
        action = "keep_research_only_diagnostic"
    return label, ";".join(reasons), action


def build_overfit_diagnostics(
    summary: pd.DataFrame,
    rolling: pd.DataFrame,
    *,
    walk_forward_summary: pd.DataFrame | None = None,
    top_quantile: float = DEFAULT_TOP_QUANTILE,
    max_pbo_proxy_loss_rate: float = DEFAULT_MAX_PBO_PROXY_LOSS_RATE,
    min_positive_qqq_excess_rate: float = DEFAULT_MIN_POSITIVE_QQQ_EXCESS_RATE,
    min_rolling_top_quantile_rate: float = DEFAULT_MIN_ROLLING_TOP_QUANTILE_RATE,
) -> dict[str, pd.DataFrame]:
    summary_frame = _with_full_sample_ranks(summary, top_quantile=top_quantile)
    rank_windows = build_rank_windows(rolling, top_quantile=top_quantile)
    rolling_agg = _aggregate_rank_windows(rank_windows)
    rolling_dd = _rolling_drawdown_summary(rolling)
    walk_forward = _walk_forward_summary_by_run(walk_forward_summary)

    diagnostics = summary_frame.merge(rolling_agg, on="Run", how="left")
    diagnostics = diagnostics.merge(rolling_dd, on="Run", how="left")
    diagnostics = diagnostics.merge(walk_forward, on="Run", how="left")
    diagnostics["Candidate Family"] = diagnostics.apply(
        lambda row: _candidate_family(row.get("Run", ""), row.get("Variant Type", "")),
        axis=1,
    )
    diagnostics["PBO Proxy Loss Rate"] = np.where(
        diagnostics["Full Sample Top Quantile"].fillna(False),
        diagnostics["Rolling Bottom Half Rate"],
        np.nan,
    )
    wf_pass_col = "walk_forward_gate_passed"
    if wf_pass_col in diagnostics.columns:
        diagnostics["Walk Forward Gate Passed"] = diagnostics[wf_pass_col].map(_bool_or_nan)
    else:
        diagnostics["Walk Forward Gate Passed"] = np.nan
    diagnostics["Walk Forward Gate Reason"] = diagnostics.get("walk_forward_gate_reason", "")
    risk_values = diagnostics.apply(
        lambda row: _risk_and_action(
            row,
            max_pbo_proxy_loss_rate=max_pbo_proxy_loss_rate,
            min_positive_qqq_excess_rate=min_positive_qqq_excess_rate,
            min_rolling_top_quantile_rate=min_rolling_top_quantile_rate,
        ),
        axis=1,
        result_type="expand",
    )
    diagnostics["overfit_risk_label"] = risk_values[0]
    diagnostics["overfit_risk_reason"] = risk_values[1]
    diagnostics["recommended_action"] = risk_values[2]
    rename_map = {
        "CAGR": "Full Sample CAGR",
        "Sharpe": "Full Sample Sharpe",
        "Max Drawdown": "Full Sample Max Drawdown",
        "Turnover/Year": "Full Sample Turnover/Year",
    }
    diagnostics = diagnostics.rename(columns=rename_map)
    for column in DIAGNOSTIC_COLUMNS:
        if column not in diagnostics.columns:
            diagnostics[column] = np.nan
    diagnostics = diagnostics.loc[:, list(DIAGNOSTIC_COLUMNS)]
    risk_order = {"high": 0, "medium": 1, "low": 2}
    diagnostics = diagnostics.assign(_risk_order=diagnostics["overfit_risk_label"].map(risk_order).fillna(99))
    diagnostics = diagnostics.sort_values(
        ["_risk_order", "Full Sample CAGR Rank"],
        ascending=[True, True],
        kind="stable",
    ).drop(columns=["_risk_order"]).reset_index(drop=True)
    promotion_gate = build_overfit_promotion_gate_summary(diagnostics)
    return {
        "overfit_candidate_diagnostics": diagnostics,
        "overfit_rank_windows": rank_windows,
        "overfit_promotion_gate_summary": promotion_gate,
    }



def _promotion_gate_row(row: pd.Series) -> dict[str, object]:
    family = str(row.get("Candidate Family", ""))
    risk_label = str(row.get("overfit_risk_label", "")).strip().lower()
    risk_reason = str(row.get("overfit_risk_reason", "")).strip()
    wf_pass = _bool_or_nan(row.get("Walk Forward Gate Passed"))
    overfit_reasons: list[str] = []
    if risk_label == "high":
        overfit_reasons.append("overfit_high_risk")
    if wf_pass is False:
        overfit_reasons.append("walk_forward_gate_failed")
    if risk_reason and risk_reason not in {"pass", "nan"}:
        overfit_reasons.append(risk_reason)
    overfit_reason = ";".join(dict.fromkeys(overfit_reasons)) if overfit_reasons else "pass"
    overfit_passed = overfit_reason == "pass"

    live_reasons: list[str] = []
    if not overfit_passed:
        live_reasons.append(overfit_reason)
    if family not in PROMOTABLE_CANDIDATE_FAMILIES:
        live_reasons.append("not_promotable_candidate_family")
    live_reason = ";".join(dict.fromkeys(reason for reason in live_reasons if reason)) if live_reasons else "pass"
    return {
        "Run": row.get("Run"),
        "Candidate Family": family,
        "overfit_gate_passed": bool(overfit_passed),
        "overfit_gate_reason": overfit_reason,
        "live_promotion_gate_passed": live_reason == "pass",
        "live_promotion_gate_reason": live_reason,
        "gate_scope": "blocker_only_not_positive_evidence",
        "overfit_risk_label": row.get("overfit_risk_label"),
        "overfit_risk_reason": row.get("overfit_risk_reason"),
        "Walk Forward Gate Passed": row.get("Walk Forward Gate Passed"),
        "Walk Forward Gate Reason": row.get("Walk Forward Gate Reason"),
        "recommended_action": row.get("recommended_action"),
    }


def build_overfit_promotion_gate_summary(diagnostics: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(diagnostics).copy()
    if frame.empty:
        return pd.DataFrame(columns=PROMOTION_GATE_COLUMNS)
    if "Run" not in frame.columns:
        raise ValueError("diagnostics must include a Run column")
    rows = [_promotion_gate_row(row) for _, row in frame.iterrows()]
    return pd.DataFrame(rows).loc[:, list(PROMOTION_GATE_COLUMNS)]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build overfit/OOS stability diagnostics from Russell concentration research outputs."
    )
    parser.add_argument("--summary", required=True, help="Input concentration_variant_summary.csv")
    parser.add_argument("--rolling", required=True, help="Input concentration_variant_rolling_summary.csv")
    parser.add_argument("--walk-forward-summary", help="Optional walk_forward_oos_summary.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for diagnostics outputs")
    parser.add_argument("--top-quantile", type=float, default=DEFAULT_TOP_QUANTILE)
    parser.add_argument("--max-pbo-proxy-loss-rate", type=float, default=DEFAULT_MAX_PBO_PROXY_LOSS_RATE)
    parser.add_argument("--min-positive-qqq-excess-rate", type=float, default=DEFAULT_MIN_POSITIVE_QQQ_EXCESS_RATE)
    parser.add_argument("--min-rolling-top-quantile-rate", type=float, default=DEFAULT_MIN_ROLLING_TOP_QUANTILE_RATE)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = read_table(args.summary)
    rolling = read_table(args.rolling)
    walk_forward = read_table(args.walk_forward_summary) if args.walk_forward_summary else None
    result = build_overfit_diagnostics(
        summary,
        rolling,
        walk_forward_summary=walk_forward,
        top_quantile=float(args.top_quantile),
        max_pbo_proxy_loss_rate=float(args.max_pbo_proxy_loss_rate),
        min_positive_qqq_excess_rate=float(args.min_positive_qqq_excess_rate),
        min_rolling_top_quantile_rate=float(args.min_rolling_top_quantile_rate),
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_path = output_dir / "overfit_candidate_diagnostics.csv"
    rank_windows_path = output_dir / "overfit_rank_windows.csv"
    promotion_gate_path = output_dir / "overfit_promotion_gate_summary.csv"
    result["overfit_candidate_diagnostics"].to_csv(diagnostics_path, index=False)
    result["overfit_rank_windows"].to_csv(rank_windows_path, index=False)
    result["overfit_promotion_gate_summary"].to_csv(promotion_gate_path, index=False)
    print(f"Wrote {diagnostics_path}")
    print(f"Wrote {rank_windows_path}")
    print(f"Wrote {promotion_gate_path}")
    print(result["overfit_candidate_diagnostics"].head(max(int(args.print_top), 0)).to_string(index=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
