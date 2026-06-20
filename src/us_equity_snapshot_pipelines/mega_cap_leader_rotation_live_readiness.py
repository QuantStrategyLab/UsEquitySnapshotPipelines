from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_REQUIRED_UNIVERSE_LAG_DAYS = 21
DEFAULT_MIN_5Y_EXCESS_CAGR_VS_QQQ = 0.0
DEFAULT_MIN_5Y_EXCESS_CAGR_VS_SPY = 0.0
DEFAULT_MIN_3Y_EXCESS_CAGR_VS_SPY = 0.0
DEFAULT_CONSERVATIVE_MAX_DRAWDOWN = -0.30
DEFAULT_BALANCED_MAX_DRAWDOWN = -0.32
DEFAULT_ROBUST_BASELINE_MAX_DRAWDOWN = -0.30
LIVE_READINESS_COLUMNS = (
    "Run",
    "Candidate Role",
    "Gate Profile",
    "Start",
    "End",
    "Universe Lag Trading Days",
    "CAGR",
    "Max Drawdown",
    "Sharpe",
    "Calmar",
    "Total Return",
    "Benchmark Total Return",
    "Broad Benchmark Total Return",
    "Turnover/Year",
    "Min 3Y QQQ Excess CAGR",
    "Min 3Y SPY Excess CAGR",
    "Min 5Y QQQ Excess CAGR",
    "Min 5Y SPY Excess CAGR",
    "Worst Rolling Max Drawdown",
    "Worst 3Y QQQ Excess Window",
    "Worst 5Y QQQ Excess Window",
    "Allowed Max Drawdown",
    "live_gate_passed",
    "live_gate_reason",
    "recommended_action",
)


def _pct(value: float) -> str:
    return f"{float(value):.2%}"


def _number(value, *, default: float = float("nan")) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else float(default)


def _window_label(row: pd.Series | None) -> str:
    if row is None or row.empty:
        return ""
    start = row.get("Window Start Year")
    end = row.get("Window End Year")
    if pd.isna(start) or pd.isna(end):
        return ""
    return f"{int(start)}-{int(end)}"


def _candidate_role(row: pd.Series) -> tuple[str, str, float]:
    run = str(row.get("Run", "")).strip()
    variant_type = str(row.get("Variant Type", "")).strip()
    daily_risk_mode = str(row.get("Daily Risk Mode", "")).strip().lower()
    top2_weight = _number(row.get("Top2 Blend Weight"), default=float("nan"))

    if run.startswith("sector_cap") or variant_type.startswith("sector_capped_"):
        return "sector_capped_research", "research_only", DEFAULT_CONSERVATIVE_MAX_DRAWDOWN
    if run.startswith("sector_penalty") or variant_type.startswith("sector_soft_penalty_"):
        return "sector_soft_penalty_research", "research_only", DEFAULT_CONSERVATIVE_MAX_DRAWDOWN
    if (
        variant_type == "dynamic_top2_drawdown_to_top4"
        or run.startswith("dynamic_")
        or (daily_risk_mode and daily_risk_mode != "none" and daily_risk_mode != "nan")
    ):
        return "dynamic_or_daily_risk_research", "research_only", DEFAULT_BALANCED_MAX_DRAWDOWN
    if run.startswith("base_top2") or run.startswith("top2_"):
        return "aggressive_research", "research_only", DEFAULT_BALANCED_MAX_DRAWDOWN
    if run.startswith("base_top4") or run.startswith("top4_"):
        return "robust_baseline", "fallback", DEFAULT_ROBUST_BASELINE_MAX_DRAWDOWN
    if run.startswith("blend_top2_25_top4_75") or (pd.notna(top2_weight) and abs(top2_weight - 0.25) < 1e-9):
        return "conservative_live_design", "conservative", DEFAULT_CONSERVATIVE_MAX_DRAWDOWN
    if (
        run.startswith("blend_top2_50_top4_50")
        or run.startswith("blend50_monthly_none")
        or (pd.notna(top2_weight) and abs(top2_weight - 0.50) < 1e-9 and daily_risk_mode in {"", "none", "nan"})
    ):
        return "balanced_offensive_live_design", "balanced_offensive", DEFAULT_BALANCED_MAX_DRAWDOWN
    if run.startswith("blend_top2_75_top4_25") or (pd.notna(top2_weight) and abs(top2_weight - 0.75) < 1e-9):
        return "aggressive_blend_research", "research_only", DEFAULT_BALANCED_MAX_DRAWDOWN
    if "top3" in run:
        return "higher_return_research", "research_only", DEFAULT_CONSERVATIVE_MAX_DRAWDOWN
    return "research_only", "research_only", DEFAULT_CONSERVATIVE_MAX_DRAWDOWN


def _rolling_metrics(rolling: pd.DataFrame, run: str) -> dict[str, object]:
    frame = pd.DataFrame(rolling).copy()
    if frame.empty or "Run" not in frame.columns:
        return {}
    frame = frame.loc[frame["Run"].astype(str).eq(str(run))].copy()
    if frame.empty:
        return {}
    frame["Window Years"] = pd.to_numeric(frame.get("Window Years"), errors="coerce")
    frame["Strategy CAGR"] = pd.to_numeric(frame.get("Strategy CAGR"), errors="coerce")
    frame["QQQ CAGR"] = pd.to_numeric(frame.get("QQQ CAGR"), errors="coerce")
    frame["SPY CAGR"] = pd.to_numeric(frame.get("SPY CAGR"), errors="coerce")
    frame["Strategy Max Drawdown"] = pd.to_numeric(frame.get("Strategy Max Drawdown"), errors="coerce")
    frame["qqq_excess"] = frame["Strategy CAGR"] - frame["QQQ CAGR"]
    frame["spy_excess"] = frame["Strategy CAGR"] - frame["SPY CAGR"]

    metrics: dict[str, object] = {
        "Worst Rolling Max Drawdown": float(frame["Strategy Max Drawdown"].min())
        if frame["Strategy Max Drawdown"].notna().any()
        else float("nan")
    }
    for window in (3, 5):
        subset = frame.loc[frame["Window Years"].eq(window)]
        if subset.empty:
            metrics[f"Min {window}Y QQQ Excess CAGR"] = float("nan")
            metrics[f"Min {window}Y SPY Excess CAGR"] = float("nan")
            metrics[f"Worst {window}Y QQQ Excess Window"] = ""
            continue
        qqq_idx = subset["qqq_excess"].idxmin()
        qqq_row = subset.loc[qqq_idx] if pd.notna(qqq_idx) else None
        metrics[f"Min {window}Y QQQ Excess CAGR"] = float(subset["qqq_excess"].min())
        metrics[f"Min {window}Y SPY Excess CAGR"] = float(subset["spy_excess"].min())
        metrics[f"Worst {window}Y QQQ Excess Window"] = _window_label(qqq_row)
    return metrics


def _recommended_action(role: str, passed: bool) -> str:
    if not passed:
        return "research_only"
    if role == "conservative_live_design":
        return "live_design_review_conservative"
    if role == "balanced_offensive_live_design":
        return "live_design_review_balanced_offensive"
    if role == "robust_baseline":
        return "fallback_live_design_review"
    return "research_only"


def _evaluate_row(
    row: pd.Series,
    rolling_metrics: dict[str, object],
    *,
    required_universe_lag_days: int,
    min_5y_excess_cagr_vs_qqq: float,
    min_5y_excess_cagr_vs_spy: float,
    min_3y_excess_cagr_vs_spy: float,
) -> dict[str, object]:
    role, profile, allowed_max_drawdown = _candidate_role(row)
    reasons: list[str] = []
    lag = _number(row.get("Universe Lag Trading Days"), default=float("nan"))
    max_drawdown = _number(row.get("Max Drawdown"))
    total_return = _number(row.get("Total Return"))
    benchmark_total_return = _number(row.get("Benchmark Total Return"))
    broad_benchmark_total_return = _number(row.get("Broad Benchmark Total Return"))
    min_5y_qqq = _number(rolling_metrics.get("Min 5Y QQQ Excess CAGR"))
    min_5y_spy = _number(rolling_metrics.get("Min 5Y SPY Excess CAGR"))
    min_3y_spy = _number(rolling_metrics.get("Min 3Y SPY Excess CAGR"))

    if profile == "research_only":
        if role == "dynamic_or_daily_risk_research":
            reasons.append("dynamic_or_daily_risk_candidate")
        else:
            reasons.append("research_only_role")
    if pd.isna(lag) or int(lag) != int(required_universe_lag_days):
        reasons.append(f"universe_lag_not_{int(required_universe_lag_days)}")
    if pd.isna(max_drawdown) or max_drawdown < allowed_max_drawdown:
        reasons.append(f"max_drawdown_below_{_pct(allowed_max_drawdown)}")
    if pd.isna(total_return) or pd.isna(benchmark_total_return) or total_return <= benchmark_total_return:
        reasons.append("full_total_return_not_above_qqq")
    if pd.isna(total_return) or pd.isna(broad_benchmark_total_return) or total_return <= broad_benchmark_total_return:
        reasons.append("full_total_return_not_above_spy")
    if pd.isna(min_5y_qqq) or min_5y_qqq < float(min_5y_excess_cagr_vs_qqq):
        reasons.append(f"min_5y_qqq_excess_below_{_pct(min_5y_excess_cagr_vs_qqq)}")
    if pd.isna(min_5y_spy) or min_5y_spy < float(min_5y_excess_cagr_vs_spy):
        reasons.append(f"min_5y_spy_excess_below_{_pct(min_5y_excess_cagr_vs_spy)}")
    if pd.isna(min_3y_spy) or min_3y_spy < float(min_3y_excess_cagr_vs_spy):
        reasons.append(f"min_3y_spy_excess_below_{_pct(min_3y_excess_cagr_vs_spy)}")

    passed = not reasons
    output = {
        "Run": row.get("Run"),
        "Candidate Role": role,
        "Gate Profile": profile,
        "Start": row.get("Start"),
        "End": row.get("End"),
        "Universe Lag Trading Days": lag,
        "CAGR": _number(row.get("CAGR")),
        "Max Drawdown": max_drawdown,
        "Sharpe": _number(row.get("Sharpe")),
        "Calmar": _number(row.get("Calmar")),
        "Total Return": total_return,
        "Benchmark Total Return": benchmark_total_return,
        "Broad Benchmark Total Return": broad_benchmark_total_return,
        "Turnover/Year": _number(row.get("Turnover/Year")),
        "Allowed Max Drawdown": float(allowed_max_drawdown),
        "live_gate_passed": bool(passed),
        "live_gate_reason": "pass" if passed else ";".join(reasons),
        "recommended_action": _recommended_action(role, passed),
    }
    output.update(rolling_metrics)
    return output


def evaluate_live_readiness(
    summary,
    rolling,
    *,
    required_universe_lag_days: int = DEFAULT_REQUIRED_UNIVERSE_LAG_DAYS,
    min_5y_excess_cagr_vs_qqq: float = DEFAULT_MIN_5Y_EXCESS_CAGR_VS_QQQ,
    min_5y_excess_cagr_vs_spy: float = DEFAULT_MIN_5Y_EXCESS_CAGR_VS_SPY,
    min_3y_excess_cagr_vs_spy: float = DEFAULT_MIN_3Y_EXCESS_CAGR_VS_SPY,
) -> pd.DataFrame:
    summary_frame = pd.DataFrame(summary).copy()
    rolling_frame = pd.DataFrame(rolling).copy()
    if summary_frame.empty:
        return pd.DataFrame(columns=LIVE_READINESS_COLUMNS)
    if "Run" not in summary_frame.columns:
        raise ValueError("summary must contain Run column")
    rows = [
        _evaluate_row(
            row,
            _rolling_metrics(rolling_frame, str(row["Run"])),
            required_universe_lag_days=int(required_universe_lag_days),
            min_5y_excess_cagr_vs_qqq=float(min_5y_excess_cagr_vs_qqq),
            min_5y_excess_cagr_vs_spy=float(min_5y_excess_cagr_vs_spy),
            min_3y_excess_cagr_vs_spy=float(min_3y_excess_cagr_vs_spy),
        )
        for _, row in summary_frame.iterrows()
    ]
    output = pd.DataFrame(rows)
    return output.loc[:, [column for column in LIVE_READINESS_COLUMNS if column in output.columns]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate Russell Top50 leader-rotation live-readiness gates.")
    parser.add_argument("--summary", required=True, help="Validation or concentration summary CSV")
    parser.add_argument("--rolling", required=True, help="Rolling-window summary CSV matching --summary")
    parser.add_argument("--output-dir", required=True, help="Directory for live-readiness output")
    parser.add_argument("--output-name", default="live_readiness_summary.csv")
    parser.add_argument("--required-universe-lag-days", type=int, default=DEFAULT_REQUIRED_UNIVERSE_LAG_DAYS)
    parser.add_argument("--min-5y-excess-cagr-vs-qqq", type=float, default=DEFAULT_MIN_5Y_EXCESS_CAGR_VS_QQQ)
    parser.add_argument("--min-5y-excess-cagr-vs-spy", type=float, default=DEFAULT_MIN_5Y_EXCESS_CAGR_VS_SPY)
    parser.add_argument("--min-3y-excess-cagr-vs-spy", type=float, default=DEFAULT_MIN_3Y_EXCESS_CAGR_VS_SPY)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = evaluate_live_readiness(
        read_table(args.summary),
        read_table(args.rolling),
        required_universe_lag_days=args.required_universe_lag_days,
        min_5y_excess_cagr_vs_qqq=args.min_5y_excess_cagr_vs_qqq,
        min_5y_excess_cagr_vs_spy=args.min_5y_excess_cagr_vs_spy,
        min_3y_excess_cagr_vs_spy=args.min_3y_excess_cagr_vs_spy,
    )
    output_path = output_dir / args.output_name
    result.to_csv(output_path, index=False)
    print(result.head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote live-readiness summary -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
