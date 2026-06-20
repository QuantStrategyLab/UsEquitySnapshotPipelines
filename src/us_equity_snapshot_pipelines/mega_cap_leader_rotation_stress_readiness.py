from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd

from .mega_cap_leader_rotation_concentration_variants import run_concentration_variant_research
from .mega_cap_leader_rotation_dynamic_validation import DEFAULT_ROLLING_WINDOW_YEARS, parse_csv_ints
from .mega_cap_leader_rotation_live_readiness import evaluate_live_readiness
from .russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_FIXED_BLEND_TOP2_WEIGHTS = (0.25, 0.50)
DEFAULT_STRESS_TURNOVER_COST_BPS = (5.0, 10.0, 15.0, 25.0)
DEFAULT_STRESS_UNIVERSE_LAG_DAYS = (21, 42)
DEFAULT_STRESS_MIN_ADV20_USD = (20_000_000.0,)
DEFAULT_FIXED_LIVE_CANDIDATE_RUNS = (
    "base_top4_cap25",
    "blend_top2_25_top4_75",
    "blend_top2_50_top4_50",
)
DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD = 0.10
DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD = 0.03
DEFAULT_PANIC_GUARD_VOL_THRESHOLD = 0.25
DEFAULT_PANIC_GUARD_STOCK_EXPOSURE = 0.50
STRESS_DETAIL_COLUMNS = (
    "Stress Scenario",
    "Stress Turnover Cost Bps",
    "Stress Universe Lag Trading Days",
    "Stress Min ADV20 USD",
    "Run",
    "Candidate Role",
    "Gate Profile",
    "Start",
    "End",
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
    "live_gate_passed",
    "live_gate_reason",
    "metric_gate_passed_excluding_research_role",
    "metric_gate_reason_excluding_research_role",
    "recommended_action",
)
STRESS_SUMMARY_COLUMNS = (
    "Run",
    "Candidate Role",
    "Gate Profile",
    "Stress Scenarios",
    "Passed Scenarios",
    "all_stress_gates_passed",
    "stress_gate_reason",
    "Metric Passed Scenarios",
    "all_metric_gates_passed_excluding_research_role",
    "metric_stress_gate_reason",
    "Worst Max Drawdown",
    "Worst Rolling Max Drawdown",
    "Min 3Y QQQ Excess CAGR",
    "Min 3Y SPY Excess CAGR",
    "Min 5Y QQQ Excess CAGR",
    "Min 5Y SPY Excess CAGR",
    "Max Turnover/Year",
    "Max Stress Turnover Cost Bps",
    "Max Stress Universe Lag Trading Days",
    "Max Stress Min ADV20 USD",
    "recommended_action",
)


def parse_csv_floats_no_percent(raw_value: str | Iterable[float] | None, *, default: tuple[float, ...]) -> tuple[float, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed: list[float] = []
    seen: set[float] = set()
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        number = float(text)
        if number in seen:
            continue
        seen.add(number)
        parsed.append(number)
    return tuple(parsed) or default


def parse_csv_strings(raw_value: str | Iterable[str] | None, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw_value is None:
        return default
    values = raw_value.split(",") if isinstance(raw_value, str) else list(raw_value)
    parsed = tuple(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))
    return parsed or default


def _scenario_label(*, turnover_cost_bps: float, universe_lag_days: int, min_adv20_usd: float) -> str:
    adv_millions = float(min_adv20_usd) / 1_000_000.0
    return f"cost{float(turnover_cost_bps):g}bps_lag{int(universe_lag_days)}d_adv{adv_millions:g}m"


def _ordered_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=columns)
    known = [column for column in columns if column in frame.columns]
    extra = [column for column in frame.columns if column not in known]
    return frame.loc[:, known + extra]


def _numeric_min(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return float("nan")
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.min()) if values.notna().any() else float("nan")


def _numeric_max(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns:
        return float("nan")
    values = pd.to_numeric(frame[column], errors="coerce")
    return float(values.max()) if values.notna().any() else float("nan")


def _metric_reason_excluding_research_role(reason: object) -> str:
    parts = [
        part
        for part in str(reason or "").split(";")
        if part and part not in {"pass", "research_only_role"}
    ]
    return ";".join(parts) if parts else "pass"


def summarize_stress_live_readiness(detail: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(detail).copy()
    if frame.empty:
        return pd.DataFrame(columns=STRESS_SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    for run, group in frame.groupby("Run", sort=False):
        passed = group["live_gate_passed"].astype(bool) if "live_gate_passed" in group.columns else pd.Series(False)
        failed = group.loc[~passed.reindex(group.index, fill_value=False)].copy()
        reasons = tuple(
            dict.fromkeys(
                str(reason)
                for reason in failed.get("live_gate_reason", pd.Series(dtype=object)).tolist()
                if str(reason).strip() and str(reason).strip().lower() != "nan"
            )
        )
        all_passed = bool(len(group) > 0 and passed.all())
        metric_passed = (
            group["metric_gate_passed_excluding_research_role"].astype(bool)
            if "metric_gate_passed_excluding_research_role" in group.columns
            else passed
        )
        metric_failed = group.loc[~metric_passed.reindex(group.index, fill_value=False)].copy()
        metric_reasons = tuple(
            dict.fromkeys(
                str(reason)
                for reason in metric_failed.get(
                    "metric_gate_reason_excluding_research_role",
                    pd.Series(dtype=object),
                ).tolist()
                if str(reason).strip() and str(reason).strip().lower() not in {"nan", "pass"}
            )
        )
        all_metric_passed = bool(len(group) > 0 and metric_passed.all())
        first = group.iloc[0]
        rows.append(
            {
                "Run": run,
                "Candidate Role": first.get("Candidate Role", ""),
                "Gate Profile": first.get("Gate Profile", ""),
                "Stress Scenarios": int(len(group)),
                "Passed Scenarios": int(passed.sum()),
                "all_stress_gates_passed": all_passed,
                "stress_gate_reason": "pass" if all_passed else ";".join(reasons) or "failed_stress_scenario",
                "Metric Passed Scenarios": int(metric_passed.sum()),
                "all_metric_gates_passed_excluding_research_role": all_metric_passed,
                "metric_stress_gate_reason": (
                    "pass" if all_metric_passed else ";".join(metric_reasons) or "failed_metric_stress_scenario"
                ),
                "Worst Max Drawdown": _numeric_min(group, "Max Drawdown"),
                "Worst Rolling Max Drawdown": _numeric_min(group, "Worst Rolling Max Drawdown"),
                "Min 3Y QQQ Excess CAGR": _numeric_min(group, "Min 3Y QQQ Excess CAGR"),
                "Min 3Y SPY Excess CAGR": _numeric_min(group, "Min 3Y SPY Excess CAGR"),
                "Min 5Y QQQ Excess CAGR": _numeric_min(group, "Min 5Y QQQ Excess CAGR"),
                "Min 5Y SPY Excess CAGR": _numeric_min(group, "Min 5Y SPY Excess CAGR"),
                "Max Turnover/Year": _numeric_max(group, "Turnover/Year"),
                "Max Stress Turnover Cost Bps": _numeric_max(group, "Stress Turnover Cost Bps"),
                "Max Stress Universe Lag Trading Days": _numeric_max(group, "Stress Universe Lag Trading Days"),
                "Max Stress Min ADV20 USD": _numeric_max(group, "Stress Min ADV20 USD"),
                "recommended_action": "stress_live_design_review" if all_passed else "research_only",
            }
        )
    return _ordered_columns(pd.DataFrame(rows), STRESS_SUMMARY_COLUMNS)


def build_stress_live_readiness(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    turnover_cost_bps_values: Iterable[float] = DEFAULT_STRESS_TURNOVER_COST_BPS,
    universe_lag_days_values: Iterable[int] = DEFAULT_STRESS_UNIVERSE_LAG_DAYS,
    min_adv20_usd_values: Iterable[float] = DEFAULT_STRESS_MIN_ADV20_USD,
    blend_top2_weights: Iterable[float] = DEFAULT_FIXED_BLEND_TOP2_WEIGHTS,
    candidate_runs: Iterable[str] = DEFAULT_FIXED_LIVE_CANDIDATE_RUNS,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    min_price_usd: float = 10.0,
    min_history_days: int = 273,
    min_5y_excess_cagr_vs_qqq: float = 0.0,
    min_5y_excess_cagr_vs_spy: float = 0.0,
    min_3y_excess_cagr_vs_spy: float = 0.0,
    include_panic_rebound_guard_variants: bool = False,
    panic_guard_drawdown_threshold: float = DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD,
    panic_guard_rebound_threshold: float = DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD,
    panic_guard_vol_threshold: float = DEFAULT_PANIC_GUARD_VOL_THRESHOLD,
    panic_guard_stock_exposure: float = DEFAULT_PANIC_GUARD_STOCK_EXPOSURE,
) -> dict[str, pd.DataFrame]:
    costs = tuple(float(value) for value in turnover_cost_bps_values)
    lags = tuple(int(value) for value in universe_lag_days_values)
    adv_values = tuple(float(value) for value in min_adv20_usd_values)
    candidates = set(parse_csv_strings(tuple(candidate_runs), default=DEFAULT_FIXED_LIVE_CANDIDATE_RUNS))
    detail_frames: list[pd.DataFrame] = []

    for cost_bps in costs:
        for lag_days in lags:
            for min_adv20_usd in adv_values:
                scenario = _scenario_label(
                    turnover_cost_bps=cost_bps,
                    universe_lag_days=lag_days,
                    min_adv20_usd=min_adv20_usd,
                )
                result = run_concentration_variant_research(
                    price_history,
                    universe_history,
                    start_date=start_date,
                    end_date=end_date,
                    universe_lag_trading_days=lag_days,
                    blend_top2_weights=blend_top2_weights,
                    dynamic_drawdown_thresholds=(0.0,),
                    rolling_window_years=rolling_window_years,
                    turnover_cost_bps=cost_bps,
                    min_price_usd=min_price_usd,
                    min_adv20_usd=min_adv20_usd,
                    min_history_days=min_history_days,
                    include_panic_rebound_guard_variants=include_panic_rebound_guard_variants,
                    panic_guard_drawdown_threshold=panic_guard_drawdown_threshold,
                    panic_guard_rebound_threshold=panic_guard_rebound_threshold,
                    panic_guard_vol_threshold=panic_guard_vol_threshold,
                    panic_guard_stock_exposure=panic_guard_stock_exposure,
                )
                live_readiness = evaluate_live_readiness(
                    result["concentration_variant_summary"],
                    result["concentration_variant_rolling_summary"],
                    required_universe_lag_days=lag_days,
                    min_5y_excess_cagr_vs_qqq=min_5y_excess_cagr_vs_qqq,
                    min_5y_excess_cagr_vs_spy=min_5y_excess_cagr_vs_spy,
                    min_3y_excess_cagr_vs_spy=min_3y_excess_cagr_vs_spy,
                )
                if candidates and not live_readiness.empty:
                    live_readiness = live_readiness.loc[live_readiness["Run"].astype(str).isin(candidates)].copy()
                if live_readiness.empty:
                    continue
                live_readiness["metric_gate_reason_excluding_research_role"] = live_readiness[
                    "live_gate_reason"
                ].map(_metric_reason_excluding_research_role)
                live_readiness["metric_gate_passed_excluding_research_role"] = live_readiness[
                    "metric_gate_reason_excluding_research_role"
                ].eq("pass")
                live_readiness.insert(0, "Stress Min ADV20 USD", float(min_adv20_usd))
                live_readiness.insert(0, "Stress Universe Lag Trading Days", int(lag_days))
                live_readiness.insert(0, "Stress Turnover Cost Bps", float(cost_bps))
                live_readiness.insert(0, "Stress Scenario", scenario)
                detail_frames.append(_ordered_columns(live_readiness, STRESS_DETAIL_COLUMNS))

    detail = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame(columns=STRESS_DETAIL_COLUMNS)
    summary = summarize_stress_live_readiness(detail)
    return {
        "stress_live_readiness_detail": _ordered_columns(detail, STRESS_DETAIL_COLUMNS),
        "stress_live_readiness_summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run stress live-readiness checks for fixed Russell Top50 leader-rotation candidates."
    )
    parser.add_argument("--prices", required=True, help="Input price history file")
    parser.add_argument("--universe", required=True, help="Input dynamic universe history file")
    parser.add_argument("--output-dir", required=True, help="Directory for stress readiness outputs")
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument(
        "--turnover-cost-bps-values",
        default=",".join(str(value) for value in DEFAULT_STRESS_TURNOVER_COST_BPS),
        help="Comma-separated turnover cost stress levels in basis points",
    )
    parser.add_argument(
        "--universe-lag-days-values",
        default=",".join(str(value) for value in DEFAULT_STRESS_UNIVERSE_LAG_DAYS),
        help="Comma-separated universe source lag stress levels in trading days",
    )
    parser.add_argument(
        "--min-adv20-usd-values",
        default=",".join(str(value) for value in DEFAULT_STRESS_MIN_ADV20_USD),
        help="Comma-separated ADV20 liquidity stress floors in USD",
    )
    parser.add_argument(
        "--blend-top2-weights",
        default=",".join(str(value) for value in DEFAULT_FIXED_BLEND_TOP2_WEIGHTS),
        help="Comma-separated fixed Top2 sleeve weights; default covers conservative and balanced variants",
    )
    parser.add_argument(
        "--candidate-runs",
        default=",".join(DEFAULT_FIXED_LIVE_CANDIDATE_RUNS),
        help="Comma-separated runs to keep in the stress output",
    )
    parser.add_argument(
        "--rolling-window-years",
        default="",
        help="Comma-separated complete-calendar-year rolling windows to summarize, for example 3,5",
    )
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--min-5y-excess-cagr-vs-qqq", type=float, default=0.0)
    parser.add_argument("--min-5y-excess-cagr-vs-spy", type=float, default=0.0)
    parser.add_argument("--min-3y-excess-cagr-vs-spy", type=float, default=0.0)
    parser.add_argument(
        "--include-panic-rebound-guard-variants",
        action="store_true",
        help="Include canonical panic-rebound guard variants in each stress scenario.",
    )
    parser.add_argument(
        "--panic-guard-drawdown-threshold",
        type=float,
        default=DEFAULT_PANIC_GUARD_DRAWDOWN_THRESHOLD,
    )
    parser.add_argument(
        "--panic-guard-rebound-threshold",
        type=float,
        default=DEFAULT_PANIC_GUARD_REBOUND_THRESHOLD,
    )
    parser.add_argument(
        "--panic-guard-vol-threshold",
        type=float,
        default=DEFAULT_PANIC_GUARD_VOL_THRESHOLD,
    )
    parser.add_argument(
        "--panic-guard-stock-exposure",
        type=float,
        default=DEFAULT_PANIC_GUARD_STOCK_EXPOSURE,
    )
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = build_stress_live_readiness(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        turnover_cost_bps_values=parse_csv_floats_no_percent(
            args.turnover_cost_bps_values,
            default=DEFAULT_STRESS_TURNOVER_COST_BPS,
        ),
        universe_lag_days_values=parse_csv_ints(args.universe_lag_days_values, default=DEFAULT_STRESS_UNIVERSE_LAG_DAYS),
        min_adv20_usd_values=parse_csv_floats_no_percent(
            args.min_adv20_usd_values,
            default=DEFAULT_STRESS_MIN_ADV20_USD,
        ),
        blend_top2_weights=parse_csv_floats_no_percent(
            args.blend_top2_weights,
            default=DEFAULT_FIXED_BLEND_TOP2_WEIGHTS,
        ),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=DEFAULT_FIXED_LIVE_CANDIDATE_RUNS),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        min_price_usd=args.min_price_usd,
        min_history_days=args.min_history_days,
        min_5y_excess_cagr_vs_qqq=args.min_5y_excess_cagr_vs_qqq,
        min_5y_excess_cagr_vs_spy=args.min_5y_excess_cagr_vs_spy,
        min_3y_excess_cagr_vs_spy=args.min_3y_excess_cagr_vs_spy,
        include_panic_rebound_guard_variants=bool(args.include_panic_rebound_guard_variants),
        panic_guard_drawdown_threshold=float(args.panic_guard_drawdown_threshold),
        panic_guard_rebound_threshold=float(args.panic_guard_rebound_threshold),
        panic_guard_vol_threshold=float(args.panic_guard_vol_threshold),
        panic_guard_stock_exposure=float(args.panic_guard_stock_exposure),
    )
    detail_path = output_dir / "stress_live_readiness_detail.csv"
    summary_path = output_dir / "stress_live_readiness_summary.csv"
    result["stress_live_readiness_detail"].to_csv(detail_path, index=False)
    result["stress_live_readiness_summary"].to_csv(summary_path, index=False)
    print(result["stress_live_readiness_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote stress readiness detail -> {detail_path}")
    print(f"wrote stress readiness summary -> {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
