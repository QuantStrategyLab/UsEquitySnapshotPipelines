from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .mega_cap_leader_rotation_crash_brake_research import (
    DEFAULT_DRAWDOWN_THRESHOLD,
    DEFAULT_FLOOR_TOP2_WEIGHT,
    DEFAULT_ROLLING_WINDOW_YEARS,
    run_crash_brake_research,
)
from .mega_cap_leader_rotation_stress_readiness import (
    parse_csv_floats_no_percent,
    parse_csv_ints,
    parse_csv_strings,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table

CRASH_BRAKE_STRESS_FOLLOWUP_SCHEMA_VERSION = "russell_top50_crash_brake_stress_followup.v1"
DEFAULT_TURNOVER_COST_BPS_VALUES = (5.0, 10.0, 15.0, 25.0)
DEFAULT_UNIVERSE_LAG_DAYS_VALUES = (21, 42)
DEFAULT_MIN_ADV20_USD_VALUES = (20_000_000.0,)
DEFAULT_ALLOWED_CAGR_SHORTFALL = 0.03
DEFAULT_ALLOWED_DRAWDOWN_WORSE = 0.02

STRESS_DETAIL_COLUMNS = (
    "Stress Scenario",
    "Stress Turnover Cost Bps",
    "Stress Universe Lag Trading Days",
    "Stress Min ADV20 USD",
    "Run",
    "Candidate Role",
    "Gate Profile",
    "CAGR",
    "Max Drawdown",
    "Sharpe",
    "Turnover/Year",
    "Reference Run",
    "Reference CAGR",
    "Reference Max Drawdown",
    "CAGR Delta Vs Reference",
    "Drawdown Delta Vs Reference",
    "stress_gate_passed",
    "stress_gate_reason",
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
    "Worst Max Drawdown",
    "Min CAGR Delta Vs Reference",
    "Worst Drawdown Delta Vs Reference",
    "Max Turnover/Year",
    "Max Stress Turnover Cost Bps",
    "Max Stress Universe Lag Trading Days",
    "Max Stress Min ADV20 USD",
    "recommended_action",
)


def _candidate_role_from_run(run: str) -> tuple[str, str]:
    normalized = str(run or "").strip()
    if normalized == "crash_brake_top2_50_floor25":
        return "panic_rebound_guard_research", "research_only"
    if normalized == "blend_top2_50_top4_50_no_brake":
        return "balanced_offensive_live_design", "balanced_offensive_reference"
    if normalized == "blend_top2_25_top4_75_no_brake":
        return "conservative_live_design", "conservative_reference"
    return "research_only", "research_only"


def _scenario_label(*, turnover_cost_bps: float, universe_lag_days: int, min_adv20_usd: float) -> str:
    adv_millions = float(min_adv20_usd) / 1_000_000.0
    return f"cost{float(turnover_cost_bps):g}bps_lag{int(universe_lag_days)}d_adv{adv_millions:g}m"


def _drawdown_not_too_worse(candidate_drawdown: float, reference_drawdown: float, tolerance: float) -> bool:
    return float(candidate_drawdown) >= float(reference_drawdown) - float(tolerance)


def _summarize(detail: pd.DataFrame) -> pd.DataFrame:
    frame = pd.DataFrame(detail).copy()
    if frame.empty:
        return pd.DataFrame(columns=STRESS_SUMMARY_COLUMNS)
    rows: list[dict[str, object]] = []
    for run, group in frame.groupby("Run", sort=False):
        passed = group["stress_gate_passed"].astype(bool)
        failed = group.loc[~passed].copy()
        reasons = tuple(
            dict.fromkeys(
                str(reason)
                for reason in failed.get("stress_gate_reason", pd.Series(dtype=object)).tolist()
                if str(reason).strip() and str(reason).strip().lower() != "nan"
            )
        )
        first = group.iloc[0]
        all_passed = bool(len(group) > 0 and passed.all())
        rows.append(
            {
                "Run": run,
                "Candidate Role": first.get("Candidate Role", ""),
                "Gate Profile": first.get("Gate Profile", ""),
                "Stress Scenarios": int(len(group)),
                "Passed Scenarios": int(passed.sum()),
                "all_stress_gates_passed": all_passed,
                "stress_gate_reason": "pass" if all_passed else ";".join(reasons) or "failed_stress_scenario",
                "Worst Max Drawdown": float(pd.to_numeric(group["Max Drawdown"], errors="coerce").min()),
                "Min CAGR Delta Vs Reference": float(
                    pd.to_numeric(group["CAGR Delta Vs Reference"], errors="coerce").min()
                ),
                "Worst Drawdown Delta Vs Reference": float(
                    pd.to_numeric(group["Drawdown Delta Vs Reference"], errors="coerce").min()
                ),
                "Max Turnover/Year": float(pd.to_numeric(group["Turnover/Year"], errors="coerce").max()),
                "Max Stress Turnover Cost Bps": float(
                    pd.to_numeric(group["Stress Turnover Cost Bps"], errors="coerce").max()
                ),
                "Max Stress Universe Lag Trading Days": float(
                    pd.to_numeric(group["Stress Universe Lag Trading Days"], errors="coerce").max()
                ),
                "Max Stress Min ADV20 USD": float(
                    pd.to_numeric(group["Stress Min ADV20 USD"], errors="coerce").max()
                ),
                "recommended_action": (
                    "crash_brake_stress_followup_passed_continue_gate_collection"
                    if all_passed
                    else "keep_research_only_stress_failed"
                ),
            }
        )
    return pd.DataFrame(rows).loc[:, list(STRESS_SUMMARY_COLUMNS)]


def build_crash_brake_stress_followup(
    price_history,
    universe_history,
    *,
    start_date: str | None = "2017-10-02",
    end_date: str | None = None,
    turnover_cost_bps_values: Iterable[float] = DEFAULT_TURNOVER_COST_BPS_VALUES,
    universe_lag_days_values: Iterable[int] = DEFAULT_UNIVERSE_LAG_DAYS_VALUES,
    min_adv20_usd_values: Iterable[float] = DEFAULT_MIN_ADV20_USD_VALUES,
    rolling_window_years: Iterable[int] = DEFAULT_ROLLING_WINDOW_YEARS,
    baseline_top2_weight: float = 0.50,
    floor_top2_weight: float = DEFAULT_FLOOR_TOP2_WEIGHT,
    drawdown_threshold: float = DEFAULT_DRAWDOWN_THRESHOLD,
    min_price_usd: float = 10.0,
    min_history_days: int = 273,
    candidate_runs: Iterable[str] = ("crash_brake_top2_50_floor25",),
    reference_run: str = "blend_top2_50_top4_50_no_brake",
    allowed_cagr_shortfall: float = DEFAULT_ALLOWED_CAGR_SHORTFALL,
    allowed_drawdown_worse: float = DEFAULT_ALLOWED_DRAWDOWN_WORSE,
) -> dict[str, pd.DataFrame]:
    candidates = tuple(str(run).strip() for run in candidate_runs if str(run).strip())
    detail_rows: list[dict[str, object]] = []
    for turnover_cost_bps in turnover_cost_bps_values:
        for universe_lag_days in universe_lag_days_values:
            for min_adv20_usd in min_adv20_usd_values:
                scenario = _scenario_label(
                    turnover_cost_bps=float(turnover_cost_bps),
                    universe_lag_days=int(universe_lag_days),
                    min_adv20_usd=float(min_adv20_usd),
                )
                result = run_crash_brake_research(
                    price_history,
                    universe_history,
                    start_date=start_date,
                    end_date=end_date,
                    universe_lag_trading_days=int(universe_lag_days),
                    rolling_window_years=rolling_window_years,
                    baseline_top2_weight=float(baseline_top2_weight),
                    floor_top2_weight=float(floor_top2_weight),
                    drawdown_threshold=float(drawdown_threshold),
                    turnover_cost_bps=float(turnover_cost_bps),
                    min_price_usd=float(min_price_usd),
                    min_adv20_usd=float(min_adv20_usd),
                    min_history_days=int(min_history_days),
                )
                summary = pd.DataFrame(result["crash_brake_summary"]).copy()
                if summary.empty:
                    continue
                indexed = summary.set_index("Run")
                if reference_run not in indexed.index:
                    raise ValueError(f"reference run missing from crash-brake stress scenario: {reference_run}")
                reference = indexed.loc[reference_run]
                for run in candidates:
                    if run not in indexed.index:
                        continue
                    row = indexed.loc[run]
                    candidate_role, gate_profile = _candidate_role_from_run(run)
                    cagr_delta = float(row.get("CAGR", float("nan"))) - float(reference.get("CAGR", float("nan")))
                    drawdown_delta = float(row.get("Max Drawdown", float("nan"))) - float(
                        reference.get("Max Drawdown", float("nan"))
                    )
                    reasons: list[str] = []
                    cagr_value = float(row.get("CAGR", float("nan")))
                    candidate_drawdown = float(row.get("Max Drawdown", float("nan")))
                    reference_drawdown = float(reference.get("Max Drawdown", float("nan")))
                    if pd.isna(cagr_value) or cagr_value <= 0.0:
                        reasons.append("non_positive_cagr")
                    if pd.isna(candidate_drawdown) or not _drawdown_not_too_worse(
                        candidate_drawdown, reference_drawdown, float(allowed_drawdown_worse)
                    ):
                        reasons.append("drawdown_too_much_worse_than_reference")
                    if pd.isna(cagr_delta) or cagr_delta < -float(allowed_cagr_shortfall):
                        reasons.append("cagr_shortfall_vs_reference_too_large")
                    detail_rows.append(
                        {
                            "Stress Scenario": scenario,
                            "Stress Turnover Cost Bps": float(turnover_cost_bps),
                            "Stress Universe Lag Trading Days": int(universe_lag_days),
                            "Stress Min ADV20 USD": float(min_adv20_usd),
                            "Run": run,
                            "Candidate Role": candidate_role,
                            "Gate Profile": gate_profile,
                            "CAGR": row.get("CAGR"),
                            "Max Drawdown": row.get("Max Drawdown"),
                            "Sharpe": row.get("Sharpe"),
                            "Turnover/Year": row.get("Turnover/Year"),
                            "Reference Run": reference_run,
                            "Reference CAGR": reference.get("CAGR"),
                            "Reference Max Drawdown": reference.get("Max Drawdown"),
                            "CAGR Delta Vs Reference": cagr_delta,
                            "Drawdown Delta Vs Reference": drawdown_delta,
                            "stress_gate_passed": not reasons,
                            "stress_gate_reason": "pass" if not reasons else ";".join(reasons),
                            "recommended_action": (
                                "collect_liquidity_shadow_decay_followup"
                                if not reasons
                                else "keep_research_only_stress_failed"
                            ),
                        }
                    )
    detail = pd.DataFrame(detail_rows)
    if detail.empty:
        detail = pd.DataFrame(columns=STRESS_DETAIL_COLUMNS)
    else:
        detail = detail.loc[:, list(STRESS_DETAIL_COLUMNS)]
    summary = _summarize(detail)
    return {
        "crash_brake_stress_detail": detail,
        "crash_brake_stress_summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build crash-brake stress follow-up artifacts from repeated crash-brake research scenarios."
    )
    parser.add_argument("--prices", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--research-manifest", help="Optional crash_brake_research_manifest.json")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", dest="start_date", default="2017-10-02")
    parser.add_argument("--end", dest="end_date")
    parser.add_argument(
        "--turnover-cost-bps-values",
        default=",".join(str(value) for value in DEFAULT_TURNOVER_COST_BPS_VALUES),
    )
    parser.add_argument(
        "--universe-lag-days-values",
        default=",".join(str(value) for value in DEFAULT_UNIVERSE_LAG_DAYS_VALUES),
    )
    parser.add_argument(
        "--min-adv20-usd-values",
        default=",".join(str(int(value)) for value in DEFAULT_MIN_ADV20_USD_VALUES),
    )
    parser.add_argument("--rolling-window-years", default="")
    parser.add_argument("--baseline-top2-weight", type=float, default=0.50)
    parser.add_argument("--floor-top2-weight", type=float, default=DEFAULT_FLOOR_TOP2_WEIGHT)
    parser.add_argument("--drawdown-threshold", type=float, default=DEFAULT_DRAWDOWN_THRESHOLD)
    parser.add_argument("--min-price-usd", type=float, default=10.0)
    parser.add_argument("--min-history-days", type=int, default=273)
    parser.add_argument("--candidate-runs", default="crash_brake_top2_50_floor25")
    parser.add_argument("--reference-run", default="blend_top2_50_top4_50_no_brake")
    parser.add_argument("--allowed-cagr-shortfall", type=float, default=DEFAULT_ALLOWED_CAGR_SHORTFALL)
    parser.add_argument("--allowed-drawdown-worse", type=float, default=DEFAULT_ALLOWED_DRAWDOWN_WORSE)
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    research_manifest: dict[str, Any] = {}
    if args.research_manifest:
        research_manifest = json.loads(Path(args.research_manifest).read_text(encoding="utf-8"))

    result = build_crash_brake_stress_followup(
        read_table(args.prices),
        read_table(args.universe),
        start_date=args.start_date,
        end_date=args.end_date,
        turnover_cost_bps_values=parse_csv_floats_no_percent(
            args.turnover_cost_bps_values,
            default=DEFAULT_TURNOVER_COST_BPS_VALUES,
        ),
        universe_lag_days_values=parse_csv_ints(
            args.universe_lag_days_values,
            default=DEFAULT_UNIVERSE_LAG_DAYS_VALUES,
        ),
        min_adv20_usd_values=parse_csv_floats_no_percent(
            args.min_adv20_usd_values,
            default=DEFAULT_MIN_ADV20_USD_VALUES,
        ),
        rolling_window_years=parse_csv_ints(args.rolling_window_years, default=DEFAULT_ROLLING_WINDOW_YEARS),
        baseline_top2_weight=float(args.baseline_top2_weight),
        floor_top2_weight=float(args.floor_top2_weight),
        drawdown_threshold=float(args.drawdown_threshold),
        min_price_usd=float(args.min_price_usd),
        min_history_days=int(args.min_history_days),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=("crash_brake_top2_50_floor25",)),
        reference_run=str(args.reference_run),
        allowed_cagr_shortfall=float(args.allowed_cagr_shortfall),
        allowed_drawdown_worse=float(args.allowed_drawdown_worse),
    )
    detail_path = output_dir / "crash_brake_stress_detail.csv"
    summary_path = output_dir / "crash_brake_stress_summary.csv"
    result["crash_brake_stress_detail"].to_csv(detail_path, index=False)
    result["crash_brake_stress_summary"].to_csv(summary_path, index=False)
    manifest = {
        "manifest_type": "russell_top50_crash_brake_stress_followup",
        "artifact_schema_version": CRASH_BRAKE_STRESS_FOLLOWUP_SCHEMA_VERSION,
        "experiment_profile": str(research_manifest.get("experiment_profile", "") or ""),
        "candidate_runs": parse_csv_strings(args.candidate_runs, default=("crash_brake_top2_50_floor25",)),
        "reference_run": str(args.reference_run),
        "inputs": {
            "prices": str(args.prices),
            "universe": str(args.universe),
            "research_manifest": str(args.research_manifest or ""),
        },
        "row_counts": {
            "crash_brake_stress_detail": int(len(result["crash_brake_stress_detail"])),
            "crash_brake_stress_summary": int(len(result["crash_brake_stress_summary"])),
        },
        "artifacts": {
            "crash_brake_stress_detail": {"path": detail_path.name},
            "crash_brake_stress_summary": {"path": summary_path.name},
        },
        "outputs": [
            detail_path.name,
            summary_path.name,
            "crash_brake_stress_followup_manifest.json",
        ],
    }
    manifest_path = output_dir / "crash_brake_stress_followup_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(result["crash_brake_stress_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote crash-brake stress detail -> {detail_path}")
    print(f"wrote crash-brake stress summary -> {summary_path}")
    print(f"wrote crash-brake stress manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
