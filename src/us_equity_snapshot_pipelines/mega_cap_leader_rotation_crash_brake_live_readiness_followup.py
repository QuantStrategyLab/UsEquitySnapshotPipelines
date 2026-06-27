from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .mega_cap_leader_rotation_crash_brake_research import DEFAULT_CRASH_BRAKE_CANDIDATE_RUNS
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

CRASH_BRAKE_LIVE_READINESS_FOLLOWUP_SCHEMA_VERSION = "russell_top50_crash_brake_live_readiness_followup.v1"
DEFAULT_REFERENCE_RUN = "blend_top2_50_top4_50_no_brake"
DEFAULT_REQUIRED_UNIVERSE_LAG_DAYS = 21
DEFAULT_ALLOWED_CAGR_SHORTFALL = 0.03
DEFAULT_ALLOWED_DRAWDOWN_WORSE = 0.02
DEFAULT_MIN_DRAWDOWN_IMPROVEMENT = 0.005
DEFAULT_MIN_PANIC_BRAKE_MODE_SHARE = 0.05
LIVE_READINESS_COLUMNS = (
    "Run",
    "Candidate Role",
    "Gate Profile",
    "Reference Run",
    "Start",
    "End",
    "Universe Lag Trading Days",
    "CAGR",
    "Max Drawdown",
    "Sharpe",
    "Turnover/Year",
    "Panic Brake Mode Share",
    "Reference CAGR",
    "Reference Max Drawdown",
    "CAGR Delta Vs Reference",
    "Drawdown Delta Vs Reference",
    "Worst Rolling Max Drawdown",
    "Reference Worst Rolling Max Drawdown",
    "Worst Rolling Drawdown Delta Vs Reference",
    "Panic Floor Mode Share",
    "live_gate_passed",
    "live_gate_reason",
    "recommended_action",
)


def _pct(value: float) -> str:
    return f"{float(value):.2%}"


def _number(value, *, default: float = float("nan")) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return float(numeric) if pd.notna(numeric) else float(default)


def _candidate_role_from_run(run: str) -> tuple[str, str]:
    normalized = str(run or "").strip()
    if normalized == "crash_brake_top2_50_floor25":
        return "panic_rebound_guard_research", "research_only"
    if normalized == "blend_top2_50_top4_50_no_brake":
        return "balanced_offensive_live_design", "balanced_offensive_reference"
    if normalized == "blend_top2_25_top4_75_no_brake":
        return "conservative_live_design", "conservative_reference"
    return "research_only", "research_only"


def _drawdown_not_too_worse(candidate_drawdown: float, reference_drawdown: float, tolerance: float) -> bool:
    return float(candidate_drawdown) >= float(reference_drawdown) - float(tolerance)


def _worst_rolling_drawdown(rolling: pd.DataFrame, run: str) -> float:
    frame = pd.DataFrame(rolling).copy()
    if frame.empty or "Run" not in frame.columns:
        return float("nan")
    subset = frame.loc[frame["Run"].astype(str).eq(str(run))].copy()
    if subset.empty or "Strategy Max Drawdown" not in subset.columns:
        return float("nan")
    values = pd.to_numeric(subset["Strategy Max Drawdown"], errors="coerce")
    return float(values.min()) if values.notna().any() else float("nan")


def _panic_floor_mode_share(mode_history: pd.DataFrame | None) -> float:
    frame = pd.DataFrame(mode_history).copy() if mode_history is not None else pd.DataFrame()
    if frame.empty or "Mode" not in frame.columns:
        return float("nan")
    modes = frame["Mode"].astype(str).str.strip().str.lower()
    return float(modes.eq("floor").mean()) if len(modes) else float("nan")


def _recommended_action(run: str, passed: bool) -> str:
    normalized = str(run or "").strip()
    if normalized != "crash_brake_top2_50_floor25":
        return "keep_as_crash_brake_reference_only"
    if passed:
        return "crash_brake_live_gate_passed_continue_gate_collection"
    return "keep_research_only_live_gate_failed"


def _evaluate_candidate_row(
    row: pd.Series,
    reference: pd.Series,
    *,
    reference_run: str,
    required_universe_lag_days: int,
    allowed_cagr_shortfall: float,
    allowed_drawdown_worse: float,
    min_drawdown_improvement: float,
    min_panic_brake_mode_share: float,
    worst_rolling_drawdown: float,
    reference_worst_rolling_drawdown: float,
    panic_floor_mode_share: float,
) -> dict[str, Any]:
    run = str(row.get("Run", "")).strip()
    candidate_role, gate_profile = _candidate_role_from_run(run)
    reasons: list[str] = []

    lag = _number(row.get("Universe Lag Trading Days"))
    cagr = _number(row.get("CAGR"))
    max_drawdown = _number(row.get("Max Drawdown"))
    reference_cagr = _number(reference.get("CAGR"))
    reference_drawdown = _number(reference.get("Max Drawdown"))
    mode_share = _number(row.get("Panic Brake Mode Share"))
    cagr_delta = cagr - reference_cagr
    drawdown_delta = max_drawdown - reference_drawdown
    worst_rolling_delta = worst_rolling_drawdown - reference_worst_rolling_drawdown

    if pd.isna(lag) or int(lag) != int(required_universe_lag_days):
        reasons.append(f"universe_lag_not_{int(required_universe_lag_days)}")
    if pd.isna(cagr) or cagr <= 0.0:
        reasons.append("non_positive_cagr")
    if pd.isna(mode_share) or mode_share < float(min_panic_brake_mode_share):
        reasons.append(f"panic_brake_mode_share_below_{_pct(min_panic_brake_mode_share)}")
    if pd.notna(panic_floor_mode_share) and panic_floor_mode_share < float(min_panic_brake_mode_share):
        reasons.append(f"panic_floor_mode_share_below_{_pct(min_panic_brake_mode_share)}")

    risk_benefit = False
    if pd.notna(drawdown_delta) and drawdown_delta >= float(min_drawdown_improvement):
        risk_benefit = True
    elif pd.notna(worst_rolling_delta) and worst_rolling_delta >= float(min_drawdown_improvement):
        risk_benefit = True

    cagr_tradeoff = False
    if pd.notna(cagr_delta) and cagr_delta >= -float(allowed_cagr_shortfall):
        if pd.notna(max_drawdown) and pd.notna(reference_drawdown) and _drawdown_not_too_worse(
            max_drawdown,
            reference_drawdown,
            float(allowed_drawdown_worse),
        ):
            cagr_tradeoff = True

    if not risk_benefit and not cagr_tradeoff:
        if pd.isna(drawdown_delta) or drawdown_delta < float(min_drawdown_improvement):
            reasons.append(f"drawdown_improvement_below_{_pct(min_drawdown_improvement)}")
        if pd.isna(cagr_delta) or cagr_delta < -float(allowed_cagr_shortfall):
            reasons.append(f"cagr_shortfall_vs_reference_above_{_pct(allowed_cagr_shortfall)}")
        if pd.isna(max_drawdown) or not _drawdown_not_too_worse(
            max_drawdown,
            reference_drawdown,
            float(allowed_drawdown_worse),
        ):
            reasons.append("drawdown_too_much_worse_than_reference")

    passed = not reasons
    if passed and risk_benefit:
        gate_reason = "pass;risk_benefit_vs_reference"
    elif passed and cagr_tradeoff:
        gate_reason = "pass;acceptable_cagr_tradeoff_vs_reference"
    else:
        gate_reason = "pass" if passed else ";".join(reasons)

    return {
        "Run": run,
        "Candidate Role": candidate_role,
        "Gate Profile": gate_profile,
        "Reference Run": reference_run,
        "Start": row.get("Start"),
        "End": row.get("End"),
        "Universe Lag Trading Days": lag,
        "CAGR": cagr,
        "Max Drawdown": max_drawdown,
        "Sharpe": _number(row.get("Sharpe")),
        "Turnover/Year": _number(row.get("Turnover/Year")),
        "Panic Brake Mode Share": mode_share,
        "Reference CAGR": reference_cagr,
        "Reference Max Drawdown": reference_drawdown,
        "CAGR Delta Vs Reference": cagr_delta,
        "Drawdown Delta Vs Reference": drawdown_delta,
        "Worst Rolling Max Drawdown": worst_rolling_drawdown,
        "Reference Worst Rolling Max Drawdown": reference_worst_rolling_drawdown,
        "Worst Rolling Drawdown Delta Vs Reference": worst_rolling_delta,
        "Panic Floor Mode Share": panic_floor_mode_share,
        "live_gate_passed": bool(passed),
        "live_gate_reason": gate_reason,
        "recommended_action": _recommended_action(run, passed),
    }


def evaluate_crash_brake_live_readiness(
    summary,
    rolling,
    *,
    mode_history=None,
    candidate_runs: Iterable[str] = DEFAULT_CRASH_BRAKE_CANDIDATE_RUNS,
    reference_run: str = DEFAULT_REFERENCE_RUN,
    required_universe_lag_days: int = DEFAULT_REQUIRED_UNIVERSE_LAG_DAYS,
    allowed_cagr_shortfall: float = DEFAULT_ALLOWED_CAGR_SHORTFALL,
    allowed_drawdown_worse: float = DEFAULT_ALLOWED_DRAWDOWN_WORSE,
    min_drawdown_improvement: float = DEFAULT_MIN_DRAWDOWN_IMPROVEMENT,
    min_panic_brake_mode_share: float = DEFAULT_MIN_PANIC_BRAKE_MODE_SHARE,
) -> pd.DataFrame:
    summary_frame = pd.DataFrame(summary).copy()
    rolling_frame = pd.DataFrame(rolling).copy()
    if summary_frame.empty:
        return pd.DataFrame(columns=LIVE_READINESS_COLUMNS)
    if "Run" not in summary_frame.columns:
        raise ValueError("summary must contain Run column")
    indexed = summary_frame.set_index("Run", drop=False)
    if reference_run not in indexed.index.astype(str):
        raise ValueError(f"reference run missing from crash-brake summary: {reference_run}")
    reference = indexed.loc[reference_run]
    reference_worst_rolling_drawdown = _worst_rolling_drawdown(rolling_frame, reference_run)
    panic_floor_mode_share = _panic_floor_mode_share(mode_history)

    rows: list[dict[str, Any]] = []
    for run in candidate_runs:
        normalized = str(run).strip()
        if not normalized or normalized not in indexed.index.astype(str):
            continue
        row = indexed.loc[normalized]
        rows.append(
            _evaluate_candidate_row(
                row,
                reference,
                reference_run=reference_run,
                required_universe_lag_days=int(required_universe_lag_days),
                allowed_cagr_shortfall=float(allowed_cagr_shortfall),
                allowed_drawdown_worse=float(allowed_drawdown_worse),
                min_drawdown_improvement=float(min_drawdown_improvement),
                min_panic_brake_mode_share=float(min_panic_brake_mode_share),
                worst_rolling_drawdown=_worst_rolling_drawdown(rolling_frame, normalized),
                reference_worst_rolling_drawdown=reference_worst_rolling_drawdown,
                panic_floor_mode_share=panic_floor_mode_share,
            )
        )
    output = pd.DataFrame(rows)
    return output.loc[:, [column for column in LIVE_READINESS_COLUMNS if column in output.columns]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate Russell crash-brake live-readiness gates versus the no-brake reference blend."
    )
    parser.add_argument("--summary", required=True, help="Input crash_brake_summary.csv")
    parser.add_argument("--rolling", required=True, help="Input crash_brake_rolling_summary.csv")
    parser.add_argument("--mode-history", help="Optional crash_brake_mode_history.csv")
    parser.add_argument("--research-manifest", help="Optional crash_brake_research_manifest.json")
    parser.add_argument("--candidate-runs", default="", help="Optional comma-separated run filter override")
    parser.add_argument("--reference-run", default=DEFAULT_REFERENCE_RUN)
    parser.add_argument("--required-universe-lag-days", type=int, default=DEFAULT_REQUIRED_UNIVERSE_LAG_DAYS)
    parser.add_argument("--allowed-cagr-shortfall", type=float, default=DEFAULT_ALLOWED_CAGR_SHORTFALL)
    parser.add_argument("--allowed-drawdown-worse", type=float, default=DEFAULT_ALLOWED_DRAWDOWN_WORSE)
    parser.add_argument("--min-drawdown-improvement", type=float, default=DEFAULT_MIN_DRAWDOWN_IMPROVEMENT)
    parser.add_argument("--min-panic-brake-mode-share", type=float, default=DEFAULT_MIN_PANIC_BRAKE_MODE_SHARE)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--output-name", default="crash_brake_live_readiness_summary.csv")
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    research_manifest: dict[str, Any] = {}
    if args.research_manifest:
        research_manifest = json.loads(Path(args.research_manifest).read_text(encoding="utf-8"))
    candidate_runs = parse_csv_strings(args.candidate_runs, default=()) or tuple(
        str(item).strip() for item in research_manifest.get("candidate_runs") or DEFAULT_CRASH_BRAKE_CANDIDATE_RUNS if str(item).strip()
    )
    candidate_runs = tuple(run for run in candidate_runs if run in DEFAULT_CRASH_BRAKE_CANDIDATE_RUNS) or DEFAULT_CRASH_BRAKE_CANDIDATE_RUNS

    mode_history = read_table(args.mode_history) if args.mode_history else None
    result = evaluate_crash_brake_live_readiness(
        read_table(args.summary),
        read_table(args.rolling),
        mode_history=mode_history,
        candidate_runs=candidate_runs,
        reference_run=str(args.reference_run),
        required_universe_lag_days=int(args.required_universe_lag_days),
        allowed_cagr_shortfall=float(args.allowed_cagr_shortfall),
        allowed_drawdown_worse=float(args.allowed_drawdown_worse),
        min_drawdown_improvement=float(args.min_drawdown_improvement),
        min_panic_brake_mode_share=float(args.min_panic_brake_mode_share),
    )
    output_path = output_dir / args.output_name
    result.to_csv(output_path, index=False)

    manifest = {
        "manifest_type": "russell_top50_crash_brake_live_readiness_followup",
        "artifact_schema_version": CRASH_BRAKE_LIVE_READINESS_FOLLOWUP_SCHEMA_VERSION,
        "experiment_profile": str(research_manifest.get("experiment_profile", "") or ""),
        "candidate_runs": list(candidate_runs),
        "reference_run": str(args.reference_run),
        "inputs": {
            "summary": str(args.summary),
            "rolling": str(args.rolling),
            "mode_history": str(args.mode_history or ""),
            "research_manifest": str(args.research_manifest or ""),
        },
        "thresholds": {
            "required_universe_lag_days": int(args.required_universe_lag_days),
            "allowed_cagr_shortfall": float(args.allowed_cagr_shortfall),
            "allowed_drawdown_worse": float(args.allowed_drawdown_worse),
            "min_drawdown_improvement": float(args.min_drawdown_improvement),
            "min_panic_brake_mode_share": float(args.min_panic_brake_mode_share),
        },
        "row_count": int(len(result)),
        "artifacts": {
            "crash_brake_live_readiness_summary": {"path": output_path.name},
        },
        "outputs": [
            output_path.name,
            "crash_brake_live_readiness_followup_manifest.json",
        ],
    }
    manifest_path = output_dir / "crash_brake_live_readiness_followup_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(result.head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote crash-brake live-readiness summary -> {output_path}")
    print(f"wrote crash-brake live-readiness manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
