from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .mega_cap_leader_rotation_promotion_review import PROMOTION_REVIEW_COLUMNS
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

CRASH_BRAKE_PROMOTION_REVIEW_SCHEMA_VERSION = "russell_top50_crash_brake_promotion_review.v1"
CRASH_BRAKE_PROMOTABLE_RUN = "crash_brake_top2_50_floor25"
CRASH_BRAKE_PROMOTION_GATE_PROFILE = "crash_brake_promotion_v1"


def _candidate_role_from_run(run: str, *, promotable: bool = False) -> tuple[str, str]:
    normalized = str(run or "").strip()
    if normalized == CRASH_BRAKE_PROMOTABLE_RUN:
        if promotable:
            return "panic_rebound_guard_live_design", CRASH_BRAKE_PROMOTION_GATE_PROFILE
        return "panic_rebound_guard_research", "research_only"
    if normalized == "blend_top2_50_top4_50_no_brake":
        return "balanced_offensive_live_design", "balanced_offensive_reference"
    if normalized == "blend_top2_25_top4_75_no_brake":
        return "conservative_live_design", "conservative_reference"
    return "research_only", "research_only"


def _recommended_action(run: str, *, live_gate_passed: bool, required_gates_passed: bool) -> str:
    normalized = str(run or "").strip()
    if normalized != "crash_brake_top2_50_floor25":
        return "keep_as_crash_brake_reference_only"
    if required_gates_passed:
        return "crash_brake_all_required_gates_passed_continue_review"
    if live_gate_passed:
        return "crash_brake_live_gate_passed_continue_gate_collection"
    return "collect_live_stress_overfit_liquidity_for_crash_brake_candidate"


def _promotion_decision(run: str, *, required_gates_passed: bool) -> str:
    normalized = str(run or "").strip()
    if normalized != CRASH_BRAKE_PROMOTABLE_RUN:
        return "research_only"
    if required_gates_passed:
        return "live_design_review_crash_brake"
    return "research_only"


def build_crash_brake_promotion_review(
    summary: pd.DataFrame,
    *,
    candidate_runs: Iterable[str] | None = None,
    live_readiness: pd.DataFrame | None = None,
    overfit_promotion: pd.DataFrame | None = None,
    stress_summary: pd.DataFrame | None = None,
    liquidity_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(summary).copy()
    if frame.empty:
        return pd.DataFrame(columns=PROMOTION_REVIEW_COLUMNS)
    if "Run" not in frame.columns:
        raise ValueError("crash-brake summary must include Run column")
    runs = tuple(str(run).strip() for run in (candidate_runs or ()) if str(run).strip())
    if runs:
        frame = frame.loc[frame["Run"].astype(str).isin(runs)].copy()
    if frame.empty:
        return pd.DataFrame(columns=PROMOTION_REVIEW_COLUMNS)

    live_map: dict[str, dict[str, Any]] = {}
    if live_readiness is not None and not pd.DataFrame(live_readiness).empty and "Run" in pd.DataFrame(live_readiness).columns:
        live_map = pd.DataFrame(live_readiness).set_index("Run").to_dict(orient="index")
    overfit_map: dict[str, dict[str, Any]] = {}
    if overfit_promotion is not None and not pd.DataFrame(overfit_promotion).empty and "Run" in pd.DataFrame(overfit_promotion).columns:
        overfit_map = pd.DataFrame(overfit_promotion).set_index("Run").to_dict(orient="index")
    stress_map: dict[str, dict[str, Any]] = {}
    if stress_summary is not None and not pd.DataFrame(stress_summary).empty and "Run" in pd.DataFrame(stress_summary).columns:
        stress_map = pd.DataFrame(stress_summary).set_index("Run").to_dict(orient="index")
    liquidity_map: dict[str, dict[str, Any]] = {}
    if liquidity_summary is not None and not pd.DataFrame(liquidity_summary).empty and "Run" in pd.DataFrame(liquidity_summary).columns:
        liquidity_frame = pd.DataFrame(liquidity_summary).copy()
        if "Portfolio NAV" in liquidity_frame.columns:
            liquidity_frame = liquidity_frame.sort_values(["Run", "Portfolio NAV"]).drop_duplicates(subset=["Run"], keep="first")
        else:
            liquidity_frame = liquidity_frame.drop_duplicates(subset=["Run"], keep="first")
        liquidity_map = liquidity_frame.set_index("Run").to_dict(orient="index")

    rows: list[dict[str, Any]] = []
    for row in frame.to_dict(orient="records"):
        run = str(row.get("Run") or "").strip()
        is_promotable_run = run == CRASH_BRAKE_PROMOTABLE_RUN
        candidate_role, gate_profile = _candidate_role_from_run(run, promotable=False)
        live = live_map.get(run, {})
        overfit = overfit_map.get(run, {})
        stress = stress_map.get(run, {})
        liquidity = liquidity_map.get(run, {})
        live_gate_passed = bool(live.get("live_gate_passed", False))
        live_gate_reason = str(
            live.get("live_gate_reason", "") or "research_only_crash_brake_requires_live_gate_followup"
        )
        overfit_gate_passed = bool(overfit.get("overfit_gate_passed", False))
        overfit_gate_reason = str(
            overfit.get("overfit_gate_reason", "") or "research_only_crash_brake_requires_overfit_followup"
        )
        stress_gate_passed = bool(stress.get("all_stress_gates_passed", False))
        stress_gate_reason = str(stress.get("stress_gate_reason", "") or "research_only_crash_brake_requires_stress_followup")
        liquidity_gate_passed = bool(liquidity.get("liquidity_gate_passed", False))
        liquidity_gate_reason = str(liquidity.get("liquidity_gate_reason", "") or "research_only_crash_brake_requires_liquidity_followup")
        gate_checks = {
            "live_gate": live_gate_passed if is_promotable_run else True,
            "stress_gate": stress_gate_passed if is_promotable_run else True,
            "overfit_gate": overfit_gate_passed if is_promotable_run else True,
            "liquidity_gate": liquidity_gate_passed if is_promotable_run else True,
        }
        failed_gates = [name for name, passed in gate_checks.items() if not passed]
        required_gates_passed = not failed_gates
        required_gate_reason = "pass" if required_gates_passed else ";".join(failed_gates)
        if is_promotable_run and required_gates_passed:
            candidate_role, gate_profile = _candidate_role_from_run(run, promotable=True)
        review_row: dict[str, Any] = {column: pd.NA for column in PROMOTION_REVIEW_COLUMNS}
        review_row.update(
            {
                "Run": run,
                "Candidate Role": candidate_role,
                "Gate Profile": gate_profile,
                "CAGR": row.get("CAGR"),
                "Max Drawdown": row.get("Max Drawdown"),
                "Sharpe": row.get("Sharpe"),
                "Turnover/Year": row.get("Turnover/Year"),
                "live_gate_passed": live_gate_passed if is_promotable_run else False,
                "live_gate_reason": (
                    live_gate_reason
                    if is_promotable_run
                    else "reference_not_live_gate_candidate"
                ),
                "stress_gate_passed": stress_gate_passed,
                "stress_gate_reason": stress_gate_reason,
                "overfit_gate_passed": overfit_gate_passed,
                "overfit_gate_reason": overfit_gate_reason,
                "liquidity_gate_passed": liquidity_gate_passed,
                "liquidity_gate_reason": liquidity_gate_reason,
                "required_gates_passed": required_gates_passed if is_promotable_run else False,
                "required_gate_reason": required_gate_reason if is_promotable_run else "reference_not_promotable",
                "statistical_support_level": (
                    "crash_brake_pre_registered_experiment_all_required_gates_passed"
                    if is_promotable_run and required_gates_passed
                    else (
                        "research_only_pre_registered_experiment_with_gate_followups"
                        if live_map or overfit_map or stress_map or liquidity_map
                        else "research_only_pre_registered_experiment"
                    )
                ),
                "promotion_decision": _promotion_decision(
                    run,
                    required_gates_passed=required_gates_passed if is_promotable_run else False,
                ),
                "recommended_action": _recommended_action(
                    run,
                    live_gate_passed=live_gate_passed,
                    required_gates_passed=required_gates_passed if is_promotable_run else False,
                ),
            }
        )
        rows.append(review_row)
    review = pd.DataFrame(rows)
    return review.loc[:, list(PROMOTION_REVIEW_COLUMNS)].reset_index(drop=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a research-only Russell crash-brake promotion-style review artifact."
    )
    parser.add_argument("--summary", required=True, help="Input crash_brake_summary.csv")
    parser.add_argument("--research-manifest", help="Optional crash_brake_research_manifest.json")
    parser.add_argument(
        "--live-readiness",
        help="Optional crash_brake_live_readiness_summary.csv from crash-brake live-readiness follow-up",
    )
    parser.add_argument("--overfit-promotion", help="Optional overfit_promotion_gate_summary.csv from crash-brake follow-up")
    parser.add_argument("--stress-summary", help="Optional crash_brake_stress_summary.csv from crash-brake follow-up")
    parser.add_argument("--liquidity-summary", help="Optional liquidity_summary.csv from crash-brake follow-up")
    parser.add_argument("--candidate-runs", default="", help="Optional comma-separated run filter override")
    parser.add_argument("--output-dir", required=True)
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
        str(item).strip() for item in research_manifest.get("candidate_runs") or () if str(item).strip()
    )

    review = build_crash_brake_promotion_review(
        read_table(args.summary),
        candidate_runs=candidate_runs,
        live_readiness=read_table(args.live_readiness) if args.live_readiness else None,
        overfit_promotion=read_table(args.overfit_promotion) if args.overfit_promotion else None,
        stress_summary=read_table(args.stress_summary) if args.stress_summary else None,
        liquidity_summary=read_table(args.liquidity_summary) if args.liquidity_summary else None,
    )
    review_path = output_dir / "live_promotion_review.csv"
    review.to_csv(review_path, index=False)

    manifest = {
        "manifest_type": "russell_top50_crash_brake_promotion_review",
        "artifact_schema_version": CRASH_BRAKE_PROMOTION_REVIEW_SCHEMA_VERSION,
        "experiment_profile": str(research_manifest.get("experiment_profile", "") or ""),
        "candidate_runs": list(candidate_runs),
        "inputs": {
            "summary": str(args.summary),
            "research_manifest": str(args.research_manifest or ""),
            "live_readiness": str(args.live_readiness or ""),
            "overfit_promotion": str(args.overfit_promotion or ""),
            "stress_summary": str(args.stress_summary or ""),
            "liquidity_summary": str(args.liquidity_summary or ""),
        },
        "artifacts": {
            "live_promotion_review": {"path": review_path.name},
        },
        "row_count": int(len(review)),
        "review_rows": [
            {
                "run": str(row.get("Run", "") or ""),
                "required_gates_passed": bool(row.get("required_gates_passed", False)),
                "promotion_decision": str(row.get("promotion_decision", "") or ""),
                "recommended_action": str(row.get("recommended_action", "") or ""),
            }
            for row in review.to_dict(orient="records")
        ],
        "outputs": [
            review_path.name,
            "crash_brake_promotion_review_manifest.json",
        ],
    }
    manifest_path = output_dir / "crash_brake_promotion_review_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(review.head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote crash-brake promotion review -> {review_path}")
    print(f"wrote crash-brake promotion review manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
