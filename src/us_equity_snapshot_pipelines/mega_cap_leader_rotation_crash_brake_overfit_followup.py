from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .mega_cap_leader_rotation_overfit_diagnostics import build_overfit_diagnostics
from .mega_cap_leader_rotation_stress_readiness import parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

CRASH_BRAKE_OVERFIT_FOLLOWUP_SCHEMA_VERSION = "russell_top50_crash_brake_overfit_followup.v1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build crash-brake overfit/walk-forward follow-up artifacts from crash-brake research outputs."
    )
    parser.add_argument("--summary", required=True, help="Input crash_brake_summary.csv")
    parser.add_argument("--rolling", required=True, help="Input crash_brake_rolling_summary.csv")
    parser.add_argument("--research-manifest", help="Optional crash_brake_research_manifest.json")
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

    summary = read_table(args.summary)
    rolling = read_table(args.rolling)
    if candidate_runs:
        summary = summary.loc[summary["Run"].astype(str).isin(candidate_runs)].copy()
        rolling = rolling.loc[rolling["Run"].astype(str).isin(candidate_runs)].copy()

    result = build_overfit_diagnostics(summary, rolling)
    diagnostics_path = output_dir / "overfit_candidate_diagnostics.csv"
    rank_windows_path = output_dir / "overfit_rank_windows.csv"
    promotion_gate_path = output_dir / "overfit_promotion_gate_summary.csv"
    result["overfit_candidate_diagnostics"].to_csv(diagnostics_path, index=False)
    result["overfit_rank_windows"].to_csv(rank_windows_path, index=False)
    result["overfit_promotion_gate_summary"].to_csv(promotion_gate_path, index=False)

    manifest = {
        "manifest_type": "russell_top50_crash_brake_overfit_followup",
        "artifact_schema_version": CRASH_BRAKE_OVERFIT_FOLLOWUP_SCHEMA_VERSION,
        "experiment_profile": str(research_manifest.get("experiment_profile", "") or ""),
        "candidate_runs": list(candidate_runs),
        "inputs": {
            "summary": str(args.summary),
            "rolling": str(args.rolling),
            "research_manifest": str(args.research_manifest or ""),
        },
        "row_counts": {
            "overfit_candidate_diagnostics": int(len(result["overfit_candidate_diagnostics"])),
            "overfit_rank_windows": int(len(result["overfit_rank_windows"])),
            "overfit_promotion_gate_summary": int(len(result["overfit_promotion_gate_summary"])),
        },
        "artifacts": {
            "overfit_candidate_diagnostics": {"path": diagnostics_path.name},
            "overfit_rank_windows": {"path": rank_windows_path.name},
            "overfit_promotion_gate_summary": {"path": promotion_gate_path.name},
        },
        "outputs": [
            diagnostics_path.name,
            rank_windows_path.name,
            promotion_gate_path.name,
            "crash_brake_overfit_followup_manifest.json",
        ],
    }
    manifest_path = output_dir / "crash_brake_overfit_followup_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"Wrote {diagnostics_path}")
    print(f"Wrote {rank_windows_path}")
    print(f"Wrote {promotion_gate_path}")
    print(f"Wrote {manifest_path}")
    print(result["overfit_candidate_diagnostics"].head(max(int(args.print_top), 0)).to_string(index=False))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
