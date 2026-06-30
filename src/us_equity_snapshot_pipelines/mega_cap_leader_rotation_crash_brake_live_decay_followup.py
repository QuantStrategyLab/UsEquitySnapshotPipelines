from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import live_decay_monitor as decay
from .mega_cap_leader_rotation_stress_readiness import parse_csv_ints, parse_csv_strings
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

CRASH_BRAKE_LIVE_DECAY_FOLLOWUP_SCHEMA_VERSION = "russell_top50_crash_brake_live_decay_followup.v1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build crash-brake live-decay follow-up artifacts from crash-brake daily returns."
    )
    parser.add_argument("--returns", required=True, help="Input crash_brake_daily_returns.csv")
    parser.add_argument("--research-manifest", help="Optional crash_brake_research_manifest.json")
    parser.add_argument("--candidate-runs", default="crash_brake_top2_50_floor25")
    parser.add_argument("--primary-benchmark", default=decay.DEFAULT_PRIMARY_BENCHMARK)
    parser.add_argument("--secondary-benchmark", default=decay.DEFAULT_SECONDARY_BENCHMARK)
    parser.add_argument("--windows", default=",".join(str(value) for value in decay.DEFAULT_WINDOWS))
    parser.add_argument("--min-observations", type=int, default=decay.DEFAULT_MIN_OBSERVATIONS)
    parser.add_argument("--min-excess-cagr-vs-primary", type=float, default=decay.DEFAULT_MIN_EXCESS_CAGR)
    parser.add_argument("--min-excess-cagr-vs-secondary", type=float, default=decay.DEFAULT_MIN_EXCESS_CAGR)
    parser.add_argument("--min-realized-expected-ratio", type=float, default=decay.DEFAULT_MIN_REALIZED_EXPECTED_RATIO)
    parser.add_argument("--expected-excess-cagr", type=float, help="Optional expected excess CAGR applied to all candidate runs.")
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
    candidate_runs = parse_csv_strings(args.candidate_runs, default=("crash_brake_top2_50_floor25",))
    expected = (
        {run: float(args.expected_excess_cagr) for run in candidate_runs}
        if args.expected_excess_cagr is not None
        else None
    )
    result = decay.build_live_decay_monitor(
        read_table(args.returns),
        candidate_runs=candidate_runs,
        primary_benchmark=str(args.primary_benchmark),
        secondary_benchmark=str(args.secondary_benchmark),
        windows=parse_csv_ints(args.windows, default=decay.DEFAULT_WINDOWS),
        min_observations=int(args.min_observations),
        min_excess_cagr_vs_primary=float(args.min_excess_cagr_vs_primary),
        min_excess_cagr_vs_secondary=float(args.min_excess_cagr_vs_secondary),
        expected_excess_cagr_by_strategy=expected,
        min_realized_expected_ratio=float(args.min_realized_expected_ratio),
        input_format="russell_daily",
    )

    window_path = output_dir / "live_decay_window_summary.csv"
    strategy_path = output_dir / "live_decay_strategy_summary.csv"
    report_path = output_dir / "live_decay_report.md"
    manifest_path = output_dir / "live_decay_monitor_manifest.json"
    result["live_decay_window_summary"].to_csv(window_path, index=False)
    result["live_decay_strategy_summary"].to_csv(strategy_path, index=False)
    report_path.write_text(
        decay.build_markdown_report(
            result["live_decay_strategy_summary"],
            result["live_decay_window_summary"],
            policy=decay.DecayPolicy(**result["manifest_inputs"]["policy"]),
        ),
        encoding="utf-8",
    )
    manifest_payload = {
        "manifest_type": "live_decay_monitor",
        "artifact_schema_version": "live_decay_monitor.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "input_format": result["manifest_inputs"]["input_format"],
        "strategies": result["manifest_inputs"]["strategies"],
        "primary_benchmark": result["manifest_inputs"]["primary_benchmark"],
        "secondary_benchmark": result["manifest_inputs"]["secondary_benchmark"],
        "windows": result["manifest_inputs"]["windows"],
        "policy": result["manifest_inputs"]["policy"],
        "expected_excess_cagr_by_strategy": result["manifest_inputs"]["expected_excess_cagr_by_strategy"],
        "source_project": "UsEquitySnapshotPipelines",
        "experiment_profile": str(research_manifest.get("experiment_profile", "") or ""),
        "artifacts": {
            "live_decay_window_summary": {"path": window_path.name},
            "live_decay_strategy_summary": {"path": strategy_path.name},
            "live_decay_report": {"path": report_path.name},
        },
        "row_counts": {
            "live_decay_window_summary": int(len(result["live_decay_window_summary"])),
            "live_decay_strategy_summary": int(len(result["live_decay_strategy_summary"])),
        },
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(result["live_decay_strategy_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote crash-brake live decay window summary -> {window_path}")
    print(f"wrote crash-brake live decay strategy summary -> {strategy_path}")
    print(f"wrote crash-brake live decay report -> {report_path}")
    print(f"wrote crash-brake live decay manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
