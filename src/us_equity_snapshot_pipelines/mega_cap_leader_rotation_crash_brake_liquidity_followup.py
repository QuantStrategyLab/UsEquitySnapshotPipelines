from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .mega_cap_leader_rotation_liquidity_diagnostics import (
    DEFAULT_ADV_WINDOW,
    DEFAULT_EXECUTION_DAYS,
    DEFAULT_EXCLUDE_SYMBOLS,
    DEFAULT_MAX_PARTICIPATION_RATE,
    DEFAULT_PORTFOLIO_NAV_VALUES,
    build_liquidity_diagnostics,
)
from .mega_cap_leader_rotation_stress_readiness import parse_csv_floats_no_percent, parse_csv_strings
from .russell_1000_multi_factor_defensive_snapshot import read_table

CRASH_BRAKE_LIQUIDITY_FOLLOWUP_SCHEMA_VERSION = "russell_top50_crash_brake_liquidity_followup.v1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build crash-brake liquidity follow-up artifacts from crash-brake rebalance trades and price history."
    )
    parser.add_argument("--trades", required=True, help="Input crash_brake_rebalance_trades.csv")
    parser.add_argument("--prices", required=True, help="Input price history CSV with close and volume")
    parser.add_argument("--research-manifest", help="Optional crash_brake_research_manifest.json")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--portfolio-nav-values",
        default=",".join(str(value) for value in DEFAULT_PORTFOLIO_NAV_VALUES),
    )
    parser.add_argument("--adv-window", type=int, default=DEFAULT_ADV_WINDOW)
    parser.add_argument("--execution-days", type=int, default=DEFAULT_EXECUTION_DAYS)
    parser.add_argument("--max-participation-rate", type=float, default=DEFAULT_MAX_PARTICIPATION_RATE)
    parser.add_argument("--exclude-symbols", default=",".join(DEFAULT_EXCLUDE_SYMBOLS))
    parser.add_argument("--candidate-runs", default="crash_brake_top2_50_floor25")
    parser.add_argument("--print-top", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    research_manifest: dict[str, Any] = {}
    if args.research_manifest:
        research_manifest = json.loads(Path(args.research_manifest).read_text(encoding="utf-8"))

    result = build_liquidity_diagnostics(
        read_table(args.trades),
        read_table(args.prices),
        portfolio_nav_values=parse_csv_floats_no_percent(args.portfolio_nav_values, default=DEFAULT_PORTFOLIO_NAV_VALUES),
        adv_window=int(args.adv_window),
        execution_days=int(args.execution_days),
        max_participation_rate=float(args.max_participation_rate),
        exclude_symbols=parse_csv_strings(args.exclude_symbols, default=DEFAULT_EXCLUDE_SYMBOLS),
        candidate_runs=parse_csv_strings(args.candidate_runs, default=("crash_brake_top2_50_floor25",)),
    )
    detail_path = output_dir / "liquidity_trade_detail.csv"
    summary_path = output_dir / "liquidity_summary.csv"
    result["liquidity_trade_detail"].to_csv(detail_path, index=False)
    result["liquidity_summary"].to_csv(summary_path, index=False)

    manifest = {
        "manifest_type": "russell_top50_crash_brake_liquidity_followup",
        "artifact_schema_version": CRASH_BRAKE_LIQUIDITY_FOLLOWUP_SCHEMA_VERSION,
        "experiment_profile": str(research_manifest.get("experiment_profile", "") or ""),
        "candidate_runs": parse_csv_strings(args.candidate_runs, default=("crash_brake_top2_50_floor25",)),
        "inputs": {
            "trades": str(args.trades),
            "prices": str(args.prices),
            "research_manifest": str(args.research_manifest or ""),
        },
        "row_counts": {
            "liquidity_trade_detail": int(len(result["liquidity_trade_detail"])),
            "liquidity_summary": int(len(result["liquidity_summary"])),
        },
        "artifacts": {
            "liquidity_trade_detail": {"path": detail_path.name},
            "liquidity_summary": {"path": summary_path.name},
        },
        "outputs": [
            detail_path.name,
            summary_path.name,
            "crash_brake_liquidity_followup_manifest.json",
        ],
    }
    manifest_path = output_dir / "crash_brake_liquidity_followup_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(result["liquidity_summary"].head(max(int(args.print_top), 0)).to_string(index=False))
    print(f"wrote crash-brake liquidity detail -> {detail_path}")
    print(f"wrote crash-brake liquidity summary -> {summary_path}")
    print(f"wrote crash-brake liquidity manifest -> {manifest_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
