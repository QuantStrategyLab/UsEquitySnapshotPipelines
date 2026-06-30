from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from .pipelines.live_decay_monitor import build_live_decay_monitor, build_markdown_report, DecayPolicy
from .pipelines.russell_1000_multi_factor_defensive_snapshot import read_table

DEFAULT_MIN_REALIZED_EXPECTED_RATIO = 0.35
DEFAULT_WINDOWS = (63, 126, 252)


def _primary_benchmark_from_ranking(ranking: pd.DataFrame) -> str:
    frame = pd.DataFrame(ranking)
    baseline_rows = frame.loc[frame["Candidate Group"].astype(str).eq("current_live_baseline")]
    if baseline_rows.empty:
        raise ValueError("snapshot ranking missing current_live_baseline candidate")
    return str(baseline_rows.iloc[0]["Candidate"])


def build_snapshot_live_decay_followup(
    *,
    ranking_path: str | Path,
    candidate_returns_path: str | Path,
    expected_excess_path: str | Path,
    output_dir: str | Path,
    candidate_ids: list[str] | None = None,
    min_realized_expected_ratio: float = DEFAULT_MIN_REALIZED_EXPECTED_RATIO,
) -> Path:
    ranking = read_table(ranking_path)
    returns = read_table(candidate_returns_path)
    expected = read_table(expected_excess_path)
    strategies = candidate_ids or (
        pd.DataFrame(ranking)
        .loc[pd.DataFrame(ranking).get("replacement_review_candidate", False).astype(bool), "Candidate"]
        .astype(str)
        .tolist()
    )
    primary_benchmark = _primary_benchmark_from_ranking(pd.DataFrame(ranking))
    expected_map = {
        str(row["strategy"]): float(row["expected_excess_cagr_vs_primary"])
        for row in pd.DataFrame(expected).to_dict(orient="records")
        if str(row.get("strategy", "")).strip()
    }
    result = build_live_decay_monitor(
        returns,
        strategies=strategies,
        primary_benchmark=primary_benchmark,
        secondary_benchmark="",
        windows=DEFAULT_WINDOWS,
        expected_excess_cagr_by_strategy=expected_map,
        min_realized_expected_ratio=float(min_realized_expected_ratio),
        input_format="wide",
    )
    window_summary = pd.DataFrame(result["live_decay_window_summary"])
    strategy_summary = pd.DataFrame(result["live_decay_strategy_summary"])
    manifest_inputs = dict(result["manifest_inputs"])
    policy = DecayPolicy(**manifest_inputs["policy"])
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    window_summary.to_csv(root / "live_decay_window_summary.csv", index=False)
    strategy_summary.to_csv(root / "live_decay_strategy_summary.csv", index=False)
    (root / "live_decay_report.md").write_text(
        build_markdown_report(strategy_summary, window_summary, policy=policy),
        encoding="utf-8",
    )
    payload = {
        "manifest_type": "live_decay_monitor",
        "artifact_schema_version": "live_decay_monitor.v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_returns": str(candidate_returns_path),
        "input_format": manifest_inputs["input_format"],
        "strategies": manifest_inputs["strategies"],
        "primary_benchmark": manifest_inputs["primary_benchmark"],
        "secondary_benchmark": manifest_inputs["secondary_benchmark"],
        "windows": manifest_inputs["windows"],
        "policy": manifest_inputs["policy"],
        "expected_excess_cagr_by_strategy": manifest_inputs["expected_excess_cagr_by_strategy"],
        "row_counts": {
            "live_decay_window_summary": int(len(window_summary)),
            "live_decay_strategy_summary": int(len(strategy_summary)),
        },
        "artifacts": {
            "live_decay_window_summary": {"path": "live_decay_window_summary.csv"},
            "live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"},
            "live_decay_report": {"path": "live_decay_report.md"},
        },
        "outputs": [
            "live_decay_window_summary.csv",
            "live_decay_strategy_summary.csv",
            "live_decay_report.md",
            "live_decay_monitor_manifest.json",
        ],
    }
    (root / "live_decay_monitor_manifest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return root


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build snapshot-specific live decay follow-up artifacts.")
    parser.add_argument("--ranking", required=True)
    parser.add_argument("--candidate-daily-returns", required=True)
    parser.add_argument("--expected-excess-cagr", required=True)
    parser.add_argument("--candidate-ids", default="")
    parser.add_argument("--min-realized-expected-ratio", type=float, default=DEFAULT_MIN_REALIZED_EXPECTED_RATIO)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidate_ids = [item.strip() for item in str(args.candidate_ids).split(",") if item.strip()]
    output_dir = build_snapshot_live_decay_followup(
        ranking_path=args.ranking,
        candidate_returns_path=args.candidate_daily_returns,
        expected_excess_path=args.expected_excess_cagr,
        output_dir=args.output_dir,
        candidate_ids=candidate_ids or None,
        min_realized_expected_ratio=float(args.min_realized_expected_ratio),
    )
    summary = pd.read_csv(output_dir / "live_decay_strategy_summary.csv")
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
