#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.live_decay_monitor import (  # noqa: E402
    DEFAULT_PRIMARY_BENCHMARK,
    DEFAULT_SECONDARY_BENCHMARK,
    DEFAULT_WINDOWS,
    DecayPolicy,
    build_live_decay_monitor,
    build_markdown_report,
)


DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"
DEFAULT_RUSSELL_CANDIDATE_RUNS = (
    "blend_top2_50_top4_50",
    "blend_top2_25_top4_75",
    "base_top4_cap25",
)
DEFAULT_GLOBAL_ETF_STRATEGIES = (
    "liveable_baseline_relative_decay_brake_baseline90_fast10_floor0",
    "liveable_blend_baseline90_fast10",
    "live_global_etf_rotation_defensive_baseline",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build live-decay monitor artifacts for monthly review when strategy return artifacts are present.",
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--primary-benchmark", default=DEFAULT_PRIMARY_BENCHMARK)
    parser.add_argument("--secondary-benchmark", default=DEFAULT_SECONDARY_BENCHMARK)
    parser.add_argument("--windows", default=",".join(str(window) for window in DEFAULT_WINDOWS))
    parser.add_argument("--min-observations", type=int, default=60)
    parser.add_argument(
        "--russell-candidate-runs",
        default=",".join(DEFAULT_RUSSELL_CANDIDATE_RUNS),
        help="Comma-separated Russell Run values to monitor when present.",
    )
    parser.add_argument(
        "--global-etf-strategies",
        default=",".join(DEFAULT_GLOBAL_ETF_STRATEGIES),
        help="Comma-separated Global ETF strategy columns to monitor when present.",
    )
    return parser.parse_args()


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return safe.strip("._") or "returns"


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))


def _parse_windows(value: str) -> tuple[int, ...]:
    windows = tuple(int(item.strip()) for item in str(value or "").split(",") if item.strip())
    if not windows or any(window <= 0 for window in windows):
        raise ValueError("windows must contain positive integers")
    return tuple(dict.fromkeys(windows))


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _is_ignored_artifact(path: Path) -> bool:
    ignored_parts = {"monthly_report_bundle", "monthly_review_inputs_health", "__pycache__"}
    return any(
        part in ignored_parts
        or part.startswith("live_decay_monitor")
        or part.startswith("live_strategy_health")
        for part in path.parts
    )


def discover_live_decay_inputs(artifact_root: Path) -> list[tuple[str, Path]]:
    inputs: list[tuple[str, Path]] = []
    for path in sorted(artifact_root.rglob("concentration_variant_daily_returns.csv")):
        if not _is_ignored_artifact(path):
            inputs.append(("russell_daily", path))
    for path in sorted(artifact_root.rglob("portfolio_returns_with_benchmarks.csv")):
        if not _is_ignored_artifact(path):
            inputs.append(("wide", path))
    return inputs


def resolve_russell_candidate_runs(frame: pd.DataFrame, requested: tuple[str, ...]) -> tuple[str, ...]:
    if "Run" not in frame.columns:
        raise ValueError("Russell daily returns must contain a Run column")
    available = tuple(dict.fromkeys(str(value) for value in frame["Run"].dropna().astype(str)))
    selected = tuple(candidate for candidate in requested if candidate in available)
    return selected or available


def resolve_wide_strategies(
    frame: pd.DataFrame,
    requested: tuple[str, ...],
    *,
    primary_benchmark: str,
    secondary_benchmark: str,
) -> tuple[str, ...]:
    selected = tuple(strategy for strategy in requested if strategy in frame.columns)
    if selected:
        return selected
    ignored = {"as_of", "date", str(primary_benchmark), str(secondary_benchmark)}
    strategies: list[str] = []
    for column in frame.columns:
        column_text = str(column or "").strip()
        if not column_text or column_text in ignored:
            continue
        if pd.to_numeric(frame[column], errors="coerce").notna().sum() > 0:
            strategies.append(column_text)
    return tuple(strategies)


def _write_monitor_artifacts(
    *,
    returns_path: Path,
    output_dir: Path,
    input_format: str,
    result: dict[str, object],
    policy: DecayPolicy,
) -> None:
    window_summary = pd.DataFrame(result["live_decay_window_summary"])
    strategy_summary = pd.DataFrame(result["live_decay_strategy_summary"])
    manifest_inputs = dict(result["manifest_inputs"])
    output_dir.mkdir(parents=True, exist_ok=True)
    window_summary.to_csv(output_dir / "live_decay_window_summary.csv", index=False)
    strategy_summary.to_csv(output_dir / "live_decay_strategy_summary.csv", index=False)
    (output_dir / "live_decay_report.md").write_text(
        build_markdown_report(strategy_summary, window_summary, policy=policy),
        encoding="utf-8",
    )
    outputs = [
        "live_decay_window_summary.csv",
        "live_decay_strategy_summary.csv",
        "live_decay_report.md",
        "live_decay_monitor_manifest.json",
    ]
    manifest = {
        "manifest_type": "live_decay_monitor",
        "artifact_schema_version": "live_decay_monitor.v1",
        "source_returns": str(returns_path),
        "input_format": input_format,
        "strategies": manifest_inputs.get("strategies", []),
        "primary_benchmark": manifest_inputs.get("primary_benchmark", ""),
        "secondary_benchmark": manifest_inputs.get("secondary_benchmark", ""),
        "windows": manifest_inputs.get("windows", []),
        "policy": manifest_inputs.get("policy", policy.to_dict()),
        "expected_excess_cagr_by_strategy": manifest_inputs.get("expected_excess_cagr_by_strategy", {}),
        "row_counts": {
            "live_decay_window_summary": int(len(window_summary)),
            "live_decay_strategy_summary": int(len(strategy_summary)),
        },
        "artifacts": {
            "live_decay_window_summary": {"path": "live_decay_window_summary.csv"},
            "live_decay_strategy_summary": {"path": "live_decay_strategy_summary.csv"},
            "live_decay_report": {"path": "live_decay_report.md"},
        },
        "outputs": outputs,
    }
    (output_dir / "live_decay_monitor_manifest.json").write_text(
        json.dumps(_json_safe(manifest), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_live_decay_for_returns(
    returns_path: Path,
    *,
    input_format: str,
    output_root: Path,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
    primary_benchmark: str = DEFAULT_PRIMARY_BENCHMARK,
    secondary_benchmark: str = DEFAULT_SECONDARY_BENCHMARK,
    min_observations: int = 60,
    russell_candidate_runs: tuple[str, ...] = DEFAULT_RUSSELL_CANDIDATE_RUNS,
    global_etf_strategies: tuple[str, ...] = DEFAULT_GLOBAL_ETF_STRATEGIES,
) -> Path | None:
    frame = pd.read_csv(returns_path)
    output_dir = output_root / f"live_decay_monitor_{_safe_name(returns_path.parent.name)}"
    policy = DecayPolicy(min_observations=int(min_observations))
    if input_format == "russell_daily":
        candidate_runs = resolve_russell_candidate_runs(frame, russell_candidate_runs)
        if not candidate_runs:
            return None
        result = build_live_decay_monitor(
            frame,
            candidate_runs=candidate_runs,
            primary_benchmark=primary_benchmark,
            secondary_benchmark=secondary_benchmark,
            windows=windows,
            min_observations=int(min_observations),
            input_format="russell_daily",
        )
    elif input_format == "wide":
        strategies = resolve_wide_strategies(
            frame,
            global_etf_strategies,
            primary_benchmark=primary_benchmark,
            secondary_benchmark=secondary_benchmark,
        )
        if not strategies:
            return None
        result = build_live_decay_monitor(
            frame,
            strategies=strategies,
            primary_benchmark=primary_benchmark,
            secondary_benchmark=secondary_benchmark,
            windows=windows,
            min_observations=int(min_observations),
            input_format="wide",
        )
    else:
        raise ValueError(f"unsupported input format: {input_format}")
    _write_monitor_artifacts(
        returns_path=returns_path,
        output_dir=output_dir,
        input_format=input_format,
        result=result,
        policy=policy,
    )
    return output_dir


def build_live_decay_error_report(returns_path: Path, *, output_root: Path, error: Exception) -> Path:
    output_dir = output_root / f"live_decay_monitor_error_{_safe_name(returns_path.parent.name)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "artifact_type": "live_decay_monitor_error",
        "source_returns": str(returns_path),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "outputs": ["live_decay_monitor_error.json", "live_decay_monitor_error.md"],
    }
    (output_dir / "live_decay_monitor_error.json").write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "live_decay_monitor_error.md").write_text(
        "\n".join(
            [
                "# Live Decay Monitor Error",
                "",
                "This evidence-only live-decay monitor could not be built. The monthly review should inspect the source returns artifact before relying on decay evidence.",
                "",
                f"- Source returns: `{returns_path}`",
                f"- Error type: `{type(error).__name__}`",
                f"- Error message: `{str(error)}`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return output_dir


def main() -> int:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    output_root = Path(args.output_root) if args.output_root else artifact_root
    output_root.mkdir(parents=True, exist_ok=True)
    windows = _parse_windows(args.windows)
    russell_runs = _split_csv(args.russell_candidate_runs)
    global_strategies = _split_csv(args.global_etf_strategies)

    outputs: list[Path] = []
    error_outputs: list[Path] = []
    for input_format, returns_path in discover_live_decay_inputs(artifact_root):
        try:
            output_dir = build_live_decay_for_returns(
                returns_path,
                input_format=input_format,
                output_root=output_root,
                windows=windows,
                primary_benchmark=str(args.primary_benchmark),
                secondary_benchmark=str(args.secondary_benchmark),
                min_observations=int(args.min_observations),
                russell_candidate_runs=russell_runs,
                global_etf_strategies=global_strategies,
            )
        except Exception as exc:
            error_outputs.append(build_live_decay_error_report(returns_path, output_root=output_root, error=exc))
            continue
        if output_dir is not None:
            outputs.append(output_dir)

    print(f"live_decay_monitor_count={len(outputs)}")
    print(f"live_decay_monitor_error_count={len(error_outputs)}")
    for output_dir in outputs:
        print(f"live_decay_monitor_dir={output_dir}")
    for output_dir in error_outputs:
        print(f"live_decay_monitor_error_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
