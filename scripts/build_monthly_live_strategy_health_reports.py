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

from us_equity_snapshot_pipelines.live_strategy_health import (  # noqa: E402
    DEFAULT_PRIMARY_BENCHMARK,
    HealthPolicy,
    build_markdown_report,
    build_strategy_health_summary,
    build_strategy_window_health,
)


DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build live strategy health reports for monthly review artifacts when return matrices are present.",
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--primary-benchmark", default=DEFAULT_PRIMARY_BENCHMARK)
    return parser.parse_args()


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return safe.strip("._") or "returns"


def discover_return_matrices(artifact_root: Path) -> list[Path]:
    ignored_parts = {"monthly_report_bundle", "monthly_review_inputs_health", "__pycache__"}
    paths: list[Path] = []
    for path in sorted(artifact_root.rglob("portfolio_and_tracker_returns.csv")):
        if any(part in ignored_parts or part.startswith("live_strategy_health") for part in path.parts):
            continue
        paths.append(path)
    return paths


def infer_strategy_columns(frame: pd.DataFrame, *, primary_benchmark: str) -> tuple[str, ...]:
    ignored = {"as_of", "date", str(primary_benchmark)}
    strategies: list[str] = []
    for column in frame.columns:
        column_text = str(column or "").strip()
        if not column_text or column_text in ignored or column_text.startswith("buy_hold_"):
            continue
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.notna().sum() > 0:
            strategies.append(column_text)
    return tuple(strategies)


def resolve_date_column(frame: pd.DataFrame) -> str:
    if "as_of" in frame.columns:
        return "as_of"
    if "date" in frame.columns:
        return "date"
    raise ValueError("return matrix must contain an as_of or date column")


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


def build_health_report_for_returns(
    returns_path: Path,
    *,
    output_root: Path,
    primary_benchmark: str = DEFAULT_PRIMARY_BENCHMARK,
    policy: HealthPolicy = HealthPolicy(),
) -> Path | None:
    frame = pd.read_csv(returns_path)
    strategies = infer_strategy_columns(frame, primary_benchmark=primary_benchmark)
    if not strategies or primary_benchmark not in frame.columns:
        return None

    date_column = resolve_date_column(frame)
    output_dir = output_root / f"live_strategy_health_{_safe_name(returns_path.parent.name)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    window_health = build_strategy_window_health(
        frame,
        strategies=strategies,
        primary_benchmark=primary_benchmark,
        policy=policy,
        date_column=date_column,
    )
    summary = build_strategy_health_summary(window_health)
    summary.to_csv(output_dir / "strategy_health_summary.csv", index=False)
    window_health.to_csv(output_dir / "strategy_health_windows.csv", index=False)
    (output_dir / "strategy_health_report.md").write_text(
        build_markdown_report(summary, window_health, policy=policy),
        encoding="utf-8",
    )
    (output_dir / "run_manifest.json").write_text(
        json.dumps(
            _json_safe(
                {
                    "artifact_type": "live_strategy_health_report",
                    "source_returns": str(returns_path),
                    "date_column": date_column,
                    "primary_benchmark": str(primary_benchmark),
                    "strategies": list(strategies),
                    "policy": policy.to_dict(),
                    "outputs": [
                        "strategy_health_summary.csv",
                        "strategy_health_windows.csv",
                        "strategy_health_report.md",
                        "run_manifest.json",
                    ],
                }
            ),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return output_dir


def build_health_error_report(returns_path: Path, *, output_root: Path, error: Exception) -> Path:
    output_dir = output_root / f"live_strategy_health_error_{_safe_name(returns_path.parent.name)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "artifact_type": "live_strategy_health_error",
        "source_returns": str(returns_path),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "outputs": [
            "strategy_health_error.json",
            "strategy_health_error.md",
        ],
    }
    (output_dir / "strategy_health_error.json").write_text(
        json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "strategy_health_error.md").write_text(
        "\n".join(
            [
                "# Live Strategy Health Error",
                "",
                "This evidence-only health report could not be built. The monthly review should inspect the source returns artifact before relying on strategy health evidence.",
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

    outputs: list[Path] = []
    error_outputs: list[Path] = []
    for returns_path in discover_return_matrices(artifact_root):
        try:
            output_dir = build_health_report_for_returns(
                returns_path,
                output_root=output_root,
                primary_benchmark=str(args.primary_benchmark),
            )
        except Exception as exc:
            error_outputs.append(build_health_error_report(returns_path, output_root=output_root, error=exc))
            continue
        if output_dir is not None:
            outputs.append(output_dir)

    print(f"health_report_count={len(outputs)}")
    print(f"health_report_error_count={len(error_outputs)}")
    for output_dir in outputs:
        print(f"health_report_dir={output_dir}")
    for output_dir in error_outputs:
        print(f"health_report_error_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
