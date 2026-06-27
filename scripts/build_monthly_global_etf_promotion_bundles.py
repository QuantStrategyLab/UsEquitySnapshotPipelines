#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.global_etf_offensive_rotation_research import resolve_experiment_profile  # noqa: E402
from us_equity_snapshot_pipelines.global_etf_promotion_bundle import build_global_etf_promotion_bundle  # noqa: E402

DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Global ETF promotion bundle artifacts for monthly review when pre-registered research outputs are present."
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--top-n-candidates", type=int, default=5)
    return parser.parse_args()


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return safe.strip("._") or "artifact"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _is_ignored_artifact(path: Path) -> bool:
    ignored_parts = {"monthly_report_bundle", "__pycache__"}
    return any(part in ignored_parts or part.startswith("promotion_bundle_") for part in path.parts)


def discover_global_etf_research_runs(artifact_root: Path) -> list[dict[str, Any]]:
    discovered: list[dict[str, Any]] = []
    for manifest_path in sorted(artifact_root.rglob("run_manifest.json")):
        if _is_ignored_artifact(manifest_path):
            continue
        try:
            payload = _load_json(manifest_path)
        except Exception:
            continue
        if str(payload.get("research", "")).strip() != "global_etf_offensive_rotation":
            continue
        experiment_profile = str(payload.get("experiment_profile", "") or "").strip()
        if not experiment_profile:
            continue
        run_dir = manifest_path.parent
        required = [
            run_dir / "ranking.csv",
            run_dir / "live_readiness_summary.csv",
            run_dir / "walk_forward_selection_summary.csv",
            run_dir / "walk_forward_selection_windows.csv",
            run_dir / "portfolio_returns_with_benchmarks.csv",
            run_dir / "rebalance_events.csv",
        ]
        if not all(path.exists() for path in required):
            continue
        discovered.append(
            {
                "manifest_path": manifest_path,
                "artifact_dir": run_dir,
                "experiment_profile": experiment_profile,
            }
        )
    return discovered


def build_global_etf_bundle_from_run(
    run: dict[str, Any],
    *,
    output_root: Path,
    top_n_candidates: int = 5,
) -> Path:
    artifact_dir = Path(run["artifact_dir"])
    experiment_profile_id = str(run.get("experiment_profile", "") or "")
    profile = resolve_experiment_profile(experiment_profile_id)
    candidate_ids = profile.liveable_composite_ids if profile is not None else ()
    output_dir = output_root / f"promotion_bundle_{_safe_name(experiment_profile_id or artifact_dir.name)}"
    build_global_etf_promotion_bundle(
        artifact_dir=artifact_dir,
        output_dir=output_dir,
        candidate_ids=candidate_ids,
        top_n_candidates=int(top_n_candidates),
        experiment_profile_id=experiment_profile_id or None,
    )
    return output_dir


def main() -> int:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    output_root_override = Path(args.output_root) if args.output_root else None
    outputs: list[Path] = []
    for run in discover_global_etf_research_runs(artifact_root):
        base_root = output_root_override if output_root_override is not None else Path(run["artifact_dir"])
        outputs.append(
            build_global_etf_bundle_from_run(
                run,
                output_root=base_root,
                top_n_candidates=int(args.top_n_candidates),
            )
        )
    print(f"global_etf_promotion_bundle_count={len(outputs)}")
    for output_dir in outputs:
        print(f"global_etf_promotion_bundle_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
