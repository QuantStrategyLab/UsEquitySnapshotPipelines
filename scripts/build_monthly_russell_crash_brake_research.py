#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.mega_cap_leader_rotation_crash_brake_research import main as research_main  # noqa: E402

DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Russell crash-brake research artifacts from downloaded snapshot source inputs."
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--rolling-window-years", default="1")
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def discover_russell_snapshot_runs(artifact_root: Path) -> list[dict[str, Any]]:
    discovered: list[dict[str, Any]] = []
    for release_path in sorted(artifact_root.rglob("release_status_summary.json")):
        try:
            payload = _load_json(release_path)
        except Exception:
            continue
        if str(payload.get("strategy_profile", "")).strip() != "russell_top50_leader_rotation":
            continue
        artifact_dir = release_path.parent
        source_inputs_dir = artifact_dir / "source_inputs"
        prices_path = source_inputs_dir / "prices.csv"
        universe_path = source_inputs_dir / "research_universe.csv"
        if not universe_path.exists():
            universe_path = source_inputs_dir / "universe.csv"
        if not prices_path.exists() or not universe_path.exists():
            continue
        discovered.append(
            {
                "release_path": release_path,
                "artifact_dir": artifact_dir,
                "prices_path": prices_path,
                "universe_path": universe_path,
                "snapshot_as_of": str(payload.get("snapshot_as_of", "") or ""),
            }
        )
    return discovered


def build_crash_brake_research_from_snapshot_run(
    run: dict[str, Any],
    *,
    output_root: Path | None = None,
    rolling_window_years: str = "1",
) -> Path:
    artifact_dir = Path(run["artifact_dir"])
    output_dir = (
        output_root / f"{artifact_dir.name}__russell_top50_crash_brake_research"
        if output_root is not None
        else artifact_dir / "russell_top50_crash_brake_research"
    )
    args = [
        "--prices",
        str(run["prices_path"]),
        "--universe",
        str(run["universe_path"]),
        "--output-dir",
        str(output_dir),
        "--rolling-window-years",
        str(rolling_window_years),
    ]
    snapshot_as_of = str(run.get("snapshot_as_of", "") or "").strip()
    if snapshot_as_of:
        args.extend(["--end", snapshot_as_of])
    if research_main(args):
        raise RuntimeError(f"failed to build crash-brake research for {artifact_dir}")
    return output_dir


def main() -> int:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    output_root = Path(args.output_root) if args.output_root else None
    outputs: list[Path] = []
    for run in discover_russell_snapshot_runs(artifact_root):
        outputs.append(
            build_crash_brake_research_from_snapshot_run(
                run,
                output_root=output_root,
                rolling_window_years=str(args.rolling_window_years or "1"),
            )
        )
    print(f"crash_brake_research_count={len(outputs)}")
    for path in outputs:
        print(f"crash_brake_research_dir={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
