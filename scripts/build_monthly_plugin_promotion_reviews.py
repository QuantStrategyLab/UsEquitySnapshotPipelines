#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.plugin_promotion_review import write_plugin_promotion_review_artifacts  # noqa: E402


def discover_plugin_promotion_review_inputs(
    artifact_root: str | Path,
    explicit_paths: list[str] | None = None,
) -> list[Path]:
    if explicit_paths:
        return [Path(path) for path in explicit_paths if str(path).strip()]
    return sorted(Path(artifact_root).rglob("ibit_dca_research_manifest.json"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build plugin promotion review artifacts from monthly research outputs.")
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument(
        "--ibit-dca-research-manifest",
        action="append",
        default=None,
        help="Optional explicit ibit_dca_research_manifest.json path; can be supplied multiple times.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_root = Path(args.artifact_root)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    count = 0
    for manifest_path in discover_plugin_promotion_review_inputs(artifact_root, args.ibit_dca_research_manifest):
        name = manifest_path.parent.name or manifest_path.stem
        output_dir = output_root / f"{name}__plugin_promotion_review"
        write_plugin_promotion_review_artifacts(
            source_manifest_path=manifest_path,
            output_dir=output_dir,
        )
        count += 1

    print(f"plugin_promotion_review_count={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
