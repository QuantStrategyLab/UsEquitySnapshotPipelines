#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copy resolved snapshot source inputs into an artifact directory.")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--prices")
    parser.add_argument("--universe")
    parser.add_argument("--research-universe")
    parser.add_argument("--source-input-manifest")
    return parser


def _copy_optional(source: str | None, target: Path) -> str:
    if not str(source or "").strip():
        return ""
    resolved = Path(str(source)).expanduser()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(f"source input file not found: {resolved}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(resolved, target)
    return str(resolved)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_dir = Path(args.artifact_dir)
    source_inputs_dir = artifact_dir / "source_inputs"
    source_inputs_dir.mkdir(parents=True, exist_ok=True)

    staged_sources = {
        "prices": _copy_optional(args.prices, source_inputs_dir / "prices.csv"),
        "universe": _copy_optional(args.universe, source_inputs_dir / "universe.csv"),
        "research_universe": _copy_optional(args.research_universe, source_inputs_dir / "research_universe.csv"),
        "source_input_manifest": _copy_optional(
            args.source_input_manifest,
            source_inputs_dir / "source_input_manifest.json",
        ),
    }
    manifest = {
        "manifest_type": "snapshot_source_inputs",
        "artifact_schema_version": "snapshot_source_inputs.v1",
        "staged_files": {
            key: value
            for key, value in {
                "prices": "prices.csv" if staged_sources["prices"] else "",
                "universe": "universe.csv" if staged_sources["universe"] else "",
                "research_universe": "research_universe.csv" if staged_sources["research_universe"] else "",
                "source_input_manifest": "source_input_manifest.json" if staged_sources["source_input_manifest"] else "",
            }.items()
            if value
        },
        "source_paths": {
            key: value for key, value in staged_sources.items() if value
        },
    }
    (source_inputs_dir / "source_inputs_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"staged_source_inputs_dir={source_inputs_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
