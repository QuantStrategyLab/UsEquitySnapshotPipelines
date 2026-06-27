#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from us_equity_snapshot_pipelines.contracts import get_profile_contract


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate publish-time snapshot artifact layout before upload.")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--summary-file", default="")
    parser.add_argument("--json-file", default="")
    return parser


def _json_text(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def collect_artifact_layout(profile: str, artifact_dir: Path) -> dict[str, Any]:
    contract = get_profile_contract(profile)
    paths = contract.artifact_paths(artifact_dir)
    required_files = {
        "snapshot": paths["snapshot"],
        "manifest": paths["manifest"],
        "ranking": paths["ranking"],
        "release_summary": paths["release_summary"],
    }
    source_inputs_dir = artifact_dir / "source_inputs"
    required_source_inputs: tuple[str, ...] = ()
    if profile == "russell_top50_leader_rotation":
        required_source_inputs = ("prices.csv", "research_universe.csv")
    source_input_files = {
        filename: source_inputs_dir / filename
        for filename in required_source_inputs
    }
    return {
        "profile": profile,
        "artifact_dir": str(artifact_dir),
        "missing_files": [name for name, path in required_files.items() if not path.exists()],
        "present_files": [name for name, path in required_files.items() if path.exists()],
        "source_inputs_dir": str(source_inputs_dir),
        "missing_source_inputs": [name for name, path in source_input_files.items() if not path.exists()],
        "present_source_inputs": [name for name, path in source_input_files.items() if path.exists()],
    }


def render_summary(layout: dict[str, Any]) -> str:
    missing_files = list(layout["missing_files"])
    present_files = list(layout["present_files"])
    missing_source_inputs = list(layout["missing_source_inputs"])
    present_source_inputs = list(layout["present_source_inputs"])
    lines = [
        "## Snapshot artifact layout diagnostics",
        "",
        f"- Profile: `{layout['profile']}`",
        f"- Artifact dir: `{layout['artifact_dir']}`",
        f"- Missing required core files: `{len(missing_files)}`",
        f"- Missing required source inputs: `{len(missing_source_inputs)}`",
        "",
        "| Category | Present | Missing |",
        "| --- | --- | --- |",
        f"| core files | {','.join(present_files) or 'none'} | {','.join(missing_files) or 'none'} |",
        f"| source inputs | {','.join(present_source_inputs) or 'none'} | {','.join(missing_source_inputs) or 'none'} |",
        "",
    ]
    if missing_files:
        lines.extend(
            [
                "### Missing core files",
                "",
                *[f"- `{name}`" for name in missing_files],
                "",
            ]
        )
    if missing_source_inputs:
        lines.extend(
            [
                "### Missing source inputs",
                "",
                *[f"- `{name}`" for name in missing_source_inputs],
                "",
            ]
        )
    if not missing_files and not missing_source_inputs:
        lines.extend(
            [
                "All required core files and staged source inputs are present.",
                "",
            ]
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    artifact_dir = Path(args.artifact_dir)
    layout = collect_artifact_layout(str(args.profile), artifact_dir)
    summary = render_summary(layout)
    if args.summary_file:
        summary_path = Path(args.summary_file)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(summary, encoding="utf-8")
        print(f"artifact_layout_summary_file={summary_path}")
    if args.json_file:
        json_path = Path(args.json_file)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(_json_text(layout), encoding="utf-8")
        print(f"artifact_layout_json_file={json_path}")
    print(_json_text(layout), end="")
    if layout["missing_files"] or layout["missing_source_inputs"]:
        raise SystemExit(
            "Snapshot artifact layout is incomplete: "
            f"missing_files={','.join(layout['missing_files']) or 'none'}; "
            f"missing_source_inputs={','.join(layout['missing_source_inputs']) or 'none'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
