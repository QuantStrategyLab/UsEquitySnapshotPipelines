#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

DEFAULT_ARTIFACT_ROOT = Path(__file__).resolve().parents[1] / "data" / "output"
REQUIRED_SOURCE_INPUT_FILES = ("prices.csv", "research_universe.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Russell snapshot artifacts have the staged inputs required for monthly crash-brake research."
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--summary-file", default="")
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def discover_russell_snapshot_artifacts(artifact_root: Path) -> list[dict[str, Any]]:
    discovered: list[dict[str, Any]] = []
    for summary_path in sorted(artifact_root.rglob("release_status_summary.json")):
        try:
            payload = _load_json(summary_path)
        except Exception:
            continue
        if str(payload.get("strategy_profile", "")).strip() != "russell_top50_leader_rotation":
            continue
        artifact_dir = summary_path.parent
        source_inputs_dir = artifact_dir / "source_inputs"
        discovered.append(
            {
                "artifact_dir": artifact_dir,
                "summary_path": summary_path,
                "release_status": str(payload.get("release_status", "") or ""),
                "source_inputs_dir": source_inputs_dir,
                "missing_source_inputs": [
                    filename for filename in REQUIRED_SOURCE_INPUT_FILES if not (source_inputs_dir / filename).exists()
                ],
            }
        )
    return discovered


def render_summary(discovered: list[dict[str, Any]]) -> str:
    lines = [
        "## Russell crash-brake input diagnostics",
        "",
        f"- Russell snapshot artifacts found: `{len(discovered)}`",
    ]
    invalid = [item for item in discovered if item["missing_source_inputs"]]
    lines.append(f"- Artifacts missing staged source inputs: `{len(invalid)}`")
    lines.append("")
    if not discovered:
        lines.append("No `russell_top50_leader_rotation` snapshot artifacts were found under the current artifact root.")
        lines.append("")
        return "\n".join(lines)
    lines.extend(
        [
            "| Artifact Dir | Release Status | Missing Source Inputs |",
            "| --- | --- | --- |",
        ]
    )
    for item in discovered:
        lines.append(
            "| {artifact_dir} | {release_status} | {missing} |".format(
                artifact_dir=str(item["artifact_dir"]).replace("|", "\\|"),
                release_status=str(item["release_status"]).replace("|", "\\|") or "unknown",
                missing=",".join(item["missing_source_inputs"]) or "none",
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    discovered = discover_russell_snapshot_artifacts(artifact_root)
    summary = render_summary(discovered)
    if args.summary_file:
        summary_path = Path(args.summary_file)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(summary, encoding="utf-8")
    print(f"russell_snapshot_artifact_count={len(discovered)}")
    invalid = [item for item in discovered if item["missing_source_inputs"]]
    for item in discovered:
        print(f"russell_snapshot_artifact_dir={item['artifact_dir']}")
        print(f"russell_snapshot_release_status={item['release_status']}")
        print(f"russell_snapshot_missing_source_inputs={','.join(item['missing_source_inputs'])}")
    if args.summary_file:
        print(f"russell_snapshot_summary_file={args.summary_file}")
    if invalid:
        missing_text = "; ".join(
            f"{item['artifact_dir']}: {','.join(item['missing_source_inputs'])}" for item in invalid
        )
        raise SystemExit(
            f"Russell crash-brake monthly inputs are incomplete; missing staged source inputs -> {missing_text}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
