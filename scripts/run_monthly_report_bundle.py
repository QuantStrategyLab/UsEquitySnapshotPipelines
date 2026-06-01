#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.contracts import list_profile_contracts


DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"
DEFAULT_OUTPUT_DIR = DEFAULT_ARTIFACT_ROOT / "monthly_report_bundle"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assemble a monthly AI review bundle from US equity snapshot profile artifacts.",
    )
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report-month", default="")
    parser.add_argument("--ranking-preview-size", type=int, default=5)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _discover_release_summaries(artifact_root: Path) -> dict[str, Path]:
    discovered: dict[str, Path] = {}
    for summary_path in sorted(artifact_root.rglob("release_status_summary.json")):
        try:
            summary = load_json(summary_path)
        except Exception:
            continue
        profile = str(summary.get("strategy_profile", "")).strip()
        if profile and profile not in discovered:
            discovered[profile] = summary_path
    return discovered


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _selected_symbols_from_summary(summary: dict[str, Any]) -> set[str] | None:
    diagnostics = summary.get("diagnostics")
    if not isinstance(diagnostics, dict) or "selected_symbols" not in diagnostics:
        return None
    raw_symbols = diagnostics.get("selected_symbols")
    if isinstance(raw_symbols, str):
        values = raw_symbols.split(",")
    elif isinstance(raw_symbols, (list, tuple, set)):
        values = raw_symbols
    else:
        values = ()
    return {normalized for symbol in values if (normalized := _normalize_symbol(symbol))}


def _selected_preview_value(row: dict[str, str], selected_symbols: set[str] | None) -> str:
    explicit = row.get("selected_flag") or row.get("selected") or row.get("is_selected")
    if explicit:
        return explicit
    if selected_symbols is None:
        return ""
    symbol = _normalize_symbol(row.get("symbol") or row.get("ticker"))
    return str(symbol in selected_symbols).lower() if symbol else ""


def _ranking_preview(path: Path, limit: int, *, selected_symbols: set[str] | None = None) -> list[dict[str, Any]]:
    if not path.exists() or limit <= 0:
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                {
                    "rank": row.get("current_rank") or row.get("rank") or "",
                    "symbol": row.get("symbol") or row.get("ticker") or "",
                    "score": row.get("final_score") or row.get("score") or "",
                    "selected": _selected_preview_value(row, selected_symbols),
                }
            )
            if len(rows) >= limit:
                break
    return rows


def _collect_profile(artifact_root: Path, profile: str, summary_path: Path | None, ranking_preview_size: int) -> dict[str, Any]:
    contract = next(item for item in list_profile_contracts() if item.profile == profile)
    if summary_path is None:
        expected_dir = artifact_root / profile
        expected_paths = contract.artifact_paths(expected_dir)
        return {
            "profile": profile,
            "display_name": contract.display_name,
            "contract_version": contract.contract_version,
            "status": "missing",
            "artifact_dir": str(expected_dir),
            "snapshot_as_of": "",
            "row_count": 0,
            "missing_files": [path.name for path in expected_paths.values()],
            "present_files": [],
            "ranking_preview": [],
        }

    artifact_dir = summary_path.parent
    summary = load_json(summary_path)
    paths = contract.artifact_paths(artifact_dir)
    present_files = [path.name for path in paths.values() if path.exists()]
    missing_files = [path.name for path in paths.values() if not path.exists()]
    ranking_preview = _ranking_preview(
        paths["ranking"],
        ranking_preview_size,
        selected_symbols=_selected_symbols_from_summary(summary),
    )
    return {
        "profile": profile,
        "display_name": contract.display_name,
        "contract_version": contract.contract_version,
        "status": str(summary.get("release_status", "unknown")),
        "artifact_dir": str(artifact_dir),
        "snapshot_as_of": str(summary.get("snapshot_as_of", "") or ""),
        "row_count": int(summary.get("row_count", 0) or 0),
        "signal_description": str(summary.get("signal_description", "") or ""),
        "status_description": str(summary.get("status_description", "") or ""),
        "missing_files": missing_files,
        "present_files": present_files,
        "ranking_preview": ranking_preview,
    }


def build_bundle(
    artifact_root: Path | str,
    *,
    report_month: str = "",
    ranking_preview_size: int = 5,
) -> dict[str, Any]:
    root = Path(artifact_root)
    discovered = _discover_release_summaries(root)
    profiles = [
        _collect_profile(root, contract.profile, discovered.get(contract.profile), ranking_preview_size)
        for contract in list_profile_contracts()
    ]
    status = "ok" if all(profile["status"] != "missing" and not profile["missing_files"] for profile in profiles) else "warning"
    snapshot_dates = sorted({profile["snapshot_as_of"] for profile in profiles if profile["snapshot_as_of"]})
    if not report_month:
        report_month = snapshot_dates[-1][:7] if snapshot_dates else datetime.now(UTC).strftime("%Y-%m")
    return {
        "schema_version": "2026-05-10",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "report_month": report_month,
        "source_project": "UsEquitySnapshotPipelines",
        "status": status,
        "artifact_root": str(root),
        "profile_count": len(profiles),
        "missing_profile_count": sum(1 for profile in profiles if profile["status"] == "missing"),
        "profiles": profiles,
    }


def render_job_summary(bundle: dict[str, Any]) -> str:
    lines = [
        "# US Equity Monthly Review Bundle",
        "",
        f"- Report month: `{bundle['report_month']}`",
        f"- Status: `{bundle['status']}`",
        f"- Profiles: `{bundle['profile_count']}`",
        f"- Missing profiles: `{bundle['missing_profile_count']}`",
        "",
        "## Profiles",
        "",
        "| Profile | Status | Snapshot date | Rows | Missing files |",
        "| --- | --- | --- | ---: | --- |",
    ]
    for profile in bundle["profiles"]:
        missing = ", ".join(profile["missing_files"]) if profile["missing_files"] else "none"
        lines.append(
            f"| `{profile['profile']}` | `{profile['status']}` | "
            f"{profile['snapshot_as_of'] or 'n/a'} | {profile['row_count']} | {missing} |"
        )
    return "\n".join(lines).strip() + "\n"


def render_ai_review_input(bundle: dict[str, Any]) -> str:
    lines = [
        "# US Equity Snapshot Monthly Review Input",
        "",
        "Use this file as the primary input for the monthly AI review of UsEquitySnapshotPipelines.",
        "",
        "## Review Intent",
        "",
        "- This is an upstream snapshot artifact review, not a broker execution review.",
        "- Focus on artifact completeness, profile contract health, stale or missing snapshot evidence, and downstream impact.",
        "- Do not recommend production strategy changes from one monthly artifact alone.",
        "- Treat missing profile artifacts as review-blocking evidence gaps.",
        "",
        "## Bundle Metadata",
        "",
        f"- Report month: `{bundle['report_month']}`",
        f"- Status: `{bundle['status']}`",
        f"- Source project: `{bundle['source_project']}`",
        f"- Artifact root: `{bundle['artifact_root']}`",
        "",
        "## Profile Summaries",
    ]
    for profile in bundle["profiles"]:
        lines.extend(
            [
                "",
                f"### {profile['display_name']} (`{profile['profile']}`)",
                "",
                f"- Status: `{profile['status']}`",
                f"- Contract version: `{profile['contract_version']}`",
                f"- Snapshot as-of: `{profile['snapshot_as_of'] or 'n/a'}`",
                f"- Row count: `{profile['row_count']}`",
                f"- Missing files: `{', '.join(profile['missing_files']) if profile['missing_files'] else 'none'}`",
                f"- Signal: {profile.get('signal_description') or 'n/a'}",
                f"- Status note: {profile.get('status_description') or 'n/a'}",
                "",
                "Ranking preview:",
            ]
        )
        if profile["ranking_preview"]:
            lines.append("")
            lines.append("| Rank | Symbol | Score | Selected |")
            lines.append("| --- | --- | --- | --- |")
            for row in profile["ranking_preview"]:
                lines.append(
                    f"| {row['rank'] or 'n/a'} | {row['symbol'] or 'n/a'} | "
                    f"{row['score'] or 'n/a'} | {row['selected'] or 'n/a'} |"
                )
        else:
            lines.append("")
            lines.append("_No ranking preview available._")
    lines.extend(
        [
            "",
            "## Review Questions",
            "",
            "1. Are all expected monthly snapshot profiles present and internally complete?",
            "2. Do snapshot dates, row counts, and contract versions look suitable for downstream runtimes?",
            "3. Are any missing artifacts, stale snapshots, or ranking previews review blockers?",
            "4. What low-risk follow-up tasks should operators consider before enabling automated fixes?",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def write_bundle(bundle: dict[str, Any], output_dir: Path | str) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "monthly_report_bundle.json"
    summary_path = root / "job_summary.md"
    review_path = root / "ai_review_input.md"
    manifest_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    summary_path.write_text(render_job_summary(bundle), encoding="utf-8")
    review_path.write_text(render_ai_review_input(bundle), encoding="utf-8")
    return {"manifest": manifest_path, "summary": summary_path, "review": review_path}


def main() -> int:
    args = parse_args()
    bundle = build_bundle(
        args.artifact_root,
        report_month=args.report_month,
        ranking_preview_size=args.ranking_preview_size,
    )
    outputs = write_bundle(bundle, args.output_dir)
    print(f"status={bundle['status']}")
    print(f"report_month={bundle['report_month']}")
    print(f"bundle={outputs['manifest']}")
    print(f"ai_review_input={outputs['review']}")
    print(f"job_summary={outputs['summary']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
