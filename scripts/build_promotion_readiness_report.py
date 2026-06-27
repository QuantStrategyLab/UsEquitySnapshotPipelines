#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON object expected: {path}")
    return dict(payload)


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def _latest_rank_from_path(path: Path) -> tuple[str, float, str]:
    text = str(path)
    best = ""
    for match in re.finditer(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", text):
        year, month, day = match.groups()
        candidate = f"{year}{month}{day}"
        if candidate > best:
            best = candidate
    try:
        mtime = float(path.stat().st_mtime)
    except OSError:
        mtime = 0.0
    return (best, mtime, text)


def _read_strategy_rows(artifact_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for manifest_path in sorted(artifact_root.rglob("live_replacement_manifest.json")):
        payload = _load_json(manifest_path)
        if str(payload.get("manifest_type", "") or "") != "live_replacement_review":
            continue
        review_path = manifest_path.parent / "live_replacement_review.csv"
        if not review_path.exists():
            continue
        frame = pd.read_csv(review_path)
        for row in frame.to_dict(orient="records"):
            rows.append(
                {
                    "scope_type": "strategy",
                    "family": _text(row.get("strategy_line")),
                    "item_name": _text(row.get("candidate")),
                    "item_role": "",
                    "required_gates_passed": _bool(row.get("required_gates_passed")),
                    "shadow_review_present": _bool(row.get("shadow_review_present")),
                    "shadow_review_passed": _bool(row.get("shadow_review_passed")),
                    "live_decay_present": _bool(row.get("live_decay_present")),
                    "live_decay_passed": _bool(row.get("live_decay_passed")),
                    "replace_live_now": _bool(row.get("replace_live_now")),
                    "blocking_reason": _text(row.get("blocking_reason")),
                    "recommended_action": _text(row.get("next_action") or row.get("current_recommendation")),
                    "manifest_path": str(manifest_path),
                    "review_path": str(review_path),
                    "_latest_rank": _latest_rank_from_path(manifest_path),
                }
            )
    return rows


def _read_plugin_rows(artifact_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for manifest_path in sorted(artifact_root.rglob("plugin_promotion_review_manifest.json")):
        payload = _load_json(manifest_path)
        if str(payload.get("manifest_type", "") or "") != "strategy_plugin_promotion_review":
            continue
        review_path = manifest_path.parent / "plugin_promotion_review.csv"
        if not review_path.exists():
            continue
        frame = pd.read_csv(review_path)
        for row in frame.to_dict(orient="records"):
            rows.append(
                {
                    "scope_type": "plugin",
                    "family": _text(row.get("strategy")),
                    "item_name": _text(row.get("plugin")),
                    "item_role": _text(row.get("plugin_role")),
                    "required_gates_passed": _bool(row.get("required_gates_passed")),
                    "shadow_review_present": _bool(row.get("shadow_review_present")),
                    "shadow_review_passed": _bool(row.get("shadow_review_passed")),
                    "live_decay_present": _bool(row.get("live_decay_present")),
                    "live_decay_passed": _bool(row.get("live_decay_passed")),
                    "replace_live_now": _bool(row.get("replace_live_component_now")),
                    "blocking_reason": _text(row.get("blocking_reason")),
                    "recommended_action": _text(row.get("recommended_action")),
                    "manifest_path": str(manifest_path),
                    "review_path": str(review_path),
                    "_latest_rank": _latest_rank_from_path(manifest_path),
                }
            )
    return rows


def _dedupe_latest_only(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    working = working.sort_values(
        by=["scope_type", "family", "item_name", "item_role", "_latest_rank"],
        ascending=[True, True, True, True, True],
        kind="stable",
    )
    working = working.drop_duplicates(
        subset=["scope_type", "family", "item_name", "item_role"],
        keep="last",
    )
    return working.reset_index(drop=True)


def _apply_filters(
    frame: pd.DataFrame,
    *,
    scope_type: str = "",
    family: str = "",
    item_name: str = "",
) -> pd.DataFrame:
    if frame.empty:
        return frame
    filtered = frame.copy()
    if str(scope_type).strip():
        filtered = filtered.loc[filtered["scope_type"].astype(str).eq(str(scope_type).strip())]
    if str(family).strip():
        filtered = filtered.loc[filtered["family"].astype(str).eq(str(family).strip())]
    if str(item_name).strip():
        filtered = filtered.loc[filtered["item_name"].astype(str).eq(str(item_name).strip())]
    return filtered.reset_index(drop=True)


def build_readiness_summary(
    artifact_root: str | Path,
    *,
    latest_only: bool = False,
    scope_type: str = "",
    family: str = "",
    item_name: str = "",
) -> pd.DataFrame:
    rows = [*_read_strategy_rows(Path(artifact_root)), *_read_plugin_rows(Path(artifact_root))]
    frame = pd.DataFrame(
        rows,
        columns=[
            "scope_type",
            "family",
            "item_name",
            "item_role",
            "required_gates_passed",
            "shadow_review_present",
            "shadow_review_passed",
            "live_decay_present",
            "live_decay_passed",
            "replace_live_now",
            "blocking_reason",
            "recommended_action",
            "manifest_path",
            "review_path",
            "_latest_rank",
        ],
    )
    if frame.empty:
        return frame
    if latest_only:
        frame = _dedupe_latest_only(frame)
    frame = _apply_filters(frame, scope_type=scope_type, family=family, item_name=item_name)
    if frame.empty:
        return frame
    frame = frame.sort_values(
        by=["replace_live_now", "required_gates_passed", "scope_type", "family", "item_name"],
        ascending=[False, False, True, True, True],
    ).reset_index(drop=True)
    if "_latest_rank" in frame.columns:
        frame = frame.drop(columns=["_latest_rank"])
    return frame


def build_blocker_counts(summary: pd.DataFrame) -> pd.DataFrame:
    counts: dict[str, int] = {}
    for value in summary.get("blocking_reason", pd.Series(dtype=str)).fillna(""):
        for part in [item.strip() for item in str(value).split(";") if item.strip()]:
            counts[part] = counts.get(part, 0) + 1
    rows = [{"blocking_reason": key, "count": value} for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]
    return pd.DataFrame(rows, columns=["blocking_reason", "count"])


def render_markdown(summary: pd.DataFrame, blockers: pd.DataFrame, *, latest_only: bool = False) -> str:
    lines = [
        "# Promotion readiness summary",
        "",
        f"- View mode: `{'latest_only' if latest_only else 'all_artifacts'}`",
        f"- Total rows: `{len(summary)}`",
        f"- Replace-live-now rows: `{int(summary['replace_live_now'].sum()) if not summary.empty else 0}`",
        "",
        "## Top blockers",
        "",
    ]
    if blockers.empty:
        lines.append("No blockers.")
    else:
        lines.extend(
            [
                "| Blocking reason | Count |",
                "| --- | ---: |",
                *[f"| {row['blocking_reason']} | {int(row['count'])} |" for row in blockers.to_dict(orient="records")],
            ]
        )
    lines.extend(["", "## Rows", ""])
    if summary.empty:
        lines.append("No readiness rows discovered.")
    else:
        lines.extend(
            [
                "| Scope | Family | Item | Role | Required gates | Replace now | Blocking reason | Recommended action |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
                *[
                    "| "
                    + " | ".join(
                        [
                            str(row["scope_type"]),
                            str(row["family"]),
                            str(row["item_name"]),
                            str(row["item_role"]),
                            str(bool(row["required_gates_passed"])).lower(),
                            str(bool(row["replace_live_now"])).lower(),
                            str(row["blocking_reason"]),
                            str(row["recommended_action"]),
                        ]
                    )
                    + " |"
                    for row in summary.to_dict(orient="records")
                ],
            ]
        )
    lines.append("")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate promotion readiness and blocker summaries.")
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--scope-type", default="", help="Optional exact scope_type filter, e.g. strategy or plugin.")
    parser.add_argument("--family", default="", help="Optional exact family filter.")
    parser.add_argument("--item-name", default="", help="Optional exact item_name filter.")
    parser.add_argument(
        "--latest-only",
        action="store_true",
        help="Keep only the latest readiness row per scope/family/item tuple.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = build_readiness_summary(
        args.artifact_root,
        latest_only=bool(args.latest_only),
        scope_type=str(args.scope_type or ""),
        family=str(args.family or ""),
        item_name=str(args.item_name or ""),
    )
    blockers = build_blocker_counts(summary)
    summary.to_csv(output_dir / "promotion_readiness_summary.csv", index=False)
    blockers.to_csv(output_dir / "promotion_blocker_counts.csv", index=False)
    (output_dir / "promotion_readiness.md").write_text(
        render_markdown(summary, blockers, latest_only=bool(args.latest_only)),
        encoding="utf-8",
    )
    print(f"promotion_readiness_rows={len(summary)}")
    print(f"promotion_blocker_count={len(blockers)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
