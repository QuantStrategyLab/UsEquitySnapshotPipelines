from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .live_replacement_review import _render_markdown
from .russell_1000_multi_factor_defensive_snapshot import read_table

PROMOTION_BUNDLE_SCHEMA_VERSION = "snapshot_us_equity_promotion_bundle.v1"


def _input_entry(path: str | Path | None) -> dict[str, object]:
    if path is None:
        return {}
    resolved = Path(path)
    payload: dict[str, object] = {"path": str(resolved)}
    if resolved.exists():
        payload["exists"] = True
    return payload


def _read_optional_table(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    resolved = Path(path)
    if not resolved.exists():
        return pd.DataFrame()
    try:
        return read_table(resolved)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _match_artifact_by_candidates(
    paths: list[Path],
    *,
    candidate_ids: list[str] | None,
    candidate_column: str,
) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    candidate_set = {str(item).strip() for item in (candidate_ids or []) if str(item).strip()}
    fallback = pd.DataFrame()
    for path in paths:
        frame = _read_optional_table(path)
        if frame.empty:
            continue
        if fallback.empty:
            fallback = frame
        if not candidate_set or candidate_column not in frame.columns:
            continue
        values = {str(value).strip() for value in frame[candidate_column].dropna().astype(str).tolist() if str(value).strip()}
        if candidate_set.intersection(values):
            return frame
    return fallback


def _bool_series(frame: pd.DataFrame, column: str, *, default: bool = False) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(default, index=frame.index, dtype=bool)
    return frame[column].fillna(default).astype(bool)


def _render_pending_promotion_markdown(summary: dict[str, object]) -> str:
    pending = list(summary.get("pending_promotion_candidates") or [])
    lines = [
        "# Pending promotion candidates",
        "",
        f"- Bundle decision: `{str(summary.get('bundle_decision', 'unknown') or 'unknown')}`",
        f"- Candidate count: `{int(summary.get('pending_promotion_candidate_count', 0) or 0)}`",
        f"- Replace-live-now count: `{int(summary.get('replace_live_now_count', 0) or 0)}`",
        "",
    ]
    if not pending:
        lines.append("No pending-promotion candidates.")
        lines.append("")
        return "\n".join(lines)

    lines.extend(
        [
            "| Candidate | Next action | Blocking reason | Replace live now |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in pending:
        lines.append(
            "| {candidate} | {next_action} | {blocking_reason} | {replace_live_now} |".format(
                candidate=str(row.get("candidate", "")).replace("|", "\\|"),
                next_action=str(row.get("next_action", "")).replace("|", "\\|"),
                blocking_reason=str(row.get("blocking_reason", "")).replace("|", "\\|"),
                replace_live_now="yes" if bool(row.get("replace_live_now")) else "no",
            )
        )
    lines.append("")
    return "\n".join(lines)


def _render_bundle_status_markdown(summary: dict[str, object]) -> str:
    pending = list(summary.get("pending_promotion_candidates") or [])
    lines = [
        "# Snapshot promotion bundle status",
        "",
        f"- Bundle decision: `{str(summary.get('bundle_decision', 'unknown') or 'unknown')}`",
        f"- Candidate count: `{len(list(summary.get('candidate_ids') or []))}`",
        f"- Replace-live-now count: `{int(summary.get('replace_live_now_count', 0) or 0)}`",
        f"- Pending-promotion count: `{int(summary.get('pending_promotion_candidate_count', 0) or 0)}`",
        "",
    ]
    if pending:
        top = pending[0]
        lines.extend(
            [
                "## Primary pending candidate",
                "",
                f"- Candidate: `{str(top.get('candidate', '') or '')}`",
                f"- Next action: `{str(top.get('next_action', '') or '')}`",
                f"- Blocking reason: `{str(top.get('blocking_reason', '') or '')}`",
                "",
            ]
        )
    lines.append("Generated from `promotion_bundle_summary.json`.")
    lines.append("")
    return "\n".join(lines)


def build_snapshot_promotion_bundle(
    *,
    artifact_dir: str | Path,
    output_dir: str | Path,
    candidate_ids: list[str] | None = None,
) -> dict[str, object]:
    artifact_root = Path(artifact_dir)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    ranking = read_table(artifact_root / "ranking.csv")
    review = read_table(artifact_root / "live_replacement_bundle" / f"live_replacement_review_{artifact_root.name}" / "live_replacement_review.csv")
    shadow = _match_artifact_by_candidates(
        sorted((artifact_root.parent).glob("snapshot_shadow_review_*/*shadow_review_rows.csv")),
        candidate_ids=candidate_ids,
        candidate_column="candidate",
    )
    decay = _match_artifact_by_candidates(
        sorted((artifact_root.parent).glob("live_decay_monitor_snapshot_*/*live_decay_strategy_summary.csv")),
        candidate_ids=candidate_ids,
        candidate_column="strategy",
    )

    candidate_list = candidate_ids or (
        review.loc[review.get("replace_live_now", False).astype(bool), "candidate"].astype(str).tolist()
        if not review.empty else []
    )
    if candidate_list:
        ranking = ranking.loc[ranking["Candidate"].astype(str).isin(candidate_list)].copy()
        review = review.loc[review["candidate"].astype(str).isin(candidate_list)].copy()
        if not shadow.empty and "candidate" in shadow.columns:
            shadow = shadow.loc[shadow["candidate"].astype(str).isin(candidate_list)].copy()
        if not decay.empty and "strategy" in decay.columns:
            decay = decay.loc[decay["strategy"].astype(str).isin(candidate_list)].copy()

    review.to_csv(out / "live_replacement_review.csv", index=False)
    (out / "live_replacement_review.md").write_text(_render_markdown(review), encoding="utf-8")
    ranking.to_csv(out / "ranking_selected.csv", index=False)
    if not shadow.empty:
        shadow.to_csv(out / "shadow_review_selected.csv", index=False)
    if not decay.empty:
        decay.to_csv(out / "live_decay_selected.csv", index=False)

    pending_mask = pd.Series(False, index=review.index, dtype=bool)
    if not review.empty:
        pending_mask = (
            _bool_series(review, "required_gates_passed")
            & _bool_series(review, "shadow_review_passed")
            & _bool_series(review, "live_decay_present")
            & ~_bool_series(review, "live_decay_passed")
        )
    pending_rows = review.loc[pending_mask].copy() if not review.empty else pd.DataFrame()
    bundle_decision = "research_only"
    if not review.empty and int(review["replace_live_now"].sum()) > 0:
        bundle_decision = "replace_live_now"
    elif not pending_rows.empty:
        bundle_decision = "pending_promotion"
    summary = {
        "candidate_ids": candidate_list,
        "bundle_decision": bundle_decision,
        "review_row_count": int(len(review)),
        "replace_live_now_count": int(review["replace_live_now"].sum()) if not review.empty else 0,
        "required_gates_passed_count": int(review["required_gates_passed"].sum()) if not review.empty else 0,
        "pending_promotion_candidate_count": int(len(pending_rows)),
        "pending_promotion_candidates": pending_rows.loc[
            :,
            [column for column in ("candidate", "next_action", "blocking_reason", "replace_live_now") if column in pending_rows.columns],
        ].to_dict(orient="records")
        if not pending_rows.empty else [],
        "review_rows": review.to_dict(orient="records"),
    }
    (out / "promotion_bundle_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out / "pending_promotion_summary.md").write_text(_render_pending_promotion_markdown(summary), encoding="utf-8")
    (out / "bundle_status.md").write_text(_render_bundle_status_markdown(summary), encoding="utf-8")

    manifest = {
        "manifest_type": "snapshot_us_equity_promotion_bundle",
        "artifact_schema_version": PROMOTION_BUNDLE_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "strategy_line": "snapshot_us_equity",
        "source_artifact_dir": str(artifact_root),
        "candidate_ids": candidate_list,
        "bundle_decision": str(summary["bundle_decision"]),
        "pending_promotion_candidate_count": int(summary["pending_promotion_candidate_count"]),
        "inputs": {
            "ranking": _input_entry(artifact_root / "ranking.csv"),
            "live_replacement_review": _input_entry(artifact_root / "live_replacement_bundle" / f"live_replacement_review_{artifact_root.name}" / "live_replacement_review.csv"),
            "shadow_review": _input_entry(None if shadow.empty else out / "shadow_review_selected.csv"),
            "live_decay": _input_entry(None if decay.empty else out / "live_decay_selected.csv"),
        },
        "artifacts": {
            "live_replacement_review": {"path": "live_replacement_review.csv"},
            "live_replacement_markdown": {"path": "live_replacement_review.md"},
            "ranking_selected": {"path": "ranking_selected.csv"},
            "promotion_bundle_summary": {"path": "promotion_bundle_summary.json"},
            "bundle_status": {"path": "bundle_status.md"},
            "pending_promotion_summary": {"path": "pending_promotion_summary.md"},
        },
        "outputs": [
            "live_replacement_review.csv",
            "live_replacement_review.md",
            "ranking_selected.csv",
            "promotion_bundle_summary.json",
            "bundle_status.md",
            "pending_promotion_summary.md",
            "promotion_bundle_manifest.json",
        ],
    }
    if not shadow.empty:
        manifest["artifacts"]["shadow_review_selected"] = {"path": "shadow_review_selected.csv"}
        manifest["outputs"].insert(3, "shadow_review_selected.csv")
    if not decay.empty:
        manifest["artifacts"]["live_decay_selected"] = {"path": "live_decay_selected.csv"}
        manifest["outputs"].insert(4 if not shadow.empty else 3, "live_decay_selected.csv")
    (out / "promotion_bundle_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"manifest": manifest, "summary": summary}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build snapshot US equity promotion bundle artifacts for downstream runtime repos.")
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--candidate-ids", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candidate_ids = [item.strip() for item in str(args.candidate_ids).split(",") if item.strip()]
    result = build_snapshot_promotion_bundle(
        artifact_dir=args.artifact_dir,
        output_dir=args.output_dir,
        candidate_ids=candidate_ids or None,
    )
    print(f"snapshot_promotion_bundle_candidates={len(result['summary']['candidate_ids'])}")
    print(f"snapshot_promotion_bundle_replace_live_now_count={result['summary']['replace_live_now_count']}")
    print(f"snapshot_promotion_bundle_pending_promotion_count={result['summary']['pending_promotion_candidate_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
