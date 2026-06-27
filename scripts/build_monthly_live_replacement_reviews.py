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

from us_equity_snapshot_pipelines.live_replacement_review import build_live_replacement_review  # noqa: E402
from us_equity_snapshot_pipelines.russell_1000_multi_factor_defensive_snapshot import read_table  # noqa: E402

DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "data" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build live replacement review artifacts from discovered research manifests.")
    parser.add_argument("--artifact-root", default=str(DEFAULT_ARTIFACT_ROOT))
    parser.add_argument("--output-root", default="")
    return parser.parse_args()


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return safe.strip("._") or "artifact"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_table(path: Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    try:
        return read_table(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _discover_manifests(root: Path, filename: str) -> list[Path]:
    return sorted(root.rglob(filename))


def _resolve_artifact_path(manifest_path: Path, artifact_entry: Any) -> Path | None:
    if isinstance(artifact_entry, dict):
        raw = artifact_entry.get("path")
    else:
        raw = artifact_entry
    if not raw:
        return None
    path = Path(str(raw))
    if path.is_absolute():
        return path
    if path.exists():
        return path.resolve()
    return (manifest_path.parent / path).resolve()


def _promotion_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("manifest_type") != "russell_top50_promotion_bundle":
        return None
    review_path = _resolve_artifact_path(path, (payload.get("artifacts") or {}).get("live_promotion_review"))
    if not review_path or not review_path.exists():
        return None
    return {
        "manifest_path": path,
        "review_path": review_path,
        "candidate_runs": tuple(str(item) for item in payload.get("candidate_runs") or ()),
    }


def _crash_brake_promotion_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("manifest_type") != "russell_top50_crash_brake_promotion_review":
        return None
    review_path = _resolve_artifact_path(path, (payload.get("artifacts") or {}).get("live_promotion_review"))
    if not review_path or not review_path.exists():
        return None
    return {
        "manifest_path": path,
        "review_path": review_path,
        "candidate_runs": tuple(str(item) for item in payload.get("candidate_runs") or ()),
    }


def _global_etf_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("research") != "global_etf_offensive_rotation":
        return None
    root = path.parent
    ranking_path = root / "ranking.csv"
    live_readiness_path = root / "live_readiness_summary.csv"
    walk_forward_path = root / "walk_forward_selection_summary.csv"
    if not ranking_path.exists() or not live_readiness_path.exists() or not walk_forward_path.exists():
        return None
    return {
        "manifest_path": path,
        "ranking_path": ranking_path,
        "live_readiness_path": live_readiness_path,
        "walk_forward_path": walk_forward_path,
    }


def _snapshot_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("research") != "us_equity_strategy_candidates":
        return None
    ranking_path = path.parent / "ranking.csv"
    candidate_returns_path = path.parent / "candidate_daily_returns.csv"
    expected_excess_path = path.parent / "candidate_expected_excess_cagr.csv"
    walk_forward_path = path.parent / "snapshot_walk_forward_summary.csv"
    if not ranking_path.exists():
        return None
    return {
        "manifest_path": path,
        "ranking_path": ranking_path,
        "candidate_returns_path": candidate_returns_path if candidate_returns_path.exists() else None,
        "expected_excess_path": expected_excess_path if expected_excess_path.exists() else None,
        "walk_forward_path": walk_forward_path if walk_forward_path.exists() else None,
    }


def _leveraged_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("research") != "leveraged_strategy_candidates":
        return None
    ranking_path = path.parent / "ranking.csv"
    if not ranking_path.exists():
        return None
    return {
        "manifest_path": path,
        "ranking_path": ranking_path,
    }


def _global_etf_bundle_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("manifest_type") != "global_etf_promotion_bundle":
        return None
    inputs = payload.get("inputs") or {}
    ranking_path = _resolve_artifact_path(path, inputs.get("ranking"))
    live_readiness_path = _resolve_artifact_path(path, inputs.get("live_readiness_summary"))
    walk_forward_path = _resolve_artifact_path(path, inputs.get("walk_forward_selection_summary"))
    if not ranking_path or not ranking_path.exists() or not live_readiness_path or not live_readiness_path.exists() or not walk_forward_path or not walk_forward_path.exists():
        return None
    return {
        "manifest_path": path,
        "ranking_path": ranking_path,
        "live_readiness_path": live_readiness_path,
        "walk_forward_path": walk_forward_path,
        "candidate_ids": tuple(str(item) for item in payload.get("candidate_ids") or ()),
        "experiment_profile": str(payload.get("experiment_profile", "") or ""),
    }


def _shadow_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("manifest_type") != "shadow_review_artifact":
        return None
    artifacts = payload.get("artifacts") or {}
    csv_path = _resolve_artifact_path(path, artifacts.get("csv"))
    if not csv_path or not csv_path.exists():
        return None
    return {
        "manifest_path": path,
        "csv_path": csv_path,
    }


def _shadow_matches_candidates(shadow: dict[str, Any], candidate_ids: set[str]) -> bool:
    if not candidate_ids:
        return False
    csv_path = shadow.get("csv_path")
    if not csv_path:
        return False
    try:
        frame = read_table(csv_path)
    except Exception:
        return False
    if frame.empty:
        return False
    values: set[str] = set()
    for column in ("candidate", "active_candidate", "shadow_candidate", "active_variant", "shadow_variant"):
        if column in frame.columns:
            values.update(
                str(value).strip()
                for value in pd.DataFrame(frame)[column].dropna().astype(str).tolist()
                if str(value).strip()
            )
    return bool(candidate_ids.intersection(values))


def _russell_shadow_matches_candidates(shadow: dict[str, Any], candidate_runs: set[str]) -> bool:
    if not candidate_runs:
        return False
    csv_path = shadow.get("csv_path")
    if not csv_path:
        return False
    try:
        frame = read_table(csv_path)
    except Exception:
        return False
    if frame.empty:
        return False
    values: set[str] = set()
    for column in ("active_variant", "shadow_variant"):
        if column in frame.columns:
            values.update(
                str(value).strip()
                for value in pd.DataFrame(frame)[column].dropna().astype(str).tolist()
                if str(value).strip()
            )
    return bool(candidate_runs.intersection(values))


def _live_decay_input_from_manifest(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if payload.get("manifest_type") != "live_decay_monitor":
        return None
    strategies = tuple(str(item) for item in payload.get("strategies") or ())
    if not strategies:
        return None
    artifacts = payload.get("artifacts") or {}
    summary_path = _resolve_artifact_path(path, artifacts.get("live_decay_strategy_summary"))
    if not summary_path or not summary_path.exists():
        return None
    return {
        "manifest_path": path,
        "summary_path": summary_path,
        "strategies": strategies,
        "input_format": str(payload.get("input_format", "") or ""),
    }


def _render_markdown(review) -> str:
    lines = [
        "# Live Replacement Review",
        "",
        "| Strategy Line | Candidate | Recommendation | Next Action | Replace Live Now | Blocking Reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in review.to_dict(orient="records"):
        lines.append(
            "| {strategy_line} | {candidate} | {current_recommendation} | {next_action} | {replace_live_now} | {blocking_reason} |".format(
                strategy_line=str(row.get("strategy_line", "")).replace("|", "\\|"),
                candidate=str(row.get("candidate", "")).replace("|", "\\|"),
                current_recommendation=str(row.get("current_recommendation", "")).replace("|", "\\|"),
                next_action=str(row.get("next_action", "")).replace("|", "\\|"),
                replace_live_now="yes" if bool(row.get("replace_live_now")) else "no",
                blocking_reason=str(row.get("blocking_reason", "")).replace("|", "\\|"),
            )
        )
    return "\n".join(lines) + "\n"


def _output_dir_name(group: dict[str, Any]) -> str:
    group_type = str(group.get("group_type") or "russell")
    if group_type == "global_etf":
        manifest_path = Path(group["global_etf"]["manifest_path"])
    elif group_type == "snapshot":
        manifest_path = Path(group["snapshot"]["manifest_path"])
    elif group_type == "leveraged":
        manifest_path = Path(group["leveraged"]["manifest_path"])
    else:
        manifest_path = Path(group["promotion"]["manifest_path"])
    parent_name = manifest_path.parent.name
    if manifest_path.name == "promotion_bundle_manifest.json" and manifest_path.parent.parent != manifest_path.parent:
        return f"{manifest_path.parent.parent.name}__{parent_name}"
    return parent_name


def discover_replacement_review_inputs(artifact_root: Path) -> list[dict[str, Any]]:
    promotions = [item for path in _discover_manifests(artifact_root, "promotion_bundle_manifest.json") if (item := _promotion_input_from_manifest(path))]
    crash_brake_reviews = [
        item
        for path in _discover_manifests(artifact_root, "crash_brake_promotion_review_manifest.json")
        if (item := _crash_brake_promotion_input_from_manifest(path))
    ]
    global_etf_bundles = [
        item for path in _discover_manifests(artifact_root, "promotion_bundle_manifest.json") if (item := _global_etf_bundle_input_from_manifest(path))
    ]
    global_etf_runs = [item for path in _discover_manifests(artifact_root, "run_manifest.json") if (item := _global_etf_input_from_manifest(path))]
    snapshot_runs = [item for path in _discover_manifests(artifact_root, "run_manifest.json") if (item := _snapshot_input_from_manifest(path))]
    leveraged_runs = [item for path in _discover_manifests(artifact_root, "run_manifest.json") if (item := _leveraged_input_from_manifest(path))]
    shadow_manifests = sorted(p for p in artifact_root.rglob("*_shadow_review_manifest.json"))
    shadows = [item for path in shadow_manifests if (item := _shadow_input_from_manifest(path))]
    decays = [item for path in _discover_manifests(artifact_root, "live_decay_monitor_manifest.json") if (item := _live_decay_input_from_manifest(path))]
    discovered: list[dict[str, Any]] = []
    for promotion in [*promotions, *crash_brake_reviews]:
        candidate_runs = set(promotion["candidate_runs"])
        shadow_candidates = [
            item for item in shadows if item["csv_path"].stem.startswith("russell_top50_leader_rotation_shadow_review_rows")
        ]
        shadow = next((item for item in shadow_candidates if _russell_shadow_matches_candidates(item, candidate_runs)), None)
        if shadow is None:
            shadow = next(iter(shadow_candidates), None)
        decay = next(
            (
                item
                for item in decays
                if item.get("input_format") == "russell_daily" and candidate_runs.intersection(item.get("strategies", ()))
            ),
            None,
        )
        discovered.append(
            {
                "group_type": "russell",
                "promotion": promotion,
                "shadow": shadow,
                "live_decay": decay,
            }
        )
    covered_global_etf_roots = {bundle["ranking_path"].parent.resolve() for bundle in global_etf_bundles}
    for global_etf in global_etf_bundles:
        ranking = read_table(global_etf["ranking_path"])
        candidate_ids = set(global_etf.get("candidate_ids") or ()) or {
            str(value).strip()
            for value in pd.DataFrame(ranking).get("Candidate", pd.Series(dtype=str)).dropna().astype(str)
            if str(value).strip()
        }
        shadow = next(
            (
                item
                for item in shadows
                if item["csv_path"].stem.startswith("global_etf_rotation_shadow_review_rows")
            ),
            None,
        )
        decay = next(
            (
                item
                for item in decays
                if item.get("input_format") == "wide" and candidate_ids.intersection(item.get("strategies", ()))
            ),
            None,
        )
        discovered.append(
            {
                "group_type": "global_etf",
                "global_etf": global_etf,
                "shadow": shadow,
                "live_decay": decay,
            }
        )
    for global_etf in global_etf_runs:
        if global_etf["ranking_path"].parent.resolve() in covered_global_etf_roots:
            continue
        ranking = read_table(global_etf["ranking_path"])
        candidate_ids = {
            str(value).strip()
            for value in pd.DataFrame(ranking).get("Candidate", pd.Series(dtype=str)).dropna().astype(str)
            if str(value).strip()
        }
        shadow = next(
            (
                item
                for item in shadows
                if item["csv_path"].stem.startswith("global_etf_rotation_shadow_review_rows")
            ),
            None,
        )
        decay = next(
            (
                item
                for item in decays
                if item.get("input_format") == "wide" and candidate_ids.intersection(item.get("strategies", ()))
            ),
            None,
        )
        discovered.append(
            {
                "group_type": "global_etf",
                "global_etf": global_etf,
                "shadow": shadow,
                "live_decay": decay,
            }
        )
    for snapshot in snapshot_runs:
        ranking = _read_optional_table(snapshot["ranking_path"])
        if ranking is None:
            ranking = pd.DataFrame()
        candidate_ids = {
            str(value).strip()
            for value in pd.DataFrame(ranking).get("Candidate", pd.Series(dtype=str)).dropna().astype(str)
            if str(value).strip()
        }
        shadow = next(
            (
                item
                for item in shadows
                if item["csv_path"].stem.startswith("snapshot_us_equity_shadow_review_rows")
                and _shadow_matches_candidates(item, candidate_ids)
            ),
            None,
        )
        decay = next(
            (
                item
                for item in decays
                if item.get("input_format") == "wide" and candidate_ids.intersection(item.get("strategies", ()))
            ),
            None,
        )
        discovered.append(
            {
                "group_type": "snapshot",
                "snapshot": snapshot,
                "shadow": shadow,
                "live_decay": decay,
            }
        )
    for leveraged in leveraged_runs:
        discovered.append(
            {
                "group_type": "leveraged",
                "leveraged": leveraged,
                "shadow": None,
                "live_decay": None,
            }
        )
    return discovered


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_live_replacement_review_from_inputs(group: dict[str, Any], *, output_root: Path) -> Path:
    group_type = str(group.get("group_type") or "russell")
    shadow = group.get("shadow")
    live_decay = group.get("live_decay")
    if group_type == "global_etf":
        global_etf = group["global_etf"]
        review = build_live_replacement_review(
            global_etf_ranking=_read_optional_table(global_etf["ranking_path"]),
            global_etf_live_readiness=_read_optional_table(global_etf["live_readiness_path"]),
            global_etf_walk_forward_summary=_read_optional_table(global_etf["walk_forward_path"]),
            global_etf_shadow_review=_read_optional_table(shadow["csv_path"]) if shadow else None,
            global_etf_live_decay_summary=_read_optional_table(live_decay["summary_path"]) if live_decay else None,
        )
        candidate_ids = tuple(str(value) for value in global_etf.get("candidate_ids") or ())
        if candidate_ids:
            review = review.loc[review["candidate"].astype(str).isin(candidate_ids)].reset_index(drop=True)
        manifest_inputs = {
            "global_etf_ranking": str(global_etf["ranking_path"]),
            "global_etf_live_readiness": str(global_etf["live_readiness_path"]),
            "global_etf_walk_forward": str(global_etf["walk_forward_path"]),
            "global_etf_shadow_review": str(shadow["csv_path"]) if shadow else "",
            "global_etf_live_decay": str(live_decay["summary_path"]) if live_decay else "",
            "experiment_profile": str(global_etf.get("experiment_profile", "") or ""),
        }
        output_name = global_etf["manifest_path"].parent.name
    elif group_type == "snapshot":
        snapshot = group["snapshot"]
        review = build_live_replacement_review(
            snapshot_ranking=_read_optional_table(snapshot["ranking_path"]),
            snapshot_walk_forward_summary=_read_optional_table(snapshot.get("walk_forward_path")),
            snapshot_shadow_review=_read_optional_table(shadow["csv_path"]) if shadow else None,
            snapshot_live_decay_summary=_read_optional_table(live_decay["summary_path"]) if live_decay else None,
        )
        manifest_inputs = {
            "snapshot_ranking": str(snapshot["ranking_path"]),
            "snapshot_candidate_daily_returns": str(snapshot["candidate_returns_path"]) if snapshot.get("candidate_returns_path") else "",
            "snapshot_candidate_expected_excess_cagr": str(snapshot["expected_excess_path"]) if snapshot.get("expected_excess_path") else "",
            "snapshot_walk_forward": str(snapshot["walk_forward_path"]) if snapshot.get("walk_forward_path") else "",
            "snapshot_shadow_review": str(shadow["csv_path"]) if shadow else "",
            "snapshot_live_decay": str(live_decay["summary_path"]) if live_decay else "",
        }
    elif group_type == "leveraged":
        leveraged = group["leveraged"]
        review = build_live_replacement_review(
            leveraged_ranking=_read_optional_table(leveraged["ranking_path"]),
        )
        manifest_inputs = {
            "leveraged_ranking": str(leveraged["ranking_path"]),
        }
    else:
        promotion = group["promotion"]
        review = build_live_replacement_review(
            russell_promotion_review=_read_optional_table(promotion["review_path"]),
            russell_shadow_review=_read_optional_table(shadow["csv_path"]) if shadow else None,
            russell_live_decay_summary=_read_optional_table(live_decay["summary_path"]) if live_decay else None,
        )
        manifest_inputs = {
            "russell_promotion_review": str(promotion["review_path"]),
            "russell_shadow_review": str(shadow["csv_path"]) if shadow else "",
            "russell_live_decay": str(live_decay["summary_path"]) if live_decay else "",
        }
    output_name = _output_dir_name(group)
    output_dir = output_root / f"live_replacement_review_{_safe_name(output_name)}"
    output_dir.mkdir(parents=True, exist_ok=True)
    review.to_csv(output_dir / "live_replacement_review.csv", index=False)
    (output_dir / "live_replacement_review.md").write_text(_render_markdown(review), encoding="utf-8")
    _write_json(
        output_dir / "live_replacement_manifest.json",
        {
            "manifest_type": "live_replacement_review",
            "artifact_schema_version": "live_replacement_review.v1",
            "inputs": manifest_inputs,
            "row_count": int(len(review)),
            "replace_live_now_count": int(review["replace_live_now"].sum()) if not review.empty else 0,
            "artifacts": {
                "live_replacement_review": {"path": "live_replacement_review.csv"},
                "live_replacement_markdown": {"path": "live_replacement_review.md"},
            },
            "outputs": [
                "live_replacement_review.csv",
                "live_replacement_review.md",
                "live_replacement_manifest.json",
            ],
        },
    )
    return output_dir


def main() -> int:
    args = parse_args()
    artifact_root = Path(args.artifact_root)
    output_root = Path(args.output_root) if args.output_root else artifact_root
    outputs: list[Path] = []
    for group in discover_replacement_review_inputs(artifact_root):
        outputs.append(build_live_replacement_review_from_inputs(group, output_root=output_root))
    print(f"live_replacement_review_count={len(outputs)}")
    for output_dir in outputs:
        print(f"live_replacement_review_dir={output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
