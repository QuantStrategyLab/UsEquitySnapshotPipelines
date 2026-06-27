from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .contracts import GLOBAL_ETF_ROTATION_PROFILE
from .global_etf_offensive_rotation_research import (
    DEFAULT_LIVE_BASELINE_CANDIDATE,
    GLOBAL_ETF_LIVEABLE_COMPOSITES,
    GLOBAL_ETF_OFFENSIVE_VARIANTS,
)
from .global_etf_rotation_shadow_review import (
    RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION,
    SHADOW_REVIEW_ROW_FIELDS,
)
from .russell_1000_multi_factor_defensive_snapshot import read_table


@dataclass(frozen=True)
class ShadowReviewInputOutputs:
    json_path: Path
    markdown_path: Path


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))


def _parse_selected_candidate_counts(value: Any) -> dict[str, int]:
    if value is None or pd.isna(value):
        return {}
    if isinstance(value, dict):
        return {str(key): int(raw) for key, raw in value.items()}
    text = str(value).strip()
    if not text:
        return {}
    for parser in (json.loads,):
        try:
            parsed = parser(text)
        except Exception:
            continue
        if isinstance(parsed, dict):
            counts: dict[str, int] = {}
            for key, raw in parsed.items():
                try:
                    counts[str(key)] = int(raw)
                except Exception:
                    counts[str(key)] = 0
            return counts
    return {}


def _infer_snapshot_as_of(rebalance_events: pd.DataFrame | None) -> str:
    if rebalance_events is None or rebalance_events.empty:
        return ""
    frame = pd.DataFrame(rebalance_events).copy()
    for column in ("next_date", "as_of"):
        if column in frame.columns:
            dates = pd.to_datetime(frame[column], errors="coerce").dropna()
            if not dates.empty:
                return pd.Timestamp(dates.max()).date().isoformat()
    return ""


def _render_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "# Global ETF Shadow Review Input",
        "",
        "| Candidate | Decision | Overlay Weight | Turnover Delta vs Active | Selected Count | Review Note |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        decision = "approved" if bool(row.get("shadow_review_passed")) else "pending_or_blocked"
        turnover_delta = row.get("turnover_delta_vs_active")
        if pd.isna(turnover_delta):
            turnover_text = ""
        else:
            turnover_text = f"{float(turnover_delta):.4f}"
        lines.append(
            "| {candidate} | {decision} | {overlay_weight} | {turnover_delta} | {selected_count} | {review_note} |".format(
                candidate=str(row.get("candidate", "")).replace("|", "\\|"),
                decision=decision,
                overlay_weight=f"{float(row.get('offensive_weight', 0.0)):.2%}",
                turnover_delta=turnover_text,
                selected_count=int(row.get("selected_count", 0) or 0),
                review_note=str(row.get("review_note", "")).replace("|", "\\|"),
            )
        )
    return "\n".join(lines) + "\n"


def build_shadow_review_input_payload(
    *,
    ranking: pd.DataFrame,
    live_readiness: pd.DataFrame,
    walk_forward_summary: pd.DataFrame,
    rebalance_events: pd.DataFrame | None = None,
    approved_candidates: tuple[str, ...] = (),
    blocked_candidates: tuple[str, ...] = (),
    snapshot_as_of: str = "",
    profile: str = GLOBAL_ETF_ROTATION_PROFILE,
) -> dict[str, object]:
    approved = set(approved_candidates)
    blocked = set(blocked_candidates)
    overlap = approved.intersection(blocked)
    if overlap:
        raise ValueError(f"candidates cannot be both approved and blocked: {sorted(overlap)}")

    ranking_frame = pd.DataFrame(ranking).copy()
    live_frame = pd.DataFrame(live_readiness).copy()
    walk_frame = pd.DataFrame(walk_forward_summary).copy()
    rebalance_frame = pd.DataFrame(rebalance_events).copy() if rebalance_events is not None else pd.DataFrame()

    if ranking_frame.empty or live_frame.empty:
        raise ValueError("ranking and live_readiness inputs are required")

    live_map = (
        live_frame.set_index("Candidate").to_dict(orient="index")
        if "Candidate" in live_frame.columns
        else {}
    )
    walk_counts: dict[str, int] = {}
    if not walk_frame.empty and "Selected Candidate Counts" in walk_frame.columns:
        for row in walk_frame.to_dict(orient="records"):
            counts = _parse_selected_candidate_counts(row.get("Selected Candidate Counts"))
            for candidate, count in counts.items():
                walk_counts[candidate] = max(int(count), walk_counts.get(candidate, 0))

    composite_by_id = {spec.candidate_id: spec for spec in GLOBAL_ETF_LIVEABLE_COMPOSITES}
    variant_by_id = {spec.candidate_id: spec for spec in GLOBAL_ETF_OFFENSIVE_VARIANTS}
    baseline_turnover = float(
        pd.to_numeric(
            ranking_frame.loc[ranking_frame["Candidate"].eq(DEFAULT_LIVE_BASELINE_CANDIDATE), "median_turnover_per_year"],
            errors="coerce",
        ).dropna().iloc[0]
    ) if not ranking_frame.loc[ranking_frame["Candidate"].eq(DEFAULT_LIVE_BASELINE_CANDIDATE)].empty else float("nan")

    rows: list[dict[str, object]] = []
    liveable = ranking_frame.loc[ranking_frame["Candidate Group"].eq("liveable_candidate")].copy()
    for row in liveable.to_dict(orient="records"):
        candidate = str(row.get("Candidate", "")).strip()
        if not candidate or candidate not in composite_by_id:
            continue
        spec = composite_by_id[candidate]
        overlay_variant = variant_by_id.get(spec.overlay_candidate_id)
        latest_overlay_weight = float(spec.overlay_weight)
        if not rebalance_frame.empty and {"candidate_id", "overlay_weight"}.issubset(rebalance_frame.columns):
            candidate_events = rebalance_frame.loc[rebalance_frame["candidate_id"].astype(str).eq(candidate)].copy()
            if not candidate_events.empty:
                sort_column = "next_date" if "next_date" in candidate_events.columns else "as_of"
                if sort_column in candidate_events.columns:
                    candidate_events[sort_column] = pd.to_datetime(candidate_events[sort_column], errors="coerce")
                    candidate_events = candidate_events.sort_values(sort_column, kind="stable")
                latest_value = pd.to_numeric(candidate_events["overlay_weight"], errors="coerce").dropna()
                if not latest_value.empty:
                    latest_overlay_weight = float(latest_value.iloc[-1])

        selected_count = 0
        if latest_overlay_weight > 0 and overlay_variant is not None:
            selected_count = int(getattr(overlay_variant, "top_n", 0) or 0)

        candidate_turnover = pd.to_numeric(pd.Series([row.get("median_turnover_per_year")]), errors="coerce").iloc[0]
        turnover_delta = float(candidate_turnover - baseline_turnover) if not pd.isna(candidate_turnover) and not pd.isna(baseline_turnover) else float("nan")

        live_gate_passed = bool(live_map.get(candidate, {}).get("live_gate_passed", False))
        walk_forward_gate_passed = bool(walk_counts.get(candidate, 0) > 0)
        if candidate in blocked:
            shadow_review_passed = False
            decision = "blocked"
        elif candidate in approved:
            shadow_review_passed = True
            decision = "approved"
        else:
            shadow_review_passed = False
            decision = "pending"
        review_note = (
            f"decision={decision} active={spec.base_candidate_id} shadow={candidate} "
            f"overlay_weight={latest_overlay_weight:.2%} turnover_delta={turnover_delta:.4f} "
            f"selected_count={selected_count} walk_forward_windows={walk_counts.get(candidate, 0)} "
            f"live_gate_passed={'yes' if live_gate_passed else 'no'} "
            f"walk_forward_candidate={'yes' if walk_forward_gate_passed else 'no'}"
        )
        rows.append(
            {
                "schema_version": RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION,
                "candidate": candidate,
                "active_candidate": spec.base_candidate_id,
                "shadow_candidate": candidate,
                "selected_count": int(selected_count),
                "offensive_weight": float(latest_overlay_weight),
                "safe_haven_weight": float(max(0.0, 1.0 - latest_overlay_weight)),
                "turnover_delta_vs_active": turnover_delta,
                "shadow_review_passed": bool(shadow_review_passed),
                "review_note": review_note,
            }
        )

    snapshot_value = str(snapshot_as_of or "").strip() or _infer_snapshot_as_of(rebalance_frame)
    return {
        "strategy_profile": str(profile or GLOBAL_ETF_ROTATION_PROFILE),
        "snapshot_as_of": snapshot_value,
        "inputs": {
            "ranking": "ranking.csv",
            "live_readiness": "live_readiness_summary.csv",
            "walk_forward_summary": "walk_forward_selection_summary.csv",
            "rebalance_events": "rebalance_events.csv" if rebalance_events is not None else "",
            "approved_candidates": list(approved_candidates),
            "blocked_candidates": list(blocked_candidates),
        },
        "diagnostics": {
            "global_etf_shadow_review_schema_version": RUNTIME_SHADOW_REVIEW_SCHEMA_VERSION,
            "global_etf_shadow_review_row_fields": list(SHADOW_REVIEW_ROW_FIELDS),
            "global_etf_shadow_review_rows": rows,
        },
    }


def build_shadow_review_input_artifacts(
    *,
    ranking_path: str | Path,
    live_readiness_path: str | Path,
    walk_forward_path: str | Path,
    output_dir: str | Path,
    rebalance_events_path: str | Path | None = None,
    approved_candidates: tuple[str, ...] = (),
    blocked_candidates: tuple[str, ...] = (),
    snapshot_as_of: str = "",
    profile: str = GLOBAL_ETF_ROTATION_PROFILE,
) -> ShadowReviewInputOutputs:
    ranking = read_table(ranking_path)
    live_readiness = read_table(live_readiness_path)
    walk_forward = read_table(walk_forward_path)
    rebalance_events = read_table(rebalance_events_path) if rebalance_events_path else None
    payload = build_shadow_review_input_payload(
        ranking=ranking,
        live_readiness=live_readiness,
        walk_forward_summary=walk_forward,
        rebalance_events=rebalance_events,
        approved_candidates=approved_candidates,
        blocked_candidates=blocked_candidates,
        snapshot_as_of=snapshot_as_of,
        profile=profile,
    )
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "global_etf_shadow_review_input.json"
    markdown_path = root / "global_etf_shadow_review_input.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rows = list((payload.get("diagnostics") or {}).get("global_etf_shadow_review_rows") or [])
    markdown_path.write_text(_render_markdown(rows), encoding="utf-8")
    return ShadowReviewInputOutputs(json_path=json_path, markdown_path=markdown_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build Global ETF operator-review diagnostics JSON for shadow review artifacts."
    )
    parser.add_argument("--ranking", required=True, help="Global ETF ranking.csv")
    parser.add_argument("--live-readiness", required=True, help="Global ETF live_readiness_summary.csv")
    parser.add_argument("--walk-forward", required=True, help="Global ETF walk_forward_selection_summary.csv")
    parser.add_argument("--rebalance-events", help="Optional Global ETF rebalance_events.csv")
    parser.add_argument("--approved-candidates", default="", help="Comma-separated approved candidate ids")
    parser.add_argument("--blocked-candidates", default="", help="Comma-separated blocked candidate ids")
    parser.add_argument("--snapshot-as-of", default="")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--profile", default=GLOBAL_ETF_ROTATION_PROFILE)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    outputs = build_shadow_review_input_artifacts(
        ranking_path=args.ranking,
        live_readiness_path=args.live_readiness,
        walk_forward_path=args.walk_forward,
        rebalance_events_path=args.rebalance_events,
        approved_candidates=_split_csv(args.approved_candidates),
        blocked_candidates=_split_csv(args.blocked_candidates),
        snapshot_as_of=args.snapshot_as_of,
        output_dir=args.output_dir,
        profile=args.profile,
    )
    print(f"shadow_review_input_json={outputs.json_path}")
    print(f"shadow_review_input_markdown={outputs.markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
