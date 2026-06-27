from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any

import pandas as pd

from .russell_1000_multi_factor_defensive_snapshot import read_table

REVIEW_COLUMNS = (
    "strategy_line",
    "candidate",
    "display_name",
    "candidate_group",
    "source_artifact",
    "research_gate_passed",
    "baseline_gate_passed",
    "walk_forward_gate_passed",
    "required_gates_passed",
    "shadow_review_present",
    "shadow_review_passed",
    "live_decay_present",
    "live_decay_passed",
    "statistical_support_level",
    "current_recommendation",
    "next_action",
    "replace_live_now",
    "replace_live_now_reason",
    "blocking_reason",
)


def _bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def _parse_selected_candidate_counts(value: Any) -> set[str]:
    if value is None or pd.isna(value):
        return set()
    if isinstance(value, dict):
        return {str(key) for key in value.keys()}
    text = str(value).strip()
    if not text:
        return set()
    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return {str(key) for key in parsed.keys()}
    return set()


def _parse_candidate_set(value: Any) -> set[str]:
    if value is None or pd.isna(value):
        return set()
    text = str(value).strip()
    if not text:
        return set()
    return {item.strip() for item in text.split(",") if item.strip()}


def _normalize_candidate_text(value: Any) -> str:
    return _text(value).strip()


def _build_russell_shadow_review_map(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        active_variant = _normalize_candidate_text(row.get("active_variant"))
        shadow_variant = _normalize_candidate_text(row.get("shadow_variant"))
        info = {
            "shadow_review_present": True,
            "shadow_review_passed": bool(active_variant or shadow_variant),
            "turnover_delta_vs_active": row.get("turnover_delta_vs_active"),
            "review_note": _text(row.get("review_note")),
        }
        for candidate in {active_variant, shadow_variant}:
            if candidate:
                mapping[candidate] = info
    return mapping


def _build_live_decay_map(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty or "strategy" not in pd.DataFrame(frame).columns:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        candidate = _normalize_candidate_text(row.get("strategy"))
        if not candidate:
            continue
        state = _text(row.get("overall_decay_state")).lower()
        mapping[candidate] = {
            "live_decay_present": True,
            "live_decay_passed": state == "keep",
            "overall_decay_state": _text(row.get("overall_decay_state")),
            "overall_reason": _text(row.get("overall_reason")),
            "recommended_action": _text(row.get("recommended_action")),
        }
    return mapping


def _shadow_review_passed_from_row(row: dict[str, Any]) -> bool:
    if "shadow_review_passed" in row and not pd.isna(row.get("shadow_review_passed")):
        return _bool(row.get("shadow_review_passed"), default=False)
    for key in ("shadow_decision", "review_decision", "review_status", "shadow_status"):
        value = _text(row.get(key)).strip().lower()
        if not value:
            continue
        if value in {"pass", "passed", "approved", "ok", "ready"}:
            return True
        if value in {"fail", "failed", "blocked", "reject", "rejected", "watch", "review"}:
            return False
    return False


def _build_global_shadow_review_map(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        candidate = _normalize_candidate_text(row.get("candidate")) or _normalize_candidate_text(row.get("Candidate"))
        if not candidate:
            continue
        mapping[candidate] = {
            "shadow_review_present": True,
            "shadow_review_passed": _shadow_review_passed_from_row(row),
            "review_note": _text(row.get("review_note")),
        }
    return mapping


def _normalize_russell_review(
    frame: pd.DataFrame | None,
    shadow_review: pd.DataFrame | None = None,
    live_decay_summary: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    shadow_map = _build_russell_shadow_review_map(shadow_review)
    live_decay_map = _build_live_decay_map(live_decay_summary)
    rows: list[dict[str, Any]] = []
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        candidate = _text(row.get("Run"))
        required_gates_passed = _bool(row.get("required_gates_passed"), default=False)
        recommendation = _text(row.get("promotion_decision")) or "research_only"
        next_action = _text(row.get("recommended_action")) or "continue_research"
        shadow = shadow_map.get(candidate, {})
        live_decay = live_decay_map.get(candidate, {})
        shadow_review_present = _bool(shadow.get("shadow_review_present"), default=False)
        shadow_review_passed = _bool(shadow.get("shadow_review_passed"), default=False)
        live_decay_present = _bool(live_decay.get("live_decay_present"), default=False)
        live_decay_passed = _bool(live_decay.get("live_decay_passed"), default=False)
        replace_live_now = bool(required_gates_passed and shadow_review_passed and live_decay_passed)
        replace_live_now_reason = (
            "all_review_evidence_present_and_passed" if replace_live_now else "shadow_live_evidence_required_before_live_change"
        )
        blocking_parts: list[str] = []
        if not required_gates_passed:
            blocking_parts.append(_text(row.get("required_gate_reason")) or "required_gates_failed")
        if not shadow_review_present:
            blocking_parts.append("missing_shadow_review_artifact")
        elif not shadow_review_passed:
            blocking_parts.append("shadow_review_not_passed")
        if not live_decay_present:
            blocking_parts.append("missing_live_decay_artifact")
        elif not live_decay_passed:
            decay_state = _text(live_decay.get("overall_decay_state")) or "unknown"
            blocking_parts.append(f"live_decay_state_{decay_state.lower()}")
        if required_gates_passed and not shadow_review_present:
            next_action = "collect_shadow_review_evidence"
        elif required_gates_passed and shadow_review_present and not live_decay_present:
            next_action = "collect_live_decay_evidence"
        elif required_gates_passed and shadow_review_present and live_decay_present and not live_decay_passed:
            next_action = _text(live_decay.get("recommended_action")) or "keep_shadow_monitoring"
        elif replace_live_now:
            next_action = "ready_for_live_config_change"
        rows.append(
            {
                "strategy_line": "russell_top50_leader_rotation",
                "candidate": candidate,
                "display_name": candidate,
                "candidate_group": _text(row.get("Candidate Role")) or _text(row.get("Gate Profile")),
                "source_artifact": "russell_promotion_review",
                "research_gate_passed": True,
                "baseline_gate_passed": _bool(row.get("live_gate_passed"), default=False),
                "walk_forward_gate_passed": _bool(row.get("overfit_gate_passed"), default=False),
                "required_gates_passed": required_gates_passed,
                "shadow_review_present": shadow_review_present,
                "shadow_review_passed": shadow_review_passed,
                "live_decay_present": live_decay_present,
                "live_decay_passed": live_decay_passed,
                "statistical_support_level": _text(row.get("statistical_support_level")),
                "current_recommendation": recommendation,
                "next_action": next_action,
                "replace_live_now": replace_live_now,
                "replace_live_now_reason": replace_live_now_reason,
                "blocking_reason": ";".join(part for part in blocking_parts if part),
            }
        )
    return rows


def _build_global_walk_forward_map(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        candidate_set = _parse_candidate_set(row.get("Candidate Set"))
        selected_candidates = _parse_selected_candidate_counts(row.get("Selected Candidate Counts"))
        gate_reason = _text(row.get("walk_forward_gate_reason"))
        for candidate in candidate_set:
            mapping[candidate] = {
                "walk_forward_gate_passed": False,
                "walk_forward_gate_reason": "not_selected_in_walk_forward",
            }
        info = {"walk_forward_gate_passed": _bool(row.get("walk_forward_gate_passed"), default=False), "walk_forward_gate_reason": gate_reason}
        for candidate in selected_candidates:
            mapping[candidate] = info
    return mapping


def _build_snapshot_walk_forward_map(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        candidate = (
            _normalize_candidate_text(row.get("Candidate"))
            or _normalize_candidate_text(row.get("candidate"))
            or _normalize_candidate_text(row.get("strategy"))
        )
        if not candidate:
            continue
        mapping[candidate] = {
            "walk_forward_gate_passed": _bool(row.get("walk_forward_gate_passed"), default=False),
            "walk_forward_gate_reason": _text(row.get("walk_forward_gate_reason")),
        }
    return mapping


def _normalize_global_etf_review(
    ranking: pd.DataFrame | None,
    live_readiness: pd.DataFrame | None,
    walk_forward_summary: pd.DataFrame | None,
    shadow_review: pd.DataFrame | None = None,
    live_decay_summary: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if ranking is None or ranking.empty:
        return []
    ranking_frame = pd.DataFrame(ranking)
    live_map = {}
    if live_readiness is not None and not live_readiness.empty and "Candidate" in live_readiness.columns:
        live_map = pd.DataFrame(live_readiness).set_index("Candidate").to_dict(orient="index")
    walk_forward_map = _build_global_walk_forward_map(walk_forward_summary)
    shadow_map = _build_global_shadow_review_map(shadow_review)
    live_decay_map = _build_live_decay_map(live_decay_summary)
    rows: list[dict[str, Any]] = []
    for row in ranking_frame.to_dict(orient="records"):
        candidate = _text(row.get("Candidate"))
        live = live_map.get(candidate, {})
        walk = walk_forward_map.get(candidate, {})
        shadow = shadow_map.get(candidate, {})
        live_decay = live_decay_map.get(candidate, {})
        baseline_gate_passed = _bool(live.get("live_gate_passed"), default=False)
        walk_forward_gate_passed = _bool(walk.get("walk_forward_gate_passed"), default=False)
        required_gates_passed = baseline_gate_passed and walk_forward_gate_passed
        shadow_review_present = _bool(shadow.get("shadow_review_present"), default=False)
        shadow_review_passed = _bool(shadow.get("shadow_review_passed"), default=False)
        live_decay_present = _bool(live_decay.get("live_decay_present"), default=False)
        live_decay_passed = _bool(live_decay.get("live_decay_passed"), default=False)
        replace_live_now = bool(required_gates_passed and shadow_review_passed and live_decay_passed)
        recommendation = _text(live.get("live_action")) or _text(row.get("review_action")) or "keep_current_live"
        next_action = recommendation
        blocking_parts = []
        if baseline_gate_passed is False and live:
            blocking_parts.append(_text(live.get("live_gate_reason")) or "live_gate_failed")
        if walk and walk_forward_gate_passed is False:
            blocking_parts.append(_text(walk.get("walk_forward_gate_reason")) or "walk_forward_gate_failed")
        elif baseline_gate_passed and not walk:
            blocking_parts.append("missing_walk_forward_summary")
        if baseline_gate_passed and walk and not walk_forward_gate_passed:
            recommendation = "blocked_by_walk_forward_oos"
            next_action = "keep_current_live"
        elif baseline_gate_passed and not walk:
            next_action = "collect_walk_forward_evidence"
        elif required_gates_passed and not shadow_review_present:
            blocking_parts.append("missing_shadow_review_artifact")
            next_action = "collect_shadow_review_evidence"
        elif required_gates_passed and shadow_review_present and not shadow_review_passed:
            blocking_parts.append("shadow_review_not_passed")
            next_action = "keep_shadow_monitoring"
        elif required_gates_passed and shadow_review_present and not live_decay_present:
            blocking_parts.append("missing_live_decay_artifact")
            next_action = "collect_live_decay_evidence"
        elif required_gates_passed and shadow_review_present and live_decay_present and not live_decay_passed:
            decay_state = _text(live_decay.get("overall_decay_state")) or "unknown"
            blocking_parts.append(f"live_decay_state_{decay_state.lower()}")
            next_action = _text(live_decay.get("recommended_action")) or "keep_shadow_monitoring"
        elif replace_live_now:
            next_action = "ready_for_live_config_change"
        rows.append(
            {
                "strategy_line": "global_etf_rotation",
                "candidate": candidate,
                "display_name": _text(row.get("Display Name")) or candidate,
                "candidate_group": _text(row.get("Candidate Group")),
                "source_artifact": "global_etf_live_readiness",
                "research_gate_passed": _bool(row.get("research_gate_passed"), default=False),
                "baseline_gate_passed": baseline_gate_passed,
                "walk_forward_gate_passed": walk_forward_gate_passed if walk else False,
                "required_gates_passed": required_gates_passed,
                "shadow_review_present": shadow_review_present,
                "shadow_review_passed": shadow_review_passed,
                "live_decay_present": live_decay_present,
                "live_decay_passed": live_decay_passed,
                "statistical_support_level": "",
                "current_recommendation": recommendation,
                "next_action": next_action,
                "replace_live_now": replace_live_now,
                "replace_live_now_reason": (
                    "all_review_evidence_present_and_passed"
                    if replace_live_now
                    else "shadow_live_evidence_required_before_live_change"
                ),
                "blocking_reason": ";".join(part for part in blocking_parts if part),
            }
        )
    return rows


def _normalize_leveraged_review(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        recommendation = _text(row.get("review_action")) or "reject"
        baseline_gate_passed = _bool(row.get("replacement_candidate"), default=False)
        required_gates_passed = baseline_gate_passed
        is_live_proxy = _text(row.get("Candidate Group")) == "current_live_proxy"
        gate_reason = _text(row.get("gate_reason"))
        if required_gates_passed:
            next_action = "collect_shadow_review_evidence"
            blocking_reason = "missing_shadow_review_artifact;missing_live_decay_artifact"
        else:
            next_action = recommendation
            if is_live_proxy and gate_reason == "pass":
                blocking_reason = ""
            elif gate_reason and gate_reason != "pass":
                blocking_reason = gate_reason
            else:
                blocking_reason = "replacement_gate_not_passed"
        rows.append(
            {
                "strategy_line": "leveraged_us_equity",
                "candidate": _text(row.get("Candidate")),
                "display_name": _text(row.get("Display Name")) or _text(row.get("Candidate")),
                "candidate_group": _text(row.get("Candidate Group")),
                "source_artifact": "leveraged_strategy_candidates",
                "research_gate_passed": _bool(row.get("research_gate_passed"), default=False),
                "baseline_gate_passed": baseline_gate_passed,
                "walk_forward_gate_passed": False,
                "required_gates_passed": required_gates_passed,
                "statistical_support_level": "",
                "current_recommendation": recommendation,
                "next_action": next_action,
                "replace_live_now": False,
                "replace_live_now_reason": (
                    "shadow_live_evidence_required_before_live_change"
                    if required_gates_passed
                    else "replacement_gate_not_passed"
                ),
                "blocking_reason": blocking_reason,
            }
        )
    return rows


def _normalize_snapshot_review(
    frame: pd.DataFrame | None,
    walk_forward_summary: pd.DataFrame | None = None,
    shadow_review: pd.DataFrame | None = None,
    live_decay_summary: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    walk_forward_map = _build_snapshot_walk_forward_map(walk_forward_summary)
    shadow_map = _build_global_shadow_review_map(shadow_review)
    live_decay_map = _build_live_decay_map(live_decay_summary)
    rows: list[dict[str, Any]] = []
    for row in pd.DataFrame(frame).to_dict(orient="records"):
        candidate = _text(row.get("Candidate"))
        recommendation = _text(row.get("review_action")) or "reject"
        baseline_gate_passed = _bool(row.get("replacement_review_candidate"), default=False)
        gate_reason = _text(row.get("gate_reason"))
        walk = walk_forward_map.get(candidate, {})
        walk_forward_gate_passed = _bool(walk.get("walk_forward_gate_passed"), default=False)
        required_gates_passed = baseline_gate_passed and walk_forward_gate_passed
        shadow = shadow_map.get(candidate, {})
        live_decay = live_decay_map.get(candidate, {})
        shadow_review_present = _bool(shadow.get("shadow_review_present"), default=False)
        shadow_review_passed = _bool(shadow.get("shadow_review_passed"), default=False)
        live_decay_present = _bool(live_decay.get("live_decay_present"), default=False)
        live_decay_passed = _bool(live_decay.get("live_decay_passed"), default=False)
        replace_live_now = bool(required_gates_passed and shadow_review_passed and live_decay_passed)
        blocking_parts: list[str] = []
        if baseline_gate_passed and walk and not walk_forward_gate_passed:
            blocking_parts.append(_text(walk.get("walk_forward_gate_reason")) or "walk_forward_gate_failed")
            recommendation = "blocked_by_walk_forward_oos"
            next_action = "keep_current_live"
        elif baseline_gate_passed and not walk:
            blocking_parts.append("missing_walk_forward_summary")
            next_action = "collect_walk_forward_evidence"
        elif required_gates_passed and not shadow_review_present:
            blocking_parts.append("missing_shadow_review_artifact")
            if not live_decay_present:
                blocking_parts.append("missing_live_decay_artifact")
            next_action = "collect_shadow_review_evidence"
        elif required_gates_passed and shadow_review_present and not shadow_review_passed:
            blocking_parts.append("shadow_review_not_passed")
            next_action = "keep_shadow_monitoring"
        elif required_gates_passed and shadow_review_present and not live_decay_present:
            blocking_parts.append("missing_live_decay_artifact")
            next_action = "collect_live_decay_evidence"
        elif required_gates_passed and shadow_review_present and live_decay_present and not live_decay_passed:
            decay_state = _text(live_decay.get("overall_decay_state")) or "unknown"
            blocking_parts.append(f"live_decay_state_{decay_state.lower()}")
            next_action = _text(live_decay.get("recommended_action")) or "keep_shadow_monitoring"
        elif required_gates_passed and replace_live_now:
            next_action = "ready_for_live_config_change"
        elif gate_reason and gate_reason != "pass":
            blocking_parts.append(gate_reason)
            next_action = recommendation
        else:
            blocking_parts.append("replacement_gate_not_passed")
            next_action = recommendation
        rows.append(
            {
                "strategy_line": "snapshot_us_equity",
                "candidate": candidate,
                "display_name": _text(row.get("Display Name")) or candidate,
                "candidate_group": _text(row.get("Candidate Group")),
                "source_artifact": "us_equity_strategy_candidates",
                "research_gate_passed": _bool(row.get("live_gate_passed"), default=False),
                "baseline_gate_passed": baseline_gate_passed,
                "walk_forward_gate_passed": walk_forward_gate_passed if walk else False,
                "required_gates_passed": required_gates_passed,
                "shadow_review_present": shadow_review_present,
                "shadow_review_passed": shadow_review_passed,
                "live_decay_present": live_decay_present,
                "live_decay_passed": live_decay_passed,
                "statistical_support_level": "",
                "current_recommendation": recommendation,
                "next_action": next_action,
                "replace_live_now": replace_live_now,
                "replace_live_now_reason": (
                    "all_review_evidence_present_and_passed"
                    if replace_live_now
                    else (
                        "shadow_live_evidence_required_before_live_change"
                        if required_gates_passed
                        else "replacement_gate_not_passed"
                    )
                ),
                "blocking_reason": ";".join(part for part in blocking_parts if part),
            }
        )
    return rows


def build_live_replacement_review(
    *,
    russell_promotion_review: pd.DataFrame | None = None,
    russell_shadow_review: pd.DataFrame | None = None,
    russell_live_decay_summary: pd.DataFrame | None = None,
    global_etf_ranking: pd.DataFrame | None = None,
    global_etf_live_readiness: pd.DataFrame | None = None,
    global_etf_walk_forward_summary: pd.DataFrame | None = None,
    global_etf_shadow_review: pd.DataFrame | None = None,
    global_etf_live_decay_summary: pd.DataFrame | None = None,
    leveraged_ranking: pd.DataFrame | None = None,
    snapshot_ranking: pd.DataFrame | None = None,
    snapshot_walk_forward_summary: pd.DataFrame | None = None,
    snapshot_shadow_review: pd.DataFrame | None = None,
    snapshot_live_decay_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows = [
        *_normalize_russell_review(russell_promotion_review, russell_shadow_review, russell_live_decay_summary),
        *(
            _normalize_global_etf_review(
                global_etf_ranking,
                global_etf_live_readiness,
                global_etf_walk_forward_summary,
                global_etf_shadow_review,
                global_etf_live_decay_summary,
            )
        ),
        *_normalize_leveraged_review(leveraged_ranking),
        *_normalize_snapshot_review(
            snapshot_ranking,
            snapshot_walk_forward_summary,
            snapshot_shadow_review,
            snapshot_live_decay_summary,
        ),
    ]
    if not rows:
        return pd.DataFrame(columns=REVIEW_COLUMNS)
    review = pd.DataFrame(rows)
    decision_order = {
        "preferred_aggressive_live_design_review": 0,
        "promote_aggressive_live_design_review": 1,
        "promote_conservative_live_design_review": 2,
        "candidate_for_live_promotion_review": 3,
        "ready_for_live_config_change": 4,
        "replacement_review_candidate": 5,
        "supplemental_review_candidate": 6,
        "keep_current_live": 7,
        "no_replacement": 8,
        "current_live_baseline": 9,
        "blocked_by_walk_forward_oos": 10,
        "collect_shadow_review_evidence": 11,
        "collect_live_decay_evidence": 12,
        "monitor_next_cycle": 13,
        "human_review_keep_runtime_unchanged": 14,
        "keep_shadow_monitoring": 15,
        "reject": 16,
        "research_only": 17,
        "continue_research": 18,
    }
    review = review.assign(
        _replace_order=review["replace_live_now"].map(lambda flag: 0 if bool(flag) else 1),
        _action_order=review["next_action"].map(decision_order).fillna(99),
    )
    review = review.sort_values(
        ["_replace_order", "_action_order", "strategy_line", "candidate"],
        kind="stable",
    ).drop(columns=["_replace_order", "_action_order"])
    for column in REVIEW_COLUMNS:
        if column not in review.columns:
            review[column] = "" if column.endswith("reason") or column.endswith("action") else False
    return review.loc[:, REVIEW_COLUMNS].reset_index(drop=True)


def _render_markdown(review: pd.DataFrame) -> str:
    lines = [
        "# Live Replacement Review",
        "",
        "This is a research aggregation artifact only. It does not change live manifests, broker settings, or runtime defaults.",
        "",
        f"- Candidates reviewed: {len(review)}",
        f"- Replace-live-now candidates: {int(review['replace_live_now'].sum()) if not review.empty else 0}",
        "",
        "| Strategy Line | Candidate | Recommendation | Next Action | Replace Live Now | Blocking Reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in review.to_dict(orient="records"):
        lines.append(
            "| {strategy_line} | {candidate} | {current_recommendation} | {next_action} | {replace_live_now} | {blocking_reason} |".format(
                strategy_line=_text(row.get("strategy_line")),
                candidate=_text(row.get("candidate")),
                current_recommendation=_text(row.get("current_recommendation")),
                next_action=_text(row.get("next_action")),
                replace_live_now="yes" if _bool(row.get("replace_live_now"), default=False) else "no",
                blocking_reason=_text(row.get("blocking_reason")),
            )
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aggregate strategy candidate and promotion artifacts into one live replacement review.")
    parser.add_argument("--russell-promotion-review", help="Input Russell live_promotion_review.csv")
    parser.add_argument("--russell-shadow-review", help="Input Russell shadow review rows CSV")
    parser.add_argument("--russell-live-decay", help="Input Russell live_decay_strategy_summary.csv")
    parser.add_argument("--global-etf-ranking", help="Input Global ETF ranking.csv")
    parser.add_argument("--global-etf-live-readiness", help="Input Global ETF live_readiness_summary.csv")
    parser.add_argument("--global-etf-walk-forward", help="Input Global ETF walk_forward_selection_summary.csv")
    parser.add_argument("--global-etf-shadow-review", help="Input Global ETF shadow review rows CSV")
    parser.add_argument("--global-etf-live-decay", help="Input Global ETF live_decay_strategy_summary.csv")
    parser.add_argument("--leveraged-ranking", help="Input leveraged strategy ranking.csv")
    parser.add_argument("--snapshot-ranking", help="Input US equity strategy ranking.csv")
    parser.add_argument("--snapshot-walk-forward", help="Input snapshot walk-forward summary CSV")
    parser.add_argument("--snapshot-shadow-review", help="Input snapshot shadow review rows CSV")
    parser.add_argument("--snapshot-live-decay", help="Input snapshot live_decay_strategy_summary.csv")
    parser.add_argument("--output-dir", default="data/output/live_replacement_review")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    review = build_live_replacement_review(
        russell_promotion_review=read_table(args.russell_promotion_review) if args.russell_promotion_review else None,
        russell_shadow_review=read_table(args.russell_shadow_review) if args.russell_shadow_review else None,
        russell_live_decay_summary=read_table(args.russell_live_decay) if args.russell_live_decay else None,
        global_etf_ranking=read_table(args.global_etf_ranking) if args.global_etf_ranking else None,
        global_etf_live_readiness=read_table(args.global_etf_live_readiness) if args.global_etf_live_readiness else None,
        global_etf_walk_forward_summary=read_table(args.global_etf_walk_forward) if args.global_etf_walk_forward else None,
        global_etf_shadow_review=read_table(args.global_etf_shadow_review) if args.global_etf_shadow_review else None,
        global_etf_live_decay_summary=read_table(args.global_etf_live_decay) if args.global_etf_live_decay else None,
        leveraged_ranking=read_table(args.leveraged_ranking) if args.leveraged_ranking else None,
        snapshot_ranking=read_table(args.snapshot_ranking) if args.snapshot_ranking else None,
        snapshot_walk_forward_summary=read_table(args.snapshot_walk_forward) if args.snapshot_walk_forward else None,
        snapshot_shadow_review=read_table(args.snapshot_shadow_review) if args.snapshot_shadow_review else None,
        snapshot_live_decay_summary=read_table(args.snapshot_live_decay) if args.snapshot_live_decay else None,
    )

    review.to_csv(output_dir / "live_replacement_review.csv", index=False)
    (output_dir / "live_replacement_review.md").write_text(_render_markdown(review), encoding="utf-8")
    (output_dir / "live_replacement_manifest.json").write_text(
        json.dumps(
            {
                "manifest_type": "live_replacement_review",
                "artifact_schema_version": "live_replacement_review.v1",
                "row_count": int(len(review)),
                "replace_live_now_count": int(review["replace_live_now"].sum()) if not review.empty else 0,
                "inputs": {
                    "russell_promotion_review": args.russell_promotion_review,
                    "russell_shadow_review": args.russell_shadow_review,
                    "russell_live_decay": args.russell_live_decay,
                    "global_etf_ranking": args.global_etf_ranking,
                    "global_etf_live_readiness": args.global_etf_live_readiness,
                    "global_etf_walk_forward": args.global_etf_walk_forward,
                    "global_etf_shadow_review": args.global_etf_shadow_review,
                    "global_etf_live_decay": args.global_etf_live_decay,
                    "leveraged_ranking": args.leveraged_ranking,
                    "snapshot_ranking": args.snapshot_ranking,
                    "snapshot_walk_forward": args.snapshot_walk_forward,
                    "snapshot_shadow_review": args.snapshot_shadow_review,
                    "snapshot_live_decay": args.snapshot_live_decay,
                },
                "outputs": [
                    "live_replacement_review.csv",
                    "live_replacement_review.md",
                    "live_replacement_manifest.json",
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(review.to_string(index=False))
    print(f"wrote live replacement review -> {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
