from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .strategy_plugin_runner import IBIT_SMART_DCA_STRATEGY, PLUGIN_IBIT_ZSCORE_EXIT
from .strategy_plugin_runner import IBIT_ZSCORE_EXIT_POLICY as DEFAULT_IBIT_POLICY

PLUGIN_PROMOTION_REVIEW_SCHEMA_VERSION = "strategy_plugin_promotion_review.v1"
PLUGIN_PROMOTION_REVIEW_MANIFEST_TYPE = "strategy_plugin_promotion_review"

PLUGIN_PROMOTION_REVIEW_COLUMNS = (
    "strategy",
    "plugin",
    "display_name",
    "plugin_role",
    "source_artifact",
    "review_status",
    "policy_evidence_status",
    "notification_allowed",
    "position_control_allowed",
    "coverage_gate_passed",
    "research_gate_passed",
    "required_gates_passed",
    "shadow_review_present",
    "shadow_review_passed",
    "live_decay_present",
    "live_decay_passed",
    "replace_live_component_now",
    "replace_live_component_now_reason",
    "blocking_reason",
    "recommended_action",
)


def _load_json_object(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON object expected: {path}")
    return dict(payload)


def _safe_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _plugin_role(*, notification_allowed: bool, position_control_allowed: bool) -> str:
    if position_control_allowed:
        return "position_control_candidate"
    if notification_allowed:
        return "notification_only"
    return "disabled"


def _render_markdown(review: pd.DataFrame) -> str:
    if review.empty:
        return "# Plugin Promotion Review\n\nNo rows.\n"
    lines = [
        "# Plugin Promotion Review",
        "",
        "| Strategy | Plugin | Role | Required gates | Replace live component now | Blocking reason | Recommended action |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in review.to_dict(orient="records"):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("strategy", "") or ""),
                    str(row.get("plugin", "") or ""),
                    str(row.get("plugin_role", "") or ""),
                    str(bool(row.get("required_gates_passed", False))).lower(),
                    str(bool(row.get("replace_live_component_now", False))).lower(),
                    str(row.get("blocking_reason", "") or ""),
                    str(row.get("recommended_action", "") or ""),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def build_plugin_promotion_review_from_ibit_research_manifest(
    manifest_path: str | Path,
    *,
    policy: Any = DEFAULT_IBIT_POLICY,
) -> pd.DataFrame:
    payload = _load_json_object(manifest_path)
    manifest_type = str(payload.get("manifest_type", "") or "")
    if manifest_type != "ibit_smart_dca_research":
        raise ValueError(f"unsupported manifest_type for plugin promotion review: {manifest_type or 'missing'}")

    review_summary = _safe_mapping(payload.get("review_summary"))
    policy_payload = policy
    notification_allowed = bool(getattr(policy_payload, "notification_allowed", False))
    position_control_allowed = bool(getattr(policy_payload, "position_control_allowed", False))
    policy_evidence_status = str(getattr(policy_payload, "evidence_status", "") or "")
    required_gates_passed = (
        str(review_summary.get("plugin_gate", "") or "") == "pass"
        and str(review_summary.get("zscore_coverage_gate", "") or "") == "pass"
    )
    coverage_gate_passed = str(review_summary.get("zscore_coverage_gate", "") or "") == "pass"
    research_gate_passed = str(review_summary.get("plugin_gate", "") or "") == "pass"

    blocking_parts = _string_list(review_summary.get("promotion_blockers"))
    replace_live_component_now = False
    replace_live_component_now_reason = ""
    recommended_action = "continue_research"
    if not required_gates_passed:
        replace_live_component_now_reason = "research_gate_or_coverage_gate_failed"
        recommended_action = "continue_research"
    elif not position_control_allowed:
        blocking_parts.append("policy_still_notification_only")
        replace_live_component_now_reason = "policy_still_notification_only"
        recommended_action = "prepare_separate_promotion_artifact"
    else:
        blocking_parts.append("missing_shadow_review_artifact")
        blocking_parts.append("missing_live_decay_artifact")
        replace_live_component_now_reason = "shadow_live_evidence_required_before_live_change"
        recommended_action = "collect_shadow_review_evidence"

    row = {
        "strategy": IBIT_SMART_DCA_STRATEGY,
        "plugin": PLUGIN_IBIT_ZSCORE_EXIT,
        "display_name": PLUGIN_IBIT_ZSCORE_EXIT,
        "plugin_role": _plugin_role(
            notification_allowed=notification_allowed,
            position_control_allowed=position_control_allowed,
        ),
        "source_artifact": "ibit_smart_dca_research",
        "review_status": str(review_summary.get("review_status", "") or ""),
        "policy_evidence_status": policy_evidence_status,
        "notification_allowed": notification_allowed,
        "position_control_allowed": position_control_allowed,
        "coverage_gate_passed": coverage_gate_passed,
        "research_gate_passed": research_gate_passed,
        "required_gates_passed": required_gates_passed,
        "shadow_review_present": False,
        "shadow_review_passed": False,
        "live_decay_present": False,
        "live_decay_passed": False,
        "replace_live_component_now": replace_live_component_now,
        "replace_live_component_now_reason": replace_live_component_now_reason,
        "blocking_reason": ";".join(dict.fromkeys(part for part in blocking_parts if part)),
        "recommended_action": recommended_action,
    }
    return pd.DataFrame([row], columns=list(PLUGIN_PROMOTION_REVIEW_COLUMNS))


def write_plugin_promotion_review_artifacts(
    *,
    source_manifest_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Path]:
    source_manifest = Path(source_manifest_path)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    review = build_plugin_promotion_review_from_ibit_research_manifest(source_manifest)
    review_csv = output_root / "plugin_promotion_review.csv"
    review_md = output_root / "plugin_promotion_review.md"
    manifest_path = output_root / "plugin_promotion_review_manifest.json"
    review.to_csv(review_csv, index=False)
    review_md.write_text(_render_markdown(review), encoding="utf-8")

    first = review.iloc[0].to_dict()
    manifest = {
        "manifest_type": PLUGIN_PROMOTION_REVIEW_MANIFEST_TYPE,
        "artifact_schema_version": PLUGIN_PROMOTION_REVIEW_SCHEMA_VERSION,
        "strategy": str(first.get("strategy", "") or ""),
        "plugin": str(first.get("plugin", "") or ""),
        "plugin_role": str(first.get("plugin_role", "") or ""),
        "policy_evidence_status": str(first.get("policy_evidence_status", "") or ""),
        "row_count": int(len(review)),
        "replace_live_component_now_count": int(review["replace_live_component_now"].sum()) if not review.empty else 0,
        "inputs": {
            "source_research_manifest": {"path": str(source_manifest)},
        },
        "artifacts": {
            "plugin_promotion_review_csv": {"path": str(review_csv)},
            "plugin_promotion_review_md": {"path": str(review_md)},
        },
        "review_rows": review.to_dict(orient="records"),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "review_csv": review_csv,
        "review_md": review_md,
        "manifest": manifest_path,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build plugin promotion review artifacts from an IBIT research manifest.")
    parser.add_argument("--ibit-dca-research-manifest", required=True)
    parser.add_argument("--output-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    outputs = write_plugin_promotion_review_artifacts(
        source_manifest_path=args.ibit_dca_research_manifest,
        output_dir=args.output_dir,
    )
    print(f"plugin_promotion_review_csv={outputs['review_csv']}")
    print(f"plugin_promotion_review_manifest={outputs['manifest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
