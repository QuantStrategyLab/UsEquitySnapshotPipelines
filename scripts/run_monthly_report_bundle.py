#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from us_equity_snapshot_pipelines.contracts import list_profile_contracts, list_scheduled_profile_contracts  # noqa: E402


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
    parser.add_argument(
        "--promotion-bundle-manifest",
        action="append",
        default=None,
        help=(
            "Optional Russell promotion_bundle_manifest.json path to include in the monthly review bundle. "
            "Can be supplied multiple times. When omitted, artifact-root is scanned."
        ),
    )
    parser.add_argument(
        "--shadow-live-ledger-manifest",
        action="append",
        default=None,
        help=(
            "Optional Russell shadow_live_ledger_manifest.json path to include in the monthly review bundle. "
            "Can be supplied multiple times. When omitted, artifact-root is scanned."
        ),
    )
    parser.add_argument(
        "--capacity-stress-manifest",
        action="append",
        default=None,
        help=(
            "Optional Russell capacity_stress_manifest.json path to include in the monthly review bundle. "
            "Can be supplied multiple times. When omitted, artifact-root is scanned."
        ),
    )
    parser.add_argument(
        "--live-decay-monitor-manifest",
        action="append",
        default=None,
        help=(
            "Optional live_decay_monitor_manifest.json path to include in the monthly review bundle. "
            "Can be supplied multiple times. When omitted, artifact-root is scanned."
        ),
    )
    parser.add_argument(
        "--ibit-dca-research-manifest",
        action="append",
        default=None,
        help=(
            "Optional ibit_dca_research_manifest.json path to include in the monthly review bundle. "
            "Can be supplied multiple times. When omitted, artifact-root is scanned."
        ),
    )
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


def _discover_promotion_bundle_manifests(artifact_root: Path, explicit_paths: list[str] | None) -> list[Path]:
    if explicit_paths:
        return [Path(path) for path in explicit_paths if str(path).strip()]
    return sorted(artifact_root.rglob("promotion_bundle_manifest.json"))


def _discover_shadow_live_ledger_manifests(artifact_root: Path, explicit_paths: list[str] | None) -> list[Path]:
    if explicit_paths:
        return [Path(path) for path in explicit_paths if str(path).strip()]
    return sorted(artifact_root.rglob("shadow_live_ledger_manifest.json"))


def _discover_capacity_stress_manifests(artifact_root: Path, explicit_paths: list[str] | None) -> list[Path]:
    if explicit_paths:
        return [Path(path) for path in explicit_paths if str(path).strip()]
    return sorted(artifact_root.rglob("capacity_stress_manifest.json"))


def _discover_live_decay_monitor_manifests(artifact_root: Path, explicit_paths: list[str] | None) -> list[Path]:
    if explicit_paths:
        return [Path(path) for path in explicit_paths if str(path).strip()]
    return sorted(artifact_root.rglob("live_decay_monitor_manifest.json"))


def _discover_ibit_dca_research_manifests(artifact_root: Path, explicit_paths: list[str] | None) -> list[Path]:
    if explicit_paths:
        return [Path(path) for path in explicit_paths if str(path).strip()]
    return sorted(artifact_root.rglob("ibit_dca_research_manifest.json"))


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


def _discover_strategy_health_reports(artifact_root: Path) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for summary_path in sorted(artifact_root.rglob("strategy_health_summary.csv")):
        rows: list[dict[str, str]] = []
        with summary_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append(
                    {
                        "strategy": row.get("strategy", ""),
                        "overall_health_state": row.get("overall_health_state", ""),
                        "overall_reason": row.get("overall_reason", ""),
                        "full_window_excess_cagr": row.get("full_window_excess_cagr", ""),
                        "full_window_drawdown_advantage": row.get("full_window_drawdown_advantage", ""),
                        "watch_windows": row.get("watch_windows", ""),
                    }
                )
        report_dir = summary_path.parent
        manifest_path = report_dir / "run_manifest.json"
        manifest: dict[str, Any] = {}
        if manifest_path.exists():
            try:
                manifest = load_json(manifest_path)
            except Exception:
                manifest = {}
        reports.append(
            {
                "artifact_dir": str(report_dir),
                "summary_path": str(summary_path),
                "report_path": str(report_dir / "strategy_health_report.md")
                if (report_dir / "strategy_health_report.md").exists()
                else "",
                "primary_benchmark": str(manifest.get("primary_benchmark", "") or ""),
                "policy": manifest.get("policy") if isinstance(manifest.get("policy"), dict) else {},
                "strategy_count": len(rows),
                "review_for_retirement_count": sum(
                    1 for row in rows if row["overall_health_state"] == "review_for_retirement"
                ),
                "watch_count": sum(1 for row in rows if row["overall_health_state"] == "watch"),
                "strategies": rows,
            }
        )
    return reports


def _discover_strategy_health_errors(artifact_root: Path) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for error_path in sorted(artifact_root.rglob("strategy_health_error.json")):
        try:
            payload = load_json(error_path)
        except Exception:
            payload = {}
        error_dir = error_path.parent
        errors.append(
            {
                "artifact_dir": str(error_dir),
                "error_path": str(error_path),
                "report_path": str(error_dir / "strategy_health_error.md")
                if (error_dir / "strategy_health_error.md").exists()
                else "",
                "source_returns": str(payload.get("source_returns", "") or ""),
                "error_type": str(payload.get("error_type", "") or ""),
                "error_message": str(payload.get("error_message", "") or ""),
            }
        )
    return errors


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item).strip()]
    return []


def _safe_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _promotion_review_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "run": str(item.get("run", "") or ""),
                "required_gates_passed": bool(item.get("required_gates_passed", False)),
                "statistical_support_level": str(item.get("statistical_support_level", "") or ""),
                "promotion_decision": str(item.get("promotion_decision", "") or ""),
                "recommended_action": str(item.get("recommended_action", "") or ""),
            }
        )
    return rows


def _collect_promotion_bundle_manifest(path: Path) -> dict[str, Any]:
    base: dict[str, Any] = {
        "manifest_path": str(path),
        "status": "missing",
        "status_reason": "manifest file not found",
        "manifest_type": "",
        "artifact_schema_version": "",
        "generated_at": "",
        "candidate_runs": [],
        "portfolio_nav": None,
        "bootstrap": {},
        "dsr_pbo": {},
        "input_count": 0,
        "artifact_count": 0,
        "review_rows": [],
    }
    if not path.exists():
        return base
    try:
        payload = load_json(path)
    except Exception as exc:
        return {
            **base,
            "status": "invalid",
            "status_reason": f"failed to parse JSON: {exc.__class__.__name__}",
        }
    if not isinstance(payload, Mapping):
        return {**base, "status": "invalid", "status_reason": "manifest root is not a JSON object"}

    manifest_type = str(payload.get("manifest_type", "") or "")
    status = "ok" if manifest_type == "russell_top50_promotion_bundle" else "invalid"
    status_reason = "" if status == "ok" else f"unsupported manifest_type: {manifest_type or 'missing'}"
    inputs = _safe_mapping(payload.get("inputs"))
    artifacts = _safe_mapping(payload.get("artifacts"))
    return {
        **base,
        "status": status,
        "status_reason": status_reason,
        "manifest_type": manifest_type,
        "artifact_schema_version": str(payload.get("artifact_schema_version", "") or ""),
        "generated_at": str(payload.get("generated_at", "") or ""),
        "candidate_runs": _string_list(payload.get("candidate_runs")),
        "portfolio_nav": payload.get("portfolio_nav"),
        "bootstrap": dict(_safe_mapping(payload.get("bootstrap"))),
        "dsr_pbo": dict(_safe_mapping(payload.get("dsr_pbo"))),
        "input_count": len(inputs),
        "artifact_count": len(artifacts),
        "review_rows": _promotion_review_rows(payload.get("review_rows")),
    }


def _collect_shadow_live_ledger_manifest(path: Path) -> dict[str, Any]:
    base: dict[str, Any] = {
        "manifest_path": str(path),
        "status": "missing",
        "status_reason": "manifest file not found",
        "manifest_type": "",
        "artifact_schema_version": "",
        "generated_at": "",
        "portfolio_nav": None,
        "slippage_bps": None,
        "forward_window_days": None,
        "safe_haven": "",
        "row_counts": {},
        "artifact_count": 0,
    }
    if not path.exists():
        return base
    try:
        payload = load_json(path)
    except Exception as exc:
        return {
            **base,
            "status": "invalid",
            "status_reason": f"failed to parse JSON: {exc.__class__.__name__}",
        }
    if not isinstance(payload, Mapping):
        return {**base, "status": "invalid", "status_reason": "manifest root is not a JSON object"}

    manifest_type = str(payload.get("manifest_type", "") or "")
    status = "ok" if manifest_type == "russell_top50_shadow_live_ledger" else "invalid"
    status_reason = "" if status == "ok" else f"unsupported manifest_type: {manifest_type or 'missing'}"
    artifacts = _safe_mapping(payload.get("artifacts"))
    return {
        **base,
        "status": status,
        "status_reason": status_reason,
        "manifest_type": manifest_type,
        "artifact_schema_version": str(payload.get("artifact_schema_version", "") or ""),
        "generated_at": str(payload.get("generated_at", "") or ""),
        "portfolio_nav": payload.get("portfolio_nav"),
        "slippage_bps": payload.get("slippage_bps"),
        "forward_window_days": payload.get("forward_window_days"),
        "safe_haven": str(payload.get("safe_haven", "") or ""),
        "row_counts": dict(_safe_mapping(payload.get("row_counts"))),
        "artifact_count": len(artifacts),
    }


def _collect_capacity_stress_manifest(path: Path) -> dict[str, Any]:
    base: dict[str, Any] = {
        "manifest_path": str(path),
        "status": "missing",
        "status_reason": "manifest file not found",
        "manifest_type": "",
        "artifact_schema_version": "",
        "generated_at": "",
        "portfolio_nav_values": [],
        "slippage_bps_values": [],
        "split_trade_days_values": [],
        "min_median_net_excess_vs_qqq": None,
        "row_counts": {},
        "artifact_count": 0,
    }
    if not path.exists():
        return base
    try:
        payload = load_json(path)
    except Exception as exc:
        return {
            **base,
            "status": "invalid",
            "status_reason": f"failed to parse JSON: {exc.__class__.__name__}",
        }
    if not isinstance(payload, Mapping):
        return {**base, "status": "invalid", "status_reason": "manifest root is not a JSON object"}

    manifest_type = str(payload.get("manifest_type", "") or "")
    status = "ok" if manifest_type == "russell_top50_capacity_stress" else "invalid"
    status_reason = "" if status == "ok" else f"unsupported manifest_type: {manifest_type or 'missing'}"
    artifacts = _safe_mapping(payload.get("artifacts"))
    return {
        **base,
        "status": status,
        "status_reason": status_reason,
        "manifest_type": manifest_type,
        "artifact_schema_version": str(payload.get("artifact_schema_version", "") or ""),
        "generated_at": str(payload.get("generated_at", "") or ""),
        "portfolio_nav_values": _string_list(payload.get("portfolio_nav_values")),
        "slippage_bps_values": _string_list(payload.get("slippage_bps_values")),
        "split_trade_days_values": _string_list(payload.get("split_trade_days_values")),
        "min_median_net_excess_vs_qqq": payload.get("min_median_net_excess_vs_qqq"),
        "row_counts": dict(_safe_mapping(payload.get("row_counts"))),
        "artifact_count": len(artifacts),
    }


def _collect_live_decay_monitor_manifest(path: Path) -> dict[str, Any]:
    base: dict[str, Any] = {
        "manifest_path": str(path),
        "status": "missing",
        "status_reason": "manifest file not found",
        "manifest_type": "",
        "artifact_schema_version": "",
        "generated_at": "",
        "input_format": "",
        "strategies": [],
        "primary_benchmark": "",
        "secondary_benchmark": "",
        "windows": [],
        "policy": {},
        "expected_excess_cagr_by_strategy": {},
        "row_counts": {},
        "artifact_count": 0,
    }
    if not path.exists():
        return base
    try:
        payload = load_json(path)
    except Exception as exc:
        return {
            **base,
            "status": "invalid",
            "status_reason": f"failed to parse JSON: {exc.__class__.__name__}",
        }
    if not isinstance(payload, Mapping):
        return {**base, "status": "invalid", "status_reason": "manifest root is not a JSON object"}

    manifest_type = str(payload.get("manifest_type", "") or "")
    status = "ok" if manifest_type == "live_decay_monitor" else "invalid"
    status_reason = "" if status == "ok" else f"unsupported manifest_type: {manifest_type or 'missing'}"
    artifacts = _safe_mapping(payload.get("artifacts"))
    return {
        **base,
        "status": status,
        "status_reason": status_reason,
        "manifest_type": manifest_type,
        "artifact_schema_version": str(payload.get("artifact_schema_version", "") or ""),
        "generated_at": str(payload.get("generated_at", "") or ""),
        "input_format": str(payload.get("input_format", "") or ""),
        "strategies": _string_list(payload.get("strategies")),
        "primary_benchmark": str(payload.get("primary_benchmark", "") or ""),
        "secondary_benchmark": str(payload.get("secondary_benchmark", "") or ""),
        "windows": _string_list(payload.get("windows")),
        "policy": dict(_safe_mapping(payload.get("policy"))),
        "expected_excess_cagr_by_strategy": dict(_safe_mapping(payload.get("expected_excess_cagr_by_strategy"))),
        "row_counts": dict(_safe_mapping(payload.get("row_counts"))),
        "artifact_count": len(artifacts),
    }


def _collect_ibit_dca_research_manifest(path: Path) -> dict[str, Any]:
    base: dict[str, Any] = {
        "manifest_path": str(path),
        "status": "missing",
        "status_reason": "manifest file not found",
        "manifest_type": "",
        "artifact_schema_version": "",
        "ibit_symbol": "",
        "parking_symbol": "",
        "price_field": "",
        "primary_benchmark": "",
        "secondary_benchmark": "",
        "btc_proxy_symbol": "",
        "proxy_rows_filled": 0,
        "proxy_scale_source": "",
        "parking_proxy_symbol": "",
        "parking_proxy_rows_filled": 0,
        "parking_proxy_scale_source": "",
        "variants": [],
        "row_counts": {},
        "artifact_count": 0,
        "research_report_path": "",
        "research_report_present": False,
        "review_status": "",
        "promotion_blockers": [],
        "plugin_gate": "",
        "plugin_reason": "",
        "plugin_signal_count": 0,
        "plugin_available_signal_count": 0,
        "plugin_route_counts": {},
        "plugin_signal_data_status_counts": {},
        "plugin_unavailable_signal_count": 0,
        "plugin_non_normal_signal_count": 0,
        "zscore_coverage_gate": "",
        "zscore_available_signal_ratio": 0.0,
        "zscore_min_available_signal_ratio": 0.0,
        "zscore_history_rows": 0,
        "zscore_history_start": "",
        "zscore_history_end": "",
    }
    if not path.exists():
        return base
    try:
        payload = load_json(path)
    except Exception as exc:
        return {
            **base,
            "status": "invalid",
            "status_reason": f"failed to parse JSON: {exc.__class__.__name__}",
        }
    if not isinstance(payload, Mapping):
        return {**base, "status": "invalid", "status_reason": "manifest root is not a JSON object"}

    manifest_type = str(payload.get("manifest_type", "") or "")
    status = "ok" if manifest_type == "ibit_smart_dca_research" else "invalid"
    status_reason = "" if status == "ok" else f"unsupported manifest_type: {manifest_type or 'missing'}"
    inputs = _safe_mapping(payload.get("inputs"))
    config = _safe_mapping(inputs.get("config"))
    proxy = _safe_mapping(inputs.get("proxy"))
    artifacts = _safe_mapping(payload.get("artifacts"))
    review_summary = _safe_mapping(payload.get("review_summary"))
    report_artifact = _safe_mapping(artifacts.get("ibit_dca_research_report"))
    report_path = str(report_artifact.get("path", "") or "")
    resolved_report_path = Path(report_path)
    if report_path and not resolved_report_path.is_absolute():
        resolved_report_path = path.parent / resolved_report_path
    return {
        **base,
        "status": status,
        "status_reason": status_reason,
        "manifest_type": manifest_type,
        "artifact_schema_version": str(payload.get("artifact_schema_version", "") or ""),
        "ibit_symbol": str(config.get("ibit_symbol", "") or ""),
        "parking_symbol": str(config.get("parking_symbol", "") or ""),
        "price_field": str(config.get("price_field", "") or ""),
        "primary_benchmark": str(config.get("primary_benchmark", "") or ""),
        "secondary_benchmark": str(config.get("secondary_benchmark", "") or ""),
        "btc_proxy_symbol": str(proxy.get("btc_proxy_symbol") or config.get("btc_proxy_symbol", "") or ""),
        "proxy_rows_filled": int(proxy.get("proxy_rows_filled", 0) or 0),
        "proxy_scale_source": str(proxy.get("proxy_scale_source", "") or ""),
        "parking_proxy_symbol": str(proxy.get("parking_proxy_symbol") or config.get("parking_proxy_symbol", "") or ""),
        "parking_proxy_rows_filled": int(proxy.get("parking_proxy_rows_filled", 0) or 0),
        "parking_proxy_scale_source": str(proxy.get("parking_proxy_scale_source", "") or ""),
        "variants": _string_list(inputs.get("variants")),
        "row_counts": dict(_safe_mapping(payload.get("row_counts"))),
        "artifact_count": len(artifacts),
        "research_report_path": report_path,
        "research_report_present": bool(report_path and resolved_report_path.exists()),
        "review_status": str(review_summary.get("review_status", "") or ""),
        "promotion_blockers": _string_list(review_summary.get("promotion_blockers")),
        "plugin_gate": str(review_summary.get("plugin_gate", "") or ""),
        "plugin_reason": str(review_summary.get("plugin_reason", "") or ""),
        "plugin_signal_count": int(review_summary.get("plugin_signal_count", 0) or 0),
        "plugin_available_signal_count": int(review_summary.get("plugin_available_signal_count", 0) or 0),
        "plugin_route_counts": dict(_safe_mapping(review_summary.get("plugin_route_counts"))),
        "plugin_signal_data_status_counts": dict(_safe_mapping(review_summary.get("plugin_signal_data_status_counts"))),
        "plugin_unavailable_signal_count": int(review_summary.get("plugin_unavailable_signal_count", 0) or 0),
        "plugin_non_normal_signal_count": int(review_summary.get("plugin_non_normal_signal_count", 0) or 0),
        "zscore_coverage_gate": str(review_summary.get("zscore_coverage_gate", "") or ""),
        "zscore_available_signal_ratio": float(review_summary.get("zscore_available_signal_ratio", 0.0) or 0.0),
        "zscore_min_available_signal_ratio": float(review_summary.get("zscore_min_available_signal_ratio", 0.0) or 0.0),
        "zscore_history_rows": int(review_summary.get("zscore_history_rows", 0) or 0),
        "zscore_history_start": str(review_summary.get("zscore_history_start", "") or ""),
        "zscore_history_end": str(review_summary.get("zscore_history_end", "") or ""),
    }


def _collect_profile(
    artifact_root: Path, profile: str, summary_path: Path | None, ranking_preview_size: int
) -> dict[str, Any]:
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
    promotion_bundle_manifest_paths: list[str] | None = None,
    shadow_live_ledger_manifest_paths: list[str] | None = None,
    capacity_stress_manifest_paths: list[str] | None = None,
    live_decay_monitor_manifest_paths: list[str] | None = None,
    ibit_dca_research_manifest_paths: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(artifact_root)
    discovered = _discover_release_summaries(root)
    strategy_health_reports = _discover_strategy_health_reports(root)
    strategy_health_errors = _discover_strategy_health_errors(root)
    promotion_bundles = [
        _collect_promotion_bundle_manifest(path)
        for path in _discover_promotion_bundle_manifests(root, promotion_bundle_manifest_paths)
    ]
    shadow_live_ledgers = [
        _collect_shadow_live_ledger_manifest(path)
        for path in _discover_shadow_live_ledger_manifests(root, shadow_live_ledger_manifest_paths)
    ]
    capacity_stresses = [
        _collect_capacity_stress_manifest(path)
        for path in _discover_capacity_stress_manifests(root, capacity_stress_manifest_paths)
    ]
    live_decay_monitors = [
        _collect_live_decay_monitor_manifest(path)
        for path in _discover_live_decay_monitor_manifests(root, live_decay_monitor_manifest_paths)
    ]
    ibit_dca_research_reports = [
        _collect_ibit_dca_research_manifest(path)
        for path in _discover_ibit_dca_research_manifests(root, ibit_dca_research_manifest_paths)
    ]
    promotion_bundle_problem_count = sum(1 for bundle in promotion_bundles if bundle["status"] != "ok")
    shadow_live_ledger_problem_count = sum(1 for ledger in shadow_live_ledgers if ledger["status"] != "ok")
    capacity_stress_problem_count = sum(1 for stress in capacity_stresses if stress["status"] != "ok")
    live_decay_monitor_problem_count = sum(1 for monitor in live_decay_monitors if monitor["status"] != "ok")
    ibit_dca_research_problem_count = sum(1 for report in ibit_dca_research_reports if report["status"] != "ok")
    profiles = [
        _collect_profile(root, contract.profile, discovered.get(contract.profile), ranking_preview_size)
        for contract in list_scheduled_profile_contracts()
    ]
    status = (
        "ok"
        if all(profile["status"] == "ready" and not profile["missing_files"] for profile in profiles)
        and not any(report["review_for_retirement_count"] for report in strategy_health_reports)
        and not strategy_health_errors
        and promotion_bundle_problem_count == 0
        and shadow_live_ledger_problem_count == 0
        and capacity_stress_problem_count == 0
        and live_decay_monitor_problem_count == 0
        and ibit_dca_research_problem_count == 0
        else "warning"
    )
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
        "non_ready_profile_count": sum(1 for profile in profiles if profile["status"] != "ready"),
        "profiles": profiles,
        "strategy_health_reports": strategy_health_reports,
        "strategy_health_error_count": len(strategy_health_errors),
        "strategy_health_errors": strategy_health_errors,
        "promotion_bundle_count": len(promotion_bundles),
        "promotion_bundle_problem_count": promotion_bundle_problem_count,
        "promotion_bundles": promotion_bundles,
        "shadow_live_ledger_count": len(shadow_live_ledgers),
        "shadow_live_ledger_problem_count": shadow_live_ledger_problem_count,
        "shadow_live_ledgers": shadow_live_ledgers,
        "capacity_stress_count": len(capacity_stresses),
        "capacity_stress_problem_count": capacity_stress_problem_count,
        "capacity_stresses": capacity_stresses,
        "live_decay_monitor_count": len(live_decay_monitors),
        "live_decay_monitor_problem_count": live_decay_monitor_problem_count,
        "live_decay_monitors": live_decay_monitors,
        "ibit_dca_research_count": len(ibit_dca_research_reports),
        "ibit_dca_research_problem_count": ibit_dca_research_problem_count,
        "ibit_dca_research_reports": ibit_dca_research_reports,
    }


def _md(value: Any) -> str:
    return str(value if value is not None else "").replace("\n", " ").replace("|", "\\|")


def _format_percent(value: Any) -> str:
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return "n/a"


def render_job_summary(bundle: dict[str, Any]) -> str:
    lines = [
        "# US Equity Monthly Review Bundle",
        "",
        f"- Report month: `{bundle['report_month']}`",
        f"- Status: `{bundle['status']}`",
        f"- Profiles: `{bundle['profile_count']}`",
        f"- Missing profiles: `{bundle['missing_profile_count']}`",
        f"- Non-ready profiles: `{bundle.get('non_ready_profile_count', 0)}`",
        f"- Strategy health reports: `{len(bundle.get('strategy_health_reports', []))}`",
        f"- Strategy health errors: `{bundle.get('strategy_health_error_count', 0)}`",
        f"- Promotion bundles: `{bundle.get('promotion_bundle_count', 0)}`",
        f"- Promotion bundle issues: `{bundle.get('promotion_bundle_problem_count', 0)}`",
        f"- Shadow-live ledgers: `{bundle.get('shadow_live_ledger_count', 0)}`",
        f"- Shadow-live ledger issues: `{bundle.get('shadow_live_ledger_problem_count', 0)}`",
        f"- Capacity stress reports: `{bundle.get('capacity_stress_count', 0)}`",
        f"- Capacity stress issues: `{bundle.get('capacity_stress_problem_count', 0)}`",
        f"- Live decay monitors: `{bundle.get('live_decay_monitor_count', 0)}`",
        f"- Live decay monitor issues: `{bundle.get('live_decay_monitor_problem_count', 0)}`",
        f"- IBIT DCA research reports: `{bundle.get('ibit_dca_research_count', 0)}`",
        f"- IBIT DCA research issues: `{bundle.get('ibit_dca_research_problem_count', 0)}`",
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
    health_reports = bundle.get("strategy_health_reports") or []
    if health_reports:
        lines.extend(
            [
                "",
                "## Strategy Health Reports",
                "",
                "| Artifact | Strategies | Watch | Review for retirement |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for report in health_reports:
            lines.append(
                f"| `{report['artifact_dir']}` | {report['strategy_count']} | "
                f"{report['watch_count']} | {report['review_for_retirement_count']} |"
            )
    health_errors = bundle.get("strategy_health_errors") or []
    if health_errors:
        lines.extend(
            [
                "",
                "## Strategy Health Errors",
                "",
                "| Artifact | Source returns | Error |",
                "| --- | --- | --- |",
            ]
        )
        for error in health_errors:
            lines.append(
                f"| `{error['artifact_dir']}` | `{error['source_returns'] or 'n/a'}` | "
                f"{error['error_type'] or 'Error'}: {error['error_message'] or 'n/a'} |"
            )
    if bundle.get("promotion_bundles"):
        lines.extend(
            [
                "",
                "## Promotion Research Bundles",
                "",
                "| Manifest | Status | Schema | Generated at | Candidates | Review rows |",
                "| --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for promotion_bundle in bundle["promotion_bundles"]:
            lines.append(
                f"| `{_md(promotion_bundle['manifest_path'])}` | `{_md(promotion_bundle['status'])}` | "
                f"`{_md(promotion_bundle['artifact_schema_version'])}` | "
                f"{_md(promotion_bundle['generated_at']) or 'n/a'} | "
                f"{len(promotion_bundle['candidate_runs'])} | {len(promotion_bundle['review_rows'])} |"
            )
    if bundle.get("shadow_live_ledgers"):
        lines.extend(
            [
                "",
                "## Shadow-live Ledgers",
                "",
                "| Manifest | Status | Schema | Generated at | Trade rows | Rebalance rows |",
                "| --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for ledger in bundle["shadow_live_ledgers"]:
            row_counts = ledger.get("row_counts") or {}
            lines.append(
                f"| `{_md(ledger['manifest_path'])}` | `{_md(ledger['status'])}` | "
                f"`{_md(ledger['artifact_schema_version'])}` | "
                f"{_md(ledger['generated_at']) or 'n/a'} | "
                f"{row_counts.get('shadow_live_trade_ledger', 0)} | "
                f"{row_counts.get('shadow_live_rebalance_summary', 0)} |"
            )
    if bundle.get("capacity_stresses"):
        lines.extend(
            [
                "",
                "## Capacity Stress Reports",
                "",
                "| Manifest | Status | Schema | Generated at | Detail rows | Summary rows |",
                "| --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for stress in bundle["capacity_stresses"]:
            row_counts = stress.get("row_counts") or {}
            lines.append(
                f"| `{_md(stress['manifest_path'])}` | `{_md(stress['status'])}` | "
                f"`{_md(stress['artifact_schema_version'])}` | "
                f"{_md(stress['generated_at']) or 'n/a'} | "
                f"{row_counts.get('capacity_stress_detail', 0)} | "
                f"{row_counts.get('capacity_stress_summary', 0)} |"
            )
    if bundle.get("live_decay_monitors"):
        lines.extend(
            [
                "",
                "## Live Decay Monitors",
                "",
                "| Manifest | Status | Schema | Generated at | Strategies | Windows |",
                "| --- | --- | --- | --- | ---: | --- |",
            ]
        )
        for monitor in bundle["live_decay_monitors"]:
            lines.append(
                f"| `{_md(monitor['manifest_path'])}` | `{_md(monitor['status'])}` | "
                f"`{_md(monitor['artifact_schema_version'])}` | "
                f"{_md(monitor['generated_at']) or 'n/a'} | "
                f"{len(monitor['strategies'])} | {', '.join(monitor['windows']) or 'n/a'} |"
            )
    if bundle.get("ibit_dca_research_reports"):
        lines.extend(
            [
                "",
                "## IBIT Smart DCA Research",
                "",
                "| Manifest | Status | Schema | Review | Plugin gate | Variants | Gate report | Trade rows | Signal rows |",
                "| --- | --- | --- | --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for report in bundle["ibit_dca_research_reports"]:
            row_counts = report.get("row_counts") or {}
            report_state = "present" if report.get("research_report_present") else "missing"
            if not report.get("research_report_path"):
                report_state = "n/a"
            lines.append(
                f"| `{_md(report['manifest_path'])}` | `{_md(report['status'])}` | "
                f"`{_md(report['artifact_schema_version'])}` | "
                f"`{_md(report.get('review_status') or 'n/a')}` | "
                f"`{_md(report.get('plugin_gate') or 'n/a')}` | "
                f"{', '.join(report['variants']) or 'n/a'} | "
                f"{report_state} | "
                f"{row_counts.get('ibit_dca_trade_ledger', 0)} | "
                f"{row_counts.get('ibit_dca_signal_consumption', 0)} |"
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
        "- Low-risk docs/tests/monthly-review reporting fixes may be automated; high-risk strategy, runtime, broker, dependency, secret, profile-contract, or live-allocation changes require human review.",
        "- Treat live strategy `review_for_retirement` states as evidence for a human follow-up issue, not as permission to delete or disable a strategy automatically.",
        "",
        "## Bundle Metadata",
        "",
        f"- Report month: `{bundle['report_month']}`",
        f"- Status: `{bundle['status']}`",
        f"- Source project: `{bundle['source_project']}`",
        f"- Artifact root: `{bundle['artifact_root']}`",
        f"- Missing profiles: `{bundle['missing_profile_count']}`",
        f"- Non-ready profiles: `{bundle.get('non_ready_profile_count', 0)}`",
        f"- Strategy health reports: `{len(bundle.get('strategy_health_reports', []))}`",
        f"- Strategy health errors: `{bundle.get('strategy_health_error_count', 0)}`",
        f"- Live decay monitors: `{bundle.get('live_decay_monitor_count', 0)}`",
        f"- Live decay monitor issues: `{bundle.get('live_decay_monitor_problem_count', 0)}`",
        f"- IBIT DCA research reports: `{bundle.get('ibit_dca_research_count', 0)}`",
        f"- IBIT DCA research issues: `{bundle.get('ibit_dca_research_problem_count', 0)}`",
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
    health_reports = bundle.get("strategy_health_reports") or []
    lines.extend(
        [
            "",
            "## Live Strategy Health Evidence",
            "",
        ]
    )
    if not health_reports:
        lines.append("_No live strategy health report artifacts were found in this bundle._")
    for report in health_reports:
        benchmark = report.get("primary_benchmark") or "n/a"
        lines.extend(
            [
                f"### Health report `{report['artifact_dir']}`",
                "",
                f"- Primary benchmark: `{benchmark}`",
                f"- Strategies: `{report['strategy_count']}`",
                f"- Watch count: `{report['watch_count']}`",
                f"- Review-for-retirement count: `{report['review_for_retirement_count']}`",
                "",
                "| Strategy | Health | Full excess CAGR | Full drawdown advantage | Watch windows | Reason |",
                "| --- | --- | ---: | ---: | --- | --- |",
            ]
        )
        for row in report["strategies"]:
            lines.append(
                f"| `{row['strategy']}` | `{row['overall_health_state']}` | "
                f"{row['full_window_excess_cagr'] or 'n/a'} | "
                f"{row['full_window_drawdown_advantage'] or 'n/a'} | "
                f"{row['watch_windows'] or 'none'} | {row['overall_reason'] or 'n/a'} |"
            )
    health_errors = bundle.get("strategy_health_errors") or []
    if health_errors:
        lines.extend(
            [
                "",
                "## Live Strategy Health Build Errors",
                "",
                "These errors are evidence gaps. They do not permit automated strategy removal or parameter changes.",
                "",
                "| Artifact | Source returns | Error |",
                "| --- | --- | --- |",
            ]
        )
        for error in health_errors:
            lines.append(
                f"| `{error['artifact_dir']}` | `{error['source_returns'] or 'n/a'}` | "
                f"{error['error_type'] or 'Error'}: {error['error_message'] or 'n/a'} |"
            )
    lines.extend(
        [
            "",
            "## Research Promotion Bundles",
            "",
            "These manifests are research-only evidence packs. They support monthly review and auditability, "
            "but they do not enable live runtime behavior by themselves.",
        ]
    )
    if not bundle.get("promotion_bundles"):
        lines.append("")
        lines.append("_No Russell promotion bundle manifest found under the artifact root._")
    for promotion_bundle in bundle.get("promotion_bundles", []):
        status_note = promotion_bundle.get("status_reason") or "n/a"
        lines.extend(
            [
                "",
                f"### Promotion bundle `{_md(promotion_bundle['manifest_path'])}`",
                "",
                f"- Status: `{_md(promotion_bundle['status'])}`",
                f"- Status reason: {_md(status_note)}",
                f"- Manifest type: `{_md(promotion_bundle['manifest_type'])}`",
                f"- Artifact schema: `{_md(promotion_bundle['artifact_schema_version'])}`",
                f"- Generated at: `{_md(promotion_bundle['generated_at']) or 'n/a'}`",
                f"- Candidate runs: `{', '.join(promotion_bundle['candidate_runs']) or 'n/a'}`",
                f"- Portfolio NAV: `{_md(promotion_bundle['portfolio_nav']) or 'n/a'}`",
                f"- DSR/PBO config: `{_md(promotion_bundle.get('dsr_pbo') or 'n/a')}`",
                f"- Declared inputs: `{promotion_bundle['input_count']}`",
                f"- Declared output artifacts: `{promotion_bundle['artifact_count']}`",
                "",
                "Promotion review rows:",
            ]
        )
        if promotion_bundle["review_rows"]:
            lines.append("")
            lines.append("| Run | Gates | Statistical support | Promotion decision | Recommended action |")
            lines.append("| --- | --- | --- | --- | --- |")
            for row in promotion_bundle["review_rows"]:
                gates = "pass" if row["required_gates_passed"] else "fail"
                lines.append(
                    f"| `{_md(row['run']) or 'n/a'}` | `{gates}` | "
                    f"{_md(row['statistical_support_level']) or 'n/a'} | "
                    f"{_md(row['promotion_decision']) or 'n/a'} | "
                    f"{_md(row['recommended_action']) or 'n/a'} |"
                )
        else:
            lines.append("")
            lines.append("_No compact review rows found in this manifest._")
    lines.extend(
        [
            "",
            "## Shadow-live Ledgers",
            "",
            "These ledgers are research-only observability artifacts. They record hypothetical target weights, "
            "trade deltas, estimated slippage, and forward benchmark-relative outcomes before broker execution.",
        ]
    )
    if not bundle.get("shadow_live_ledgers"):
        lines.append("")
        lines.append("_No Russell shadow-live ledger manifest found under the artifact root._")
    for ledger in bundle.get("shadow_live_ledgers", []):
        row_counts = ledger.get("row_counts") or {}
        status_note = ledger.get("status_reason") or "n/a"
        lines.extend(
            [
                "",
                f"### Shadow-live ledger `{_md(ledger['manifest_path'])}`",
                "",
                f"- Status: `{_md(ledger['status'])}`",
                f"- Status reason: {_md(status_note)}",
                f"- Manifest type: `{_md(ledger['manifest_type'])}`",
                f"- Artifact schema: `{_md(ledger['artifact_schema_version'])}`",
                f"- Generated at: `{_md(ledger['generated_at']) or 'n/a'}`",
                f"- Portfolio NAV: `{_md(ledger['portfolio_nav']) or 'n/a'}`",
                f"- Slippage bps: `{_md(ledger['slippage_bps']) or 'n/a'}`",
                f"- Forward window days: `{_md(ledger['forward_window_days']) or 'n/a'}`",
                f"- Safe haven: `{_md(ledger['safe_haven']) or 'n/a'}`",
                f"- Trade ledger rows: `{row_counts.get('shadow_live_trade_ledger', 0)}`",
                f"- Holdings ledger rows: `{row_counts.get('shadow_live_holdings_ledger', 0)}`",
                f"- Rebalance summary rows: `{row_counts.get('shadow_live_rebalance_summary', 0)}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Capacity Stress Reports",
            "",
            "These reports are research-only implementation-shortfall stress artifacts. They vary NAV, slippage, "
            "and split-trade-day assumptions before broker execution.",
        ]
    )
    if not bundle.get("capacity_stresses"):
        lines.append("")
        lines.append("_No Russell capacity stress manifest found under the artifact root._")
    for stress in bundle.get("capacity_stresses", []):
        row_counts = stress.get("row_counts") or {}
        status_note = stress.get("status_reason") or "n/a"
        lines.extend(
            [
                "",
                f"### Capacity stress `{_md(stress['manifest_path'])}`",
                "",
                f"- Status: `{_md(stress['status'])}`",
                f"- Status reason: {_md(status_note)}",
                f"- Manifest type: `{_md(stress['manifest_type'])}`",
                f"- Artifact schema: `{_md(stress['artifact_schema_version'])}`",
                f"- Generated at: `{_md(stress['generated_at']) or 'n/a'}`",
                f"- Portfolio NAV values: `{', '.join(stress['portfolio_nav_values']) or 'n/a'}`",
                f"- Slippage bps values: `{', '.join(stress['slippage_bps_values']) or 'n/a'}`",
                f"- Split trade days values: `{', '.join(stress['split_trade_days_values']) or 'n/a'}`",
                f"- Detail rows: `{row_counts.get('capacity_stress_detail', 0)}`",
                f"- Summary rows: `{row_counts.get('capacity_stress_summary', 0)}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Live Decay Monitors",
            "",
            "These monitors compare recent realized strategy returns against QQQ/SPY-style benchmarks and optional "
            "backtest-implied expectations. They are review signals only and must not trigger automated live "
            "allocation changes by themselves.",
        ]
    )
    if not bundle.get("live_decay_monitors"):
        lines.append("")
        lines.append("_No live decay monitor manifest found under the artifact root._")
    for monitor in bundle.get("live_decay_monitors", []):
        row_counts = monitor.get("row_counts") or {}
        status_note = monitor.get("status_reason") or "n/a"
        lines.extend(
            [
                "",
                f"### Live decay monitor `{_md(monitor['manifest_path'])}`",
                "",
                f"- Status: `{_md(monitor['status'])}`",
                f"- Status reason: {_md(status_note)}",
                f"- Manifest type: `{_md(monitor['manifest_type'])}`",
                f"- Artifact schema: `{_md(monitor['artifact_schema_version'])}`",
                f"- Generated at: `{_md(monitor['generated_at']) or 'n/a'}`",
                f"- Input format: `{_md(monitor['input_format']) or 'n/a'}`",
                f"- Strategies: `{', '.join(monitor['strategies']) or 'n/a'}`",
                f"- Primary benchmark: `{_md(monitor['primary_benchmark']) or 'n/a'}`",
                f"- Secondary benchmark: `{_md(monitor['secondary_benchmark']) or 'n/a'}`",
                f"- Windows: `{', '.join(monitor['windows']) or 'n/a'}`",
                f"- Policy: `{_md(monitor.get('policy') or 'n/a')}`",
                f"- Window rows: `{row_counts.get('live_decay_window_summary', 0)}`",
                f"- Strategy summary rows: `{row_counts.get('live_decay_strategy_summary', 0)}`",
            ]
        )
    lines.extend(
        [
            "",
            "## IBIT Smart DCA Research",
            "",
            "These artifacts replay buy-only DCA, parking-only baseline, and optional deterministic z-score "
            "plugin consumption. They are research-only and must not enable IBIT runtime changes by themselves.",
        ]
    )
    if not bundle.get("ibit_dca_research_reports"):
        lines.append("")
        lines.append("_No IBIT Smart DCA research manifest found under the artifact root._")
    for report in bundle.get("ibit_dca_research_reports", []):
        row_counts = report.get("row_counts") or {}
        status_note = report.get("status_reason") or "n/a"
        lines.extend(
            [
                "",
                f"### IBIT DCA research `{_md(report['manifest_path'])}`",
                "",
                f"- Status: `{_md(report['status'])}`",
                f"- Status reason: {_md(status_note)}",
                f"- Manifest type: `{_md(report['manifest_type'])}`",
                f"- Artifact schema: `{_md(report['artifact_schema_version'])}`",
                f"- IBIT symbol: `{_md(report['ibit_symbol']) or 'n/a'}`",
                f"- Parking symbol: `{_md(report['parking_symbol']) or 'n/a'}`",
                f"- Price field: `{_md(report.get('price_field') or 'n/a')}`",
                f"- Primary benchmark: `{_md(report['primary_benchmark']) or 'n/a'}`",
                f"- Secondary benchmark: `{_md(report['secondary_benchmark']) or 'n/a'}`",
                f"- BTC proxy: `{_md(report['btc_proxy_symbol']) or 'n/a'}`",
                f"- Proxy rows filled: `{_md(report['proxy_rows_filled'])}`",
                f"- Parking proxy: `{_md(report.get('parking_proxy_symbol') or 'n/a')}`",
                f"- Parking proxy rows filled: `{_md(report.get('parking_proxy_rows_filled', 0))}`",
                f"- Variants: `{', '.join(report['variants']) or 'n/a'}`",
                f"- Review status: `{_md(report.get('review_status') or 'n/a')}`",
                f"- Promotion blockers: `{_md(', '.join(report.get('promotion_blockers') or []) or 'none')}`",
                f"- Plugin gate: `{_md(report.get('plugin_gate') or 'n/a')}`",
                f"- Plugin reason: {_md(report.get('plugin_reason') or 'n/a')}",
                f"- Z-score history: `{_md(report.get('zscore_history_start') or 'n/a')}` to "
                f"`{_md(report.get('zscore_history_end') or 'n/a')}` "
                f"(`{_md(report.get('zscore_history_rows', 0))}` rows)",
                f"- Plugin signal count: `{_md(report.get('plugin_signal_count', 0))}`",
                f"- Plugin non-normal signal count: `{_md(report.get('plugin_non_normal_signal_count', 0))}`",
                f"- Plugin unavailable z-score signal count: `{_md(report.get('plugin_unavailable_signal_count', 0))}`",
                f"- Plugin route counts: `{_md(report.get('plugin_route_counts') or {})}`",
                f"- Plugin signal data-status counts: `{_md(report.get('plugin_signal_data_status_counts') or {})}`",
                f"- Z-score coverage gate: `{_md(report.get('zscore_coverage_gate') or 'n/a')}`",
                f"- Z-score available signal ratio: `{_md(_format_percent(report.get('zscore_available_signal_ratio', 0.0)))}` "
                f"(min `{_md(_format_percent(report.get('zscore_min_available_signal_ratio', 0.0)))}`)",
                f"- Gate report: `{_md(report.get('research_report_path') or 'n/a')}` "
                f"({'present' if report.get('research_report_present') else 'missing' if report.get('research_report_path') else 'n/a'})",
                f"- Period summary rows: `{row_counts.get('ibit_dca_period_summary', 0)}`",
                f"- Trade ledger rows: `{row_counts.get('ibit_dca_trade_ledger', 0)}`",
                f"- Signal consumption rows: `{row_counts.get('ibit_dca_signal_consumption', 0)}`",
                f"- Live-readiness rows: `{row_counts.get('ibit_dca_live_readiness_summary', 0)}`",
            ]
        )

    lines.extend(
        [
            "",
            "## Review Questions",
            "",
            "1. Are all expected monthly snapshot profiles present and internally complete?",
            "2. Do snapshot dates, row counts, and contract versions look suitable for downstream runtimes?",
            "3. Are any missing artifacts, stale snapshots, or ranking previews review blockers?",
            "4. Do live strategy health reports suggest watch or retirement-review follow-up without overfitting?",
            "5. If promotion manifests are present, do their review rows and statistical/context diagnostics support the intended research-only conclusion?",
            "6. If shadow-live ledgers are present, do the trade deltas, slippage estimates, and forward returns support continuing toward paper/live promotion?",
            "7. If capacity stress reports are present, which NAV/slippage/split-day assumptions remain implementable?",
            "8. If live decay monitors are present, do recent QQQ/SPY or expected-edge gaps require human review without changing runtime automatically?",
            "9. If IBIT DCA research is present, does plugin-on beat buy-only DCA and parking-only baselines after cash-flow-adjusted returns and benchmark-relative review?",
            "10. Which follow-up tasks are low/medium-risk enough for unattended remediation, and which must stay human-reviewed?",
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
        promotion_bundle_manifest_paths=args.promotion_bundle_manifest,
        shadow_live_ledger_manifest_paths=args.shadow_live_ledger_manifest,
        capacity_stress_manifest_paths=args.capacity_stress_manifest,
        live_decay_monitor_manifest_paths=args.live_decay_monitor_manifest,
        ibit_dca_research_manifest_paths=args.ibit_dca_research_manifest,
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
