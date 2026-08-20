"""Project one verified SOXL P3 summary into a watcher-safe observation.

The SOXL P3 summary is already metrics-only, but it contains several fixed
folds and transaction-cost scenarios.  This adapter accepts only its exact
digest-bound trailing 252-session OOS replay at the predeclared 10 bps cost.
It emits the common ``strategy_performance.v2`` shape used by the issue-only
research watcher.  It is not a comparison, optimization verdict, promotion,
or execution authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .soxl_core_only_p2_v3_contract import P2_V3_CONTRACT
from .soxl_core_only_p3_evidence_summary import EVIDENCE_SUMMARY_SCHEMA

STRATEGY_PERFORMANCE_SCHEMA_VERSION = "strategy_performance.v2"
METRICS_KIND = "performance"
REPOSITORY = "QuantStrategyLab/UsEquitySnapshotPipelines"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_OOS_WINDOW_ID = "trailing_252_xnys_session_oos"
_OOS_WINDOW_KIND = "rolling_locked_oos"
_OOS_COST_BPS = 10
_WATCHER_METRIC_NAMES = ("sharpe", "cagr", "calmar", "win_rate", "max_dd")


class SoxlP3StrategyPerformanceError(ValueError):
    """Sanitized rejection of an unusable SOXL P3 performance observation."""


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SoxlP3StrategyPerformanceError("invalid SOXL P3 performance evidence") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoxlP3StrategyPerformanceError(f"invalid {label}")
    return dict(value)


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise SoxlP3StrategyPerformanceError(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise SoxlP3StrategyPerformanceError(f"invalid {label}")
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        raise SoxlP3StrategyPerformanceError("invalid computed timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SoxlP3StrategyPerformanceError("invalid computed timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SoxlP3StrategyPerformanceError("invalid computed timestamp")
    return value


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise SoxlP3StrategyPerformanceError(f"invalid {label}")
    return float(value)


def _validated_oos_metrics(value: object) -> dict[str, float]:
    metrics = _mapping(value, "OOS metrics")
    required = {
        "initial_equity",
        "final_equity",
        "net_return",
        "max_drawdown",
        "one_way_turnover",
        "cost_total",
        "executed_signal_count",
        "unexecuted_final_signal",
        "replay_result_sha256",
        "sharpe",
        "cagr",
        "calmar",
        "win_rate",
    }
    if set(metrics) != required:
        raise SoxlP3StrategyPerformanceError("invalid OOS metrics")
    for key in (
        "initial_equity",
        "final_equity",
        "net_return",
        "max_drawdown",
        "one_way_turnover",
        "cost_total",
        "sharpe",
        "cagr",
        "calmar",
        "win_rate",
    ):
        _finite(metrics[key], key)
    if (
        float(metrics["initial_equity"]) <= 0.0
        or float(metrics["final_equity"]) <= 0.0
        or float(metrics["max_drawdown"]) < 0.0
        or float(metrics["one_way_turnover"]) < 0.0
        or float(metrics["cost_total"]) < 0.0
        or not 0.0 <= float(metrics["win_rate"]) <= 1.0
        or not isinstance(metrics["executed_signal_count"], int)
        or metrics["executed_signal_count"] < 1
        or metrics["unexecuted_final_signal"] is not True
    ):
        raise SoxlP3StrategyPerformanceError("invalid OOS metrics")
    _digest(metrics["replay_result_sha256"], "OOS replay digest")
    return {
        "sharpe": float(metrics["sharpe"]),
        "cagr": float(metrics["cagr"]),
        "calmar": float(metrics["calmar"]),
        "win_rate": float(metrics["win_rate"]),
        "max_dd": float(metrics["max_drawdown"]),
    }


def _validate_evidence_summary(
    value: object, *, expected_evidence_sha256: str
) -> tuple[dict[str, float], str, str]:
    summary = _mapping(value, "SOXL P3 evidence summary")
    claimed_digest = summary.pop("evidence_summary_sha256", None)
    expected_fields = {
        "schema_version",
        "status",
        "p1_identity",
        "p2_identity",
        "materialized_input_sha256",
        "evidence_plan_sha256",
        "execution_identity",
        "runs",
    }
    if (
        set(summary) != expected_fields
        or summary["schema_version"] != EVIDENCE_SUMMARY_SCHEMA
        or summary["status"] != "SUCCESS"
        or _digest(claimed_digest, "P3 evidence digest") != expected_evidence_sha256
        or _sha256(summary) != expected_evidence_sha256
    ):
        raise SoxlP3StrategyPerformanceError("invalid SOXL P3 evidence summary")
    p1 = _mapping(summary["p1_identity"], "P1 identity")
    if set(p1) != {"input_manifest_sha256", "binding_sha256", "bars_member_sha256", "date_cutoff"}:
        raise SoxlP3StrategyPerformanceError("invalid P1 identity")
    p1_digest = _digest(p1["input_manifest_sha256"], "P1 manifest digest")
    _digest(p1["binding_sha256"], "P1 binding digest")
    _digest(p1["bars_member_sha256"], "P1 member digest")
    if not isinstance(p1["date_cutoff"], str) or not _DATE.fullmatch(p1["date_cutoff"]):
        raise SoxlP3StrategyPerformanceError("invalid P1 data cutoff")
    p2 = _mapping(summary["p2_identity"], "P2 identity")
    if p2 != {"candidate_id": P2_V3_CONTRACT.candidate_id, "config_sha256": P2_V3_CONTRACT.config_sha256}:
        raise SoxlP3StrategyPerformanceError("unexpected P2 identity")
    _digest(summary["materialized_input_sha256"], "materialized input digest")
    _digest(summary["evidence_plan_sha256"], "evidence plan digest")
    execution = _mapping(summary["execution_identity"], "execution identity")
    if execution.get("repository") != "QuantStrategyLab/UsEquityStrategies":
        raise SoxlP3StrategyPerformanceError("unexpected execution identity")
    strategy_revision = _revision(execution.get("revision"), "strategy revision")
    runs = summary["runs"]
    if not isinstance(runs, list):
        raise SoxlP3StrategyPerformanceError("invalid P3 runs")
    selected = [
        _mapping(item, "P3 run")
        for item in runs
        if isinstance(item, Mapping)
        and item.get("window_id") == _OOS_WINDOW_ID
        and item.get("window_kind") == _OOS_WINDOW_KIND
        and item.get("cost_bps") == _OOS_COST_BPS
    ]
    if len(selected) != 1 or set(selected[0]) != {
        "window_id",
        "window_kind",
        "cost_bps",
        "replay_input_sha256",
        "metrics",
    }:
        raise SoxlP3StrategyPerformanceError("missing fixed trailing OOS evidence")
    _digest(selected[0]["replay_input_sha256"], "OOS replay input digest")
    return _validated_oos_metrics(selected[0]["metrics"]), p1_digest, strategy_revision


def build_soxl_p3_strategy_performance(
    *,
    evidence_summary: object,
    expected_evidence_sha256: object,
    producer_revision: object,
    computed_at: object,
) -> dict[str, object]:
    """Project one fixed-cost, trailing-OOS P3 run into watcher-safe metrics."""
    evidence_sha256 = _digest(expected_evidence_sha256, "expected P3 evidence digest")
    revision = _revision(producer_revision, "producer revision")
    timestamp = _timestamp(computed_at)
    metrics, p1_digest, strategy_revision = _validate_evidence_summary(
        evidence_summary, expected_evidence_sha256=evidence_sha256
    )
    summary = _mapping(evidence_summary, "SOXL P3 evidence summary")
    p1 = _mapping(summary["p1_identity"], "P1 identity")
    return {
        "schema_version": STRATEGY_PERFORMANCE_SCHEMA_VERSION,
        "metrics_kind": METRICS_KIND,
        "repository": REPOSITORY,
        "strategy_profile": P2_V3_CONTRACT.candidate_id,
        "candidate_kind": "individual",
        "domain": "us_equity",
        "generated_at": timestamp,
        "as_of": p1["date_cutoff"],
        "current_metrics": {name: metrics[name] for name in _WATCHER_METRIC_NAMES},
        "evidence": {
            "p1_input_digest": p1_digest,
            "p2_config_digest": P2_V3_CONTRACT.config_sha256,
            "p3_evidence_id": evidence_sha256,
            "strategy_revision": strategy_revision,
            "producer_revision": revision,
        },
        "lifecycle": {"stage": "P3", "status": "verified"},
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }


def canonical_soxl_p3_strategy_performance_bytes(value: object) -> bytes:
    """Return canonical bytes for the bounded SOXL performance observation."""
    if not isinstance(value, Mapping):
        raise SoxlP3StrategyPerformanceError("invalid SOXL strategy performance payload")
    return _canonical(dict(value))


__all__ = [
    "METRICS_KIND",
    "REPOSITORY",
    "STRATEGY_PERFORMANCE_SCHEMA_VERSION",
    "SoxlP3StrategyPerformanceError",
    "build_soxl_p3_strategy_performance",
    "canonical_soxl_p3_strategy_performance_bytes",
]
