"""Build a bounded P3 performance record for research automation.

The daily TQQQ P3 evidence package is private to the replay job.  This module
projects only five finite, independently reproducible OOS metrics plus the
immutable evidence identities needed by a research watcher.  It deliberately
does not carry bars, artifact paths, account material, parameters, orders, or
any P4--P6 authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any, Mapping

from quant_platform_kit.strategy_lifecycle.evidence_package_v2 import (
    canonical_evidence_package_v2_bytes,
    validate_evidence_package_v2,
)

from .tqqq_core_only_p1_binding import P2_V5_CONTRACT


STRATEGY_PERFORMANCE_SCHEMA_VERSION = "strategy_performance.v2"
METRICS_KIND = "performance"
REPOSITORY = "QuantStrategyLab/UsEquitySnapshotPipelines"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_REQUIRED_METRICS = {
    "sharpe": "sharpe_ratio",
    "cagr": "annualized_return",
    "calmar": "calmar_ratio",
    "win_rate": "win_rate",
    "max_dd": "max_drawdown",
}


class TqqqP3StrategyPerformanceError(ValueError):
    """Raised when a private P3 evidence package cannot make a safe projection."""


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise TqqqP3StrategyPerformanceError(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise TqqqP3StrategyPerformanceError(f"invalid {label}")
    return value


def _timestamp(value: object) -> str:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        raise TqqqP3StrategyPerformanceError("invalid computed timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TqqqP3StrategyPerformanceError("invalid computed timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TqqqP3StrategyPerformanceError("invalid computed timestamp")
    return value


def _finite_metric(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TqqqP3StrategyPerformanceError(f"invalid {label}")
    number = float(value)
    if not math.isfinite(number):
        raise TqqqP3StrategyPerformanceError(f"invalid {label}")
    return number


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TqqqP3StrategyPerformanceError(f"invalid {label}")
    return value


def _validate_research_only_evidence(
    evidence: Mapping[str, Any], *, expected_evidence_sha256: str
) -> tuple[dict[str, float], dict[str, str], str, str]:
    try:
        issues = validate_evidence_package_v2(evidence)
    except (TypeError, ValueError) as exc:
        raise TqqqP3StrategyPerformanceError("invalid P3 evidence package") from exc
    if issues:
        raise TqqqP3StrategyPerformanceError("invalid P3 evidence package")
    try:
        observed_evidence_sha256 = hashlib.sha256(
            canonical_evidence_package_v2_bytes(dict(evidence))
        ).hexdigest()
    except (TypeError, ValueError) as exc:
        raise TqqqP3StrategyPerformanceError("invalid P3 evidence package") from exc
    if observed_evidence_sha256 != expected_evidence_sha256:
        raise TqqqP3StrategyPerformanceError("P3 evidence digest mismatch")

    strategy = _mapping(evidence.get("strategy"), "strategy")
    if strategy.get("profile") != P2_V5_CONTRACT.candidate_id or strategy.get("domain") != "us_equity":
        raise TqqqP3StrategyPerformanceError("unexpected P3 strategy identity")
    strategy_revision = _revision(strategy.get("source_revision"), "strategy revision")

    claims = _mapping(evidence.get("lifecycle_claims"), "lifecycle claims")
    if claims != {
        "learning_only": True,
        "promotion_eligible": False,
        "live_ready": False,
        "size_zero_required": True,
        "no_order": True,
    }:
        raise TqqqP3StrategyPerformanceError("P3 evidence is not research-only")

    provenance = _mapping(evidence.get("input_provenance"), "input provenance")
    digests = _mapping(evidence.get("digests"), "evidence digests")
    identities = {
        "p1_input_digest": _digest(provenance.get("manifest_sha256"), "P1 manifest digest"),
        "p2_config_digest": _digest(digests.get("config_sha256"), "P2 config digest"),
        "p3_evidence_id": expected_evidence_sha256,
    }
    evidence_metrics = _mapping(evidence.get("metrics"), "evidence metrics")
    metrics = {
        output_name: _finite_metric(evidence_metrics.get(evidence_name), output_name)
        for output_name, evidence_name in _REQUIRED_METRICS.items()
    }
    p1_range = _mapping(provenance.get("range"), "P1 range")
    return metrics, identities, strategy_revision, str(p1_range.get("end") or "")


def build_tqqq_p3_strategy_performance(
    *,
    evidence_package: object,
    expected_evidence_sha256: object,
    producer_revision: object,
    computed_at: object,
) -> dict[str, object]:
    """Project one validated P3 completion into ``strategy_performance.v2``.

    This is a one-run observation, not a parameter baseline or an optimization
    verdict.  The downstream watcher must obtain a distinct earlier completed
    record before it may compare metrics.
    """
    evidence = _mapping(evidence_package, "evidence package")
    evidence_sha256 = _digest(expected_evidence_sha256, "expected P3 evidence digest")
    revision = _revision(producer_revision, "producer revision")
    timestamp = _timestamp(computed_at)
    metrics, identities, strategy_revision, as_of = _validate_research_only_evidence(
        evidence, expected_evidence_sha256=evidence_sha256
    )
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", as_of):
        raise TqqqP3StrategyPerformanceError("invalid P3 data cutoff")

    return {
        "schema_version": STRATEGY_PERFORMANCE_SCHEMA_VERSION,
        "metrics_kind": METRICS_KIND,
        "repository": REPOSITORY,
        "strategy_profile": P2_V5_CONTRACT.candidate_id,
        "candidate_kind": "individual",
        "domain": "us_equity",
        "generated_at": timestamp,
        "as_of": as_of,
        "current_metrics": metrics,
        "evidence": {
            **identities,
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


def canonical_tqqq_p3_strategy_performance_bytes(value: object) -> bytes:
    """Return canonical bytes for the bounded performance projection."""
    if not isinstance(value, Mapping):
        raise TqqqP3StrategyPerformanceError("invalid strategy performance payload")
    return json.dumps(
        dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


__all__ = [
    "METRICS_KIND",
    "REPOSITORY",
    "STRATEGY_PERFORMANCE_SCHEMA_VERSION",
    "TqqqP3StrategyPerformanceError",
    "build_tqqq_p3_strategy_performance",
    "canonical_tqqq_p3_strategy_performance_bytes",
]
