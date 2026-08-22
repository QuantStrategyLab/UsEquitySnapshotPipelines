"""Small adapter for exposing P3 evidence in the shared shadow vocabulary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..shadow_contract import SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION


def build_research_only_shadow_metadata(
    evidence: Mapping[str, Any], *, strategy_profile: str
) -> dict[str, Any]:
    """Project a validated P3 evidence package into shadow-only metadata.

    This deliberately does not mutate or broaden the P3 evidence package.
    TQQQ/SOXL callers can attach the returned object to a review record while
    preserving their existing evidence schemas and promotion permissions.
    """
    if not isinstance(evidence, Mapping):
        raise ValueError("evidence package must be an object")
    if evidence.get("schema_version") != "strategy_evidence_package.v2":
        raise ValueError("unsupported evidence package schema")
    profile = str(strategy_profile or "").strip()
    if not profile:
        raise ValueError("strategy_profile is required")
    return {
        "schema_version": SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION,
        "strategy_profile": profile,
        "mode": "research_only",
        "no_order": True,
        "broker_access": False,
        "evidence_package_id": str(evidence.get("evidence_package_id") or ""),
        "evidence_schema_version": str(evidence["schema_version"]),
    }
