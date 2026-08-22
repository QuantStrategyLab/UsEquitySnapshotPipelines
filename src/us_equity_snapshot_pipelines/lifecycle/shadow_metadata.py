"""Small adapter for exposing P3 evidence in the shared shadow vocabulary."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import re

from ..shadow_contract import SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION

FORWARD_OBSERVATION_CONTRACT_SCHEMA_VERSION = "forward_observation_contract.v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


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


def build_research_only_forward_observation(
    evidence: Mapping[str, Any],
    *,
    strategy_profile: str,
    observation_id: str,
    observation_mode: str,
    input_manifest_sha256: str,
    performance_artifact_sha256: str,
) -> dict[str, Any]:
    """Build the small forward-observation envelope shared by TQQQ and SOXL.

    This is deliberately a control-plane record only.  It carries exact
    bindings to the already validated P3 package and its performance artifact;
    it never accepts broker/order fields and cannot promote a candidate.
    ``observation_mode`` is explicit so synthetic contract checks cannot be
    confused with a real forward observation.
    """
    metadata = build_research_only_shadow_metadata(evidence, strategy_profile=strategy_profile)
    observation = str(observation_id or "").strip()
    if not observation or any(char.isspace() for char in observation):
        raise ValueError("observation_id is required and must not contain whitespace")
    mode = str(observation_mode or "").strip().lower()
    if mode not in {"synthetic", "forward"}:
        raise ValueError("observation_mode must be synthetic or forward")
    for name, value in {
        "input_manifest_sha256": input_manifest_sha256,
        "performance_artifact_sha256": performance_artifact_sha256,
    }.items():
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise ValueError(f"{name} must be a lowercase SHA256 digest")
    return {
        "schema_version": FORWARD_OBSERVATION_CONTRACT_SCHEMA_VERSION,
        "observation_id": observation,
        "observation_mode": mode,
        "strategy_profile": metadata["strategy_profile"],
        "shadow_contract": metadata,
        "evidence_package_id": metadata["evidence_package_id"],
        "evidence_schema_version": metadata["evidence_schema_version"],
        "input_manifest_sha256": input_manifest_sha256,
        "performance_artifact_sha256": performance_artifact_sha256,
        "promotion_eligible": False,
    }
