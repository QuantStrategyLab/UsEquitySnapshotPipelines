"""Fixed P3 evidence planning and summary for the isolated P2 v4 candidate.

The mathematical replay-window and metric logic is shared with v3 through
fresh private modules.  This adapter makes the v4 materialized schema and its
two-member P1 identity explicit before delegating; it never mutates a v3
module or permits a v3 plan/result to satisfy v4.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path

from .soxl_core_only_free_split_close_p3_input_materializer import MATERIALIZED_INPUT_SCHEMA
from .soxl_core_only_p2_v4_free_split_close_contract import P2_V4_FREE_SPLIT_CLOSE_CONTRACT

_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class SoxlCoreOnlyFreeSplitCloseP3EvidenceError(ValueError):
    """Fail-closed v4 P3 planning or summary failure without source data."""


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
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    return dict(value)


def _p2_contract(value: object | None) -> object:
    contract = P2_V4_FREE_SPLIT_CLOSE_CONTRACT if value is None else value
    candidate_id = getattr(contract, "candidate_id", None)
    config_sha256 = getattr(contract, "config_sha256", None)
    if (
        not isinstance(candidate_id, str)
        or not candidate_id
        or not isinstance(config_sha256, str)
        or not _DIGEST.fullmatch(config_sha256)
    ):
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    return contract


def _load_module(filename: str, name: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(
        f"us_equity_snapshot_pipelines.lifecycle.{name}",
        path,
    )
    if spec is None or spec.loader is None:
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("SOXL free-source P3 evidence runtime unavailable")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # pragma: no cover - defensive module boundary
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("SOXL free-source P3 evidence runtime unavailable") from exc
    return module


def _legacy_planner_view(
    materialized: Mapping[str, object], *, p2_contract: object | None = None
) -> tuple[dict[str, object], dict[str, object]]:
    """Adapt only identity-field names for the invariant v3 planner mechanics."""
    contract = _p2_contract(p2_contract)
    original = _mapping(materialized)
    if set(original) != {
        "schema_version",
        "p1_identity",
        "p2_identity",
        "indicator_spec",
        "sessions",
        "materialized_input_sha256",
    } or original["schema_version"] != MATERIALIZED_INPUT_SCHEMA:
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    claimed_materialized_sha256 = original.pop("materialized_input_sha256")
    if (
        not isinstance(claimed_materialized_sha256, str)
        or not _DIGEST.fullmatch(claimed_materialized_sha256)
        or claimed_materialized_sha256 != _sha256(original)
    ):
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    original["materialized_input_sha256"] = claimed_materialized_sha256
    p1 = _mapping(original["p1_identity"])
    if set(p1) != {
        "input_manifest_sha256",
        "binding_sha256",
        "closes_member_sha256",
        "assurance_member_sha256",
        "date_cutoff",
    } or any(
        not isinstance(p1[field], str) or not _DIGEST.fullmatch(p1[field])
        for field in ("input_manifest_sha256", "binding_sha256", "closes_member_sha256", "assurance_member_sha256")
    ):
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    if _mapping(original["p2_identity"]) != {
        "candidate_id": contract.candidate_id,
        "config_sha256": contract.config_sha256,
    }:
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    if _mapping(original["indicator_spec"]).get("id") != "soxl-soxx-core-only-split-adjusted-close-indicators.v1":
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input")
    legacy = json.loads(_canonical(original))
    legacy["p1_identity"] = {
        "input_manifest_sha256": p1["input_manifest_sha256"],
        "binding_sha256": p1["binding_sha256"],
        "bars_member_sha256": _sha256(
            {
                "closes_member_sha256": p1["closes_member_sha256"],
                "assurance_member_sha256": p1["assurance_member_sha256"],
            }
        ),
        "date_cutoff": p1["date_cutoff"],
    }
    indicator_spec = _mapping(legacy["indicator_spec"])
    indicator_spec["id"] = "soxl-soxx-core-only-close-indicators.v1"
    legacy["indicator_spec"] = indicator_spec
    legacy.pop("materialized_input_sha256")
    legacy["materialized_input_sha256"] = _sha256(legacy)
    return original, legacy


def build_soxl_core_only_free_split_close_p3_evidence_plan(
    materialized: Mapping[str, object],
    *,
    p2_contract: object | None = None,
) -> dict[str, object]:
    """Build fixed fold/OOS cost requests for a verified candidate materialization."""
    contract = _p2_contract(p2_contract)
    original, legacy = _legacy_planner_view(materialized, p2_contract=contract)
    planner = _load_module("soxl_core_only_p3_evidence_plan.py", "qsl_soxl_core_only_p3_v4_plan_core")
    planner.P2_V3_CONTRACT = contract
    planner.MATERIALIZED_INPUT_SCHEMA = MATERIALIZED_INPUT_SCHEMA
    try:
        result = planner.build_soxl_core_only_p3_evidence_plan(legacy)
    except ValueError as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input") from exc
    result["p1_identity"] = original["p1_identity"]
    result["materialized_input_sha256"] = original["materialized_input_sha256"]
    result.pop("evidence_plan_sha256")
    result["evidence_plan_sha256"] = _sha256(result)
    return result


def build_soxl_core_only_free_split_close_p3_evidence_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
    p2_contract: object | None = None,
) -> dict[str, object]:
    """Execute exactly the fixed candidate requests and return metrics-only evidence."""
    contract = _p2_contract(p2_contract)
    summary = _load_module("soxl_core_only_p3_evidence_summary.py", "qsl_soxl_core_only_p3_v4_summary_core")
    summary.P2_V3_CONTRACT = contract
    summary.MATERIALIZED_INPUT_SCHEMA = MATERIALIZED_INPUT_SCHEMA
    summary.build_soxl_core_only_p3_evidence_plan = partial(
        build_soxl_core_only_free_split_close_p3_evidence_plan,
        p2_contract=contract,
    )
    try:
        return summary.build_soxl_core_only_p3_evidence_summary(
            materialized=materialized,
            evidence_plan=evidence_plan,
            replay_executor=replay_executor,
        )
    except ValueError as exc:
        raise SoxlCoreOnlyFreeSplitCloseP3EvidenceError("invalid SOXL free-source P3 evidence input") from exc


__all__ = [
    "SoxlCoreOnlyFreeSplitCloseP3EvidenceError",
    "build_soxl_core_only_free_split_close_p3_evidence_plan",
    "build_soxl_core_only_free_split_close_p3_evidence_summary",
]
