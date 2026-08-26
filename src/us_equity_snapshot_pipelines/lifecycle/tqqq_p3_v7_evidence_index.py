"""Durable, no-bars P3 index for TQQQ's separate v7 acceptance candidate."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from typing import Any

from .tqqq_core_only_p1_binding import P2_V7_CONTRACT

SCHEMA_VERSION = "qsl.tqqq-p1-p3-v7-evidence-index.v1"
P3_STATUS = "EVIDENCE_V2_COMPLETE"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_RUN_NUMBER = re.compile(r"^[1-9][0-9]*$")
_MANDATE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
_VERDICTS = frozenset(
    {
        "PASS_PENDING_FORWARD_CONFIRMATION",
        "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        "INCONCLUSIVE_DATA_OR_EXECUTION",
    }
)
_INDEX_FIELDS = frozenset(
    {
        "schema_version",
        "candidate",
        "nonlive_scope_record",
        "p1_manifest_sha256",
        "p3_evidence_sha256",
        "p3_promotion_result_sha256",
        "relative_benchmark_policy_sha256",
        "status",
        "verdict",
        "input_producer",
        "producer",
        "lifecycle_claims",
    }
)
_LIFECYCLE_CLAIMS = {
    "authority_scope": "RESEARCH_ONLY",
    "learning_only": True,
    "promotion_eligible": False,
    "live_ready": False,
    "size_zero_required": True,
    "no_order": True,
}


class TqqqP3V7EvidenceIndexError(ValueError):
    """Fail closed for malformed or non-research-only v7 index metadata."""


def _mapping(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise TqqqP3V7EvidenceIndexError(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise TqqqP3V7EvidenceIndexError(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise TqqqP3V7EvidenceIndexError(f"invalid {label}")
    return value


def _nonlive_scope_record(value: object) -> dict[str, str]:
    record = _mapping(value, frozenset({"mandate_id", "receipt_sha256"}), "non-live scope record")
    if not isinstance(record["mandate_id"], str) or not _MANDATE_ID.fullmatch(record["mandate_id"]):
        raise TqqqP3V7EvidenceIndexError("invalid non-live scope record")
    return {
        "mandate_id": record["mandate_id"],
        "receipt_sha256": _digest(record["receipt_sha256"], "non-live scope receipt"),
    }


def _input_producer(value: object) -> dict[str, str]:
    fields = frozenset({"repository", "commit_sha", "tree_sha", "tool", "tool_version"})
    producer = _mapping(value, fields, "P1 producer")
    if (
        producer["repository"] != "QuantStrategyLab/UsEquitySnapshotPipelines"
        or producer["tool"] != "tqqq_core_only_p1_alpaca_sip_acquisition"
        or producer["tool_version"] != "v1"
    ):
        raise TqqqP3V7EvidenceIndexError("invalid P1 producer")
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"], "P1 producer commit"),
        "tree_sha": _revision(producer["tree_sha"], "P1 producer tree"),
        "tool": producer["tool"],
        "tool_version": producer["tool_version"],
    }


def _producer(value: object) -> dict[str, str]:
    fields = frozenset(
        {"repository", "commit_sha", "tree_sha", "tool", "tool_version", "workflow_run_id", "workflow_run_attempt"}
    )
    producer = _mapping(value, fields, "P3 producer")
    if (
        producer["repository"] != "QuantStrategyLab/UsEquitySnapshotPipelines"
        or producer["tool"] != "tqqq_p1_p3_v7_nonlive_evidence_index"
        or producer["tool_version"] != "v1"
        or not isinstance(producer["workflow_run_id"], str)
        or not _RUN_NUMBER.fullmatch(producer["workflow_run_id"])
        or not isinstance(producer["workflow_run_attempt"], str)
        or not _RUN_NUMBER.fullmatch(producer["workflow_run_attempt"])
    ):
        raise TqqqP3V7EvidenceIndexError("invalid P3 producer")
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"], "P3 producer commit"),
        "tree_sha": _revision(producer["tree_sha"], "P3 producer tree"),
        "tool": producer["tool"],
        "tool_version": producer["tool_version"],
        "workflow_run_id": producer["workflow_run_id"],
        "workflow_run_attempt": producer["workflow_run_attempt"],
    }


def validate_tqqq_p3_v7_result(value: Mapping[str, object]) -> dict[str, str]:
    """Validate only the v7 P3 completion summary, never its raw artifacts."""
    fields = frozenset(
        {
            "evidence_sha256",
            "promotion_result_sha256",
            "relative_benchmark_policy_sha256",
            "status",
            "verdict",
        }
    )
    result = _mapping(value, fields, "P3 v7 result")
    if result["status"] != P3_STATUS or result["verdict"] not in _VERDICTS:
        raise TqqqP3V7EvidenceIndexError("invalid completed P3 v7 result")
    return {
        "evidence_sha256": _digest(result["evidence_sha256"], "P3 evidence digest"),
        "promotion_result_sha256": _digest(result["promotion_result_sha256"], "P3 result digest"),
        "relative_benchmark_policy_sha256": _digest(
            result["relative_benchmark_policy_sha256"], "relative policy digest"
        ),
        "status": P3_STATUS,
        "verdict": result["verdict"],
    }


def build_tqqq_p3_v7_evidence_index(
    *,
    p1_manifest_sha256: str,
    nonlive_scope_record: Mapping[str, object],
    p3_result: Mapping[str, object],
    input_producer: Mapping[str, object],
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Build the only v7 P3 object suitable for durable remote retention."""
    result = validate_tqqq_p3_v7_result(p3_result)
    return validate_tqqq_p3_v7_evidence_index(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate": {
                "candidate_id": P2_V7_CONTRACT.candidate_id,
                "config_sha256": P2_V7_CONTRACT.config_sha256,
            },
            "nonlive_scope_record": nonlive_scope_record,
            "p1_manifest_sha256": p1_manifest_sha256,
            "p3_evidence_sha256": result["evidence_sha256"],
            "p3_promotion_result_sha256": result["promotion_result_sha256"],
            "relative_benchmark_policy_sha256": result["relative_benchmark_policy_sha256"],
            "status": result["status"],
            "verdict": result["verdict"],
            "input_producer": input_producer,
            "producer": producer,
            "lifecycle_claims": _LIFECYCLE_CLAIMS,
        }
    )


def validate_tqqq_p3_v7_evidence_index(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the closed, metadata-only v7 P3 index."""
    index = _mapping(value, _INDEX_FIELDS, "TQQQ P3 v7 evidence index")
    candidate = _mapping(index["candidate"], frozenset({"candidate_id", "config_sha256"}), "candidate")
    if candidate != {
        "candidate_id": P2_V7_CONTRACT.candidate_id,
        "config_sha256": P2_V7_CONTRACT.config_sha256,
    } or index["schema_version"] != SCHEMA_VERSION:
        raise TqqqP3V7EvidenceIndexError("invalid candidate or index schema")
    result = validate_tqqq_p3_v7_result(
        {
            "evidence_sha256": index["p3_evidence_sha256"],
            "promotion_result_sha256": index["p3_promotion_result_sha256"],
            "relative_benchmark_policy_sha256": index["relative_benchmark_policy_sha256"],
            "status": index["status"],
            "verdict": index["verdict"],
        }
    )
    input_producer = _input_producer(index["input_producer"])
    producer = _producer(index["producer"])
    if (
        index["lifecycle_claims"] != _LIFECYCLE_CLAIMS
        or input_producer["repository"] != producer["repository"]
        or input_producer["commit_sha"] != producer["commit_sha"]
        or input_producer["tree_sha"] != producer["tree_sha"]
    ):
        raise TqqqP3V7EvidenceIndexError("invalid v7 non-live provenance")
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate": candidate,
        "nonlive_scope_record": _nonlive_scope_record(index["nonlive_scope_record"]),
        "p1_manifest_sha256": _digest(index["p1_manifest_sha256"], "P1 manifest digest"),
        "p3_evidence_sha256": result["evidence_sha256"],
        "p3_promotion_result_sha256": result["promotion_result_sha256"],
        "relative_benchmark_policy_sha256": result["relative_benchmark_policy_sha256"],
        "status": result["status"],
        "verdict": result["verdict"],
        "input_producer": input_producer,
        "producer": producer,
        "lifecycle_claims": copy.deepcopy(_LIFECYCLE_CLAIMS),
    }


def canonical_tqqq_p3_v7_evidence_index_bytes(value: Mapping[str, object]) -> bytes:
    """Return canonical bytes only after closed-schema validation."""
    return json.dumps(
        validate_tqqq_p3_v7_evidence_index(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


__all__ = [
    "P3_STATUS",
    "SCHEMA_VERSION",
    "TqqqP3V7EvidenceIndexError",
    "build_tqqq_p3_v7_evidence_index",
    "canonical_tqqq_p3_v7_evidence_index_bytes",
    "validate_tqqq_p3_v7_evidence_index",
    "validate_tqqq_p3_v7_result",
]
