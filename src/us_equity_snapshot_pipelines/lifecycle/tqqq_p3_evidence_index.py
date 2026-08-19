"""Canonical, non-live-only index for a completed TQQQ P3 evidence package."""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from typing import Any

from .tqqq_core_only_p1_binding import CANDIDATE_CONFIG_SHA256, CANDIDATE_ID

SCHEMA_VERSION = "qsl.tqqq-p1-p3-evidence-index.v1"
P3_STATUS = "EVIDENCE_V2_COMPLETE"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_RUN_NUMBER = re.compile(r"^[1-9][0-9]*$")
_P3_RESULT_FIELDS = frozenset({"evidence_sha256", "status", "verdict"})
_INDEX_FIELDS = frozenset(
    {
        "schema_version",
        "candidate",
        "nonlive_scope_record",
        "p1_manifest_sha256",
        "p3_evidence_sha256",
        "status",
        "verdict",
        "input_producer",
        "producer",
        "lifecycle_claims",
    }
)
_INPUT_PRODUCER_FIELDS = frozenset(
    {"repository", "commit_sha", "tree_sha", "tool", "tool_version"}
)
_MANDATE_FIELDS = frozenset({"mandate_id", "receipt_sha256"})
_MANDATE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
_P3_PRODUCER_FIELDS = _INPUT_PRODUCER_FIELDS | frozenset(
    {"workflow_run_id", "workflow_run_attempt"}
)
_VERDICTS = frozenset(
    {
        "PASS_READY_FOR_SEPARATE_HUMAN_PROMOTION_DECISION",
        "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        "INCONCLUSIVE_DATA_OR_EXECUTION",
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


class TqqqP3EvidenceIndexError(ValueError):
    """Fail-closed error for a malformed or non-research P3 evidence index."""


def canonical_tqqq_p3_evidence_index_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and encode the bounded metadata that may leave the P3 runner."""
    return json.dumps(
        validate_tqqq_p3_evidence_index(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _mapping(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise TqqqP3EvidenceIndexError(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise TqqqP3EvidenceIndexError(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise TqqqP3EvidenceIndexError(f"invalid {label}")
    return value


def _input_producer(value: object) -> dict[str, str]:
    producer = _mapping(value, _INPUT_PRODUCER_FIELDS, "P1 producer")
    if (
        producer["repository"] != "QuantStrategyLab/UsEquitySnapshotPipelines"
        or producer["tool"] != "tqqq_core_only_p1_alpaca_sip_acquisition"
        or producer["tool_version"] != "v1"
    ):
        raise TqqqP3EvidenceIndexError("invalid P1 producer")
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"], "P1 producer commit"),
        "tree_sha": _revision(producer["tree_sha"], "P1 producer tree"),
        "tool": producer["tool"],
        "tool_version": producer["tool_version"],
    }


def _p3_producer(value: object) -> dict[str, str]:
    producer = _mapping(value, _P3_PRODUCER_FIELDS, "P3 producer")
    if (
        producer["repository"] != "QuantStrategyLab/UsEquitySnapshotPipelines"
        or producer["tool"] != "tqqq_p1_p3_nonlive_evidence_index"
        or producer["tool_version"] != "v1"
        or not isinstance(producer["workflow_run_id"], str)
        or not _RUN_NUMBER.fullmatch(producer["workflow_run_id"])
        or not isinstance(producer["workflow_run_attempt"], str)
        or not _RUN_NUMBER.fullmatch(producer["workflow_run_attempt"])
    ):
        raise TqqqP3EvidenceIndexError("invalid P3 producer")
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"], "P3 producer commit"),
        "tree_sha": _revision(producer["tree_sha"], "P3 producer tree"),
        "tool": producer["tool"],
        "tool_version": producer["tool_version"],
        "workflow_run_id": producer["workflow_run_id"],
        "workflow_run_attempt": producer["workflow_run_attempt"],
    }


def _nonlive_scope_record(value: object) -> dict[str, str]:
    mandate = _mapping(value, _MANDATE_FIELDS, "non-live scope record")
    if not isinstance(mandate["mandate_id"], str) or not _MANDATE_ID.fullmatch(mandate["mandate_id"]):
        raise TqqqP3EvidenceIndexError("invalid non-live scope record")
    return {
        "mandate_id": mandate["mandate_id"],
        "receipt_sha256": _digest(mandate["receipt_sha256"], "non-live scope record receipt"),
    }


def validate_tqqq_p3_result(value: Mapping[str, object]) -> dict[str, str]:
    """Accept only the success summary emitted by ``run_tqqq_p3.py``."""
    result = _mapping(value, _P3_RESULT_FIELDS, "P3 result")
    if (
        result["status"] != P3_STATUS
        or not isinstance(result["verdict"], str)
        or result["verdict"] not in _VERDICTS
    ):
        raise TqqqP3EvidenceIndexError("invalid completed P3 result")
    return {
        "evidence_sha256": _digest(result["evidence_sha256"], "P3 evidence digest"),
        "status": result["status"],
        "verdict": result["verdict"],
    }


def build_tqqq_p3_evidence_index(
    *,
    p1_manifest_sha256: str,
    nonlive_scope_record: Mapping[str, object],
    p3_result: Mapping[str, object],
    input_producer: Mapping[str, object],
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Build the only P3 material eligible for remote retention: metadata and digests."""
    result = validate_tqqq_p3_result(p3_result)
    return validate_tqqq_p3_evidence_index(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate": {
                "candidate_id": CANDIDATE_ID,
                "config_sha256": CANDIDATE_CONFIG_SHA256,
            },
            "nonlive_scope_record": nonlive_scope_record,
            "p1_manifest_sha256": p1_manifest_sha256,
            "p3_evidence_sha256": result["evidence_sha256"],
            "status": result["status"],
            "verdict": result["verdict"],
            "input_producer": input_producer,
            "producer": producer,
            "lifecycle_claims": _LIFECYCLE_CLAIMS,
        }
    )


def validate_tqqq_p3_evidence_index(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the exact durable index schema and its non-live boundary."""
    index = _mapping(value, _INDEX_FIELDS, "TQQQ P3 evidence index")
    candidate = _mapping(index["candidate"], frozenset({"candidate_id", "config_sha256"}), "candidate")
    if candidate != {"candidate_id": CANDIDATE_ID, "config_sha256": CANDIDATE_CONFIG_SHA256}:
        raise TqqqP3EvidenceIndexError("invalid candidate")
    if index["schema_version"] != SCHEMA_VERSION:
        raise TqqqP3EvidenceIndexError("invalid evidence index schema")
    nonlive_scope_record = _nonlive_scope_record(index["nonlive_scope_record"])
    result = validate_tqqq_p3_result(
        {
            "evidence_sha256": index["p3_evidence_sha256"],
            "status": index["status"],
            "verdict": index["verdict"],
        }
    )
    if index["lifecycle_claims"] != _LIFECYCLE_CLAIMS:
        raise TqqqP3EvidenceIndexError("invalid lifecycle claims")
    input_producer = _input_producer(index["input_producer"])
    producer = _p3_producer(index["producer"])
    if (
        input_producer["repository"] != producer["repository"]
        or input_producer["commit_sha"] != producer["commit_sha"]
        or input_producer["tree_sha"] != producer["tree_sha"]
    ):
        raise TqqqP3EvidenceIndexError("P1 and P3 producer revisions must match")
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate": candidate,
        "nonlive_scope_record": nonlive_scope_record,
        "p1_manifest_sha256": _digest(index["p1_manifest_sha256"], "P1 manifest digest"),
        "p3_evidence_sha256": result["evidence_sha256"],
        "status": result["status"],
        "verdict": result["verdict"],
        "input_producer": input_producer,
        "producer": producer,
        "lifecycle_claims": copy.deepcopy(_LIFECYCLE_CLAIMS),
    }


__all__ = [
    "P3_STATUS",
    "SCHEMA_VERSION",
    "TqqqP3EvidenceIndexError",
    "build_tqqq_p3_evidence_index",
    "canonical_tqqq_p3_evidence_index_bytes",
    "validate_tqqq_p3_evidence_index",
    "validate_tqqq_p3_result",
]
