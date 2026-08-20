"""Bounded, research-only index for completed historical-combo P3 evidence.

The index contains identifiers and hashes only.  It is deliberately separate
from a future replay runner: constructing it does not open market data, write
an evidence artifact, make a strategy decision, or authorize any execution.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "qsl.us-equity-historical-combo-p3-evidence-index.v1"
P3_STATUS = "HISTORICAL_COMBO_RESEARCH_EVIDENCE_COMPLETE"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_RUN_NUMBER = re.compile(r"^[1-9][0-9]*$")
_RESULT_FIELDS = frozenset({"evidence_sha256", "status", "verdict"})
_INDEX_FIELDS = frozenset(
    {
        "schema_version",
        "research_only",
        "candidate",
        "p1_input_sha256",
        "p2_candidate_sha256",
        "p3_evidence_sha256",
        "status",
        "verdict",
        "producer",
        "lifecycle_claims",
    }
)
_PRODUCER_FIELDS = frozenset(
    {
        "repository",
        "commit_sha",
        "tree_sha",
        "tool",
        "tool_version",
        "workflow_run_id",
        "workflow_run_attempt",
    }
)
_VERDICTS = frozenset(
    {
        "PASS_RESEARCH_EVIDENCE_NOT_PROMOTION",
        "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        "INCONCLUSIVE_DATA_OR_EXECUTION",
    }
)
_LIFECYCLE_CLAIMS = {
    "authority_scope": "RESEARCH_ONLY",
    "promotion_eligible": False,
    "paper_authorized": False,
    "shadow_authorized": False,
    "live_authorized": False,
    "no_order": True,
}


class HistoricalComboP3EvidenceIndexError(ValueError):
    """Fail-closed error for malformed or execution-capable P3 metadata."""


def _mapping(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise HistoricalComboP3EvidenceIndexError(f"invalid {label}")
    return copy.deepcopy(dict(value))


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise HistoricalComboP3EvidenceIndexError(f"invalid {label}")
    return value


def _identity(value: object, label: str) -> str:
    if not isinstance(value, str) or not _IDENTITY.fullmatch(value):
        raise HistoricalComboP3EvidenceIndexError(f"invalid {label}")
    return value


def _revision(value: object, label: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        raise HistoricalComboP3EvidenceIndexError(f"invalid {label}")
    return value


def _producer(value: object) -> dict[str, str]:
    producer = _mapping(value, _PRODUCER_FIELDS, "P3 producer")
    if (
        producer["repository"] != "QuantStrategyLab/UsEquitySnapshotPipelines"
        or producer["tool"] != "historical_combo_p3_evidence_index"
        or producer["tool_version"] != "v1"
        or not isinstance(producer["workflow_run_id"], str)
        or not _RUN_NUMBER.fullmatch(producer["workflow_run_id"])
        or not isinstance(producer["workflow_run_attempt"], str)
        or not _RUN_NUMBER.fullmatch(producer["workflow_run_attempt"])
    ):
        raise HistoricalComboP3EvidenceIndexError("invalid P3 producer")
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"], "P3 producer commit"),
        "tree_sha": _revision(producer["tree_sha"], "P3 producer tree"),
        "tool": producer["tool"],
        "tool_version": producer["tool_version"],
        "workflow_run_id": producer["workflow_run_id"],
        "workflow_run_attempt": producer["workflow_run_attempt"],
    }


def validate_historical_combo_p3_result(value: Mapping[str, object]) -> dict[str, str]:
    """Accept only a completed, bounded P3 result summary from a future runner."""
    result = _mapping(value, _RESULT_FIELDS, "P3 result")
    if (
        result["status"] != P3_STATUS
        or not isinstance(result["verdict"], str)
        or result["verdict"] not in _VERDICTS
    ):
        raise HistoricalComboP3EvidenceIndexError("invalid completed P3 result")
    return {
        "evidence_sha256": _digest(result["evidence_sha256"], "P3 evidence digest"),
        "status": result["status"],
        "verdict": result["verdict"],
    }


def build_historical_combo_p3_evidence_index(
    *,
    candidate_id: str,
    p1_input_sha256: str,
    p2_candidate_sha256: str,
    p3_result: Mapping[str, object],
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Build retention-safe P3 metadata without making a promotion claim."""
    result = validate_historical_combo_p3_result(p3_result)
    return validate_historical_combo_p3_evidence_index(
        {
            "schema_version": SCHEMA_VERSION,
            "research_only": True,
            "candidate": {"candidate_id": candidate_id},
            "p1_input_sha256": p1_input_sha256,
            "p2_candidate_sha256": p2_candidate_sha256,
            "p3_evidence_sha256": result["evidence_sha256"],
            "status": result["status"],
            "verdict": result["verdict"],
            "producer": producer,
            "lifecycle_claims": _LIFECYCLE_CLAIMS,
        }
    )


def validate_historical_combo_p3_evidence_index(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the exact P1/P2/P3 identity chain and research-only boundary."""
    index = _mapping(value, _INDEX_FIELDS, "historical combo P3 evidence index")
    if index["schema_version"] != SCHEMA_VERSION:
        raise HistoricalComboP3EvidenceIndexError("invalid P3 evidence index schema")
    if index["research_only"] is not True:
        raise HistoricalComboP3EvidenceIndexError("P3 evidence index must be research only")
    candidate = _mapping(index["candidate"], frozenset({"candidate_id"}), "candidate")
    result = validate_historical_combo_p3_result(
        {
            "evidence_sha256": index["p3_evidence_sha256"],
            "status": index["status"],
            "verdict": index["verdict"],
        }
    )
    if index["lifecycle_claims"] != _LIFECYCLE_CLAIMS:
        raise HistoricalComboP3EvidenceIndexError("invalid P3 lifecycle claims")
    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "candidate": {"candidate_id": _identity(candidate["candidate_id"], "candidate id")},
        "p1_input_sha256": _digest(index["p1_input_sha256"], "P1 input digest"),
        "p2_candidate_sha256": _digest(index["p2_candidate_sha256"], "P2 candidate digest"),
        "p3_evidence_sha256": result["evidence_sha256"],
        "status": result["status"],
        "verdict": result["verdict"],
        "producer": _producer(index["producer"]),
        "lifecycle_claims": copy.deepcopy(_LIFECYCLE_CLAIMS),
    }


def canonical_historical_combo_p3_evidence_index_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and canonically encode bounded P3 metadata for later retention."""
    return json.dumps(
        validate_historical_combo_p3_evidence_index(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


__all__ = [
    "P3_STATUS",
    "SCHEMA_VERSION",
    "HistoricalComboP3EvidenceIndexError",
    "build_historical_combo_p3_evidence_index",
    "canonical_historical_combo_p3_evidence_index_bytes",
    "validate_historical_combo_p3_evidence_index",
    "validate_historical_combo_p3_result",
]
