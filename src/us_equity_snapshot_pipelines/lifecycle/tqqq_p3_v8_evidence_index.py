"""Durable, no-bars P3 index for the TQQQ V8 free-data candidate."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from .tqqq_core_only_p1_binding import P2_V8_CONTRACT
from .tqqq_p3_v7_evidence_index import P3_STATUS, validate_tqqq_p3_v7_result

_SCHEMA = "qsl.tqqq-p1-p3-v8-free-ohlcv-evidence-index.v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_MANDATE = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
_NUMBER = re.compile(r"^[1-9][0-9]*$")
_FIELDS = frozenset({"schema_version", "candidate", "nonlive_scope_record", "p1_manifest_sha256", "p3_evidence_sha256", "p3_promotion_result_sha256", "relative_benchmark_policy_sha256", "status", "verdict", "input_producer", "producer", "lifecycle_claims"})
_CLAIMS = {"authority_scope": "RESEARCH_ONLY", "learning_only": True, "promotion_eligible": False, "live_ready": False, "size_zero_required": True, "no_order": True}


class TqqqP3V8EvidenceIndexError(ValueError):
    """Reject malformed or broader-than-research-only V8 provenance."""


def _mapping(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise TqqqP3V8EvidenceIndexError("invalid V8 evidence index")
    return dict(value)


def _digest(value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise TqqqP3V8EvidenceIndexError("invalid V8 evidence digest")
    return value


def _input_producer(value: object) -> dict[str, str]:
    producer = _mapping(value, frozenset({"repository", "commit_sha", "tree_sha", "tool", "tool_version"}))
    if producer.get("repository") != "QuantStrategyLab/UsEquitySnapshotPipelines" or producer.get("tool") != "tqqq_core_only_free_ohlcv_p1" or producer.get("tool_version") != "v1":
        raise TqqqP3V8EvidenceIndexError("invalid V8 P1 producer")
    if not all(isinstance(producer[key], str) and _REVISION.fullmatch(str(producer[key])) for key in ("commit_sha", "tree_sha")):
        raise TqqqP3V8EvidenceIndexError("invalid V8 P1 producer")
    return {key: str(producer[key]) for key in producer}


def _producer(value: object) -> dict[str, str]:
    producer = _mapping(value, frozenset({"repository", "commit_sha", "tree_sha", "tool", "tool_version", "workflow_run_id", "workflow_run_attempt"}))
    if producer.get("repository") != "QuantStrategyLab/UsEquitySnapshotPipelines" or producer.get("tool") != "tqqq_p1_p3_v8_free_ohlcv_evidence_index" or producer.get("tool_version") != "v1":
        raise TqqqP3V8EvidenceIndexError("invalid V8 P3 producer")
    if not all(isinstance(producer[key], str) and _REVISION.fullmatch(str(producer[key])) for key in ("commit_sha", "tree_sha")) or not all(isinstance(producer[key], str) and _NUMBER.fullmatch(str(producer[key])) for key in ("workflow_run_id", "workflow_run_attempt")):
        raise TqqqP3V8EvidenceIndexError("invalid V8 P3 producer")
    return {key: str(producer[key]) for key in producer}


def validate_tqqq_p3_v8_evidence_index(value: Mapping[str, object]) -> dict[str, object]:
    index = _mapping(value, _FIELDS)
    if index["schema_version"] != _SCHEMA or index["candidate"] != {"candidate_id": P2_V8_CONTRACT.candidate_id, "config_sha256": P2_V8_CONTRACT.config_sha256} or index["lifecycle_claims"] != _CLAIMS:
        raise TqqqP3V8EvidenceIndexError("invalid V8 evidence identity")
    scope = _mapping(index["nonlive_scope_record"], frozenset({"mandate_id", "receipt_sha256"}))
    if not isinstance(scope["mandate_id"], str) or not _MANDATE.fullmatch(scope["mandate_id"]):
        raise TqqqP3V8EvidenceIndexError("invalid V8 non-live scope")
    result = validate_tqqq_p3_v7_result({"evidence_sha256": index["p3_evidence_sha256"], "promotion_result_sha256": index["p3_promotion_result_sha256"], "relative_benchmark_policy_sha256": index["relative_benchmark_policy_sha256"], "status": index["status"], "verdict": index["verdict"]})
    input_producer, producer = _input_producer(index["input_producer"]), _producer(index["producer"])
    if input_producer["repository"] != producer["repository"] or input_producer["commit_sha"] != producer["commit_sha"] or input_producer["tree_sha"] != producer["tree_sha"]:
        raise TqqqP3V8EvidenceIndexError("invalid V8 provenance")
    return {"schema_version": _SCHEMA, "candidate": dict(index["candidate"]), "nonlive_scope_record": {"mandate_id": str(scope["mandate_id"]), "receipt_sha256": _digest(scope["receipt_sha256"])}, "p1_manifest_sha256": _digest(index["p1_manifest_sha256"]), "p3_evidence_sha256": result["evidence_sha256"], "p3_promotion_result_sha256": result["promotion_result_sha256"], "relative_benchmark_policy_sha256": result["relative_benchmark_policy_sha256"], "status": P3_STATUS, "verdict": result["verdict"], "input_producer": input_producer, "producer": producer, "lifecycle_claims": dict(_CLAIMS)}


def build_tqqq_p3_v8_evidence_index(*, p1_manifest_sha256: str, nonlive_scope_record: Mapping[str, object], p3_result: Mapping[str, object], input_producer: Mapping[str, object], producer: Mapping[str, object]) -> dict[str, object]:
    result = validate_tqqq_p3_v7_result(p3_result)
    return validate_tqqq_p3_v8_evidence_index({"schema_version": _SCHEMA, "candidate": {"candidate_id": P2_V8_CONTRACT.candidate_id, "config_sha256": P2_V8_CONTRACT.config_sha256}, "nonlive_scope_record": nonlive_scope_record, "p1_manifest_sha256": p1_manifest_sha256, "p3_evidence_sha256": result["evidence_sha256"], "p3_promotion_result_sha256": result["promotion_result_sha256"], "relative_benchmark_policy_sha256": result["relative_benchmark_policy_sha256"], "status": result["status"], "verdict": result["verdict"], "input_producer": input_producer, "producer": producer, "lifecycle_claims": _CLAIMS})


def canonical_tqqq_p3_v8_evidence_index_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(validate_tqqq_p3_v8_evidence_index(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


__all__ = ["TqqqP3V8EvidenceIndexError", "build_tqqq_p3_v8_evidence_index", "canonical_tqqq_p3_v8_evidence_index_bytes", "validate_tqqq_p3_v8_evidence_index"]
