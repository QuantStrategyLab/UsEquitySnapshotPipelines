"""Durable, sanitized diagnostics for a verified historical P1 snapshot.

These records are deliberately separate from formal P1/P3 evidence indexes.
They permit a newer research revision to explain a previously verified input
without claiming that the P1 and P3 producers were the same revision.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping

from .tqqq_p3_v7_evidence_index import validate_tqqq_p3_v7_result

_SCHEMA = "qsl.verified-snapshot-relative-benchmark-diagnostic.v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40}$")
_NUMBER = re.compile(r"^[1-9][0-9]*$")
_CANDIDATE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_CLAIMS = {
    "authority_scope": "RESEARCH_ONLY",
    "diagnostic_only": True,
    "formal_evidence_index": False,
    "promotion_eligible": False,
    "live_ready": False,
    "no_order": True,
}
_FIELDS = frozenset({"schema_version", "source_snapshot", "diagnostic", "lifecycle_claims"})


class VerifiedSnapshotRelativeBenchmarkDiagnosticError(ValueError):
    """Reject diagnostic records that could be mistaken for formal evidence."""


def _fail() -> None:
    raise VerifiedSnapshotRelativeBenchmarkDiagnosticError("invalid verified snapshot diagnostic")


def _mapping(value: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail()
    return dict(value)


def _digest(value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail()
    return value


def _revision(value: object) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        _fail()
    return value


def _candidate(value: object) -> dict[str, str]:
    candidate = _mapping(value, frozenset({"candidate_id", "config_sha256"}))
    if not isinstance(candidate["candidate_id"], str) or not _CANDIDATE.fullmatch(candidate["candidate_id"]):
        _fail()
    return {"candidate_id": candidate["candidate_id"], "config_sha256": _digest(candidate["config_sha256"])}


def _input_producer(value: object) -> dict[str, str]:
    producer = _mapping(value, frozenset({"repository", "commit_sha", "tree_sha", "tool", "tool_version"}))
    if (
        not isinstance(producer["repository"], str)
        or not producer["repository"]
        or not isinstance(producer["tool"], str)
        or not producer["tool"]
        or not isinstance(producer["tool_version"], str)
        or not producer["tool_version"]
    ):
        _fail()
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"]),
        "tree_sha": _revision(producer["tree_sha"]),
        "tool": producer["tool"],
        "tool_version": producer["tool_version"],
    }


def _diagnostic_producer(value: object) -> dict[str, str]:
    producer = _mapping(
        value,
        frozenset({"repository", "commit_sha", "tree_sha", "tool", "tool_version", "workflow_run_id", "workflow_run_attempt"}),
    )
    if (
        not isinstance(producer["repository"], str)
        or not producer["repository"]
        or producer["tool"] != "verified_snapshot_relative_benchmark_diagnostic"
        or producer["tool_version"] != "v1"
        or not isinstance(producer["workflow_run_id"], str)
        or not _NUMBER.fullmatch(producer["workflow_run_id"])
        or not isinstance(producer["workflow_run_attempt"], str)
        or not _NUMBER.fullmatch(producer["workflow_run_attempt"])
    ):
        _fail()
    return {
        "repository": producer["repository"],
        "commit_sha": _revision(producer["commit_sha"]),
        "tree_sha": _revision(producer["tree_sha"]),
        "tool": "verified_snapshot_relative_benchmark_diagnostic",
        "tool_version": "v1",
        "workflow_run_id": producer["workflow_run_id"],
        "workflow_run_attempt": producer["workflow_run_attempt"],
    }


def validate_verified_snapshot_relative_benchmark_diagnostic(value: Mapping[str, object]) -> dict[str, object]:
    """Validate a no-bars diagnostic record; it is never formal promotion evidence."""
    record = _mapping(value, _FIELDS)
    if record["schema_version"] != _SCHEMA or record["lifecycle_claims"] != _CLAIMS:
        _fail()
    source = _mapping(record["source_snapshot"], frozenset({"candidate", "input_manifest_sha256", "input_producer"}))
    candidate = _candidate(source["candidate"])
    input_producer = _input_producer(source["input_producer"])
    diagnostic = _mapping(
        record["diagnostic"],
        frozenset({"p3_terminal", "relative_benchmark_summary_sha256", "producer"}),
    )
    try:
        p3_terminal = validate_tqqq_p3_v7_result(diagnostic["p3_terminal"])
    except ValueError:
        _fail()
    summary_sha256 = _digest(diagnostic["relative_benchmark_summary_sha256"])
    producer = _diagnostic_producer(diagnostic["producer"])
    if producer["repository"] != input_producer["repository"]:
        _fail()
    return {
        "schema_version": _SCHEMA,
        "source_snapshot": {
            "candidate": candidate,
            "input_manifest_sha256": _digest(source["input_manifest_sha256"]),
            "input_producer": input_producer,
        },
        "diagnostic": {
            "p3_terminal": p3_terminal,
            "relative_benchmark_summary_sha256": summary_sha256,
            "producer": producer,
        },
        "lifecycle_claims": dict(_CLAIMS),
    }


def build_verified_snapshot_relative_benchmark_diagnostic(
    *,
    candidate: Mapping[str, object],
    input_manifest_sha256: str,
    input_producer: Mapping[str, object],
    p3_terminal: Mapping[str, object],
    relative_benchmark_summary_sha256: str,
    producer: Mapping[str, object],
) -> dict[str, object]:
    """Build a diagnostic-only replay record after independent root verification."""
    return validate_verified_snapshot_relative_benchmark_diagnostic(
        {
            "schema_version": _SCHEMA,
            "source_snapshot": {
                "candidate": candidate,
                "input_manifest_sha256": input_manifest_sha256,
                "input_producer": input_producer,
            },
            "diagnostic": {
                "p3_terminal": p3_terminal,
                "relative_benchmark_summary_sha256": relative_benchmark_summary_sha256,
                "producer": producer,
            },
            "lifecycle_claims": _CLAIMS,
        }
    )


def canonical_verified_snapshot_relative_benchmark_diagnostic_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        validate_verified_snapshot_relative_benchmark_diagnostic(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


__all__ = [
    "VerifiedSnapshotRelativeBenchmarkDiagnosticError",
    "build_verified_snapshot_relative_benchmark_diagnostic",
    "canonical_verified_snapshot_relative_benchmark_diagnostic_bytes",
    "validate_verified_snapshot_relative_benchmark_diagnostic",
]
