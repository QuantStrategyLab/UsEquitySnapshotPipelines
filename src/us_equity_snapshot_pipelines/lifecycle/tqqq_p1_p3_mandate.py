"""Validate a bounded, non-live permit before the TQQQ P1/P3 workflow reads Alpaca."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .tqqq_core_only_p1_binding import CANDIDATE_CONFIG_SHA256, CANDIDATE_ID

SCHEMA_VERSION = "qsl.tqqq-p1-p3-nonlive-run-mandate.v1"
_MANDATE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,63}$")
_APPROVER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_RECORD = re.compile(r"^github-environment:tqqq-p1-p3-nonlive$")
_TOP_LEVEL_FIELDS = frozenset({"schema_version", "mandate_id", "candidate", "scope", "attestation"})
_CANDIDATE_FIELDS = frozenset({"candidate_id", "config_sha256"})
_SCOPE_FIELDS = frozenset(
    {
        "authority_scope",
        "provider",
        "allowed_operations",
        "no_order",
        "no_paper",
        "no_shadow",
        "no_live",
        "no_capital",
    }
)
_ATTESTATION_FIELDS = frozenset({"record_source", "recorded_by", "recorded_at", "expires_at"})
_SCOPE = {
    "authority_scope": "P1_P3_RESEARCH_ONLY",
    "provider": "ALPACA_SIP",
    "allowed_operations": [
        "p1_data_acquisition",
        "p1_private_root_create_only_upload",
        "p3_historical_replay",
        "p3_private_root_read",
        "p3_private_evidence_index_create_only_upload",
    ],
    "no_order": True,
    "no_paper": True,
    "no_shadow": True,
    "no_live": True,
    "no_capital": True,
}
_MAX_VALIDITY = timedelta(days=31)


class TqqqP1P3MandateError(ValueError):
    """Fail closed when a non-live run is not covered by a current scoped record."""


def _mapping(value: object, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise TqqqP1P3MandateError(f"invalid {label}")
    return dict(value)


def _timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise TqqqP1P3MandateError(f"invalid {label}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TqqqP1P3MandateError(f"invalid {label}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or parsed.microsecond:
        raise TqqqP1P3MandateError(f"invalid {label}")
    return parsed.astimezone(UTC)


def _now(now_utc: datetime | None) -> datetime:
    value = datetime.now(UTC) if now_utc is None else now_utc
    if value.tzinfo is None:
        raise TqqqP1P3MandateError("invalid current time")
    return value.astimezone(UTC)


def validate_tqqq_p1_p3_mandate(
    value: Mapping[str, object],
    *,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    """Validate one bounded non-live technical scope record for P1 and P3.

    This record is a narrow, expiring, no-order technical scope control. It is
    not, by itself, a pre-authorized autonomous policy or evidence that a
    non-execution data-acquisition authorization is active. That separately
    defined external authorization remains outside this repository.
    """
    mandate = _mapping(value, _TOP_LEVEL_FIELDS, "TQQQ P1/P3 mandate")
    if mandate["schema_version"] != SCHEMA_VERSION:
        raise TqqqP1P3MandateError("invalid mandate schema")
    mandate_id = mandate["mandate_id"]
    if not isinstance(mandate_id, str) or not _MANDATE_ID.fullmatch(mandate_id):
        raise TqqqP1P3MandateError("invalid mandate id")

    candidate = _mapping(mandate["candidate"], _CANDIDATE_FIELDS, "candidate")
    if candidate != {"candidate_id": CANDIDATE_ID, "config_sha256": CANDIDATE_CONFIG_SHA256}:
        raise TqqqP1P3MandateError("invalid mandate candidate")

    scope = _mapping(mandate["scope"], _SCOPE_FIELDS, "mandate scope")
    if scope != _SCOPE:
        raise TqqqP1P3MandateError("invalid mandate scope")

    attestation = _mapping(mandate["attestation"], _ATTESTATION_FIELDS, "mandate attestation")
    if (
        not isinstance(attestation["record_source"], str)
        or not _RECORD.fullmatch(attestation["record_source"])
        or not isinstance(attestation["recorded_by"], str)
        or not _APPROVER.fullmatch(attestation["recorded_by"])
    ):
        raise TqqqP1P3MandateError("invalid mandate attestation")
    recorded_at = _timestamp(attestation["recorded_at"], "record time")
    expires_at = _timestamp(attestation["expires_at"], "expiry time")
    now = _now(now_utc)
    if recorded_at > now or expires_at < now or expires_at <= recorded_at or expires_at - recorded_at > _MAX_VALIDITY:
        raise TqqqP1P3MandateError("mandate is not current")

    return {
        "schema_version": SCHEMA_VERSION,
        "mandate_id": mandate_id,
        "candidate": {"candidate_id": CANDIDATE_ID, "config_sha256": CANDIDATE_CONFIG_SHA256},
        "scope": dict(_SCOPE),
        "attestation": {
            "record_source": attestation["record_source"],
            "recorded_by": attestation["recorded_by"],
            "recorded_at": attestation["recorded_at"],
            "expires_at": attestation["expires_at"],
        },
    }


def canonical_tqqq_p1_p3_mandate_bytes(value: Mapping[str, object], *, now_utc: datetime | None = None) -> bytes:
    """Return canonical bytes for a valid current mandate."""
    return json.dumps(
        validate_tqqq_p1_p3_mandate(value, now_utc=now_utc),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def tqqq_p1_p3_mandate_receipt_sha256(value: Mapping[str, object], *, now_utc: datetime | None = None) -> str:
    """Return the replay provenance digest for the exact scoped mandate."""
    return hashlib.sha256(canonical_tqqq_p1_p3_mandate_bytes(value, now_utc=now_utc)).hexdigest()


def load_tqqq_p1_p3_mandate(
    mandates_root: str | Path,
    mandate_id: str,
    *,
    now_utc: datetime | None = None,
) -> tuple[dict[str, object], str]:
    """Load exactly one checked-in mandate without allowing path traversal."""
    if not isinstance(mandate_id, str) or not _MANDATE_ID.fullmatch(mandate_id):
        raise TqqqP1P3MandateError("invalid mandate id")
    root = Path(mandates_root)
    path = root / f"{mandate_id}.json"
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, TypeError, json.JSONDecodeError) as exc:
        raise TqqqP1P3MandateError("mandate record is unavailable") from exc
    validated = validate_tqqq_p1_p3_mandate(payload, now_utc=now_utc)
    if validated["mandate_id"] != mandate_id:
        raise TqqqP1P3MandateError("mandate record identity mismatch")
    return validated, tqqq_p1_p3_mandate_receipt_sha256(validated, now_utc=now_utc)


__all__ = [
    "SCHEMA_VERSION",
    "TqqqP1P3MandateError",
    "canonical_tqqq_p1_p3_mandate_bytes",
    "load_tqqq_p1_p3_mandate",
    "tqqq_p1_p3_mandate_receipt_sha256",
    "validate_tqqq_p1_p3_mandate",
]
