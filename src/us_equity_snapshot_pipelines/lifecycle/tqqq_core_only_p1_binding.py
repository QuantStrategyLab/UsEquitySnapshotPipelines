"""Static P1 data identity binding for the frozen TQQQ core-only candidate."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping

from quant_platform_kit.data.research_input import (
    research_input_manifest_sha256,
    validate_research_input_manifest,
)

CANDIDATE_ID = "tqqq_core_only_p2_v1"
CANDIDATE_CONFIG_SHA256 = "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69"
UES_REVISION = "8b6b418bac74318f8054c5951521c9b62391de3e"
INPUT_CONTRACT_ID = "tqqq_core_only_ibkr_adjusted_last.v1"
_INPUT_SCHEMA = "qsl.tqqq_core_only_p1_data_binding.v1"
_UNIVERSE = ("QQQ", "TQQQ", "QQQM", "BOXX")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")


class TqqqCoreOnlyP1BindingError(ValueError):
    """Sanitized failure for an invalid static P1 binding."""


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def build_tqqq_core_only_p1_binding() -> dict[str, object]:
    """Return the frozen data-only identity; this function performs no acquisition."""
    return {
        "schema_version": _INPUT_SCHEMA,
        "candidate": {
            "candidate_id": CANDIDATE_ID,
            "config_sha256": CANDIDATE_CONFIG_SHA256,
        },
        "source": {
            "repository": "QuantStrategyLab/UsEquityStrategies",
            "revision": UES_REVISION,
        },
        "data_identity": {
            "provider": "IBKR",
            "feed": "ADJUSTED_LAST",
            "calendar": {
                "calendar_id": "XNYS",
                "timezone": "America/New_York",
                "source": "exchange_calendars",
            },
            "adjustment": {
                "policy": "total_return_adjusted",
                "source": "IBKR_ADJUSTED_LAST",
            },
            "universe": list(_UNIVERSE),
            "date_cutoff": "2026-07-31",
            "cost_assumptions": {
                "turnover_cost_bps": 5.0,
                "stress_turnover_cost_bps": [10.0, 25.0],
                "borrow_cost_bps": 0.0,
                "cash_yield_assumption": 0.0,
                "execution_timing": "next_complete_trading_session_after_signal_effective_date",
            },
            "retention": {
                "policy": "PRIVATE_LOCAL_ENCRYPTED_RESEARCH_SNAPSHOT_NO_BACKUP_NO_REDISTRIBUTION",
                "redistribution_allowed": False,
            },
        },
    }


def canonical_binding_bytes(value: Mapping[str, object]) -> bytes:
    """Validate and encode one exact binding in canonical form."""
    validated = validate_tqqq_core_only_p1_binding(value)
    return _canonical(validated)


def binding_sha256(value: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_binding_bytes(value)).hexdigest()


def validate_tqqq_core_only_p1_binding(value: Mapping[str, object]) -> dict[str, object]:
    """Reject any binding other than the P2-frozen source/config/data identity."""
    expected = build_tqqq_core_only_p1_binding()
    if not isinstance(value, Mapping) or dict(value) != expected:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only P1 binding")
    return expected


def build_tqqq_core_only_input_manifest(
    binding: Mapping[str, object],
    *,
    observed_at: str,
    producer: Mapping[str, object],
    member_bytes: bytes,
    source_content_sha256: Mapping[str, str],
) -> dict[str, object]:
    """Build the future immutable-input manifest from one already-collected member."""
    frozen = validate_tqqq_core_only_p1_binding(binding)
    if (
        not isinstance(member_bytes, bytes)
        or set(source_content_sha256) != set(_UNIVERSE)
        or any(
            not isinstance(digest, str) or not _DIGEST.fullmatch(digest)
            for digest in source_content_sha256.values()
        )
    ):
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input member")
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = binding_sha256(frozen)
    manifest = validate_research_input_manifest(
        {
            "schema_version": "research_input_manifest.v1",
            "manifest_id": f"tqqq-core-only-{binding_digest[:24]}-{hashlib.sha256(member_bytes).hexdigest()[:24]}",
            "research_input_contract_id": INPUT_CONTRACT_ID,
            "domain": "us_equity",
            "profile": CANDIDATE_ID,
            "artifact_type": "immutable_adjusted_ohlcv_etf_only",
            "observed_at": observed_at,
            "effective_at": observed_at,
            "as_of": observed_at,
            "producer": dict(producer),
            "calendar": {
                **identity["calendar"],
                "session_date": identity["date_cutoff"],
                "source_revision": binding_digest,
            },
            "adjustment": {
                **identity["adjustment"],
                "source_revision": binding_digest,
            },
            "sources": [
                {
                    "source_id": f"ibkr_adjusted_last:{symbol}",
                    "revision": binding_digest,
                    "observed_at": observed_at,
                    "content_sha256": source_content_sha256[symbol],
                }
                for symbol in sorted(_UNIVERSE)
            ],
            "members": [
                {
                    "path": "bars.json",
                    "media_type": "application/json",
                    "size_bytes": len(member_bytes),
                    "sha256": hashlib.sha256(member_bytes).hexdigest(),
                }
            ],
        }
    )
    validate_tqqq_core_only_input_manifest(manifest, frozen)
    return manifest


def validate_tqqq_core_only_input_manifest(
    manifest: Mapping[str, object], binding: Mapping[str, object]
) -> str:
    """Validate a QPK immutable-input manifest against the frozen static binding."""
    frozen = validate_tqqq_core_only_p1_binding(binding)
    try:
        validated = validate_research_input_manifest(manifest)
    except ValueError as exc:
        raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only input manifest") from exc
    identity = frozen["data_identity"]
    assert isinstance(identity, dict)
    binding_digest = binding_sha256(frozen)
    expected_source_ids = {f"ibkr_adjusted_last:{symbol}" for symbol in _UNIVERSE}
    sources = validated["sources"]
    if (
        validated["research_input_contract_id"] != INPUT_CONTRACT_ID
        or validated["domain"] != "us_equity"
        or validated["profile"] != CANDIDATE_ID
        or validated["artifact_type"] != "immutable_adjusted_ohlcv_etf_only"
        or validated["calendar"]
        != {
            **identity["calendar"],
            "session_date": identity["date_cutoff"],
            "source_revision": binding_digest,
        }
        or validated["adjustment"]
        != {
            **identity["adjustment"],
            "source_revision": binding_digest,
        }
        or {source["source_id"] for source in sources} != expected_source_ids
        or {source["revision"] for source in sources} != {binding_digest}
    ):
        raise TqqqCoreOnlyP1BindingError("TQQQ core-only input binding mismatch")
    return research_input_manifest_sha256(validated)
