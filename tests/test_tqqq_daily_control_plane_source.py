from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.tqqq_core_only_p1_binding import P2_V5_CONTRACT
from us_equity_snapshot_pipelines.lifecycle.tqqq_daily_control_plane_source import (
    SOURCE_ID,
    SOURCE_SCHEMA_VERSION,
    TqqqDailyControlPlaneSourceError,
    build_tqqq_daily_control_plane_source_snapshot,
)
from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_evidence_index import P3_STATUS


NOW = "2026-08-19T03:00:00Z"
REVISION = "a" * 40
MANIFEST = "b" * 64
EVIDENCE = "c" * 64


def _build(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "computed_at": NOW,
        "source_revision": REVISION,
        "p1_status": "ACCEPTED",
        "p1_reason_code": "",
        "p1_provider_retry_state": "NOT_TRIGGERED",
        "p1_manifest_sha256": MANIFEST,
        "p2_config_sha256": P2_V5_CONTRACT.config_sha256,
        "p3_status": P3_STATUS,
        "p3_evidence_sha256": EVIDENCE,
        "p3_failure_class": "",
    }
    values.update(overrides)
    return build_tqqq_daily_control_plane_source_snapshot(**values)


def test_completed_p3_publishes_only_bound_research_summary() -> None:
    snapshot = _build()

    assert snapshot["schema_version"] == SOURCE_SCHEMA_VERSION
    assert snapshot["source_id"] == SOURCE_ID
    assert snapshot["data_status"] == "ready"
    assert snapshot["errors"] == []
    candidate = snapshot["candidates"][0]
    assert candidate == {
        "candidate_id": "tqqq_core_only_p2_v5",
        "candidate_kind": "individual",
        "domain": "us_equity",
        "lifecycle": {"stage": "P3", "status": "verified"},
        "evidence": {
            "p1_input_digest": MANIFEST,
            "p2_config_digest": P2_V5_CONTRACT.config_sha256,
            "p3_evidence_id": EVIDENCE,
            "source_revision": REVISION,
        },
        "recommendation": {
            "code": "keep_research",
            "reason": (
                "P3 evidence completed; candidate remains research-only. "
                "P1 input was acquired without a 403 retry."
            ),
        },
        "freshness": {"status": "fresh", "age_seconds": 0},
    }


def test_deferred_p1_stops_before_p3_and_does_not_publish_digests() -> None:
    snapshot = _build(
        p1_status="DEFERRED",
        p1_reason_code="INPUT_UNAVAILABLE",
        p1_manifest_sha256="",
        p3_status="",
        p3_evidence_sha256="",
    )

    candidate = snapshot["candidates"][0]
    assert candidate["lifecycle"] == {"stage": "P1", "status": "deferred"}
    assert candidate["evidence"]["p1_input_digest"] is None
    assert candidate["evidence"]["p3_evidence_id"] is None
    assert candidate["recommendation"]["code"] == "defer"
    assert candidate["recommendation"]["reason"] == (
        "P1 deferred: input_unavailable; retry on the next scheduled session. "
        "P1 provider 403 retry was not triggered."
    )
    assert snapshot["errors"] == ["p1_deferred_input_unavailable"]


def test_deferred_missing_sessions_is_distinct_from_an_unavailable_provider() -> None:
    snapshot = _build(
        p1_status="DEFERRED",
        p1_reason_code="MISSING_SESSIONS",
        p1_manifest_sha256="",
        p3_status="",
        p3_evidence_sha256="",
    )

    candidate = snapshot["candidates"][0]
    assert candidate["recommendation"]["reason"] == (
        "P1 deferred: missing_sessions; retry on the next scheduled session. "
        "P1 provider 403 retry was not triggered."
    )
    assert snapshot["errors"] == ["p1_deferred_missing_sessions"]


@pytest.mark.parametrize(
    ("reason_code", "provider_retry_state", "expected_recommendation", "expected_error"),
    [
        (
            "ALPACA_RATE_LIMITED",
            "NOT_TRIGGERED",
            {
                "code": "defer",
                "reason": (
                    "P1 deferred: alpaca_rate_limited; retry on the next scheduled session. "
                    "P1 provider 403 retry was not triggered."
                ),
            },
            "p1_deferred_alpaca_rate_limited",
        ),
        (
            "ALPACA_AUTHENTICATION_FAILED",
            "NOT_TRIGGERED",
            {
                "code": "defer",
                "reason": (
                    "P1 deferred: alpaca_authentication_failed; verify the non-live Alpaca key pair. "
                    "P1 provider 403 retry was not triggered."
                ),
            },
            "p1_deferred_operator_attention_alpaca_authentication_failed",
        ),
        (
            "ALPACA_SIP_ACCESS_FORBIDDEN",
            "SIP_403_EXHAUSTED",
            {
                "code": "defer",
                "reason": (
                    "P1 deferred: alpaca_sip_access_forbidden; verify SIP market-data access "
                    "and request configuration. P1 provider request exhausted its one same-request 403 retry."
                ),
            },
            "p1_deferred_operator_attention_alpaca_sip_access_forbidden",
        ),
    ],
)
def test_provider_reason_drives_retry_or_operator_attention(
    reason_code: str,
    provider_retry_state: str,
    expected_recommendation: dict[str, str],
    expected_error: str,
) -> None:
    snapshot = _build(
        p1_status="DEFERRED",
        p1_reason_code=reason_code,
        p1_provider_retry_state=provider_retry_state,
        p1_manifest_sha256="",
        p3_status="",
        p3_evidence_sha256="",
    )

    assert snapshot["candidates"][0]["recommendation"] == expected_recommendation
    assert snapshot["errors"] == [expected_error]


def test_quarantined_p1_is_parked_without_p3() -> None:
    snapshot = _build(
        p1_status="QUARANTINED",
        p1_reason_code="P1_CONTRACT_FAILURE",
        p1_manifest_sha256="",
        p3_status="",
        p3_evidence_sha256="",
    )

    candidate = snapshot["candidates"][0]
    assert candidate["lifecycle"] == {"stage": "P1", "status": "parked"}
    assert candidate["recommendation"]["code"] == "park"
    assert snapshot["errors"] == ["p1_quarantined_p1_contract_failure"]


def test_accepted_p1_with_sanitized_parked_p3_remains_parked() -> None:
    snapshot = _build(
        p3_status="PARKED",
        p3_evidence_sha256="",
        p3_failure_class="runtime_internal_failure",
    )

    candidate = snapshot["candidates"][0]
    assert candidate["lifecycle"] == {"stage": "P3", "status": "parked"}
    assert candidate["recommendation"] == {
        "code": "park",
        "reason": "P3 parked: runtime_internal_failure. P1 input was acquired without a 403 retry.",
    }
    assert snapshot["errors"] == ["p3_parked"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"p2_config_sha256": "d" * 64}, "unexpected P2 v5 config digest"),
        ({"p1_manifest_sha256": "", "p3_status": P3_STATUS}, "accepted P1 requires a manifest digest"),
        (
            {
                "p1_status": "DEFERRED",
                "p1_reason_code": "INPUT_UNAVAILABLE",
                "p1_manifest_sha256": "",
                "p3_status": "PARKED",
                "p3_evidence_sha256": "",
                "p3_failure_class": "runtime_internal_failure",
            },
            "deferred P1 cannot carry a P3 result",
        ),
        (
            {"p3_status": "PARKED", "p3_evidence_sha256": "", "p3_failure_class": "not_safe"},
            "parked P3 requires a sanitized failure class",
        ),
        (
            {
                "p1_status": "DEFERRED",
                "p1_manifest_sha256": "",
                "p3_status": "",
                "p3_evidence_sha256": "",
            },
            "invalid deferred P1 reason code",
        ),
        ({"p1_provider_retry_state": "unknown"}, "invalid P1 provider retry state"),
        (
            {"p1_provider_retry_state": "SIP_403_EXHAUSTED"},
            "exhausted P1 retry cannot be accepted",
        ),
    ],
)
def test_invalid_or_misbound_terminal_states_fail_closed(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(TqqqDailyControlPlaneSourceError, match=message):
        _build(**overrides)


def test_recovered_p1_403_retry_is_visible_without_changing_research_authority() -> None:
    snapshot = _build(p1_provider_retry_state="SIP_403_RECOVERED")

    candidate = snapshot["candidates"][0]
    assert candidate["lifecycle"] == {"stage": "P3", "status": "verified"}
    assert candidate["recommendation"] == {
        "code": "keep_research",
        "reason": (
            "P3 evidence completed; candidate remains research-only. "
            "P1 provider request recovered after one or more same-request 403 retries."
        ),
    }
    assert snapshot["errors"] == []
