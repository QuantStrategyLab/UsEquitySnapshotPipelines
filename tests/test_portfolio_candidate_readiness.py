from __future__ import annotations

from copy import deepcopy
import hashlib
import json

import pytest

from us_equity_snapshot_pipelines.research.portfolio_candidate_readiness import (
    PARKED_STATUS,
    READY_STATUS,
    PortfolioCandidateReadinessError,
    build_component_observation,
    build_portfolio_candidate_readiness,
    validate_portfolio_candidate_readiness,
)
from us_equity_snapshot_pipelines.research.strategy_candidate_registry import (
    SOXL_SOXX_CORE_ONLY_P2_V3,
    TQQQ_CORE_ONLY_P2_V5,
)


def _tqqq_p1(*, status: str = "ACCEPTED", cutoff: str = "2026-08-21") -> dict[str, object]:
    return {
        "schema_version": "qsl.tqqq-core-only-daily-p1-status.v1",
        "status": status,
        "reason_code": "" if status == "ACCEPTED" else "ALPACA_SIP_ACCESS_FORBIDDEN",
        "provider_retry_state": "NOT_TRIGGERED",
        "date_cutoff": cutoff,
        "candidate": {
            "candidate_id": TQQQ_CORE_ONLY_P2_V5.candidate_id,
            "config_sha256": TQQQ_CORE_ONLY_P2_V5.config_sha256,
        },
        "input_manifest_sha256": "a" * 64 if status == "ACCEPTED" else "",
        "p1_health_sha256": "b" * 64,
    }


def _tqqq_p3(*, cutoff: str = "2026-08-21", status: str = "EVIDENCE_V2_COMPLETE") -> dict[str, object]:
    terminal: dict[str, object] = {
        "status": status,
        "evidence_sha256": "c" * 64,
    }
    if status == "PARKED":
        terminal = {"status": "PARKED", "failure_class": "runtime_internal_failure", "stage": "P3"}
    return {
        "schema_version": "qsl.tqqq-daily-research-status.v1",
        "candidate": {
            "candidate_id": TQQQ_CORE_ONLY_P2_V5.candidate_id,
            "config_sha256": TQQQ_CORE_ONLY_P2_V5.config_sha256,
        },
        "date_cutoff": cutoff,
        "input_manifest_sha256": "a" * 64,
        "p1_health_sha256": "b" * 64,
        "p3_terminal": terminal,
    }


def _soxl_p1(*, status: str = "ACCEPTED", cutoff: str = "2026-08-21") -> dict[str, object]:
    return {
        "schema_version": "qsl.soxl-soxx-core-only-daily-p1-status.v1",
        "status": status,
        "reason_code": "" if status == "ACCEPTED" else "ALPACA_SIP_ACCESS_FORBIDDEN",
        "provider_retry_state": "NOT_TRIGGERED",
        "date_cutoff": cutoff,
        "candidate": {
            "candidate_id": SOXL_SOXX_CORE_ONLY_P2_V3.candidate_id,
            "config_sha256": SOXL_SOXX_CORE_ONLY_P2_V3.config_sha256,
        },
        "input_manifest_sha256": "d" * 64 if status == "ACCEPTED" else "",
    }


def _soxl_p3(*, status: str = "SUCCESS") -> dict[str, object]:
    if status == "PARKED":
        return {
            "schema_version": "qsl.soxl-soxx-core-only-p3-offline-run.v1",
            "status": "PARKED",
            "failure_class": "p1_p2_p3_contract_or_runtime_unavailable",
        }
    terminal: dict[str, object] = {
        "schema_version": "qsl.soxl-soxx-core-only-p3-evidence-summary.v1",
        "status": "SUCCESS",
        "p1_identity": {
            "input_manifest_sha256": "d" * 64,
            "binding_sha256": "f" * 64,
            "bars_member_sha256": "1" * 64,
            "date_cutoff": "2026-08-21",
        },
        "p2_identity": {
            "candidate_id": SOXL_SOXX_CORE_ONLY_P2_V3.candidate_id,
            "config_sha256": SOXL_SOXX_CORE_ONLY_P2_V3.config_sha256,
        },
        "materialized_input_sha256": "2" * 64,
        "evidence_plan_sha256": "3" * 64,
        "execution_identity": {"repository": "QuantStrategyLab/UsEquityStrategies"},
        "runs": [],
    }
    terminal["evidence_summary_sha256"] = hashlib.sha256(
        json.dumps(terminal, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return terminal


def _components(
    *, tqqq_p1_status: str = "ACCEPTED", soxl_p1_status: str = "ACCEPTED", soxl_cutoff: str = "2026-08-21"
) -> tuple[dict[str, str], dict[str, str]]:
    return (
        build_component_observation(
            candidate=TQQQ_CORE_ONLY_P2_V5,
            p1_terminal=_tqqq_p1(status=tqqq_p1_status),
            p3_terminal=_tqqq_p3() if tqqq_p1_status == "ACCEPTED" else None,
        ),
        build_component_observation(
            candidate=SOXL_SOXX_CORE_ONLY_P2_V3,
            p1_terminal=_soxl_p1(status=soxl_p1_status, cutoff=soxl_cutoff),
            p3_terminal=_soxl_p3() if soxl_p1_status == "ACCEPTED" else None,
        ),
    )


def test_ready_signal_is_a_deduplicable_ai_research_prompt_only() -> None:
    result = build_portfolio_candidate_readiness(
        components=_components(), observed_at="2026-08-22T05:00:00Z"
    )

    assert result["status"] == READY_STATUS
    assert result["reason_codes"] == []
    assert result["proposal"]["component_candidate_ids"] == [
        SOXL_SOXX_CORE_ONLY_P2_V3.candidate_id,
        TQQQ_CORE_ONLY_P2_V5.candidate_id,
    ]
    assert result["proposal"]["p2_freeze_authorized"] is False
    assert result["proposal"]["p6_live_authorized"] is False
    assert result["readiness_sha256"] == build_portfolio_candidate_readiness(
        components=_components(), observed_at="2026-08-22T07:00:00Z"
    )["readiness_sha256"]
    assert validate_portfolio_candidate_readiness(result) == result


def test_missing_component_p1_or_p3_evidence_parks_without_proposing_a_portfolio() -> None:
    result = build_portfolio_candidate_readiness(
        components=_components(tqqq_p1_status="DEFERRED", soxl_p1_status="DEFERRED"),
        observed_at="2026-08-22T05:00:00Z",
    )

    assert result["status"] == PARKED_STATUS
    assert result["reason_codes"] == [
        "SOXL_P1_NOT_ACCEPTED",
        "SOXL_P3_EVIDENCE_INCOMPLETE",
        "TQQQ_P1_NOT_ACCEPTED",
        "TQQQ_P3_EVIDENCE_INCOMPLETE",
    ]


def test_mismatched_cutoffs_park_even_when_both_component_p3_results_are_complete() -> None:
    result = build_portfolio_candidate_readiness(
        components=_components(soxl_cutoff="2026-08-20"), observed_at="2026-08-22T05:00:00Z"
    )

    assert result["status"] == PARKED_STATUS
    assert result["reason_codes"] == ["COMPONENT_CUTOFF_MISMATCH"]


def test_tampered_authority_or_component_digest_is_rejected_fail_closed() -> None:
    result = build_portfolio_candidate_readiness(
        components=_components(), observed_at="2026-08-22T05:00:00Z"
    )
    tampered = deepcopy(result)
    tampered["proposal"]["p4_paper_authorized"] = True

    with pytest.raises(PortfolioCandidateReadinessError):
        validate_portfolio_candidate_readiness(tampered)

    tampered = deepcopy(result)
    tampered["components"][0]["candidate_sha256"] = "f" * 64
    with pytest.raises(PortfolioCandidateReadinessError):
        validate_portfolio_candidate_readiness(tampered)


def test_component_p3_records_must_bind_the_current_p1_input() -> None:
    p3 = _tqqq_p3()
    p3["p1_health_sha256"] = "d" * 64
    with pytest.raises(PortfolioCandidateReadinessError, match="health mismatch"):
        build_component_observation(
            candidate=TQQQ_CORE_ONLY_P2_V5,
            p1_terminal=_tqqq_p1(),
            p3_terminal=p3,
        )

    p3 = _soxl_p3()
    p3["p1_identity"] = dict(p3["p1_identity"], date_cutoff="2026-08-20")
    evidence = {key: value for key, value in p3.items() if key != "evidence_summary_sha256"}
    p3["evidence_summary_sha256"] = hashlib.sha256(
        json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    with pytest.raises(PortfolioCandidateReadinessError, match="identity mismatch"):
        build_component_observation(
            candidate=SOXL_SOXX_CORE_ONLY_P2_V3,
            p1_terminal=_soxl_p1(),
            p3_terminal=p3,
        )
