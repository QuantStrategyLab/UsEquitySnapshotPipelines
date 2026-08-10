from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from quant_platform_kit.data.research_mandate import ResearchMandateAuthorityGuard
from quant_platform_kit.ibkr import StrictAdjustedHistoryResult
from quant_platform_kit.ibkr.market_data import (
    AdjustedHistoricalCandle,
    StrictAdjustedHistoryDiagnostic,
    StrictAdjustedHistoryProvenance,
)

import us_equity_snapshot_pipelines.lifecycle.soxl_acquisition_orchestration as orchestration
from us_equity_snapshot_pipelines.lifecycle.soxl_acquisition_orchestration import (
    SoxlOrchestrationAuthority,
    SoxlOrchestrationError,
    orchestrate_soxl_promotion,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_pit_input_packager import (
    FIRST_ELIGIBLE_SESSION,
    FROZEN_XNYS_SESSIONS,
    SOXL_PROMOTION_ASSETS,
)

RUNNER_REVISION = "a" * 40
RUNNER_TREE_SHA = "b" * 40
AUTHORITY_SHA256 = "c" * 64


def _authority() -> SoxlOrchestrationAuthority:
    return SoxlOrchestrationAuthority(
        authority_receipt_sha256=AUTHORITY_SHA256,
        entitlement_receipt_sha256="d" * 64,
        license_receipt_sha256="e" * 64,
        retention_expires_at="2026-12-31T00:00:00Z",
        risk_standard_id="soxl_p3_candidate_bound_v1",
        risk_standard_sha256="f" * 64,
        input_license="authority-bound private internal research",
        input_usage_scope="non-commercial internal research",
    )


def _results() -> dict[str, StrictAdjustedHistoryResult]:
    results = {}
    for symbol in SOXL_PROMOTION_ASSETS:
        sessions = tuple(
            date.fromisoformat(value)
            for value in FROZEN_XNYS_SESSIONS
            if value >= FIRST_ELIGIBLE_SESSION.get(symbol, FROZEN_XNYS_SESSIONS[0])
        )
        candles = tuple(
            AdjustedHistoricalCandle(
                session=session,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1_000_000.0,
            )
            for session in sessions
        )
        results[symbol] = StrictAdjustedHistoryResult(
            candles=candles,
            provenance=StrictAdjustedHistoryProvenance(
                symbol=symbol,
                exchange="SMART",
                currency="USD",
                end_datetime="2026-08-05T03:59:59Z",
                duration={
                    "SOXL": "9 Y",
                    "SOXX": "9 Y",
                    "BOXX": "4 Y",
                    "SCHD": "9 Y",
                    "DGRO": "9 Y",
                    "SGOV": "7 Y",
                    "SPYI": "4 Y",
                    "QQQI": "3 Y",
                    "QQQ": "9 Y",
                }[symbol],
                bar_size="1 day",
                what_to_show="ADJUSTED_LAST",
                use_rth=True,
                format_date=1,
                keep_up_to_date=False,
                returned_row_count=len(candles),
            ),
            diagnostic=StrictAdjustedHistoryDiagnostic(
                classification="exact_match",
                completion_observed=True,
                expected_count=len(candles),
                observed_in_window_count=len(candles),
                missing_count=0,
                extra_count=0,
                duplicate_count=0,
                missing_sessions_sha256="0" * 64,
                extra_sessions_sha256="0" * 64,
                duplicate_sessions_sha256="0" * 64,
                provider_error_code_counts=(),
            ),
        )
    return results


def test_exact_results_publish_content_addressed_snapshot_then_consume_one_rerun(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events = []
    prepared = {}
    real_prepare = orchestration.prepare_soxl_pit_input
    real_publish = orchestration.publish_soxl_pit_input

    def prepare(raw_sessions, source_contract, *, trusted_regime_source_contract_sha256):
        events.append("prepare")
        assert len(raw_sessions) == len(FROZEN_XNYS_SESSIONS) == 2_010
        assert tuple(raw_sessions[0]["bars"]) == ("SOXL", "SOXX", "SCHD", "DGRO", "QQQ")
        assert tuple(raw_sessions[-1]["bars"]) == SOXL_PROMOTION_ASSETS
        assert source_contract["data_class"] == "provider_observed"
        assert source_contract["input_content_sha256"] == hashlib.sha256(
            orchestration.canonical_json_bytes(raw_sessions)
        ).hexdigest()
        assert trusted_regime_source_contract_sha256 == hashlib.sha256(
            orchestration.canonical_json_bytes(source_contract)
        ).hexdigest()
        prepared["value"] = real_prepare(
            raw_sessions,
            source_contract,
            trusted_regime_source_contract_sha256=trusted_regime_source_contract_sha256,
        )
        return prepared["value"]

    class Guard:
        def __init__(self, database, *, clock):
            self._guard = ResearchMandateAuthorityGuard(database, clock=clock)

        def issue(self, **kwargs):
            events.append("issue")
            return self._guard.issue(**kwargs)

        def consume(self, mandate, **kwargs):
            events.append("consume")
            return self._guard.consume(mandate, **kwargs)

    def publish(prepared_input, binding, output_dir):
        events.append("publish")
        assert prepared_input is prepared["value"]
        assert binding["input_manifest_sha256"] == prepared_input.input_manifest_sha256
        assert binding["mandate_digest_sha256"] != "0" * 64
        return real_publish(prepared_input, binding, output_dir)

    def run(*, input_payload, config_payload, output_dir, generated_at=None):
        events.append("run")
        assert input_payload["schema_version"] == "soxl_p3_core_only_9_input.v1"
        candidate = config_payload["candidate_identity"]
        mandate = config_payload["mandate_provenance"]
        assert candidate["input_manifest_sha256"] == prepared["value"].input_manifest_sha256
        assert candidate["authority_receipt_sha256"] == AUTHORITY_SHA256
        assert mandate["candidate_identity_sha256"] != "0" * 64
        assert mandate["effective_at"].endswith("Z")
        assert mandate["expires_at"].endswith("Z")
        validated = orchestration.promotion_runner.SoxlPromotionRunner(
            input_payload,
            config_payload,
            variant_id="explicit_qqq_fallback",
        )
        assert validated.candidate_identity.candidate_sha256 == mandate[
            "candidate_identity_sha256"
        ]
        output = Path(output_dir)
        output.mkdir()
        evidence = output / "strategy-evidence-package.v2.json"
        evidence.write_text('{"schema_version":"strategy_evidence_package.v2"}')
        return {
            "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "cost_stress_25bp_sha256": "4" * 64,
            "promotion_result_sha256": "5" * 64,
        }

    def validate(evidence, *, base_dir):
        events.append("validate")
        assert evidence["schema_version"] == "strategy_evidence_package.v2"
        assert Path(base_dir).name == "evidence"
        return []

    monkeypatch.setattr(orchestration, "prepare_soxl_pit_input", prepare)
    monkeypatch.setattr(orchestration, "ResearchMandateAuthorityGuard", Guard)
    monkeypatch.setattr(orchestration, "publish_soxl_pit_input", publish)
    monkeypatch.setattr(orchestration, "run_soxl_promotion_research", run)
    monkeypatch.setattr(orchestration, "validate_evidence_package_v2", validate)
    monkeypatch.setattr(
        orchestration.promotion_runner,
        "_resolve_runner_revision",
        lambda: RUNNER_REVISION,
    )

    result = orchestrate_soxl_promotion(
        _results(),
        authority=_authority(),
        output_root=tmp_path / "runs",
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        clock=lambda: datetime(2026, 8, 10, 14, 0, tzinfo=UTC),
    )

    assert events == ["prepare", "issue", "publish", "consume", "run", "validate"]
    assert result == {
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
        "asset_count": 9,
        "snapshot_digest": result["snapshot_digest"],
        "evidence_digest": result["evidence_digest"],
        "mandate_receipt_digest": result["mandate_receipt_digest"],
        "rerun_count": 1,
    }
    assert len(result["snapshot_digest"]) == 64
    assert len(result["evidence_digest"]) == 64
    assert len(result["mandate_receipt_digest"]) == 64
    run_root = tmp_path / "runs" / prepared["value"].input_manifest_sha256
    assert run_root.is_dir()
    assert run_root.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in run_root.rglob("*") if path.is_file())


def test_incomplete_results_fail_before_snapshot_nonce_or_rerun(tmp_path: Path) -> None:
    results = _results()
    del results["QQQ"]

    with pytest.raises(SoxlOrchestrationError, match="exact nine-input result"):
        orchestrate_soxl_promotion(
            results,
            authority=_authority(),
            output_root=tmp_path / "runs",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )

    assert not (tmp_path / "runs").exists()


def test_post_snapshot_runner_failure_retains_only_sanitized_committed_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_before_first_artifact(*, output_dir, **_kwargs):
        evidence_root = Path(output_dir)
        evidence_root.mkdir()
        (evidence_root / "artifacts").mkdir()
        raise orchestration.SoxlPromotionContractError(
            "private runner exception must not serialize"
        )

    monkeypatch.setattr(
        orchestration,
        "run_soxl_promotion_research",
        fail_before_first_artifact,
    )
    monkeypatch.setattr(
        orchestration.promotion_runner,
        "_resolve_runner_revision",
        lambda: RUNNER_REVISION,
    )

    with pytest.raises(SoxlOrchestrationError, match="promotion rerun failed") as caught:
        orchestrate_soxl_promotion(
            _results(),
            authority=_authority(),
            output_root=tmp_path / "runs",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            clock=lambda: datetime(2026, 8, 10, 14, 0, tzinfo=UTC),
        )

    failure = caught.value.sanitized_failure
    assert failure == {
        "backtest_orchestrator_invocation_count": None,
        "classification": "promotion_rerun_failed",
        "evidence_artifact_count": 0,
        "mandate_digest": failure["mandate_digest"],
        "mandate_receipt_digest": failure["mandate_receipt_digest"],
        "risk_engine_assessment_count": None,
        "runner_completion_count": 0,
        "runner_invocation_count": 1,
        "snapshot_digest": failure["snapshot_digest"],
        "stage": "promotion_runner_pre_evidence",
    }
    assert len(failure["snapshot_digest"]) == 64
    assert len(failure["mandate_digest"]) == 64
    assert len(failure["mandate_receipt_digest"]) == 64
    assert "private runner exception" not in str(caught.value)
