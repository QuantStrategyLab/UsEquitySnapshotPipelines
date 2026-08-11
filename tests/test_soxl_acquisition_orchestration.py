from __future__ import annotations

import hashlib
import subprocess
from dataclasses import replace
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
    orchestrate_existing_soxl_snapshot,
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
FRESH_AUTHORITY_SHA256 = "1" * 64


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


def _fresh_authority() -> SoxlOrchestrationAuthority:
    return replace(_authority(), authority_receipt_sha256=FRESH_AUTHORITY_SHA256)


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


def _published_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> tuple[Path, str]:
    def run(*, output_dir, **_kwargs):
        output = Path(output_dir)
        output.mkdir()
        evidence = output / "strategy-evidence-package.v2.json"
        evidence.write_text('{"schema_version":"strategy_evidence_package.v2"}')
        return {
            "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "cost_stress_25bp_sha256": "4" * 64,
            "promotion_result_sha256": "5" * 64,
        }

    monkeypatch.setattr(orchestration, "run_soxl_promotion_research", run)
    monkeypatch.setattr(orchestration, "validate_evidence_package_v2", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        orchestration.promotion_runner,
        "_resolve_runner_revision",
        lambda: RUNNER_REVISION,
    )
    result = orchestrate_soxl_promotion(
        _results(),
        authority=_authority(),
        output_root=tmp_path / "acquisition",
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        clock=lambda: datetime(2026, 8, 10, 14, 0, tzinfo=UTC),
    )
    snapshot = tmp_path / "acquisition" / next((tmp_path / "acquisition").iterdir()).name / "snapshot"
    return snapshot, result["snapshot_digest"]


def _allow_pinned_dependency_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        orchestration,
        "_installed_vcs_revision",
        lambda distribution_name: {
            "quant-platform-kit": orchestration.QPK_REVISION,
            "us-equity-strategies": orchestration.promotion_runner._UES_REVISION,
        }[distribution_name],
    )


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


def test_existing_snapshot_validates_then_consumes_one_fresh_mandate_and_runs_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)
    events = []

    class Guard:
        def __init__(self, database, *, clock):
            self._guard = ResearchMandateAuthorityGuard(database, clock=clock)

        def issue(self, **kwargs):
            events.append("issue")
            return self._guard.issue(**kwargs)

        def consume(self, mandate, **kwargs):
            events.append("consume")
            return self._guard.consume(mandate, **kwargs)

    def run(*, input_payload, config_payload, output_dir, generated_at=None):
        events.append("run")
        assert config_payload["candidate_identity"]["authority_receipt_sha256"] == (
            FRESH_AUTHORITY_SHA256
        )
        assert config_payload["mandate_provenance"][
            "research_mandate_consumption_receipt_sha256"
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

    monkeypatch.setattr(orchestration, "ResearchMandateAuthorityGuard", Guard)
    monkeypatch.setattr(orchestration, "run_soxl_promotion_research", run)
    monkeypatch.setattr(orchestration, "validate_evidence_package_v2", validate)

    result = orchestrate_existing_soxl_snapshot(
        snapshot,
        expected_snapshot_digest=snapshot_digest,
        authority=_fresh_authority(),
        output_root=tmp_path / "snapshot-rerun",
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        clock=lambda: datetime(2026, 8, 10, 15, 0, tzinfo=UTC),
    )

    assert events == ["issue", "consume", "run", "validate"]
    assert result["status"] == "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE"
    assert result["snapshot_digest"] == snapshot_digest
    assert result["rerun_count"] == 1


def test_existing_snapshot_compatible_ancestor_binds_fresh_current_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)
    current_revision = "9" * 40
    current_tree = "8" * 40
    compatibility_calls = []
    observed_config = {}

    monkeypatch.setattr(
        orchestration,
        "_require_snapshot_execution_compatibility",
        lambda snapshot_revision, snapshot_tree, runner_revision, runner_tree: (
            compatibility_calls.append(
                (snapshot_revision, snapshot_tree, runner_revision, runner_tree)
            )
        ),
        raising=False,
    )

    def run(*, config_payload, output_dir, **_kwargs):
        observed_config.update(config_payload)
        output = Path(output_dir)
        output.mkdir()
        evidence = output / "strategy-evidence-package.v2.json"
        evidence.write_text('{"schema_version":"strategy_evidence_package.v2"}')
        return {
            "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "cost_stress_25bp_sha256": "4" * 64,
            "promotion_result_sha256": "5" * 64,
        }

    monkeypatch.setattr(orchestration, "run_soxl_promotion_research", run)
    monkeypatch.setattr(
        orchestration,
        "validate_evidence_package_v2",
        lambda *_args, **_kwargs: [],
    )

    result = orchestrate_existing_soxl_snapshot(
        snapshot,
        expected_snapshot_digest=snapshot_digest,
        authority=_fresh_authority(),
        output_root=tmp_path / "compatible-rerun",
        runner_revision=current_revision,
        runner_tree_sha=current_tree,
        clock=lambda: datetime(2026, 8, 10, 15, 0, tzinfo=UTC),
    )

    assert compatibility_calls == [
        (RUNNER_REVISION, RUNNER_TREE_SHA, current_revision, current_tree)
    ]
    assert observed_config["runner_revision"] == current_revision
    assert observed_config["candidate_identity"]["runner_revision"] == current_revision
    assert observed_config["mandate_provenance"]["runner_revision"] == current_revision
    assert result["status"] == "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE"
    assert result["rerun_count"] == 1


def test_existing_snapshot_config_mismatch_fails_before_mandate_or_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)
    monkeypatch.setattr(
        orchestration,
        "ResearchMandateAuthorityGuard",
        lambda *_args, **_kwargs: pytest.fail("mandate must not be created"),
    )
    monkeypatch.setattr(
        orchestration,
        "run_soxl_promotion_research",
        lambda **_kwargs: pytest.fail("runner must not be invoked"),
    )

    with pytest.raises(SoxlOrchestrationError, match="config identity mismatch"):
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=replace(_fresh_authority(), risk_standard_id="changed"),
            output_root=tmp_path / "snapshot-rerun",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )

    assert not (tmp_path / "snapshot-rerun").exists()


def test_existing_snapshot_revision_and_installed_provenance_mismatch_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)

    def reject_incompatible_source(*_args):
        raise SoxlOrchestrationError("snapshot execution source mismatch")

    monkeypatch.setattr(
        orchestration,
        "_require_snapshot_execution_compatibility",
        reject_incompatible_source,
    )

    with pytest.raises(SoxlOrchestrationError, match="execution source mismatch"):
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "revision-rerun",
            runner_revision="9" * 40,
            runner_tree_sha=RUNNER_TREE_SHA,
        )
    assert not (tmp_path / "revision-rerun").exists()

    monkeypatch.undo()
    _allow_pinned_dependency_provenance(monkeypatch)

    monkeypatch.setattr(orchestration, "_installed_vcs_revision", lambda _name: "8" * 40)
    with pytest.raises(SoxlOrchestrationError, match="installed dependency revision mismatch"):
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "provenance-rerun",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )


def test_existing_snapshot_current_non_runner_contract_change_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)
    current_revision = "9" * 40
    real_builder = orchestration._config_without_authority

    monkeypatch.setattr(
        orchestration,
        "_require_snapshot_execution_compatibility",
        lambda *_args: None,
    )

    def changed_current_config(*, source_contract_sha256, runner_revision, authority):
        config = real_builder(
            source_contract_sha256=source_contract_sha256,
            runner_revision=runner_revision,
            authority=authority,
        )
        if runner_revision == current_revision:
            config["benchmark_symbol"] = "QQQ"
        return config

    monkeypatch.setattr(orchestration, "_config_without_authority", changed_current_config)
    monkeypatch.setattr(
        orchestration,
        "ResearchMandateAuthorityGuard",
        lambda *_args, **_kwargs: pytest.fail("mandate must not be created"),
    )

    with pytest.raises(SoxlOrchestrationError, match="execution contract mismatch"):
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "contract-rerun",
            runner_revision=current_revision,
            runner_tree_sha="8" * 40,
        )
    assert not (tmp_path / "contract-rerun").exists()


def test_existing_snapshot_same_revision_wrong_current_tree_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)
    monkeypatch.setattr(
        orchestration,
        "ResearchMandateAuthorityGuard",
        lambda *_args, **_kwargs: pytest.fail("mandate must not be created"),
    )

    with pytest.raises(SoxlOrchestrationError, match="current runner tree mismatch"):
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "tree-rerun",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha="8" * 40,
        )
    assert not (tmp_path / "tree-rerun").exists()


def test_snapshot_execution_compatibility_requires_ancestor_and_fixed_blob_equality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_revision = "1" * 40
    current_revision = "2" * 40
    snapshot_tree = "3" * 40
    current_tree = "4" * 40
    state = {"ancestor": True, "mismatch_path": None}
    compared_paths = []

    def run(arguments, *, check, capture_output, text):
        assert capture_output is True
        assert text is True
        git_arguments = arguments[3:]
        if git_arguments[:2] == ["merge-base", "--is-ancestor"]:
            return subprocess.CompletedProcess(
                arguments,
                0 if state["ancestor"] else 1,
                "",
                "",
            )
        assert check is True
        assert git_arguments[0] == "rev-parse"
        identity = git_arguments[1]
        if identity == f"{snapshot_revision}^{{tree}}":
            value = snapshot_tree
        elif identity == f"{current_revision}^{{tree}}":
            value = current_tree
        else:
            revision, path = identity.split(":", maxsplit=1)
            if revision == snapshot_revision:
                compared_paths.append(path)
            value = (
                "6" * 40
                if path == state["mismatch_path"] and revision == current_revision
                else "5" * 40
            )
        return subprocess.CompletedProcess(arguments, 0, value + "\n", "")

    monkeypatch.setattr(orchestration.subprocess, "run", run)
    orchestration._require_snapshot_execution_compatibility(
        snapshot_revision,
        snapshot_tree,
        current_revision,
        current_tree,
    )
    assert compared_paths == list(orchestration._SNAPSHOT_EXECUTION_CONTRACT_PATHS)

    state["mismatch_path"] = orchestration._SNAPSHOT_EXECUTION_CONTRACT_PATHS[0]
    with pytest.raises(SoxlOrchestrationError, match="execution source mismatch"):
        orchestration._require_snapshot_execution_compatibility(
            snapshot_revision,
            snapshot_tree,
            current_revision,
            current_tree,
        )

    state["mismatch_path"] = None
    state["ancestor"] = False
    with pytest.raises(SoxlOrchestrationError, match="not a current ancestor"):
        orchestration._require_snapshot_execution_compatibility(
            snapshot_revision,
            snapshot_tree,
            current_revision,
            current_tree,
        )


def test_existing_snapshot_tamper_fails_before_mandate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)
    input_path = snapshot / "input.json"
    input_path.write_bytes(input_path.read_bytes() + b" ")
    input_path.chmod(0o600)
    monkeypatch.setattr(
        orchestration,
        "ResearchMandateAuthorityGuard",
        lambda *_args, **_kwargs: pytest.fail("mandate must not be created"),
    )

    with pytest.raises(SoxlOrchestrationError, match="snapshot member integrity mismatch"):
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "snapshot-rerun",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )


def test_existing_snapshot_mandate_failure_stops_before_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)

    class Guard:
        def __init__(self, *_args, **_kwargs):
            pass

        def issue(self, **_kwargs):
            raise ValueError("private mandate failure")

    monkeypatch.setattr(orchestration, "ResearchMandateAuthorityGuard", Guard)
    monkeypatch.setattr(
        orchestration,
        "run_soxl_promotion_research",
        lambda **_kwargs: pytest.fail("runner must not be invoked"),
    )

    with pytest.raises(SoxlOrchestrationError, match="snapshot-only mandate failed") as caught:
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "snapshot-rerun",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )
    assert "private mandate failure" not in str(caught.value)


def test_existing_snapshot_runner_failure_retains_sanitized_committed_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    snapshot, snapshot_digest = _published_snapshot(monkeypatch, tmp_path)
    _allow_pinned_dependency_provenance(monkeypatch)

    def fail_before_first_artifact(*, output_dir, **_kwargs):
        evidence_root = Path(output_dir)
        evidence_root.mkdir()
        (evidence_root / "artifacts").mkdir()
        raise RuntimeError("private snapshot-only runner failure")

    monkeypatch.setattr(orchestration, "run_soxl_promotion_research", fail_before_first_artifact)

    with pytest.raises(SoxlOrchestrationError, match="promotion rerun failed") as caught:
        orchestrate_existing_soxl_snapshot(
            snapshot,
            expected_snapshot_digest=snapshot_digest,
            authority=_fresh_authority(),
            output_root=tmp_path / "snapshot-rerun",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            clock=lambda: datetime(2026, 8, 10, 15, 0, tzinfo=UTC),
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
        "snapshot_digest": snapshot_digest,
        "stage": "promotion_runner_pre_evidence",
    }
    assert "private snapshot-only runner failure" not in str(caught.value)
