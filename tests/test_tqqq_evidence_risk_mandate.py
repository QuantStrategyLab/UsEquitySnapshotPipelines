from __future__ import annotations

import hashlib
import json
import sqlite3
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import us_equity_snapshot_pipelines.lifecycle.tqqq_evidence_risk_mandate as mandate
import quant_platform_kit.risk.gate as qpk_gate
from quant_platform_kit.risk.contracts import CandidateRiskIdentity
from quant_platform_kit.risk.contracts import RiskAction
from quant_platform_kit.common.strategy_contracts import PositionTarget, StrategyDecision


AUTHORITY_DIGEST = "c0c5020fbe64057b735f987b3bcc490dfe708304b58f01d57cd581344afb44c8"
AUTHORITY_REVISION = "ca259ebde6967309771d61f75af33d036239678a"


@pytest.fixture(autouse=True)
def _canonical_authority_ledger(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ledger_root = tmp_path / "authority-ledger"
    ledger_root.mkdir(mode=0o700)
    ledger_root.chmod(0o700)
    ledger_path = ledger_root / f"{AUTHORITY_DIGEST}.sqlite3"
    ledger_path.touch(mode=0o600)
    ledger_path.chmod(0o600)
    monkeypatch.setattr(
        mandate,
        "CANONICAL_AUTHORITY_LEDGER_PATH",
        ledger_path,
    )


def _authority_payload() -> dict[str, object]:
    return {
        "allowed_tradable_assets": ["BOXX", "QQQM", "TQQQ"],
        "authority_role": "risk-authority",
        "authority_scope": "RESEARCH_ONLY",
        "authority_store": "QuantStrategyLab/QuantRuntimeSettings",
        "benchmark_only_assets": ["QQQ"],
        "capital_scope": "allocated_sleeve",
        "currency": "USD",
        "decided_at": "2026-09-02T05:23:04Z",
        "decided_by": "QuantStrategyLab human authority",
        "decision": "APPROVE",
        "decision_id": "QSL-RISK-20260902-001",
        "decision_source": "explicit_user_authorization_in_codex_thread",
        "drawdown_scalars": [
            {"lower_exclusive": None, "scalar": "1.0", "upper_inclusive": "0.05"},
            {"lower_exclusive": "0.05", "scalar": "0.5", "upper_inclusive": "0.10"},
            {
                "lower_exclusive": "0.10",
                "outcome": "PARK",
                "scalar": "0.0",
                "upper_inclusive": None,
            },
        ],
        "effective_product_caps": {"BOXX": "0.50", "QQQM": "0.50", "TQQQ": "0.45"},
        "execution_constraints": {
            "no_live": True,
            "no_order": True,
            "no_paper": True,
            "no_promotion_authority": True,
            "no_shadow": True,
        },
        "fx_conversion_allowed": False,
        "integrity": "sha256_sidecar_and_immutable_repository_revision",
        "leverage_factors": {"BOXX": "1", "QQQM": "1", "TQQQ": "3"},
        "loss_budget_equity_reference": "completed_session_equity",
        "loss_budget_fraction": "0.01",
        "mandate_validity_seconds": 300,
        "max_nonzero_assets": 3,
        "modeled_stress_is_not_stop_order": True,
        "modeled_stress_loss_distance": {"BOXX": "0.05", "QQQM": "0.05", "TQQQ": "0.05"},
        "nominal_product_caps": {"BOXX": "0.50", "QQQM": "0.50", "TQQQ": "0.15"},
        "policy_bundle": "CONSERVATIVE_RESEARCH_V1",
        "prohibited": [
            "runner_or_workflow_self_signature",
            "legacy_p1_p3_mandate_reuse",
            "provider_or_replay_reacquisition",
            "cloud_write",
            "paper_or_live_execution",
        ],
        "purpose": "TQQQ_CANDIDATE_RESEARCH_EVIDENCE_ONLY",
        "runner_is_authority": False,
        "schema_version": "qsl.human-authority-receipt.v1",
        "signature": None,
        "single_consumption": True,
        "snapshot_max_age_seconds": 300,
        "source_revision_binding": "immutable_merge_commit_external_to_receipt",
        "total_effective_exposure_cap": "0.50",
        "valuation_basis": "allocated_sleeve_ledger",
    }


def _write_authority(root: Path) -> Path:
    path = root / "tqqq-conservative-research-v1.json"
    payload = (json.dumps(_authority_payload(), indent=2, sort_keys=True) + "\n").encode()
    assert hashlib.sha256(payload).hexdigest() == AUTHORITY_DIGEST
    path.write_bytes(payload)
    path.with_suffix(".json.sha256").write_text(
        f"{AUTHORITY_DIGEST}  {path.name}\n", encoding="utf-8"
    )
    return path


def _candidate() -> CandidateRiskIdentity:
    return CandidateRiskIdentity(
        strategy_profile="tqqq_core_parity_v1",
        account_mode="single_strategy_account_v1",
        strategy_revision="1" * 40,
        runner_revision="2" * 40,
        config_sha256="3" * 64,
        input_manifest_sha256="4" * 64,
        authority_receipt_sha256=AUTHORITY_DIGEST,
    )


def _decision() -> StrategyDecision:
    return StrategyDecision(
        positions=(
            PositionTarget(symbol="TQQQ", target_weight=0.10),
            PositionTarget(symbol="QQQM", target_weight=0.10),
            PositionTarget(symbol="BOXX", target_weight=0.0),
        )
    )


def _load(root: Path, logical_time: datetime) -> mandate.TqqqEvidenceRiskMandateSession:
    authority = _write_authority(root)
    return mandate.load_tqqq_evidence_risk_mandate(
        authority_receipt_path=authority,
        authority_source_revision=AUTHORITY_REVISION,
        consumption_store_path=mandate.CANONICAL_AUTHORITY_LEDGER_PATH,
        logical_evaluation_time=logical_time,
    )


def _approved_result(candidate: CandidateRiskIdentity, evaluated_at: str) -> object:
    return SimpleNamespace(
        assessment=SimpleNamespace(
            assessment_sha256="a" * 64,
            candidate_identity_sha256=candidate.candidate_sha256,
            evaluated_at=evaluated_at,
            execution_authorized=False,
            mandate_authority_receipt_sha256=AUTHORITY_DIGEST,
            mandate_id="tqqq_core_parity_v1",
            mandate_scope="RESEARCH_ONLY",
            outcome="APPROVE",
            qpk_source_revision=AUTHORITY_REVISION,
            reason_codes=(),
        )
    )


def test_loader_accepts_only_the_frozen_receipt_and_source_revision(tmp_path: Path) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    session = _load(tmp_path, logical_time)

    assert session.authority_receipt_sha256 == AUTHORITY_DIGEST
    assert session.authority_source_revision == AUTHORITY_REVISION

    with pytest.raises(mandate.TqqqEvidenceRiskMandateError):
        mandate.load_tqqq_evidence_risk_mandate(
            authority_receipt_path=tmp_path / "tqqq-conservative-research-v1.json",
            authority_source_revision="0" * 40,
            consumption_store_path=tmp_path / "other.sqlite3",
            logical_evaluation_time=logical_time,
        )

    receipt = tmp_path / "tqqq-conservative-research-v1.json"
    receipt.write_bytes(receipt.read_bytes().replace(b'"APPROVE"', b'"REJECT"'))
    with pytest.raises(mandate.TqqqEvidenceRiskMandateError):
        mandate.load_tqqq_evidence_risk_mandate(
            authority_receipt_path=receipt,
            authority_source_revision=AUTHORITY_REVISION,
            consumption_store_path=tmp_path / "tampered.sqlite3",
            logical_evaluation_time=logical_time,
        )


def test_session_cannot_be_constructed_or_forged_without_loader(tmp_path: Path) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)

    with pytest.raises(mandate.TqqqEvidenceRiskMandateError):
        mandate.TqqqEvidenceRiskMandateSession(
            consumption_store_path=tmp_path / "forged.sqlite3",
            logical_evaluation_time=logical_time,
            verification_token=object(),
        )

    forged = object.__new__(mandate.TqqqEvidenceRiskMandateSession)
    assert forged.is_verified is False


def test_loader_rejects_noncanonical_consumption_store(tmp_path: Path) -> None:
    authority = _write_authority(tmp_path)

    with pytest.raises(
        mandate.TqqqEvidenceRiskMandateError, match="noncanonical consumption store"
    ):
        mandate.load_tqqq_evidence_risk_mandate(
            authority_receipt_path=authority,
            authority_source_revision=AUTHORITY_REVISION,
            consumption_store_path=tmp_path / "other.sqlite3",
            logical_evaluation_time=datetime.now(UTC).replace(microsecond=0),
        )


def test_loader_rejects_missing_preprovisioned_canonical_ledger(
    tmp_path: Path,
) -> None:
    authority = _write_authority(tmp_path)
    mandate.CANONICAL_AUTHORITY_LEDGER_PATH.unlink()

    with pytest.raises(mandate.TqqqEvidenceRiskMandateError, match="consumption store"):
        mandate.load_tqqq_evidence_risk_mandate(
            authority_receipt_path=authority,
            authority_source_revision=AUTHORITY_REVISION,
            consumption_store_path=mandate.CANONICAL_AUTHORITY_LEDGER_PATH,
            logical_evaluation_time=datetime.now(UTC).replace(microsecond=0),
        )


def test_receipt_copies_share_one_preprovisioned_authority_ledger(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    logical_time = datetime.now(UTC).replace(microsecond=0)
    first = mandate.load_tqqq_evidence_risk_mandate(
        authority_receipt_path=_write_authority(first_root),
        authority_source_revision=AUTHORITY_REVISION,
        consumption_store_path=mandate.CANONICAL_AUTHORITY_LEDGER_PATH,
        logical_evaluation_time=logical_time,
    )
    second = mandate.load_tqqq_evidence_risk_mandate(
        authority_receipt_path=_write_authority(second_root),
        authority_source_revision=AUTHORITY_REVISION,
        consumption_store_path=mandate.CANONICAL_AUTHORITY_LEDGER_PATH,
        logical_evaluation_time=logical_time,
    )

    first.start(_candidate())
    with pytest.raises(mandate.TqqqEvidenceRiskMandateError, match="already consumed"):
        second.start(_candidate())


@pytest.mark.parametrize("offset_seconds", (-301, 60))
def test_loader_rejects_stale_or_future_logical_time(
    tmp_path: Path, offset_seconds: int
) -> None:
    authority = _write_authority(tmp_path)

    with pytest.raises(
        mandate.TqqqEvidenceRiskMandateError, match="logical evaluation time"
    ):
        mandate.load_tqqq_evidence_risk_mandate(
            authority_receipt_path=authority,
            authority_source_revision=AUTHORITY_REVISION,
            consumption_store_path=mandate.CANONICAL_AUTHORITY_LEDGER_PATH,
            logical_evaluation_time=datetime.now(UTC).replace(microsecond=0)
            + timedelta(seconds=offset_seconds),
        )


def test_assessment_binds_exact_material_and_calls_public_gate_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    session = _load(tmp_path, logical_time)
    candidate = _candidate()
    calls: list[dict[str, object]] = []

    def assess(*_args: object, **kwargs: object) -> object:
        calls.append(kwargs)
        return _approved_result(candidate, logical_time.isoformat().replace("+00:00", "Z"))

    monkeypatch.setattr(mandate, "assess_with_evidence", assess)
    session.start(candidate)
    result = session.assess(
        _decision(),
        market_data={},
        equity=100_000.0,
        current_weights={"TQQQ": 0.05, "QQQM": 0.10, "BOXX": 0.0},
        account_drawdown_fraction=0.02,
        source_identity_sha256="5" * 64,
    )

    assert result.approved is True
    assert len(calls) == 1
    assert calls[0]["candidate_identity"] == candidate
    assert calls[0]["logical_evaluation_time"] == logical_time
    assert calls[0]["mandate_provenance"]["authority"] == {
        "authority_scope": "RESEARCH_ONLY",
        "authority_receipt_sha256": AUTHORITY_DIGEST,
        "source_revision": AUTHORITY_REVISION,
        "runner_is_authority": False,
        "no_order": True,
        "no_paper": True,
        "no_shadow": True,
        "no_live": True,
        "no_promotion_authority": True,
    }
    receipt = session.seal(expected_decision_count=1)
    session.complete()
    with sqlite3.connect(
        mandate.CANONICAL_AUTHORITY_LEDGER_PATH
    ) as connection:
        assert connection.execute(
            "SELECT status, consumption_receipt_sha256 FROM risk_consumptions"
        ).fetchone() == ("COMPLETED", receipt["consumption_receipt_sha256"])


def test_derived_material_is_accepted_by_qpk_static_gate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    session = _load(tmp_path, logical_time)
    candidate = _candidate()
    engine_calls = 0

    class _Engine:
        def assess(self, *_args: object, **_kwargs: object) -> RiskAction:
            nonlocal engine_calls
            engine_calls += 1
            return RiskAction(action="approve", reason="offline fixture")

    monkeypatch.setattr(qpk_gate, "build_risk_engine", lambda: _Engine())
    monkeypatch.setattr(mandate, "assess_with_evidence", qpk_gate.assess_with_evidence)
    session.start(candidate)

    result = session.assess(
        _decision(),
        market_data={},
        equity=100_000.0,
        current_weights={"TQQQ": 0.05, "QQQM": 0.10, "BOXX": 0.0},
        account_drawdown_fraction=0.02,
        source_identity_sha256="5" * 64,
    )

    assert result.approved is True
    assert engine_calls == 1


def test_qpk_nonapprove_is_preserved_as_zero_authority_rejection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    session = _load(tmp_path, logical_time)
    candidate = _candidate()

    class _Engine:
        def assess(self, *_args: object, **_kwargs: object) -> RiskAction:
            return RiskAction(action="approve", reason="offline fixture")

    monkeypatch.setattr(qpk_gate, "build_risk_engine", lambda: _Engine())
    monkeypatch.setattr(mandate, "assess_with_evidence", qpk_gate.assess_with_evidence)
    session.start(candidate)
    result = session.assess(
        StrategyDecision(
            positions=(PositionTarget(symbol="TQQQ", target_weight=10.0),)
        ),
        market_data={},
        equity=100_000.0,
        current_weights={},
        account_drawdown_fraction=0.0,
        source_identity_sha256="5" * 64,
    )

    assert result.approved is False
    assert "product_exposure_cap" in result.reason_codes
    assert "effective_exposure_cap" in result.reason_codes
    receipt = session.seal(expected_decision_count=1)
    assert receipt["approved_count"] == 0
    assert receipt["rejected_count"] == 1
    assert receipt["assessment_disposition"] == "POLICY_REJECTED"
    with sqlite3.connect(
        mandate.CANONICAL_AUTHORITY_LEDGER_PATH
    ) as connection:
        assert connection.execute(
            "SELECT status, outcome FROM risk_assessments"
        ).fetchone() == ("COMPLETED", "REJECT")


@pytest.mark.parametrize(
    "binding", ("capital_binding", "portfolio_binding", "risk_state_binding")
)
def test_mismatched_snapshot_binding_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, binding: str
) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    session = _load(tmp_path, logical_time)
    candidate = _candidate()

    class _Engine:
        def assess(self, *_args: object, **_kwargs: object) -> RiskAction:
            return RiskAction(action="approve", reason="offline fixture")

    original = session._materials

    def mismatched(**kwargs: object) -> tuple[object, ...]:
        material = list(original(**kwargs))
        altered = deepcopy(material[0])
        altered[binding]["snapshot_digest_sha256"] = "0" * 64
        material[0] = altered
        return tuple(material)

    monkeypatch.setattr(qpk_gate, "build_risk_engine", lambda: _Engine())
    monkeypatch.setattr(mandate, "assess_with_evidence", qpk_gate.assess_with_evidence)
    monkeypatch.setattr(session, "_materials", mismatched)
    session.start(candidate)
    with pytest.raises(
        mandate.TqqqEvidenceRiskMandateError, match="integrity rejected"
    ):
        session.assess(
            _decision(),
            market_data={},
            equity=100_000.0,
            current_weights={},
            account_drawdown_fraction=0.0,
            source_identity_sha256="5" * 64,
        )
    with sqlite3.connect(mandate.CANONICAL_AUTHORITY_LEDGER_PATH) as connection:
        assert connection.execute(
            "SELECT status, failure_code FROM risk_consumptions"
        ).fetchone() == ("PARKED", "ASSESSMENT_INTEGRITY_REJECTED")


def test_started_assessment_is_not_retried_after_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    session = _load(tmp_path, logical_time)
    candidate = _candidate()
    calls = 0

    def fail(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise RuntimeError("private detail")

    monkeypatch.setattr(mandate, "assess_with_evidence", fail)
    session.start(candidate)
    with pytest.raises(mandate.TqqqEvidenceRiskMandateError):
        session.assess(
            _decision(),
            market_data={},
            equity=100_000.0,
            current_weights={},
            account_drawdown_fraction=0.0,
            source_identity_sha256="5" * 64,
        )

    second = mandate.load_tqqq_evidence_risk_mandate(
        authority_receipt_path=tmp_path / "tqqq-conservative-research-v1.json",
        authority_source_revision=AUTHORITY_REVISION,
        consumption_store_path=(
            mandate.CANONICAL_AUTHORITY_LEDGER_PATH
        ),
        logical_evaluation_time=logical_time,
    )
    with pytest.raises(mandate.TqqqEvidenceRiskMandateError):
        second.start(candidate)
    assert calls == 1
    with sqlite3.connect(
        mandate.CANONICAL_AUTHORITY_LEDGER_PATH
    ) as connection:
        assert connection.execute(
            "SELECT status, failure_code FROM risk_consumptions"
        ).fetchone() == ("PARKED", "ASSESSMENT_INDETERMINATE")
        assert connection.execute(
            "SELECT status, failure_code FROM risk_assessments"
        ).fetchone() == ("PARKED", "ASSESSMENT_INDETERMINATE")


def test_same_frozen_material_has_stable_mandate_and_receipt_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    logical_time = datetime.now(UTC).replace(microsecond=0)
    candidate = _candidate()
    captured: list[bytes] = []

    def assess(*_args: object, **kwargs: object) -> object:
        captured.append(
            json.dumps(
                kwargs["mandate_provenance"], sort_keys=True, separators=(",", ":")
            ).encode()
        )
        return _approved_result(candidate, logical_time.isoformat().replace("+00:00", "Z"))

    monkeypatch.setattr(mandate, "assess_with_evidence", assess)
    session = _load(tmp_path, logical_time)
    session.start(candidate)
    material_args = {
        "equity": 100_000.0,
        "current_weights": {"TQQQ": 0.05, "QQQM": 0.10, "BOXX": 0.0},
        "account_drawdown_fraction": 0.02,
        "source_identity_sha256": "5" * 64,
    }
    first = session._materials(**material_args)
    second = session._materials(**material_args)
    session.assess(_decision(), market_data={}, **material_args)
    receipt = session.seal(expected_decision_count=1)

    assert json.dumps(first[0], sort_keys=True, separators=(",", ":")) == json.dumps(
        second[0], sort_keys=True, separators=(",", ":")
    )
    assert len(captured) == 1
    assert receipt["consumption_semantics"] == (
        "AT_MOST_ONCE_PER_CANONICAL_AUTHORITY_LEDGER"
    )
    assert receipt["assessment_disposition"] == "ALL_APPROVED"
    assert receipt["canonical_completion_required"] is True
    assert receipt["consumption_ledger_id"] == (
        f"qsl-risk-authority-ledger-v1:{AUTHORITY_DIGEST}"
    )
    assert receipt["distributed_exactly_once"] is False
    assert receipt["consumption_receipt_sha256"] == hashlib.sha256(
        json.dumps(
            {
                key: value
                for key, value in receipt.items()
                if key != "consumption_receipt_sha256"
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
