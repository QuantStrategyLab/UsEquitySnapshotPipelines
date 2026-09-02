from __future__ import annotations

import hashlib
import importlib.util
import json
from functools import wraps
from pathlib import Path

import pytest

import us_equity_snapshot_pipelines.lifecycle.tqqq_p2_v2_synthetic_evidence as synthetic_evidence
import us_equity_snapshot_pipelines.lifecycle.tqqq_evidence_risk_mandate as risk_mandate
import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding


def _load_script_module():
    script = Path(__file__).parents[1] / "scripts" / "run_tqqq_p3.py"
    spec = importlib.util.spec_from_file_location("run_tqqq_p3", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.TqqqEvidenceRiskMandateSession = _VerifiedRiskSession
    module.load_tqqq_evidence_risk_mandate = lambda **_kwargs: _RISK_SESSION
    return module


class _VerifiedRiskSession:
    is_verified = True

    def complete(self) -> None:
        pass

    def park(self, _failure_code: str) -> None:
        pass


_RISK_SESSION = _VerifiedRiskSession()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
        "tool_version": "v1",
    }


def _alpaca_symbol_payload(symbol: str, date_cutoff: str) -> dict[str, object]:
    first_eligible = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
    return {
        "bars": [
            {
                "date": session.isoformat(),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            }
            for session in p1_binding._expected_xnys_sessions(date_cutoff)
            if first_eligible is None or session.isoformat() >= first_eligible
        ]
    }


def _write_canonical_snapshot(
    root: Path,
    contract: p1_binding.TqqqCoreOnlyCandidateContract | None = None,
    *,
    date_cutoff: str | None = None,
    producer_tool: str | None = None,
) -> dict[str, object]:
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    frozen_contract = contract or p1_binding.resolve_tqqq_core_only_candidate_contract(
        "tqqq_core_only_p2_v1"
    )
    binding = p1_binding.build_tqqq_core_only_p1_binding_for_contract(
        frozen_contract, date_cutoff=date_cutoff
    )
    bound_cutoff = binding["data_identity"]["date_cutoff"]
    assert isinstance(bound_cutoff, str)
    bars = {
        "schema_version": "tqqq_core_only_private_bars.v1",
        "symbols": {
            symbol: _alpaca_symbol_payload(symbol, bound_cutoff)
            for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")
        },
    }
    bars_bytes = _canonical(bars)
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-15T00:00:00Z",
        producer={**_producer(), **({"tool": producer_tool} if producer_tool else {})},
        member_bytes=bars_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(bars["symbols"][symbol])).hexdigest()
            for symbol in bars["symbols"]
        },
        contract=frozen_contract,
    )
    (root / "binding.json").write_bytes(
        p1_binding.canonical_tqqq_core_only_p1_binding_bytes_for_contract(
            binding, frozen_contract
        )
    )
    (root / "manifest.json").write_bytes(p1_binding.canonical_research_input_manifest_bytes(manifest))
    (root / "bars.json").write_bytes(bars_bytes)
    return {"binding": binding, "input_manifest": manifest, "bars": bars}


def _completed_evidence_result(input_manifest_sha256: str) -> dict[str, str]:
    return {
        "evidence_sha256": "1" * 64,
        "promotion_result_sha256": "2" * 64,
        "candidate_identity_sha256": "3" * 64,
        "input_manifest_sha256": input_manifest_sha256,
        "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
    }


def test_cli_passes_canonical_p1_root_to_evidence_consumer(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    input_payload = _write_canonical_snapshot(snapshot)
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    captured: dict[str, object] = {}

    def run_evidence(**kwargs):
        captured.update(kwargs)
        return _completed_evidence_result(
            p1_binding.validate_tqqq_core_only_input_manifest(
                input_payload["input_manifest"], input_payload["binding"]
            )
        )

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root", str(snapshot),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 0
    assert captured["input_payload"] == input_payload
    assert captured["risk_mandate_session"] is _RISK_SESSION
    assert json.loads(capsys.readouterr().out) == {
        "evidence_sha256": "1" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
    }


def test_cli_emits_the_versioned_v7_policy_and_terminal_digests(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot-v7"
    input_payload = _write_canonical_snapshot(
        snapshot, p1_binding.P2_V7_CONTRACT, date_cutoff="2026-08-18"
    )
    config_path = tmp_path / "config-v7.json"
    config_path.write_text(
        '{"candidate_id":"tqqq_core_only_p2_v7_relative_benchmark"}', encoding="utf-8"
    )

    def run_evidence(**_kwargs: object) -> dict[str, str]:
        return {
            **_completed_evidence_result(
                p1_binding.validate_tqqq_core_only_input_manifest(
                    input_payload["input_manifest"], input_payload["binding"],
                    contract=p1_binding.P2_V7_CONTRACT,
                )
            ),
            "relative_benchmark_policy_sha256": "4" * 64,
        }

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root", str(snapshot), "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out) == {
        "evidence_sha256": "1" * 64,
        "promotion_result_sha256": "2" * 64,
        "relative_benchmark_policy_sha256": "4" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
    }


def test_cli_uses_the_v9_free_ohlcv_verification_route(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_script_module()
    config_path = (
        Path(__file__).parents[1]
        / "config"
        / "tqqq_core_only_p2_v9_benchmark_drawdown_guard.json"
    )
    expected_manifest = "4" * 64
    observed: dict[str, object] = {}

    def verify(snapshot_root: Path, *, contract: object) -> str:
        observed["snapshot_root"] = snapshot_root
        observed["contract"] = contract
        return expected_manifest

    monkeypatch.setattr(module, "verify_tqqq_core_only_free_ohlcv_p1_input_root", verify)
    monkeypatch.setattr(module, "_snapshot_payload", lambda _root: {"synthetic": True})
    monkeypatch.setattr(
        module,
        "run_tqqq_promotion_evidence",
        lambda **_kwargs: {
            **_completed_evidence_result(expected_manifest),
            "relative_benchmark_policy_sha256": "5" * 64,
        },
    )

    assert module.main(
        [
            "--snapshot-root",
            str(tmp_path / "v9-snapshot"),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir",
            str(tmp_path / "output"),
        ]
    ) == 0

    assert observed["contract"] == p1_binding.P2_V9_CONTRACT
    assert json.loads(capsys.readouterr().out)["relative_benchmark_policy_sha256"] == "5" * 64


def test_cli_parks_instead_of_accepting_completion_for_a_different_input_binding(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    events: list[str] = []

    class _TrackingRiskSession(_VerifiedRiskSession):
        def complete(self) -> None:
            events.append("complete")

        def park(self, failure_code: str) -> None:
            events.append(f"park:{failure_code}")

    risk_session = _TrackingRiskSession()
    module.TqqqEvidenceRiskMandateSession = _TrackingRiskSession
    module.load_tqqq_evidence_risk_mandate = lambda **_kwargs: risk_session
    snapshot = tmp_path / "snapshot"
    _write_canonical_snapshot(snapshot)
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    calls = 0

    def run_evidence(**_kwargs: object) -> dict[str, str]:
        nonlocal calls
        calls += 1
        return _completed_evidence_result("f" * 64)

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir",
            str(tmp_path / "output"),
        ]
    ) == 2
    assert calls == 1
    assert events == ["park:CLI_EVIDENCE_FAILED"]
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "orchestrator_contract_failure",
        "replay_started": True,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "orchestrator_contract",
        "status": "PARKED",
    }


def test_cli_does_not_publish_when_canonical_completion_fails(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    events: list[str] = []

    class _CompletionFails(_VerifiedRiskSession):
        def complete(self) -> None:
            events.append("complete")
            raise risk_mandate.TqqqEvidenceRiskMandateError("completion failed")

        def park(self, failure_code: str) -> None:
            events.append(f"park:{failure_code}")

    session = _CompletionFails()
    module.TqqqEvidenceRiskMandateSession = _CompletionFails
    module.load_tqqq_evidence_risk_mandate = lambda **_kwargs: session
    snapshot = tmp_path / "snapshot"
    input_payload = _write_canonical_snapshot(snapshot)
    manifest_sha256 = p1_binding.validate_tqqq_core_only_input_manifest(
        input_payload["input_manifest"], input_payload["binding"]
    )
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    module.run_tqqq_promotion_evidence = lambda **_kwargs: _completed_evidence_result(
        manifest_sha256
    )
    output = tmp_path / "must-not-exist"

    assert module.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--logical-evaluation-time",
            "2026-09-02T10:00:00Z",
            "--output-dir",
            str(output),
        ]
    ) == 2
    assert events == ["complete", "park:CLI_EVIDENCE_FAILED"]
    assert not output.exists()
    assert not tuple(tmp_path.glob(f".{output.name}.*"))
    result = json.loads(capsys.readouterr().out)
    assert result["failure_class"] == "risk_contract_failure"
    assert result["replay_started"] is True
    assert result["status"] == "PARKED"


def test_cli_sanitizes_unexpected_replay_failure_as_runtime_park(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    _write_canonical_snapshot(snapshot)
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    private_detail = "private provider bars /secret/path"

    def run_evidence(**_kwargs: object) -> object:
        raise KeyError(private_detail)

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir",
            str(tmp_path / "output"),
        ]
    ) == 2
    output = capsys.readouterr().out
    assert private_detail not in output
    assert json.loads(output) == {
        "complete_evidence": False,
        "failure_class": "runtime_internal_failure",
        "replay_started": True,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "orchestrator_contract",
        "status": "PARKED",
    }


def test_cli_rejects_tampered_source_identity_before_evidence_replay(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    input_payload = _write_canonical_snapshot(snapshot)
    input_payload["input_manifest"]["sources"][0]["content_sha256"] = "0" * 64  # type: ignore[index]
    (snapshot / "manifest.json").write_bytes(
        p1_binding.canonical_research_input_manifest_bytes(input_payload["input_manifest"])
    )
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    calls = 0

    def run_evidence(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("canonical root verification must run first")

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root", str(snapshot),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 2
    assert calls == 0
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "input_validation_failure",
        "replay_started": False,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "input_validation",
        "status": "PARKED",
    }


def test_cli_parks_historical_v2_before_reading_snapshot_or_starting_replay(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    config_path = Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v2.json"
    output = tmp_path / "must-not-exist"
    calls = 0

    def run_evidence(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("historical P2 v2 must not enter the P3 replay")

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root",
            str(tmp_path / "unreadable-snapshot"),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir",
            str(output),
        ]
    ) == 2
    assert calls == 0
    assert not output.exists()
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "config_contract_failure",
        "replay_started": False,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "config_contract",
        "status": "PARKED",
    }


def test_cli_parks_before_replay_when_evidence_risk_authority_is_missing(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    module.load_tqqq_evidence_risk_mandate = (
        risk_mandate.load_tqqq_evidence_risk_mandate
    )
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    calls = 0

    def run_evidence(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("replay must not start")

    module.run_tqqq_promotion_evidence = run_evidence
    assert module.main(
        [
            "--snapshot-root", str(tmp_path / "unreadable"),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 2
    assert calls == 0
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "risk_contract_failure",
        "replay_started": False,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "risk_contract",
        "status": "PARKED",
    }


@pytest.mark.parametrize(
    ("logical_time_args", "expected_stage"),
    (([], "input_validation"), (["--logical-evaluation-time", "invalid"], "risk_contract")),
)
def test_cli_rejects_missing_or_invalid_logical_time_before_replay(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    logical_time_args: list[str],
    expected_stage: str,
) -> None:
    module = _load_script_module()
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    calls = 0

    def run_evidence(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("replay must not start")

    module.run_tqqq_promotion_evidence = run_evidence
    assert module.main(
        [
            "--snapshot-root",
            str(tmp_path / "must-not-read"),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            *logical_time_args,
            "--output-dir",
            str(tmp_path / "must-not-exist"),
        ]
    ) == 2
    assert calls == 0
    assert not (tmp_path / "must-not-exist").exists()
    result = json.loads(capsys.readouterr().out)
    assert result["replay_started"] is False
    assert result["stage"] == expected_stage


def test_cli_parks_v4_before_synthetic_replay_without_verified_risk_authority(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_script_module()
    config_path = Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v4.json"
    calls = 0
    public_adapter = evidence.build_tqqq_core_only_p2_v2_research_decision

    @wraps(public_adapter)
    def tracked_public_adapter(context):
        nonlocal calls
        calls += 1
        return public_adapter(context)

    monkeypatch.setattr(
        evidence,
        "build_tqqq_core_only_p2_v2_research_decision",
        tracked_public_adapter,
    )
    module.load_tqqq_evidence_risk_mandate = (
        risk_mandate.load_tqqq_evidence_risk_mandate
    )

    assert module.main(
        [
            "--snapshot-root", str(tmp_path / "must-not-read"),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir", str(tmp_path / "must-not-exist"),
        ]
    ) == 2
    assert calls == 0
    assert not (tmp_path / "must-not-exist").exists()
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "risk_contract_failure",
        "replay_started": False,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "risk_contract",
        "status": "PARKED",
    }

def test_p2_v2_synthetic_evidence_calls_public_adapter_and_binds_artifact(
    tmp_path: Path,
) -> None:
    snapshot = tmp_path / "synthetic-p2-v2"
    payload = _write_canonical_snapshot(
        snapshot, p1_binding.P2_V2_CONTRACT, producer_tool="synthetic_fixture"
    )
    candidate = json.loads(
        (Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v2.json").read_text()
    )
    result = synthetic_evidence.run_synthetic_p2_v2_evidence(
        input_payload=payload, candidate=candidate, output_dir=tmp_path / "proof"
    )
    artifact = tmp_path / "proof" / "synthetic-adapter-evidence.json"
    assert result["status"] == "SYNTHETIC_ONLY_VERIFIED"
    assert result["evidence_sha256"] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    package = json.loads(artifact.read_text())
    assert package["adapter"] == "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision"
    assert package["candidate_id"] == "tqqq_core_only_p2_v2"
    assert package["no_order"] is True


def test_cli_parks_v5_before_synthetic_replay_without_verified_risk_authority(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_script_module()
    config_path = Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v5.json"
    calls = 0
    public_adapter = evidence.build_tqqq_core_only_p2_v2_research_decision

    @wraps(public_adapter)
    def tracked_public_adapter(context):
        nonlocal calls
        calls += 1
        return public_adapter(context)

    monkeypatch.setattr(
        evidence,
        "build_tqqq_core_only_p2_v2_research_decision",
        tracked_public_adapter,
    )
    module.load_tqqq_evidence_risk_mandate = (
        risk_mandate.load_tqqq_evidence_risk_mandate
    )

    assert module.main(
        [
            "--snapshot-root", str(tmp_path / "must-not-read"),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir", str(tmp_path / "must-not-exist"),
        ]
    ) == 2
    assert calls == 0
    assert not (tmp_path / "must-not-exist").exists()
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "risk_contract_failure",
        "replay_started": False,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "risk_contract",
        "status": "PARKED",
    }


@pytest.mark.parametrize(
    ("error_name", "failure_class", "stage"),
    (
        ("InputValidationError", "input_validation_failure", "input_validation"),
        ("ConfigContractError", "config_contract_failure", "config_contract"),
        ("OrchestratorContractError", "orchestrator_contract_failure", "orchestrator_contract"),
        ("RiskContractError", "risk_contract_failure", "risk_contract"),
        ("EvidenceValidationError", "evidence_validation_failure", "evidence_validation"),
        ("RuntimeInternalError", "runtime_internal_failure", "runtime_internal"),
    ),
)
def test_cli_emits_allowlisted_sanitized_typed_failure(
    capsys: pytest.CaptureFixture[str],
    error_name: str,
    failure_class: str,
    stage: str,
    tmp_path: Path,
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    _write_canonical_snapshot(snapshot)
    config_path = tmp_path / "config.json"
    config_path.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}', encoding="utf-8")
    private_detail = "private provider bars /secret/path"
    error_type = type(error_name, (ValueError,), {})
    calls = 0

    def run_evidence(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise error_type(private_detail)

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir",
            str(tmp_path / "output"),
        ]
    ) == 2
    assert calls == 1
    output = capsys.readouterr().out
    assert private_detail not in output
    assert json.loads(output) == {
        "complete_evidence": False,
        "failure_class": failure_class,
        "replay_started": True,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": stage,
        "status": "PARKED",
    }
