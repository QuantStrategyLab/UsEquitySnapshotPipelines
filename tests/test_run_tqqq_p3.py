from __future__ import annotations

import hashlib
import importlib.util
import json
from functools import wraps
from pathlib import Path

import pytest

import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence as evidence
import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_runner as promotion_runner
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding


def _load_script_module():
    script = Path(__file__).parents[1] / "scripts" / "run_tqqq_p3.py"
    spec = importlib.util.spec_from_file_location("run_tqqq_p3", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
) -> dict[str, object]:
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    frozen_contract = contract or p1_binding.resolve_tqqq_core_only_candidate_contract(
        "tqqq_core_only_p2_v1"
    )
    binding = p1_binding.build_tqqq_core_only_p1_binding_for_contract(frozen_contract)
    date_cutoff = binding["data_identity"]["date_cutoff"]
    assert isinstance(date_cutoff, str)
    bars = {
        "schema_version": "tqqq_core_only_private_bars.v1",
        "symbols": {
            symbol: _alpaca_symbol_payload(symbol, date_cutoff)
            for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")
        },
    }
    bars_bytes = _canonical(bars)
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-15T00:00:00Z",
        producer=_producer(),
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
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 0
    assert captured["input_payload"] == input_payload
    assert json.loads(capsys.readouterr().out) == {
        "evidence_sha256": "1" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
    }


def test_cli_parks_instead_of_accepting_completion_for_a_different_input_binding(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
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
            "--output-dir",
            str(tmp_path / "output"),
        ]
    ) == 2
    assert calls == 1
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "orchestrator_contract_failure",
        "replay_started": True,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "orchestrator_contract",
        "status": "PARKED",
    }


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


def test_cli_rejects_a_v1_snapshot_for_the_v2_candidate_before_replay(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    _write_canonical_snapshot(snapshot)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"candidate_id":"tqqq_core_only_p2_v2"}', encoding="utf-8"
    )
    calls = 0

    def run_evidence(**_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("candidate-bound root verification must run first")

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root", str(snapshot),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 2
    assert calls == 0
    assert json.loads(capsys.readouterr().out)["stage"] == "input_validation"


def test_cli_runs_complete_v4_p3_evidence_from_synthetic_input(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the complete v4 P3 path without provider access or real bars."""
    module = _load_script_module()
    snapshot = tmp_path / "synthetic-snapshot"
    input_payload = _write_canonical_snapshot(snapshot, p1_binding.P2_V4_CONTRACT)
    config_path = tmp_path / "p2-v4.json"
    config_path.write_bytes(
        (Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v4.json").read_bytes()
    )
    output = tmp_path / "evidence"
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
    # This fixture verifies only the synthetic replay contract.  A preceding
    # test must not make that result depend on the checkout cleanliness guard.
    monkeypatch.setattr(evidence, "_resolve_runner_revision", lambda: "a" * 40)
    monkeypatch.setattr(promotion_runner, "_resolve_runner_revision", lambda: "a" * 40)

    assert module.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--output-dir",
            str(output),
        ]
    ) == 0

    assert calls > 0
    summary = json.loads(capsys.readouterr().out)
    evidence_package = json.loads(
        (output / "strategy-evidence-package.v2.json").read_text(encoding="utf-8")
    )
    backtest = json.loads(
        (output / "artifacts" / "backtest.json").read_text(encoding="utf-8")
    )
    frozen_config = json.loads(
        (output / "artifacts" / "config.json").read_text(encoding="utf-8")
    )
    terminal = json.loads(
        (output / "promotion-research-result.v1.json").read_text(encoding="utf-8")
    )
    expected_manifest_sha256 = p1_binding.validate_tqqq_core_only_input_manifest(
        input_payload["input_manifest"],
        input_payload["binding"],
        contract=p1_binding.P2_V4_CONTRACT,
    )

    assert summary["status"] == "EVIDENCE_V2_COMPLETE"
    assert evidence_package["strategy"] == {
        "profile": "tqqq_core_only_p2_v4",
        "domain": "us_equity",
        "source_revision": p1_binding.P2_V4_UES_REVISION,
    }
    assert evidence_package["input_provenance"]["manifest_sha256"] == expected_manifest_sha256
    assert frozen_config["candidate"] == json.loads(config_path.read_text(encoding="utf-8"))
    assert backtest["strategy_execution"] == {
        "callable": "us_equity_strategies.entrypoints.build_tqqq_core_only_p2_v2_research_decision",
        "ues_revision": p1_binding.P2_V4_UES_REVISION,
    }
    assert terminal["status"] == "EVIDENCE_V2_COMPLETE"
    assert terminal["authority_scope"] == "RESEARCH_ONLY"
    assert terminal["human_acceptance"] is None
    assert terminal["no_order"] is True
    assert terminal["size_zero_required"] is True
    assert not (output / "bars.json").exists()


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
