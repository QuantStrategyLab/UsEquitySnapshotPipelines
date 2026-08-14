from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

def _load_script_module():
    script = Path(__file__).parents[1] / "scripts" / "run_tqqq_p3.py"
    spec = importlib.util.spec_from_file_location("run_tqqq_p3", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_passes_new_p1_root_to_evidence_consumer(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    binding = {"binding": "identity"}
    manifest = {"manifest": "identity"}
    bars = {"bars": "private"}
    for filename, value in (("binding.json", binding), ("manifest.json", manifest), ("bars.json", bars)):
        (snapshot / filename).write_text(json.dumps(value), encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    captured: dict[str, object] = {}

    def run_evidence(**kwargs):
        captured.update(kwargs)
        return {"evidence_sha256": "1" * 64, "verdict": "INCONCLUSIVE"}

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root", str(snapshot),
            "--config", str(config_path),
            "--mandate-receipt-sha256", "2" * 64,
            "--output-dir", str(tmp_path / "output"),
        ]
    ) == 0
    assert captured["input_payload"] == {
        "binding": binding,
        "input_manifest": manifest,
        "bars": bars,
    }
    assert json.loads(capsys.readouterr().out) == {
        "evidence_sha256": "1" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "INCONCLUSIVE",
    }


def test_cli_reads_new_p1_root_without_personal_attestation(tmp_path: Path) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    binding = {"binding": "identity"}
    manifest = {"manifest": "identity"}
    bars = {"bars": "private"}
    for filename, value in (("binding.json", binding), ("manifest.json", manifest), ("bars.json", bars)):
        (snapshot / filename).write_text(json.dumps(value), encoding="utf-8")

    assert module._snapshot_payload(snapshot) == {
        "binding": binding,
        "input_manifest": manifest,
        "bars": bars,
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
    snapshot.mkdir()
    for filename in ("binding.json", "manifest.json", "bars.json"):
        (snapshot / filename).write_text("{}", encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
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
