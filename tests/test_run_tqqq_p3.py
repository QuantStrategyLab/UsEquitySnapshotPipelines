from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_script_module():
    script = Path(__file__).parents[1] / "scripts" / "run_tqqq_p3.py"
    spec = importlib.util.spec_from_file_location("run_tqqq_p3", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_passes_personal_attestation_to_evidence_consumer(tmp_path: Path) -> None:
    module = _load_script_module()
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    manifest = {"manifest": "identity"}
    bars = {"bars": "private"}
    attestation = {"attestation": "human_attested"}
    (snapshot / "input-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (snapshot / "bars.json").write_text(json.dumps(bars), encoding="utf-8")
    attestation_path = tmp_path / "scope-retention-attestation.v1.json"
    attestation_path.write_text(json.dumps(attestation), encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    captured: dict[str, object] = {}

    def run_evidence(**kwargs):
        captured.update(kwargs)
        return {"evidence_sha256": "1" * 64, "verdict": "INCONCLUSIVE"}

    module.run_tqqq_promotion_evidence = run_evidence

    assert module.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--personal-attestation",
            str(attestation_path),
            "--config",
            str(config_path),
            "--mandate-receipt-sha256",
            "2" * 64,
            "--output-dir",
            str(tmp_path / "output"),
        ]
    ) == 0
    assert captured["input_payload"] == {
        "provenance": attestation,
        "input_manifest": manifest,
        "bars": bars,
    }
