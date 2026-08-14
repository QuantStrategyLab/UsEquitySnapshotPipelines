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


def test_cli_passes_new_p1_root_to_evidence_consumer(tmp_path: Path) -> None:
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
