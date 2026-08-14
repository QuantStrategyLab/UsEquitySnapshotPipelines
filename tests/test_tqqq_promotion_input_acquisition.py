from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import run_tqqq_p3 as cli
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    TqqqPromotionEvidenceError,
)


def _write_snapshot(root: Path) -> Path:
    root.mkdir()
    (root / "binding.json").write_text('{"binding":"preserved"}')
    (root / "manifest.json").write_text('{"manifest":"preserved"}')
    (root / "bars.json").write_text('{"bars":"preserved"}')
    return root


def test_cli_consumes_only_preserved_snapshot_layout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    snapshot = _write_snapshot(tmp_path / "snapshot")
    config = tmp_path / "config.json"
    config.write_text('{"config":"frozen"}')
    captured: dict[str, object] = {}

    def write_evidence(**kwargs: object) -> dict[str, str]:
        captured.update(kwargs)
        return {
            "evidence_sha256": "a" * 64,
            "promotion_result_sha256": "b" * 64,
            "candidate_identity_sha256": "c" * 64,
            "input_manifest_sha256": "d" * 64,
            "verdict": "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        }

    monkeypatch.setattr(cli, "run_tqqq_promotion_evidence", write_evidence)

    assert cli.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config),
            "--mandate-receipt-sha256",
            "e" * 64,
            "--output-dir",
            str(tmp_path / "evidence"),
        ]
    ) == 0
    assert captured == {
        "input_payload": {
            "binding": {"binding": "preserved"},
            "input_manifest": {"manifest": "preserved"},
            "bars": {"bars": "preserved"},
        },
        "config_payload": {"config": "frozen"},
        "mandate_receipt_sha256": "e" * 64,
        "output_dir": tmp_path / "evidence",
    }
    assert json.loads(capsys.readouterr().out) == {
        "evidence_sha256": "a" * 64,
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
    }


def test_cli_parks_without_exposing_failure_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    snapshot = _write_snapshot(tmp_path / "snapshot")
    config = tmp_path / "config.json"
    config.write_text("{}")
    monkeypatch.setattr(
        cli,
        "run_tqqq_promotion_evidence",
        lambda **_kwargs: (_ for _ in ()).throw(
            TqqqPromotionEvidenceError("private replay failure")
        ),
    )

    assert cli.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config),
            "--mandate-receipt-sha256",
            "e" * 64,
            "--output-dir",
            str(tmp_path / "evidence"),
        ]
    ) == 2
    assert json.loads(capsys.readouterr().out) == {
        "complete_evidence": False,
        "failure_class": "evidence_validation_failure",
        "replay_started": True,
        "source_commit": "6f346ac1b4fbff7b3d190b8c86d2d6701346e3a2",
        "stage": "evidence_validation",
        "status": "PARKED",
    }


def test_cli_has_no_legacy_or_acquisition_imports() -> None:
    source = inspect.getsource(cli).lower()

    for forbidden in (
        "tqqq_acquisition_orchestration",
        "acquire_tqqq_promotion_inputs",
        "run_existing_tqqq_snapshot",
        "ibapi",
        "reqhistoricaldata",
        "placeorder",
    ):
        assert forbidden not in source
