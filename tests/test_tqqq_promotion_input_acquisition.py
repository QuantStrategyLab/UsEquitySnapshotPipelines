from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import pytest

from scripts import run_tqqq_p3 as cli
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding
from us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence import (
    TqqqPromotionEvidenceError,
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _write_snapshot(root: Path) -> Path:
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    binding = p1_binding.build_tqqq_core_only_p1_binding()
    symbols: dict[str, object] = {}
    for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX"):
        first_eligible = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
        symbols[symbol] = {
            "bars": [
                {
                    "date": session.isoformat(),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1.0,
                }
                for session in p1_binding._expected_xnys_sessions("2026-07-31")
                if first_eligible is None or session.isoformat() >= first_eligible
            ]
        }
    bars = {"schema_version": "tqqq_core_only_private_bars.v1", "symbols": symbols}
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-15T00:00:00Z",
        producer={
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": "a" * 40,
            "tree_sha": "b" * 40,
            "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
            "tool_version": "v1",
        },
        member_bytes=_canonical(bars),
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(value)).hexdigest()
            for symbol, value in symbols.items()
        },
    )
    (root / "binding.json").write_bytes(p1_binding.canonical_binding_bytes(binding))
    (root / "manifest.json").write_bytes(p1_binding.canonical_research_input_manifest_bytes(manifest))
    (root / "bars.json").write_bytes(_canonical(bars))
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
            "binding": json.loads((snapshot / "binding.json").read_bytes()),
            "input_manifest": json.loads((snapshot / "manifest.json").read_bytes()),
            "bars": json.loads((snapshot / "bars.json").read_bytes()),
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
