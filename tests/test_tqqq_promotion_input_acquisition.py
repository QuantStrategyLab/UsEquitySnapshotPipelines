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


class _VerifiedRiskSession:
    is_verified = True

    def complete(self) -> None:
        pass

    def park(self, _failure_code: str) -> None:
        pass


_RISK_SESSION = _VerifiedRiskSession()


@pytest.fixture(autouse=True)
def _verified_risk_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "TqqqEvidenceRiskMandateSession", _VerifiedRiskSession)
    monkeypatch.setattr(
        cli,
        "load_tqqq_evidence_risk_mandate",
        lambda **_kwargs: _RISK_SESSION,
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
    manifest_sha256 = p1_binding.verify_tqqq_core_only_input_root(snapshot)
    config = tmp_path / "config.json"
    config.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}')
    captured: dict[str, object] = {}

    def write_evidence(**kwargs: object) -> dict[str, str]:
        captured.update(kwargs)
        return {
            "evidence_sha256": "a" * 64,
            "promotion_result_sha256": "b" * 64,
            "candidate_identity_sha256": "c" * 64,
            "input_manifest_sha256": manifest_sha256,
            "verdict": "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        }

    monkeypatch.setattr(cli, "run_tqqq_promotion_evidence", write_evidence)
    output = tmp_path / "evidence"

    assert cli.main(
        [
            "--snapshot-root",
            str(snapshot),
            "--config",
            str(config),
            "--mandate-receipt-sha256",
            "e" * 64,
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
            "--output-dir",
            str(output),
        ]
    ) == 0
    captured_output = captured.pop("output_dir")
    assert isinstance(captured_output, Path)
    assert captured_output.parent == output.parent
    assert captured_output.name.startswith(f".{output.name}.")
    assert output.is_dir()
    assert captured == {
        "input_payload": {
            "binding": json.loads((snapshot / "binding.json").read_bytes()),
            "input_manifest": json.loads((snapshot / "manifest.json").read_bytes()),
            "bars": json.loads((snapshot / "bars.json").read_bytes()),
        },
        "config_payload": {"candidate_id": "tqqq_core_only_p2_v1"},
        "defer_risk_completion": True,
        "generated_at": "2026-09-02T10:00:00Z",
        "mandate_receipt_sha256": "e" * 64,
        "risk_mandate_session": _RISK_SESSION,
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
    config.write_text('{"candidate_id":"tqqq_core_only_p2_v1"}')
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
            "--logical-evaluation-time", "2026-09-02T10:00:00Z",
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


def test_orchestrators_forward_verified_risk_session_to_evidence() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "us_equity_snapshot_pipelines"
        / "lifecycle"
        / "tqqq_acquisition_orchestration.py"
    ).read_text(encoding="utf-8")

    assert source.count("risk_mandate_session=risk_mandate_session") == 2
    assert source.count("load_tqqq_evidence_risk_mandate(") == 2
    assert source.index("risk_mandate_session.complete()") < source.index(
        "_publish_noreplace(temporary, published_root)"
    )
    direct_completion = source.rindex("risk_mandate_session.complete()")
    assert direct_completion < source.index(
        '_publish_noreplace(temporary_evidence, run_root / "evidence")'
    )
    assert 'tempfile.mkdtemp(prefix=".evidence.", dir=run_root)' in source
    assert "_sync_directory(published_root.parent)" in source
    assert "_sync_directory(run_root)" in source
