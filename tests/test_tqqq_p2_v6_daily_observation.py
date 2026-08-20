from __future__ import annotations

import hashlib
import importlib.util
import json
from copy import deepcopy
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding
from us_equity_snapshot_pipelines.lifecycle import tqqq_p2_v6_daily_observation as daily
from us_equity_snapshot_pipelines.lifecycle import tqqq_p5_forward_observation as forward


QSP_REVISION = daily.QSP_QQQ_PRICE_REGIME_OBSERVER_REVISION


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


def _bars(symbol: str, *, cutoff: str) -> dict[str, object]:
    first_eligible = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
    rows: list[dict[str, object]] = []
    for index, session in enumerate(p1_binding._expected_xnys_sessions(cutoff)):
        if first_eligible is not None and session.isoformat() < first_eligible:
            continue
        close = 100.0 + index * 0.1 if symbol == "QQQ" else 100.0
        rows.append(
            {
                "date": session.isoformat(),
                "open": close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1.0,
            }
        )
    return {"bars": rows}


def _write_verified_root(root: Path, *, cutoff: str = "2026-08-18") -> str:
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    binding = p1_binding.build_tqqq_core_only_p1_binding_for_contract(
        p1_binding.P2_V5_CONTRACT, date_cutoff=cutoff
    )
    symbols = {symbol: _bars(symbol, cutoff=cutoff) for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")}
    bars = {"schema_version": "tqqq_core_only_private_bars.v1", "symbols": symbols}
    bars_bytes = _canonical(bars)
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=bars_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(payload)).hexdigest() for symbol, payload in symbols.items()
        },
        contract=p1_binding.P2_V5_CONTRACT,
    )
    (root / "binding.json").write_bytes(
        p1_binding.canonical_tqqq_core_only_p1_binding_bytes_for_contract(
            binding, p1_binding.P2_V5_CONTRACT
        )
    )
    (root / "bars.json").write_bytes(bars_bytes)
    (root / "manifest.json").write_bytes(p1_binding.canonical_research_input_manifest_bytes(manifest))
    return p1_binding.verify_tqqq_core_only_input_root(root, contract=p1_binding.P2_V5_CONTRACT)


def _daily_status(manifest_sha256: str) -> dict[str, object]:
    return {
        "schema_version": "qsl.tqqq-daily-research-status.v1",
        "candidate": {
            "candidate_id": "tqqq_core_only_p2_v5",
            "config_sha256": p1_binding.P2_V5_CONTRACT.config_sha256,
        },
        "date_cutoff": "2026-08-18",
        "input_manifest_sha256": manifest_sha256,
        "p1_health_sha256": "c" * 64,
        "p3_terminal": {
            "evidence_sha256": "d" * 64,
            "status": "EVIDENCE_V2_COMPLETE",
            "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
        },
    }


def _forward_observation(root: Path, status: dict[str, object]) -> dict[str, object]:
    config_path = Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v5.json"
    return forward.build_tqqq_p5_forward_observation(
        snapshot_root=root,
        config_payload=json.loads(config_path.read_text(encoding="utf-8")),
        daily_research_status=status,
        producer_revision="e" * 40,
        produced_at="2026-08-19T20:00:00Z",
    )


def _load_script_module():
    script = Path(__file__).parents[1] / "scripts" / "build_tqqq_p2_v6_daily_observation.py"
    spec = importlib.util.spec_from_file_location("build_tqqq_p2_v6_daily_observation", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_daily_observation_recomputes_the_verified_root_and_redacts_outputs(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    status = _daily_status(_write_verified_root(root))
    record = daily.build_tqqq_p2_v6_daily_observation(
        snapshot_root=root,
        daily_research_status=status,
        forward_observation=_forward_observation(root, status),
        qsp_revision=QSP_REVISION,
        produced_at="2026-08-19T20:00:00Z",
    )

    assert daily.validate_tqqq_p2_v6_daily_observation(record) == record
    assert record["target_equivalence"]["equivalent"] is True
    assert record["retention"] == {
        "storage": "GITHUB_ACTIONS_ARTIFACT",
        "retention_days": 35,
        "durable_retention_authorized": False,
        "raw_bars_included": False,
    }
    assert set(record["signal"]) == {
        "schema_version",
        "plugin_id",
        "payload_sha256",
        "producer_revision",
        "config_sha256",
    }
    serialized = json.dumps(record, sort_keys=True)
    assert "allocation_bps" not in serialized
    assert '"payload"' not in serialized
    assert '"QQQ"' not in serialized

    tampered = deepcopy(record)
    tampered["signal"]["producer_revision"] = "f" * 40
    tampered["observation_sha256"] = daily.calculate_tqqq_p2_v6_daily_observation_sha256(tampered)
    with pytest.raises(daily.TqqqP2V6DailyObservationError, match="invalid_daily_observation"):
        daily.validate_tqqq_p2_v6_daily_observation(tampered)


def test_daily_observation_rejects_a_forward_record_bound_to_other_p3_evidence(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    status = _daily_status(_write_verified_root(root))
    observation = _forward_observation(root, status)
    status["p3_terminal"]["evidence_sha256"] = "0" * 64

    with pytest.raises(daily.TqqqP2V6DailyObservationError, match="p3_evidence_mismatch"):
        daily.build_tqqq_p2_v6_daily_observation(
            snapshot_root=root,
            daily_research_status=status,
            forward_observation=observation,
            qsp_revision=QSP_REVISION,
            produced_at="2026-08-19T20:00:00Z",
        )


def test_daily_observation_rejects_a_daily_status_with_other_p1_manifest(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    status = _daily_status(_write_verified_root(root))
    observation = _forward_observation(root, status)
    status["input_manifest_sha256"] = "0" * 64

    with pytest.raises(daily.TqqqP2V6DailyObservationError, match="p1_manifest_mismatch"):
        daily.build_tqqq_p2_v6_daily_observation(
            snapshot_root=root,
            daily_research_status=status,
            forward_observation=observation,
            qsp_revision=QSP_REVISION,
            produced_at="2026-08-19T20:00:00Z",
        )


def test_cli_writes_create_only_record_and_parks_on_repeat(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _load_script_module()
    root = tmp_path / "snapshot"
    status = _daily_status(_write_verified_root(root))
    forward_path = tmp_path / "forward.json"
    status_path = tmp_path / "status.json"
    output_path = tmp_path / "observation.json"
    forward_path.write_text(json.dumps(_forward_observation(root, status)), encoding="utf-8")
    status_path.write_text(json.dumps(status), encoding="utf-8")
    argv = [
        "--snapshot-root",
        str(root),
        "--daily-research-status",
        str(status_path),
        "--forward-observation",
        str(forward_path),
        "--qsp-revision",
        QSP_REVISION,
        "--produced-at",
        "2026-08-19T20:00:00Z",
        "--output",
        str(output_path),
    ]

    assert module.main(argv) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "OBSERVATION_RECORDED"
    assert daily.validate_tqqq_p2_v6_daily_observation(json.loads(output_path.read_text()))
    assert module.main(argv) == 2
    assert json.loads(capsys.readouterr().out) == {
        "stage": "v6_plugin_observation",
        "status": "PARKED",
    }
