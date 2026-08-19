from __future__ import annotations

import hashlib
import importlib.util
import json
from functools import wraps
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding
from us_equity_snapshot_pipelines.lifecycle import tqqq_p5_forward_observation as forward


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


def _bars(symbol: str, *, date_cutoff: str) -> dict[str, object]:
    first_eligible = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
    result: list[dict[str, object]] = []
    for index, session in enumerate(p1_binding._expected_xnys_sessions(date_cutoff)):
        if first_eligible is not None and session.isoformat() < first_eligible:
            continue
        close = 100.0 + index if symbol == "QQQ" else 100.0
        result.append(
            {
                "date": session.isoformat(),
                "open": close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1.0,
            }
        )
    return {"bars": result}


def _write_p2_v5_root(root: Path, *, date_cutoff: str = "2026-08-18") -> str:
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    binding = p1_binding.build_tqqq_core_only_p1_binding_for_contract(
        p1_binding.P2_V5_CONTRACT, date_cutoff=date_cutoff
    )
    bars = {
        "schema_version": "tqqq_core_only_private_bars.v1",
        "symbols": {
            symbol: _bars(symbol, date_cutoff=date_cutoff)
            for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")
        },
    }
    bars_bytes = _canonical(bars)
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=bars_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(bars["symbols"][symbol])).hexdigest()
            for symbol in bars["symbols"]
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


def _config() -> dict[str, object]:
    return json.loads((Path(__file__).parents[1] / "config" / "tqqq_core_only_p2_v5.json").read_text())


def _daily_status(*, manifest_sha256: str, date_cutoff: str = "2026-08-18") -> dict[str, object]:
    return {
        "schema_version": "qsl.tqqq-daily-research-status.v1",
        "candidate": {
            "candidate_id": "tqqq_core_only_p2_v5",
            "config_sha256": p1_binding.P2_V5_CONTRACT.config_sha256,
        },
        "date_cutoff": date_cutoff,
        "input_manifest_sha256": manifest_sha256,
        "p1_health_sha256": "c" * 64,
        "p3_terminal": {
            "evidence_sha256": "d" * 64,
            "status": "EVIDENCE_V2_COMPLETE",
            "verdict": "INCONCLUSIVE_DATA_OR_EXECUTION",
        },
    }


def _load_script_module():
    script = Path(__file__).parents[1] / "scripts" / "build_tqqq_p5_forward_observation.py"
    spec = importlib.util.spec_from_file_location("build_tqqq_p5_forward_observation", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_builds_exact_p2_v5_forward_observation_from_bound_daily_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "snapshot"
    manifest_sha256 = _write_p2_v5_root(root)
    calls = 0
    adapter = forward.build_tqqq_core_only_p2_v2_research_decision

    @wraps(adapter)
    def tracked_adapter(context):
        nonlocal calls
        calls += 1
        return adapter(context)

    monkeypatch.setattr(forward, "build_tqqq_core_only_p2_v2_research_decision", tracked_adapter)
    observation = forward.build_tqqq_p5_forward_observation(
        snapshot_root=root,
        config_payload=_config(),
        daily_research_status=_daily_status(manifest_sha256=manifest_sha256),
        producer_revision="e" * 40,
        produced_at="2026-08-19T20:00:00Z",
    )

    assert calls == 1
    assert observation["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v5",
        "config_sha256": p1_binding.P2_V5_CONTRACT.config_sha256,
        "strategy_repository": "QuantStrategyLab/UsEquityStrategies",
        "strategy_revision": p1_binding.P2_V5_UES_REVISION,
    }
    assert observation["source_evidence"] == {
        "p1_manifest_sha256": manifest_sha256,
        "p2_config_sha256": p1_binding.P2_V5_CONTRACT.config_sha256,
        "p3_evidence_sha256": "d" * 64,
        "producer_revision": "e" * 40,
    }
    assert observation["forward_decision"]["effective_session"] == "2026-08-19"
    assert observation["forward_decision"]["allocation_bps"] == {
        "TQQQ": 4500,
        "QQQM": 4500,
        "BOXX": 800,
        "CASH": 200,
    }
    assert forward.validate_tqqq_p5_forward_observation(observation) == observation


def test_daily_status_must_bind_the_exact_p1_root(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    manifest_sha256 = _write_p2_v5_root(root)
    status = _daily_status(manifest_sha256=manifest_sha256)
    status["input_manifest_sha256"] = "0" * 64

    with pytest.raises(forward.TqqqP5ForwardObservationError, match="bind this P1 root"):
        forward.build_tqqq_p5_forward_observation(
            snapshot_root=root,
            config_payload=_config(),
            daily_research_status=status,
            producer_revision="e" * 40,
            produced_at="2026-08-19T20:00:00Z",
        )


def test_calendar_labels_next_xnys_session_not_weekend_or_labor_day() -> None:
    assert p1_binding.next_tqqq_core_only_xnys_session_after("2026-08-21").isoformat() == "2026-08-24"
    assert p1_binding.next_tqqq_core_only_xnys_session_after("2026-09-04").isoformat() == "2026-09-08"


def test_validator_rejects_modified_allocation_without_digest_rewrite(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    manifest_sha256 = _write_p2_v5_root(root)
    observation = forward.build_tqqq_p5_forward_observation(
        snapshot_root=root,
        config_payload=_config(),
        daily_research_status=_daily_status(manifest_sha256=manifest_sha256),
        producer_revision="e" * 40,
        produced_at="2026-08-19T20:00:00Z",
    )
    observation["forward_decision"]["allocation_bps"]["TQQQ"] = 0

    with pytest.raises(forward.TqqqP5ForwardObservationError, match="allocation|digest"):
        forward.validate_tqqq_p5_forward_observation(observation)


def test_cli_writes_create_only_observation_and_parks_on_repeat(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    module = _load_script_module()
    root = tmp_path / "snapshot"
    manifest_sha256 = _write_p2_v5_root(root)
    config_path = tmp_path / "config.json"
    status_path = tmp_path / "daily-status.json"
    output_path = tmp_path / "observation.json"
    config_path.write_text(json.dumps(_config()), encoding="utf-8")
    status_path.write_text(json.dumps(_daily_status(manifest_sha256=manifest_sha256)), encoding="utf-8")
    argv = [
        "--snapshot-root", str(root),
        "--config", str(config_path),
        "--daily-research-status", str(status_path),
        "--producer-revision", "e" * 40,
        "--produced-at", "2026-08-19T20:00:00Z",
        "--output", str(output_path),
    ]

    assert module.main(argv) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "FORWARD_OBSERVATION_RECORDED"
    assert forward.validate_tqqq_p5_forward_observation(json.loads(output_path.read_text()))
    assert module.main(argv) == 2
    assert json.loads(capsys.readouterr().out) == {"stage": "forward_observation", "status": "PARKED"}
