from __future__ import annotations

import base64
import inspect
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

from us_equity_snapshot_pipelines import tqqq_local_no_order_runner as runner
from us_equity_snapshot_pipelines.tqqq_local_no_order_runner import TqqqForwardInputEnvelope, run_tqqq_local_no_order
from us_equity_snapshot_pipelines.tqqq_local_no_order_present import run_tqqq_local_no_order_present


def test_tqqq_local_no_order_runner_exposes_only_the_frozen_inputs() -> None:
    assert tuple(inspect.signature(run_tqqq_local_no_order).parameters) == (
        "benchmark_history_csv",
        "as_of",
        "session_id",
        "output_parent",
    )


def test_tqqq_forward_envelope_is_immutable_and_absent_only() -> None:
    envelope = TqqqForwardInputEnvelope(
        schema="qsl.tqqq_forward_input_envelope.v1",
        as_of="2026-07-17",
        session_id="XNAS:2026-07-17",
        uesp_commit_sha="a" * 40,
        market_csv_bytes=b"session_date,close\n",
        merged_config_json=b"{}",
        portfolio_json=b"{}",
        plugin_control_status="ABSENT",
    )

    assert envelope.plugin_control_status == "ABSENT"
    try:
        envelope.as_of = "2026-07-18"  # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:  # pragma: no cover - defensive assertion for dataclass semantics
        raise AssertionError("envelope must be frozen")


def _write_history(path: Path, *, as_of: str = "2026-07-17") -> None:
    rows = ["session_date,close"]
    start = date.fromisoformat(as_of) - timedelta(days=251)
    for offset in range(252):
        rows.append(f"{start + timedelta(days=offset)},{100.0 + offset}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _write_present_package(path: Path, *, as_of: str = "2026-07-17", qsp_commit_sha: str = "b" * 40) -> str:
    payload = {
        "as_of": as_of,
        "audit_summary": {},
        "arbiter": {},
        "canonical_route": {},
        "component_signals": {},
        "configured_mode": "shadow",
        "consumption_policy": {
            "evidence_status": "automation_approved",
            "plugin": "market_regime_control",
            "position_control_allowed": True,
            "strategy": "tqqq_growth_income",
        },
        "effective_mode": "shadow",
        "execution_controls": {},
        "generated_at": "2026-07-17T00:00:00+00:00",
        "localized_messages": {},
        "log_record": {},
        "mode": "shadow",
        "notification": {},
        "plugin": "market_regime_control",
        "position_control": {},
        "profile": "market_regime_control",
        "schema_version": "market_regime_control.v1",
        "strategy": "tqqq_growth_income",
        "strategy_policy": {},
        "suggested_action": {},
        "target_type": "strategy",
        "would_trade_if_enabled": False,
    }
    payload_bytes = runner._canonical_json(payload)
    config_value = {
        "as_of": as_of,
        "enabled": True,
        "mode": "shadow",
        "plugin": "market_regime_control",
        "prices": "@input:prices",
        "strategy": "tqqq_growth_income",
    }
    package = {
        "as_of": as_of,
        "config": {"sha256": runner._sha256(runner._canonical_json(config_value)), "value": config_value},
        "inputs": {
            "external_context": {"status": "ABSENT"},
            "prices": {"format": "csv", "sha256": runner._sha256(b"prices"), "size_bytes": 6},
        },
        "payload": {
            "bytes_b64": base64.b64encode(payload_bytes).decode("ascii"),
            "schema_version": "market_regime_control.v1",
            "sha256": runner._sha256(payload_bytes),
            "size_bytes": len(payload_bytes),
        },
        "producer": {
            "commit_sha": qsp_commit_sha,
            "entrypoint": "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin",
            "repository": "QuantStrategyLab/QuantStrategyPlugins",
        },
        "schema": "qsl.tqqq_market_regime_control_present.v1",
        "session_id": f"XNAS:{as_of}",
        "status": "PRESENT",
        "subject": {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"},
    }
    package_bytes = runner._canonical_json(package)
    digest = runner._sha256(package_bytes)
    path.write_bytes(package_bytes)
    return digest


def _install_decision_spy(monkeypatch) -> list[object]:
    import us_equity_strategies.entrypoints as entrypoints

    contexts: list[object] = []
    decision = SimpleNamespace(positions={"TQQQ": 1.0}, budgets={"risk": 1.0}, risk_flags=("ok",), diagnostics={"source": "spy"})

    def compute(context):
        contexts.append(context)
        return decision

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", compute)
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    return contexts


def test_runner_publishes_an_absent_only_two_file_package(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    _write_history(history)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    _, package = run_tqqq_local_no_order(
        benchmark_history_csv=history,
        as_of="2026-07-17",
        session_id="XNAS:2026-07-17",
        output_parent=tmp_path,
    )

    assert {path.name for path in package.iterdir()} == {"input_envelope.json", "decision.json"}
    envelope = json.loads((package / "input_envelope.json").read_text(encoding="utf-8"))
    decision = json.loads((package / "decision.json").read_text(encoding="utf-8"))
    assert envelope["plugin_control"] == {"status": "ABSENT"}
    assert envelope["portfolio"]["value"]["metadata"] == {}
    assert decision["input_envelope_sha256"] == package.name.rsplit("-", 1)[-1]


def test_cli_rejects_removed_plugin_control_option(capsys) -> None:
    assert runner.main(["--plugin-control-json", "control.json"]) == 2
    assert capsys.readouterr().err == "ERROR T2B1_INPUT_INVALID\n"


def test_present_consumer_binds_evidence_without_changing_decision_context(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    absent_parent = tmp_path / "absent"
    present_parent = tmp_path / "present"
    package_path = tmp_path / "tqqq-market-regime-control-present-2026-07-17.json"
    absent_parent.mkdir()
    present_parent.mkdir()
    _write_history(history)
    digest = _write_present_package(package_path)
    package_path.rename(package_path.with_name(f"tqqq-market-regime-control-present-2026-07-17-{digest}.json"))
    package_path = package_path.with_name(f"tqqq-market-regime-control-present-2026-07-17-{digest}.json")
    contexts = _install_decision_spy(monkeypatch)

    absent_decision, _ = run_tqqq_local_no_order(
        benchmark_history_csv=history,
        as_of="2026-07-17",
        session_id="XNAS:2026-07-17",
        output_parent=absent_parent,
    )
    present_decision, present_output = run_tqqq_local_no_order_present(
        benchmark_history_csv=history,
        as_of="2026-07-17",
        session_id="XNAS:2026-07-17",
        output_parent=present_parent,
        plugin_control_package=package_path,
        plugin_control_package_sha256=digest,
        qsp_commit_sha="b" * 40,
    )

    assert absent_decision is present_decision
    assert len(contexts) == 2
    assert contexts[0].portfolio.metadata == contexts[1].portfolio.metadata == {}
    assert contexts[0].market_data["benchmark_history"].equals(contexts[1].market_data["benchmark_history"])
    assert contexts[0].state == contexts[1].state == {}
    assert contexts[0].runtime_config == contexts[1].runtime_config == {}
    assert contexts[0].capabilities == contexts[1].capabilities == {}
    assert contexts[0].artifacts == contexts[1].artifacts == {}
    envelope = json.loads((present_output / "input_envelope.json").read_text(encoding="utf-8"))
    assert envelope["plugin_control"] == json.loads(package_path.read_text(encoding="utf-8"))


def test_present_consumer_rejects_untrusted_package_before_compute(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    output_parent = tmp_path / "output"
    package_path = tmp_path / "tqqq-market-regime-control-present-2026-07-17-invalid.json"
    output_parent.mkdir()
    _write_history(history)
    _write_present_package(package_path)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    def must_not_compute(_context):
        raise AssertionError("invalid package must fail before compute")

    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", must_not_compute)
    try:
        run_tqqq_local_no_order_present(
            benchmark_history_csv=history,
            as_of="2026-07-17",
            session_id="XNAS:2026-07-17",
            output_parent=output_parent,
            plugin_control_package=package_path,
            plugin_control_package_sha256="c" * 64,
            qsp_commit_sha="b" * 40,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B2_PRESENT_INVALID"
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("invalid package must be rejected")
    assert list(output_parent.iterdir()) == []


def test_present_publication_failure_retains_the_computed_decision(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    output_parent = tmp_path / "output"
    package_path = tmp_path / "tqqq-market-regime-control-present-2026-07-17.json"
    output_parent.mkdir()
    _write_history(history)
    digest = _write_present_package(package_path)
    package_path.rename(package_path.with_name(f"tqqq-market-regime-control-present-2026-07-17-{digest}.json"))
    package_path = package_path.with_name(f"tqqq-market-regime-control-present-2026-07-17-{digest}.json")
    decision = SimpleNamespace(positions={}, budgets={}, risk_flags=(), diagnostics={})
    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", lambda _context: decision)
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    monkeypatch.setattr(
        runner,
        "_strict_readback",
        lambda *_: (_ for _ in ()).throw(runner._RunnerError("T2B1_READBACK_FAILED")),
    )

    try:
        run_tqqq_local_no_order_present(
            benchmark_history_csv=history,
            as_of="2026-07-17",
            session_id="XNAS:2026-07-17",
            output_parent=output_parent,
            plugin_control_package=package_path,
            plugin_control_package_sha256=digest,
            qsp_commit_sha="b" * 40,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B1_READBACK_FAILED"
        assert error.decision is decision
    else:  # pragma: no cover - failure-boundary assertion
        raise AssertionError("staged readback failure must be preserved")
    assert list(output_parent.iterdir()) == []
