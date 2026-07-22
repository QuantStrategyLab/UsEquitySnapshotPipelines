from __future__ import annotations

import base64
import inspect
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from us_equity_snapshot_pipelines import tqqq_local_no_order_runner as runner
from us_equity_snapshot_pipelines import tqqq_local_no_order_present as present
from us_equity_snapshot_pipelines.tqqq_local_no_order_runner import TqqqForwardInputEnvelope, run_tqqq_local_no_order
from us_equity_snapshot_pipelines.tqqq_local_no_order_present import run_tqqq_local_no_order_present


QSP_COMMIT = "c798397d9ca9230e404673d7774bac3d478217dc"
QSP_SYMBOLS = ("QQQ", "SPY", "TQQQ", "^VIX", "^VIX3M", "HYG", "IEF", "LQD", "XLF", "KRE", "TLT")
QSP_CONFIG_BYTES = b'''default_mode = "shadow"

[[strategy_plugins]]
strategy = "tqqq_growth_income"
plugin = "market_regime_control"
enabled = true

[strategy_plugins.inputs]
prices = "prices.csv"
event_set = "geopolitical-deescalation"
benchmark_symbol = "QQQ"
attack_symbol = "TQQQ"
vix_symbols = ["VIX", "^VIX", "VIXCLS"]
vix3m_symbols = ["VIX3M", "^VIX3M", "VXV", "^VXV"]
credit_pairs = ["HYG:IEF", "LQD:IEF"]
financial_symbols = ["XLF", "KRE"]
rate_symbols = ["IEF", "TLT"]
strategy_policy = "levered_growth_income_v1"
realized_vol_threshold = 0.30
realized_vol_requires_confirmation = true
external_stress_actionable = false
delever_risk_asset_scalar = 0.0
taco_opportunity_size_scalar = 0.0
crisis_enabled = true
macro_enabled = true
taco_enabled = true
panic_reversal_enabled = false

[strategy_plugins.outputs]
output_dir = "data/output/tqqq_growth_income/plugins/market_regime_control"
'''


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


def _write_gapped_history(path: Path, *, as_of: str = "2026-07-21") -> list[str]:
    start = date.fromisoformat(as_of) - timedelta(days=252)
    dates = [start + timedelta(days=offset) for offset in range(253)]
    omitted = dates.pop(126)
    rows = ["session_date,close", *(f"{value},{100.0 + index}" for index, value in enumerate(dates))]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return [value.isoformat() for value in dates if value != omitted]


def _write_present_package(
    path: Path, *, as_of: str = "2026-07-17", qsp_commit_sha: str = "b" * 40, prices_bytes: bytes = b"prices"
) -> str:
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
        "attack_symbol": "TQQQ",
        "as_of": as_of,
        "benchmark_symbol": "QQQ",
        "credit_pairs": ["HYG:IEF", "LQD:IEF"],
        "crisis_enabled": True,
        "delever_risk_asset_scalar": 0.0,
        "enabled": True,
        "event_set": "geopolitical-deescalation",
        "external_stress_actionable": False,
        "financial_symbols": ["XLF", "KRE"],
        "macro_enabled": True,
        "mode": "shadow",
        "panic_reversal_enabled": False,
        "plugin": "market_regime_control",
        "prices": "@input:prices",
        "rate_symbols": ["IEF", "TLT"],
        "realized_vol_requires_confirmation": True,
        "realized_vol_threshold": 0.3,
        "strategy": "tqqq_growth_income",
        "strategy_policy": "levered_growth_income_v1",
        "taco_enabled": True,
        "taco_opportunity_size_scalar": 0.0,
        "vix3m_symbols": ["VIX3M", "^VIX3M", "VXV", "^VXV"],
        "vix_symbols": ["VIX", "^VIX", "VIXCLS"],
    }
    package = {
        "as_of": as_of,
        "config": {"sha256": runner._sha256(runner._canonical_json(config_value)), "value": config_value},
        "inputs": {
            "external_context": {"status": "ABSENT"},
            "prices": {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(prices_bytes), "size_bytes": len(prices_bytes)},
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


def _write_qsp_bundle(
    path: Path, *, as_of: str = "2026-07-21", observed_dates: list[str] | None = None
) -> tuple[str, bytes, bytes]:
    start = date.fromisoformat(as_of) - timedelta(days=251)
    dates = observed_dates or [(start + timedelta(days=offset)).isoformat() for offset in range(252)]
    rows = []
    for offset, observed in enumerate(dates):
        for symbol_index, symbol in enumerate(sorted(QSP_SYMBOLS)):
            close = 100.1 + offset + symbol_index / 10
            rows.append(
                ",".join(
                    (
                        symbol,
                        observed,
                        format(close - 0.2, ".17g"),
                        format(close + 0.2, ".17g"),
                        format(close - 0.4, ".17g"),
                        format(close, ".17g"),
                        format(1000.1 + offset + symbol_index, ".17g"),
                    )
                )
            )
    raw = ("symbol,as_of,open,high,low,close,volume\n" + "\n".join(rows) + "\n").encode("ascii")
    qqq_rows = [row.split(",") for row in rows if row.startswith("QQQ,")]
    benchmark = ("session_date,close\n" + "".join(f"{row[1]},{row[5]}\n" for row in qqq_rows)).encode("ascii")
    manifest = {
        "config": {"filename": "config.toml", "sha256": runner._sha256(QSP_CONFIG_BYTES), "size_bytes": len(QSP_CONFIG_BYTES)},
        "external_context": {"status": "ABSENT"},
        "prices": {
            "filename": "prices.csv",
            "first_date": rows[0].split(",")[1],
            "format": "qsp.t2b3.long_price_csv.v1",
            "last_date": as_of,
            "row_count": len(rows),
            "sha256": runner._sha256(raw),
            "size_bytes": len(raw),
            "symbols": sorted(QSP_SYMBOLS),
        },
        "producer": {
            "commit_sha": QSP_COMMIT,
            "entrypoint": "quant_strategy_plugins.tqqq_research_input_bundle",
            "repository": "QuantStrategyLab/QuantStrategyPlugins",
        },
        "projection": {
            "benchmark_sha256": runner._sha256(benchmark),
            "benchmark_size_bytes": len(benchmark),
            "first_date": qqq_rows[0][1],
            "last_date": as_of,
            "raw_sha256": runner._sha256(raw),
            "row_count": len(qqq_rows),
            "symbol": "QQQ",
            "transform_id": "qsp.t2b3.qqq_session_date_close_csv",
            "transform_version": "1",
        },
        "provider": {
            "auto_adjust": True,
            "credentials": "ABSENT",
            "end_exclusive": "2026-07-22",
            "path": "quant_strategy_plugins.yfinance_prices:download_price_history",
            "provider_id": "yahoo_yfinance_public",
            "requested_symbols": list(QSP_SYMBOLS),
            "start": "2010-01-01",
        },
        "schema": "qsl.t2b3.qqq_price_projection_bundle.v1",
        "session": {
            "as_of": as_of,
            "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF",
            "session_id": f"XNAS:{as_of}",
            "source": "LAST_COMPLETE_QQQ_ROW",
        },
        "status": "READY",
    }
    manifest_bytes = runner._canonical_json(manifest)
    path.mkdir()
    (path / "config.toml").write_bytes(QSP_CONFIG_BYTES)
    (path / "prices.csv").write_bytes(raw)
    (path / "manifest.json").write_bytes(manifest_bytes)
    return runner._sha256(manifest_bytes), raw, benchmark


def _write_qsp_present_package(tmp_path: Path, *, observed_dates: list[str] | None = None) -> tuple[Path, str, Path, str, bytes]:
    bundle_path = tmp_path / "qsp-bundle"
    package_path = tmp_path / "tqqq-market-regime-control-present-2026-07-21.json"
    manifest_digest, raw, benchmark = _write_qsp_bundle(bundle_path, observed_dates=observed_dates)
    package_digest = _write_present_package(package_path, as_of="2026-07-21", qsp_commit_sha=QSP_COMMIT, prices_bytes=raw)
    final_path = package_path.with_name(f"tqqq-market-regime-control-present-2026-07-21-{package_digest}.json")
    package_path.rename(final_path)
    return bundle_path, manifest_digest, final_path, package_digest, benchmark


def _install_decision_spy(monkeypatch) -> list[object]:
    import us_equity_strategies.entrypoints as entrypoints

    contexts: list[object] = []
    decision = SimpleNamespace(positions={"TQQQ": 1.0}, budgets={"risk": 1.0}, risk_flags=("ok",), diagnostics={"source": "spy"})

    def compute(context):
        contexts.append(context)
        return decision

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", compute, raising=False)
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    return contexts


def test_runner_publishes_an_absent_only_two_file_package(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    _write_history(history)
    _install_decision_spy(monkeypatch)

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
    absent_parent.mkdir()
    present_parent.mkdir()
    bundle_path, manifest_digest, package_path, package_digest, benchmark = _write_qsp_present_package(tmp_path)
    history.write_bytes(benchmark)
    contexts = _install_decision_spy(monkeypatch)

    absent_decision, _ = run_tqqq_local_no_order(
        benchmark_history_csv=history,
        as_of="2026-07-21",
        session_id="XNAS:2026-07-21",
        output_parent=absent_parent,
    )
    present_decision, present_output = run_tqqq_local_no_order_present(
        output_parent=present_parent,
        input_bundle=bundle_path,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package_path,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
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
    assert envelope["plugin_control"]["package"] == {
        "sha256": package_digest,
        "value": json.loads(package_path.read_text(encoding="utf-8")),
    }


def test_t2b3_consumer_accepts_qsp_exact_17g_provider_float_wire(monkeypatch, tmp_path: Path) -> None:
    bundle_path = tmp_path / "qsp-bundle"
    output_parent = tmp_path / "output"
    package_path = tmp_path / "tqqq-market-regime-control-present-2026-07-21.json"
    output_parent.mkdir()
    manifest_digest, raw, benchmark = _write_qsp_bundle(bundle_path)
    package_digest = _write_present_package(package_path, as_of="2026-07-21", qsp_commit_sha=QSP_COMMIT, prices_bytes=raw)
    package_path = package_path.with_name(f"tqqq-market-regime-control-present-2026-07-21-{package_digest}.json")
    (tmp_path / "tqqq-market-regime-control-present-2026-07-21.json").rename(package_path)
    _install_decision_spy(monkeypatch)

    _, output = run_tqqq_local_no_order_present(
        input_bundle=bundle_path,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package_path,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
        output_parent=output_parent,
    )

    assert b"HYG,2025-11-12,99.899999999999991,100.3,99.699999999999989,100.09999999999999" in raw
    envelope = json.loads((output / "input_envelope.json").read_text(encoding="utf-8"))
    assert base64.b64decode(envelope["market"]["bytes_b64"]) == benchmark


def test_t2b3_consumer_rejects_every_non_dict_present_inputs(tmp_path: Path) -> None:
    bundle_path = tmp_path / "qsp-bundle"
    output_parent = tmp_path / "output"
    package_path = tmp_path / "tqqq-market-regime-control-present-2026-07-21.json"
    output_parent.mkdir()
    manifest_digest, raw, _ = _write_qsp_bundle(bundle_path)
    _write_present_package(package_path, as_of="2026-07-21", qsp_commit_sha=QSP_COMMIT, prices_bytes=raw)
    package = json.loads(package_path.read_text(encoding="utf-8"))
    package["inputs"] = []
    malformed_bytes = runner._canonical_json(package)
    malformed_digest = runner._sha256(malformed_bytes)
    malformed_path = package_path.with_name(f"tqqq-market-regime-control-present-2026-07-21-{malformed_digest}.json")
    malformed_path.write_bytes(malformed_bytes)

    try:
        run_tqqq_local_no_order_present(
            input_bundle=bundle_path,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=malformed_path,
            plugin_control_package_sha256=malformed_digest,
            qsp_commit_sha=QSP_COMMIT,
            output_parent=output_parent,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B2_PRESENT_INVALID"
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("non-dict inputs must fail closed")
    assert list(output_parent.iterdir()) == []


def test_t2b3_cli_rejects_malformed_bundle_manifests_as_present_invalid(monkeypatch, capsys, tmp_path: Path) -> None:
    bundle_path, manifest_digest, package_path, package_digest, _ = _write_qsp_present_package(tmp_path)
    monkeypatch.setattr(sys.modules["__main__"], "__spec__", present.__spec__)
    arguments = [
        "--output-parent",
        str(tmp_path / "output"),
        "--input-bundle",
        str(bundle_path),
        "--input-bundle-manifest-sha256",
        manifest_digest,
        "--plugin-control-package",
        str(package_path),
        "--plugin-control-package-sha256",
        package_digest,
        "--qsp-commit-sha",
        QSP_COMMIT,
    ]

    for manifest in (b"{", b"\xff"):
        (bundle_path / "manifest.json").write_bytes(manifest)
        assert present.main(arguments) == 2
        assert capsys.readouterr().err == "ERROR T2B2_PRESENT_INVALID\n"


def test_t2b3_provider_observed_gap_is_preserved_without_calendar_inference(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    observed_dates = _write_gapped_history(tmp_path / "ignored.csv")
    bundle_path, manifest_digest, package_path, package_digest, _ = _write_qsp_present_package(
        tmp_path, observed_dates=observed_dates
    )
    _install_decision_spy(monkeypatch)

    _, output = run_tqqq_local_no_order_present(
        output_parent=output_parent,
        input_bundle=bundle_path,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package_path,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
    )

    envelope = json.loads((output / "input_envelope.json").read_text(encoding="utf-8"))
    observed = base64.b64decode(envelope["market"]["bytes_b64"]).decode("utf-8").splitlines()[1:]
    assert [row.split(",", 1)[0] for row in observed] == observed_dates
    assert envelope["market"]["last_date"] == observed_dates[-1] == "2026-07-21"


def test_t2b3_present_evidence_marks_provider_observed_unverified_and_ineligible(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, package_digest, _ = _write_qsp_present_package(tmp_path)
    _install_decision_spy(monkeypatch)

    _, output = run_tqqq_local_no_order_present(
        output_parent=output_parent,
        input_bundle=bundle_path,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package_path,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
    )

    package = json.loads(package_path.read_text(encoding="utf-8"))
    control = json.loads((output / "input_envelope.json").read_text(encoding="utf-8"))["plugin_control"]
    assert control == {
        "calendar_authority": "provider_observed_unverified",
        "historical_backfill": False,
        "input_bundle": {"manifest": json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8")), "manifest_sha256": manifest_digest},
        "optimization_eligible": False,
        "package": {"sha256": package_digest, "value": package},
        "status": "PRESENT",
    }


def test_t2b3_forward_cutover_is_not_historical_backfill(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, package_digest, _ = _write_qsp_present_package(tmp_path)
    _install_decision_spy(monkeypatch)

    try:
        run_tqqq_local_no_order_present(
            output_parent=output_parent,
            input_bundle=bundle_path,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package_path,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha="b" * 40,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B2_PRESENT_INVALID"
    else:  # pragma: no cover - forward-only assertion
        raise AssertionError("pre-cutover evidence must be rejected")
    assert list(output_parent.iterdir()) == []


def test_t2b3_gapped_present_and_absent_decisions_are_equal(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "benchmark.csv"
    absent_parent = tmp_path / "absent"
    present_parent = tmp_path / "present"
    absent_parent.mkdir()
    present_parent.mkdir()
    observed_dates = _write_gapped_history(tmp_path / "ignored.csv")
    bundle_path, manifest_digest, package_path, package_digest, benchmark = _write_qsp_present_package(
        tmp_path, observed_dates=observed_dates
    )
    history.write_bytes(benchmark)
    _install_decision_spy(monkeypatch)

    absent_decision, _ = run_tqqq_local_no_order(
        benchmark_history_csv=history,
        as_of="2026-07-21",
        session_id="XNAS:2026-07-21",
        output_parent=absent_parent,
    )
    present_decision, _ = run_tqqq_local_no_order_present(
        output_parent=present_parent,
        input_bundle=bundle_path,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package_path,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
    )

    assert absent_decision is present_decision


def test_present_consumer_rejects_untrusted_package_before_compute(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, _, _ = _write_qsp_present_package(tmp_path)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    def must_not_compute(_context):
        raise AssertionError("invalid package must fail before compute")

    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", must_not_compute, raising=False)
    try:
        run_tqqq_local_no_order_present(
            output_parent=output_parent,
            input_bundle=bundle_path,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package_path,
            plugin_control_package_sha256="c" * 64,
            qsp_commit_sha=QSP_COMMIT,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B2_PRESENT_INVALID"
    else:  # pragma: no cover - fail-closed assertion
        raise AssertionError("invalid package must be rejected")
    assert list(output_parent.iterdir()) == []


def test_present_publication_failure_retains_the_computed_decision(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, package_digest, _ = _write_qsp_present_package(tmp_path)
    decision = SimpleNamespace(positions={}, budgets={}, risk_flags=(), diagnostics={})
    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", lambda _context: decision, raising=False)
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    monkeypatch.setattr(
        runner,
        "_strict_readback",
        lambda *_: (_ for _ in ()).throw(runner._RunnerError("T2B1_READBACK_FAILED")),
    )

    try:
        run_tqqq_local_no_order_present(
            output_parent=output_parent,
            input_bundle=bundle_path,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package_path,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha=QSP_COMMIT,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B1_READBACK_FAILED"
        assert error.decision is decision
    else:  # pragma: no cover - failure-boundary assertion
        raise AssertionError("staged readback failure must be preserved")
    assert list(output_parent.iterdir()) == []


def test_present_consumer_rejects_prices_not_bound_to_benchmark_before_compute(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, _, _ = _write_qsp_present_package(tmp_path)
    package = json.loads(package_path.read_text(encoding="utf-8"))
    package["inputs"]["prices"]["sha256"] = runner._sha256(b"unrelated-prices")
    malformed = runner._canonical_json(package)
    digest = runner._sha256(malformed)
    package_path = package_path.with_name(f"tqqq-market-regime-control-present-2026-07-21-{digest}.json")
    package_path.write_bytes(malformed)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    def must_not_compute(_context):
        raise AssertionError("unbound prices must fail before compute")

    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", must_not_compute, raising=False)
    try:
        run_tqqq_local_no_order_present(
            output_parent=output_parent,
            input_bundle=bundle_path,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package_path,
            plugin_control_package_sha256=digest,
            qsp_commit_sha=QSP_COMMIT,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B2_PRESENT_INVALID"
    else:  # pragma: no cover - integrity-boundary assertion
        raise AssertionError("package prices must bind the exact benchmark bytes")
    assert list(output_parent.iterdir()) == []


def test_present_consumer_explicitly_rejects_noncanonical_as_of_before_compute(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, _, _ = _write_qsp_present_package(tmp_path)
    package = json.loads(package_path.read_text(encoding="utf-8"))
    package["as_of"] = "20260717"
    malformed = runner._canonical_json(package)
    digest = runner._sha256(malformed)
    package_path = package_path.with_name(f"tqqq-market-regime-control-present-20260717-{digest}.json")
    package_path.write_bytes(malformed)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    def must_not_compute(_context):
        raise AssertionError("noncanonical as_of must fail before compute")

    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", must_not_compute, raising=False)
    try:
        run_tqqq_local_no_order_present(
            output_parent=output_parent,
            input_bundle=bundle_path,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package_path,
            plugin_control_package_sha256=digest,
            qsp_commit_sha=QSP_COMMIT,
        )
    except runner._RunnerError as error:
        assert error.code == "T2B2_PRESENT_INVALID"
    else:  # pragma: no cover - canonical-date assertion
        raise AssertionError("noncanonical as_of must be rejected")
    assert list(output_parent.iterdir()) == []


def test_present_consumer_uses_one_verified_benchmark_snapshot_for_compute_and_envelope(monkeypatch, tmp_path: Path) -> None:
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    bundle_path, manifest_digest, package_path, package_digest, verified_bytes = _write_qsp_present_package(tmp_path)
    _install_decision_spy(monkeypatch)
    original_parse_inputs = runner._parse_inputs

    def mutate_between_reads(csv_path, as_of, session_id):
        (bundle_path / "prices.csv").write_bytes(b"mutated")
        return original_parse_inputs(csv_path, as_of, session_id)

    monkeypatch.setattr(runner, "_parse_inputs", mutate_between_reads)

    _, output = run_tqqq_local_no_order_present(
        output_parent=output_parent,
        input_bundle=bundle_path,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package_path,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
    )

    envelope = json.loads((output / "input_envelope.json").read_text(encoding="utf-8"))
    assert envelope["market"]["sha256"] == runner._sha256(verified_bytes)


def test_t2b3_present_consumer_rejects_invalid_prices_status_before_compute(monkeypatch, tmp_path: Path) -> None:
    bundle_path, manifest_digest, package_path, _, _ = _write_qsp_present_package(tmp_path)
    package = json.loads(package_path.read_text(encoding="utf-8"))
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))

    def must_not_compute(_context):
        raise AssertionError("invalid prices status must fail before compute")

    import us_equity_strategies.entrypoints as entrypoints

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", must_not_compute, raising=False)
    for name, prices in (
        ("missing-status", {key: value for key, value in package["inputs"]["prices"].items() if key != "status"}),
        ("non-present-status", {**package["inputs"]["prices"], "status": "ABSENT"}),
        ("extra-key", {**package["inputs"]["prices"], "unexpected": True}),
    ):
        malformed_package = {**package, "inputs": {**package["inputs"], "prices": prices}}
        malformed = runner._canonical_json(malformed_package)
        digest = runner._sha256(malformed)
        malformed_path = package_path.with_name(f"{name}-{digest}.json")
        malformed_path.write_bytes(malformed)
        output_parent = tmp_path / name
        try:
            run_tqqq_local_no_order_present(
                output_parent=output_parent,
                input_bundle=bundle_path,
                input_bundle_manifest_sha256=manifest_digest,
                plugin_control_package=malformed_path,
                plugin_control_package_sha256=digest,
                qsp_commit_sha=QSP_COMMIT,
            )
        except runner._RunnerError as error:
            assert error.code == "T2B2_PRESENT_INVALID"
        else:  # pragma: no cover - fail-closed assertion
            raise AssertionError("invalid prices status must fail closed")
        assert not output_parent.exists()
