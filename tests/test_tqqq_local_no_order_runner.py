from __future__ import annotations

import base64
from datetime import date, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from us_equity_snapshot_pipelines import tqqq_local_no_order_present as present
from us_equity_snapshot_pipelines import tqqq_local_no_order_runner as runner
from us_equity_snapshot_pipelines.tqqq_local_no_order_runner import run_tqqq_local_no_order


QSP_COMMIT = "c798397d9ca9230e404673d7774bac3d478217dc"
CONTRACT_SHA256 = "22223aea8b94ab3157c7897eb883fb84c79fa4d6db271f6629bd47e4ca2b8e06"
TRANSFORM_ID = "qsp.t2b3.qqq_session_date_close_csv"
TRANSFORM_VERSION = "1"
SYMBOLS = ("QQQ", "SPY", "TQQQ", "^VIX", "^VIX3M", "HYG", "IEF", "LQD", "XLF", "KRE", "TLT")
CONFIG_BYTES = b'''default_mode = "shadow"

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


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _qsp_package_config(as_of: str) -> dict[str, object]:
    return {
        "as_of": as_of,
        "attack_symbol": "TQQQ",
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
        "realized_vol_threshold": 0.30,
        "strategy": "tqqq_growth_income",
        "strategy_policy": "levered_growth_income_v1",
        "taco_enabled": True,
        "taco_opportunity_size_scalar": 0.0,
        "vix3m_symbols": ["VIX3M", "^VIX3M", "VXV", "^VXV"],
        "vix_symbols": ["VIX", "^VIX", "VIXCLS"],
    }


def _payload(as_of: str) -> dict[str, object]:
    return {
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
        "generated_at": "2026-07-21T00:00:00+00:00",
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


def _raw_prices(as_of: str) -> bytes:
    start = date.fromisoformat(as_of) - timedelta(days=251)
    lines = ["symbol,as_of,open,high,low,close,volume"]
    for offset in range(252):
        session = (start + timedelta(days=offset)).isoformat()
        for index, symbol in enumerate(sorted(SYMBOLS)):
            value = str(100 + offset + index)
            lines.append(f"{symbol},{session},{value},{value},{value},{value},1")
    return ("\n".join(lines) + "\n").encode("ascii")


def _benchmark(raw: bytes) -> bytes:
    return b"session_date,close\n" + b"".join(
        b",".join((fields[1], fields[5])) + b"\n"
        for fields in (line.split(b",") for line in raw.splitlines()[1:])
        if fields[0] == b"QQQ"
    )


def _write_bundle(tmp_path: Path, *, as_of: str = "2026-07-21") -> tuple[Path, str, bytes, str]:
    raw = _raw_prices(as_of)
    benchmark = _benchmark(raw)
    first_date = (date.fromisoformat(as_of) - timedelta(days=251)).isoformat()
    manifest = {
        "config": {"filename": "config.toml", "sha256": runner._sha256(CONFIG_BYTES), "size_bytes": len(CONFIG_BYTES)},
        "external_context": {"status": "ABSENT"},
        "prices": {
            "filename": "prices.csv",
            "first_date": first_date,
            "format": "qsp.t2b3.long_price_csv.v1",
            "last_date": as_of,
            "row_count": 252 * len(SYMBOLS),
            "sha256": runner._sha256(raw),
            "size_bytes": len(raw),
            "symbols": sorted(SYMBOLS),
        },
        "producer": {
            "commit_sha": QSP_COMMIT,
            "entrypoint": "quant_strategy_plugins.tqqq_research_input_bundle",
            "repository": "QuantStrategyLab/QuantStrategyPlugins",
        },
        "projection": {
            "benchmark_sha256": runner._sha256(benchmark),
            "benchmark_size_bytes": len(benchmark),
            "first_date": first_date,
            "last_date": as_of,
            "raw_sha256": runner._sha256(raw),
            "row_count": 252,
            "symbol": "QQQ",
            "transform_id": TRANSFORM_ID,
            "transform_version": TRANSFORM_VERSION,
        },
        "provider": {
            "auto_adjust": True,
            "credentials": "ABSENT",
            "end_exclusive": "2026-07-22",
            "path": "quant_strategy_plugins.yfinance_prices:download_price_history",
            "provider_id": "yahoo_yfinance_public",
            "requested_symbols": list(SYMBOLS),
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
    manifest_bytes = _canonical(manifest)
    digest = runner._sha256(manifest_bytes)
    bundle = tmp_path / f"qsp-t2b3-qqq-input-v1-{as_of}-{digest}"
    bundle.mkdir()
    (bundle / "config.toml").write_bytes(CONFIG_BYTES)
    (bundle / "prices.csv").write_bytes(raw)
    (bundle / "manifest.json").write_bytes(manifest_bytes)
    return bundle, digest, benchmark, runner._sha256(raw)


def _write_present_package(tmp_path: Path, *, as_of: str, raw: bytes, external_context: object = None) -> tuple[Path, str]:
    payload_bytes = _canonical(_payload(as_of))
    config = _qsp_package_config(as_of)
    package = {
        "as_of": as_of,
        "config": {"sha256": runner._sha256(_canonical(config)), "value": config},
        "inputs": {
            "external_context": {"status": "ABSENT"} if external_context is None else external_context,
            "prices": {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(raw), "size_bytes": len(raw)},
        },
        "payload": {
            "bytes_b64": base64.b64encode(payload_bytes).decode("ascii"),
            "schema_version": "market_regime_control.v1",
            "sha256": runner._sha256(payload_bytes),
            "size_bytes": len(payload_bytes),
        },
        "producer": {
            "commit_sha": QSP_COMMIT,
            "entrypoint": "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin",
            "repository": "QuantStrategyLab/QuantStrategyPlugins",
        },
        "schema": "qsl.tqqq_market_regime_control_present.v1",
        "session_id": f"XNAS:{as_of}",
        "status": "PRESENT",
        "subject": {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"},
    }
    value = _canonical(package)
    digest = runner._sha256(value)
    path = tmp_path / f"tqqq-market-regime-control-present-{as_of}-{digest}.json"
    path.write_bytes(value)
    return path, digest


def _install_decision_spy(monkeypatch) -> tuple[list[object], object]:
    import us_equity_strategies.entrypoints as entrypoints

    contexts: list[object] = []
    decision = SimpleNamespace(positions={"TQQQ": 1.0}, budgets={"risk": 1.0}, risk_flags=("ok",), diagnostics={"source": "spy"})

    def compute(context):
        contexts.append(context)
        return decision

    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", compute, raising=False)
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    return contexts, decision


def _run_present(monkeypatch, tmp_path: Path):
    bundle, manifest_digest, benchmark, raw_digest = _write_bundle(tmp_path)
    package, package_digest = _write_present_package(tmp_path, as_of="2026-07-21", raw=(bundle / "prices.csv").read_bytes())
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    contexts, decision = _install_decision_spy(monkeypatch)
    result = present.run_tqqq_local_no_order_present(
        input_bundle=bundle,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=QSP_COMMIT,
        output_parent=output_parent,
    )
    return result, bundle, manifest_digest, benchmark, raw_digest, package, package_digest, contexts, decision


def test_t2b3_cross_repo_golden_vector_pins_contract_transform_and_cqsp(monkeypatch, tmp_path: Path) -> None:
    """The frozen QSP vector derives B locally; UESP never imports QSP's projector."""
    (decision, output), bundle, manifest_digest, benchmark, _, package, package_digest, _, _ = _run_present(monkeypatch, tmp_path)

    assert present.QSP_COMMIT == QSP_COMMIT
    assert present.CONTRACT_SHA256 == CONTRACT_SHA256
    assert present.TRANSFORM_ID == TRANSFORM_ID
    assert present.TRANSFORM_VERSION == TRANSFORM_VERSION
    envelope = json.loads((output / "input_envelope.json").read_text(encoding="utf-8"))
    assert base64.b64decode(envelope["market"]["bytes_b64"]) == benchmark
    assert envelope["market"]["sha256"] == runner._sha256(benchmark)
    assert decision.positions == {"TQQQ": 1.0}
    assert bundle.name.endswith(manifest_digest)
    assert package.name.endswith(f"{package_digest}.json")


def test_t2b3_accepts_r_not_equal_b_only_when_present_package_binds_raw(monkeypatch, tmp_path: Path) -> None:
    (_, output), bundle, manifest_digest, benchmark, raw_digest, _, package_digest, _, _ = _run_present(monkeypatch, tmp_path)

    assert (bundle / "prices.csv").read_bytes() != benchmark
    envelope = json.loads((output / "input_envelope.json").read_text(encoding="utf-8"))
    assert envelope["plugin_control"] == {
        "input_bundle": {"manifest": json.loads((bundle / "manifest.json").read_text()), "manifest_sha256": manifest_digest},
        "package": {"sha256": package_digest, "value": envelope["plugin_control"]["package"]["value"]},
        "status": "PRESENT",
    }
    assert envelope["plugin_control"]["package"]["value"]["inputs"]["prices"]["sha256"] == raw_digest


def test_t2b3_rejects_statusless_or_self_attested_trust_values_before_compute(monkeypatch, tmp_path: Path) -> None:
    bundle, manifest_digest, _, _ = _write_bundle(tmp_path)
    package, package_digest = _write_present_package(tmp_path, as_of="2026-07-21", raw=(bundle / "prices.csv").read_bytes())
    value = json.loads(package.read_text())
    del value["inputs"]["prices"]["status"]
    package.write_bytes(_canonical(value))
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    _install_decision_spy(monkeypatch)

    with pytest.raises(runner._RunnerError, match="T2B2_PRESENT_INVALID"):
        present.run_tqqq_local_no_order_present(
            input_bundle=bundle,
            input_bundle_manifest_sha256=runner._sha256((bundle / "manifest.json").read_bytes()),
            plugin_control_package=package,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha=QSP_COMMIT,
            output_parent=output_parent,
        )
    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(
            input_bundle=bundle,
            input_bundle_manifest_sha256="0" * 64,
            plugin_control_package=package,
            plugin_control_package_sha256=runner._sha256(package.read_bytes()),
            qsp_commit_sha=QSP_COMMIT,
            output_parent=output_parent,
        )
    assert list(output_parent.iterdir()) == []


@pytest.mark.parametrize("mutation", ["extra", "provider", "external_context", "session", "raw"])
def test_t2b3_bundle_lineage_mismatches_fail_before_compute(monkeypatch, tmp_path: Path, mutation: str) -> None:
    bundle, manifest_digest, _, _ = _write_bundle(tmp_path)
    package, package_digest = _write_present_package(tmp_path, as_of="2026-07-21", raw=(bundle / "prices.csv").read_bytes())
    if mutation == "extra":
        (bundle / "extra").write_text("x")
    elif mutation == "raw":
        (bundle / "prices.csv").write_bytes((bundle / "prices.csv").read_bytes().replace(b",100,100,100,100,1", b",101,101,101,101,1", 1))
    else:
        manifest = json.loads((bundle / "manifest.json").read_text())
        if mutation == "provider":
            manifest["provider"]["auto_adjust"] = False
        elif mutation == "external_context":
            manifest["external_context"] = {"status": "PRESENT"}
        else:
            manifest["session"]["session_id"] = "XNAS:2026-07-20"
        (bundle / "manifest.json").write_bytes(_canonical(manifest))
        manifest_digest = runner._sha256((bundle / "manifest.json").read_bytes())
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    _install_decision_spy(monkeypatch)

    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(
            input_bundle=bundle,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha=QSP_COMMIT,
            output_parent=output_parent,
        )
    assert list(output_parent.iterdir()) == []


def test_t2b3_external_context_present_fails_before_compute(monkeypatch, tmp_path: Path) -> None:
    bundle, manifest_digest, _, _ = _write_bundle(tmp_path)
    package, package_digest = _write_present_package(
        tmp_path, as_of="2026-07-21", raw=(bundle / "prices.csv").read_bytes(), external_context={"status": "PRESENT"}
    )
    output_parent = tmp_path / "output"
    output_parent.mkdir()
    _install_decision_spy(monkeypatch)

    with pytest.raises(runner._RunnerError, match="T2B2_PRESENT_INVALID"):
        present.run_tqqq_local_no_order_present(
            input_bundle=bundle,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha=QSP_COMMIT,
            output_parent=output_parent,
        )


def test_t2b3_present_is_evidence_only_and_matches_absent_decision(monkeypatch, tmp_path: Path) -> None:
    (present_decision, _), _, _, benchmark, _, _, _, contexts, _ = _run_present(monkeypatch, tmp_path)
    absent_parent = tmp_path / "absent"
    absent_parent.mkdir()
    history = tmp_path / "benchmark.csv"
    history.write_bytes(benchmark)
    absent_decision, _ = run_tqqq_local_no_order(
        benchmark_history_csv=history,
        as_of="2026-07-21",
        session_id="XNAS:2026-07-21",
        output_parent=absent_parent,
    )

    assert present_decision is absent_decision
    assert len(contexts) == 2
    assert contexts[0].portfolio.metadata == contexts[1].portfolio.metadata == {}
    assert contexts[0].market_data["benchmark_history"].equals(contexts[1].market_data["benchmark_history"])
    assert contexts[0].state == contexts[1].state == {}
    assert contexts[0].runtime_config == contexts[1].runtime_config == {}
    assert contexts[0].capabilities == contexts[1].capabilities == {}
    assert contexts[0].artifacts == contexts[1].artifacts == {}


def test_t2b3_snapshot_survives_later_path_mutation_and_readback_preserves_identity(monkeypatch, tmp_path: Path) -> None:
    (decision, _), bundle, manifest_digest, benchmark, _, package, package_digest, _, expected_decision = _run_present(monkeypatch, tmp_path)
    assert decision is expected_decision
    output_parent = tmp_path / "fault"
    output_parent.mkdir()
    original_parse = runner._parse_market_bytes

    def mutate_after_snapshot(raw: bytes, as_of: str, session_id: str):
        (bundle / "prices.csv").write_bytes(b"mutated")
        return original_parse(raw, as_of, session_id)

    monkeypatch.setattr(runner, "_parse_market_bytes", mutate_after_snapshot)
    monkeypatch.setattr(runner, "_strict_readback", lambda *_: (_ for _ in ()).throw(runner._RunnerError("T2B1_READBACK_FAILED")))
    with pytest.raises(runner._RunnerError, match="T2B1_READBACK_FAILED") as error:
        present.run_tqqq_local_no_order_present(
            input_bundle=bundle,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha=QSP_COMMIT,
            output_parent=output_parent,
        )
    assert error.value.decision is expected_decision
    assert list(output_parent.iterdir()) == []
    assert benchmark.startswith(b"session_date,close\n")


def test_t2b3_cli_rejects_old_caller_supplied_benchmark_as_of_and_session(capsys) -> None:
    assert present.main(["--benchmark-history-csv", "x", "--as-of", "2026-07-21", "--session-id", "XNAS:2026-07-21"]) == 2
    assert capsys.readouterr().err == "ERROR T2B2_PRESENT_INVALID\n"
