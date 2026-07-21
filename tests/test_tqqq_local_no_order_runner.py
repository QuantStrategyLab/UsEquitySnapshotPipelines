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
PROJECTION_CONTRACT_SHA256 = "22223aea8b94ab3157c7897eb883fb84c79fa4d6db271f6629bd47e4ca2b8e06"
QSP_RECOVERY_CONTRACT_SHA256 = "dfeffa2ab9d6d4fa25f8b5ac5525912174910f85bd9ee61caf62b7a87b9172ce"
STAGE2_RECOVERY_CONTRACT_SHA256 = "2b836bb1da2d2762e6851dc3097654998068806d9ff70e7e4924b5fdfbe13933"
TRANSFORM_ID = "qsp.t2b3.qqq_session_date_close_csv"
TRANSFORM_VERSION = "1"
MIN_AS_OF = "2026-07-21"
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
crisis_enabled = true
macro_enabled = true
taco_enabled = true
panic_reversal_enabled = false

[strategy_plugins.outputs]
output_dir = "data/output/tqqq_growth_income/plugins/market_regime_control"
'''


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


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


def _manifest(raw: bytes, benchmark: bytes, as_of: str, *, end_exclusive: str | None = None) -> dict[str, object]:
    first_date = (date.fromisoformat(as_of) - timedelta(days=251)).isoformat()
    return {
        "config": {"filename": "config.toml", "sha256": runner._sha256(CONFIG_BYTES), "size_bytes": len(CONFIG_BYTES)},
        "external_context": {"status": "ABSENT"},
        "prices": {
            "filename": "prices.csv", "first_date": first_date, "format": "qsp.t2b3.long_price_csv.v1", "last_date": as_of,
            "row_count": 252 * len(SYMBOLS), "sha256": runner._sha256(raw), "size_bytes": len(raw), "symbols": sorted(SYMBOLS),
        },
        "producer": {"commit_sha": QSP_COMMIT, "entrypoint": "quant_strategy_plugins.tqqq_research_input_bundle", "repository": "QuantStrategyLab/QuantStrategyPlugins"},
        "projection": {
            "benchmark_sha256": runner._sha256(benchmark), "benchmark_size_bytes": len(benchmark), "first_date": first_date,
            "last_date": as_of, "raw_sha256": runner._sha256(raw), "row_count": 252, "symbol": "QQQ",
            "transform_id": TRANSFORM_ID, "transform_version": TRANSFORM_VERSION,
        },
        "provider": {
            "auto_adjust": True, "credentials": "ABSENT", "end_exclusive": end_exclusive or (date.fromisoformat(as_of) + timedelta(days=1)).isoformat(),
            "path": "quant_strategy_plugins.yfinance_prices:download_price_history", "provider_id": "yahoo_yfinance_public",
            "requested_symbols": list(SYMBOLS), "start": "2010-01-01",
        },
        "schema": "qsl.t2b3.qqq_price_projection_bundle.v1",
        "session": {"as_of": as_of, "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF", "session_id": f"XNAS:{as_of}", "source": "LAST_COMPLETE_QQQ_ROW"},
        "status": "READY",
    }


def _package_config(as_of: str) -> dict[str, object]:
    return {
        "as_of": as_of, "attack_symbol": "TQQQ", "benchmark_symbol": "QQQ", "credit_pairs": ["HYG:IEF", "LQD:IEF"],
        "crisis_enabled": True, "delever_risk_asset_scalar": 0.0, "enabled": True, "event_set": "geopolitical-deescalation",
        "external_stress_actionable": False, "financial_symbols": ["XLF", "KRE"], "macro_enabled": True, "mode": "shadow",
        "panic_reversal_enabled": False, "plugin": "market_regime_control", "prices": "@input:prices", "rate_symbols": ["IEF", "TLT"],
        "realized_vol_requires_confirmation": True, "realized_vol_threshold": 0.30, "strategy": "tqqq_growth_income",
        "strategy_policy": "levered_growth_income_v1", "taco_enabled": True, "taco_opportunity_size_scalar": 0.0,
        "vix3m_symbols": ["VIX3M", "^VIX3M", "VXV", "^VXV"], "vix_symbols": ["VIX", "^VIX", "VIXCLS"],
    }


def _payload(as_of: str) -> dict[str, object]:
    return {
        "as_of": as_of, "audit_summary": {}, "arbiter": {}, "canonical_route": {}, "component_signals": {},
        "configured_mode": "shadow", "consumption_policy": {"evidence_status": "automation_approved", "plugin": "market_regime_control", "position_control_allowed": True, "strategy": "tqqq_growth_income"},
        "effective_mode": "shadow", "execution_controls": {}, "generated_at": "2026-07-21T00:00:00+00:00", "localized_messages": {},
        "log_record": {}, "mode": "shadow", "notification": {}, "plugin": "market_regime_control", "position_control": {},
        "profile": "market_regime_control", "schema_version": "market_regime_control.v1", "strategy": "tqqq_growth_income",
        "strategy_policy": {}, "suggested_action": {}, "target_type": "strategy", "would_trade_if_enabled": False,
    }


def _write_bundle(tmp_path: Path, *, as_of: str = MIN_AS_OF) -> tuple[Path, str, bytes]:
    raw = _raw_prices(as_of)
    benchmark = _benchmark(raw)
    manifest_bytes = _canonical(_manifest(raw, benchmark, as_of))
    digest = runner._sha256(manifest_bytes)
    bundle = tmp_path / f"qsp-t2b3-qqq-input-v1-{as_of}-{digest}"
    bundle.mkdir()
    (bundle / "config.toml").write_bytes(CONFIG_BYTES)
    (bundle / "prices.csv").write_bytes(raw)
    (bundle / "manifest.json").write_bytes(manifest_bytes)
    return bundle, digest, benchmark


def _write_package(tmp_path: Path, *, as_of: str, raw: bytes, external_context: object = None) -> tuple[Path, str]:
    payload_bytes = _canonical(_payload(as_of))
    config = _package_config(as_of)
    package = {
        "as_of": as_of, "config": {"sha256": runner._sha256(_canonical(config)), "value": config},
        "inputs": {"external_context": {"status": "ABSENT"} if external_context is None else external_context, "prices": {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(raw), "size_bytes": len(raw)}},
        "payload": {"bytes_b64": base64.b64encode(payload_bytes).decode("ascii"), "schema_version": "market_regime_control.v1", "sha256": runner._sha256(payload_bytes), "size_bytes": len(payload_bytes)},
        "producer": {"commit_sha": QSP_COMMIT, "entrypoint": "quant_strategy_plugins.strategy_plugin_runner:run_market_regime_control_plugin", "repository": "QuantStrategyLab/QuantStrategyPlugins"},
        "schema": "qsl.tqqq_market_regime_control_present.v1", "session_id": f"XNAS:{as_of}", "status": "PRESENT",
        "subject": {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"},
    }
    value = _canonical(package)
    digest = runner._sha256(value)
    path = tmp_path / f"tqqq-market-regime-control-present-{as_of}-{digest}.json"
    path.write_bytes(value)
    return path, digest


def _spy(monkeypatch):
    import us_equity_strategies.entrypoints as entrypoints

    contexts: list[object] = []
    decision = SimpleNamespace(positions={"TQQQ": 1.0}, budgets={}, risk_flags=(), diagnostics={})
    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", lambda context: contexts.append(context) or decision, raising=False)
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    return contexts, decision


def _run_present(monkeypatch, tmp_path: Path):
    bundle, manifest_digest, benchmark = _write_bundle(tmp_path)
    package, package_digest = _write_package(tmp_path, as_of=MIN_AS_OF, raw=(bundle / "prices.csv").read_bytes())
    parent = tmp_path / "output"
    parent.mkdir()
    contexts, decision = _spy(monkeypatch)
    result = present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=manifest_digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)
    return result, bundle, manifest_digest, benchmark, package, package_digest, contexts, decision


def test_t2b3_cross_repo_golden_vector_pins_composite_contracts_transform_and_qsp_commit(monkeypatch, tmp_path: Path) -> None:
    (decision, output), bundle, digest, benchmark, package, package_digest, _, _ = _run_present(monkeypatch, tmp_path)
    assert (present.QSP_COMMIT, present.PROJECTION_CONTRACT_SHA256, present.QSP_RECOVERY_CONTRACT_SHA256, present.STAGE2_RECOVERY_CONTRACT_SHA256) == (QSP_COMMIT, PROJECTION_CONTRACT_SHA256, QSP_RECOVERY_CONTRACT_SHA256, STAGE2_RECOVERY_CONTRACT_SHA256)
    assert (present.TRANSFORM_ID, present.TRANSFORM_VERSION, present.MIN_AS_OF) == (TRANSFORM_ID, TRANSFORM_VERSION, MIN_AS_OF)
    envelope = json.loads((output / "input_envelope.json").read_text())
    assert base64.b64decode(envelope["market"]["bytes_b64"]) == benchmark
    assert decision.positions == {"TQQQ": 1.0}
    assert bundle.name.endswith(digest) and package.name.endswith(f"{package_digest}.json")


def test_t2b3_accepts_r_not_equal_b_only_when_present_package_binds_raw(monkeypatch, tmp_path: Path) -> None:
    (_, output), bundle, manifest_digest, benchmark, _, package_digest, _, _ = _run_present(monkeypatch, tmp_path)
    raw = (bundle / "prices.csv").read_bytes()
    envelope = json.loads((output / "input_envelope.json").read_text())
    assert raw != benchmark
    assert envelope["plugin_control"] == {"input_bundle": {"manifest": json.loads((bundle / "manifest.json").read_text()), "manifest_sha256": manifest_digest}, "package": {"sha256": package_digest, "value": envelope["plugin_control"]["package"]["value"]}, "status": "PRESENT"}
    assert envelope["plugin_control"]["package"]["value"]["inputs"]["prices"]["sha256"] == runner._sha256(raw)


@pytest.mark.parametrize("token", [b"100.0", b"100.1", b"1e2"])
def test_t2b3_accepts_only_qsp_canonical_binary64_tokens(monkeypatch, tmp_path: Path, token: bytes) -> None:
    bundle, digest, _ = _write_bundle(tmp_path)
    raw = (bundle / "prices.csv").read_bytes().replace(b",100,100,100,100,1", b"," + token + b"," + token + b"," + token + b"," + token + b",1", 1)
    (bundle / "prices.csv").write_bytes(raw)
    package, package_digest = _write_package(tmp_path, as_of=MIN_AS_OF, raw=raw)
    parent = tmp_path / "output"
    parent.mkdir()
    _spy(monkeypatch)
    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)
    assert list(parent.iterdir()) == []


def test_t2b3_present_wrapper_is_intentional_opaque_evidence_contract(monkeypatch, tmp_path: Path) -> None:
    (present_decision, _), _, _, benchmark, _, _, contexts, _ = _run_present(monkeypatch, tmp_path)
    history = tmp_path / "benchmark.csv"
    history.write_bytes(benchmark)
    parent = tmp_path / "absent"
    parent.mkdir()
    absent_decision, _ = run_tqqq_local_no_order(benchmark_history_csv=history, as_of=MIN_AS_OF, session_id=f"XNAS:{MIN_AS_OF}", output_parent=parent)
    assert present_decision is absent_decision and len(contexts) == 2
    assert contexts[0].portfolio.metadata == contexts[1].portfolio.metadata == {}
    assert contexts[0].state == contexts[1].state == contexts[0].runtime_config == contexts[1].runtime_config == {}


def test_t2b3_requires_absolute_bundle_and_package_paths(monkeypatch, tmp_path: Path) -> None:
    bundle, digest, _ = _write_bundle(tmp_path)
    package, package_digest = _write_package(tmp_path, as_of=MIN_AS_OF, raw=(bundle / "prices.csv").read_bytes())
    parent = tmp_path / "output"
    parent.mkdir()
    _spy(monkeypatch)
    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle.relative_to(tmp_path), input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)
    with pytest.raises(runner._RunnerError, match="T2B2_PRESENT_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package.relative_to(tmp_path), plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)


def test_t2b3_min_as_of_is_fixed_forward_cutover(monkeypatch, tmp_path: Path) -> None:
    bundle, digest, _ = _write_bundle(tmp_path, as_of="2026-07-20")
    package, package_digest = _write_package(tmp_path, as_of="2026-07-20", raw=(bundle / "prices.csv").read_bytes())
    parent = tmp_path / "output"
    parent.mkdir()
    _spy(monkeypatch)
    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)


def test_t2b3_manifest_end_exclusive_is_authenticated_by_independent_digest(monkeypatch, tmp_path: Path) -> None:
    bundle, digest, _ = _write_bundle(tmp_path)
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["provider"]["end_exclusive"] = MIN_AS_OF
    (bundle / "manifest.json").write_bytes(_canonical(manifest))
    package, package_digest = _write_package(tmp_path, as_of=MIN_AS_OF, raw=(bundle / "prices.csv").read_bytes())
    parent = tmp_path / "output"
    parent.mkdir()
    _spy(monkeypatch)
    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)


@pytest.mark.parametrize("member", ["config.toml", "prices.csv", "manifest.json"])
def test_t2b3_member_tamper_fails_before_compute(monkeypatch, tmp_path: Path, member: str) -> None:
    bundle, digest, _ = _write_bundle(tmp_path)
    path = bundle / member
    path.write_bytes(path.read_bytes() + b"x")
    package, package_digest = _write_package(tmp_path, as_of=MIN_AS_OF, raw=(bundle / "prices.csv").read_bytes())
    parent = tmp_path / "output"
    parent.mkdir()
    _spy(monkeypatch)
    with pytest.raises(runner._RunnerError, match="T2B3_BUNDLE_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)
    assert list(parent.iterdir()) == []


def test_t2b3_rejects_external_context_present_and_package_self_attestation(monkeypatch, tmp_path: Path) -> None:
    bundle, digest, _ = _write_bundle(tmp_path)
    package, package_digest = _write_package(tmp_path, as_of=MIN_AS_OF, raw=(bundle / "prices.csv").read_bytes(), external_context={"status": "PRESENT"})
    parent = tmp_path / "output"
    parent.mkdir()
    _spy(monkeypatch)
    with pytest.raises(runner._RunnerError, match="T2B2_PRESENT_INVALID"):
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)


def test_t2b3_snapshot_replay_two_file_output_and_persistence_identity(monkeypatch, tmp_path: Path) -> None:
    (decision, output), bundle, digest, benchmark, package, package_digest, _, expected = _run_present(monkeypatch, tmp_path)
    assert decision is expected and {item.name for item in output.iterdir()} == {"input_envelope.json", "decision.json"}
    parent = tmp_path / "fault"
    parent.mkdir()
    original = runner._parse_market_bytes
    def mutate(raw: bytes, as_of: str, session_id: str):
        (bundle / "prices.csv").write_bytes(b"changed")
        return original(raw, as_of, session_id)
    monkeypatch.setattr(runner, "_parse_market_bytes", mutate)
    monkeypatch.setattr(runner, "_strict_readback", lambda *_: (_ for _ in ()).throw(runner._RunnerError("T2B1_READBACK_FAILED")))
    with pytest.raises(runner._RunnerError, match="T2B1_READBACK_FAILED") as error:
        present.run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=QSP_COMMIT, output_parent=parent)
    assert error.value.decision is expected and list(parent.iterdir()) == [] and benchmark.startswith(b"session_date,close\n")


def test_t2b3_cli_rejects_old_caller_supplied_benchmark_as_of_and_session(capsys) -> None:
    assert present.main(["--benchmark-history-csv", "x", "--as-of", MIN_AS_OF, "--session-id", f"XNAS:{MIN_AS_OF}"]) == 2
    assert capsys.readouterr().err == "ERROR T2B2_PRESENT_INVALID\n"
