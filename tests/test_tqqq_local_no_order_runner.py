from __future__ import annotations

import base64
import inspect
import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from us_equity_snapshot_pipelines import tqqq_local_no_order_runner as runner
from us_equity_snapshot_pipelines import tqqq_local_no_order_present as present
from us_equity_snapshot_pipelines.tqqq_local_no_order_present import run_tqqq_local_no_order_present


def _canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _raw(as_of: str = "2026-07-21", *, short_token: bool = False, gap: bool = False) -> bytes:
    end = date.fromisoformat(as_of)
    dates = [end - timedelta(days=251 - offset) for offset in range(252)]
    if gap:
        dates.pop(126)
        dates.insert(0, end - timedelta(days=252))
    rows = []
    for current in dates:
        for symbol in sorted(present.REQUESTED_SYMBOLS):
            number = "100.0" if short_token else "100"
            rows.append(f"{symbol},{current},{number},{number},{number},{number},1")
    return present.RAW_HEADER + ("\n".join(rows) + "\n").encode()


def _projection(raw: bytes) -> bytes:
    return b"session_date,close\n" + b"".join(
        f"{parts[1]},{parts[5]}\n".encode() for parts in (line.split(",") for line in raw.decode().splitlines()[1:]) if parts[0] == "QQQ"
    )


def _bundle(tmp_path: Path, *, raw: bytes | None = None, as_of: str = "2026-07-21") -> tuple[Path, str, bytes]:
    raw = raw or _raw(as_of)
    benchmark = _projection(raw)
    qqq_dates = [line.split(",")[1] for line in raw.decode().splitlines()[1:] if line.startswith("QQQ,")]
    all_dates = [line.split(",")[1] for line in raw.decode().splitlines()[1:]]
    manifest = {
        "config": {"filename": "config.toml", "sha256": runner._sha256(present._CONFIG_BYTES), "size_bytes": len(present._CONFIG_BYTES)},
        "external_context": {"status": "ABSENT"},
        "prices": {"filename": "prices.csv", "first_date": min(all_dates), "format": "qsp.t2b3.long_price_csv.v1", "last_date": as_of, "row_count": len(all_dates), "sha256": runner._sha256(raw), "size_bytes": len(raw), "symbols": sorted(present.REQUESTED_SYMBOLS)},
        "producer": {"commit_sha": present.QSP_COMMIT, "entrypoint": present.QSP_BUNDLE_ENTRYPOINT, "repository": present.QSP_REPOSITORY},
        "projection": {"benchmark_sha256": runner._sha256(benchmark), "benchmark_size_bytes": len(benchmark), "first_date": qqq_dates[0], "last_date": as_of, "raw_sha256": runner._sha256(raw), "row_count": len(qqq_dates), "symbol": "QQQ", "transform_id": present.TRANSFORM_ID, "transform_version": present.TRANSFORM_VERSION},
        "provider": {"auto_adjust": True, "credentials": "ABSENT", "end_exclusive": "2026-07-22", "path": "quant_strategy_plugins.yfinance_prices:download_price_history", "provider_id": "yahoo_yfinance_public", "requested_symbols": list(present.REQUESTED_SYMBOLS), "start": "2010-01-01"},
        "schema": present.BUNDLE_SCHEMA,
        "session": {"as_of": as_of, "claim": "PROVIDER_OBSERVED_ONLY_NOT_OFFICIAL_XNAS_PROOF", "session_id": f"XNAS:{as_of}", "source": "LAST_COMPLETE_QQQ_ROW"},
        "status": "READY",
    }
    manifest_bytes = _canonical(manifest)
    directory = tmp_path / f"qsp-t2b3-qqq-input-v1-{as_of}-{runner._sha256(manifest_bytes)}"
    directory.mkdir()
    (directory / "config.toml").write_bytes(present._CONFIG_BYTES)
    (directory / "prices.csv").write_bytes(raw)
    (directory / "manifest.json").write_bytes(manifest_bytes)
    return directory, runner._sha256(manifest_bytes), raw


def _package(tmp_path: Path, raw: bytes, *, as_of: str = "2026-07-21", external_context: object = None) -> tuple[Path, str]:
    config = {"as_of": as_of, "enabled": True, "mode": "shadow", "plugin": "market_regime_control", "prices": "@input:prices", "strategy": "tqqq_growth_income"}
    payload = _canonical({})
    package = {
        "as_of": as_of,
        "config": {"sha256": runner._sha256(_canonical(config)), "value": config},
        "inputs": {"external_context": {"status": "ABSENT"} if external_context is None else external_context, "prices": {"status": "PRESENT", "format": "csv", "sha256": runner._sha256(raw), "size_bytes": len(raw)}},
        "payload": {"bytes_b64": base64.b64encode(payload).decode(), "schema_version": "market_regime_control.v1", "sha256": runner._sha256(payload), "size_bytes": len(payload)},
        "producer": {"commit_sha": present.QSP_COMMIT, "entrypoint": present.QSP_ENTRYPOINT, "repository": present.QSP_REPOSITORY},
        "schema": present.PRESENT_SCHEMA,
        "session_id": f"XNAS:{as_of}",
        "status": "PRESENT",
        "subject": {"mode": "shadow", "plugin": "market_regime_control", "strategy": "tqqq_growth_income"},
    }
    data = _canonical(package)
    digest = runner._sha256(data)
    path = tmp_path / f"tqqq-market-regime-control-present-{as_of}-{digest}.json"
    path.write_bytes(data)
    return path, digest


def _install_runner(monkeypatch) -> list[object]:
    import us_equity_strategies.entrypoints as entrypoints

    contexts: list[object] = []
    decision = SimpleNamespace(positions={"TQQQ": 1.0}, budgets={}, risk_flags=(), diagnostics={})
    monkeypatch.setattr(runner, "_source_identity", lambda: (Path("/source"), "a" * 40))
    monkeypatch.setattr(runner, "_runtime_pin", lambda *_: None)
    monkeypatch.setattr(entrypoints, "compute_tqqq_growth_income_decision", lambda context: contexts.append(context) or decision)
    return contexts


def _run(monkeypatch, tmp_path: Path, *, raw: bytes | None = None, qsp_commit: str = present.QSP_COMMIT) -> tuple[object, Path, list[object]]:
    bundle, manifest_digest, raw = _bundle(tmp_path, raw=raw)
    package, package_digest = _package(tmp_path, raw)
    contexts = _install_runner(monkeypatch)
    output = tmp_path / "output"
    output.mkdir()
    decision, destination = run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=manifest_digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=qsp_commit, output_parent=output)
    return decision, destination, contexts


def test_t2b3_cross_repo_golden_vector_pins_composite_contracts_transform_and_qsp_commit() -> None:
    assert tuple(inspect.signature(run_tqqq_local_no_order_present).parameters) == ("input_bundle", "input_bundle_manifest_sha256", "plugin_control_package", "plugin_control_package_sha256", "qsp_commit_sha", "output_parent")
    assert present.QSP_COMMIT == "c798397d9ca9230e404673d7774bac3d478217dc"
    assert present.TRANSFORM_ID == "qsp.t2b3.qqq_session_date_close_csv"
    assert present.TRANSFORM_VERSION == "1"
    assert present.PROJECTION_CONTRACT_SHA256 == "22223aea8b94ab3157c7897eb883fb84c79fa4d6db271f6629bd47e4ca2b8e06"


def test_t2b3_accepts_r_not_equal_b_only_when_present_package_binds_raw(monkeypatch, tmp_path: Path) -> None:
    _, destination, contexts = _run(monkeypatch, tmp_path)
    envelope = json.loads((destination / "input_envelope.json").read_text())
    assert base64.b64decode(envelope["market"]["bytes_b64"]).startswith(b"session_date,close\n")
    assert len(contexts) == 1
    assert envelope["plugin_control"]["status"] == "PRESENT"


@pytest.mark.parametrize("mutate", ["manifest", "raw", "package", "external_context", "short_number"])
def test_t2b3_tamper_and_external_context_fail_closed_before_compute(monkeypatch, tmp_path: Path, mutate: str) -> None:
    raw = _raw(short_token=mutate == "short_number")
    bundle, manifest_digest, raw = _bundle(tmp_path, raw=raw)
    package, package_digest = _package(tmp_path, raw, external_context={"status": "PRESENT"} if mutate == "external_context" else None)
    if mutate == "manifest":
        (bundle / "manifest.json").write_bytes(b"{}")
    if mutate == "raw":
        (bundle / "prices.csv").write_bytes(raw.replace(b",100,", b",101,", 1))
    if mutate == "package":
        (package).write_bytes(package.read_bytes().replace(b'"PRESENT"', b'"ABSENT"', 1))
    contexts = _install_runner(monkeypatch)
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(runner._RunnerError):
        run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=manifest_digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=present.QSP_COMMIT, output_parent=output)
    assert contexts == [] and list(output.iterdir()) == []


def test_t2b3_present_wrapper_is_intentional_opaque_evidence_contract(monkeypatch, tmp_path: Path) -> None:
    _, destination, contexts = _run(monkeypatch, tmp_path)
    control = json.loads((destination / "input_envelope.json").read_text())["plugin_control"]
    assert control["calendar_authority"] == "provider_observed_unverified"
    assert control["historical_backfill"] is False and control["optimization_eligible"] is False
    context = contexts[0]
    assert context.portfolio.metadata == context.state == context.runtime_config == context.capabilities == context.artifacts == {}


def test_t2b3_requires_absolute_bundle_and_package_paths(monkeypatch, tmp_path: Path) -> None:
    bundle, manifest_digest, raw = _bundle(tmp_path)
    package, package_digest = _package(tmp_path, raw)
    _install_runner(monkeypatch)
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(runner._RunnerError):
        run_tqqq_local_no_order_present(input_bundle=bundle.name, input_bundle_manifest_sha256=manifest_digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=present.QSP_COMMIT, output_parent=output)


def test_t2b3_min_as_of_is_fixed_forward_cutover(monkeypatch, tmp_path: Path) -> None:
    raw = _raw("2026-07-20")
    bundle, digest, raw = _bundle(tmp_path, raw=raw, as_of="2026-07-20")
    package, package_digest = _package(tmp_path, raw, as_of="2026-07-20")
    _install_runner(monkeypatch)
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(runner._RunnerError):
        run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=present.QSP_COMMIT, output_parent=output)


def test_t2b3_manifest_end_exclusive_is_authenticated_by_independent_digest(monkeypatch, tmp_path: Path) -> None:
    bundle, digest, raw = _bundle(tmp_path)
    package, package_digest = _package(tmp_path, raw)
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["provider"]["end_exclusive"] = "2026-07-21"
    (bundle / "manifest.json").write_bytes(_canonical(manifest))
    _install_runner(monkeypatch)
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(runner._RunnerError):
        run_tqqq_local_no_order_present(input_bundle=bundle, input_bundle_manifest_sha256=digest, plugin_control_package=package, plugin_control_package_sha256=package_digest, qsp_commit_sha=present.QSP_COMMIT, output_parent=output)


def test_t2b3_provider_observed_gap_is_preserved_without_calendar_inference(monkeypatch, tmp_path: Path) -> None:
    raw = _raw(gap=True)
    _, destination, _ = _run(monkeypatch, tmp_path, raw=raw)
    market = base64.b64decode(json.loads((destination / "input_envelope.json").read_text())["market"]["bytes_b64"])
    assert market == _projection(raw)


def test_t2b3_present_and_absent_decisions_are_equal_for_identical_b(monkeypatch, tmp_path: Path) -> None:
    bundle, manifest_digest, raw = _bundle(tmp_path)
    package, package_digest = _package(tmp_path, raw)
    _install_runner(monkeypatch)
    present_parent = tmp_path / "present"
    absent_parent = tmp_path / "absent"
    present_parent.mkdir()
    absent_parent.mkdir()
    present_decision, _ = run_tqqq_local_no_order_present(
        input_bundle=bundle,
        input_bundle_manifest_sha256=manifest_digest,
        plugin_control_package=package,
        plugin_control_package_sha256=package_digest,
        qsp_commit_sha=present.QSP_COMMIT,
        output_parent=present_parent,
    )
    absent_decision, _ = runner._run_tqqq_local_no_order(
        benchmark_history_csv="<test-projection>",
        as_of="2026-07-21",
        session_id="XNAS:2026-07-21",
        output_parent=absent_parent,
        plugin_control={"status": "ABSENT"},
        market_csv_bytes=_projection(raw),
    )
    assert present_decision is absent_decision


def test_t2b3_independent_qsp_commit_cannot_be_self_attested(monkeypatch, tmp_path: Path) -> None:
    bundle, manifest_digest, raw = _bundle(tmp_path)
    package, package_digest = _package(tmp_path, raw)
    contexts = _install_runner(monkeypatch)
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(runner._RunnerError):
        run_tqqq_local_no_order_present(
            input_bundle=bundle,
            input_bundle_manifest_sha256=manifest_digest,
            plugin_control_package=package,
            plugin_control_package_sha256=package_digest,
            qsp_commit_sha="a" * 40,
            output_parent=output,
        )
    assert contexts == [] and list(output.iterdir()) == []


def test_t2b3_old_cli_is_rejected(capsys) -> None:
    assert present.main(["--benchmark-history-csv", "x", "--as-of", "2026-07-21", "--session-id", "XNAS:2026-07-21", "--output-parent", "x"]) == 2
    assert capsys.readouterr().err == "ERROR T2B2_PRESENT_INVALID\n"
