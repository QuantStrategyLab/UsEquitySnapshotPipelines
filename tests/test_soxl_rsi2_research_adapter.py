import hashlib
import json
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_rsi2_research_adapter as adapter


def _paths() -> adapter.Rsi2OfflineInputPaths:
    return adapter.Rsi2OfflineInputPaths(Path("manifest"), Path("artifact"), Path("readback"))


def _write_full_input(tmp_path, source_revision):
    rows = []
    start = date(2022, 1, 3)
    for index in range(753):
        day = (start + timedelta(days=index)).isoformat()
        soxx_close = 100.0 + (index % 11) * 0.1
        soxl_close = 30.0 + (index % 7) * 0.2
        for symbol, close in (("SOXL", soxl_close), ("SOXX", soxx_close)):
            rows.append((day, symbol, close, close + 1.0, close - 1.0, close, 100.0))
    raw = ("symbol,as_of,open,high,low,close,volume\n" + "".join(
        f"{symbol},{day},{open_:.17g},{high:.17g},{low:.17g},{close_:.17g},{volume:.17g}\n"
        for day, symbol, open_, high, low, close_, volume in rows
    )).encode()
    digest = hashlib.sha256(raw).hexdigest()
    manifest = {
        "schema": "qsl.research.price_snapshot.v1", "research_only": True,
        "provider": "yahoo_chart", "price_field": "adjusted_close",
        "provider_completeness": "unverified", "calendar_authority": "unverified",
        "canonicalization": "csv.writer_utf8_lf_float17g_v1", "source_revision": source_revision,
        "retrieved_at": "2026-09-14T00:00:00Z", "symbols": ["SOXX", "SOXL"],
        "request": {"start": "2022-01-03", "end_exclusive": "2024-02-01"},
        "sha256": digest, "bytes": len(raw), "counts": {"SOXX": 753, "SOXL": 753},
        "coverage": {symbol: {"start": "2022-01-03", "end": (start + timedelta(days=752)).isoformat()} for symbol in ("SOXX", "SOXL")},
    }
    readback = {
        "schema": "qsl.research.price_snapshot_readback.v1", "canonical_csv_sha256": digest,
        "row_count": 1506, "aligned_observations": 753, "counts": {"SOXX": 753, "SOXL": 753},
        "date_set_equal": True, "unique_symbol_date_rows": True,
        "deterministic_order": "as_of_ascending_then_symbol_ascending",
        "finite_positive_ohlc": True, "finite_nonnegative_volume": True,
        "raw_persisted_bytes_equal": True, "raw_sha256_readback_equal": True,
        "round_trip_canonical_bytes_equal": True,
    }
    manifest_path = tmp_path / "manifest.json"
    artifact_path = tmp_path / "artifact.csv"
    readback_path = tmp_path / "readback.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    artifact_path.write_bytes(raw)
    readback_path.write_text(json.dumps(readback), encoding="utf-8")
    source = adapter.load_rsi2_offline_input(adapter.Rsi2OfflineInputPaths(manifest_path, artifact_path, readback_path))
    return adapter.Rsi2OfflineInputPaths(manifest_path, artifact_path, readback_path), source


def _make_provenance_repo(tmp_path):
    repo = tmp_path / "ues-provenance-repo"
    repo.mkdir()
    paths = (
        "src/us_equity_strategies/research/soxl_core_optimization.py",
        "src/us_equity_strategies/research/soxl_soxx_offline_input_contract.py",
        "src/us_equity_strategies/research/soxl_soxx_typed_baseline_result.py",
    )
    for relative in paths:
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(f"# synthetic provenance fixture: {relative}\n", encoding="utf-8")
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Synthetic Test"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "--quiet", "-m", "fixture"], check=True)
    commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    blobs = {
        relative: subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", f"HEAD:{relative}"], text=True
        ).strip()
        for relative in paths
    }
    return str(repo), commit, blobs


def test_loader_reuses_typed_ues_contract(monkeypatch):
    expected = object()
    monkeypatch.setattr(adapter, "load_offline_input", lambda *args: expected)
    assert adapter.load_rsi2_offline_input(_paths()) is expected


def test_loader_accepts_verified_typed_input_fixture(tmp_path):
    rows = [
        (day, symbol, 100.0, 101.0, 99.0, 100.5, 10.0)
        for day in ("2026-01-02", "2026-01-05")
        for symbol in ("SOXL", "SOXX")
    ]
    raw = ("symbol,as_of,open,high,low,close,volume\n" + "".join(
        f"{symbol},{day},{open_:.17g},{high:.17g},{low:.17g},{close:.17g},{volume:.17g}\n"
        for day, symbol, open_, high, low, close, volume in rows
    )).encode()
    digest = hashlib.sha256(raw).hexdigest()
    manifest = {
        "schema": "qsl.research.price_snapshot.v1",
        "research_only": True,
        "provider": "yahoo_chart",
        "price_field": "adjusted_close",
        "provider_completeness": "unverified",
        "calendar_authority": "unverified",
        "canonicalization": "csv.writer_utf8_lf_float17g_v1",
        "source_revision": "fixture-source",
        "retrieved_at": "2026-01-06T00:00:00Z",
        "symbols": ["SOXX", "SOXL"],
        "request": {"start": "2026-01-02", "end_exclusive": "2026-01-06"},
        "sha256": digest,
        "bytes": len(raw),
        "counts": {"SOXX": 2, "SOXL": 2},
        "coverage": {
            "SOXX": {"start": "2026-01-02", "end": "2026-01-05"},
            "SOXL": {"start": "2026-01-02", "end": "2026-01-05"},
        },
    }
    readback = {
        "schema": "qsl.research.price_snapshot_readback.v1",
        "canonical_csv_sha256": digest,
        "row_count": 4,
        "aligned_observations": 2,
        "counts": {"SOXX": 2, "SOXL": 2},
        "date_set_equal": True,
        "unique_symbol_date_rows": True,
        "deterministic_order": "as_of_ascending_then_symbol_ascending",
        "finite_positive_ohlc": True,
        "finite_nonnegative_volume": True,
        "raw_persisted_bytes_equal": True,
        "raw_sha256_readback_equal": True,
        "round_trip_canonical_bytes_equal": True,
    }
    manifest_path = tmp_path / "manifest.json"
    artifact_path = tmp_path / "artifact.csv"
    readback_path = tmp_path / "readback.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    artifact_path.write_bytes(raw)
    readback_path.write_text(json.dumps(readback), encoding="utf-8")
    source = adapter.load_rsi2_offline_input(
        adapter.Rsi2OfflineInputPaths(manifest_path, artifact_path, readback_path)
    )
    assert type(source).__name__ == "OfflineInput"
    assert source.source_revision == "fixture-source"
    assert source.input_digest


def test_result_maps_only_real_candidate_parameters(monkeypatch):
    proposal = adapter._proposal_from_result({
        "schema": "qsl.research.soxl_rsi2_mean_reversion.v1",
        "outcome": "CHARACTERIZATION_CANDIDATE_FOUND",
        "locked_winner": "RSI2_ENTRY_10_EXIT_70",
        "candidates": ["a", "b"],
    })
    assert proposal.proposed_params == {"candidate_id": "RSI2_ENTRY_10_EXIT_70"}
    assert proposal.recommendation == "research_candidate"


def test_budget_is_checked_before_strategy_calculation(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(adapter, "run_soxl_rsi2_mean_reversion", lambda source: calls.append(source))
    optimize = adapter.make_soxl_rsi2_optimize(
        input_paths=_paths(),
        output_root=tmp_path,
        source_commit="ues",
        source_blobs={},
        ues_repo_root=None,
        source=object(),
    )
    with pytest.raises(adapter.SoxlRsi2ResearchAdapterError, match="budget"):
        optimize(SimpleNamespace(strategy_profile=adapter.PROFILE, domain=adapter.DOMAIN), SimpleNamespace(max_search_iterations=3, max_param_keys=1))
    assert calls == []


def test_identity_mismatch_is_rejected_before_cycle(monkeypatch, tmp_path):
    source = SimpleNamespace(input_digest="a" * 64, source_revision="ues")
    monkeypatch.setattr(adapter, "load_offline_input", lambda *args: source)
    with pytest.raises(adapter.SoxlRsi2ResearchAdapterError, match="identity_mismatch"):
        adapter.run_soxl_rsi2_research_promotion(
            as_of="2026-09-14",
            drift_score=1.0,
            source_revision="ues",
            input_paths=_paths(),
            output_root=tmp_path / "result",
            source_commit="ues",
            source_blobs={},
            ues_repo_root=None,
            research_identity={"code_revision": "wrong"},
            ticket_dir=tmp_path / "tickets",
        )


def test_no_improvement_does_not_claim_comparable_metrics():
    proposal = adapter._proposal_from_result({
        "schema": "qsl.research.soxl_rsi2_mean_reversion.v1",
        "outcome": "NO_IMPROVEMENT",
        "locked_winner": None,
        "candidates": ["a", "b", "c", "d"],
    })
    assert proposal.recommendation == "reject"
    assert proposal.current_metrics is None
    assert proposal.proposed_metrics is None


def test_saved_public_cycle_parks_and_reentry_reuses_result(monkeypatch, tmp_path):
    calls = {"research": 0, "persist": 0}
    monkeypatch.setattr(adapter, "_validate_research_identity", lambda *args: None)
    monkeypatch.setattr(adapter, "load_offline_input", lambda *args: object())

    def research(_source):
        calls["research"] += 1
        return {
            "schema": "qsl.research.soxl_rsi2_mean_reversion.v1",
            "outcome": "NO_IMPROVEMENT",
            "locked_winner": None,
            "candidates": ["UNSCALED_SMA200"],
        }

    monkeypatch.setattr(adapter, "run_soxl_rsi2_mean_reversion", research)
    monkeypatch.setattr(
        adapter,
        "persist_rsi2_mean_reversion_result",
        lambda *args, **kwargs: calls.__setitem__("persist", calls["persist"] + 1),
    )
    identity = {
        key: key
        for key in (
            "code_revision",
            "input_revision",
            "param_space_revision",
            "cost_model_revision",
            "validator_revision",
        )
    }
    kwargs = {
        "as_of": "2026-09-14",
        "drift_score": 1.0,
        "source_revision": "ues-test",
        "input_paths": _paths(),
        "output_root": tmp_path / "result",
        "source_commit": "ues-test",
        "source_blobs": {},
        "ues_repo_root": None,
        "research_identity": identity,
        "ticket_dir": tmp_path / "tickets",
    }
    first = adapter.run_soxl_rsi2_research_promotion(**kwargs)
    second = adapter.run_soxl_rsi2_research_promotion(**kwargs)
    assert first["status"] == "parked"
    assert first["reason"] == "promotion_cycle_completed"
    assert second["reason"] == "saved_research_ticket_terminal"
    assert calls == {"research": 1, "persist": 1}


def test_candidate_found_stops_at_gate_without_shadow(monkeypatch, tmp_path):
    monkeypatch.setattr(adapter, "_validate_research_identity", lambda *args: None)
    monkeypatch.setattr(adapter, "load_offline_input", lambda *args: object())
    monkeypatch.setattr(
        adapter,
        "run_soxl_rsi2_mean_reversion",
        lambda _source: {
            "schema": "qsl.research.soxl_rsi2_mean_reversion.v1",
            "outcome": "CHARACTERIZATION_CANDIDATE_FOUND",
            "locked_winner": "RSI2_ENTRY_5_EXIT_70",
            "candidates": ["a", "b", "c", "d"],
        },
    )
    monkeypatch.setattr(adapter, "persist_rsi2_mean_reversion_result", lambda *args, **kwargs: None)
    identity = {key: key for key in ("code_revision", "input_revision", "param_space_revision", "cost_model_revision", "validator_revision")}
    result = adapter.run_soxl_rsi2_research_promotion(
        as_of="2026-09-14",
        drift_score=1.0,
        source_revision="ues",
        input_paths=_paths(),
        output_root=tmp_path / "result",
        source_commit="ues",
        source_blobs={},
        ues_repo_root=None,
        research_identity=identity,
        ticket_dir=tmp_path / "tickets",
    )
    assert result["status"] == "parked"
    assert result["ticket"]["notes"] == ["promotion_backtest_gate_failed", "missing_promotion_backtest_evidence"]
    assert result["ticket"]["proposed_params"] == {"candidate_id": "RSI2_ENTRY_5_EXIT_70"}


def test_real_ues_runner_persist_and_qpk_reentry(monkeypatch, tmp_path):
    source_repo, source_commit, blobs = _make_provenance_repo(tmp_path)
    paths, source = _write_full_input(tmp_path, "synthetic-source-revision")
    synthetic_as_of = datetime.now(timezone.utc).date().isoformat()
    identity = {
        "code_revision": source_commit,
        "input_revision": source.input_digest,
        "param_space_revision": adapter._PARAM_SPACE_REVISION,
        "cost_model_revision": adapter._COST_MODEL_REVISION,
        "validator_revision": adapter._VALIDATOR_REVISION,
    }
    kwargs = {
        "as_of": synthetic_as_of, "drift_score": 1.0, "source_revision": "synthetic-source-revision",
        "input_paths": paths, "output_root": tmp_path / "result", "source_commit": source_commit,
        "source_blobs": blobs, "ues_repo_root": source_repo,
        "research_identity": identity, "ticket_dir": tmp_path / "tickets",
    }
    first = adapter.run_soxl_rsi2_research_promotion(**kwargs)
    assert first["status"] == "parked"
    assert (tmp_path / "result" / "soxl_rsi2_mean_reversion_v1.json").is_file()
    saved_ticket = json.loads(Path(first["ticket_path"]).read_text(encoding="utf-8"))
    assert saved_ticket["research_progress"]["lifecycle"]["no_improvement_count"] == 0
    second = adapter.run_soxl_rsi2_research_promotion(**kwargs)
    assert second["resumed"] is True
    assert second["reason"] in {"saved_research_ticket_terminal", "saved_research_ticket_reused"}
