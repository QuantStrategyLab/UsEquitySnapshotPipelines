from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from quant_platform_kit.data.research_input import (
    research_input_manifest_sha256,
    validate_research_input_manifest,
)
from quant_platform_kit.data.research_mandate import ResearchMandateAuthorityGuard
from quant_platform_kit.ibkr import StrictAdjustedHistoryError, StrictAdjustedHistoryResult
from quant_platform_kit.ibkr.market_data import (
    AdjustedHistoricalCandle,
    StrictAdjustedHistoryDiagnostic,
    StrictAdjustedHistoryProvenance,
)

from scripts import acquire_tqqq_promotion_inputs_ibkr as acquisition_cli
import us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition as acquisition
import us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration as orchestration
import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence as evidence
from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    TqqqOrchestrationAuthority,
    TqqqOrchestrationError,
    orchestrate_tqqq_promotion,
)


RUNNER_REVISION = "a" * 40
RUNNER_TREE_SHA = "b" * 40
AUTHORITY_SHA256 = "c" * 64
MANDATE_RECEIPT_SHA256 = "6" * 64


def _authority() -> TqqqOrchestrationAuthority:
    return TqqqOrchestrationAuthority(
        authority_receipt_sha256=AUTHORITY_SHA256,
        entitlement_receipt_sha256="d" * 64,
        license_receipt_sha256="e" * 64,
        retention_expires_at="2026-12-31T00:00:00Z",
        risk_standard_id="qpk.strategy_promotion_risk_standard.zh-CN.v2",
        risk_standard_sha256="f" * 64,
        platform_execution_revision="1" * 40,
        input_license=orchestration.INPUT_LICENSE,
        input_usage_scope=orchestration.INPUT_USAGE_SCOPE,
    )


def _results() -> dict[str, StrictAdjustedHistoryResult]:
    results: dict[str, StrictAdjustedHistoryResult] = {}
    for symbol in orchestration.TQQQ_PROMOTION_ASSETS:
        first = orchestration.FIRST_ELIGIBLE_SESSION.get(
            symbol, orchestration.FROZEN_XNYS_SESSIONS[0]
        )
        sessions = tuple(
            date.fromisoformat(value)
            for value in orchestration.FROZEN_XNYS_SESSIONS
            if value >= first
        )
        candles = tuple(
            AdjustedHistoricalCandle(
                session=session,
                open=100.0 + index / 100,
                high=101.0 + index / 100,
                low=99.0 + index / 100,
                close=100.5 + index / 100,
                volume=1_000_000.0 + index,
            )
            for index, session in enumerate(sessions)
        )
        results[symbol] = StrictAdjustedHistoryResult(
            candles=candles,
            provenance=StrictAdjustedHistoryProvenance(
                symbol=symbol,
                exchange="SMART",
                currency="USD",
                end_datetime=orchestration.FIXED_CUTOFF,
                duration=orchestration.EXACT_DURATIONS[symbol],
                bar_size="1 day",
                what_to_show="ADJUSTED_LAST",
                use_rth=True,
                format_date=1,
                keep_up_to_date=False,
                returned_row_count=len(candles),
            ),
            diagnostic=StrictAdjustedHistoryDiagnostic(
                classification="exact_match",
                completion_observed=True,
                expected_count=len(candles),
                observed_in_window_count=len(candles),
                missing_count=0,
                extra_count=0,
                duplicate_count=0,
                missing_sessions_sha256="0" * 64,
                extra_sessions_sha256="0" * 64,
                duplicate_sessions_sha256="0" * 64,
                provider_error_code_counts=(),
            ),
        )
    return results


def _allow_pinned_dependency_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        orchestration,
        "_installed_vcs_revision",
        lambda distribution_name: {
            "quant-platform-kit": orchestration.QPK_REVISION,
            "us-equity-strategies": orchestration.UES_REVISION,
        }[distribution_name],
    )


def _fake_producer(
    output_dir: str | Path,
    *,
    input_manifest_sha256: str,
    mandate_receipt_sha256: str,
) -> dict[str, str]:
    output = Path(output_dir)
    output.mkdir()
    evidence = output / "strategy-evidence-package.v2.json"
    terminal = output / "promotion-research-result.v1.json"
    evidence.write_text(
        '{"backtest":{"promotion_run":{"fold_results":[{"params":'
        '{"mandate_receipt_sha256":"'
        + mandate_receipt_sha256
        + '"}}],"locked_oos_result":{"params":{"mandate_receipt_sha256":"'
        + mandate_receipt_sha256
        + '"}}}},"lifecycle_claims":{"learning_only":false,"promotion_eligible":false,'
        '"live_ready":false,"no_order":true,"size_zero_required":true}}'
    )
    terminal.write_text(
        '{"candidate_identity_sha256":"'
        + "2" * 64
        + '","input_manifest_sha256":"'
        + input_manifest_sha256
        + '","status":"EVIDENCE_V2_COMPLETE","promotion_eligible":false,'
        '"live_ready":false,"no_order":true,"size_zero_required":true}'
    )
    return {
        "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
        "promotion_result_sha256": hashlib.sha256(terminal.read_bytes()).hexdigest(),
        "candidate_identity_sha256": "2" * 64,
        "input_manifest_sha256": input_manifest_sha256,
    }


def test_exact_three_results_publish_then_consume_one_mandate_and_run_existing_producer_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    events: list[str] = []
    real_publish = orchestration._publish_input

    def publish(*args, **kwargs):
        events.append("publish")
        return real_publish(*args, **kwargs)

    class Guard:
        def __init__(self, database, *, clock):
            self._guard = ResearchMandateAuthorityGuard(database, clock=clock)

        def issue(self, **kwargs):
            events.append("issue")
            return self._guard.issue(**kwargs)

        def consume(self, mandate, **kwargs):
            events.append("consume")
            return self._guard.consume(mandate, **kwargs)

    consumed_receipts: list[str] = []

    def producer(
        *,
        input_payload,
        config_payload,
        output_dir,
        generated_at,
        mandate_receipt_sha256,
    ):
        events.append("producer")
        consumed_receipts.append(mandate_receipt_sha256)
        manifest = validate_research_input_manifest(input_payload["input_manifest"])
        assert tuple(input_payload["bars"]["symbols"]) == orchestration.TQQQ_PROMOTION_ASSETS
        assert [source["source_id"] for source in manifest["sources"]] == [
            "ibkr:BOXX",
            "ibkr:QQQ",
            "ibkr:TQQQ",
        ]
        assert manifest["producer"] == {
            "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
            "commit_sha": RUNNER_REVISION,
            "tree_sha": RUNNER_TREE_SHA,
            "tool": "tqqq_ibkr_paper_single_acquisition",
            "tool_version": "v1",
        }
        assert input_payload["provenance"]["session_class"] == "paper"
        assert config_payload == {
            "schema_version": "tqqq_etf_only_replay_config.v1",
            "strategy_profile": "tqqq_etf_only_single_strategy_research_v1",
            "signal_model": "qqq_sma_200_close_t_open_t_plus_1",
            "signal_window_sessions": 200,
            "tqqq_nominal_cap": 0.15,
            "boxx_nominal_cap": 0.50,
            "risk_mandate_id": "tqqq_etf_only_research_v1",
            "risk_standard_id": _authority().risk_standard_id,
            "risk_standard_sha256": _authority().risk_standard_sha256,
            "authority_receipt_sha256": AUTHORITY_SHA256,
            "platform_execution_revision": _authority().platform_execution_revision,
            "input_license": orchestration.INPUT_LICENSE,
            "input_usage_scope": orchestration.INPUT_USAGE_SCOPE,
            "session_class": "paper",
        }
        assert generated_at == "2026-08-11T08:00:00Z"
        return _fake_producer(
            output_dir,
            input_manifest_sha256=research_input_manifest_sha256(manifest),
            mandate_receipt_sha256=mandate_receipt_sha256,
        )

    monkeypatch.setattr(orchestration, "_publish_input", publish)
    monkeypatch.setattr(orchestration, "ResearchMandateAuthorityGuard", Guard)
    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", producer)
    validator_calls: list[tuple[object, Path]] = []
    monkeypatch.setattr(
        orchestration,
        "validate_evidence_package_v2",
        lambda payload, *, base_dir: validator_calls.append((payload, Path(base_dir)))
        or (),
        raising=False,
    )

    result = orchestrate_tqqq_promotion(
        _results(),
        authority=_authority(),
        output_root=tmp_path / "runs",
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        clock=lambda: datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
    )

    assert events == ["publish", "issue", "consume", "producer"]
    assert consumed_receipts == [result["mandate_receipt_digest"]]
    assert consumed_receipts != [AUTHORITY_SHA256]
    assert validator_calls == [
        (
            json.loads(
                (
                    tmp_path
                    / "runs"
                    / result["snapshot_digest"]
                    / "evidence"
                    / "strategy-evidence-package.v2.json"
                ).read_bytes()
            ),
            tmp_path / "runs" / result["snapshot_digest"] / "evidence",
        )
    ]
    assert result == {
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
        "asset_count": 3,
        "snapshot_digest": result["snapshot_digest"],
        "evidence_digest": result["evidence_digest"],
        "mandate_receipt_digest": result["mandate_receipt_digest"],
        "rerun_count": 1,
    }
    assert all(len(result[key]) == 64 for key in ("snapshot_digest", "evidence_digest", "mandate_receipt_digest"))
    run_root = tmp_path / "runs" / result["snapshot_digest"]
    assert (run_root / "snapshot" / "bars.json").is_file()
    assert (run_root / "snapshot" / "input-manifest.json").is_file()
    assert run_root.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in run_root.rglob("*") if path.is_file())


def test_referenced_evidence_validation_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)

    def producer(
        *,
        input_payload,
        output_dir,
        mandate_receipt_sha256=MANDATE_RECEIPT_SHA256,
        **_kwargs,
    ):
        manifest = validate_research_input_manifest(input_payload["input_manifest"])
        return _fake_producer(
            output_dir,
            input_manifest_sha256=research_input_manifest_sha256(manifest),
            mandate_receipt_sha256=mandate_receipt_sha256,
        )

    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", producer)
    monkeypatch.setattr(
        orchestration,
        "validate_evidence_package_v2",
        lambda *_args, **_kwargs: ("artifacts.backtest.sha256 mismatch",),
        raising=False,
    )

    with pytest.raises(TqqqOrchestrationError, match="promotion evidence failed") as caught:
        orchestrate_tqqq_promotion(
            _results(),
            authority=_authority(),
            output_root=tmp_path / "runs",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            clock=lambda: datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
        )

    assert "backtest" not in str(caught.value)
    assert caught.value.sanitized_failure["runner_completion_count"] == 0


def test_publish_is_atomic_and_failed_temporary_member_never_appears_final(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "runs"
    digest = "9" * 64
    final = output / digest
    real_write = orchestration._write_private
    calls = 0

    def fail_second(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        assert not final.exists()
        if calls == 2:
            raise OSError("simulated second member failure")
        real_write(path, payload)

    monkeypatch.setattr(orchestration, "_write_private", fail_second)

    with pytest.raises(TqqqOrchestrationError, match="content-addressed"):
        orchestration._publish_input(
            output,
            input_manifest_sha256=digest,
            bars_bytes=b"{}",
            manifest_bytes=b"{}",
        )

    assert not final.exists()
    assert list(output.iterdir()) == []


def test_atomic_rename_failure_preserves_existing_final_and_cleans_only_temp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "runs"
    digest = "9" * 64
    final = output / digest

    monkeypatch.setattr(orchestration, "_readback_input", lambda *_args, **_kwargs: ({}, {}))

    def lose_publish_race(_temporary: Path, destination: Path) -> None:
        destination.mkdir()
        (destination / "existing").write_text("winner", encoding="utf-8")
        raise RuntimeError("simulated no-replace race")

    monkeypatch.setattr(orchestration, "_publish_noreplace", lose_publish_race)

    with pytest.raises(TqqqOrchestrationError, match="publication failed"):
        orchestration._publish_input(
            output,
            input_manifest_sha256=digest,
            bars_bytes=b"{}",
            manifest_bytes=b"{}",
        )

    assert (final / "existing").read_text(encoding="utf-8") == "winner"
    assert list(output.iterdir()) == [final]


def test_sealing_rejects_symlink_root_without_touching_target(tmp_path: Path) -> None:
    target = tmp_path / "outside"
    target.mkdir(mode=0o755)
    member = target / "member"
    member.write_text("private", encoding="utf-8")
    member.chmod(0o644)
    link = tmp_path / "run-root"
    link.symlink_to(target, target_is_directory=True)

    with pytest.raises(TqqqOrchestrationError, match="symlink"):
        orchestration._seal_private_tree(link)

    assert target.stat().st_mode & 0o777 == 0o755
    assert member.stat().st_mode & 0o777 == 0o644


def test_incomplete_or_provenance_mismatched_results_fail_before_publish_or_mandate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    incomplete = _results()
    del incomplete["BOXX"]
    with pytest.raises(TqqqOrchestrationError, match="exact three-input result"):
        orchestrate_tqqq_promotion(
            incomplete,
            authority=_authority(),
            output_root=tmp_path / "incomplete",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )
    assert not (tmp_path / "incomplete").exists()

    mismatched = _results()
    mismatched["TQQQ"] = replace(
        mismatched["TQQQ"],
        provenance=replace(mismatched["TQQQ"].provenance, what_to_show="TRADES"),
    )
    with pytest.raises(TqqqOrchestrationError, match="history result identity"):
        orchestrate_tqqq_promotion(
            mismatched,
            authority=_authority(),
            output_root=tmp_path / "mismatch",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )
    assert not (tmp_path / "mismatch").exists()


def test_published_input_tamper_fails_before_mandate_or_producer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    real_publish = orchestration._publish_input
    events: list[str] = []

    def publish(*args, **kwargs):
        published = real_publish(*args, **kwargs)
        (published / "bars.json").write_bytes(b"{}")
        return published

    class Guard:
        def __init__(self, *_args, **_kwargs):
            events.append("guard")

    monkeypatch.setattr(orchestration, "_publish_input", publish)
    monkeypatch.setattr(orchestration, "ResearchMandateAuthorityGuard", Guard)
    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", lambda **_kwargs: events.append("producer"))

    with pytest.raises(TqqqOrchestrationError, match="snapshot readback"):
        orchestrate_tqqq_promotion(
            _results(),
            authority=_authority(),
            output_root=tmp_path / "runs",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )
    assert events == []


def test_dependency_revision_and_retention_authority_fail_closed_before_publish(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(orchestration, "_installed_vcs_revision", lambda _name: "9" * 40)
    with pytest.raises(TqqqOrchestrationError, match="dependency identity"):
        orchestrate_tqqq_promotion(
            _results(),
            authority=_authority(),
            output_root=tmp_path / "dependency",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
        )
    assert not (tmp_path / "dependency").exists()

    _allow_pinned_dependency_provenance(monkeypatch)
    expired = replace(_authority(), retention_expires_at="2026-08-10T00:00:00Z")
    with pytest.raises(TqqqOrchestrationError, match="retention authority is expired"):
        orchestrate_tqqq_promotion(
            _results(),
            authority=expired,
            output_root=tmp_path / "expired",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            clock=lambda: datetime(2026, 8, 11, tzinfo=UTC),
        )
    assert not (tmp_path / "expired").exists()


def test_producer_failure_retains_only_sanitized_committed_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)

    def fail(*, output_dir, **_kwargs):
        Path(output_dir).mkdir()
        raise RuntimeError("raw provider bars and private runner details")

    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", fail)

    with pytest.raises(TqqqOrchestrationError, match="promotion evidence failed") as caught:
        orchestrate_tqqq_promotion(
            _results(),
            authority=_authority(),
            output_root=tmp_path / "runs",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            clock=lambda: datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
        )

    failure = caught.value.sanitized_failure
    assert failure == {
        "classification": "promotion_evidence_failed",
        "evidence_artifact_count": 0,
        "mandate_receipt_digest": failure["mandate_receipt_digest"],
        "runner_completion_count": 0,
        "runner_invocation_count": 1,
        "snapshot_digest": failure["snapshot_digest"],
        "stage": "promotion_evidence_pre_artifact",
    }
    assert "raw provider bars" not in str(caught.value)


def test_exact_acquisition_order_and_first_failure_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_acquire(app, symbol, **kwargs):
        calls.append({"app": app, "symbol": symbol, **kwargs})
        return SimpleNamespace(symbol=symbol)

    monkeypatch.setattr(acquisition_cli, "acquire_strict_adjusted_last", fake_acquire)
    app = object()
    factory = object()
    results = acquisition_cli.run_exact_acquisition(app, contract_factory=factory)
    assert tuple(results) == acquisition_cli.EXACT_ASSETS == ("QQQ", "TQQQ", "BOXX")
    assert [call["duration"] for call in calls] == ["9 Y", "9 Y", "4 Y"]
    assert all(call["app"] is app and call["stock_factory"] is factory for call in calls)
    assert calls[0]["expected_sessions"][0] == date(2018, 1, 2)
    assert calls[1]["expected_sessions"] == calls[0]["expected_sessions"]
    assert calls[2]["expected_sessions"][0] == date(2022, 12, 28)
    assert all(call["expected_sessions"][-1] == date(2025, 7, 1) for call in calls)

    observed: list[str] = []

    def fail_on_tqqq(_app, symbol, **_kwargs):
        observed.append(symbol)
        if symbol == "TQQQ":
            raise StrictAdjustedHistoryError("material")
        return SimpleNamespace(symbol=symbol)

    monkeypatch.setattr(acquisition_cli, "acquire_strict_adjusted_last", fail_on_tqqq)
    with pytest.raises(StrictAdjustedHistoryError, match="material"):
        acquisition_cli.run_exact_acquisition(object(), contract_factory=object())
    assert observed == ["QQQ", "TQQQ"]


@pytest.mark.parametrize("session_class", ("paper", "live-data-only"))
def test_session_identity_binds_source_manifest_config_candidate_and_evidence(
    session_class: str,
) -> None:
    results = _results()
    bars = orchestration._strict_bars(results)
    payload, _bars_bytes, _manifest_bytes, _manifest_sha256 = orchestration._input_payload(
        bars,
        results,
        _authority(),
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        observed_at="2026-08-11T08:00:00Z",
        session_class=session_class,
    )
    config = orchestration._config(_authority(), session_class=session_class)

    assert payload["provenance"]["session_class"] == session_class
    assert config["session_class"] == session_class
    validated_config = evidence._validate_config(config)
    provenance, _bars, _digest = evidence._validate_input(payload, validated_config)
    assert provenance["session_class"] == session_class

    mismatched = dict(config)
    mismatched["session_class"] = (
        "live-data-only" if session_class == "paper" else "paper"
    )
    with pytest.raises(evidence.TqqqPromotionEvidenceError, match="provider provenance"):
        evidence._validate_input(payload, evidence._validate_config(mismatched))

    missing = dict(config)
    del missing["session_class"]
    with pytest.raises(evidence.TqqqPromotionEvidenceError, match="config"):
        evidence._validate_config(missing)


class _FakeRuntimeApp:
    def __init__(self) -> None:
        self.connect_calls: list[tuple[str, int, int]] = []
        self.disconnect_calls = 0
        self.events: list[str] = []

    def connect(self, host: str, port: int, client_id: int) -> None:
        self.events.append("connect")
        self.connect_calls.append((host, port, client_id))

    def isConnected(self) -> bool:
        return True

    def start_reader(self) -> None:
        self.events.append("start_reader")

    def wait_for_handshake(self) -> bool:
        self.events.append("handshake")
        return True

    def disconnect(self) -> None:
        self.events.append("disconnect")
        self.disconnect_calls += 1

    def sanitized_lifecycle(self):
        return ({"phase": "history", "status": "SUCCESS"},)


def _valid_cli_args() -> list[str]:
    return [
        "--authority-receipt-sha256",
        "1" * 64,
        "--entitlement-receipt-sha256",
        "2" * 64,
        "--license-receipt-sha256",
        "3" * 64,
        "--retention-expires-at",
        "2026-12-31T00:00:00Z",
        "--risk-standard-id",
        "qpk.strategy_promotion_risk_standard.zh-CN.v2",
        "--risk-standard-sha256",
        "4" * 64,
        "--platform-execution-revision",
        "5" * 40,
        "--input-license",
        orchestration.INPUT_LICENSE,
        "--input-usage-scope",
        orchestration.INPUT_USAGE_SCOPE,
    ]


@pytest.mark.parametrize(
    ("extra_args", "expected_port", "expected_session_class"),
    (([], 4002, "paper"), (["--session-mode", "live-data-only"], 4001, "live-data-only")),
)
def test_cli_connects_once_and_emits_only_sanitized_terminal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    extra_args: list[str],
    expected_port: int,
    expected_session_class: str,
) -> None:
    app = _FakeRuntimeApp()
    events: list[str] = []
    monkeypatch.setattr(acquisition_cli, "_LOCAL_RESEARCH_ROOT", tmp_path / "runs")
    monkeypatch.setattr(acquisition_cli, "_require_filevault_local_root", lambda: events.append("filevault"))
    monkeypatch.setattr(
        acquisition_cli,
        "resolve_tqqq_runtime_identity",
        lambda: events.append("identity") or (RUNNER_REVISION, RUNNER_TREE_SHA),
    )
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: (app, object()))
    monkeypatch.setattr(
        acquisition_cli,
        "run_exact_acquisition",
        lambda *_args, **_kwargs: {
            symbol: SimpleNamespace(private_bars="must not serialize")
            for symbol in acquisition_cli.EXACT_ASSETS
        },
    )

    def orchestrate(results, **kwargs):
        events.append("orchestrate")
        assert tuple(results) == acquisition_cli.EXACT_ASSETS
        assert kwargs["output_root"] == tmp_path / "runs"
        assert kwargs["session_class"] == expected_session_class
        return {
            "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
            "asset_count": 3,
            "snapshot_digest": "6" * 64,
            "evidence_digest": "7" * 64,
            "mandate_receipt_digest": "8" * 64,
            "rerun_count": 1,
        }

    monkeypatch.setattr(acquisition_cli, "orchestrate_tqqq_promotion", orchestrate)
    assert acquisition_cli.main([*_valid_cli_args(), *extra_args]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "asset_count": 3,
        "evidence_digest": "7" * 64,
        "lifecycle": [{"phase": "history", "status": "SUCCESS"}],
        "mandate_receipt_digest": "8" * 64,
        "rerun_count": 1,
        "snapshot_digest": "6" * 64,
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
    }
    assert len(app.connect_calls) == 1
    assert app.connect_calls[0][0:2] == ("127.0.0.1", expected_port)
    assert app.disconnect_calls == 1
    assert app.events == ["connect", "start_reader", "handshake", "disconnect"]
    assert events == ["filevault", "identity", "orchestrate"]
    serialized = json.dumps(payload, sort_keys=True).lower()
    for forbidden in ("private_bars", "provider_message", "response_body", "credential", '"bars"', '"price"'):
        assert forbidden not in serialized


def test_cli_filevault_failure_stops_before_runtime_or_provider(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        acquisition_cli,
        "_require_filevault_local_root",
        lambda: (_ for _ in ()).throw(RuntimeError("private failure")),
    )
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: calls.append("runtime"))
    assert acquisition_cli.main(_valid_cli_args()) == 1
    assert calls == []
    assert json.loads(capsys.readouterr().out) == {
        "asset_count": 0,
        "evidence_digest": None,
        "lifecycle": [],
        "mandate_receipt_digest": None,
        "rerun_count": 0,
        "snapshot_digest": None,
        "status": "FAILED_MATERIAL",
    }


def test_committed_caller_has_historical_data_only_api_surface() -> None:
    cli_source = inspect.getsource(acquisition_cli)
    acquisition_source = inspect.getsource(acquisition)
    assert cli_source.count("app.connect(") == 1
    assert acquisition_source.count("self.reqContractDetails(") == 1
    assert acquisition_source.count("self.reqHistoricalData(") == 1
    assert acquisition_source.count("self.cancelHistoricalData(") == 1
    combined = "\n".join((cli_source, acquisition_source))
    for forbidden in (
        "reqAccount",
        "reqPositions",
        "reqOpenOrders",
        "placeOrder",
        "cancelOrder",
        "reqExecutions",
        "reqPnL",
        "reqIds",
        "reqGlobalCancel",
        "reqCompletedOrders",
        "exerciseOptions",
    ):
        assert forbidden not in combined
