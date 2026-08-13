from __future__ import annotations

import hashlib
import inspect
import json
import sys
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

import us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition as acquisition
import us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration as orchestration
import us_equity_snapshot_pipelines.lifecycle.tqqq_promotion_evidence as evidence
from scripts import acquire_tqqq_promotion_inputs_ibkr as acquisition_cli
from scripts import run_existing_tqqq_snapshot_diagnostic as diagnostic_cli
from scripts import run_existing_tqqq_snapshot_promotion as snapshot_cli
from us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration import (
    TqqqOrchestrationAuthority,
    TqqqOrchestrationError,
    orchestrate_existing_tqqq_snapshot_diagnostic,
    orchestrate_existing_tqqq_snapshot_promotion,
    orchestrate_tqqq_promotion,
)

SNAPSHOT_REVISION = "a" * 40
SNAPSHOT_TREE_SHA = "b" * 40
RUNNER_REVISION = "c" * 40
RUNNER_TREE_SHA = "d" * 40
AUTHORITY_SHA256 = "e" * 64
MANDATE_RECEIPT_SHA256 = "6" * 64
RUNTIME_MANIFEST = {
    "schema_version": "qsl.tqqq.offline-replay-runtime.v1",
    "uesp_revision": RUNNER_REVISION,
    "lockfile_sha256": "7" * 64,
    "qpk_revision": "8" * 40,
    "ues_revision": "9" * 40,
    "python_major_minor": "3.12",
}


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
        + '"}}}},"lifecycle_claims":{"learning_only":true,"promotion_eligible":false,'
        '"live_ready":false,"no_order":true,"size_zero_required":true}}'
    )
    terminal.write_text(
        '{"candidate_identity_sha256":"'
        + "2" * 64
        + '","input_manifest_sha256":"'
        + input_manifest_sha256
        + '","status":"EVIDENCE_V2_COMPLETE","promotion_eligible":false,'
        '"learning_only":true,"live_ready":false,"no_order":true,'
        '"size_zero_required":true}'
    )
    return {
        "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
        "promotion_result_sha256": hashlib.sha256(terminal.read_bytes()).hexdigest(),
        "candidate_identity_sha256": "2" * 64,
        "input_manifest_sha256": input_manifest_sha256,
    }


def _write_private_json(path: Path, payload: dict[str, object]) -> str:
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    path.chmod(0o600)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _runner_consumable_execution_binding(
    *,
    authority_receipt: Path,
    authority_receipt_sha256: str,
    runtime_manifest_path: Path,
    runtime_manifest_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": "qsl.tqqq.execution-binding-record.runner-consumable.v3",
        "binding": {
            "immutable_snapshot_identity": {"snapshot_digest": "3" * 64},
            "source_mandate_identity": {"mandate_receipt_digest": "2" * 64},
            "authority_identity": {
                "authority_receipt": str(authority_receipt),
                "authority_receipt_sha256": authority_receipt_sha256,
                "entitlement_receipt_sha256": "d" * 64,
                "license_receipt_sha256": "e" * 64,
                "retention_expires_at": "2026-12-31T00:00:00Z",
                "risk_standard_id": _authority().risk_standard_id,
                "risk_standard_sha256": _authority().risk_standard_sha256,
                "platform_execution_revision": _authority().platform_execution_revision,
            },
        },
        "execution": {
            "snapshot_execution_identity": {
                "revision": SNAPSHOT_REVISION,
                "tree_sha": SNAPSHOT_TREE_SHA,
            },
            "runner_runtime_identity": {
                "revision": RUNNER_REVISION,
                "tree_sha": RUNNER_TREE_SHA,
            },
            "runtime_manifest": {
                "identity": dict(RUNTIME_MANIFEST),
                "materialization": {
                    "manifest_path": str(runtime_manifest_path),
                    "manifest_sha256": runtime_manifest_sha256,
                    "python_executable": str(Path(sys.executable).resolve()),
                },
            },
            "session_identity": {"session_class": "live-data-only"},
        },
        "verification": {
            "authority_transaction_consumed": True,
            "evidence_artifact_count": 0,
            "evidence_runner_invocation_count": 1,
            "evidence_runner_completion_count": 0,
            "provider_reacquisition": 0,
            "second_evidence_run": 0,
            "safety": {
                "no_order": True,
                "size_zero_required": True,
                "order_calls": 0,
                "account_positions_funds_orders_executions_capital_calls": 0,
                "raw_bars_dates_prices_volumes_provider_messages_logged": 0,
            },
        },
    }


def test_runner_consumable_execution_binding_loads_only_explicit_matching_identities(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(diagnostic_cli, "_current_runtime_manifest", lambda: RUNTIME_MANIFEST)
    monkeypatch.setattr(
        diagnostic_cli, "_current_runtime_identity", lambda: (RUNNER_REVISION, RUNNER_TREE_SHA)
    )
    authority_receipt = tmp_path / "authority.json"
    authority_receipt_sha256 = _write_private_json(authority_receipt, {"status": "authorized"})
    runtime_manifest = tmp_path / "runtime-manifest.json"
    runtime_manifest_sha256 = _write_private_json(runtime_manifest, RUNTIME_MANIFEST)
    binding = tmp_path / "execution-binding.json"
    binding_sha256 = _write_private_json(
        binding,
        _runner_consumable_execution_binding(
            authority_receipt=authority_receipt,
            authority_receipt_sha256=authority_receipt_sha256,
            runtime_manifest_path=runtime_manifest,
            runtime_manifest_sha256=runtime_manifest_sha256,
        ),
    )

    assert diagnostic_cli._load_execution_binding(
        binding,
        binding_sha256,
        risk_standard_id=_authority().risk_standard_id,
        risk_standard_sha256=_authority().risk_standard_sha256,
        platform_execution_revision=_authority().platform_execution_revision,
    ) == (
        diagnostic_cli._LOCAL_RESEARCH_ROOT / ("3" * 64),
        replace(_authority(), authority_receipt_sha256=authority_receipt_sha256),
        "3" * 64,
        "2" * 64,
        SNAPSHOT_REVISION,
        SNAPSHOT_TREE_SHA,
        "live-data-only",
    )


@pytest.mark.parametrize(
    "mutation",
    (
        ("schema_version", "qsl.tqqq.execution-binding-record.runner-consumable.v2"),
        ("binding.immutable_snapshot_identity.snapshot_digest", "not-a-digest"),
        ("execution.runtime_manifest.identity.lockfile_sha256", "not-a-digest"),
        ("execution.runtime_manifest.identity.uesp_revision", "0" * 40),
        ("execution.runtime_manifest.identity.legacy_runtime_identity", "legacy"),
        ("execution.runtime_manifest.materialization.python_executable", "invalid"),
        ("execution.runtime_manifest.materialization.manifest_sha256", "0" * 64),
        ("execution.runner_runtime_identity.tree_sha", "not-a-revision"),
        ("execution.legacy_runtime_identity", "legacy"),
        ("binding.authority_identity.risk_standard_sha256", "0" * 64),
        ("execution.snapshot_execution_identity.tree_sha", "not-a-revision"),
        ("execution.session_identity.session_class", "paper"),
        ("verification.evidence_artifact_count", False),
        ("verification.safety.order_calls", 0.0),
        ("verification.safety.no_order", False),
    ),
)
def test_non_runner_consumable_binding_fails_closed_before_runner_or_provider(
    mutation: tuple[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    authority_receipt = tmp_path / "authority.json"
    authority_receipt_sha256 = _write_private_json(authority_receipt, {"status": "authorized"})
    runtime_manifest = tmp_path / "runtime-manifest.json"
    runtime_manifest_sha256 = _write_private_json(runtime_manifest, RUNTIME_MANIFEST)
    record = _runner_consumable_execution_binding(
        authority_receipt=authority_receipt,
        authority_receipt_sha256=authority_receipt_sha256,
        runtime_manifest_path=runtime_manifest,
        runtime_manifest_sha256=runtime_manifest_sha256,
    )
    target = record
    *parents, leaf = mutation[0].split(".")
    for key in parents:
        target = target[key]  # type: ignore[index]
    target[leaf] = mutation[1]  # type: ignore[index]
    binding = tmp_path / "execution-binding.json"
    binding_sha256 = _write_private_json(binding, record)
    calls: list[str] = []
    monkeypatch.setattr(diagnostic_cli, "_require_filevault", lambda: calls.append("filevault"))
    monkeypatch.setattr(diagnostic_cli, "_current_runtime_manifest", lambda: RUNTIME_MANIFEST)
    monkeypatch.setattr(
        diagnostic_cli, "_current_runtime_identity", lambda: (RUNNER_REVISION, RUNNER_TREE_SHA)
    )
    monkeypatch.setattr(
        diagnostic_cli,
        "orchestrate_existing_tqqq_snapshot_diagnostic",
        lambda *_args, **_kwargs: calls.append("runner"),
    )

    output = tmp_path / "terminal.json"
    assert diagnostic_cli.main(
        [
            "--execution-terminal",
            str(binding),
            "--execution-terminal-sha256",
            binding_sha256,
            "--risk-standard-id",
            _authority().risk_standard_id,
            "--risk-standard-sha256",
            _authority().risk_standard_sha256,
            "--platform-execution-revision",
            _authority().platform_execution_revision,
            "--output",
            str(output),
        ]
    ) == 1
    assert calls == ["filevault"]
    assert json.loads(output.read_bytes()) == {
        "config_digest": None,
        "exception_class": "ValueError",
        "function_identifiers": [],
        "mandate_receipt_digest": None,
        "runner_completion_count": 0,
        "runner_invocation_count": 0,
        "snapshot_digest": None,
        "stage": "preflight_validation_failed",
    }


def test_exact_four_results_publish_then_consume_one_mandate_and_run_existing_producer_once(
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
            "ibkr:QQQM",
            "ibkr:TQQQ",
        ]
        assert manifest["profile"] == "tqqq_core_parity_v1"
        assert manifest["calendar"] == {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "session_date": "2026-07-31",
            "source": "exchange_calendars",
            "source_revision": (
                "exchange_calendars:4.13.2:XNYS:"
                "18b12a992cfb245e6aec7145797e5f0b7b2b03eed880961896ba370d8a7d5380"
            ),
        }
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
            "strategy_profile": "tqqq_core_parity_v1",
            "signal_model": (
                "ues_tqqq_growth_income_core_parity_5loss_20xnys_defensive_cooldown"
            ),
            "signal_window_sessions": 257,
            "tqqq_nominal_cap": 0.15,
            "qqqm_nominal_cap": 0.50,
            "boxx_nominal_cap": 0.50,
            "risk_mandate_id": "tqqq_core_parity_v1",
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
        "asset_count": 4,
        "execution_authorized": False,
        "no_order": True,
        "research_only": True,
        "size_zero_required": True,
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
    failure = caught.value.sanitized_failure
    assert failure["failure_class"] == "referenced_artifact_validation_failed"
    assert failure["recoverability"] == "fresh_human_authority_required"
    assert failure["runner_completion_count"] == 0
    assert failure["stage"] == "promotion_evidence_referenced_artifact_validation"


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
    del incomplete["QQQM"]
    with pytest.raises(TqqqOrchestrationError, match="exact four-input result"):
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
        "failure_class": "promotion_runner_failed",
        "mandate_receipt_digest": failure["mandate_receipt_digest"],
        "recoverability": "fresh_human_authority_required",
        "runner_completion_count": 0,
        "runner_invocation_count": 1,
        "snapshot_digest": failure["snapshot_digest"],
        "stage": "promotion_evidence_runner",
    }
    assert "raw provider bars" not in str(caught.value)


def test_structural_evidence_readback_failure_is_classified_as_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)

    def producer(*, input_payload, output_dir, mandate_receipt_sha256, **_kwargs):
        del mandate_receipt_sha256
        output = Path(output_dir)
        output.mkdir()
        evidence_file = output / "strategy-evidence-package.v2.json"
        terminal_file = output / "promotion-research-result.v1.json"
        evidence_file.write_text("[]", encoding="utf-8")
        terminal_file.write_text("{}", encoding="utf-8")
        return {
            "evidence_sha256": hashlib.sha256(evidence_file.read_bytes()).hexdigest(),
            "promotion_result_sha256": hashlib.sha256(
                terminal_file.read_bytes()
            ).hexdigest(),
            "candidate_identity_sha256": "2" * 64,
            "input_manifest_sha256": research_input_manifest_sha256(
                validate_research_input_manifest(input_payload["input_manifest"])
            ),
        }

    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", producer)

    with pytest.raises(TqqqOrchestrationError) as caught:
        orchestrate_tqqq_promotion(
            _results(),
            authority=_authority(),
            output_root=tmp_path / "runs",
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            clock=lambda: datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
        )

    failure = caught.value.sanitized_failure
    assert failure["failure_class"] == "promotion_evidence_readback_invalid"
    assert failure["stage"] == "promotion_evidence_readback_validation"


def test_unified_duration_covers_every_required_session_at_latest_authority_expiry() -> None:
    latest_request_session = datetime.fromisoformat(
        _authority().retention_expires_at
    ).date()
    required_start_session = date.fromisoformat(
        orchestration.FROZEN_XNYS_SESSIONS[0]
    )

    assert orchestration.EXACT_DURATIONS == {
        symbol: "9 Y" for symbol in orchestration.TQQQ_PROMOTION_ASSETS
    }
    for symbol in orchestration.TQQQ_PROMOTION_ASSETS:
        first_eligible_session = date.fromisoformat(
            orchestration.FIRST_ELIGIBLE_SESSION.get(
                symbol, required_start_session.isoformat()
            )
        )
        duration_years = int(orchestration.EXACT_DURATIONS[symbol].split()[0])
        earliest_covered_session = latest_request_session.replace(
            year=latest_request_session.year - duration_years
        )
        assert earliest_covered_session < first_eligible_session


def test_locked_oos_calendar_identity_is_exact_and_at_least_twelve_months() -> None:
    locked = tuple(
        value
        for value in orchestration.FROZEN_XNYS_SESSIONS
        if "2025-07-02" <= value <= "2026-07-31"
    )

    assert orchestration.FIXED_CUTOFF == "2026-08-03T03:59:59Z"
    assert len(locked) == 272
    assert hashlib.sha256(
        json.dumps(list(locked), separators=(",", ":")).encode()
    ).hexdigest() == "fe4120013da919f99ec3585898c82409e8fc26423df4649377eafa665da103b8"


def test_exact_acquisition_order_and_first_failure_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_acquire(app, symbol, **kwargs):
        calls.append({"app": app, "symbol": symbol, **kwargs})
        return SimpleNamespace(symbol=symbol)

    monkeypatch.setattr(acquisition_cli, "acquire_strict_adjusted_last", fake_acquire)
    app = object()
    factory = object()
    results = acquisition_cli.run_exact_acquisition(app, contract_factory=factory)
    assert tuple(results) == acquisition_cli.EXACT_ASSETS == ("QQQ", "TQQQ", "QQQM", "BOXX")
    assert acquisition_cli.APPLICATION_CALL_CEILING == 8
    acquisition_source = inspect.getsource(acquisition_cli.run_exact_acquisition)
    assert acquisition_source.count("acquire_strict_adjusted_last(") == 1
    assert "while " not in acquisition_source
    assert [call["duration"] for call in calls] == ["9 Y"] * 4
    assert all(call["app"] is app and call["stock_factory"] is factory for call in calls)
    assert calls[0]["expected_sessions"][0] == date(2018, 1, 2)
    assert calls[1]["expected_sessions"] == calls[0]["expected_sessions"]
    assert calls[2]["expected_sessions"][0] == date(2020, 10, 13)
    assert calls[3]["expected_sessions"][0] == date(2022, 12, 28)
    assert all(call["expected_sessions"][-1] == date(2026, 7, 31) for call in calls)

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


def test_application_call_ceiling_blocks_ninth_timeout_cancellation() -> None:
    class App:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def reqContractDetails(self, *_args) -> None:
            self.calls.append("qualification")

        def reqHistoricalData(self, *_args) -> None:
            self.calls.append("historical")

        def cancelHistoricalData(self, *_args) -> None:
            self.calls.append("cancellation")

    app = acquisition_cli._bound_application_calls(App())
    for index in range(4):
        app.reqContractDetails(index, object())
        app.reqHistoricalData(index, object())

    with pytest.raises(TqqqOrchestrationError, match="application call ceiling"):
        app.cancelHistoricalData(4)

    assert app.calls == ["qualification", "historical"] * 4


@pytest.mark.parametrize("session_class", ("paper", "live-data-only"))
def test_current_acquisition_constructs_exact_core_parity_input(
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
    assert tuple(payload["bars"]["symbols"]) == ("QQQ", "TQQQ", "QQQM", "BOXX")
    expected_source_revision = hashlib.sha256(
        orchestration._canonical(
            {
                "authority_receipt_sha256": _authority().authority_receipt_sha256,
                    "calendar_sha256": (
                        "18b12a992cfb245e6aec7145797e5f0b7b2b03eed880961896ba370d8a7d5380"
                    ),
                "candidate_profile": "tqqq_core_parity_v1",
                "entitlement_receipt_sha256": _authority().entitlement_receipt_sha256,
                "fallback": False,
                "fixed_cutoff": orchestration.FIXED_CUTOFF,
                "input_license": orchestration.INPUT_LICENSE,
                "input_usage_scope": orchestration.INPUT_USAGE_SCOPE,
                "license_receipt_sha256": _authority().license_receipt_sha256,
                "observed_at": "2026-08-11T08:00:00Z",
                "official_ibapi_provenance_sha256": (
                    orchestration.OFFICIAL_IBAPI_PROVENANCE_SHA256
                ),
                "provider": orchestration._SESSION_PROVIDER[session_class],
                "qpk_revision": orchestration.QPK_REVISION,
                "requests": [
                    {
                        "bar_size": "1 day",
                        "duration": orchestration.EXACT_DURATIONS[symbol],
                        "end_datetime": "",
                        "format_date": 1,
                        "keep_up_to_date": False,
                        "security": "STK/SMART/USD",
                        "session_class": session_class,
                        "symbol": symbol,
                        "use_rth": True,
                        "what_to_show": "ADJUSTED_LAST",
                    }
                    for symbol in orchestration.TQQQ_PROMOTION_ASSETS
                ],
                "retention_expires_at": _authority().retention_expires_at,
                "retry_count": 0,
                "session_class": session_class,
                "substitution": False,
                "ues_revision": orchestration.UES_REVISION,
                "uesp_revision": RUNNER_REVISION,
                "uesp_tree_sha": RUNNER_TREE_SHA,
            }
        )
    ).hexdigest()
    assert {source["revision"] for source in payload["input_manifest"]["sources"]} == {
        expected_source_revision
    }
    validated_config = evidence._validate_config(config)
    provenance, parsed, manifest_sha256 = evidence._validate_input(
        payload, validated_config
    )
    assert tuple(parsed) == ("BOXX", "QQQ", "QQQM", "TQQQ")
    assert provenance["provider_revision"] == payload["provenance"]["provider_revision"]
    assert manifest_sha256 == research_input_manifest_sha256(
        payload["input_manifest"]
    )


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
            "asset_count": 4,
            "execution_authorized": False,
            "no_order": True,
            "research_only": True,
            "size_zero_required": True,
            "snapshot_digest": "6" * 64,
            "evidence_digest": "7" * 64,
            "mandate_receipt_digest": "8" * 64,
            "rerun_count": 1,
        }

    monkeypatch.setattr(acquisition_cli, "orchestrate_tqqq_promotion", orchestrate)
    assert acquisition_cli.main([*_valid_cli_args(), *extra_args]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "asset_count": 4,
        "evidence_digest": "7" * 64,
        "execution_authorized": False,
        "lifecycle": [{"phase": "history", "status": "SUCCESS"}],
        "mandate_receipt_digest": "8" * 64,
        "no_order": True,
        "research_only": True,
        "rerun_count": 1,
        "snapshot_digest": "6" * 64,
        "size_zero_required": True,
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
        "execution_authorized": False,
        "lifecycle": [],
        "mandate_receipt_digest": None,
        "no_order": True,
        "research_only": True,
        "rerun_count": 0,
        "snapshot_digest": None,
        "size_zero_required": True,
        "status": "PARK_MATERIAL",
    }


def test_cli_expired_authority_stops_before_filevault_runtime_or_provider(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []
    args = _valid_cli_args()
    args[args.index("--retention-expires-at") + 1] = "2000-01-01T00:00:00Z"
    monkeypatch.setattr(
        acquisition_cli,
        "_require_filevault_local_root",
        lambda: calls.append("filevault"),
    )
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: calls.append("runtime"))

    assert acquisition_cli.main(args) == 1
    assert calls == []
    assert json.loads(capsys.readouterr().out)["status"] == "PARK_MATERIAL"


def test_committed_caller_has_historical_data_only_api_surface() -> None:
    cli_source = inspect.getsource(acquisition_cli)
    acquisition_source = inspect.getsource(acquisition)
    assert cli_source.count("app.connect(") == 1
    assert acquisition_source.count("self.reqContractDetails(") == 1
    assert acquisition_source.count("self.reqHistoricalData(") == 1
    assert acquisition_source.count("self.cancelHistoricalData(") == 1
    combined = f"{cli_source}\n{acquisition_source}"
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


def _consumed_diagnostic_run(
    tmp_path: Path,
) -> tuple[Path, str, str, str]:
    authority = _authority()
    bars = orchestration._strict_bars(_results())
    _payload, bars_bytes, manifest_bytes, snapshot_digest = orchestration._input_payload(
        bars,
        _results(),
        authority,
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        observed_at="2026-08-11T08:00:00Z",
        session_class="live-data-only",
    )
    snapshot = orchestration._publish_input(
        tmp_path / "runs",
        input_manifest_sha256=snapshot_digest,
        bars_bytes=bars_bytes,
        manifest_bytes=manifest_bytes,
    )
    config_digest = hashlib.sha256(
        orchestration._canonical(
            orchestration._config(authority, session_class="live-data-only")
        )
    ).hexdigest()
    candidate = orchestration.CandidateRiskIdentity(
        strategy_profile="tqqq_core_parity_v1",
        account_mode="single_strategy_account_v1",
        strategy_revision=orchestration.UES_REVISION,
        runner_revision=RUNNER_REVISION,
        config_sha256=config_digest,
        input_manifest_sha256=snapshot_digest,
        authority_receipt_sha256=authority.authority_receipt_sha256,
    )
    guard = ResearchMandateAuthorityGuard(
        snapshot.parent / "mandate-authority.sqlite3",
        clock=lambda: datetime(2026, 8, 11, 8, 0, tzinfo=UTC),
    )
    mandate = guard.issue(
        candidate_id=candidate.candidate_sha256,
        mandate_id="tqqq_core_parity_v1",
        config_digest=config_digest,
        input_digest=snapshot_digest,
        authority_id=authority.authority_receipt_sha256,
    )
    receipt = guard.consume(
        mandate,
        candidate_id=candidate.candidate_sha256,
        mandate_id="tqqq_core_parity_v1",
        config_digest=config_digest,
        input_digest=snapshot_digest,
        authority_id=authority.authority_receipt_sha256,
    )
    orchestration._seal_private_tree(snapshot.parent)
    return snapshot.parent, snapshot_digest, config_digest, receipt.receipt_digest


def test_existing_snapshot_promotion_consumes_fresh_mandate_and_validates_evidence_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    source_root, snapshot_digest, config_digest, source_receipt_digest = (
        _consumed_diagnostic_run(tmp_path)
    )
    source_hashes = {
        path.relative_to(source_root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source_root.rglob("*")
        if path.is_file()
    }
    monkeypatch.setattr(
        orchestration, "_require_diagnostic_execution_compatibility", lambda *_args: None
    )
    events: list[str] = []
    issued: list[dict[str, str]] = []

    class Guard:
        def __init__(self, database, *, clock):
            self._guard = ResearchMandateAuthorityGuard(database, clock=clock)

        def issue(self, **kwargs):
            events.append("issue")
            issued.append(kwargs)
            return self._guard.issue(**kwargs)

        def consume(self, mandate, **kwargs):
            events.append("consume")
            return self._guard.consume(mandate, **kwargs)

    current_revision = "9" * 40
    current_tree = "8" * 40

    def producer(
        *,
        input_payload,
        config_payload,
        output_dir,
        generated_at,
        mandate_receipt_sha256,
    ):
        events.append("producer")
        assert input_payload["input_manifest"]["producer"]["commit_sha"] == RUNNER_REVISION
        assert config_payload == orchestration._config(
            _authority(), session_class="live-data-only"
        )
        assert generated_at == "2026-08-11T09:00:00Z"
        return _fake_producer(
            output_dir,
            input_manifest_sha256=snapshot_digest,
            mandate_receipt_sha256=mandate_receipt_sha256,
        )

    monkeypatch.setattr(orchestration, "ResearchMandateAuthorityGuard", Guard)
    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", producer)
    validator_calls: list[Path] = []
    monkeypatch.setattr(
        orchestration,
        "validate_evidence_package_v2",
        lambda _payload, *, base_dir: validator_calls.append(Path(base_dir)) or (),
    )

    output_root = tmp_path / "fresh-output"
    result = orchestrate_existing_tqqq_snapshot_promotion(
        source_root,
        expected_snapshot_digest=snapshot_digest,
        expected_source_mandate_receipt_digest=source_receipt_digest,
        authority=_authority(),
        output_root=output_root,
        execution_revision=RUNNER_REVISION,
        execution_tree_sha=RUNNER_TREE_SHA,
        runner_revision=current_revision,
        runner_tree_sha=current_tree,
        session_class="live-data-only",
        clock=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=UTC),
    )

    assert events == ["issue", "consume", "producer"]
    assert result == {
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
        "asset_count": 4,
        "snapshot_digest": snapshot_digest,
        "evidence_digest": result["evidence_digest"],
        "mandate_receipt_digest": result["mandate_receipt_digest"],
        "rerun_count": 1,
    }
    expected_candidate = orchestration.CandidateRiskIdentity(
        strategy_profile="tqqq_core_parity_v1",
        account_mode="single_strategy_account_v1",
        strategy_revision=orchestration.UES_REVISION,
        runner_revision=current_revision,
        config_sha256=config_digest,
        input_manifest_sha256=snapshot_digest,
        authority_receipt_sha256=AUTHORITY_SHA256,
    )
    assert issued == [
        {
            "candidate_id": expected_candidate.candidate_sha256,
            "mandate_id": "tqqq_core_parity_v1",
            "config_digest": config_digest,
            "input_digest": snapshot_digest,
            "authority_id": AUTHORITY_SHA256,
        }
    ]
    run_root = output_root / snapshot_digest
    assert len(validator_calls) == 1
    assert validator_calls[0].name == "evidence"
    assert validator_calls[0].parent.parent == output_root
    assert {path.name for path in run_root.iterdir()} == {
        "evidence",
        "mandate-authority.sqlite3",
    }
    assert all(
        path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)
        for path in (output_root, run_root, *run_root.rglob("*"))
    )
    assert source_hashes == {
        path.relative_to(source_root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in source_root.rglob("*")
        if path.is_file()
    }


def test_existing_snapshot_promotion_failure_retains_sanitized_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    source_root, snapshot_digest, _config_digest, source_receipt_digest = (
        _consumed_diagnostic_run(tmp_path)
    )
    monkeypatch.setattr(
        orchestration, "_require_diagnostic_execution_compatibility", lambda *_args: None
    )

    def fail(*, output_dir, **_kwargs):
        Path(output_dir).mkdir()
        raise RuntimeError("private runner failure")

    monkeypatch.setattr(orchestration, "run_tqqq_promotion_evidence", fail)

    with pytest.raises(TqqqOrchestrationError) as caught:
        orchestrate_existing_tqqq_snapshot_promotion(
            source_root,
            expected_snapshot_digest=snapshot_digest,
            expected_source_mandate_receipt_digest=source_receipt_digest,
            authority=_authority(),
            output_root=tmp_path / "fresh-output",
            execution_revision=RUNNER_REVISION,
            execution_tree_sha=RUNNER_TREE_SHA,
            runner_revision="9" * 40,
            runner_tree_sha="8" * 40,
            session_class="live-data-only",
            clock=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=UTC),
        )

    failure = caught.value.sanitized_failure
    assert failure["failure_class"] == "promotion_runner_failed"
    assert failure["mandate_receipt_digest"]
    assert failure["recoverability"] == "fresh_human_authority_required"
    assert failure["snapshot_digest"] == snapshot_digest
    assert failure["stage"] == "promotion_evidence_runner"


@pytest.mark.parametrize("failure", ("snapshot", "source_config", "output_exists"))
def test_existing_snapshot_promotion_preflight_failure_issues_no_mandate_or_replay(
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    source_root, snapshot_digest, _config_digest, source_receipt_digest = (
        _consumed_diagnostic_run(tmp_path)
    )
    monkeypatch.setattr(
        orchestration, "_require_diagnostic_execution_compatibility", lambda *_args: None
    )
    authority = _authority()
    output_root = tmp_path / "fresh-output"
    if failure == "snapshot":
        (source_root / "snapshot" / "bars.json").write_bytes(b"{}")
    elif failure == "source_config":
        authority = replace(authority, risk_standard_sha256="7" * 64)
    else:
        output_root.mkdir()
    calls: list[str] = []
    monkeypatch.setattr(
        orchestration,
        "ResearchMandateAuthorityGuard",
        lambda *_args, **_kwargs: calls.append("mandate"),
    )
    monkeypatch.setattr(
        orchestration,
        "run_tqqq_promotion_evidence",
        lambda **_kwargs: calls.append("replay"),
    )

    with pytest.raises(TqqqOrchestrationError):
        orchestrate_existing_tqqq_snapshot_promotion(
            source_root,
            expected_snapshot_digest=snapshot_digest,
            expected_source_mandate_receipt_digest=source_receipt_digest,
            authority=authority,
            output_root=output_root,
            execution_revision=RUNNER_REVISION,
            execution_tree_sha=RUNNER_TREE_SHA,
            runner_revision="9" * 40,
            runner_tree_sha="8" * 40,
            session_class="live-data-only",
            clock=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=UTC),
        )
    assert calls == []
    if failure != "output_exists":
        assert not output_root.exists()


def test_existing_snapshot_promotion_cli_is_provider_free_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    events: list[str] = []
    source_root = tmp_path / "source"
    monkeypatch.setattr(snapshot_cli, "_LOCAL_RESEARCH_ROOT", tmp_path / "output")
    monkeypatch.setattr(snapshot_cli, "_require_filevault", lambda: events.append("filevault"))
    monkeypatch.setattr(
        snapshot_cli,
        "_load_execution_binding",
        lambda *_args, **_kwargs: (
            source_root,
            _authority(),
            "3" * 64,
            "2" * 64,
            RUNNER_REVISION,
            RUNNER_TREE_SHA,
            "live-data-only",
        ),
    )
    monkeypatch.setattr(
        snapshot_cli,
        "resolve_tqqq_runtime_identity",
        lambda: events.append("identity") or ("9" * 40, "8" * 40),
    )

    def orchestrate(run_root, **kwargs):
        events.append("orchestrate")
        assert run_root == source_root
        assert kwargs["expected_source_mandate_receipt_digest"] == "2" * 64
        assert kwargs["output_root"] == tmp_path / "output" / AUTHORITY_SHA256
        return {
            "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
            "asset_count": 4,
            "snapshot_digest": "3" * 64,
            "evidence_digest": "4" * 64,
            "mandate_receipt_digest": "5" * 64,
            "rerun_count": 1,
        }

    monkeypatch.setattr(
        snapshot_cli, "orchestrate_existing_tqqq_snapshot_promotion", orchestrate
    )
    assert snapshot_cli.main(
        [
            "--execution-terminal",
            str(tmp_path / "execution.json"),
            "--execution-terminal-sha256",
            "6" * 64,
            "--risk-standard-id",
            _authority().risk_standard_id,
            "--risk-standard-sha256",
            _authority().risk_standard_sha256,
            "--platform-execution-revision",
            _authority().platform_execution_revision,
        ]
    ) == 0
    assert events == ["filevault", "identity", "orchestrate"]
    assert json.loads(capsys.readouterr().out) == {
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
        "asset_count": 4,
        "snapshot_digest": "3" * 64,
        "evidence_digest": "4" * 64,
        "mandate_receipt_digest": "5" * 64,
        "rerun_count": 1,
    }
    combined = "\n".join(
        (
            inspect.getsource(snapshot_cli),
            inspect.getsource(orchestrate_existing_tqqq_snapshot_promotion),
        )
    ).lower()
    for forbidden in (
        "ibapi",
        ".connect(",
        "run_exact_acquisition",
        "reqhistoricaldata",
        "reqaccount",
        "reqpositions",
        "reqopenorders",
        "placeorder",
        "cancelorder",
        "reqexecutions",
    ):
        assert forbidden not in combined


def test_existing_snapshot_diagnostic_uses_consumed_identity_once_and_sanitizes_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    run_root, snapshot_digest, config_digest, mandate_receipt_digest = (
        _consumed_diagnostic_run(tmp_path)
    )
    monkeypatch.setattr(
        orchestration, "_require_diagnostic_execution_compatibility", lambda *_args: None
    )
    calls = 0

    def fail(**_kwargs) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("private bars, dates, prices, volumes, and traceback")

    monkeypatch.setattr(orchestration, "run_tqqq_promotion_diagnostic", fail)
    result = orchestrate_existing_tqqq_snapshot_diagnostic(
        run_root,
        expected_snapshot_digest=snapshot_digest,
        expected_mandate_receipt_digest=mandate_receipt_digest,
        authority=_authority(),
        execution_revision=RUNNER_REVISION,
        execution_tree_sha=RUNNER_TREE_SHA,
        runner_revision=RUNNER_REVISION,
        runner_tree_sha=RUNNER_TREE_SHA,
        session_class="live-data-only",
        clock=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=UTC),
    )

    assert calls == 1
    assert result == {
        "config_digest": config_digest,
        "exception_class": "RuntimeError",
        "function_identifiers": [
            "us_equity_snapshot_pipelines.lifecycle.tqqq_acquisition_orchestration:orchestrate_existing_tqqq_snapshot_diagnostic"
        ],
        "mandate_receipt_digest": mandate_receipt_digest,
        "runner_completion_count": 0,
        "runner_invocation_count": 1,
        "snapshot_digest": snapshot_digest,
        "stage": "promotion_replay_exception",
    }
    serialized = json.dumps(result, sort_keys=True).lower()
    for forbidden in ("private bars", "dates", "prices", "volumes", "traceback", str(run_root).lower()):
        assert forbidden not in serialized


@pytest.mark.parametrize("mismatch", ("snapshot", "config", "provenance"))
def test_existing_snapshot_diagnostic_mismatch_fails_before_invocation(
    mismatch: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _allow_pinned_dependency_provenance(monkeypatch)
    run_root, snapshot_digest, _config_digest, mandate_receipt_digest = (
        _consumed_diagnostic_run(tmp_path)
    )
    authority = _authority()
    execution_tree_sha = RUNNER_TREE_SHA
    if mismatch == "snapshot":
        (run_root / "snapshot" / "bars.json").write_bytes(b"{}")
    elif mismatch == "config":
        authority = replace(authority, risk_standard_sha256="7" * 64)
    else:
        execution_tree_sha = "8" * 40
    if mismatch != "provenance":
        monkeypatch.setattr(
            orchestration,
            "_require_diagnostic_execution_compatibility",
            lambda *_args: None,
        )
    calls = 0

    def forbidden(**_kwargs) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(orchestration, "run_tqqq_promotion_diagnostic", forbidden)
    with pytest.raises(TqqqOrchestrationError):
        orchestrate_existing_tqqq_snapshot_diagnostic(
            run_root,
            expected_snapshot_digest=snapshot_digest,
            expected_mandate_receipt_digest=mandate_receipt_digest,
            authority=authority,
            execution_revision=RUNNER_REVISION,
            execution_tree_sha=execution_tree_sha,
            runner_revision=RUNNER_REVISION,
            runner_tree_sha=RUNNER_TREE_SHA,
            session_class="live-data-only",
            clock=lambda: datetime(2026, 8, 11, 9, 0, tzinfo=UTC),
        )
    assert calls == 0


def test_existing_snapshot_diagnostic_cli_writes_only_mode_0600_sanitized_terminal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    output = tmp_path / "diagnostic.json"
    outcome = {
        "config_digest": "1" * 64,
        "exception_class": "TypeError",
        "function_identifiers": [
            "quant_platform_kit.strategy_lifecycle.backtest_orchestrator:run_promotion"
        ],
        "mandate_receipt_digest": "2" * 64,
        "runner_completion_count": 0,
        "runner_invocation_count": 1,
        "snapshot_digest": "3" * 64,
        "stage": "promotion_replay_exception",
    }
    monkeypatch.setattr(diagnostic_cli, "_require_filevault", lambda: None)
    monkeypatch.setattr(
        diagnostic_cli,
        "_load_execution_binding",
        lambda *_args, **_kwargs: (
            tmp_path / "run",
            _authority(),
            "3" * 64,
            "2" * 64,
            RUNNER_REVISION,
            RUNNER_TREE_SHA,
            "live-data-only",
        ),
    )
    monkeypatch.setattr(
        diagnostic_cli,
        "resolve_tqqq_runtime_identity",
        lambda: (RUNNER_REVISION, RUNNER_TREE_SHA),
    )
    monkeypatch.setattr(
        diagnostic_cli,
        "orchestrate_existing_tqqq_snapshot_diagnostic",
        lambda *_args, **_kwargs: outcome,
    )

    assert diagnostic_cli.main(
        [
            "--execution-terminal",
            str(tmp_path / "execution.json"),
            "--execution-terminal-sha256",
            "4" * 64,
            "--risk-standard-id",
            _authority().risk_standard_id,
            "--risk-standard-sha256",
            _authority().risk_standard_sha256,
            "--platform-execution-revision",
            _authority().platform_execution_revision,
            "--output",
            str(output),
        ]
    ) == 0
    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads(output.read_bytes()) == outcome
    assert json.loads(capsys.readouterr().out) == outcome


def test_existing_snapshot_diagnostic_committed_surface_has_no_provider_or_order_calls() -> None:
    combined = "\n".join(
        (
            inspect.getsource(diagnostic_cli),
            inspect.getsource(orchestrate_existing_tqqq_snapshot_diagnostic),
        )
    ).lower()
    for forbidden in (
        "ibapi",
        ".connect(",
        "run_exact_acquisition",
        "reqhistoricaldata",
        "reqaccount",
        "reqpositions",
        "reqopenorders",
        "placeorder",
        "cancelorder",
        "reqexecutions",
    ):
        assert forbidden not in combined
