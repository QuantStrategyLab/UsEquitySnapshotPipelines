from __future__ import annotations

import inspect
import json
from datetime import date
from types import SimpleNamespace

import pytest
from quant_platform_kit.ibkr import (
    StrictAdjustedHistoryDiagnostic,
    StrictAdjustedHistoryError,
)

from scripts import acquire_soxl_tqqq_promotion_inputs_ibkr as acquisition_cli
import us_equity_snapshot_pipelines.lifecycle.soxl_acquisition_orchestration as orchestration
import us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition as acquisition


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
        "soxl_p3_candidate_bound_v1",
        "--risk-standard-sha256",
        "4" * 64,
        "--input-license",
        "authority-bound private internal research",
        "--input-usage-scope",
        "non-commercial internal research",
    ]


def test_exact_acquisition_reuses_frozen_nine_input_contract(monkeypatch) -> None:
    calls = []

    def fake_acquire(
        app,
        symbol,
        *,
        end_datetime,
        duration,
        expected_sessions,
        stock_factory,
        requester,
    ):
        calls.append(
            {
                "app": app,
                "symbol": symbol,
                "end_datetime": end_datetime,
                "duration": duration,
                "expected_sessions": tuple(expected_sessions),
                "stock_factory": stock_factory,
                "requester": requester,
            }
        )
        return SimpleNamespace(symbol=symbol)

    monkeypatch.setattr(acquisition_cli, "acquire_strict_adjusted_last", fake_acquire)
    app = object()
    contract_factory = object()

    results = acquisition_cli.run_exact_acquisition(
        app,
        contract_factory=contract_factory,
    )

    assert tuple(results) == acquisition_cli.EXACT_ASSETS == (
        "SOXL",
        "SOXX",
        "BOXX",
        "SCHD",
        "DGRO",
        "SGOV",
        "SPYI",
        "QQQI",
        "QQQ",
    )
    assert [item["duration"] for item in calls] == [
        "9 Y",
        "9 Y",
        "4 Y",
        "9 Y",
        "9 Y",
        "7 Y",
        "4 Y",
        "3 Y",
        "9 Y",
    ]
    assert all(item["app"] is app for item in calls)
    assert all(item["stock_factory"] is contract_factory for item in calls)
    assert all(callable(item["requester"]) for item in calls)
    assert calls[0]["expected_sessions"][0] == date(2018, 8, 3)
    assert calls[2]["expected_sessions"][0] == date(2022, 12, 28)
    assert calls[5]["expected_sessions"][0] == date(2020, 5, 28)
    assert calls[7]["expected_sessions"][0] == date.fromisoformat(
        orchestration.FIRST_ELIGIBLE_SESSION["QQQI"]
    ) == date(2024, 1, 30)
    assert all(
        item["expected_sessions"][-1] == date(2026, 8, 4) for item in calls
    )


def test_exact_acquisition_stops_after_first_material_failure(monkeypatch) -> None:
    calls = []

    def fail_on_boxx(_app, symbol, **_kwargs):
        calls.append(symbol)
        if symbol == "BOXX":
            raise StrictAdjustedHistoryError("synthetic material failure")
        return SimpleNamespace(symbol=symbol)

    monkeypatch.setattr(acquisition_cli, "acquire_strict_adjusted_last", fail_on_boxx)

    with pytest.raises(StrictAdjustedHistoryError, match="synthetic material failure"):
        acquisition_cli.run_exact_acquisition(object(), contract_factory=object())

    assert calls == ["SOXL", "SOXX", "BOXX"]


class _FakeRuntimeApp:
    def __init__(self) -> None:
        self.connect_calls = []
        self.start_reader_calls = 0
        self.disconnect_calls = 0
        self.events = []

    def connect(self, host: str, port: int, client_id: int) -> None:
        self.events.append("connect")
        self.connect_calls.append((host, port, client_id))

    def isConnected(self) -> bool:
        return True

    def start_reader(self) -> None:
        self.events.append("start_reader")
        self.start_reader_calls += 1

    def wait_for_handshake(self) -> bool:
        self.events.append("wait_for_handshake")
        return True

    def disconnect(self) -> None:
        self.events.append("disconnect")
        self.disconnect_calls += 1

    def sanitized_lifecycle(self):
        return (
            {
                "phase": "history",
                "status": "SUCCESS",
                "terminal_trigger": "response_callback",
                "readiness_or_progress_observed": True,
                "matching_callback_count": 1,
                "foreign_callback_count": 0,
                "matching_completion_count": 1,
                "transition_code_counts": {"2107": 1, "2106": 1},
                "cancellation_count": 0,
                "elapsed_monotonic_ms": 1,
                "request_envelope_sha256": "a" * 64,
            },
        )


@pytest.mark.parametrize(
    ("session_args", "expected_port", "expected_session_class"),
    [
        ([], 4002, "paper"),
        (["--session-mode", "live-data-only"], 4001, "live-data-only"),
    ],
)
def test_cli_connects_once_and_passes_results_to_single_orchestration(
    monkeypatch,
    capsys,
    tmp_path,
    session_args,
    expected_port,
    expected_session_class,
) -> None:
    app = _FakeRuntimeApp()
    events = []
    monkeypatch.setattr(acquisition_cli, "_LOCAL_RESEARCH_ROOT", tmp_path / "runs")
    monkeypatch.setattr(acquisition_cli, "_require_filevault_local_root", lambda: events.append("filevault"))
    monkeypatch.setattr(
        acquisition_cli,
        "resolve_soxl_runtime_identity",
        lambda: events.append("identity") or ("a" * 40, "b" * 40),
    )
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: (app, object()))

    def run_exact(_app, *, contract_factory, on_strict_history_failure):
        assert callable(on_strict_history_failure)
        app.events.append("acquire")
        return {
            symbol: SimpleNamespace(private_bars="must not serialize")
            for symbol in acquisition_cli.EXACT_ASSETS
        }

    def orchestrate(results, **kwargs):
        events.append("orchestrate")
        assert tuple(results) == acquisition_cli.EXACT_ASSETS
        assert all(result.private_bars == "must not serialize" for result in results.values())
        assert kwargs["output_root"] == tmp_path / "runs"
        assert kwargs["runner_revision"] == "a" * 40
        assert kwargs["runner_tree_sha"] == "b" * 40
        assert kwargs["authority"].authority_receipt_sha256 == "1" * 64
        assert kwargs["session_class"] == expected_session_class
        return {
            "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
            "asset_count": 9,
            "snapshot_digest": "5" * 64,
            "evidence_digest": "6" * 64,
            "mandate_receipt_digest": "7" * 64,
            "rerun_count": 1,
        }

    monkeypatch.setattr(acquisition_cli, "run_exact_acquisition", run_exact)
    monkeypatch.setattr(acquisition_cli, "orchestrate_soxl_promotion", orchestrate)

    assert acquisition_cli.main([*_valid_cli_args(), *session_args]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "asset_count": 9,
        "evidence_digest": "6" * 64,
        "lifecycle": list(app.sanitized_lifecycle()),
        "mandate_receipt_digest": "7" * 64,
        "rerun_count": 1,
        "snapshot_digest": "5" * 64,
        "status": "VALIDATED_EVIDENCE_V2_AWAITING_HUMAN_PROMOTION_ACCEPTANCE",
    }
    assert len(app.connect_calls) == 1
    assert app.connect_calls[0][0:2] == ("127.0.0.1", expected_port)
    assert app.start_reader_calls == 1
    assert app.disconnect_calls == 1
    assert app.events == [
        "connect",
        "start_reader",
        "wait_for_handshake",
        "acquire",
        "disconnect",
    ]
    assert events == ["filevault", "identity", "orchestrate"]
    serialized = json.dumps(payload, sort_keys=True)
    assert "private_bars" not in serialized
    assert "must not serialize" not in serialized
    for forbidden in (
        "provider_message",
        "request_body",
        "response_body",
        "account",
        "credential",
        '"bars"',
        '"price"',
        '"volume"',
    ):
        assert forbidden not in serialized.lower()


def test_cli_retains_only_sanitized_post_snapshot_failure_state(
    monkeypatch,
    capsys,
) -> None:
    app = _FakeRuntimeApp()
    failure = {
        "backtest_orchestrator_invocation_count": None,
        "classification": "promotion_rerun_failed",
        "evidence_artifact_count": 0,
        "mandate_digest": "6" * 64,
        "mandate_receipt_digest": "7" * 64,
        "risk_engine_assessment_count": None,
        "runner_completion_count": 0,
        "runner_invocation_count": 1,
        "snapshot_digest": "5" * 64,
        "stage": "promotion_runner_pre_evidence",
    }
    monkeypatch.setattr(acquisition_cli, "_require_filevault_local_root", lambda: None)
    monkeypatch.setattr(
        acquisition_cli,
        "resolve_soxl_runtime_identity",
        lambda: ("a" * 40, "b" * 40),
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

    def fail_orchestration(*_args, **_kwargs):
        raise orchestration.SoxlOrchestrationError(
            "private runner exception must not serialize",
            stage="promotion_runner_pre_evidence",
            snapshot_digest="5" * 64,
            mandate_digest="6" * 64,
            mandate_receipt_digest="7" * 64,
            evidence_artifact_count=0,
        )

    monkeypatch.setattr(
        acquisition_cli,
        "orchestrate_soxl_promotion",
        fail_orchestration,
    )

    assert acquisition_cli.main(_valid_cli_args()) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "asset_count": 9,
        "evidence_digest": None,
        "lifecycle": list(app.sanitized_lifecycle()),
        "mandate_receipt_digest": "7" * 64,
        "orchestration_failure": failure,
        "rerun_count": 0,
        "snapshot_digest": "5" * 64,
        "status": "FAILED_MATERIAL",
    }
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in (
        "private runner exception",
        "provider_message",
        "request_body",
        "response_body",
        "account",
        "credential",
        "private_bars",
        "must not serialize",
        '"bars"',
        '"date"',
        '"price"',
        '"volume"',
    ):
        assert forbidden not in serialized.lower()


def test_cli_retains_only_sanitized_strict_history_failure_diagnostic(
    monkeypatch,
    capsys,
) -> None:
    app = _FakeRuntimeApp()
    diagnostic = StrictAdjustedHistoryDiagnostic(
        classification="session_contract_mismatch",
        completion_observed=True,
        expected_count=986,
        observed_in_window_count=985,
        missing_count=1,
        extra_count=0,
        duplicate_count=0,
        missing_sessions_sha256="b" * 64,
        extra_sessions_sha256="c" * 64,
        duplicate_sessions_sha256="d" * 64,
        provider_error_code_counts=(),
    )
    monkeypatch.setattr(acquisition_cli, "_require_filevault_local_root", lambda: None)
    monkeypatch.setattr(
        acquisition_cli,
        "resolve_soxl_runtime_identity",
        lambda: ("a" * 40, "b" * 40),
    )
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: (app, object()))

    def acquire_or_fail(_app, symbol, **_kwargs):
        if symbol == "SPYI":
            raise StrictAdjustedHistoryError(
                "synthetic provider message must not serialize",
                diagnostic=diagnostic,
            )
        return SimpleNamespace()

    monkeypatch.setattr(acquisition_cli, "acquire_strict_adjusted_last", acquire_or_fail)

    assert acquisition_cli.main(_valid_cli_args()) == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "FAILED_MATERIAL"
    assert payload["asset_count"] == 0
    assert payload["lifecycle"] == list(app.sanitized_lifecycle())
    assert payload["strict_history_failure"] == {
        "classification": "session_contract_mismatch",
        "commitments": {
            "algorithm": "sha256",
            "canonicalization": "sorted_unique_iso_sessions_json_utf8.v1",
            "duplicate_sessions_sha256": "d" * 64,
            "extra_sessions_sha256": "c" * 64,
            "missing_sessions_sha256": "b" * 64,
        },
        "counts": {
            "duplicate_count": 0,
            "expected_count": 986,
            "extra_count": 0,
            "missing_count": 1,
            "observed_in_window_count": 985,
        },
        "failing_symbol": "SPYI",
        "provider_error_code_counts": {},
        "request_completion_observed": True,
        "schema_version": "strict_adjusted_history_diagnostic.v1",
        "strict_complete_input_count": 6,
        "terminal_trigger": "response_callback",
    }
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in (
        "synthetic provider message",
        "provider_message",
        "request_body",
        "response_body",
        "account",
        "credential",
        '"bars"',
        '"date"',
        '"price"',
        '"volume"',
    ):
        assert forbidden not in serialized.lower()


def test_live_data_only_source_identity_binds_session_and_official_runtime(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        orchestration,
        "runtime_producer_source_identity",
        lambda **_kwargs: {"repository": "synthetic"},
    )
    raw_sessions = [
        {
            "date": "2026-08-04",
            "bars": {
                symbol: {
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 1_000_000.0,
                }
                for symbol in orchestration.SOXL_PROMOTION_ASSETS
            },
        }
    ]
    results = {
        symbol: SimpleNamespace(
            provenance=SimpleNamespace(
                exchange="SMART",
                currency="USD",
                duration=orchestration.EXACT_DURATIONS[symbol],
                bar_size="1 day",
                what_to_show="ADJUSTED_LAST",
                use_rth=True,
                format_date=1,
                keep_up_to_date=False,
            )
        )
        for symbol in orchestration.SOXL_PROMOTION_ASSETS
    }
    kwargs = {
        "authority": SimpleNamespace(
            entitlement_receipt_sha256="1" * 64,
            license_receipt_sha256="2" * 64,
            retention_expires_at="2026-12-31T00:00:00Z",
        ),
        "runner_revision": "a" * 40,
        "runner_tree_sha": "b" * 40,
        "observed_at": "2026-08-11T00:00:00Z",
    }

    paper = orchestration._source_contract(
        raw_sessions,
        results,
        session_class="paper",
        **kwargs,
    )
    live = orchestration._source_contract(
        raw_sessions,
        results,
        session_class="live-data-only",
        **kwargs,
    )

    assert {item["provider_id"] for item in paper["logical_inputs"]} == {
        "IBKR_PAPER_GATEWAY"
    }
    assert {item["provider_id"] for item in live["logical_inputs"]} == {
        "IBKR_LIVE_GATEWAY_DATA_ONLY"
    }
    assert {item["source_revision"] for item in live["logical_inputs"]} == {
        orchestration.OFFICIAL_IBAPI_PROVENANCE_SHA256
    }
    assert [item["request_sha256"] for item in paper["logical_inputs"]] != [
        item["request_sha256"] for item in live["logical_inputs"]
    ]


def test_committed_caller_has_historical_data_only_api_surface() -> None:
    cli_source = inspect.getsource(acquisition_cli)
    acquisition_source = inspect.getsource(acquisition)
    source = "\n".join((cli_source, acquisition_source))
    assert cli_source.count("app.connect(") == 1
    assert acquisition_source.count("self.reqContractDetails(") == 1
    assert acquisition_source.count("self.reqHistoricalData(") == 1
    assert acquisition_source.count("self.cancelHistoricalData(") == 1
    assert "reqContractDetails" in source
    assert "reqHistoricalData" in source
    assert "cancelHistoricalData" in source
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
        assert forbidden not in source


def test_cli_filevault_failure_stops_before_runtime_or_provider(monkeypatch, capsys) -> None:
    runtime_calls = []

    def fail_filevault() -> None:
        raise RuntimeError("synthetic FileVault failure")

    monkeypatch.setattr(acquisition_cli, "_require_filevault_local_root", fail_filevault)
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: runtime_calls.append(True))

    assert acquisition_cli.main(_valid_cli_args()) == 1

    assert runtime_calls == []
    assert json.loads(capsys.readouterr().out) == {
        "asset_count": 0,
        "evidence_digest": None,
        "lifecycle": [],
        "mandate_receipt_digest": None,
        "rerun_count": 0,
        "snapshot_digest": None,
        "status": "FAILED_MATERIAL",
    }
