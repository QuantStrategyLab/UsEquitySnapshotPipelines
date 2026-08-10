from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

import pytest
from quant_platform_kit.ibkr import StrictAdjustedHistoryError

from scripts import acquire_soxl_tqqq_promotion_inputs_ibkr as acquisition_cli


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
    assert calls[7]["expected_sessions"][0] == date(2024, 1, 29)
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


def test_cli_connects_once_and_passes_results_to_single_orchestration(
    monkeypatch,
    capsys,
    tmp_path,
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

    def run_exact(_app, *, contract_factory):
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

    assert acquisition_cli.main(_valid_cli_args()) == 0

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
    assert app.connect_calls[0][0:2] == ("127.0.0.1", 4002)
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
