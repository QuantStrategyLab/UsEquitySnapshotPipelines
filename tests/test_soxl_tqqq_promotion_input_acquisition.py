from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

import pytest
from quant_platform_kit.ibkr import StrictAdjustedHistoryError

from scripts import acquire_soxl_tqqq_promotion_inputs_ibkr as acquisition_cli


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


def test_cli_connects_once_and_outputs_only_sanitized_terminal(
    monkeypatch,
    capsys,
) -> None:
    app = _FakeRuntimeApp()
    monkeypatch.setattr(acquisition_cli, "_runtime", lambda: (app, object()))
    def run_exact(_app, *, contract_factory):
        app.events.append("acquire")
        return {
            symbol: SimpleNamespace(private_bars="must not serialize")
            for symbol in acquisition_cli.EXACT_ASSETS
        }

    monkeypatch.setattr(acquisition_cli, "run_exact_acquisition", run_exact)

    assert acquisition_cli.main([]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "asset_count": 9,
        "lifecycle": list(app.sanitized_lifecycle()),
        "status": "STRICT_COMPLETE",
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
