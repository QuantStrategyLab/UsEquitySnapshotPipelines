from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest

from quant_platform_kit.ibkr import (
    StrictAdjustedHistoryError,
    StrictAdjustedHistoryRequestOutcome,
)
from us_equity_snapshot_pipelines.lifecycle import soxl_adjusted_last_acquisition as adjusted_last
from us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition import (
    acquire_strict_adjusted_last,
    classify_request_validation_321,
    write_sanitized_adjusted_last_diagnostic,
    write_sanitized_request_validation_321_diagnostic,
)


EXPECTED = (date(2026, 8, 1), date(2026, 8, 2))
CUTOFF = datetime(2026, 8, 5, 3, 59, 59, tzinfo=timezone.utc)
REQUEST_ENVELOPE = (
    b'{"barSizeSetting":"1 day","durationStr":"9 Y",'
    b'"endDateTime":"20260805 03:59:59 UTC","formatDate":1,'
    b'"keepUpToDate":false,"useRTH":true,"whatToShow":"ADJUSTED_LAST"}'
)


def _commitment(domain: bytes, value: bytes) -> str:
    return hashlib.sha256(domain + b"\x00" + value).hexdigest()


def _bar(session: date, close: float = 98_765.4321) -> SimpleNamespace:
    return SimpleNamespace(
        date=session,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000.0,
    )


class OfflineIB:
    def __init__(self) -> None:
        self.history_calls = 0

    def qualifyContracts(self, contract):
        return [contract]

    def reqHistoricalData(self, _contract, **_kwargs):
        self.history_calls += 1
        raise AssertionError("provider calls are forbidden in synthetic tests")


def _stock(symbol: str, exchange: str, currency: str) -> SimpleNamespace:
    return SimpleNamespace(symbol=symbol, exchange=exchange, currency=currency)


def _requester(*, bars, completion_observed: bool, provider_error_codes=()):
    def request(_contract, **kwargs):
        assert kwargs["whatToShow"] == "ADJUSTED_LAST"
        assert kwargs["useRTH"] is True
        return StrictAdjustedHistoryRequestOutcome(
            bars=bars,
            completion_observed=completion_observed,
            provider_error_codes=provider_error_codes,
        )

    return request


@pytest.mark.parametrize(
    ("classification", "bars", "completion_observed", "provider_error_codes", "counts"),
    [
        ("session_contract_mismatch", [_bar(EXPECTED[0])], True, (), (1, 0, 0)),
        (
            "session_contract_mismatch",
            [_bar(EXPECTED[0]), _bar(EXPECTED[1]), _bar(date(2026, 8, 3))],
            True,
            (),
            (0, 1, 0),
        ),
        ("session_contract_mismatch", [_bar(EXPECTED[0]), _bar(EXPECTED[0])], True, (), (1, 0, 1)),
        ("empty_response", [], True, (), (2, 0, 0)),
        ("provider_error", [_bar(EXPECTED[0])], False, (10089, 10089), (1, 0, 0)),
        ("completion_not_observed", [_bar(EXPECTED[0])], False, (), (1, 0, 0)),
    ],
)
def test_failures_emit_only_sanitized_diagnostic(
    tmp_path: Path,
    classification: str,
    bars,
    completion_observed: bool,
    provider_error_codes,
    counts: tuple[int, int, int],
) -> None:
    ib = OfflineIB()
    with pytest.raises(StrictAdjustedHistoryError) as caught:
        acquire_strict_adjusted_last(
            ib,
            "SOXL",
            end_datetime=CUTOFF,
            duration="9 Y",
            expected_sessions=EXPECTED,
            stock_factory=_stock,
            requester=_requester(
                bars=bars,
                completion_observed=completion_observed,
                provider_error_codes=provider_error_codes,
            ),
        )

    destination = tmp_path / f"{classification}.json"
    write_sanitized_adjusted_last_diagnostic(destination, caught.value)
    payload = json.loads(destination.read_bytes())
    assert payload["classification"] == classification
    assert (
        payload["counts"]["missing_count"],
        payload["counts"]["extra_count"],
        payload["counts"]["duplicate_count"],
    ) == counts
    assert payload["provider_error_code_counts"] == (
        {"10089": 2} if provider_error_codes else {}
    )
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert ib.history_calls == 0

    serialized = destination.read_text(encoding="utf-8")
    for forbidden in (
        "2026-08-01",
        "2026-08-02",
        "2026-08-03",
        "98765.4321",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "provider message",
        "secret",
    ):
        assert forbidden not in serialized


def test_exact_match_preserves_strict_request_and_returns_no_failure_artifact(
    tmp_path: Path,
) -> None:
    ib = OfflineIB()
    result = acquire_strict_adjusted_last(
        ib,
        "SOXL",
        end_datetime=CUTOFF,
        duration="9 Y",
        expected_sessions=EXPECTED,
        stock_factory=_stock,
        requester=_requester(
            bars=[_bar(EXPECTED[0]), _bar(EXPECTED[1])],
            completion_observed=True,
        ),
    )

    assert result.diagnostic.to_dict()["classification"] == "exact_match"
    assert tuple(candle.session for candle in result.candles) == EXPECTED
    assert list(tmp_path.iterdir()) == []
    assert ib.history_calls == 0


def test_foreign_321_does_not_pollute_exact_historical_request() -> None:
    bound = adjusted_last.bind_strict_adjusted_history_request(
        active_request_id=41,
        bars=[_bar(EXPECTED[0]), _bar(EXPECTED[1])],
        error_events=(
            (99, 321, "synthetic foreign validation detail"),
            (-1, 321, None),
            (88, 10089, None),
            (41, 2104, None),
        ),
        historical_data_end_request_ids=(41,),
        informational_error_codes=(2104,),
        request_envelope=REQUEST_ENVELOPE,
        expected_session_count=len(EXPECTED),
    )

    result = acquire_strict_adjusted_last(
        OfflineIB(),
        "BOXX",
        end_datetime=CUTOFF,
        duration="9 Y",
        expected_sessions=EXPECTED,
        stock_factory=_stock,
        requester=lambda _contract, **_kwargs: bound.history_outcome,
    )

    assert result.diagnostic.to_dict()["classification"] == "exact_match"
    assert tuple(bound.history_outcome.provider_error_codes) == ()
    assert bound.foreign_error_code_counts == ((321, 2), (10089, 1))
    assert tuple(
        diagnostic.cause for diagnostic in bound.request_validation_321_diagnostics
    ) == ("request_id_mismatch", "request_id_mismatch")


def test_matching_321_is_bound_only_to_its_historical_request() -> None:
    bound = adjusted_last.bind_strict_adjusted_history_request(
        active_request_id=41,
        bars=[_bar(EXPECTED[0]), _bar(EXPECTED[1])],
        error_events=((41, 321, "invalid duration"), (99, 10089, None)),
        historical_data_end_request_ids=(41,),
        informational_error_codes=(),
        request_envelope=REQUEST_ENVELOPE,
        expected_session_count=len(EXPECTED),
    )

    assert tuple(bound.history_outcome.provider_error_codes) == (321,)
    assert bound.foreign_error_code_counts == ((10089, 1),)
    assert tuple(
        diagnostic.cause for diagnostic in bound.request_validation_321_diagnostics
    ) == ("invalid_duration",)
    with pytest.raises(StrictAdjustedHistoryError) as caught:
        acquire_strict_adjusted_last(
            OfflineIB(),
            "BOXX",
            end_datetime=CUTOFF,
            duration="9 Y",
            expected_sessions=EXPECTED,
            stock_factory=_stock,
            requester=lambda _contract, **_kwargs: bound.history_outcome,
        )
    assert caught.value.diagnostic is not None
    assert caught.value.diagnostic.to_dict()["classification"] == "provider_error"


def test_only_matching_historical_data_end_marks_request_complete() -> None:
    bound = adjusted_last.bind_strict_adjusted_history_request(
        active_request_id=41,
        bars=[_bar(EXPECTED[0]), _bar(EXPECTED[1])],
        error_events=(),
        historical_data_end_request_ids=(99,),
        informational_error_codes=(),
        request_envelope=REQUEST_ENVELOPE,
        expected_session_count=len(EXPECTED),
    )

    assert bound.history_outcome.completion_observed is False
    assert bound.matching_historical_data_end_count == 0
    assert bound.foreign_historical_data_end_count == 1
    with pytest.raises(StrictAdjustedHistoryError) as caught:
        acquire_strict_adjusted_last(
            OfflineIB(),
            "BOXX",
            end_datetime=CUTOFF,
            duration="9 Y",
            expected_sessions=EXPECTED,
            stock_factory=_stock,
            requester=lambda _contract, **_kwargs: bound.history_outcome,
        )
    assert caught.value.diagnostic is not None
    assert caught.value.diagnostic.to_dict()["classification"] == "completion_not_observed"


@pytest.mark.parametrize(
    ("provider_message", "expected_cause"),
    [
        ("invalid endDateTime", "invalid_end_datetime"),
        (
            "Error validating request.-'bS' : cause - invalid end date/time",
            "invalid_end_datetime",
        ),
        ("invalid duration", "invalid_duration"),
        (
            "Error validating request.-'bS' : cause - invalid duration",
            "invalid_duration",
        ),
        ("invalid whatToShow", "invalid_what_to_show"),
        (
            "Error validating request.-'bS' : cause - invalid what to show",
            "invalid_what_to_show",
        ),
        ("invalid endDateTime and duration", "unknown_321"),
        (
            "Error validating request: invalid duration; invalid endDateTime",
            "unknown_321",
        ),
        (None, "unknown_321"),
    ],
)
def test_matching_request_id_uses_closed_321_cause_allowlist(
    provider_message: str | None,
    expected_cause: str,
) -> None:
    diagnostic = classify_request_validation_321(
        active_request_id=41,
        error_request_id=41,
        error_code=321,
        provider_message=provider_message,
        request_envelope=REQUEST_ENVELOPE,
        completion_request_ids=(41,),
        expected_session_count=2_264,
        observed_session_count=0,
    )

    assert diagnostic.to_dict()["cause"] == expected_cause
    assert diagnostic.to_dict()["request_completion_observed"] is True


def test_mismatching_ids_are_counted_without_persisting_raw_ids() -> None:
    diagnostic = classify_request_validation_321(
        active_request_id=41,
        error_request_id=99,
        error_code=321,
        provider_message="invalid duration",
        request_envelope=REQUEST_ENVELOPE,
        completion_request_ids=(99, 41, 100),
        expected_session_count=2_264,
        observed_session_count=0,
    )

    payload = diagnostic.to_dict()
    assert payload["cause"] == "request_id_mismatch"
    assert payload["request_completion_observed"] is True
    assert payload["counts"] == {
        "matching_error_count": 0,
        "mismatching_error_count": 1,
        "matching_completion_count": 1,
        "mismatching_completion_count": 2,
        "expected_session_count": 2_264,
        "observed_session_count": 0,
    }
    assert {
        "active_request_id",
        "error_request_id",
        "completion_request_ids",
    }.isdisjoint(payload)


def test_321_error_precedes_completion_and_session_state() -> None:
    diagnostic = classify_request_validation_321(
        active_request_id=41,
        error_request_id=41,
        error_code=321,
        provider_message="invalid duration",
        request_envelope=REQUEST_ENVELOPE,
        completion_request_ids=(99,),
        expected_session_count=2_264,
        observed_session_count=0,
    )

    payload = diagnostic.to_dict()
    assert payload["cause"] == "invalid_duration"
    assert payload["request_completion_observed"] is False
    assert payload["counts"]["mismatching_completion_count"] == 1
    assert payload["counts"]["observed_session_count"] == 0


def test_request_validation_writer_is_exclusive_mode_0600_and_sanitized(
    tmp_path: Path,
) -> None:
    provider_message = "invalid duration"
    diagnostic = classify_request_validation_321(
        active_request_id=41,
        error_request_id=41,
        error_code=321,
        provider_message=provider_message,
        request_envelope=REQUEST_ENVELOPE,
        completion_request_ids=(),
        expected_session_count=2_264,
        observed_session_count=0,
    )
    destination = tmp_path / "request-validation-321.json"

    write_sanitized_request_validation_321_diagnostic(destination, diagnostic)
    payload = json.loads(destination.read_bytes())

    assert set(payload) == {
        "cause",
        "numeric_code",
        "request_envelope_sha256",
        "provider_message_sha256",
        "request_completion_observed",
        "counts",
    }
    assert payload["cause"] == "invalid_duration"
    assert payload["numeric_code"] == 321
    assert payload["request_envelope_sha256"] == _commitment(
        b"qsl.soxl.request-envelope.v1",
        REQUEST_ENVELOPE,
    )
    assert payload["provider_message_sha256"] == _commitment(
        b"qsl.soxl.provider-message.v1",
        provider_message.encode("utf-8"),
    )
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600

    serialized = destination.read_text(encoding="utf-8")
    for forbidden in (
        provider_message,
        "20260805",
        "ADJUSTED_LAST",
        "SOXL",
        "SMART",
        "USD",
        "clientId",
        "account",
        "open",
        "close",
        "volume",
    ):
        assert forbidden not in serialized

    with pytest.raises(FileExistsError):
        write_sanitized_request_validation_321_diagnostic(destination, diagnostic)
    assert json.loads(destination.read_bytes()) == payload


def test_request_validation_exception_and_logs_do_not_expose_sensitive_inputs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive_message = "private provider detail token=never-persist"
    sensitive_envelope = b'{"clientId":48291,"symbol":"SOXL"}'

    with pytest.raises(ValueError) as caught:
        classify_request_validation_321(
            active_request_id=41,
            error_request_id=41,
            error_code=10089,
            provider_message=sensitive_message,
            request_envelope=sensitive_envelope,
            completion_request_ids=(),
            expected_session_count=0,
            observed_session_count=0,
        )

    exposed = repr(caught.value) + caplog.text
    assert sensitive_message not in exposed
    assert sensitive_envelope.decode("utf-8") not in exposed
    assert "41" not in exposed


@pytest.mark.parametrize(
    "contradictory_fields",
    [
        {
            "counts": (
                ("matching_error_count", 0),
                ("mismatching_error_count", 1),
                ("matching_completion_count", 0),
                ("mismatching_completion_count", 0),
                ("expected_session_count", 2_264),
                ("observed_session_count", 0),
            )
        },
        {"request_completion_observed": True},
    ],
)
def test_writer_rejects_contradictory_request_correlation_aggregates(
    tmp_path: Path,
    contradictory_fields: dict[str, object],
) -> None:
    diagnostic = classify_request_validation_321(
        active_request_id=41,
        error_request_id=41,
        error_code=321,
        provider_message="invalid duration",
        request_envelope=REQUEST_ENVELOPE,
        completion_request_ids=(),
        expected_session_count=2_264,
        observed_session_count=0,
    )

    with pytest.raises(ValueError):
        write_sanitized_request_validation_321_diagnostic(
            tmp_path / "contradictory.json",
            replace(diagnostic, **contradictory_fields),
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "provider_end_time",
    ["20260805 03:59:59 UTC", "20260805-03:59:59"],
)
def test_frozen_request_fields_and_aware_or_legacy_end_time_representations(
    provider_end_time: str,
) -> None:
    captured: dict[str, object] = {}

    def requester(contract, **kwargs):
        captured["contract"] = contract
        captured.update(kwargs)
        return StrictAdjustedHistoryRequestOutcome(
            bars=[_bar(EXPECTED[0]), _bar(EXPECTED[1])],
            completion_observed=True,
        )

    acquire_strict_adjusted_last(
        OfflineIB(),
        "SOXL",
        end_datetime=CUTOFF,
        duration="9 Y",
        expected_sessions=EXPECTED,
        stock_factory=_stock,
        requester=requester,
    )

    contract = captured.pop("contract")
    assert vars(contract) == {
        "symbol": "SOXL",
        "exchange": "SMART",
        "currency": "USD",
    }
    assert captured == {
        "endDateTime": CUTOFF,
        "durationStr": "9 Y",
        "barSizeSetting": "1 day",
        "whatToShow": "ADJUSTED_LAST",
        "useRTH": True,
        "formatDate": 1,
        "keepUpToDate": False,
    }
    assert provider_end_time in {
        CUTOFF.strftime("%Y%m%d %H:%M:%S UTC"),
        CUTOFF.strftime("%Y%m%d-%H:%M:%S"),
    }
