from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest

from quant_platform_kit.ibkr import (
    StrictAdjustedHistoryError,
    StrictAdjustedHistoryRequestOutcome,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_adjusted_last_acquisition import (
    acquire_strict_adjusted_last,
    write_sanitized_adjusted_last_diagnostic,
)


EXPECTED = (date(2026, 8, 1), date(2026, 8, 2))
CUTOFF = datetime(2026, 8, 5, 3, 59, 59, tzinfo=timezone.utc)


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
