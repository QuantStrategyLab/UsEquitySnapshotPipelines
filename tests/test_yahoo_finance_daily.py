from __future__ import annotations

from urllib.error import HTTPError

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_INVALID,
    SOURCE_OBSERVATION_READY,
    SOURCE_OBSERVATION_UNAVAILABLE,
)

from us_equity_snapshot_pipelines import yahoo_finance_daily


class _Frame:
    def __init__(self, rows: object) -> None:
        self._rows = rows

    def to_dict(self, *, orient: str) -> object:
        assert orient == "records"
        return self._rows


def _rows(*, symbol: str = "SOXL") -> list[dict[str, object]]:
    return [
        {
            "symbol": symbol,
            "as_of": "2026-08-20",
            "open": 99,
            "high": 101,
            "low": 98,
            "close": 100,
            "volume": 900_000,
        },
        {
            "symbol": symbol,
            "as_of": "2026-08-21",
            "open": 100,
            "high": 102,
            "low": 99,
            "close": 101,
            "volume": 1_000_000,
        },
    ]


def test_adjusted_daily_observation_uses_existing_chart_adapter_and_keeps_cutoff_inclusive(monkeypatch) -> None:
    calls = []

    def download(symbols, *, start: str, end: str, price_field: str):
        calls.append((symbols, start, end, price_field))
        return _Frame(_rows())

    monkeypatch.setattr(yahoo_finance_daily, "download_yahoo_chart_price_history", download)

    observation = yahoo_finance_daily.observe_yahoo_finance_adjusted_daily_bars(
        symbol="soxl",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    assert observation.status == SOURCE_OBSERVATION_READY
    assert observation.snapshot is not None
    assert tuple(bar.session_date for bar in observation.snapshot.bars) == ("2026-08-20", "2026-08-21")
    assert calls == [(["SOXL"], "2026-08-20", "2026-08-22", "adjusted_close")]


def test_rate_limited_and_malformed_public_responses_become_safe_terminal_states(monkeypatch) -> None:
    def rate_limited(*args, **kwargs):
        raise HTTPError("https://query1.finance.yahoo.com/", 429, "rate limited", {}, None)

    monkeypatch.setattr(yahoo_finance_daily, "download_yahoo_chart_price_history", rate_limited)
    rate_limited_result = yahoo_finance_daily.observe_yahoo_finance_adjusted_daily_bars(
        symbol="SOXL",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    monkeypatch.setattr(yahoo_finance_daily, "download_yahoo_chart_price_history", lambda *args, **kwargs: _Frame([]))
    malformed_result = yahoo_finance_daily.observe_yahoo_finance_adjusted_daily_bars(
        symbol="SOXL",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    assert rate_limited_result.status == SOURCE_OBSERVATION_UNAVAILABLE
    assert rate_limited_result.reason_codes == (yahoo_finance_daily.YAHOO_FINANCE_RATE_LIMITED,)
    assert malformed_result.status == SOURCE_OBSERVATION_INVALID
    assert malformed_result.reason_codes == (yahoo_finance_daily.YAHOO_FINANCE_PAYLOAD_INVALID,)
