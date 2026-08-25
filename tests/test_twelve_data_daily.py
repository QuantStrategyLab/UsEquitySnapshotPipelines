from __future__ import annotations

import json
from typing import Self
from urllib.error import HTTPError

from quant_platform_kit.data.multisource_assurance import (
    SOURCE_OBSERVATION_INVALID,
    SOURCE_OBSERVATION_READY,
    SOURCE_OBSERVATION_UNAVAILABLE,
)

from us_equity_snapshot_pipelines import twelve_data_daily


class _Response:
    status = 200

    def __init__(self, payload: object) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _payload(*, symbol: str = "SOXL") -> dict[str, object]:
    return {
        "meta": {"symbol": symbol},
        "values": [
            {
                "datetime": "2026-08-21",
                "open": "100",
                "high": "102",
                "low": "99",
                "close": "101",
                "volume": "1000000",
            },
            {
                "datetime": "2026-08-20",
                "open": "99",
                "high": "101",
                "low": "98",
                "close": "100",
                "volume": "900000",
            },
        ],
    }


def test_adjusted_daily_observation_uses_header_auth_and_never_exposes_key(monkeypatch) -> None:
    requests = []

    def fake_urlopen(request, *, timeout: float):
        requests.append((request, timeout))
        return _Response(_payload())

    monkeypatch.setattr(twelve_data_daily, "urlopen", fake_urlopen)

    observation = twelve_data_daily.observe_twelve_data_adjusted_daily_bars(
        api_key="private-key",
        symbol="soxl",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    assert observation.status == SOURCE_OBSERVATION_READY
    assert observation.snapshot is not None
    assert tuple(bar.session_date for bar in observation.snapshot.bars) == ("2026-08-20", "2026-08-21")
    request, timeout = requests[0]
    assert request.get_header("Authorization") == "apikey private-key"
    assert "private-key" not in request.full_url
    assert "adjust=all" in request.full_url
    assert "end_date=2026-08-22" in request.full_url
    assert timeout == twelve_data_daily.TWELVE_DATA_TIMEOUT_SECONDS


def test_missing_key_and_http_forbidden_become_safe_unavailable_states(monkeypatch) -> None:
    missing = twelve_data_daily.observe_twelve_data_adjusted_daily_bars(
        api_key=None,
        symbol="SOXL",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    def forbidden(*args, **kwargs):
        raise HTTPError("https://api.twelvedata.com/time_series", 403, "forbidden", {}, None)

    monkeypatch.setattr(twelve_data_daily, "urlopen", forbidden)
    forbidden_result = twelve_data_daily.observe_twelve_data_adjusted_daily_bars(
        api_key="private-key",
        symbol="SOXL",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    assert missing.status == SOURCE_OBSERVATION_UNAVAILABLE
    assert missing.reason_codes == (twelve_data_daily.TWELVE_DATA_SECRET_NOT_CONFIGURED,)
    assert forbidden_result.status == SOURCE_OBSERVATION_UNAVAILABLE
    assert forbidden_result.reason_codes == (twelve_data_daily.TWELVE_DATA_AUTH_OR_ENTITLEMENT,)


def test_invalid_provider_payload_becomes_a_safe_invalid_state(monkeypatch) -> None:
    monkeypatch.setattr(twelve_data_daily, "urlopen", lambda *args, **kwargs: _Response({"status": "error"}))

    observation = twelve_data_daily.observe_twelve_data_adjusted_daily_bars(
        api_key="private-key",
        symbol="SOXL",
        start_date="2026-08-20",
        date_cutoff="2026-08-21",
    )

    assert observation.status == SOURCE_OBSERVATION_INVALID
    assert observation.reason_codes == (twelve_data_daily.TWELVE_DATA_PAYLOAD_INVALID,)
