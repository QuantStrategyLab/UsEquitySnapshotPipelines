from __future__ import annotations

import hashlib
import importlib.util

import pandas as pd
import pytest
from google.api_core.exceptions import Forbidden
from google.cloud import storage

_SPEC = importlib.util.spec_from_file_location("run_russell_core_case", "scripts/run_russell_core_case.py")
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_MODULE)

(
    SYMBOLS,
    _expected_sessions,
    fetch_bars,
    load_feature_snapshot,
    run_case,
    validate_bars_payload,
) = (_MODULE.SYMBOLS, _MODULE._expected_sessions, _MODULE.fetch_bars, _MODULE.load_feature_snapshot, _MODULE.run_case, _MODULE.validate_bars_payload)


def _payload():
    return {
        "bars": {
            symbol: [
                {"t": f"{session.isoformat()}T04:00:00Z", "o": 100.0, "c": 101.0}
                for session in _expected_sessions()
            ]
            for symbol in SYMBOLS
        }
    }


def _synthetic_features():
    rows = []
    for rank, symbol in enumerate((*SYMBOLS, "QQQ", "SPY", "BOXX"), start=1):
        rows.append({
            "as_of": "2026-08-31", "available_at": "2026-09-01T05:18:23Z", "symbol": symbol,
            "sector": "Technology", "close": 100.0 + rank, "adv20_usd": 100_000_000.0,
            "history_days": 300, "mom_3m": 0.20 - rank * 0.01, "mom_6m": 0.40 - rank * 0.01,
            "mom_12_1": 0.50 - rank * 0.01, "rel_mom_6m_vs_benchmark": 0.30 - rank * 0.01,
            "rel_mom_6m_vs_broad_benchmark": 0.25 - rank * 0.01, "high_252_gap": -0.02,
            "sma200_gap": 0.10, "vol_63": 0.20, "maxdd_126": -0.05,
            "eligible": symbol in SYMBOLS,
        })
    return pd.DataFrame(rows)


class _Response:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_fetch_uses_one_non_redirecting_request_without_secret_output():
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(_payload())

    frame = fetch_bars(get, "id", "secret")
    assert len(frame) == 32
    assert len(calls) == 1
    assert calls[0][1]["allow_redirects"] is False
    assert "secret" not in calls[0][0]


@pytest.mark.parametrize("status", [403, 429])
def test_http_rejection_does_not_retry_or_parse_body(status):
    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        return type("Response", (), {"status_code": status, "text": "secret-body"})()

    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        fetch_bars(get, "id", "secret")
    assert len(calls) == 1


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda p: p.update(next_page_token="more"), "pagination"),
        (lambda p: p["bars"].pop("AMD"), "exactly"),
        (lambda p: p["bars"]["AMD"].__setitem__(0, {**p["bars"]["AMD"][0], "o": 0}), "price"),
        (lambda p: p["bars"]["AMD"].__setitem__(1, p["bars"]["AMD"][0].copy()), "duplicate"),
        (lambda p: p["bars"]["AMD"].__setitem__(0, {**p["bars"]["AMD"][0], "c": float("nan")}), "price"),
        (lambda p: p["bars"]["AMD"].__setitem__(0, {**p["bars"]["AMD"][0], "t": "2026-09-01T05:00:00Z"}), "bar"),
        (lambda p: p["bars"]["AMD"].pop(), "complete"),
    ],
)
def test_bars_contract_rejects_bad_or_paged_payload(mutate, message):
    payload = _payload()
    mutate(payload)
    with pytest.raises((ValueError, TypeError), match=message):
        validate_bars_payload(payload)


def test_feature_generation_hash_is_checked_once(tmp_path):
    raw = b"as_of,symbol\n2026-08-31,AMD\n"

    class Blob:
        def __init__(self):
            self.calls = 0

        def download_as_bytes(self, *, retry):
            self.calls += 1
            assert retry is None
            return raw

    blob = Blob()

    class Bucket:
        def blob(self, name, *, generation):
            assert generation == "g"
            return blob

    class Client:
        def bucket(self, name):
            assert name == "bucket"
            return Bucket()

    with pytest.raises(ValueError, match="hash"):
        load_feature_snapshot(Client(), bucket="bucket", object_name="object", generation="g", expected_sha256=hashlib.sha256(b"other").hexdigest())
    assert blob.calls == 1


def test_success_reuses_core_runner_and_outputs_only_aggregates():
    features = _synthetic_features()
    prices = pd.DataFrame(
        [
            {"session": session.isoformat(), "symbol": symbol, "open": 100.0, "close": 101.0}
            for session in _expected_sessions()
            for symbol in SYMBOLS
        ]
    )
    summary = run_case(features, prices, data_kind="synthetic")
    assert summary["research_flags"]["research_scope"] == "core_signal_only"
    assert summary["data_kind"] == "synthetic"
    assert summary["research_flags"]["promotion_eligible"] is False
    assert summary["sample_count"] == 8
    assert set(summary) == {"strategy_profile", "variant", "data_kind", "source", "sample_count", "total_return", "max_drawdown", "fees", "research_flags"}


def test_main_converts_provider_and_google_errors_to_safe_reason(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY_ID", "id")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret")

    monkeypatch.setattr(storage, "Client", lambda: object())

    def fail(*args, **kwargs):
        raise Forbidden("sensitive-marker")

    monkeypatch.setattr(_MODULE, "load_feature_snapshot", fail)
    monkeypatch.setattr(_MODULE.requests, "get", lambda *args, **kwargs: pytest.fail("provider called"))
    with pytest.raises(SystemExit, match="bounded_validation_or_provider_error"):
        _MODULE.main(["--run"])


def test_noop_core_preflight_is_rejected():
    features = _synthetic_features()
    features["available_at"] = "2026-09-20T05:18:23Z"
    prices = pd.DataFrame(
        [
            {"session": session.isoformat(), "symbol": symbol, "open": 100.0, "close": 100.0}
            for session in _expected_sessions()
            for symbol in SYMBOLS
        ]
    )
    with pytest.raises(ValueError, match="four requested target positions"):
        run_case(features, prices, data_kind="synthetic")
