import io
import json
import runpy
from pathlib import Path
from urllib.error import HTTPError

import pytest

from scripts.acquire_qqqm_boxx_raw_history import (
    MAX_PAGES,
    PREFIX,
    AcquisitionError,
    BoundedProvider,
    _action_summary,
    _bars_summary,
    _provider_error,
    run,
)


class _Response:
    status = 200

    def __init__(self, body):
        self.body = body
        self.headers = {"Content-Length": str(len(body))}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _size):
        return self.body


class _Opener:
    def __init__(self, body=None, error=None):
        self.body = body
        self.error = error
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request.full_url, timeout))
        if self.error:
            raise self.error
        return _Response(self.body)


def test_provider_fixed_host_scope_and_page_budget():
    opener = _Opener(body=b'{"bars":[],"next_page_token":null}')
    provider = BoundedProvider("key", "secret", opener=opener)
    with pytest.raises(AcquisitionError, match="ENDPOINT_OUT_OF_SCOPE"):
        provider.get("/v2/stocks/AAPL/bars", {})
    for _ in range(MAX_PAGES):
        provider.get("/v2/stocks/QQQM/bars", {"feed": "sip"})
    assert provider.pages == MAX_PAGES
    assert opener.calls[0][0].startswith("https://data.alpaca.markets/v2/stocks/QQQM/bars?")
    with pytest.raises(AcquisitionError, match="PAGE_BUDGET_EXHAUSTED"):
        provider.get("/v2/stocks/QQQM/bars", {})
    assert len(opener.calls) == MAX_PAGES


def test_provider_error_is_sanitized_and_counted():
    body = b'{"code":42210000,"message":"email x@example.org token abcdefghijklmnopqrstuvwxyz"}'
    opener = _Opener(error=HTTPError("https://data.alpaca.markets", 403, "Forbidden", {}, io.BytesIO(body)))
    provider = BoundedProvider("key", "secret", opener=opener)
    with pytest.raises(AcquisitionError) as caught:
        provider.get("/v2/stocks/BOXX/bars", {})
    assert caught.value.code == "PROVIDER_REJECTED"
    assert caught.value.status == 403
    assert caught.value.provider_code == 42210000
    assert caught.value.provider_message is None
    assert provider.pages == 1
    assert provider.bytes == len(body)
    assert _provider_error(b"invalid") is None


def test_bar_summary_rejects_duplicate_and_bad_ohlc():
    bar = {"t": "2024-06-03T04:00:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}
    assert _bars_summary({"bars": [bar]}, "QQQM", None, "2020-10-13T04:00:00Z") == (
        1, "2024-06-03T04:00:00Z", "2024-06-03T04:00:00Z"
    )
    with pytest.raises(AcquisitionError, match="BAR_DUPLICATE_OR_ORDER"):
        _bars_summary({"bars": [bar, bar]}, "QQQM", None, "2020-10-13T04:00:00Z")
    with pytest.raises(AcquisitionError, match="BAR_OHLCV_INVALID"):
        _bars_summary({"bars": [{**bar, "h": 1}]}, "QQQM", None, "2020-10-13T04:00:00Z")
    with pytest.raises(AcquisitionError, match="BAR_TIMESTAMP_INVALID"):
        _bars_summary({"bars": [{**bar, "t": "2025-01-01T05:00:00.000001Z"}]},
                      "QQQM", None, "2020-10-13T04:00:00Z")


def test_grouped_corporate_actions_count_and_symbol():
    payload = {"corporate_actions": {"cash_dividends": [{"symbol": "BOXX", "rate": "0.12"}],
                                      "forward_splits": []}}
    assert _action_summary(payload, "BOXX") == 1
    with pytest.raises(AcquisitionError, match="ACTION_SYMBOL_MISMATCH"):
        _action_summary(payload, "QQQM")


def test_private_write_probe_precedes_any_provider_request():
    events = []

    class Store:
        def create_and_verify(self, name, body):
            events.append(("write", name))
            assert name == "_write_probe.json"
            assert json.loads(body)["scope"] == PREFIX
            raise AcquisitionError("OBJECT_WRITE_OR_READBACK_UNKNOWN")

    class Provider:
        pages = 0
        bytes = 0

        def get(self, *_args):
            events.append(("provider", None))

    with pytest.raises(AcquisitionError, match="OBJECT_WRITE_OR_READBACK_UNKNOWN"):
        run(Store(), Provider())
    assert events == [("write", "_write_probe.json")]


def test_r9_workflow_uses_only_frozen_extension_scope(monkeypatch):
    monkeypatch.setenv("GITHUB_WORKFLOW", "R9 Raw Temporal Extension")
    module = runpy.run_path(str(Path(__file__).parents[1] / "scripts" /
                                "acquire_qqqm_boxx_raw_history.py"), run_name="r9_test")
    assert module["PREFIX"] == "research/v2/input/r9-temporal-extension-20260926-001/"
    assert module["MAX_PAGES"] == 60
    assert module["MAX_BYTES"] == 128 * 1024 * 1024
    assert module["ACTION_START"] == "2024-10-01"
    assert module["ACTION_END"] == "2026-08-25"
    assert module["ASOF"] == "2026-08-25"
    assert set(module["WINDOWS"]) == {"QQQ", "TQQQ", "QQQM", "SOXL", "SOXX", "BOXX"}
    assert all(value == ("2025-01-01T00:00:00-05:00", "2025-01-01")
               for value in module["WINDOWS"].values())
    bar = {"t": "2026-08-25T04:00:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}
    assert module["_bars_summary"]({"bars": [bar]}, "QQQM", None,
                                    "2025-01-01T05:00:00Z")[0] == 1
    with pytest.raises(module["AcquisitionError"], match="BAR_TIMESTAMP_INVALID"):
        module["_bars_summary"]({"bars": [{**bar, "t": "2026-08-26T04:00:00Z"}]},
                                "QQQM", None, "2025-01-01T05:00:00Z")
