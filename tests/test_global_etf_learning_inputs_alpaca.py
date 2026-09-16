from __future__ import annotations

import importlib.util
import json
import math
import socket
from datetime import date
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pytest

_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "acquire_global_etf_learning_inputs_alpaca.py"
_SPEC = importlib.util.spec_from_file_location("global_etf_learning_inputs_alpaca", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
module = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(module)


def _sessions() -> tuple[date, ...]:
    return (date(2016, 1, 4), date(2016, 1, 5))


def _bar(day: date, *, close: float = 100.0, timestamp: str | None = None) -> dict[str, object]:
    return {
        "t": timestamp or f"{day.isoformat()}T05:00:00Z",
        "o": close - 1.0,
        "h": close + 1.0,
        "l": close - 2.0,
        "c": close,
        "v": 1000,
    }


def _payload(symbol: str, *, sessions: tuple[date, ...] = _sessions()) -> dict[str, object]:
    return {"bars": {symbol: [_bar(day, close=100.0 + index) for index, day in enumerate(sessions)]}}


class _Transport:
    def __init__(self, responder=None) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.responder = responder

    def __call__(self, url: str, params: dict[str, str]) -> dict[str, object]:
        self.calls.append((url, dict(params)))
        if self.responder is not None:
            return self.responder(params)
        return _payload(params["symbols"])


def _patch_small_calendar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_expected_xnys_sessions", lambda: _sessions())


def test_collects_the_fixed_27_symbols_once_with_frozen_request_params(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    transport = _Transport()

    result = module.collect_global_etf_learning_inputs(transport)

    assert len(module.SYMBOLS) == 27
    assert [params["symbols"] for _, params in transport.calls] == list(module.SYMBOLS)
    assert len(transport.calls) == 27
    assert all(url == module._ALPACA_BARS_URL for url, _ in transport.calls)
    assert all(
        params == {
            "symbols": params["symbols"],
            "timeframe": "1Day",
            "start": "2016-01-01T00:00:00-05:00",
            "end": "2025-01-01T00:00:00-05:00",
            "asof": "2024-12-31",
            "feed": "sip",
            "adjustment": "all",
            "currency": "USD",
            "sort": "asc",
            "limit": "10000",
        }
        for _, params in transport.calls
    )
    assert len(result["bars"]) == 54
    assert result["summary"] == {
        "provider": "Alpaca",
        "feed": "sip",
        "adjustment": "all",
        "asof": "2024-12-31",
        "currency": "USD",
        "symbol_count": 27,
        "bar_count": 54,
        "first_session": "2016-01-04",
        "last_session": "2016-01-05",
        "pit_verified": False,
    }
    assert result["controls"] == {"no_order": True, "promotion_eligible": False, "live_ready": False}


@pytest.mark.parametrize("status", [401, 403, 429])
def test_provider_http_errors_stop_on_first_symbol_and_are_sanitized(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    _patch_small_calendar(monkeypatch)

    class ProviderError(RuntimeError):
        status_code = status

    transport = _Transport(responder=lambda _params: (_ for _ in ()).throw(ProviderError("secret response")))

    with pytest.raises(module.GlobalEtfLearningInputError, match="provider request failed") as exc_info:
        module.collect_global_etf_learning_inputs(transport)

    assert "secret" not in str(exc_info.value)
    assert len(transport.calls) == 1


def test_injected_boundary_errors_are_sanitized_and_stop_without_partial_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_small_calendar(monkeypatch)

    def responder(params: dict[str, str]) -> dict[str, object]:
        if params["symbols"] == module.SYMBOLS[1]:
            raise module.GlobalEtfLearningInputError("secret provider detail")
        return _payload(params["symbols"])

    transport = _Transport(responder=responder)
    with pytest.raises(module.GlobalEtfLearningInputError, match="provider request failed") as exc_info:
        module.collect_global_etf_learning_inputs(transport)

    assert "secret" not in str(exc_info.value)
    assert len(transport.calls) == 2


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload | {"next_page_token": "next"},
        lambda payload: {"bars": {**payload["bars"], "EXTRA": []}},
        lambda payload: {"bars": {next(iter(payload["bars"])): payload["bars"][next(iter(payload["bars"]))][:-1]}},
        lambda payload: {
            "bars": {
                next(iter(payload["bars"])): [payload["bars"][next(iter(payload["bars"]))][0]] * 2,
            }
        },
        lambda payload: {
            "bars": {
                next(iter(payload["bars"])): [
                    _bar(date(2015, 12, 31)),
                    _bar(date(2016, 1, 5)),
                ]
            }
        },
        lambda payload: {
            "bars": {
                next(iter(payload["bars"])): [
                    _bar(date(2016, 1, 4), close=math.nan),
                    _bar(date(2016, 1, 5)),
                ]
            }
        },
        lambda payload: {
            "bars": {
                next(iter(payload["bars"])): [
                    _bar(date(2016, 1, 5)),
                    _bar(date(2016, 1, 4)),
                ]
            }
        },
        lambda payload: {
            "bars": {
                next(iter(payload["bars"])): [
                    _bar(date(2016, 1, 4), close=float("inf")),
                    _bar(date(2016, 1, 5)),
                ]
            }
        },
        lambda payload: {
            "bars": {
                next(iter(payload["bars"])): [
                    {**_bar(date(2016, 1, 4)), "v": True},
                    _bar(date(2016, 1, 5)),
                ]
            }
        },
    ],
)
def test_invalid_provider_payloads_fail_without_returning_partial_data(
    monkeypatch: pytest.MonkeyPatch, mutator
) -> None:
    _patch_small_calendar(monkeypatch)
    mutated = mutator(_payload(module.SYMBOLS[0]))

    def responder(params: dict[str, str]) -> dict[str, object]:
        return mutated if params["symbols"] == module.SYMBOLS[0] else _payload(params["symbols"])

    transport = _Transport(responder=responder)
    with pytest.raises(module.GlobalEtfLearningInputError):
        module.collect_global_etf_learning_inputs(transport)
    assert len(transport.calls) == 1


def test_timestamp_is_converted_from_utc_to_new_york_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_expected_xnys_sessions", lambda: (date(2016, 1, 4),))
    symbol = module.SYMBOLS[0]

    def responder(params: dict[str, str]) -> dict[str, object]:
        if params["symbols"] == symbol:
            return {"bars": {symbol: [_bar(date(2016, 1, 4), timestamp="2016-01-05T00:30:00Z")]}}
        return _payload(params["symbols"], sessions=(date(2016, 1, 4),))

    transport = _Transport(responder=responder)

    result = module.collect_global_etf_learning_inputs(transport)

    assert result["bars"][0]["date"] == "2016-01-04"


def test_default_calendar_is_the_real_fixed_xnys_window() -> None:
    expected = module._expected_xnys_sessions()

    assert len(expected) == 2264
    assert expected[0] == date(2016, 1, 4)
    assert expected[-1] == date(2024, 12, 31)


class _Response:
    status = 200
    headers = {}

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return self._body


class _Opener:
    def __init__(self, result) -> None:
        self.calls: list[tuple[object, int]] = []
        self.result = result

    def open(self, request, *, timeout: int):
        self.calls.append((request, timeout))
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def test_https_transport_uses_fixed_request_and_no_proxy_or_redirect_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handlers = []
    monkeypatch.setattr(module, "build_opener", lambda *items: handlers.extend(items) or _Opener(_Response(b'{"bars":{}}')))

    transport = module.AlpacaHttpsTransport("key-id", "secret-key")
    payload = transport(module._ALPACA_BARS_URL, module._request_params(module.SYMBOLS[0]))

    assert payload == {"bars": {}}
    assert any(isinstance(item, module.ProxyHandler) and item.proxies == {} for item in handlers)
    assert any(isinstance(item, module._RejectRedirects) for item in handlers)


def test_https_transport_has_no_retry_and_does_not_leak_provider_error() -> None:
    opener = _Opener(HTTPError(module._ALPACA_BARS_URL, 429, "secret response", {}, BytesIO(b"secret")))
    transport = module.AlpacaHttpsTransport("key-id", "secret-key", opener=opener)

    with pytest.raises(module.GlobalEtfLearningInputError, match="provider request failed") as exc_info:
        transport(module._ALPACA_BARS_URL, module._request_params(module.SYMBOLS[0]))

    assert "secret" not in str(exc_info.value)
    assert len(opener.calls) == 1


def test_https_transport_rejects_redirect_without_following() -> None:
    opener = _Opener(HTTPError(module._ALPACA_BARS_URL, 302, "redirect secret", {}, BytesIO(b"secret")))
    transport = module.AlpacaHttpsTransport("key-id", "secret-key", opener=opener)

    with pytest.raises(module.GlobalEtfLearningInputError):
        transport(module._ALPACA_BARS_URL, module._request_params(module.SYMBOLS[0]))

    assert len(opener.calls) == 1


def test_cli_is_plan_only_does_not_open_socket_or_write_files(
    monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path
) -> None:
    monkeypatch.setenv("ALPACA_API_KEY_ID", "secret-id")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "secret-key")
    monkeypatch.chdir(tmp_path)

    def fail_socket(*_args, **_kwargs):
        raise AssertionError("CLI must not open a socket")

    monkeypatch.setattr(socket, "socket", fail_socket)

    module.main([])
    output = json.loads(capsys.readouterr().out)

    assert output["status"] == "PLAN_ONLY"
    assert output["request_count"] == 27
    assert output["symbols"] == list(module.SYMBOLS)
    assert output["learning_start"] == "2017-01-01"
    assert output["controls"] == {"no_order": True, "promotion_eligible": False, "live_ready": False}
    assert output["pit_verified"] is False
    assert list(tmp_path.iterdir()) == []


class _TargetBlob:
    def __init__(self, *, exists: bool = False, error: BaseException | None = None) -> None:
        self._exists = exists
        self.error = error
        self.uploads: list[dict[str, object]] = []

    def exists(self, *, retry, timeout):
        assert retry is None
        assert timeout == 30
        return self._exists

    def upload_from_string(self, payload: str, **kwargs) -> None:
        if self.error is not None:
            raise self.error
        self.uploads.append({"payload": payload, **kwargs})


class _Bucket:
    def __init__(self, target: _TargetBlob) -> None:
        self.target = target

    def blob(self, name: str):
        assert name == "global-etf-learning/2017-2024/20260916/bars.json"
        return self.target


class _StorageClient:
    def __init__(self, bucket: _Bucket) -> None:
        self._bucket = bucket
        self.calls = 0

    def bucket(self, name: str):
        assert name == "qsl-runtime-logs-shared"
        self.calls += 1
        return self._bucket


def _cloud_environment() -> dict[str, str]:
    return {
        "GITHUB_ACTIONS": "true",
        "GITHUB_WORKFLOW": "Global ETF Learning Inputs",
        "ALPACA_API_KEY_ID": "key-id",
        "ALPACA_API_SECRET_KEY": "secret-key",
    }


def test_cloud_execution_requires_github_actions_before_storage_or_http(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    storage = _StorageClient(_Bucket(_TargetBlob()))
    transport = _Transport()

    with pytest.raises(module.GlobalEtfLearningInputError, match="cloud execution unavailable"):
        module.execute_global_etf_learning_inputs_cloud(
            storage_client=storage, http_transport=transport, environ={**_cloud_environment(), "GITHUB_ACTIONS": "false"}
        )

    assert storage.calls == 0
    assert transport.calls == []


def test_cloud_execution_target_exists_stops_before_alpaca(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    storage = _StorageClient(_Bucket(_TargetBlob(exists=True)))
    transport = _Transport()

    with pytest.raises(module.GlobalEtfLearningInputError, match="target object already exists"):
        module.execute_global_etf_learning_inputs_cloud(
            storage_client=storage, http_transport=transport, environ=_cloud_environment()
        )

    assert transport.calls == []


def test_cloud_execution_unknown_target_check_stops_before_alpaca(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)

    class UnknownTarget(_TargetBlob):
        def exists(self, *, retry, timeout):
            raise RuntimeError("secret gcs state")

    storage = _StorageClient(_Bucket(UnknownTarget()))
    transport = _Transport()

    with pytest.raises(module.GlobalEtfLearningInputError, match="bucket preflight unavailable") as exc_info:
        module.execute_global_etf_learning_inputs_cloud(
            storage_client=storage, http_transport=transport, environ=_cloud_environment()
        )

    assert "secret" not in str(exc_info.value)
    assert transport.calls == []


def test_cloud_execution_uploads_one_full_result_with_create_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    target = _TargetBlob()
    storage = _StorageClient(_Bucket(target))
    transport = _Transport()

    result = module.execute_global_etf_learning_inputs_cloud(
        storage_client=storage, http_transport=transport, environ=_cloud_environment()
    )

    assert result["status"] == "COLLECTED_AND_UPLOADED"
    assert len(transport.calls) == 27
    assert len(target.uploads) == 1
    upload = target.uploads[0]
    assert upload["if_generation_match"] == 0
    assert upload["retry"] is None
    assert upload["timeout"] == 30
    stored = json.loads(upload["payload"])
    assert len(stored["bars"]) == 54
    assert stored["learning_only"] is True
    assert stored["size_zero_required"] is True
    assert stored["controls"]["no_order"] is True
    assert stored["summary"]["pit_verified"] is False


def test_cloud_upload_failure_is_unknown_and_does_not_leak_details(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    target = _TargetBlob(error=RuntimeError("secret gcs response"))
    storage = _StorageClient(_Bucket(target))

    with pytest.raises(module.GlobalEtfLearningInputError, match="object upload unknown") as exc_info:
        module.execute_global_etf_learning_inputs_cloud(
            storage_client=storage, http_transport=_Transport(), environ=_cloud_environment()
        )

    assert "secret" not in str(exc_info.value)
