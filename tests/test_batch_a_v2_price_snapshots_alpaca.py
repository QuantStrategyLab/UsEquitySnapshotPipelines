from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import socket
from datetime import date
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pytest

_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "acquire_batch_a_v2_price_snapshots_alpaca.py"
_SPEC = importlib.util.spec_from_file_location("batch_a_v2_price_snapshots_alpaca", _SCRIPT_PATH)
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


class _Storage:
    def __init__(self, *, existing: set[str] | None = None, fail_on: str | None = None) -> None:
        self.existing = set(existing or ())
        self.fail_on = fail_on
        self.uploads: list[dict[str, object]] = []
        self._payloads: dict[str, bytes] = {}
        self._generations: dict[str, str] = {}
        self.exists_calls: list[str] = []
        self._next_generation = 1000

    def exists(self, object_path: str) -> bool:
        self.exists_calls.append(object_path)
        return object_path in self.existing

    def upload_create_only(self, object_path: str, payload: bytes, *, content_type: str) -> None:
        if self.fail_on == object_path or self.fail_on == "any":
            raise RuntimeError("secret gcs response")
        if object_path in self.existing or object_path in self._payloads:
            raise RuntimeError("already exists")
        self.uploads.append({"object": object_path, "payload": payload, "content_type": content_type})
        self._payloads[object_path] = payload
        self._generations[object_path] = str(self._next_generation)
        self._next_generation += 1
        self.existing.add(object_path)

    def readback_identity(self, object_path: str) -> tuple[str, int]:
        if object_path not in self._payloads:
            raise RuntimeError("missing object")
        return self._generations[object_path], len(self._payloads[object_path])


def _patch_small_calendar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "_expected_xnys_sessions", lambda: _sessions())


def _cloud_environment() -> dict[str, str]:
    return {
        "GITHUB_ACTIONS": "true",
        "GITHUB_WORKFLOW": "Batch A v2 Alpaca SIP Inputs",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_SHA": "abc123deadbeef",
        "ALPACA_API_KEY_ID": "key-id",
        "ALPACA_API_SECRET_KEY": "secret-key",
    }


def test_cli_plan_only_does_not_open_socket_or_write_files(
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
    assert output["schema"] == "qsl.research.price_snapshot.v2"
    assert output["sleeves"] == {"soxl_soxx": ["SOXX", "SOXL"], "tqqq_qqq": ["QQQ", "TQQQ"]}
    assert output["request_count"] == 4
    assert output["controls"] == {
        "no_order": True,
        "research_only": True,
        "execution_authorized": False,
    }
    assert list(tmp_path.iterdir()) == []


def test_execute_rejects_invalid_batch_id_and_non_main_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_small_calendar(monkeypatch)
    storage = _Storage()
    transport = _Transport()

    with pytest.raises(module.BatchAV2AcquisitionError, match="batch id rejected"):
        module.execute_batch_a_v2_acquisition(
            batch_id="../evil",
            storage=storage,
            http_transport=transport,
            environ=_cloud_environment(),
        )
    assert transport.calls == []
    assert storage.uploads == []

    with pytest.raises(module.BatchAV2AcquisitionError, match="main branch required"):
        module.execute_batch_a_v2_acquisition(
            batch_id="batch-a-demo",
            storage=storage,
            http_transport=transport,
            environ={**_cloud_environment(), "GITHUB_REF": "refs/heads/feature"},
        )
    assert transport.calls == []


def test_execute_requires_github_actions_before_storage_or_http(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    storage = _Storage()
    transport = _Transport()

    with pytest.raises(module.BatchAV2AcquisitionError, match="cloud execution unavailable"):
        module.execute_batch_a_v2_acquisition(
            batch_id="batch-a-demo",
            storage=storage,
            http_transport=transport,
            environ={**_cloud_environment(), "GITHUB_ACTIONS": "false"},
        )

    assert storage.exists_calls == []
    assert transport.calls == []


def test_target_exists_stops_before_alpaca(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    existing = {
        "research/v2/input/batch-a-demo/soxl_soxx/prices.csv",
    }
    storage = _Storage(existing=existing)
    transport = _Transport()

    with pytest.raises(module.BatchAV2AcquisitionError, match="target object already exists"):
        module.execute_batch_a_v2_acquisition(
            batch_id="batch-a-demo",
            storage=storage,
            http_transport=transport,
            environ=_cloud_environment(),
        )

    assert transport.calls == []
    assert storage.uploads == []


@pytest.mark.parametrize("status", [403, 429])
def test_provider_http_errors_stop_without_retry(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    _patch_small_calendar(monkeypatch)

    class ProviderError(RuntimeError):
        status_code = status

    transport = _Transport(responder=lambda _params: (_ for _ in ()).throw(ProviderError("secret")))
    storage = _Storage()

    with pytest.raises(module.BatchAV2AcquisitionError, match="provider request failed") as exc_info:
        module.execute_batch_a_v2_acquisition(
            batch_id="batch-a-demo",
            storage=storage,
            http_transport=transport,
            environ=_cloud_environment(),
        )

    assert "secret" not in str(exc_info.value)
    assert len(transport.calls) == 1
    assert storage.uploads == []


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload | {"next_page_token": "next"},
        lambda payload: {
            "bars": {next(iter(payload["bars"])): payload["bars"][next(iter(payload["bars"]))][:-1]}
        },
        lambda payload: {
            "bars": {next(iter(payload["bars"])): [payload["bars"][next(iter(payload["bars"]))][0]] * 2}
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
                    _bar(date(2016, 1, 4), close=math.nan),
                    _bar(date(2016, 1, 5)),
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
    ],
)
def test_invalid_provider_payloads_fail_closed(monkeypatch: pytest.MonkeyPatch, mutator) -> None:
    _patch_small_calendar(monkeypatch)
    first_symbol = module._ALL_SYMBOLS[0]
    mutated = mutator(_payload(first_symbol))

    def responder(params: dict[str, str]) -> dict[str, object]:
        return mutated if params["symbols"] == first_symbol else _payload(params["symbols"])

    transport = _Transport(responder=responder)
    storage = _Storage()

    with pytest.raises(module.BatchAV2AcquisitionError):
        module.execute_batch_a_v2_acquisition(
            batch_id="batch-a-demo",
            storage=storage,
            http_transport=transport,
            environ=_cloud_environment(),
        )
    assert len(transport.calls) == 1
    assert storage.uploads == []


def test_successful_execute_writes_canonical_create_only_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_small_calendar(monkeypatch)
    storage = _Storage()
    transport = _Transport()

    result = module.execute_batch_a_v2_acquisition(
        batch_id="batch-a-demo",
        storage=storage,
        http_transport=transport,
        environ=_cloud_environment(),
        retrieved_at="2026-09-22T00:00:00Z",
        code_version="test-code",
    )

    assert result["status"] == "COLLECTED_AND_UPLOADED"
    assert [params["symbols"] for _, params in transport.calls] == ["SOXX", "SOXL", "QQQ", "TQQQ"]
    assert all(
        params
        == {
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
    assert len(storage.uploads) == 6
    assert storage.exists_calls == [
        "research/v2/input/batch-a-demo/soxl_soxx/prices.csv",
        "research/v2/input/batch-a-demo/soxl_soxx/object_identity.json",
        "research/v2/input/batch-a-demo/soxl_soxx/prices.csv.manifest.json",
        "research/v2/input/batch-a-demo/tqqq_qqq/prices.csv",
        "research/v2/input/batch-a-demo/tqqq_qqq/object_identity.json",
        "research/v2/input/batch-a-demo/tqqq_qqq/prices.csv.manifest.json",
    ]

    for sleeve, symbols in module.SLEEVES.items():
        prices_object = f"research/v2/input/batch-a-demo/{sleeve}/prices.csv"
        identity_object = f"research/v2/input/batch-a-demo/{sleeve}/object_identity.json"
        manifest_object = f"research/v2/input/batch-a-demo/{sleeve}/prices.csv.manifest.json"
        prices_upload = next(item for item in storage.uploads if item["object"] == prices_object)
        identity_upload = next(item for item in storage.uploads if item["object"] == identity_object)
        manifest_upload = next(item for item in storage.uploads if item["object"] == manifest_object)
        upload_order = [item["object"] for item in storage.uploads if f"/{sleeve}/" in item["object"]]
        assert upload_order == [prices_object, identity_object, manifest_object]

        prices = prices_upload["payload"]
        assert prices.startswith(b"symbol,as_of,open,high,low,close,volume\n")
        lines = prices.decode().splitlines()[1:]
        assert [line.split(",")[0] for line in lines[:2]] == sorted(symbols)
        sha256 = hashlib.sha256(prices).hexdigest()

        identity = json.loads(identity_upload["payload"])
        assert identity["generation"].isdigit()
        assert identity["bytes"] == len(prices)
        assert identity["sha256"] == sha256
        assert identity["object"] == prices_object

        manifest = json.loads(manifest_upload["payload"])
        assert set(manifest) == {
            "schema",
            "research_only",
            "dataset_id",
            "provider",
            "feed",
            "price_field",
            "adjustment",
            "calendar",
            "timezone",
            "license_retention",
            "code_version",
            "source_revision",
            "retrieved_at",
            "symbols",
            "request",
            "gcs",
            "counts",
            "coverage",
        }
        assert manifest["schema"] == "qsl.research.price_snapshot.v2"
        assert manifest["research_only"] is True
        assert manifest["dataset_id"] == f"batch-a-demo/{sleeve}"
        assert manifest["provider"] == "alpaca"
        assert manifest["feed"] == "sip"
        assert manifest["price_field"] == "adjusted_close"
        assert manifest["adjustment"] == "all"
        assert manifest["symbols"] == list(symbols)
        assert manifest["request"] == {"start": "2016-01-01", "end_exclusive": "2025-01-01"}
        assert manifest["gcs"] == {
            "bucket": "qsl-research-evidence-831478360303",
            "object": prices_object,
            "generation": identity["generation"],
            "bytes": len(prices),
            "sha256": sha256,
        }
        assert manifest["counts"] == {symbol: 2 for symbol in symbols}
        assert manifest["coverage"] == {
            symbol: {"start": "2016-01-04", "end": "2016-01-05"} for symbol in symbols
        }
        assert manifest["code_version"] == "test-code"
        assert manifest["retrieved_at"] == "2026-09-22T00:00:00Z"


def test_upload_unknown_is_reported_without_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_small_calendar(monkeypatch)
    storage = _Storage(fail_on="research/v2/input/batch-a-demo/soxl_soxx/prices.csv")
    transport = _Transport()

    with pytest.raises(module.BatchAV2AcquisitionError) as exc_info:
        module.execute_batch_a_v2_acquisition(
            batch_id="batch-a-demo",
            storage=storage,
            http_transport=transport,
            environ=_cloud_environment(),
        )

    assert exc_info.value.status == "UNKNOWN"
    assert exc_info.value.code == "OBJECT_UPLOAD_UNKNOWN"
    assert "secret" not in str(exc_info.value)
    assert len(transport.calls) == 4
    assert storage.uploads == []


def test_default_calendar_is_the_real_fixed_xnys_window() -> None:
    expected = module._expected_xnys_sessions()
    assert len(expected) == 2264
    assert expected[0] == date(2016, 1, 4)
    assert expected[-1] == date(2024, 12, 31)


class _Response:
    status = 200

    def __init__(self, body: bytes) -> None:
        self._body = body
        self.headers: dict[str, str] = {}

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


def test_https_transport_rejects_403_and_429_without_retry() -> None:
    for status in (403, 429):
        opener = _Opener(HTTPError(module._ALPACA_BARS_URL, status, "secret", {}, BytesIO(b"secret")))
        transport = module.AlpacaHttpsTransport("key-id", "secret-key", opener=opener)
        with pytest.raises(module.BatchAV2AcquisitionError, match=f"provider http {status}") as exc_info:
            transport(module._ALPACA_BARS_URL, module._request_params("SOXX"))
        assert "secret" not in str(exc_info.value)
        assert len(opener.calls) == 1


def test_cli_execute_without_batch_id_prints_parked(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "Batch A v2 Alpaca SIP Inputs")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    with pytest.raises(SystemExit) as exc_info:
        module.main(["--execute"])
    assert exc_info.value.code == 2
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "PARKED"
    assert output["reason_code"] == "batch id rejected"
    assert output["execution_authorized"] is False
