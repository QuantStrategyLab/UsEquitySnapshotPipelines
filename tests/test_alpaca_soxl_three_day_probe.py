from __future__ import annotations

from copy import deepcopy
import hashlib
from http.client import BadStatusLine, IncompleteRead
import io
import json
from pathlib import Path
import runpy
import socket
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse

import pytest

MODULE = runpy.run_path("scripts/alpaca_soxl_three_day_probe.py")
PROBE = MODULE["probe"]
VALIDATE = MODULE["validate"]
ENV = {"GITHUB_ACTIONS": "true", "RUNNER_OS": "Linux", "GITHUB_REF": "refs/heads/main",
       "GITHUB_RUN_ATTEMPT": "1", "DATE_CUTOFF": "2026-10-21",
       "ALPACA_API_KEY_ID": "test-key-not-a-credential", "ALPACA_API_SECRET_KEY": "test-secret-not-a-credential"}


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def denied(*_args, **_kwargs):
        raise AssertionError("Network is forbidden in probe tests")
    for name in ("connect", "connect_ex"):
        monkeypatch.setattr(socket.socket, name, denied)
    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket, "getaddrinfo", denied)


def payload():
    return {"symbol": "SOXL", "next_page_token": None, "bars": [
        {"t": f"2026-10-{day}T04:00:00Z", "o": 50.0, "h": 52.0, "l": 49.0,
         "c": 51.0, "v": 1200, "n": 500, "vw": 50.5} for day in (19, 20, 21)]}


class FakeResponse(io.BytesIO):
    status = 200


class FakeOpener:
    def __init__(self, body=None, error=None):
        self.body = json.dumps(payload()).encode() if body is None else body
        self.error = error
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        assert timeout == 20
        if self.error:
            raise self.error
        return FakeResponse(self.body)


def test_one_fixed_get_records_schema_coverage_hash_without_values():
    opener = FakeOpener()
    result = PROBE(ENV, opener=opener)
    assert len(opener.requests) == result["requests_attempted"] == 1
    request = opener.requests[0]
    assert request.get_method() == "GET"
    headers = {name.lower(): value for name, value in request.header_items()}
    assert headers["apca-api-key-id"] == ENV["ALPACA_API_KEY_ID"]
    assert headers["apca-api-secret-key"] == ENV["ALPACA_API_SECRET_KEY"]
    assert urlparse(request.full_url).path == "/v2/stocks/SOXL/bars"
    assert parse_qs(urlparse(request.full_url).query) == {
        name: [value] for name, value in MODULE["PARAMS"].items()}
    assert result["exit_code"] == 0
    assert result["row_count"] == 3 and result["schema_valid"] is True
    assert result["response_sha256"] == hashlib.sha256(opener.body).hexdigest()
    assert result["session_dates"] == MODULE["DATES"]
    assert result["raw_persisted"] is result["b4_admissible"] is result["historical_pit_verified"] is False
    rendered = json.dumps(result)
    for forbidden in (ENV["ALPACA_API_KEY_ID"], ENV["ALPACA_API_SECRET_KEY"], '"bars"', '"o":', '"vw":'):
        assert forbidden not in rendered


@pytest.mark.parametrize("missing", ["ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"])
def test_missing_credential_stops_before_request(missing):
    env = {**ENV, missing: ""}
    opener = FakeOpener()
    result = PROBE(env, opener=opener)
    assert result["status"] == "BLOCKED_CREDENTIAL_ENTRY_UNAVAILABLE"
    assert result["exit_code"] == 20 and result["requests_attempted"] == 0
    assert not opener.requests


@pytest.mark.parametrize("name,value", [("GITHUB_RUN_ATTEMPT", "2"), ("GITHUB_REF", "refs/heads/probe"),
                                      ("GITHUB_ACTIONS", "false"), ("RUNNER_OS", "macOS"),
                                      ("DATE_CUTOFF", "2026-10-22")])
def test_context_or_rerun_blocked_without_network(name, value):
    opener = FakeOpener()
    result = PROBE({**ENV, name: value}, opener=opener)
    assert result["exit_code"] == 21 and not opener.requests


@pytest.mark.parametrize("status", [301, 302, 307, 308, 401, 403, 429, 500])
def test_http_failure_never_retries_or_reads_error_body(status):
    body = io.BytesIO(b"sensitive-provider-error")
    opener = FakeOpener(error=HTTPError(MODULE["URL"], status, "hidden", {}, body))
    result = PROBE(ENV, opener=opener)
    assert len(opener.requests) == 1 and result["http_status"] == status
    assert result["exit_code"] == 22 and result["response_sha256"] is None
    assert "sensitive-provider-error" not in json.dumps(result)
    assert body.closed


@pytest.mark.parametrize("error", [URLError("private-error"), TimeoutError("private-error"), OSError("private-error")])
def test_transport_failure_is_sanitized_and_not_retried(error):
    opener = FakeOpener(error=error)
    result = PROBE(ENV, opener=opener)
    assert result["status"] == "TRANSPORT_FAILED" and result["exit_code"] == 22
    assert len(opener.requests) == 1 and "private-error" not in json.dumps(result)


@pytest.mark.parametrize("rows,token", [(0, None), (2, None), (3, "private-page-token")])
def test_missing_dates_or_pagination_fails_without_second_request(rows, token):
    sample = payload()
    sample["bars"] = sample["bars"][:rows]
    sample["next_page_token"] = token
    opener = FakeOpener(json.dumps(sample).encode())
    result = PROBE(ENV, opener=opener)
    assert result["schema_valid"] is True and result["exit_code"] == 23
    assert len(opener.requests) == 1 and "private-page-token" not in json.dumps(result)


@pytest.mark.parametrize("field,value", [("t", "2026-10-19T04:00:00"), ("t", "2026-10-19T05:00:00Z"),
                                      ("o", True), ("c", float("nan")), ("n", 0.5), ("v", -1),
                                      ("h", 48.0), ("vw", 0)])
def test_invalid_row_rejected(field, value):
    sample = payload()
    sample["bars"][0][field] = value
    with pytest.raises(ValueError):
        VALIDATE(sample)


def test_duplicates_order_and_missing_schema_are_rejected():
    for mutation in (lambda sample: sample["bars"].reverse(),
                     lambda sample: sample["bars"].append(deepcopy(sample["bars"][0])),
                     lambda sample: sample.pop("next_page_token"),
                     lambda sample: sample["bars"][0].pop("vw")):
        sample = payload()
        mutation(sample)
        with pytest.raises(ValueError):
            VALIDATE(sample)


@pytest.mark.parametrize("body", [b"private-invalid-json", b"x" * 65537])
def test_invalid_or_oversized_body_is_not_persisted(body):
    result = PROBE(ENV, opener=FakeOpener(body))
    assert result["exit_code"] == 24 and "private-invalid-json" not in json.dumps(result)


def test_existing_output_blocks_main_without_request(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    Path("diagnostic.json").write_text("original")
    assert MODULE["main"]() == 21
    assert Path("diagnostic.json").read_text() == "original"
    assert capsys.readouterr().out.strip() == "OUTPUT_EXISTS_NO_REQUEST"


@pytest.mark.parametrize("error", [None, BadStatusLine("private-protocol-response"),
                                  IncompleteRead(b"private-protocol-response", 100)])
def test_main_writes_sanitized_result_for_success_and_protocol_failures(tmp_path, monkeypatch, capsys, error):
    monkeypatch.chdir(tmp_path)
    for name, value in ENV.items():
        monkeypatch.setenv(name, value)
    opener = FakeOpener(error=error)
    monkeypatch.setitem(MODULE["main"].__globals__, "build_opener", lambda *_handlers: opener)
    exit_code = MODULE["main"]()
    artifact = Path("diagnostic.json").read_text()
    result = json.loads(artifact)
    captured = capsys.readouterr()
    assert len(opener.requests) == result["requests_attempted"] == 1
    assert exit_code == result["exit_code"] == (0 if error is None else 22)
    assert result["status"] == ("SCHEMA_AND_COVERAGE_PASS" if error is None else "TRANSPORT_FAILED")
    assert captured.err == ""
    assert json.loads(captured.out.removeprefix("ALPACA_SOXL_PROBE=")) == result
    for forbidden in ("private-protocol-response", ENV["ALPACA_API_KEY_ID"], ENV["ALPACA_API_SECRET_KEY"],
                      '"bars"', '"o":', '"vw":'):
        assert forbidden not in artifact + captured.out + captured.err


def test_redirect_handler_refuses_followup():
    assert MODULE["NoRedirect"]().redirect_request(None, None, 302, "hidden", {}, "https://example.org") is None
