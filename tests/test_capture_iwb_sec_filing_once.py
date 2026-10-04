"""Synthetic transport only: no real SEC body or request is used by these tests."""

from __future__ import annotations

import hashlib
import importlib
import io
import json
import runpy
import socket
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError, URLError

import pytest

from scripts import capture_iwb_sec_filing_once as capture
from us_equity_snapshot_pipelines.russell_1000_history import (
    IwbSecFilingAdapterError,
    select_iwb_sec_filing_input_version_at_cutoff,
)


def index_bytes(*, accepted="2026-05-22 15:05:15", form="NPORT-P"):
    return (
        "<html><body>CIK: 0001100663<br>"
        "Accession Number: 0001004726-26-003726<br>"
        f"Form: {form}<br>Period of Report: 2026-03-31<br>"
        f"Accepted: {accepted}</body></html>"
    ).encode()


def xml_bytes(*, ticker="AAA", form="NPORT-P"):
    holding_ticker = f"<ticker>{ticker}</ticker>" if ticker else ""
    return (
        '<edgarSubmission xmlns="http://www.sec.gov/edgar/nport">'
        f"<headerData><submissionType>{form}</submissionType>"
        "<cik>0001100663</cik><accessionNumber>0001004726-26-003726</accessionNumber>"
        "<seriesClassInfo><seriesId>S000004347</seriesId><classId>C000012077</classId>"
        "<ticker>IWB</ticker></seriesClassInfo></headerData>"
        "<formData><genInfo><repPdDate>2026-03-31</repPdDate></genInfo>"
        f"<invstOrSecs><invstOrSec><name>Synthetic holding</name><identifiers>{holding_ticker}"
        "</identifiers><assetCat>EC</assetCat></invstOrSec></invstOrSecs></formData></edgarSubmission>"
    ).encode()


class FakeClock:
    def __init__(self):
        self.wall = datetime(2026, 10, 4, 12, 0, 0, 123456, tzinfo=UTC)
        self.elapsed = 0.0
        self.sleeps = []

    def now(self):
        return self.wall

    def monotonic(self):
        return self.elapsed

    def advance(self, seconds):
        self.elapsed += seconds
        self.wall += timedelta(seconds=seconds)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


class FakeResponse:
    def __init__(self, body, clock, *, url=None, status=200, headers=None, duration=0.25, chunk=97):
        self.body = io.BytesIO(body)
        self.clock = clock
        self.url = url
        self.status = status
        self.headers = Message()
        values = {"Content-Length": str(len(body))} if headers is None else headers
        for key, value in values.items():
            self.headers[key] = value
        self.duration = duration
        self.chunk = chunk
        self.completed_at = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def geturl(self):
        return self.url

    def read(self, size):
        data = self.body.read(min(size, self.chunk))
        if not data:
            self.clock.advance(self.duration)
            self.completed_at = self.clock.now()
        return data


class FakeOpener:
    def __init__(self, responses, clock):
        self.responses = list(responses)
        self.clock = clock
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request, timeout, self.clock.monotonic()))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        if response.url is None:
            response.url = request.full_url
        return response


class FakeStore:
    def __init__(self, clock, *, corrupt=None, fail=None):
        self.clock = clock
        self.corrupt = corrupt
        self.fail = fail
        self.items = {}
        self.events = []
        self.published = False

    def reserve(self, version_id):
        self.events.append(("reserve", version_id))

    def create_and_verify(self, name, body):
        self.events.append(("raw", name))
        self.clock.advance(3600)
        if name == self.fail:
            raise capture.CaptureError("LOCAL_WRITE_OR_READBACK_FAILED")
        self.items[name] = body
        readback = body + b"corrupt" if name == self.corrupt else body
        if hashlib.sha256(body).digest() != hashlib.sha256(readback).digest():
            raise capture.CaptureError("LOCAL_READBACK_MISMATCH")
        return {"name": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}

    def publish_manifest(self, body):
        self.events.append(("manifest", "manifest.json"))
        if self.fail == "manifest.json":
            raise capture.CaptureError("LOCAL_WRITE_OR_READBACK_FAILED")
        self.items["manifest.json"] = body
        self.published = True


@pytest.fixture(autouse=True)
def forbid_real_network(monkeypatch):
    def denied(*_args, **_kwargs):
        raise AssertionError("Real networking is forbidden in the synthetic capture tests")

    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(capture, "build_opener", denied)


def config(**changes):
    result = capture.CaptureConfig(
        run=True,
        index_url=capture.INDEX_URL,
        xml_url=capture.XML_URL,
        accepted_timezone="America/New_York",
        version_id="synthetic-first",
        user_agent="Synthetic Test operator@example.invalid",
        user_agent_reference="env:TEST_SEC_USER_AGENT",
    )
    return replace(result, **changes)


def setup_capture(*, index=None, xml=None, first_headers=None, second_headers=None, **store_args):
    clock = FakeClock()
    responses = [
        FakeResponse(index if index is not None else index_bytes(), clock, headers=first_headers),
        FakeResponse(xml if xml is not None else xml_bytes(), clock, headers=second_headers),
    ]
    opener = FakeOpener(responses, clock)
    store = FakeStore(clock, **store_args)
    return clock, responses, opener, store


def test_complete_receipt_uses_response_completion_before_storage_and_preserves_bytes():
    clock, responses, opener, store = setup_capture()
    result = capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 2
    assert [call[0].full_url for call in opener.calls] == [capture.INDEX_URL, capture.XML_URL]
    assert all(call[1] == 30 for call in opener.calls)
    assert opener.calls[1][2] - (opener.calls[0][2] + 0.25) >= 1.0
    assert clock.sleeps == [1.0]
    assert result.version.observed_at == max(response.completed_at for response in responses)
    assert result.version.observed_at < clock.now() - timedelta(hours=1)
    manifest = json.loads(store.items["manifest.json"])
    assert manifest == result.manifest
    assert manifest["observed_at"] == result.version.observed_at.isoformat()
    assert manifest["raw_xsd_validation"] == "not_validated"
    assert manifest["production_eligible"] is False
    assert manifest["trading_eligible"] is False
    assert manifest["historical_pit_proven"] is False
    assert manifest["first_public_visibility_proven"] is False
    assert store.items["filing-index.html"] == index_bytes()
    assert store.items["primary_doc.xml"] == xml_bytes()
    for entry in manifest["raw_objects"]:
        assert entry["sha256"] == hashlib.sha256(store.items[entry["name"]]).hexdigest()
    assert "operator@example.invalid" not in json.dumps(manifest)
    assert store.events[-1][0] == "manifest"


@pytest.mark.parametrize(
    "changes",
    [
        {"run": False},
        {"index_url": capture.INDEX_URL + "?x=1"},
        {"xml_url": capture.XML_URL.replace("www.sec.gov", "sec.gov")},
        {"index_url": capture.INDEX_URL.replace("https:", "http:")},
        {"xml_url": capture.XML_URL.replace("primary_doc.xml", "other.xml")},
        {"accepted_timezone": ""},
        {"accepted_timezone": "Invalid/Timezone"},
        {"user_agent": ""},
        {"user_agent": "test\r\nAuthorization: secret"},
        {"user_agent_reference": ""},
        {"version_id": "../escape"},
        {"version_id": "."},
    ],
)
def test_invalid_scope_or_configuration_has_zero_requests(changes):
    clock, _, opener, store = setup_capture()
    with pytest.raises(capture.CaptureError):
        capture.run_capture(config(**changes), opener=opener, clock=clock, storage=store)
    assert opener.calls == []
    assert store.events == []


@pytest.mark.parametrize("status", [301, 302, 307, 308, 403, 429, 500])
def test_http_status_fails_without_retry_or_xml(status):
    clock, _, opener, store = setup_capture()
    opener.responses[0] = HTTPError(capture.INDEX_URL, status, "secret detail", {}, io.BytesIO(b"secret"))
    with pytest.raises(capture.CaptureError) as error:
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert str(error.value) == "SEC_HTTP_REJECTED"
    assert error.value.status == status
    assert len(opener.calls) == 1
    assert not store.published


@pytest.mark.parametrize("error", [URLError("secret"), TimeoutError("secret"), OSError("secret")])
def test_transport_errors_are_sanitized_without_retry(error):
    clock, _, opener, store = setup_capture()
    opener.responses[0] = error
    with pytest.raises(capture.CaptureError, match="SEC_TRANSPORT_FAILED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1
    assert not store.published


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Encoding": "gzip"},
        {"Content-Encoding": "br"},
        {"Content-Length": "not-an-int"},
        {"Content-Length": "-1"},
        {"Content-Length": "0"},
        {"Content-Length": "1"},
        {"Content-Length": str(capture.MAX_INDEX_BYTES + 1)},
    ],
)
def test_unsupported_encoding_length_and_truncation_fail_closed(headers):
    clock, _, opener, store = setup_capture(first_headers=headers)
    with pytest.raises(capture.CaptureError):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1
    assert not store.published


@pytest.mark.parametrize("which,limit", [(0, capture.MAX_INDEX_BYTES), (1, capture.MAX_XML_BYTES)])
def test_absent_length_reads_to_eof_but_never_accepts_over_budget(which, limit):
    clock, _, opener, store = setup_capture()
    opener.responses[which] = FakeResponse(b"x" * (limit + 1), clock, headers={}, chunk=limit + 1)
    with pytest.raises(capture.CaptureError, match="SEC_RESPONSE_TOO_LARGE"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == which + 1
    assert not store.published


def test_duplicate_length_or_silent_redirect_rejected():
    for redirected in [False, True]:
        clock, responses, opener, store = setup_capture()
        if redirected:
            responses[0].url = capture.XML_URL
        else:
            responses[0].headers["Content-Length"] = str(len(index_bytes()))
        with pytest.raises(capture.CaptureError):
            capture.run_capture(config(), opener=opener, clock=clock, storage=store)
        assert len(opener.calls) == 1
        assert not store.published


@pytest.mark.parametrize(
    "body",
    [
        b"<html><body>Request Rate Threshold Exceeded</body></html>",
        b"<html><body>Your request originates from an undeclared automated tool</body></html>",
        b"<html><body>Verify you are human: CAPTCHA</body></html>",
    ],
)
def test_http_200_challenge_fails_without_retry(body):
    clock, _, opener, store = setup_capture(index=body)
    with pytest.raises(capture.CaptureError, match="SEC_CHALLENGE_REJECTED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1
    assert not store.published


@pytest.mark.parametrize(
    "which,old,new",
    [
        (0, b"0001004726-26-003726", b"0001004726-26-003727"),
        (0, b"2026-03-31", b"2026-02-28"),
        (0, b"0001100663", b"0001100664"),
        (1, b"0001004726-26-003726", b"0001004726-26-003727"),
        (1, b"2026-03-31", b"2026-02-28"),
        (1, b"S000004347", b"S000004348"),
        (1, b"C000012077", b"C000012078"),
        (1, b"<ticker>IWB", b"<ticker>IWF"),
        (1, b"http://www.sec.gov/edgar/nport", b"urn:foreign"),
        (1, b"</edgarSubmission>", b""),
        (0, b"</html>", b""),
    ],
)
def test_identity_period_namespace_or_document_shape_conflicts_never_publish(which, old, new):
    bodies = [index_bytes(), xml_bytes()]
    bodies[which] = bodies[which].replace(old, new)
    clock, _, opener, store = setup_capture(index=bodies[0], xml=bodies[1])
    with pytest.raises(capture.CaptureError):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert not store.published
    assert not store.items
    assert len(opener.calls) == which + 1


def test_missing_holding_ticker_is_unresolved_without_invented_mapping():
    clock, _, opener, store = setup_capture(xml=xml_bytes(ticker=None))
    result = capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    holding = result.version.holdings[0]
    assert holding.ticker is None
    assert holding.status == "unresolved"
    assert "missing_ticker" in holding.reasons
    assert result.manifest["holdings_status_counts"] == {"unresolved": 1}


def test_holding_identity_conflicts_retain_existing_unresolved_policy_and_original_rows():
    body = xml_bytes().replace(b"<ticker>AAA</ticker>", b"<ticker>AAA</ticker><ticker>BBB</ticker>")
    clock, _, opener, store = setup_capture(xml=body)
    result = capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(result.version.holdings) == 1
    assert result.version.holdings[0].status == "unresolved"
    assert "conflicting_ticker_identity" in result.version.holdings[0].reasons
    assert store.items["primary_doc.xml"] == body
    assert result.manifest["trading_eligible"] is False


@pytest.mark.parametrize("accepted", ["2026-03-08 02:30:00", "2026-11-01 01:30:00"])
def test_existing_accepted_dst_rules_are_fail_closed_before_xml(accepted):
    clock, _, opener, store = setup_capture(index=index_bytes(accepted=accepted))
    with pytest.raises(capture.CaptureError, match="SEC_INDEX_INPUT_REJECTED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1
    assert not store.published


def test_observed_max_is_conservative_under_wall_clock_adjustment():
    clock, responses, opener, store = setup_capture()
    original_read = responses[1].read
    adjusted = False

    def adjusted_read(size):
        nonlocal adjusted
        result = original_read(size)
        if not result and not adjusted:
            clock.wall -= timedelta(minutes=1)
            adjusted = True
            responses[1].completed_at = clock.now()
        return result

    responses[1].read = adjusted_read
    result = capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert responses[0].completed_at > responses[1].completed_at
    assert result.version.observed_at == responses[0].completed_at
    assert result.manifest["responses"][1]["completed_at"] == responses[1].completed_at.isoformat()


@pytest.mark.parametrize("which,limit", [(0, capture.MAX_INDEX_BYTES), (1, capture.MAX_XML_BYTES)])
def test_exact_byte_limit_is_accepted_only_after_eof(which, limit):
    bodies = [index_bytes(), xml_bytes()]
    bodies[which] += b" " * (limit - len(bodies[which]))
    clock, _, opener, store = setup_capture(index=bodies[0], xml=bodies[1])
    result = capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert result.manifest["responses"][which]["bytes"] == limit


def test_second_http_rejection_stops_at_two_without_raw_or_manifest():
    clock, _, opener, store = setup_capture()
    opener.responses[1] = HTTPError(capture.XML_URL, 429, "secret", {}, io.BytesIO(b"secret"))
    with pytest.raises(capture.CaptureError, match="SEC_HTTP_REJECTED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 2
    assert not store.items


def test_advertised_truncated_length_and_http_incomplete_read_rejected():
    from http.client import IncompleteRead

    clock, responses, opener, store = setup_capture()
    responses[1].headers.replace_header("Content-Length", str(len(xml_bytes()) + 1))
    with pytest.raises(capture.CaptureError, match="SEC_CONTENT_LENGTH_MISMATCH"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert not store.published
    clock, responses, opener, store = setup_capture()

    def truncated(_size):
        raise IncompleteRead(b"partial", 10)

    responses[0].read = truncated
    with pytest.raises(capture.CaptureError, match="SEC_TRANSPORT_FAILED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1


@pytest.mark.parametrize("fail", ["filing-index.html", "primary_doc.xml", "manifest.json"])
def test_storage_failure_never_produces_success_manifest(fail):
    clock, _, opener, store = setup_capture(fail=fail)
    with pytest.raises(capture.CaptureError):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert not store.published
    assert "manifest.json" not in store.items


def test_raw_readback_mismatch_prevents_manifest():
    clock, _, opener, store = setup_capture(corrupt="primary_doc.xml")
    with pytest.raises(capture.CaptureError, match="LOCAL_READBACK_MISMATCH"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert not store.published


def test_local_create_only_and_atomic_manifest(tmp_path):
    clock, _, opener, _ = setup_capture()
    store = capture.LocalStore(tmp_path)
    result = capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    directory = tmp_path / config().version_id
    assert json.loads((directory / "manifest.json").read_bytes()) == result.manifest
    assert (directory / "filing-index.html").read_bytes() == index_bytes()
    assert (directory / "primary_doc.xml").read_bytes() == xml_bytes()
    assert not (directory / ".manifest.pending").exists()
    other_clock, _, other_opener, _ = setup_capture()
    with pytest.raises(capture.CaptureError, match="LOCAL_VERSION_ALREADY_EXISTS"):
        capture.run_capture(config(), opener=other_opener, clock=other_clock, storage=capture.LocalStore(tmp_path))
    assert other_opener.calls == []


def test_local_existing_raw_or_manifest_never_overwritten(tmp_path):
    store = capture.LocalStore(tmp_path)
    store.reserve("test-local")
    store.create_and_verify("filing-index.html", b"first")
    with pytest.raises(capture.CaptureError, match="LOCAL_WRITE_OR_READBACK_FAILED"):
        store.create_and_verify("filing-index.html", b"second")
    assert (tmp_path / "test-local" / "filing-index.html").read_bytes() == b"first"
    (tmp_path / "test-local" / "manifest.json").write_bytes(b"existing manifest")
    with pytest.raises(capture.CaptureError, match="LOCAL_MANIFEST_PUBLICATION_FAILED"):
        store.publish_manifest(b"new manifest")
    assert (tmp_path / "test-local" / "manifest.json").read_bytes() == b"existing manifest"


def test_local_raw_and_manifest_readback_mismatch_never_publish(tmp_path, monkeypatch):
    for corrupt_name in ["primary_doc.xml", ".manifest.pending"]:
        store = capture.LocalStore(tmp_path / corrupt_name)
        original = Path.read_bytes

        def corrupt(path):
            body = original(path)
            return body + b"corrupt" if path.name == corrupt_name else body

        with monkeypatch.context() as patch:
            patch.setattr(Path, "read_bytes", corrupt)
            clock, _, opener, _ = setup_capture()
            with pytest.raises(capture.CaptureError, match="LOCAL_READBACK_MISMATCH"):
                capture.run_capture(config(), opener=opener, clock=clock, storage=store)
        assert not (tmp_path / corrupt_name / config().version_id / "manifest.json").exists()


def test_observed_after_accepted_cutoff_and_same_accession_revision_semantics():
    clock, _, opener, store = setup_capture()
    first = capture.run_capture(config(), opener=opener, clock=clock, storage=store).version
    with pytest.raises(IwbSecFilingAdapterError, match="no filing input version known"):
        select_iwb_sec_filing_input_version_at_cutoff(
            [first], decision_at=first.observed_at - timedelta(microseconds=1)
        )
    assert select_iwb_sec_filing_input_version_at_cutoff([first], decision_at=first.observed_at) is first
    later_clock, _, later_opener, later_store = setup_capture(xml=xml_bytes(ticker="BBB"))
    later_clock.advance(10)
    second = capture.run_capture(
        config(version_id="synthetic-revision"), opener=later_opener, clock=later_clock, storage=later_store
    ).version
    assert second.accession_number == first.accession_number
    assert second.raw_binding_sha256 != first.raw_binding_sha256
    assert select_iwb_sec_filing_input_version_at_cutoff([first, second], decision_at=first.observed_at) is first
    assert select_iwb_sec_filing_input_version_at_cutoff([first, second], decision_at=second.observed_at) is second


def test_observation_before_accepted_and_invalid_clock_fail_closed():
    for wall in [datetime(2026, 5, 22, 19, 0, tzinfo=UTC), datetime(2026, 10, 4, 12, 0)]:
        clock, _, opener, store = setup_capture()
        clock.wall = wall
        with pytest.raises(capture.CaptureError):
            capture.run_capture(config(), opener=opener, clock=clock, storage=store)
        assert not store.published


def test_timeout_budget_and_unverified_sleep_do_not_start_extra_request():
    clock, responses, opener, store = setup_capture()
    responses[0].duration = 31
    with pytest.raises(capture.CaptureError, match="SEC_REQUEST_DEADLINE_EXCEEDED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1
    clock, _, opener, store = setup_capture()
    clock.sleep = lambda seconds: None
    with pytest.raises(capture.CaptureError, match="SEC_REQUEST_INTERVAL_UNVERIFIED"):
        capture.run_capture(config(), opener=opener, clock=clock, storage=store)
    assert len(opener.calls) == 1


def test_no_length_and_explicit_identity_encoding_can_complete():
    clock, _, opener, store = setup_capture(first_headers={}, second_headers={"Content-Encoding": "identity"})
    assert capture.run_capture(config(), opener=opener, clock=clock, storage=store).manifest["request_count"] == 2
    assert opener.calls[0][0].get_header("User-agent") == config().user_agent


def test_import_help_missing_arguments_and_missing_explicit_identity_have_zero_network(monkeypatch, capsys):
    importlib.reload(capture)
    # reload resets the patched global, so deny real opener creation again.
    monkeypatch.setattr(capture, "build_opener", lambda *_args: pytest.fail("unexpected real opener"))
    with pytest.raises(SystemExit) as helped:
        capture.main(["--help"])
    assert helped.value.code == 0
    with pytest.raises(SystemExit) as missing:
        capture.main([])
    assert missing.value.code == 2
    args = [
        "--run",
        "--index-url",
        capture.INDEX_URL,
        "--xml-url",
        capture.XML_URL,
        "--accepted-timezone",
        "America/New_York",
        "--version-id",
        "test-cli",
        "--output-dir",
        "/unused",
        "--user-agent-env",
        "TEST_SEC_CAPTURE_MISSING",
    ]
    monkeypatch.delenv("TEST_SEC_CAPTURE_MISSING", raising=False)
    assert capture.main(args) == 1
    assert "USER_AGENT_REFERENCE_UNCONFIGURED" in capsys.readouterr().out
    monkeypatch.setattr("sys.argv", ["capture_iwb_sec_filing_once.py", "--help"])
    with pytest.raises(SystemExit) as script_help:
        runpy.run_path(str(Path(capture.__file__)), run_name="__main__")
    assert script_help.value.code == 0
