from datetime import UTC, datetime, timedelta
from io import BytesIO
import json
from pathlib import Path
import sys
from urllib.error import HTTPError

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from alpaca_access_diagnostic import diagnose, safe_message
from alpaca_feed_windows import feed_windows

NOW = datetime(2030, 5, 1, 20, tzinfo=UTC)


def test_history_is_unchanged_and_recent_tail_is_separate_without_overlap():
    start = NOW - timedelta(days=2)
    boundary = NOW - timedelta(minutes=16)
    old = feed_windows(start, boundary, now=NOW, env={})
    assert len(old) == 1 and old[0]["feed"] == "sip"
    segments = feed_windows(start, NOW, now=NOW, env={})
    assert [s["feed"] for s in segments] == ["sip", "iex"]
    assert datetime.fromisoformat(segments[0]["end"]) == boundary
    assert datetime.fromisoformat(segments[1]["start"]) == boundary + timedelta(microseconds=1)
    assert segments[1]["segment"] == "recent_tail"


def test_recent_only_and_explicit_delay_only_and_feed_override():
    start = NOW - timedelta(minutes=3)
    assert [s["feed"] for s in feed_windows(start, NOW, now=NOW, env={})] == ["iex"]
    assert feed_windows(start, NOW, now=NOW, env={"ALPACA_FEED_RECENT": "none"}) == []
    result = feed_windows(NOW - timedelta(hours=1), NOW, now=NOW,
                          env={"ALPACA_SIP_MIN_DELAY_MINUTES": "30", "ALPACA_FEED_HISTORICAL": "iex"})
    assert result[0]["feed"] == "iex"
    assert datetime.fromisoformat(result[0]["end"]) == NOW - timedelta(minutes=30)


@pytest.mark.parametrize("env", [
    {"ALPACA_SIP_MIN_DELAY_MINUTES": "0"}, {"ALPACA_SIP_MIN_DELAY_MINUTES": "nan"},
    {"ALPACA_SIP_MIN_DELAY_MINUTES": "15"},
    {"ALPACA_FEED_RECENT": "sip"}, {"ALPACA_FEED_HISTORICAL": "unknown"},
])
def test_invalid_config_fails_before_requests(env):
    with pytest.raises(ValueError):
        feed_windows(NOW - timedelta(hours=1), NOW, now=NOW, env=env)


def test_naive_future_and_reversed_windows_fail():
    for start, end in [(NOW.replace(tzinfo=None), NOW), (NOW, NOW + timedelta(seconds=1)),
                       (NOW, NOW - timedelta(seconds=1))]:
        with pytest.raises(ValueError):
            feed_windows(start, end, now=NOW, env={})


class Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, *_args):
        raise AssertionError("diagnostic must never read success market data")


def test_diagnostic_consumes_planner_and_never_reads_bars():
    class Opener:
        calls = []

        def open(self, request, timeout):
            self.calls.append(request)
            return Response()

    opener = Opener()
    result = diagnose({"DATE_CUTOFF": "2030-05-01", "ALPACA_API_KEY_ID": "synthetic-key",
                       "ALPACA_API_SECRET_KEY": "synthetic-secret"}, now=NOW, opener=opener)
    assert result["status"] == "ALPACA_ACCESS_OK" and len(opener.calls) == 12
    assert {s["feed"] for s in result["segments"]} == {"sip", "iex"}
    assert result["price_basis"] == "split_dividend_adjusted"
    assert "synthetic-key" not in json.dumps(result)


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_denial_preserves_only_safe_message_and_status_without_retry(status):
    class Opener:
        calls = 0

        def open(self, request, timeout):
            self.calls += 1
            body = {"message": "subscription does not permit querying recent SIP data", "headers": "private"}
            raise HTTPError("https://synthetic.invalid/private?token=hidden", status, "ignored", {},
                            BytesIO(json.dumps(body).encode()))

    opener = Opener()
    result = diagnose({"DATE_CUTOFF": "2030-05-01", "ALPACA_API_KEY_ID": "synthetic-key",
                       "ALPACA_API_SECRET_KEY": "synthetic-secret"}, now=NOW, opener=opener)
    assert opener.calls == 1 and result["status"] == "ALPACA_DIAGNOSTIC_STOPPED"
    assert result["segments"][0]["http_status"] == status
    assert result["segments"][0]["provider_message"] == "subscription does not permit querying recent SIP data"
    assert all(x not in json.dumps(result) for x in ("private", "hidden", "headers", "synthetic-key"))


def test_error_body_sanitizer_removes_credentials_and_personal_values():
    text = "denied synthetic-secret U12345678 test@example.invalid https://synthetic.invalid/?token=x token=abc"
    exc = HTTPError("ignored", 403, "ignored", {}, BytesIO(json.dumps({"message": text}).encode()))
    message = safe_message(exc, ("synthetic-secret",))
    assert all(x not in message for x in ("synthetic-secret", "U12345678", "@", "http", "abc"))
