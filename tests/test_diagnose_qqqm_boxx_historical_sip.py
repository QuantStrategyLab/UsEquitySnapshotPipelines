import io
import json
from urllib.error import HTTPError

from scripts.diagnose_qqqm_boxx_historical_sip import PARAMS, _provider_error, diagnose


class _Response:
    status = 200

    def __init__(self, body=b'{"bars":[{"t":"2024-06-03T04:00:00Z"}]}'):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit):
        return self.body


class _Opener:
    def __init__(self, error=None, body=None):
        self.error = error
        self.body = body
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request.full_url, timeout))
        if self.error:
            raise self.error
        return _Response(self.body) if self.body is not None else _Response()


def test_fixed_request_has_historical_sip_raw_window():
    opener = _Opener()
    result = diagnose("QQQM", "id", "secret", opener=opener)
    assert result["status"] == "ACCESS_OK"
    assert len(opener.calls) == 1
    url = opener.calls[0][0]
    assert url.startswith("https://data.alpaca.markets/v2/stocks/QQQM/bars?")
    for key in ("feed", "adjustment", "start", "end", "limit"):
        assert key in PARAMS
    assert "feed=sip" in url and "adjustment=raw" in url and "limit=5" in url


def test_provider_error_extracts_only_code_and_redacted_message():
    body = json.dumps({"code": 42210000, "message": "subscription does not permit recent SIP; "
                       "key ABCDEFGHIJKLMNOPQRST", "detail": "private"}).encode()
    error = HTTPError("https://data.alpaca.markets", 403, "Forbidden", {}, io.BytesIO(body))
    opener = _Opener(error=error)
    result = diagnose("BOXX", "id", "secret", opener=opener)
    assert result["http_status"] == 403
    assert result["provider_code"] == 42210000
    assert "[redacted]" in result["provider_message"]
    assert "private" not in result["provider_message"]
    assert len(opener.calls) == 1


def test_non_json_error_does_not_escape_body():
    assert _provider_error(b"private raw error") == (None, None)


def test_untrusted_success_timestamp_is_not_logged():
    body = json.dumps({"bars": [{"t": {"secret": "do not print"}}]}).encode()
    result = diagnose("QQQM", "id", "secret", opener=_Opener(body=body))
    assert result == {"symbol": "QQQM", "http_status": 200, "status": "SUCCESS_FORMAT_INVALID"}
    for bars in ([{}], [{"t": "2024-06-08T04:00:00Z"}]):
        result = diagnose("QQQM", "id", "secret", opener=_Opener(body=json.dumps({"bars": bars}).encode()))
        assert result["status"] == "SUCCESS_FORMAT_INVALID"
