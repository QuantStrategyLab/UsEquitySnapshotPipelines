from __future__ import annotations

import hashlib
import importlib.util
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from urllib.error import HTTPError

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_p1_publisher as publisher
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p1_binding import (
    BARS_SCHEMA,
    SoxlCoreOnlyP1BindingError,
    expected_soxl_core_only_sessions,
)
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p3_input_materializer import (
    materialize_soxl_core_only_p3_input,
)

_CUTOFF = "2026-08-18"
_SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "acquire_soxl_core_only_p1_inputs_alpaca.py"
_SCRIPT_SPEC = importlib.util.spec_from_file_location("soxl_core_only_p1_acquisition_cli", _SCRIPT_PATH)
assert _SCRIPT_SPEC is not None and _SCRIPT_SPEC.loader is not None
acquisition_cli = importlib.util.module_from_spec(_SCRIPT_SPEC)
_SCRIPT_SPEC.loader.exec_module(acquisition_cli)


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "soxl_core_only_p1_publisher_test",
        "tool_version": "v1",
    }


def _series_for_sessions(sessions: tuple[date, ...], *, offset: float) -> list[dict[str, object]]:
    return [
        {
            "date": session.isoformat(),
            "open": offset + index,
            "high": offset + index + 2.0,
            "low": offset + index - 1.0,
            "close": offset + index + 1.0,
            "volume": 1000.0 + index,
        }
        for index, session in enumerate(sessions, start=1)
    ]


class _CompleteProvider:
    def __init__(self, *, missing_session_for: str | None = None) -> None:
        self._expected = expected_soxl_core_only_sessions(_CUTOFF)
        self._missing_session_for = missing_session_for
        self.requests: list[dict[str, str]] = []

    def fetch_historical_bars(
        self,
        *,
        symbol: str,
        calendar_id: str,
        timezone: str,
        adjustment_policy: str,
        feed: str,
        date_cutoff: str,
    ) -> Mapping[str, object]:
        self.requests.append(
            {
                "symbol": symbol,
                "calendar_id": calendar_id,
                "timezone": timezone,
                "adjustment_policy": adjustment_policy,
                "feed": feed,
                "date_cutoff": date_cutoff,
            }
        )
        sessions = self._expected[symbol]
        if symbol == self._missing_session_for:
            sessions = sessions[:-1]
        offsets = {"SOXL": 10.0, "SOXX": 100.0, "BOXX": 20.0}
        return {"bars": _series_for_sessions(sessions, offset=offsets[symbol])}


class _FakeAlpacaTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self._expected = expected_soxl_core_only_sessions(_CUTOFF)

    def __call__(self, *, url: str, params: Mapping[str, str]) -> Mapping[str, object]:
        self.calls.append({"url": url, "params": dict(params)})
        symbol = params["symbols"]
        offsets = {"SOXL": 10.0, "SOXX": 100.0, "BOXX": 20.0}
        return {
            "bars": {
                symbol: [
                    {
                        "t": f"{row['date']}T00:00:00Z",
                        "o": row["open"],
                        "h": row["high"],
                        "l": row["low"],
                        "c": row["close"],
                        "v": row["volume"],
                    }
                    for row in _series_for_sessions(self._expected[symbol], offset=offsets[symbol])
                ]
            }
        }


def test_publisher_writes_one_verified_three_asset_root_without_provider_fallback(tmp_path: Path) -> None:
    provider = _CompleteProvider()
    output_root = tmp_path / "immutable-p1-root"

    result = publisher.publish_soxl_core_only_p1_inputs(
        provider,
        output_root=output_root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )

    assert result["status"] == "P1_DATA_ONLY_INPUTS_PUBLISHED"
    assert publisher.verify_soxl_core_only_input_root(output_root) == result["manifest_sha256"]
    assert output_root.stat().st_mode & 0o777 == 0o700
    assert {path.name for path in output_root.iterdir()} == {"bars.json", "binding.json", "manifest.json"}
    assert provider.requests == [
        {
            "symbol": symbol,
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "adjustment_policy": "total_return_adjusted",
            "feed": "SIP",
            "date_cutoff": _CUTOFF,
        }
        for symbol in ("SOXL", "SOXX", "BOXX")
    ]
    bars = json.loads((output_root / "bars.json").read_bytes())
    assert bars["schema_version"] == BARS_SCHEMA
    assert set(bars["series"]) == {"SOXL", "SOXX", "BOXX"}
    manifest = json.loads((output_root / "manifest.json").read_bytes())
    assert {source["source_id"] for source in manifest["sources"]} == {
        "alpaca_sip_1day_adjustment_all:SOXL",
        "alpaca_sip_1day_adjustment_all:SOXX",
        "alpaca_sip_1day_adjustment_all:BOXX",
    }
    materialized = materialize_soxl_core_only_p3_input(
        binding=json.loads((output_root / "binding.json").read_bytes()),
        manifest=manifest,
        member_bytes=(output_root / "bars.json").read_bytes(),
    )
    assert materialized["p1_identity"]["input_manifest_sha256"] == result["manifest_sha256"]
    assert len(materialized["sessions"]) >= 2


def test_publisher_rejects_one_missing_xnys_session_and_leaves_no_root(tmp_path: Path) -> None:
    output_root = tmp_path / "incomplete-p1-root"

    with pytest.raises(publisher.SoxlCoreOnlyP1BindingError, match="historical coverage"):
        publisher.publish_soxl_core_only_p1_inputs(
            _CompleteProvider(missing_session_for="SOXX"),
            output_root=output_root,
            observed_at="2026-08-19T00:00:00Z",
            producer=_producer(),
            date_cutoff=_CUTOFF,
        )

    assert not output_root.exists()


def test_verifier_rejects_source_digest_drift(tmp_path: Path) -> None:
    output_root = tmp_path / "drifted-p1-root"
    publisher.publish_soxl_core_only_p1_inputs(
        _CompleteProvider(),
        output_root=output_root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )
    manifest_path = output_root / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["sources"][0]["content_sha256"] = hashlib.sha256(b"drift").hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8")

    with pytest.raises(publisher.SoxlCoreOnlyP1BindingError, match="input root"):
        publisher.verify_soxl_core_only_input_root(output_root)


def test_remote_completion_binds_every_verified_p1_member_and_rejects_drift(tmp_path: Path) -> None:
    output_root = tmp_path / "remote-completion-root"
    result = publisher.publish_soxl_core_only_p1_inputs(
        _CompleteProvider(),
        output_root=output_root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )
    completion_path = tmp_path / publisher.REMOTE_COMPLETION_FILENAME
    completion = publisher.build_soxl_core_only_p1_remote_completion(output_root)
    completion_path.write_bytes(publisher.canonical_soxl_core_only_p1_remote_completion_bytes(completion))

    assert publisher.verify_soxl_core_only_p1_remote_completion(output_root, completion_path) == result[
        "manifest_sha256"
    ]

    completion["members"]["bars.json"] = "0" * 64
    completion_path.write_bytes(publisher.canonical_soxl_core_only_p1_remote_completion_bytes(completion))
    with pytest.raises(publisher.SoxlCoreOnlyP1BindingError, match="completion marker"):
        publisher.verify_soxl_core_only_p1_remote_completion(output_root, completion_path)


def test_binding_rejects_a_weekday_that_is_not_an_xnys_session() -> None:
    with pytest.raises(SoxlCoreOnlyP1BindingError, match="date cutoff"):
        expected_soxl_core_only_sessions("2025-01-09")


def test_alpaca_adapter_uses_only_the_frozen_sip_daily_request_envelopes(tmp_path: Path) -> None:
    transport = _FakeAlpacaTransport()
    output_root = tmp_path / "alpaca-adapter-root"

    result = acquisition_cli.publish_soxl_core_only_p1_inputs(
        acquisition_cli.AlpacaSipHistoricalBarsProvider(transport, date_cutoff=_CUTOFF),
        output_root=output_root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=_CUTOFF,
    )

    assert result["status"] == "P1_DATA_ONLY_INPUTS_PUBLISHED"
    assert [call["params"] for call in transport.calls] == [
        {
            "symbols": "SOXL",
            "timeframe": "1Day",
            "start": "2022-01-03",
            "end": _CUTOFF,
            "adjustment": "all",
            "feed": "sip",
            "sort": "asc",
            "limit": "10000",
        },
        {
            "symbols": "SOXX",
            "timeframe": "1Day",
            "start": "2022-01-03",
            "end": _CUTOFF,
            "adjustment": "all",
            "feed": "sip",
            "sort": "asc",
            "limit": "10000",
        },
        {
            "symbols": "BOXX",
            "timeframe": "1Day",
            "start": "2022-12-28",
            "end": _CUTOFF,
            "adjustment": "all",
            "feed": "sip",
            "sort": "asc",
            "limit": "10000",
        },
    ]
    assert {call["url"] for call in transport.calls} == {"https://data.alpaca.markets/v2/stocks/bars"}


@pytest.mark.parametrize(
    ("status", "reason_code"),
    [
        (401, "ALPACA_AUTHENTICATION_FAILED"),
        (403, "ALPACA_SIP_ACCESS_FORBIDDEN"),
    ],
)
def test_alpaca_http_diagnostic_never_exposes_response_details(
    monkeypatch: pytest.MonkeyPatch, status: int, reason_code: str
) -> None:
    def _raise_http_error(*_args: object, **_kwargs: object) -> object:
        raise HTTPError(
            "https://data.alpaca.markets/v2/stocks/bars", status, "hidden", None, None
        )

    monkeypatch.setattr(acquisition_cli, "urlopen", _raise_http_error)

    with pytest.raises(acquisition_cli.P1InputUnavailableError) as error:
        acquisition_cli.AlpacaSipHttpTransport("test-key", "test-secret")(
            url="https://data.alpaca.markets/v2/stocks/bars",
            params={"symbols": "SOXL"},
        )

    assert str(error.value) == reason_code
    assert error.value.reason_code == reason_code


def test_cli_without_an_injected_provider_parks_without_writing_a_root(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output_root = tmp_path / "parked-root"

    assert acquisition_cli.main(
        [
            "--output-root",
            str(output_root),
            "--observed-at",
            "2026-08-19T00:00:00Z",
            "--date-cutoff",
            _CUTOFF,
        ]
    ) == 2

    assert capsys.readouterr().out == '{"status": "PARKED"}\n'
    assert not output_root.exists()
