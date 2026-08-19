from __future__ import annotations

import hashlib
import inspect
from pathlib import Path

import pytest
from quant_platform_kit.data.research_input import validate_research_input_manifest

from scripts import acquire_tqqq_core_only_p1_inputs_alpaca as acquisition_cli
from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as binding


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
        "tool_version": "v1",
    }


def _bars_for(symbol: str) -> list[dict[str, object]]:
    eligible_start = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
    return [
        {
            "t": f"{session.isoformat()}T00:00:00Z",
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.0,
            "v": 1.0,
        }
        for session in binding._expected_xnys_sessions("2026-07-31")
        if eligible_start is None or session.isoformat() >= eligible_start
    ]


class _FakeAlpacaTransport:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.fail_on = fail_on

    def __call__(self, *, url: str, params: dict[str, str]) -> dict[str, object]:
        self.calls.append({"url": url, "params": dict(params)})
        symbol = params["symbols"]
        if symbol == self.fail_on:
            raise RuntimeError("synthetic transport failure")
        return {"bars": {symbol: _bars_for(symbol)}}


def test_binding_freezes_alpaca_sip_total_return_identity() -> None:
    value = binding.build_tqqq_core_only_p1_binding()

    assert value["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v1",
        "config_sha256": "969cae10850f5a2d72c17fedd77689301411f62dc24d9a530026e3f7efdc1c69",
    }
    assert value["source"] == {
        "repository": "QuantStrategyLab/UsEquityStrategies",
        "revision": "8b6b418bac74318f8054c5951521c9b62391de3e",
    }
    assert value["data_identity"]["provider"] == "ALPACA_MARKET_DATA"
    assert value["data_identity"]["feed"] == "SIP"
    assert value["data_identity"]["calendar"] == {
        "calendar_id": "XNYS",
        "timezone": "America/New_York",
        "source": "exchange_calendars",
    }
    assert value["data_identity"]["adjustment"] == {
        "policy": "total_return_adjusted",
        "source": "ALPACA_MARKET_DATA adjustment=all(split,dividend,spin-off)",
    }
    assert value["data_identity"]["universe"] == ["QQQ", "TQQQ", "QQQM", "BOXX"]
    assert value["data_identity"]["date_cutoff"] == "2026-07-31"


def test_manifest_uses_qpk_canonical_policy_and_preserves_alpaca_source_recipe() -> None:
    value = binding.build_tqqq_core_only_p1_binding()
    manifest = binding.build_tqqq_core_only_input_manifest(
        value,
        observed_at="2026-08-15T00:00:00Z",
        producer=_producer(),
        member_bytes=b'{"schema_version":"tqqq_core_only_private_bars.v1","symbols":{}}',
        source_content_sha256={symbol: hashlib.sha256(symbol.encode()).hexdigest() for symbol in value["data_identity"]["universe"]},
    )

    assert validate_research_input_manifest(manifest)["adjustment"] == {
        "policy": "total_return_adjusted",
        "source": "ALPACA_MARKET_DATA adjustment=all(split,dividend,spin-off)",
        "source_revision": binding.binding_sha256(value),
    }
    assert {source["source_id"] for source in manifest["sources"]} == {
        "alpaca_sip_1day_adjustment_all:BOXX",
        "alpaca_sip_1day_adjustment_all:QQQ",
        "alpaca_sip_1day_adjustment_all:QQQM",
        "alpaca_sip_1day_adjustment_all:TQQQ",
    }
    manifest["adjustment"]["policy"] = "all"
    with pytest.raises(ValueError):
        validate_research_input_manifest(manifest)


def test_static_binding_rejects_provider_adjustment_literal_as_canonical_policy() -> None:
    value = binding.build_tqqq_core_only_p1_binding()
    value["data_identity"]["adjustment"]["policy"] = "all"

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.validate_tqqq_core_only_p1_binding(value)


def test_injected_alpaca_transport_uses_exact_single_request_envelopes_and_publishes_root(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    transport = _FakeAlpacaTransport()
    provider = acquisition_cli.AlpacaSipHistoricalBarsProvider(transport)

    result = acquisition_cli.publish_tqqq_core_only_p1_inputs(provider, output_root=output, observed_at="2026-08-15T00:00:00Z", producer=_producer())

    assert [call["params"] for call in transport.calls] == [
        {"symbols": "QQQ", "timeframe": "1Day", "start": "2018-01-02", "end": "2026-07-31", "adjustment": "all", "feed": "sip", "sort": "asc", "limit": "10000"},
        {"symbols": "TQQQ", "timeframe": "1Day", "start": "2018-01-02", "end": "2026-07-31", "adjustment": "all", "feed": "sip", "sort": "asc", "limit": "10000"},
        {"symbols": "QQQM", "timeframe": "1Day", "start": "2020-10-13", "end": "2026-07-31", "adjustment": "all", "feed": "sip", "sort": "asc", "limit": "10000"},
        {"symbols": "BOXX", "timeframe": "1Day", "start": "2022-12-28", "end": "2026-07-31", "adjustment": "all", "feed": "sip", "sort": "asc", "limit": "10000"},
    ]
    assert all(call["url"] == "https://data.alpaca.markets/v2/stocks/bars" for call in transport.calls)
    assert result["status"] == "P1_DATA_ONLY_INPUTS_PUBLISHED"
    assert binding.verify_tqqq_core_only_input_root(output) == result["manifest_sha256"]

    completion = binding.build_tqqq_core_only_p1_remote_completion(output)
    completion_path = tmp_path / binding.REMOTE_COMPLETION_FILENAME
    completion_path.write_bytes(binding.canonical_tqqq_core_only_p1_remote_completion_bytes(completion))
    assert binding.verify_tqqq_core_only_p1_remote_completion(output, completion_path) == result["manifest_sha256"]


def test_remote_completion_marker_rejects_any_remote_member_drift(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    provider = acquisition_cli.AlpacaSipHistoricalBarsProvider(_FakeAlpacaTransport())
    acquisition_cli.publish_tqqq_core_only_p1_inputs(provider, output_root=output, observed_at="2026-08-15T00:00:00Z", producer=_producer())
    completion = binding.build_tqqq_core_only_p1_remote_completion(output)
    completion["members"]["bars.json"] = "0" * 64  # type: ignore[index]
    completion_path = tmp_path / binding.REMOTE_COMPLETION_FILENAME
    completion_path.write_bytes(binding.canonical_tqqq_core_only_p1_remote_completion_bytes(completion))

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="completion marker"):
        binding.verify_tqqq_core_only_p1_remote_completion(output, completion_path)


def test_transport_failure_stops_without_retry_or_root(tmp_path: Path) -> None:
    output = tmp_path / "immutable-input"
    transport = _FakeAlpacaTransport(fail_on="TQQQ")
    provider = acquisition_cli.AlpacaSipHistoricalBarsProvider(transport)

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError, match="data-only acquisition failed"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(provider, output_root=output, observed_at="2026-08-15T00:00:00Z", producer=_producer())

    assert [call["params"]["symbols"] for call in transport.calls] == ["QQQ", "TQQQ"]
    assert not output.exists()


def test_cli_without_injected_provider_parks_without_root(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    output = tmp_path / "immutable-input"

    assert acquisition_cli.main(["--output-root", str(output), "--observed-at", "2026-08-15T00:00:00Z"]) == 2

    assert capsys.readouterr().out == '{"status":"PARKED"}\n'
    assert not output.exists()


def test_alpaca_slice_has_no_ibkr_fallback_or_credential_handling() -> None:
    source = inspect.getsource(acquisition_cli).lower()
    binding_source = inspect.getsource(binding).lower()

    for forbidden in ("ibkr", "gateway", "credential", "authorization", "retry", "pagination", "place_order", "placeorder", "replay", "p3"):
        assert forbidden not in source
    assert "ibkr" not in binding_source


def test_binding_freezes_private_short_term_cloud_storage_and_shared_metadata_lifecycle() -> None:
    storage = binding.build_tqqq_core_only_p1_cloud_storage_binding()

    assert storage == {
        "provider": "GOOGLE_CLOUD_STORAGE",
        "access_scope": "PRIVATE",
        "public_access_prevention": "enforced",
        "raw_snapshot_lifecycle": {
            "policy": "SHORT_TERM_PRIVATE_CLOUD_RESEARCH_SNAPSHOT_NO_REDISTRIBUTION",
            "active_lifecycle_days": 7,
            "soft_delete_lifecycle_days": 7,
            "retention_extension_authorized": False,
            "retention_decision": "PENDING_LICENSE_AND_RETENTION_REVIEW",
        },
        "evidence_metadata_boundary": {
            "logical_separation_from_raw_snapshot": True,
            "shares_raw_snapshot_lifecycle": True,
            "separate_or_long_term_retention_authorized": False,
            "write_mode": "CREATE_ONLY",
            "raw_bars_included": False,
            "content": "DIGESTS_AND_NON_SENSITIVE_RESEARCH_PROVENANCE_ONLY",
        },
    }
    assert "bucket" not in storage
    assert "prefix" not in storage
    assert binding.build_tqqq_core_only_p1_binding()["cloud_storage"] == storage
    assert binding.build_tqqq_core_only_p1_binding()["data_identity"]["retention"] == {
        "policy": "PRIVATE_CLOUD_SHORT_TERM_RESEARCH_SNAPSHOT_NO_REDISTRIBUTION",
        "redistribution_allowed": False,
        "long_term_retention_authorized": False,
    }


def test_binding_rejects_raw_retention_extension_and_contains_no_object_store_url() -> None:
    value = binding.build_tqqq_core_only_p1_binding()
    assert b"gs://" not in binding.canonical_binding_bytes(value)

    value["cloud_storage"]["raw_snapshot_lifecycle"]["retention_extension_authorized"] = True

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.validate_tqqq_core_only_p1_binding(value)

    value = binding.build_tqqq_core_only_p1_binding()
    value["cloud_storage"]["evidence_metadata_boundary"]["shares_raw_snapshot_lifecycle"] = False

    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.validate_tqqq_core_only_p1_binding(value)
