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


def _bars_for(symbol: str, *, date_cutoff: str = "2026-07-31") -> list[dict[str, object]]:
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
        for session in binding._expected_xnys_sessions(date_cutoff)
        if eligible_start is None or session.isoformat() >= eligible_start
    ]


def _canonical_bars_for(symbol: str, *, date_cutoff: str) -> list[dict[str, object]]:
    return [
        {
            "date": str(bar["t"])[:10],
            "open": bar["o"],
            "high": bar["h"],
            "low": bar["l"],
            "close": bar["c"],
            "volume": bar["v"],
        }
        for bar in _bars_for(symbol, date_cutoff=date_cutoff)
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
        return {"bars": {symbol: _bars_for(symbol, date_cutoff=params["end"])}}


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


def test_v2_candidate_requires_its_own_source_config_and_input_profile() -> None:
    contract = binding.P2_V2_CONTRACT
    value = binding.build_tqqq_core_only_p1_binding_for_contract(contract)
    member_bytes = b'{"schema_version":"tqqq_core_only_private_bars.v1","symbols":{}}'
    manifest = binding.build_tqqq_core_only_input_manifest(
        value,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=member_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(symbol.encode()).hexdigest()
            for symbol in value["data_identity"]["universe"]
        },
        contract=contract,
    )

    assert value["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v2",
        "config_sha256": "f1d6e4cf8aa0f7ab818768fb6a6e9c86bcd03cc567e5a5a844024a446a43bd31",
    }
    assert value["source"]["revision"] == "5f0c30cdcaf3ee0f3f1c050acbe172580ea40c81"
    assert manifest["profile"] == "tqqq_core_only_p2_v2"
    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.validate_tqqq_core_only_p1_binding(value)


def test_superseded_v3_candidate_cannot_bind_new_input() -> None:
    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.resolve_tqqq_core_only_candidate_contract("tqqq_core_only_p2_v3")


def test_v4_candidate_extends_only_its_own_immutable_input_cutoff() -> None:
    contract = binding.P2_V4_CONTRACT
    value = binding.build_tqqq_core_only_p1_binding_for_contract(contract)

    assert value["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v4",
        "config_sha256": "b20335a16d0c5001dc28d3a1555dc1d46e6331fc714ca489a952d779de3279f1",
    }
    assert value["source"]["revision"] == binding.P2_V2_UES_REVISION
    assert value["data_identity"]["date_cutoff"] == "2026-08-04"
    assert binding.build_tqqq_core_only_p1_binding()["data_identity"]["date_cutoff"] == "2026-07-31"


def test_v5_candidate_binds_a_completed_daily_cutoff_without_changing_v1_to_v4() -> None:
    cutoff = "2026-08-18"
    value = binding.build_tqqq_core_only_p1_binding_for_contract(
        binding.P2_V5_CONTRACT, date_cutoff=cutoff
    )

    assert value["candidate"] == {
        "candidate_id": "tqqq_core_only_p2_v5",
        "config_sha256": "e6422cf7c3819734ec300a7bfa3d936d5273993c0ce865dfe0218d7b7f8426e2",
    }
    assert value["data_identity"]["date_cutoff"] == cutoff
    assert (
        binding.validate_tqqq_core_only_p1_binding_for_contract(
            value, binding.P2_V5_CONTRACT
        )
        == value
    )
    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.build_tqqq_core_only_p1_binding_for_contract(binding.P2_V5_CONTRACT)
    with pytest.raises(binding.TqqqCoreOnlyP1BindingError):
        binding.build_tqqq_core_only_p1_binding_for_contract(
            binding.P2_V5_CONTRACT, date_cutoff="2026-08-16"
        )


def test_v5_generic_publisher_keeps_daily_cutoff_in_the_verified_root(tmp_path: Path) -> None:
    cutoff = "2026-08-18"

    class _CanonicalProvider:
        def fetch_historical_bars(self, **kwargs: object) -> dict[str, object]:
            assert kwargs["date_cutoff"] == cutoff
            symbol = kwargs["symbol"]
            assert isinstance(symbol, str)
            return {"bars": _canonical_bars_for(symbol, date_cutoff=cutoff)}

    root = tmp_path / "v5-root"
    result = binding.publish_tqqq_core_only_p1_inputs_for_contract(
        _CanonicalProvider(),
        output_root=root,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        contract=binding.P2_V5_CONTRACT,
        date_cutoff=cutoff,
    )

    assert result["status"] == "P1_DATA_ONLY_INPUTS_PUBLISHED"
    assert binding.verify_tqqq_core_only_input_root(
        root, contract=binding.P2_V5_CONTRACT
    ) == result["manifest_sha256"]


def test_alpaca_adapter_allows_only_its_bound_v5_daily_cutoff(tmp_path: Path) -> None:
    cutoff = "2026-08-18"
    transport = _FakeAlpacaTransport()
    provider = acquisition_cli.AlpacaSipHistoricalBarsProvider(
        transport, date_cutoff=cutoff
    )

    result = acquisition_cli.publish_tqqq_core_only_p1_inputs_for_contract(
        provider,
        output_root=tmp_path / "v5-alpaca-root",
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        date_cutoff=cutoff,
    )

    assert result["status"] == "P1_DATA_ONLY_INPUTS_PUBLISHED"
    assert {call["params"]["end"] for call in transport.calls} == {cutoff}


def test_v4_input_root_uses_its_own_bound_coverage_cutoff(tmp_path: Path) -> None:
    contract = binding.P2_V4_CONTRACT
    value = binding.build_tqqq_core_only_p1_binding_for_contract(contract)
    cutoff = value["data_identity"]["date_cutoff"]
    assert isinstance(cutoff, str)
    bars = {
        "schema_version": "tqqq_core_only_private_bars.v1",
        "symbols": {
            symbol: {"bars": _canonical_bars_for(symbol, date_cutoff=cutoff)}
            for symbol in value["data_identity"]["universe"]
        },
    }
    bars_bytes = binding._canonical(bars)
    manifest = binding.build_tqqq_core_only_input_manifest(
        value,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=bars_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(binding._canonical(bars["symbols"][symbol])).hexdigest()
            for symbol in value["data_identity"]["universe"]
        },
        contract=contract,
    )
    root = tmp_path / "v4-input-root"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    (root / "binding.json").write_bytes(
        binding.canonical_tqqq_core_only_p1_binding_bytes_for_contract(value, contract)
    )
    (root / "manifest.json").write_bytes(binding.canonical_research_input_manifest_bytes(manifest))
    (root / "bars.json").write_bytes(bars_bytes)

    assert binding.verify_tqqq_core_only_input_root(
        root, contract=contract
    ) == binding.research_input_manifest_sha256(manifest)


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

    with pytest.raises(acquisition_cli.P1InputUnavailableError, match="data-only acquisition failed"):
        acquisition_cli.publish_tqqq_core_only_p1_inputs(provider, output_root=output, observed_at="2026-08-15T00:00:00Z", producer=_producer())

    assert [call["params"]["symbols"] for call in transport.calls] == ["QQQ", "TQQQ"]
    assert not output.exists()


def test_cli_classifies_temporary_provider_unavailability_as_inconclusive(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "immutable-input"
    provider = acquisition_cli.AlpacaSipHistoricalBarsProvider(_FakeAlpacaTransport(fail_on="TQQQ"))

    assert acquisition_cli.main(
        ["--output-root", str(output), "--observed-at", "2026-08-15T00:00:00Z"],
        provider=provider,
        producer=_producer(),
    ) == 2

    assert capsys.readouterr().out == (
        '{"candidate_id": "tqqq_core_only_p2_v1", "reason": "INPUT_UNAVAILABLE", '
        '"status": "PARKED", "verdict": "INCONCLUSIVE"}\n'
    )
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
