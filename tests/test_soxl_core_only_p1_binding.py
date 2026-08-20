from __future__ import annotations

import hashlib

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_p1_binding as binding
from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v3_contract import P2_V3_CONTRACT

_CUTOFF = "2026-08-18"


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "soxl_core_only_p1_contract_test",
        "tool_version": "v1",
    }


def _content_digests() -> dict[str, str]:
    return {
        symbol: hashlib.sha256(f"synthetic:{symbol}".encode()).hexdigest()
        for symbol in ("SOXL", "SOXX", "BOXX")
    }


def test_binding_freezes_the_clean_three_asset_candidate_and_daily_data_identity() -> None:
    value = binding.build_soxl_core_only_p1_binding(date_cutoff=_CUTOFF)

    assert value["schema_version"] == "qsl.soxl_soxx_core_only_p1_data_binding.v2"
    assert binding.INPUT_CONTRACT_ID == P2_V3_CONTRACT.input_contract_id
    assert value["candidate"] == {
        "candidate_id": P2_V3_CONTRACT.candidate_id,
        "config_sha256": P2_V3_CONTRACT.config_sha256,
    }
    assert value["source"] == {
        "repository": "QuantStrategyLab/UsEquityStrategies",
        "revision": P2_V3_CONTRACT.ues_revision,
    }
    assert value["data_identity"] == {
        "provider": "ALPACA_MARKET_DATA",
        "feed": "SIP",
        "calendar": {
            "calendar_id": "XNYS",
            "timezone": "America/New_York",
            "source": "exchange_calendars:4.13.2:XNYS",
        },
        "adjustment": {
            "policy": "total_return_adjusted",
            "source": "ALPACA_MARKET_DATA adjustment=all(split,dividend,spin-off)",
        },
        "universe": ["SOXL", "SOXX", "BOXX"],
        "date_cutoff": _CUTOFF,
    }
    assert binding.validate_soxl_core_only_p1_binding(value) == value


@pytest.mark.parametrize("cutoff", ("2026-08-16", "2026-08-18T00:00:00Z", "not-a-date"))
def test_binding_rejects_a_non_session_or_noncanonical_cutoff(cutoff: str) -> None:
    with pytest.raises(binding.SoxlCoreOnlyP1BindingError, match="date cutoff"):
        binding.build_soxl_core_only_p1_binding(date_cutoff=cutoff)


def test_manifest_binds_all_three_sources_without_reading_market_data() -> None:
    value = binding.build_soxl_core_only_p1_binding(date_cutoff=_CUTOFF)
    manifest = binding.build_soxl_core_only_input_manifest(
        value,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=b'{"schema_version":"soxl_core_only_private_bars.v1","symbols":{}}',
        source_content_sha256=_content_digests(),
    )

    assert manifest["research_input_contract_id"] == binding.INPUT_CONTRACT_ID
    assert manifest["profile"] == P2_V3_CONTRACT.candidate_id
    assert [source["source_id"] for source in manifest["sources"]] == [
        "alpaca_sip_1day_adjustment_all:BOXX",
        "alpaca_sip_1day_adjustment_all:SOXL",
        "alpaca_sip_1day_adjustment_all:SOXX",
    ]
    assert binding.validate_soxl_core_only_input_manifest(manifest, value) == (
        binding.research_input_manifest_sha256(manifest)
    )


def test_manifest_and_binding_fail_closed_on_candidate_or_source_identity_drift() -> None:
    value = binding.build_soxl_core_only_p1_binding(date_cutoff=_CUTOFF)
    drifted_binding = {**value, "candidate": {**value["candidate"], "candidate_id": "legacy_soxl"}}
    with pytest.raises(binding.SoxlCoreOnlyP1BindingError, match="P1 binding"):
        binding.validate_soxl_core_only_p1_binding(drifted_binding)

    with pytest.raises(binding.SoxlCoreOnlyP1BindingError, match="input member"):
        binding.build_soxl_core_only_input_manifest(
            value,
            observed_at="2026-08-19T00:00:00Z",
            producer=_producer(),
            member_bytes=b"{}",
            source_content_sha256={"SOXL": "a" * 64},
        )
