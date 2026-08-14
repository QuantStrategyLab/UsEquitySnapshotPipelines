from __future__ import annotations

import hashlib

import pytest
from quant_platform_kit.data.research_input import (
    InvalidResearchInputEvidence,
    canonical_research_input_manifest_bytes,
    read_research_input_manifest_json,
)

from us_equity_snapshot_pipelines.lifecycle.tqqq_p3_direct import (
    INPUT_LICENSE,
    INPUT_USAGE_SCOPE,
    TqqqP3ContractError,
    _require_authority,
)


def _manifest() -> dict[str, object]:
    raw = b'{"synthetic":true}'
    return {
        "schema_version": "research_input_manifest.v1",
        "manifest_id": "tqqq-retained-manifest-contract",
        "research_input_contract_id": "tqqq_etf_only_ibkr_adjusted_last.v1",
        "domain": "us_equity",
        "profile": "tqqq_core_parity_v1",
        "artifact_type": "immutable_adjusted_ohlcv_etf_only",
        "observed_at": "2025-07-02T20:00:00Z",
        "effective_at": "2025-07-02T20:00:00Z",
        "as_of": "2025-07-02T20:00:00Z",
        "producer": {"repository": "QuantStrategyLab/UsEquitySnapshotPipelines", "commit_sha": "a" * 40, "tree_sha": "b" * 40, "tool": "synthetic", "tool_version": "v1"},
        "calendar": {"calendar_id": "XNYS", "timezone": "America/New_York", "session_date": "2025-07-02", "source": "exchange_calendars", "source_revision": "synthetic"},
        "adjustment": {"policy": "total_return_adjusted", "source": "IBKR_ADJUSTED_LAST", "source_revision": "synthetic"},
        "sources": [{"source_id": "ibkr:QQQ", "revision": "synthetic", "observed_at": "2025-07-02T20:00:00Z", "content_sha256": hashlib.sha256(raw).hexdigest()}],
        "members": [{"path": "bars.json", "media_type": "application/json", "size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}],
    }


def test_manifest_rejects_tampered_member_identity() -> None:
    manifest = _manifest()
    manifest["members"] = [{**manifest["members"][0], "sha256": "0" * 64}]

    encoded = canonical_research_input_manifest_bytes(manifest)
    assert read_research_input_manifest_json(encoded)["members"][0]["sha256"] == "0" * 64


def test_manifest_rejects_noncanonical_member_path() -> None:
    manifest = _manifest()
    manifest["members"] = [{**manifest["members"][0], "path": "../bars.json"}]

    with pytest.raises(InvalidResearchInputEvidence):
        canonical_research_input_manifest_bytes(manifest)


def test_direct_authority_rejects_wrong_data_rights() -> None:
    authority = {
        "authority_receipt_sha256": "a" * 64,
        "entitlement_receipt_sha256": "b" * 64,
        "license_receipt_sha256": "c" * 64,
        "retention_expires_at": "2030-01-01T00:00:00Z",
        "risk_standard_id": "qpk.strategy_promotion_risk_standard.zh-CN.v2",
        "risk_standard_sha256": "d" * 64,
        "platform_execution_revision": "e" * 40,
        "input_license": INPUT_LICENSE,
        "input_usage_scope": INPUT_USAGE_SCOPE,
    }
    assert _require_authority(authority)["input_license"] == INPUT_LICENSE
    authority["input_usage_scope"] = "wrong"
    with pytest.raises(TqqqP3ContractError):
        _require_authority(authority)
