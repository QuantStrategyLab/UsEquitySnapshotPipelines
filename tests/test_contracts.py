from __future__ import annotations

from us_equity_snapshot_pipelines.contracts import get_profile_contract, list_profile_contracts, list_scheduled_profile_contracts


def test_lists_snapshot_profile_contracts() -> None:
    profiles = {contract.profile for contract in list_profile_contracts()}
    assert "russell_top50_leader_rotation" in profiles
    assert "global_etf_rotation" in profiles
    assert "russell_1000_multi_factor_defensive" not in profiles
    assert "mega_cap_leader_rotation_dynamic_top20" not in profiles
    assert "mega_cap_leader_rotation_aggressive" not in profiles
    assert "dynamic_mega_leveraged_pullback" not in profiles


def test_lists_scheduled_snapshot_profile_contracts_without_research_only_tech() -> None:
    profiles = {contract.profile for contract in list_scheduled_profile_contracts()}
    assert profiles == {"global_etf_rotation", "russell_top50_leader_rotation"}


def test_global_etf_contract_requires_manifest_for_runtime() -> None:
    contract = get_profile_contract("global_etf_rotation")
    assert contract.contract_version == "global_etf_rotation.feature_snapshot.v1"
    assert contract.manifest_required_by_runtime is True
    assert contract.snapshot_filename == "global_etf_rotation_feature_snapshot_latest.csv"
