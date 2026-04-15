from __future__ import annotations

from us_equity_snapshot_pipelines.contracts import get_profile_contract, list_profile_contracts


def test_resolves_legacy_qqq_tech_alias_to_canonical_profile() -> None:
    contract = get_profile_contract("qqq_tech_enhancement")
    assert contract.profile == "tech_communication_pullback_enhancement"
    assert contract.manifest_required_by_runtime is True
    assert contract.snapshot_filename == "tech_communication_pullback_enhancement_feature_snapshot_latest.csv"


def test_lists_snapshot_profile_contracts() -> None:
    profiles = {contract.profile for contract in list_profile_contracts()}
    assert "tech_communication_pullback_enhancement" in profiles
    assert "russell_1000_multi_factor_defensive" in profiles
    assert "mega_cap_leader_rotation_dynamic_top20" in profiles
    assert "dynamic_mega_leveraged_pullback" in profiles
