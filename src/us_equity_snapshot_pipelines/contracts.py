from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


SOURCE_PROJECT = "UsEquitySnapshotPipelines"
TECH_COMMUNICATION_PULLBACK_PROFILE = "tech_communication_pullback_enhancement"
RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE = "russell_1000_multi_factor_defensive"
MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE = "mega_cap_leader_rotation_dynamic_top20"
MEGA_CAP_LEADER_ROTATION_AGGRESSIVE_PROFILE = "mega_cap_leader_rotation_aggressive"
DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE = "dynamic_mega_leveraged_pullback"


@dataclass(frozen=True)
class SnapshotProfileContract:
    profile: str
    display_name: str
    contract_version: str
    snapshot_filename: str
    manifest_filename: str
    ranking_filename: str
    release_summary_filename: str = "release_status_summary.json"
    legacy_aliases: tuple[str, ...] = ()
    current_gcs_prefix_hint: str | None = None
    neutral_gcs_prefix_hint: str | None = None
    manifest_required_by_runtime: bool = False

    def artifact_paths(self, output_dir: str | Path) -> dict[str, Path]:
        root = Path(output_dir)
        return {
            "snapshot": root / self.snapshot_filename,
            "manifest": root / self.manifest_filename,
            "ranking": root / self.ranking_filename,
            "release_summary": root / self.release_summary_filename,
        }


_PROFILE_CONTRACTS = {
    TECH_COMMUNICATION_PULLBACK_PROFILE: SnapshotProfileContract(
        profile=TECH_COMMUNICATION_PULLBACK_PROFILE,
        display_name="Tech/Communication Pullback Enhancement",
        contract_version="tech_communication_pullback_enhancement.feature_snapshot.v1",
        snapshot_filename="tech_communication_pullback_enhancement_feature_snapshot_latest.csv",
        manifest_filename="tech_communication_pullback_enhancement_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="tech_communication_pullback_enhancement_ranking_latest.csv",
        legacy_aliases=("qqq_tech_enhancement",),
        current_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/interactive_brokers/"
            "tech_communication_pullback_enhancement"
        ),
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
            "tech_communication_pullback_enhancement"
        ),
        manifest_required_by_runtime=True,
    ),
    RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE: SnapshotProfileContract(
        profile=RUSSELL_1000_MULTI_FACTOR_DEFENSIVE_PROFILE,
        display_name="Russell 1000 Multi-Factor Defensive",
        contract_version="russell_1000_multi_factor_defensive.feature_snapshot.v1",
        snapshot_filename="russell_1000_multi_factor_defensive_feature_snapshot_latest.csv",
        manifest_filename="russell_1000_multi_factor_defensive_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="russell_1000_multi_factor_defensive_ranking_latest.csv",
        current_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/interactive_brokers/"
            "russell_1000_multi_factor_defensive"
        ),
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
            "russell_1000_multi_factor_defensive"
        ),
        manifest_required_by_runtime=False,
    ),
    MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE: SnapshotProfileContract(
        profile=MEGA_CAP_LEADER_ROTATION_DYNAMIC_TOP20_PROFILE,
        display_name="Mega Cap Leader Rotation Dynamic Top20",
        contract_version="mega_cap_leader_rotation_dynamic_top20.feature_snapshot.v1",
        snapshot_filename="mega_cap_leader_rotation_dynamic_top20_feature_snapshot_latest.csv",
        manifest_filename="mega_cap_leader_rotation_dynamic_top20_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="mega_cap_leader_rotation_dynamic_top20_ranking_latest.csv",
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
            "mega_cap_leader_rotation_dynamic_top20"
        ),
        manifest_required_by_runtime=True,
    ),
    MEGA_CAP_LEADER_ROTATION_AGGRESSIVE_PROFILE: SnapshotProfileContract(
        profile=MEGA_CAP_LEADER_ROTATION_AGGRESSIVE_PROFILE,
        display_name="Mega Cap Leader Rotation Aggressive",
        contract_version="mega_cap_leader_rotation_aggressive.feature_snapshot.v1",
        snapshot_filename="mega_cap_leader_rotation_aggressive_feature_snapshot_latest.csv",
        manifest_filename="mega_cap_leader_rotation_aggressive_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="mega_cap_leader_rotation_aggressive_ranking_latest.csv",
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
            "mega_cap_leader_rotation_aggressive"
        ),
        manifest_required_by_runtime=True,
    ),
    DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE: SnapshotProfileContract(
        profile=DYNAMIC_MEGA_LEVERAGED_PULLBACK_PROFILE,
        display_name="Dynamic Mega Leveraged Pullback",
        contract_version="dynamic_mega_leveraged_pullback.feature_snapshot.v1",
        snapshot_filename="dynamic_mega_leveraged_pullback_feature_snapshot_latest.csv",
        manifest_filename="dynamic_mega_leveraged_pullback_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="dynamic_mega_leveraged_pullback_ranking_latest.csv",
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
            "dynamic_mega_leveraged_pullback"
        ),
        manifest_required_by_runtime=True,
    ),
}

_ALIAS_TO_PROFILE = {
    alias: contract.profile
    for contract in _PROFILE_CONTRACTS.values()
    for alias in (contract.profile, *contract.legacy_aliases)
}


def get_profile_contract(profile: str) -> SnapshotProfileContract:
    normalized = str(profile or "").strip().lower().replace("-", "_")
    canonical = _ALIAS_TO_PROFILE.get(normalized)
    if canonical is None:
        known = ", ".join(sorted(_PROFILE_CONTRACTS))
        raise ValueError(f"Unknown snapshot profile {profile!r}; known profiles: {known}")
    return _PROFILE_CONTRACTS[canonical]


def list_profile_contracts() -> tuple[SnapshotProfileContract, ...]:
    return tuple(_PROFILE_CONTRACTS.values())
