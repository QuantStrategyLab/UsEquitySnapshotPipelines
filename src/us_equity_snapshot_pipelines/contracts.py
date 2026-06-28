from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


SOURCE_PROJECT = "UsEquitySnapshotPipelines"
RUSSELL_TOP50_LEADER_ROTATION_PROFILE = "russell_top50_leader_rotation"
GLOBAL_ETF_ROTATION_PROFILE = "global_etf_rotation"
NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE = "new_r1000_residual_strength_20"


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
    RUSSELL_TOP50_LEADER_ROTATION_PROFILE: SnapshotProfileContract(
        profile=RUSSELL_TOP50_LEADER_ROTATION_PROFILE,
        display_name="Russell Top50 Leader Rotation",
        contract_version="russell_top50_leader_rotation.feature_snapshot.v1",
        snapshot_filename="russell_top50_leader_rotation_feature_snapshot_latest.csv",
        manifest_filename="russell_top50_leader_rotation_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="russell_top50_leader_rotation_ranking_latest.csv",
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-interactivebrokersquant/strategy-artifacts/us_equity/"
            "russell_top50_leader_rotation_staging"
        ),
        manifest_required_by_runtime=True,
    ),
    GLOBAL_ETF_ROTATION_PROFILE: SnapshotProfileContract(
        profile=GLOBAL_ETF_ROTATION_PROFILE,
        display_name="Global ETF Rotation",
        contract_version="global_etf_rotation.feature_snapshot.v1",
        snapshot_filename="global_etf_rotation_feature_snapshot_latest.csv",
        manifest_filename="global_etf_rotation_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="global_etf_rotation_ranking_latest.csv",
        neutral_gcs_prefix_hint=(
            "gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/"
            "global_etf_rotation"
        ),
        manifest_required_by_runtime=True,
    ),
    NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE: SnapshotProfileContract(
        profile=NEW_R1000_RESIDUAL_STRENGTH_20_PROFILE,
        display_name="R1000 Residual Strength 20",
        contract_version="new_r1000_residual_strength_20.feature_snapshot.v1",
        snapshot_filename="new_r1000_residual_strength_20_feature_snapshot_latest.csv",
        manifest_filename="new_r1000_residual_strength_20_feature_snapshot_latest.csv.manifest.json",
        ranking_filename="new_r1000_residual_strength_20_ranking_latest.csv",
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


def list_scheduled_profile_contracts() -> tuple[SnapshotProfileContract, ...]:
    return tuple(
        _PROFILE_CONTRACTS[profile]
        for profile in (
            GLOBAL_ETF_ROTATION_PROFILE,
            RUSSELL_TOP50_LEADER_ROTATION_PROFILE,
        )
    )
