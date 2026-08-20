"""Immutable P2 v3 identity for the eligible SOXL/SOXX core-only candidate.

P2 v3 preserves P2 v2's strategy parameters and source behavior, but has a
new identity because its verified source dependency chain and data-only P1/P3
research entry are now frozen.  It grants neither execution nor promotion
authority.
"""

from __future__ import annotations

from dataclasses import dataclass

CANDIDATE_ID = "soxl_soxx_core_only_p2_v3"
CONFIG_SHA256 = "ff8fa0acf4f175a7c40c3e1e6a3304ea2748b6b81c3797342085a4df3810ab4d"
UES_REVISION = "7756fe32585e85cf1d09a163203a02e3eee39fe1"
QPK_REVISION = "3acab1923a97b805b077c85c6c19657be0143bac"
INPUT_CONTRACT_ID = "soxl_soxx_core_only_daily_observed_bars.v2"
P1_P3_RESEARCH_ONLY_GATE = "SOXL_CORE_ONLY_P1_P3_RESEARCH_ONLY_DAILY_DRIVER"


@dataclass(frozen=True)
class SoxlCoreOnlyP2V3Contract:
    """The P2 v3 source/config identity; it grants no execution authority."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    input_contract_id: str


P2_V3_CONTRACT = SoxlCoreOnlyP2V3Contract(
    candidate_id=CANDIDATE_ID,
    config_sha256=CONFIG_SHA256,
    ues_revision=UES_REVISION,
    qpk_revision=QPK_REVISION,
    input_contract_id=INPUT_CONTRACT_ID,
)
