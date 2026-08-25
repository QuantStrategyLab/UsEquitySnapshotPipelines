"""Immutable P2 v6 identity for SOXL long-term-compounding research.

v6 is a separate research candidate because it changes the acceptance policy.
It preserves v5 runtime behavior and never grants broker, shadow, paper, or
live authority.
"""

from __future__ import annotations

from dataclasses import dataclass

CANDIDATE_ID = "soxl_soxx_core_only_p2_v6_longterm_compounding"
CONFIG_SHA256 = "2b8361c5d2a3bbae850213414e6e7ba1edf8c50ad5a76629a5f3cb25a8c1d19e"
UES_REVISION = "be692f75f64557e68edbff93786781e26c4f5893"
QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
INPUT_CONTRACT_ID = "soxl_soxx_core_only_v6_longterm_compounding_daily_split_adjusted_close_assured.v1"
P1_P3_RESEARCH_ONLY_GATE = "SOXL_CORE_ONLY_V6_LONGTERM_COMPOUNDING_P1_P3_RESEARCH_ONLY_DRIVER"


@dataclass(frozen=True)
class SoxlCoreOnlyP2V6LongtermCompoundingContract:
    """The v6 source/config identity; it is not an execution entitlement."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    input_contract_id: str


P2_V6_LONGTERM_COMPOUNDING_CONTRACT = SoxlCoreOnlyP2V6LongtermCompoundingContract(
    candidate_id=CANDIDATE_ID,
    config_sha256=CONFIG_SHA256,
    ues_revision=UES_REVISION,
    qpk_revision=QPK_REVISION,
    input_contract_id=INPUT_CONTRACT_ID,
)
