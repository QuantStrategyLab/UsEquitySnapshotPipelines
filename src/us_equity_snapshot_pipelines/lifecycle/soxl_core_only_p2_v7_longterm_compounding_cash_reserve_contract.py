"""Immutable P2 v7 identity for SOXL cash-reserve long-term research.

v7 is a separate, research-only candidate.  It pins the upstream source
revision in which ``cash_reserve_ratio`` is deducted from target exposure,
so no v6 evidence can be reused for it.
"""

from __future__ import annotations

from dataclasses import dataclass

CANDIDATE_ID = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
CONFIG_SHA256 = "843ab4e93e81985c2b3becc61a2f0b971508ccf25afa59acf402e75f574514d1"
UES_REVISION = "07b164d95f2ab4d4c54fd993f6f2040bd207d664"
QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
INPUT_CONTRACT_ID = "soxl_soxx_core_only_v7_longterm_compounding_cash_reserve_daily_split_adjusted_close_assured.v1"
P1_P3_RESEARCH_ONLY_GATE = "SOXL_CORE_ONLY_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_P1_P3_RESEARCH_ONLY_DRIVER"


@dataclass(frozen=True)
class SoxlCoreOnlyP2V7LongtermCompoundingCashReserveContract:
    """The v7 source/config identity; it is not an execution entitlement."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    input_contract_id: str


P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT = (
    SoxlCoreOnlyP2V7LongtermCompoundingCashReserveContract(
        candidate_id=CANDIDATE_ID,
        config_sha256=CONFIG_SHA256,
        ues_revision=UES_REVISION,
        qpk_revision=QPK_REVISION,
        input_contract_id=INPUT_CONTRACT_ID,
    )
)
