"""Immutable P2 v5 identity for the long-term SOXL drawdown candidate.

This is a research-only risk-budget candidate.  It does not alter P2 v4,
authorize execution, or make any automatic promotion decision.
"""

from __future__ import annotations

from dataclasses import dataclass

CANDIDATE_ID = "soxl_soxx_core_only_p2_v5_longterm_drawdown"
CONFIG_SHA256 = "d1e9278400b1f94ebdf6bb43e796c50edc7f616fa8cb647a38b4eec32cb0f0ba"
UES_REVISION = "be692f75f64557e68edbff93786781e26c4f5893"
QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
INPUT_CONTRACT_ID = "soxl_soxx_core_only_v5_longterm_drawdown_daily_split_adjusted_close_assured.v1"
P1_P3_RESEARCH_ONLY_GATE = "SOXL_CORE_ONLY_V5_LONGTERM_DRAWDOWN_P1_P3_RESEARCH_ONLY_DAILY_DRIVER"


@dataclass(frozen=True)
class SoxlCoreOnlyP2V5LongtermDrawdownContract:
    """The P2 v5 source/config identity; it grants no execution authority."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    input_contract_id: str


P2_V5_LONGTERM_DRAWDOWN_CONTRACT = SoxlCoreOnlyP2V5LongtermDrawdownContract(
    candidate_id=CANDIDATE_ID,
    config_sha256=CONFIG_SHA256,
    ues_revision=UES_REVISION,
    qpk_revision=QPK_REVISION,
    input_contract_id=INPUT_CONTRACT_ID,
)
