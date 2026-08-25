"""Immutable P2 v4 identity for the free, split-adjusted-close SOXL candidate.

This candidate deliberately does not replace P2 v3.  It preserves the frozen
research strategy behavior, but changes its market-data contract from the
licensed total-return-adjusted OHLCV source to a separately assured
split-adjusted close-only source.  It grants neither execution nor promotion
authority.
"""

from __future__ import annotations

from dataclasses import dataclass

CANDIDATE_ID = "soxl_soxx_core_only_p2_v4_free_split_close"
CONFIG_SHA256 = "142fe512dd48d9e61c8fe302710dd00eeeb1b945a60892c95c5dd4a439fd0550"
UES_REVISION = "be692f75f64557e68edbff93786781e26c4f5893"
QPK_REVISION = "f30e7b1910df8da22fdcedc347ab847df5adcd76"
INPUT_CONTRACT_ID = "soxl_soxx_core_only_daily_split_adjusted_close_assured.v1"
P1_P3_RESEARCH_ONLY_GATE = "SOXL_CORE_ONLY_FREE_SPLIT_CLOSE_P1_P3_RESEARCH_ONLY_DAILY_DRIVER"


@dataclass(frozen=True)
class SoxlCoreOnlyP2V4FreeSplitCloseContract:
    """The P2 v4 source/config identity; it grants no execution authority."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    input_contract_id: str


P2_V4_FREE_SPLIT_CLOSE_CONTRACT = SoxlCoreOnlyP2V4FreeSplitCloseContract(
    candidate_id=CANDIDATE_ID,
    config_sha256=CONFIG_SHA256,
    ues_revision=UES_REVISION,
    qpk_revision=QPK_REVISION,
    input_contract_id=INPUT_CONTRACT_ID,
)
