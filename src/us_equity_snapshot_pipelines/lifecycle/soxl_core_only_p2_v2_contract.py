"""Immutable P2 identity for the SOXL/SOXX core-only migration candidate.

This module intentionally contains no provider, storage, workflow, replay, or
execution integration.  A future SOXL P1/P3 implementation must bind to this
identity exactly; it cannot silently inherit the legacy fixed-cutoff route.
"""

from __future__ import annotations

from dataclasses import dataclass


CANDIDATE_ID = "soxl_soxx_core_only_p2_v2"
CONFIG_SHA256 = "c63c6d96057644a3c3cfc506a93d61c14836a5f7aa164bd629fa03ca234ff140"
UES_REVISION = "7756fe32585e85cf1d09a163203a02e3eee39fe1"
QPK_REVISION = "3acab1923a97b805b077c85c6c19657be0143bac"
FUTURE_INPUT_CONTRACT_ID = "soxl_soxx_core_only_daily_observed_bars.v1"
FUTURE_P3_VERIFIER_GATE = "FRESH_SOXL_CORE_ONLY_P1_INPUT_CONTRACT_AND_P3_VERIFIER"


@dataclass(frozen=True)
class SoxlCoreOnlyP2V2Contract:
    """The P2 source/config identity; it grants no data or execution authority."""

    candidate_id: str
    config_sha256: str
    ues_revision: str
    qpk_revision: str
    future_input_contract_id: str


P2_V2_CONTRACT = SoxlCoreOnlyP2V2Contract(
    candidate_id=CANDIDATE_ID,
    config_sha256=CONFIG_SHA256,
    ues_revision=UES_REVISION,
    qpk_revision=QPK_REVISION,
    future_input_contract_id=FUTURE_INPUT_CONTRACT_ID,
)
