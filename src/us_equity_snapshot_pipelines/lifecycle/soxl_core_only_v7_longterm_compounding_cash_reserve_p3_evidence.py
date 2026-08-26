"""SOXL v7 metrics-only evidence with its cash-reserve source identity pinned."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from .soxl_core_only_v6_longterm_compounding_p3_evidence import (
    SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError,
    _build_longterm_compounding_p3_evidence_plan,
    _build_longterm_compounding_p3_evidence_summary,
)


class SoxlCoreOnlyV7LongtermCompoundingCashReserveP3EvidenceError(ValueError):
    """Fail-closed v7 evidence error without raw price data in diagnostics."""


def build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan(
    materialized: Mapping[str, object],
) -> dict[str, object]:
    """Build v7's fixed long-horizon plan using only its pinned identity."""
    try:
        return _build_longterm_compounding_p3_evidence_plan(
            materialized,
            p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
        )
    except SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError as exc:
        raise SoxlCoreOnlyV7LongtermCompoundingCashReserveP3EvidenceError(
            "invalid SOXL v7 long-term P3 evidence input"
        ) from exc


def build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary(
    *,
    materialized: Mapping[str, object],
    evidence_plan: Mapping[str, object],
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    """Return v7 metrics-only evidence; forward confirmation remains mandatory."""
    try:
        return _build_longterm_compounding_p3_evidence_summary(
            materialized=materialized,
            evidence_plan=evidence_plan,
            replay_executor=replay_executor,
            p2_contract=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
        )
    except SoxlCoreOnlyV6LongtermCompoundingP3EvidenceError as exc:
        raise SoxlCoreOnlyV7LongtermCompoundingCashReserveP3EvidenceError(
            "invalid SOXL v7 long-term P3 evidence input"
        ) from exc


__all__ = [
    "SoxlCoreOnlyV7LongtermCompoundingCashReserveP3EvidenceError",
    "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan",
    "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary",
]
