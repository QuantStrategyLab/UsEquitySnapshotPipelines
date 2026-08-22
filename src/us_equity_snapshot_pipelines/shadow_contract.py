"""Shared safety contract for research-only shadow cycles."""

from __future__ import annotations

from typing import Any, Mapping

SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION = "shadow_cycle_contract.v1"
# Keep this contract intentionally narrow until every cycle has a no-order adapter.


def validate_shadow_cycle_contract(payload: Mapping[str, Any]) -> None:
    """Reject shadow artifacts that could be mistaken for executable orders."""
    contract = payload.get("shadow_contract")
    if not isinstance(contract, Mapping):
        raise ValueError("shadow cycle payload missing shadow_contract")
    if contract.get("schema_version") != SHADOW_CYCLE_CONTRACT_SCHEMA_VERSION:
        raise ValueError("unsupported shadow cycle contract schema_version")
    if contract.get("mode") != "research_only":
        raise ValueError("shadow cycle must remain research_only")
    if contract.get("no_order") is not True or contract.get("broker_access") is not False:
        raise ValueError("shadow cycle contract permits order or broker access")
