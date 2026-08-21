"""Read-only P1--P3 route catalogue for independently frozen strategies.

The catalogue makes the common research-driver shape explicit without turning
one strategy's acquisition, replay, or execution code into a shared runtime.
Entries are descriptive only: they cannot acquire data, schedule work, call a
broker, alter a strategy target, or grant P4--P6 authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass

from ..lifecycle.soxl_core_only_p2_v3_contract import (
    INPUT_CONTRACT_ID as SOXL_INPUT_CONTRACT_ID,
    P2_V3_CONTRACT as SOXL_P2_V3_CONTRACT,
)
from ..lifecycle.tqqq_core_only_p1_binding import (
    INPUT_CONTRACT_ID as TQQQ_INPUT_CONTRACT_ID,
    P2_V5_CONTRACT,
)


SCHEMA_VERSION = "qsl.multi-strategy-research-driver-catalog.v1"
P1_P3_STAGES = ("P1", "P2", "P3")
DAILY_RESEARCH_WIRED = "DAILY_RESEARCH_WIRED"
MIGRATION_REQUIRED = "MIGRATION_REQUIRED"
_ROUTE_STATES = frozenset({DAILY_RESEARCH_WIRED, MIGRATION_REQUIRED})
_ROUTE_ID = re.compile(r"^[a-z][a-z0-9_.-]{2,95}$")
_IDENTITY_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{2,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class MultiStrategyDriverCatalogError(ValueError):
    """Raised when a descriptive P1--P3 route is malformed."""


def _nonblank(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise MultiStrategyDriverCatalogError(f"invalid {label}")
    return value


@dataclass(frozen=True)
class ResearchDriverRoute:
    """One non-executing route from a frozen strategy identity through P3.

    ``DAILY_RESEARCH_WIRED`` means only that a route already has its own
    research scheduler. ``MIGRATION_REQUIRED`` may name a frozen P2 identity,
    but it must not be scheduled until it obtains its own P1 input contract
    and P3 verifier. Its ``p3_replay_entrypoint`` is then a planned canonical
    name, not an importable runtime. Neither state grants paper, shadow, or
    live authority.
    """

    route_id: str
    research_identity_id: str
    input_contract_id: str
    p2_config_sha256: str
    p3_replay_entrypoint: str
    state: str
    migration_blockers: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _ROUTE_ID.fullmatch(_nonblank(self.route_id, "route id")):
            raise MultiStrategyDriverCatalogError("invalid route id")
        if not _IDENTITY_ID.fullmatch(_nonblank(self.research_identity_id, "research identity id")):
            raise MultiStrategyDriverCatalogError("invalid research identity id")
        _nonblank(self.input_contract_id, "input contract id")
        if not isinstance(self.p2_config_sha256, str) or not _SHA256.fullmatch(self.p2_config_sha256):
            raise MultiStrategyDriverCatalogError("invalid P2 config digest")
        _nonblank(self.p3_replay_entrypoint, "P3 replay entrypoint")
        if self.state not in _ROUTE_STATES:
            raise MultiStrategyDriverCatalogError("invalid route state")
        if type(self.migration_blockers) is not tuple or any(
            not isinstance(blocker, str) or not blocker or blocker != blocker.strip()
            for blocker in self.migration_blockers
        ):
            raise MultiStrategyDriverCatalogError("invalid migration blockers")
        if len(set(self.migration_blockers)) != len(self.migration_blockers):
            raise MultiStrategyDriverCatalogError("migration blockers must be unique")
        if self.state == DAILY_RESEARCH_WIRED and self.migration_blockers:
            raise MultiStrategyDriverCatalogError("daily route cannot carry migration blockers")
        if self.state == MIGRATION_REQUIRED and not self.migration_blockers:
            raise MultiStrategyDriverCatalogError("migration route requires explicit blockers")

    @property
    def route_sha256(self) -> str:
        return hashlib.sha256(canonical_route_bytes(self)).hexdigest()


def _route_payload(route: ResearchDriverRoute) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "route_id": route.route_id,
        "research_identity_id": route.research_identity_id,
        "input_contract_id": route.input_contract_id,
        "p2_config_sha256": route.p2_config_sha256,
        "p3_replay_entrypoint": route.p3_replay_entrypoint,
        "permitted_stages": list(P1_P3_STAGES),
        "state": route.state,
        "migration_blockers": list(route.migration_blockers),
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }


def canonical_route_bytes(route: ResearchDriverRoute) -> bytes:
    """Encode one validated route deterministically for cross-strategy review."""

    if type(route) is not ResearchDriverRoute:
        raise MultiStrategyDriverCatalogError("route must be immutable")
    return json.dumps(
        _route_payload(route),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


TQQQ_DAILY_RESEARCH_ROUTE = ResearchDriverRoute(
    route_id="tqqq.core-only.v5.daily-research",
    research_identity_id=P2_V5_CONTRACT.candidate_id,
    input_contract_id=TQQQ_INPUT_CONTRACT_ID,
    p2_config_sha256=P2_V5_CONTRACT.config_sha256,
    p3_replay_entrypoint="scripts/run_tqqq_p3.py",
    state=DAILY_RESEARCH_WIRED,
)

SOXL_DAILY_RESEARCH_ROUTE = ResearchDriverRoute(
    route_id="soxl.soxx.core-only.v3.daily-research",
    research_identity_id=SOXL_P2_V3_CONTRACT.candidate_id,
    input_contract_id=SOXL_INPUT_CONTRACT_ID,
    p2_config_sha256=SOXL_P2_V3_CONTRACT.config_sha256,
    p3_replay_entrypoint="scripts/run_soxl_core_only_p3_evidence.py",
    state=DAILY_RESEARCH_WIRED,
)

CURRENT_RESEARCH_DRIVER_ROUTES: tuple[ResearchDriverRoute, ...] = (
    TQQQ_DAILY_RESEARCH_ROUTE,
    SOXL_DAILY_RESEARCH_ROUTE,
)


def build_multi_strategy_research_driver_catalog(
    routes: Sequence[ResearchDriverRoute] = CURRENT_RESEARCH_DRIVER_ROUTES,
) -> dict[str, object]:
    """Return immutable route facts without resolving or activating a route."""

    entries = tuple(routes)
    if not entries or any(type(route) is not ResearchDriverRoute for route in entries):
        raise MultiStrategyDriverCatalogError("catalogue requires immutable routes")
    if len({route.route_id for route in entries}) != len(entries):
        raise MultiStrategyDriverCatalogError("route ids must be unique")
    return {
        "schema_version": SCHEMA_VERSION,
        "routes": [{**_route_payload(route), "route_sha256": route.route_sha256} for route in entries],
    }


__all__ = [
    "CURRENT_RESEARCH_DRIVER_ROUTES",
    "DAILY_RESEARCH_WIRED",
    "MIGRATION_REQUIRED",
    "MultiStrategyDriverCatalogError",
    "P1_P3_STAGES",
    "ResearchDriverRoute",
    "SCHEMA_VERSION",
    "SOXL_DAILY_RESEARCH_ROUTE",
    "TQQQ_DAILY_RESEARCH_ROUTE",
    "build_multi_strategy_research_driver_catalog",
    "canonical_route_bytes",
]
