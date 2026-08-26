"""Research-only catalogue for frozen strategy, portfolio, and plugin candidates.

This module is deliberately a P2 catalogue, not a promotion or execution
contract.  In particular, entries do not carry broker, credential, account,
order, capital, paper, shadow, or live authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass

from ..lifecycle.soxl_core_only_p2_v3_contract import (
    INPUT_CONTRACT_ID as SOXL_CORE_ONLY_INPUT_CONTRACT_ID,
)
from ..lifecycle.soxl_core_only_p2_v3_contract import (
    P2_V3_CONTRACT,
)
from ..lifecycle.soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT,
)
from ..lifecycle.tqqq_core_only_p1_binding import (
    FREE_OHLCV_INPUT_CONTRACT_ID,
    INPUT_CONTRACT_ID,
    P2_V5_CONTRACT,
    P2_V9_CONTRACT,
)

STRATEGY_CANDIDATE_REGISTRY_SCHEMA = "qsl.strategy-candidate-registry.v1"
RESEARCH_STAGES = ("P1", "P2", "P3")
SINGLE_STRATEGY = "single_strategy"
PORTFOLIO = "portfolio"
PLUGIN = "plugin"
_CANDIDATE_KINDS = frozenset({SINGLE_STRATEGY, PORTFOLIO, PLUGIN})
_LOWER_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_LOWER_HEX_64 = re.compile(r"^[0-9a-f]{64}$")


class StrategyCandidateRegistryError(ValueError):
    """Raised when a research candidate is malformed or not registered."""


@dataclass(frozen=True)
class SourceRevision:
    """One immutable source dependency of a research candidate."""

    repository: str
    revision: str

    def __post_init__(self) -> None:
        if not isinstance(self.repository, str) or not self.repository or self.repository != self.repository.strip():
            raise StrategyCandidateRegistryError("source repository must be a non-empty canonical string")
        if not isinstance(self.revision, str) or not _LOWER_HEX_40.fullmatch(self.revision):
            raise StrategyCandidateRegistryError("source revision must be a lowercase 40-character Git revision")


@dataclass(frozen=True)
class PluginBinding:
    """A versioned, read-only plugin input declared by a research candidate."""

    plugin_id: str
    source_revision: SourceRevision
    config_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.plugin_id, str) or not self.plugin_id or self.plugin_id != self.plugin_id.strip():
            raise StrategyCandidateRegistryError("plugin id must be a non-empty canonical string")
        if type(self.source_revision) is not SourceRevision:
            raise StrategyCandidateRegistryError("plugin source revision must be immutable")
        if not isinstance(self.config_sha256, str) or not _LOWER_HEX_64.fullmatch(self.config_sha256):
            raise StrategyCandidateRegistryError("plugin config digest must be a lowercase SHA-256 digest")


@dataclass(frozen=True)
class StrategyCandidate:
    """A frozen, research-only candidate identity for P1 through P3.

    ``component_candidate_ids`` models a portfolio candidate's inputs.  It
    intentionally does not resolve or execute those components.  That keeps
    portfolio construction a future pure P2 concern, rather than creating an
    implicit P4/P5/P6 path here.
    """

    candidate_id: str
    kind: str
    config_sha256: str
    data_contract_id: str
    source_revisions: tuple[SourceRevision, ...]
    component_candidate_ids: tuple[str, ...] = ()
    plugin_bindings: tuple[PluginBinding, ...] = ()
    permitted_stages: tuple[str, ...] = RESEARCH_STAGES

    def __post_init__(self) -> None:
        if not isinstance(self.candidate_id, str) or not self.candidate_id or self.candidate_id != self.candidate_id.strip():
            raise StrategyCandidateRegistryError("candidate id must be a non-empty canonical string")
        if self.kind not in _CANDIDATE_KINDS:
            raise StrategyCandidateRegistryError("candidate kind is not supported")
        if not isinstance(self.config_sha256, str) or not _LOWER_HEX_64.fullmatch(self.config_sha256):
            raise StrategyCandidateRegistryError("candidate config digest must be a lowercase SHA-256 digest")
        if not isinstance(self.data_contract_id, str) or not self.data_contract_id or self.data_contract_id != self.data_contract_id.strip():
            raise StrategyCandidateRegistryError("data contract id must be a non-empty canonical string")
        if type(self.source_revisions) is not tuple or not self.source_revisions:
            raise StrategyCandidateRegistryError("candidate source revisions must be an immutable tuple")
        if any(type(item) is not SourceRevision for item in self.source_revisions):
            raise StrategyCandidateRegistryError("candidate must declare immutable source revisions")
        if len({item.repository for item in self.source_revisions}) != len(self.source_revisions):
            raise StrategyCandidateRegistryError("candidate source repositories must be unique")
        if self.permitted_stages != RESEARCH_STAGES:
            raise StrategyCandidateRegistryError("research candidates are limited to exact P1/P2/P3 stages")

        if type(self.component_candidate_ids) is not tuple or type(self.plugin_bindings) is not tuple:
            raise StrategyCandidateRegistryError("candidate components and plugin bindings must be immutable tuples")
        components = self.component_candidate_ids
        if any(not isinstance(item, str) or not item or item != item.strip() for item in components):
            raise StrategyCandidateRegistryError("component candidate ids must be non-empty canonical strings")
        if len(set(components)) != len(components) or self.candidate_id in components:
            raise StrategyCandidateRegistryError("portfolio components must be unique and cannot include the portfolio itself")

        if self.kind == PORTFOLIO and len(components) < 2:
            raise StrategyCandidateRegistryError("portfolio candidate requires at least two component candidates")
        if self.kind != PORTFOLIO and components:
            raise StrategyCandidateRegistryError("only portfolio candidates may declare component candidates")
        if any(type(item) is not PluginBinding for item in self.plugin_bindings):
            raise StrategyCandidateRegistryError("plugin bindings must be versioned plugin bindings")
        if self.kind == PLUGIN and len(self.plugin_bindings) != 1:
            raise StrategyCandidateRegistryError("plugin candidate requires exactly one plugin binding")

    @property
    def candidate_sha256(self) -> str:
        return hashlib.sha256(canonical_candidate_bytes(self)).hexdigest()


def _candidate_payload(candidate: StrategyCandidate) -> dict[str, object]:
    return {
        "schema_version": STRATEGY_CANDIDATE_REGISTRY_SCHEMA,
        "candidate_id": candidate.candidate_id,
        "kind": candidate.kind,
        "config_sha256": candidate.config_sha256,
        "data_contract_id": candidate.data_contract_id,
        "source_revisions": [
            {"repository": item.repository, "revision": item.revision}
            for item in candidate.source_revisions
        ],
        "component_candidate_ids": list(candidate.component_candidate_ids),
        "plugin_bindings": [
            {
                "plugin_id": item.plugin_id,
                "source_repository": item.source_revision.repository,
                "source_revision": item.source_revision.revision,
                "config_sha256": item.config_sha256,
            }
            for item in candidate.plugin_bindings
        ],
        "permitted_stages": list(candidate.permitted_stages),
    }


def canonical_candidate_bytes(candidate: StrategyCandidate) -> bytes:
    """Encode one validated candidate deterministically for evidence binding."""
    if type(candidate) is not StrategyCandidate:
        raise StrategyCandidateRegistryError("candidate must be a StrategyCandidate")
    return json.dumps(
        _candidate_payload(candidate),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


TQQQ_CORE_ONLY_P2_V5 = StrategyCandidate(
    candidate_id=P2_V5_CONTRACT.candidate_id,
    kind=SINGLE_STRATEGY,
    config_sha256=P2_V5_CONTRACT.config_sha256,
    data_contract_id=INPUT_CONTRACT_ID,
    source_revisions=(
        SourceRevision("QuantStrategyLab/QuantPlatformKit", P2_V5_CONTRACT.qpk_revision),
        SourceRevision("QuantStrategyLab/UsEquityStrategies", P2_V5_CONTRACT.ues_revision),
    ),
)

SOXL_SOXX_CORE_ONLY_P2_V3 = StrategyCandidate(
    candidate_id=P2_V3_CONTRACT.candidate_id,
    kind=SINGLE_STRATEGY,
    config_sha256=P2_V3_CONTRACT.config_sha256,
    data_contract_id=SOXL_CORE_ONLY_INPUT_CONTRACT_ID,
    source_revisions=(
        SourceRevision("QuantStrategyLab/QuantPlatformKit", P2_V3_CONTRACT.qpk_revision),
        SourceRevision("QuantStrategyLab/UsEquityStrategies", P2_V3_CONTRACT.ues_revision),
    ),
)

TQQQ_CORE_ONLY_P2_V9 = StrategyCandidate(
    candidate_id=P2_V9_CONTRACT.candidate_id,
    kind=SINGLE_STRATEGY,
    config_sha256=P2_V9_CONTRACT.config_sha256,
    data_contract_id=FREE_OHLCV_INPUT_CONTRACT_ID,
    source_revisions=(
        SourceRevision("QuantStrategyLab/QuantPlatformKit", P2_V9_CONTRACT.qpk_revision),
        SourceRevision("QuantStrategyLab/QuantStrategyPlugins", "af1963e102d9fd42cd23622d1d2799d2ea654747"),
        SourceRevision("QuantStrategyLab/UsEquityStrategies", P2_V9_CONTRACT.ues_revision),
    ),
)

SOXL_SOXX_CORE_ONLY_P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE = StrategyCandidate(
    candidate_id=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.candidate_id,
    kind=SINGLE_STRATEGY,
    config_sha256=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.config_sha256,
    data_contract_id=P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.input_contract_id,
    source_revisions=(
        SourceRevision(
            "QuantStrategyLab/QuantPlatformKit",
            P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.qpk_revision,
        ),
        SourceRevision(
            "QuantStrategyLab/UsEquityStrategies",
            P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT.ues_revision,
        ),
    ),
)

CURRENT_RESEARCH_CANDIDATES: tuple[StrategyCandidate, ...] = (
    TQQQ_CORE_ONLY_P2_V5,
    SOXL_SOXX_CORE_ONLY_P2_V3,
    TQQQ_CORE_ONLY_P2_V9,
    SOXL_SOXX_CORE_ONLY_P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE,
)


def resolve_research_candidate(candidate_id: object) -> StrategyCandidate:
    """Return a registered P1/P2/P3 candidate without granting any authority."""
    if not isinstance(candidate_id, str):
        raise StrategyCandidateRegistryError("unknown research candidate")
    for candidate in CURRENT_RESEARCH_CANDIDATES:
        if candidate.candidate_id == candidate_id:
            return candidate
    raise StrategyCandidateRegistryError("unknown research candidate")


def build_research_candidate_registry(
    candidates: Sequence[StrategyCandidate] = CURRENT_RESEARCH_CANDIDATES,
) -> dict[str, object]:
    """Return the read-only catalogue with deterministic candidate digests."""
    entries = tuple(candidates)
    if not entries or any(type(candidate) is not StrategyCandidate for candidate in entries):
        raise StrategyCandidateRegistryError("registry must contain StrategyCandidate entries")
    candidate_ids = {candidate.candidate_id for candidate in entries}
    if len(candidate_ids) != len(entries):
        raise StrategyCandidateRegistryError("registry candidate ids must be unique")
    for candidate in entries:
        if candidate.kind == PORTFOLIO:
            unresolved = set(candidate.component_candidate_ids) - candidate_ids
            if unresolved:
                raise StrategyCandidateRegistryError("portfolio components must be registered candidates")
    return {
        "schema_version": STRATEGY_CANDIDATE_REGISTRY_SCHEMA,
        "candidates": [
            {**_candidate_payload(candidate), "candidate_sha256": candidate.candidate_sha256}
            for candidate in entries
        ],
    }
