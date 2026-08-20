"""Pure P2/P3 contract for an observe-only TQQQ plugin signal.

This module deliberately accepts already-materialized JSON mappings only.  It
does not read a P1 directory, connect to storage, schedule work, or call a
strategy/broker runtime.  A future caller must verify its own immutable P1
root before passing the root digest to this contract.

The P2 v6 record is not a replacement for the active P2 v5 candidate.  It is
an explicit, per-input observation binding around that unchanged candidate;
the plugin signal cannot transform strategy targets or grant any authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import date

from quant_strategy_plugins.plugin_signal_envelope_v2 import (
    SCHEMA_VERSION as SIGNAL_ENVELOPE_SCHEMA_VERSION,
    SignalEnvelopeValidationError,
    canonical_json_bytes,
    validate_signal_envelope,
)

from .tqqq_core_only_p1_binding import (
    P2_V5_CONTRACT,
    TqqqCoreOnlyP1BindingError,
    tqqq_core_only_p1_binding_sha256_for_contract,
    validate_tqqq_core_only_input_manifest,
    validate_tqqq_core_only_p1_binding_for_contract,
)


P2_V6_PLUGIN_OBSERVE_SCHEMA_VERSION = "qsl.tqqq-core-only-p2-plugin-observe.v6"
P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION = "qsl.tqqq-p3-plugin-observe-evidence.v1"
P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID = "tqqq_core_only_p2_v6_plugin_observe"
OBSERVE_ONLY_MODE = "observe_only"

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_REVISION = re.compile(r"^[0-9a-f]{40,64}$")
_CONTRACT_FIELDS = frozenset(
    {
        "schema_version",
        "candidate_id",
        "base_strategy",
        "p1",
        "signal",
        "observer",
        "contract_sha256",
    }
)
_BASE_STRATEGY_FIELDS = frozenset(
    {"candidate_id", "config_sha256", "repository", "revision"}
)
_P1_FIELDS = frozenset(
    {"binding_sha256", "p1_manifest_sha256", "input_root_sha256", "date_cutoff"}
)
_SIGNAL_FIELDS = frozenset(
    {"schema_version", "plugin_id", "payload_sha256", "producer"}
)
_SIGNAL_PRODUCER_FIELDS = frozenset(
    {"repository", "revision", "entrypoint", "code_sha256", "config_sha256"}
)
_OBSERVER_FIELDS = frozenset(
    {"mode", "strategy_target_transform", "execution_authorized", "ai_input_allowed"}
)
_QSP_REPOSITORY = "QuantStrategyLab/QuantStrategyPlugins"
_UES_REPOSITORY = "QuantStrategyLab/UsEquityStrategies"


class TqqqP2V6PluginObserveError(ValueError):
    """A sanitized P2/P3 observation-contract validation failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _fail(code: str) -> None:
    raise TqqqP2V6PluginObserveError(code)


def _exact_mapping(value: object, fields: frozenset[str], code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(code)
    return value


def _digest(value: object, code: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail(code)
    return value


def _revision(value: object, code: str) -> str:
    if not isinstance(value, str) or not _REVISION.fullmatch(value):
        _fail(code)
    return value


def _canonical_bytes(value: object, *, code: str) -> bytes:
    try:
        return canonical_json_bytes(value)
    except SignalEnvelopeValidationError as exc:
        raise TqqqP2V6PluginObserveError(code) from exc


def calculate_tqqq_p2_v6_plugin_observe_contract_sha256(value: Mapping[str, object]) -> str:
    """Return the deterministic digest of a v6 record excluding its self-digest."""

    material = dict(value)
    material.pop("contract_sha256", None)
    return hashlib.sha256(_canonical_bytes(material, code="invalid_observation_contract")).hexdigest()


def _validate_date_cutoff(value: object, code: str) -> str:
    if not isinstance(value, str):
        _fail(code)
    # The P1 binding has already checked that this is a valid completed XNYS
    # session.  This independent check protects a standalone P2 record too.
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        _fail(code)
    try:
        if date.fromisoformat(value).isoformat() != value:
            _fail(code)
    except ValueError:
        _fail(code)
    return value


def _base_strategy() -> dict[str, str]:
    return {
        "candidate_id": P2_V5_CONTRACT.candidate_id,
        "config_sha256": P2_V5_CONTRACT.config_sha256,
        "repository": _UES_REPOSITORY,
        "revision": P2_V5_CONTRACT.ues_revision,
    }


def _validate_base_strategy(value: object) -> dict[str, str]:
    base = _exact_mapping(value, _BASE_STRATEGY_FIELDS, "invalid_base_strategy")
    expected = _base_strategy()
    if dict(base) != expected:
        _fail("invalid_base_strategy")
    return expected


def _validated_p1_identity(
    *,
    p1_binding: Mapping[str, object],
    p1_manifest: Mapping[str, object],
    input_root_sha256: object,
) -> dict[str, str]:
    """Validate supplied JSON provenance without reading any root or storage."""

    root_digest = _digest(input_root_sha256, "invalid_p1_root")
    try:
        frozen_binding = validate_tqqq_core_only_p1_binding_for_contract(
            p1_binding, P2_V5_CONTRACT
        )
        manifest_digest = validate_tqqq_core_only_input_manifest(
            p1_manifest, frozen_binding, contract=P2_V5_CONTRACT
        )
        binding_digest = tqqq_core_only_p1_binding_sha256_for_contract(
            frozen_binding, P2_V5_CONTRACT
        )
        identity = frozen_binding["data_identity"]
        if not isinstance(identity, Mapping):
            raise TqqqCoreOnlyP1BindingError("invalid TQQQ core-only P1 binding")
        cutoff = identity["date_cutoff"]
    except (KeyError, TypeError, ValueError, TqqqCoreOnlyP1BindingError) as exc:
        raise TqqqP2V6PluginObserveError("invalid_p1_identity") from exc
    return {
        "binding_sha256": binding_digest,
        "p1_manifest_sha256": manifest_digest,
        "input_root_sha256": root_digest,
        "date_cutoff": _validate_date_cutoff(cutoff, "invalid_p1_identity"),
    }


def _validate_p1_reference(value: object) -> dict[str, str]:
    p1 = _exact_mapping(value, _P1_FIELDS, "invalid_p1_reference")
    return {
        "binding_sha256": _digest(p1["binding_sha256"], "invalid_p1_reference"),
        "p1_manifest_sha256": _digest(p1["p1_manifest_sha256"], "invalid_p1_reference"),
        "input_root_sha256": _digest(p1["input_root_sha256"], "invalid_p1_reference"),
        "date_cutoff": _validate_date_cutoff(p1["date_cutoff"], "invalid_p1_reference"),
    }


def _signal_reference(
    signal_envelope: Mapping[str, object], *, p1: Mapping[str, str]
) -> dict[str, object]:
    try:
        envelope = validate_signal_envelope(signal_envelope)
    except SignalEnvelopeValidationError as exc:
        raise TqqqP2V6PluginObserveError("plugin_signal_rejected") from exc

    signal_input = envelope["input"]
    if not isinstance(signal_input, Mapping) or dict(signal_input) != {
        "p1_manifest_sha256": p1["p1_manifest_sha256"],
        "input_root_sha256": p1["input_root_sha256"],
        "date_cutoff": p1["date_cutoff"],
    }:
        _fail("plugin_input_provenance_mismatch")
    producer = envelope["producer"]
    if not isinstance(producer, Mapping) or producer.get("repo") != _QSP_REPOSITORY:
        _fail("plugin_provenance_mismatch")
    reference = {
        "schema_version": envelope["schema_version"],
        "plugin_id": envelope["plugin_id"],
        "payload_sha256": envelope["payload_sha256"],
        "producer": {
            "repository": producer["repo"],
            "revision": producer["revision"],
            "entrypoint": producer["entrypoint"],
            "code_sha256": producer["code_sha256"],
            "config_sha256": producer["config_sha256"],
        },
    }
    return _validate_signal_reference(reference)


def _validate_signal_reference(value: object) -> dict[str, object]:
    signal = _exact_mapping(value, _SIGNAL_FIELDS, "invalid_signal_reference")
    if signal["schema_version"] != SIGNAL_ENVELOPE_SCHEMA_VERSION:
        _fail("invalid_signal_reference")
    plugin_id = signal["plugin_id"]
    if not isinstance(plugin_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", plugin_id):
        _fail("invalid_signal_reference")
    producer = _exact_mapping(signal["producer"], _SIGNAL_PRODUCER_FIELDS, "invalid_signal_reference")
    if producer["repository"] != _QSP_REPOSITORY:
        _fail("invalid_signal_reference")
    entrypoint = producer["entrypoint"]
    if not isinstance(entrypoint, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*", entrypoint
    ):
        _fail("invalid_signal_reference")
    return {
        "schema_version": SIGNAL_ENVELOPE_SCHEMA_VERSION,
        "plugin_id": plugin_id,
        "payload_sha256": _digest(signal["payload_sha256"], "invalid_signal_reference"),
        "producer": {
            "repository": _QSP_REPOSITORY,
            "revision": _revision(producer["revision"], "invalid_signal_reference"),
            "entrypoint": entrypoint,
            "code_sha256": _digest(producer["code_sha256"], "invalid_signal_reference"),
            "config_sha256": _digest(producer["config_sha256"], "invalid_signal_reference"),
        },
    }


def _observer_rule() -> dict[str, object]:
    return {
        "mode": OBSERVE_ONLY_MODE,
        "strategy_target_transform": "none",
        "execution_authorized": False,
        "ai_input_allowed": False,
    }


def _validate_observer_rule(value: object) -> dict[str, object]:
    observer = _exact_mapping(value, _OBSERVER_FIELDS, "invalid_observer_rule")
    if dict(observer) != _observer_rule():
        _fail("invalid_observer_rule")
    return _observer_rule()


def build_tqqq_p2_v6_plugin_observe_contract(
    *,
    p1_binding: Mapping[str, object],
    p1_manifest: Mapping[str, object],
    input_root_sha256: str,
    signal_envelope: Mapping[str, object],
) -> dict[str, object]:
    """Build one exact v6 observe-only binding from already supplied JSON data."""

    p1 = _validated_p1_identity(
        p1_binding=p1_binding,
        p1_manifest=p1_manifest,
        input_root_sha256=input_root_sha256,
    )
    signal = _signal_reference(signal_envelope, p1=p1)
    contract: dict[str, object] = {
        "schema_version": P2_V6_PLUGIN_OBSERVE_SCHEMA_VERSION,
        "candidate_id": P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
        "base_strategy": _base_strategy(),
        "p1": p1,
        "signal": signal,
        "observer": _observer_rule(),
    }
    contract["contract_sha256"] = calculate_tqqq_p2_v6_plugin_observe_contract_sha256(contract)
    return validate_tqqq_p2_v6_plugin_observe_contract(contract)


def validate_tqqq_p2_v6_plugin_observe_contract(value: Mapping[str, object]) -> dict[str, object]:
    """Validate one stored contract without looking up files, roots, or services."""

    contract = _exact_mapping(value, _CONTRACT_FIELDS, "invalid_observation_contract")
    if (
        contract["schema_version"] != P2_V6_PLUGIN_OBSERVE_SCHEMA_VERSION
        or contract["candidate_id"] != P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID
    ):
        _fail("invalid_observation_contract")
    normalized: dict[str, object] = {
        "schema_version": P2_V6_PLUGIN_OBSERVE_SCHEMA_VERSION,
        "candidate_id": P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
        "base_strategy": _validate_base_strategy(contract["base_strategy"]),
        "p1": _validate_p1_reference(contract["p1"]),
        "signal": _validate_signal_reference(contract["signal"]),
        "observer": _validate_observer_rule(contract["observer"]),
    }
    supplied_digest = _digest(contract["contract_sha256"], "invalid_observation_contract")
    if supplied_digest != calculate_tqqq_p2_v6_plugin_observe_contract_sha256(normalized):
        _fail("invalid_observation_contract")
    normalized["contract_sha256"] = supplied_digest
    return normalized


def observe_only_strategy_targets(strategy_targets: Mapping[str, object]) -> dict[str, object]:
    """Return one detached, JSON-only target mapping for P3 comparison."""

    if not isinstance(strategy_targets, Mapping) or not strategy_targets:
        _fail("invalid_base_strategy_targets")
    try:
        copied = json.loads(_canonical_bytes(dict(strategy_targets), code="invalid_base_strategy_targets"))
    except json.JSONDecodeError as exc:  # defensive: canonical JSON must decode.
        raise TqqqP2V6PluginObserveError("invalid_base_strategy_targets") from exc
    if not isinstance(copied, dict) or not copied:
        _fail("invalid_base_strategy_targets")
    return copied


def _parked_evidence(reason_code: str) -> dict[str, object]:
    return {
        "schema_version": P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION,
        "status": "PARKED",
        "reason_code": reason_code,
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }


def verify_tqqq_p3_v6_plugin_observe(
    *,
    contract: Mapping[str, object],
    p1_binding: Mapping[str, object],
    p1_manifest: Mapping[str, object],
    input_root_sha256: str,
    signal_envelope: Mapping[str, object],
    base_strategy_targets: Mapping[str, object],
    observer_strategy_targets: Mapping[str, object],
) -> dict[str, object]:
    """Verify an exact v6 binding and emit redacted P3 observation evidence.

    All failure outcomes are intentionally reduced to a reason code.  The
    returned evidence never contains bars, paths, signal payload, strategy
    targets, credentials, execution permissions, or raw validation errors.
    """

    try:
        frozen = validate_tqqq_p2_v6_plugin_observe_contract(contract)
        supplied_p1 = _validated_p1_identity(
            p1_binding=p1_binding,
            p1_manifest=p1_manifest,
            input_root_sha256=input_root_sha256,
        )
        expected_p1 = frozen["p1"]
        assert isinstance(expected_p1, Mapping)
        if supplied_p1["binding_sha256"] != expected_p1["binding_sha256"]:
            _fail("p1_binding_mismatch")
        if supplied_p1["p1_manifest_sha256"] != expected_p1["p1_manifest_sha256"]:
            _fail("p1_manifest_mismatch")
        if supplied_p1["input_root_sha256"] != expected_p1["input_root_sha256"]:
            _fail("p1_root_mismatch")
        if supplied_p1["date_cutoff"] != expected_p1["date_cutoff"]:
            _fail("p1_cutoff_mismatch")
        signal = _signal_reference(signal_envelope, p1=supplied_p1)
        if signal != frozen["signal"]:
            _fail("plugin_provenance_mismatch")
        base_targets = observe_only_strategy_targets(base_strategy_targets)
        observed_targets = observe_only_strategy_targets(observer_strategy_targets)
        base_targets_sha256 = hashlib.sha256(
            _canonical_bytes(base_targets, code="invalid_base_strategy_targets")
        ).hexdigest()
        if base_targets_sha256 != hashlib.sha256(
            _canonical_bytes(observed_targets, code="invalid_base_strategy_targets")
        ).hexdigest():
            _fail("strategy_target_equivalence_failed")
    except TqqqP2V6PluginObserveError as exc:
        return _parked_evidence(exc.code)

    producer = signal["producer"]
    assert isinstance(producer, Mapping)
    return {
        "schema_version": P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION,
        "status": "VERIFIED_OBSERVE_ONLY",
        "candidate": {
            "candidate_id": P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID,
            "contract_sha256": frozen["contract_sha256"],
            "base_candidate_id": P2_V5_CONTRACT.candidate_id,
            "base_config_sha256": P2_V5_CONTRACT.config_sha256,
        },
        "p1": {
            "p1_manifest_sha256": supplied_p1["p1_manifest_sha256"],
            "input_root_sha256": supplied_p1["input_root_sha256"],
            "date_cutoff": supplied_p1["date_cutoff"],
        },
        "signal": {
            "schema_version": signal["schema_version"],
            "plugin_id": signal["plugin_id"],
            "payload_sha256": signal["payload_sha256"],
            "producer_revision": producer["revision"],
            "config_sha256": producer["config_sha256"],
        },
        "observer": _observer_rule(),
        "target_equivalence": {
            "base_candidate_id": P2_V5_CONTRACT.candidate_id,
            "equivalent": True,
            "strategy_targets_sha256": base_targets_sha256,
        },
        "authority": {
            "research_only": True,
            "no_order": True,
            "p4_p5_p6_authorized": False,
        },
    }


__all__ = [
    "OBSERVE_ONLY_MODE",
    "P2_V6_PLUGIN_OBSERVE_CANDIDATE_ID",
    "P2_V6_PLUGIN_OBSERVE_SCHEMA_VERSION",
    "P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION",
    "TqqqP2V6PluginObserveError",
    "build_tqqq_p2_v6_plugin_observe_contract",
    "calculate_tqqq_p2_v6_plugin_observe_contract_sha256",
    "observe_only_strategy_targets",
    "validate_tqqq_p2_v6_plugin_observe_contract",
    "verify_tqqq_p3_v6_plugin_observe",
]
