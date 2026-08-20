"""P1-root adapter for the design-only TQQQ v6 QQQ observation contract.

The adapter is deliberately local and side-effect free.  It verifies an
already-materialized immutable P1 root before extracting the QQQ bars needed
by the strict V6 recomputation seam.  It does not acquire data, call cloud
storage, write artifacts, schedule work, run a strategy, or use a broker.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path

from quant_strategy_plugins.plugin_signal_envelope_v2 import canonical_json_bytes

from .tqqq_core_only_p1_binding import P2_V5_CONTRACT, verify_tqqq_core_only_input_root
from .tqqq_p2_v6_plugin_observe import (
    P3_V6_PLUGIN_OBSERVE_EVIDENCE_SCHEMA_VERSION,
    TqqqP2V6PluginObserveError,
    build_tqqq_p2_v6_qqq_price_regime_observe_contract,
    verify_tqqq_p3_v6_qqq_price_regime_observe,
)


class TqqqP2V6QqqPriceRegimeRootError(ValueError):
    """Sanitized error for a root that cannot safely feed the V6 observer."""


def _fail(code: str) -> None:
    raise TqqqP2V6QqqPriceRegimeRootError(code)


def _read_json(path: Path) -> object:
    def no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        return json.loads(path.read_bytes(), object_pairs_hook=no_duplicates)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise TqqqP2V6QqqPriceRegimeRootError("invalid_p1_root") from exc


def _root_digest(*, binding_bytes: bytes, bars_bytes: bytes, manifest_sha256: str) -> str:
    """Derive a root identity from all verified members without exposing them."""

    return hashlib.sha256(
        canonical_json_bytes(
            {
                "binding_sha256": hashlib.sha256(binding_bytes).hexdigest(),
                "bars_sha256": hashlib.sha256(bars_bytes).hexdigest(),
                "manifest_sha256": manifest_sha256,
            }
        )
    ).hexdigest()


def _verified_qqq_p1_material(
    snapshot_root: str | Path,
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]], str]:
    root = Path(snapshot_root)
    try:
        manifest_sha256 = verify_tqqq_core_only_input_root(root, contract=P2_V5_CONTRACT)
        binding_bytes = (root / "binding.json").read_bytes()
        bars_bytes = (root / "bars.json").read_bytes()
        # Verify a second time after reading so a changed member cannot be
        # combined with a prior root verdict.
        if verify_tqqq_core_only_input_root(root, contract=P2_V5_CONTRACT) != manifest_sha256:
            _fail("invalid_p1_root")
    except (OSError, ValueError) as exc:
        raise TqqqP2V6QqqPriceRegimeRootError("invalid_p1_root") from exc

    binding = _read_json(root / "binding.json")
    manifest = _read_json(root / "manifest.json")
    bars_payload = _read_json(root / "bars.json")
    if (
        not isinstance(binding, Mapping)
        or not isinstance(manifest, Mapping)
        or not isinstance(bars_payload, Mapping)
    ):
        _fail("invalid_p1_root")
    identity = binding.get("data_identity")
    symbols = bars_payload.get("symbols")
    if not isinstance(identity, Mapping) or not isinstance(symbols, Mapping):
        _fail("invalid_p1_root")
    qqq = symbols.get("QQQ")
    if not isinstance(qqq, Mapping) or not isinstance(qqq.get("bars"), list):
        _fail("invalid_p1_root")
    history: list[dict[str, object]] = []
    for row in qqq["bars"]:
        if not isinstance(row, Mapping):
            _fail("invalid_p1_root")
        history.append(dict(row))
    return (
        dict(binding),
        dict(manifest),
        history,
        _root_digest(
            binding_bytes=binding_bytes,
            bars_bytes=bars_bytes,
            manifest_sha256=manifest_sha256,
        ),
    )


def build_tqqq_p2_v6_qqq_price_regime_observe_from_root(
    *, snapshot_root: str | Path, qsp_revision: str
) -> tuple[dict[str, object], dict[str, object]]:
    """Build one P1-bound QQQ observation contract from a verified local root."""

    binding, manifest, qqq_bars, root_sha256 = _verified_qqq_p1_material(snapshot_root)
    return build_tqqq_p2_v6_qqq_price_regime_observe_contract(
        p1_binding=binding,
        p1_manifest=manifest,
        input_root_sha256=root_sha256,
        qqq_bars=qqq_bars,
        qsp_revision=qsp_revision,
    )


def _parked(reason_code: str) -> dict[str, object]:
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


def verify_tqqq_p3_v6_qqq_price_regime_from_root(
    *,
    snapshot_root: str | Path,
    contract: Mapping[str, object],
    signal_envelope: Mapping[str, object],
    base_strategy_targets: Mapping[str, object],
    observer_strategy_targets: Mapping[str, object],
) -> dict[str, object]:
    """Verify a V6 observation against the exact QQQ bars in a verified root.

    No input payload is returned.  Root or recomputation problems become the
    same redacted `PARKED` boundary used by the rest of the design-only V6
    contract.
    """

    try:
        binding, manifest, qqq_bars, root_sha256 = _verified_qqq_p1_material(snapshot_root)
    except TqqqP2V6QqqPriceRegimeRootError:
        return _parked("invalid_p1_root")
    try:
        return verify_tqqq_p3_v6_qqq_price_regime_observe(
            contract=contract,
            p1_binding=binding,
            p1_manifest=manifest,
            input_root_sha256=root_sha256,
            qqq_bars=qqq_bars,
            signal_envelope=signal_envelope,
            base_strategy_targets=base_strategy_targets,
            observer_strategy_targets=observer_strategy_targets,
        )
    except (TqqqP2V6PluginObserveError, TypeError, ValueError):
        return _parked("qqq_observer_recomputation_failure")


__all__ = [
    "TqqqP2V6QqqPriceRegimeRootError",
    "build_tqqq_p2_v6_qqq_price_regime_observe_from_root",
    "verify_tqqq_p3_v6_qqq_price_regime_from_root",
]
