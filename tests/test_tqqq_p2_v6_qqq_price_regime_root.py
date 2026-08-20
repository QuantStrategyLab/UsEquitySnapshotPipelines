from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle import tqqq_core_only_p1_binding as p1_binding
from us_equity_snapshot_pipelines.lifecycle.tqqq_p2_v6_qqq_price_regime_root import (
    TqqqP2V6QqqPriceRegimeRootError,
    build_tqqq_p2_v6_qqq_price_regime_observe_from_root,
    verify_tqqq_p3_v6_qqq_price_regime_from_root,
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _producer() -> dict[str, str]:
    return {
        "repository": "QuantStrategyLab/UsEquitySnapshotPipelines",
        "commit_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "tool": "tqqq_core_only_p1_alpaca_sip_acquisition",
        "tool_version": "v1",
    }


def _bars(symbol: str, *, cutoff: str) -> dict[str, object]:
    first_eligible = {"QQQM": "2020-10-13", "BOXX": "2022-12-28"}.get(symbol)
    rows: list[dict[str, object]] = []
    for index, session in enumerate(p1_binding._expected_xnys_sessions(cutoff)):
        if first_eligible is not None and session.isoformat() < first_eligible:
            continue
        close = 100.0 + index * 0.1 if symbol == "QQQ" else 100.0
        rows.append(
            {
                "date": session.isoformat(),
                "open": close,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 1.0,
            }
        )
    return {"bars": rows}


def _write_verified_v5_root(root: Path, *, cutoff: str = "2026-08-18") -> Path:
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    binding = p1_binding.build_tqqq_core_only_p1_binding_for_contract(
        p1_binding.P2_V5_CONTRACT, date_cutoff=cutoff
    )
    symbols = {symbol: _bars(symbol, cutoff=cutoff) for symbol in ("QQQ", "TQQQ", "QQQM", "BOXX")}
    bars = {"schema_version": "tqqq_core_only_private_bars.v1", "symbols": symbols}
    bars_bytes = _canonical(bars)
    manifest = p1_binding.build_tqqq_core_only_input_manifest(
        binding,
        observed_at="2026-08-19T00:00:00Z",
        producer=_producer(),
        member_bytes=bars_bytes,
        source_content_sha256={
            symbol: hashlib.sha256(_canonical(payload)).hexdigest() for symbol, payload in symbols.items()
        },
        contract=p1_binding.P2_V5_CONTRACT,
    )
    (root / "binding.json").write_bytes(
        p1_binding.canonical_tqqq_core_only_p1_binding_bytes_for_contract(
            binding, p1_binding.P2_V5_CONTRACT
        )
    )
    (root / "bars.json").write_bytes(bars_bytes)
    (root / "manifest.json").write_bytes(p1_binding.canonical_research_input_manifest_bytes(manifest))
    assert p1_binding.verify_tqqq_core_only_input_root(root, contract=p1_binding.P2_V5_CONTRACT)
    return root


def test_root_adapter_binds_and_recomputes_only_the_verified_qqq_bars(tmp_path: Path) -> None:
    root = _write_verified_v5_root(tmp_path / "snapshot")
    contract, signal = build_tqqq_p2_v6_qqq_price_regime_observe_from_root(
        snapshot_root=root,
        qsp_revision="7" * 40,
    )

    evidence = verify_tqqq_p3_v6_qqq_price_regime_from_root(
        snapshot_root=root,
        contract=contract,
        signal_envelope=signal,
        base_strategy_targets={"TQQQ": 0.45, "QQQM": 0.45, "BOXX": 0.08, "cash": 0.02},
        observer_strategy_targets={"TQQQ": 0.45, "QQQM": 0.45, "BOXX": 0.08, "cash": 0.02},
    )

    assert contract["p1"]["input_root_sha256"] != contract["p1"]["p1_manifest_sha256"]
    assert evidence["status"] == "VERIFIED_OBSERVE_ONLY"
    assert evidence["recomputation"]["matched"] is True
    assert "payload" not in evidence["signal"]


def test_root_adapter_parks_a_signal_that_cannot_be_recomputed_from_this_root(tmp_path: Path) -> None:
    root = _write_verified_v5_root(tmp_path / "snapshot")
    contract, signal = build_tqqq_p2_v6_qqq_price_regime_observe_from_root(
        snapshot_root=root,
        qsp_revision="7" * 40,
    )
    changed_signal = json.loads(json.dumps(signal))
    changed_signal["payload"]["facts"]["trend_state"] = "BELOW_TREND_MEAN"

    evidence = verify_tqqq_p3_v6_qqq_price_regime_from_root(
        snapshot_root=root,
        contract=contract,
        signal_envelope=changed_signal,
        base_strategy_targets={"TQQQ": 0.45, "QQQM": 0.45, "BOXX": 0.08, "cash": 0.02},
        observer_strategy_targets={"TQQQ": 0.45, "QQQM": 0.45, "BOXX": 0.08, "cash": 0.02},
    )

    assert evidence["status"] == "PARKED"
    assert evidence["reason_code"] == "qqq_observer_recomputation_mismatch"


def test_root_adapter_rejects_a_root_that_no_longer_matches_its_p1_manifest(tmp_path: Path) -> None:
    root = _write_verified_v5_root(tmp_path / "snapshot")
    (root / "bars.json").write_bytes(b'{"tampered":true}')

    with pytest.raises(TqqqP2V6QqqPriceRegimeRootError, match="invalid_p1_root"):
        build_tqqq_p2_v6_qqq_price_regime_observe_from_root(
            snapshot_root=root,
            qsp_revision="7" * 40,
        )

    evidence = verify_tqqq_p3_v6_qqq_price_regime_from_root(
        snapshot_root=root,
        contract={},
        signal_envelope={},
        base_strategy_targets={"TQQQ": 0.45},
        observer_strategy_targets={"TQQQ": 0.45},
    )
    assert evidence["status"] == "PARKED"
    assert evidence["reason_code"] == "invalid_p1_root"
