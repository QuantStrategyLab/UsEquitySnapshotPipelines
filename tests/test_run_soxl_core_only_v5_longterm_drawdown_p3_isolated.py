from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v5_longterm_drawdown_contract import (
    CANDIDATE_ID,
    CONFIG_SHA256,
    QPK_REVISION,
    UES_REVISION,
)

SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_v5_longterm_drawdown_p3_isolated.py"
V5_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v5_longterm_drawdown.json"
V4_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v4_free_split_close.json"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_v5_longterm_drawdown_p3_isolated", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v5_runner_installs_its_own_identity_and_rejects_v4_candidate() -> None:
    module = _module()
    core = module._core()

    assert Path(core.__file__) == SCRIPT.resolve()
    assert (module.P2_CANDIDATE_ID, module.P2_CONFIG_SHA256) == (CANDIDATE_ID, CONFIG_SHA256)
    assert (module.P2_UES_REVISION, module.P2_QPK_REVISION) == (UES_REVISION, QPK_REVISION)
    assert module.validate_p2_candidate(json.loads(V5_CANDIDATE.read_text(encoding="utf-8")))["candidate_id"] == CANDIDATE_ID
    with pytest.raises(Exception, match="identity mismatch"):
        module.validate_p2_candidate(json.loads(V4_CANDIDATE.read_text(encoding="utf-8")))


def test_v5_runner_accepts_its_frozen_boxx_delever_redirect_without_relaxing_v4() -> None:
    module = _module()
    v5_core = module._core()
    v4_script = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_free_split_close_p3_isolated.py"
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_free_split_close_p3_isolated", v4_script)
    assert spec and spec.loader
    v4_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(v4_module)
    v4_core = v4_module._core()
    decision = SimpleNamespace(
        positions=tuple(
            SimpleNamespace(symbol=symbol, target_value=weight)
            for symbol, weight in (("SOXL", 0.0), ("SOXX", 25_000.0), ("BOXX", 75_000.0))
        ),
        diagnostics={
            "blend_tier": "full",
            "base_blend_tier": "full",
            "active_risk_asset": "SOXX",
            "blend_gate_volatility_delever_triggered": True,
            "blend_gate_volatility_delever_redirect_symbol": "BOXX",
            "market_regime_control_enabled": False,
            "market_regime_control_applied": False,
        },
    )

    result = v5_core._summarize_source_decision(decision, as_of=datetime(2026, 1, 2, tzinfo=UTC))

    assert result["diagnostics"]["blend_gate_volatility_delever_redirect_symbol"] == "BOXX"
    with pytest.raises(v4_core.SoxlCoreOnlyP3IsolatedRunnerError):
        v4_core._summarize_source_decision(decision, as_of=datetime(2026, 1, 2, tzinfo=UTC))
