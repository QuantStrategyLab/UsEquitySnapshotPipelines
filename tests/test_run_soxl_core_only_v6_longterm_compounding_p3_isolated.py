from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v6_longterm_compounding_contract import (
    CANDIDATE_ID,
    CONFIG_SHA256,
    QPK_REVISION,
    UES_REVISION,
)

SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_v6_longterm_compounding_p3_isolated.py"
V6_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v6_longterm_compounding.json"
V5_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v5_longterm_drawdown.json"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_v6_longterm_compounding_p3_isolated", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v6_runner_installs_its_own_identity_and_rejects_v5_candidate() -> None:
    module = _module()

    assert (module.P2_CANDIDATE_ID, module.P2_CONFIG_SHA256) == (CANDIDATE_ID, CONFIG_SHA256)
    assert (module.P2_UES_REVISION, module.P2_QPK_REVISION) == (UES_REVISION, QPK_REVISION)
    assert module.validate_p2_candidate(json.loads(V6_CANDIDATE.read_text(encoding="utf-8")))["candidate_id"] == CANDIDATE_ID
    with pytest.raises(Exception, match="identity mismatch"):
        module.validate_p2_candidate(json.loads(V5_CANDIDATE.read_text(encoding="utf-8")))
