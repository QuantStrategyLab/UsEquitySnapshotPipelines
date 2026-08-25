from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_p2_v4_free_split_close_contract import (
    CANDIDATE_ID,
    CONFIG_SHA256,
    QPK_REVISION,
    UES_REVISION,
)

SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_free_split_close_p3_isolated.py"
V4_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v4_free_split_close.json"
V3_CANDIDATE = Path(__file__).parents[1] / "config" / "soxl_soxx_core_only_p2_v3.json"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_free_split_close_p3_isolated", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v4_runner_installs_a_private_v4_identity_before_candidate_validation() -> None:
    module = _module()
    core = module._core()

    assert core.P2_CANDIDATE_ID == module.P2_CANDIDATE_ID
    assert core.P2_CONFIG_SHA256 == module.P2_CONFIG_SHA256
    assert core.P2_UES_REVISION == module.P2_UES_REVISION
    assert core.P2_QPK_REVISION == module.P2_QPK_REVISION
    assert Path(core.__file__) == SCRIPT.resolve()
    assert module.P2_CANDIDATE_ID == CANDIDATE_ID
    assert module.P2_CONFIG_SHA256 == CONFIG_SHA256
    assert module.P2_UES_REVISION == UES_REVISION
    assert module.P2_QPK_REVISION == QPK_REVISION
    assert module.validate_p2_candidate(json.loads(V4_CANDIDATE.read_text(encoding="utf-8"))) == {
        "candidate_id": module.P2_CANDIDATE_ID,
        "config_sha256": module.P2_CONFIG_SHA256,
        "runtime_config": json.loads(V4_CANDIDATE.read_text(encoding="utf-8"))["runtime_config"],
    }
    with pytest.raises(Exception, match="identity mismatch"):
        module.validate_p2_candidate(json.loads(V3_CANDIDATE.read_text(encoding="utf-8")))


def test_v4_outer_runner_rejects_a_source_result_with_non_v4_candidate_identity(monkeypatch, tmp_path) -> None:
    module = _module()
    input_path = tmp_path / "context.json"
    input_path.write_text("{}", encoding="utf-8")
    candidate = json.loads(V4_CANDIDATE.read_text(encoding="utf-8"))
    candidate_path = tmp_path / "candidate.json"
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")
    core = module._core()
    monkeypatch.setattr(core, "validate_ues_project", lambda _path: {})
    monkeypatch.setattr(
        core,
        "validate_p2_candidate",
        lambda _value: {"candidate_id": module.P2_CANDIDATE_ID, "config_sha256": module.P2_CONFIG_SHA256},
    )
    monkeypatch.setattr(core.shutil, "which", lambda _name: "/usr/bin/uv")
    source = {
        "schema_version": core.STATEFUL_REPLAY_RESULT_SCHEMA,
        "entrypoint": core.ENTRYPOINT,
        "execution_timing": "next_complete_trading_session_after_signal_effective_date",
        "cost_bps": 5.0,
        "initial_equity": 100_000.0,
        "final_equity": 100_000.0,
        "executed_signal_count": 1,
        "unexecuted_final_signal": True,
        "one_way_turnover": 0.0,
        "cost_total": 0.0,
        "decisions": [],
    }
    source["output_sha256"] = hashlib.sha256(
        json.dumps(source, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    monkeypatch.setattr(
        core.subprocess,
        "run",
        lambda *args, **kwargs: type("Result", (), {"returncode": 0, "stdout": json.dumps(source)})(),
    )
    monkeypatch.setattr(module, "_core", lambda: core)

    result = module.run_isolated_replay(
        ues_project=tmp_path,
        input_path=input_path,
        p2_candidate_path=candidate_path,
    )

    assert result["p2_identity"] == {
        "candidate_id": module.P2_CANDIDATE_ID,
        "config_sha256": module.P2_CONFIG_SHA256,
    }
