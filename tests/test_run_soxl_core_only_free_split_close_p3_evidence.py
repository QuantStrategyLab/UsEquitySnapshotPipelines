from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from us_equity_snapshot_pipelines.lifecycle.soxl_core_only_v7_forward_confirmation_p4_evidence import (
    SoxlCoreOnlyV7ForwardConfirmationP4WindowIncomplete,
)

SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_free_split_close_p3_evidence.py"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_free_split_close_p3_evidence", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_v4_offline_facade_binds_two_p1_members_and_one_temp_replay_per_request(monkeypatch, tmp_path) -> None:
    module = _module()
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(module, "materialize_soxl_core_only_free_split_close_p3_input", lambda **kwargs: {"v4": "ok"})
    monkeypatch.setattr(module, "build_soxl_core_only_free_split_close_p3_evidence_plan", lambda value: {"plan": "v4"})

    def summary(*, materialized, evidence_plan, replay_executor):
        assert materialized == {"v4": "ok"}
        assert evidence_plan == {"plan": "v4"}
        return {"status": "SUCCESS", "runs": [replay_executor({"cost_bps": 5})]}

    monkeypatch.setattr(module, "build_soxl_core_only_free_split_close_p3_evidence_summary", summary)

    def isolated_replay(**kwargs):
        path = kwargs["input_path"]
        calls.append(
            {
                "ues_project": kwargs["ues_project"],
                "p2_candidate_path": kwargs["p2_candidate_path"],
                "payload": json.loads(path.read_text(encoding="utf-8")),
            }
        )
        return {"isolated": "result"}

    result = module.run_soxl_core_only_free_split_close_p3_offline_evidence(
        binding={"binding": "v4"},
        manifest={"manifest": "v4"},
        closes_bytes=b"closes",
        assurance_bytes=b"assurance",
        ues_project=tmp_path / "ues",
        p2_candidate_path=tmp_path / "candidate.json",
        isolated_replay=isolated_replay,
    )

    assert result["status"] == "SUCCESS"
    assert calls == [
        {
            "ues_project": tmp_path / "ues",
            "p2_candidate_path": tmp_path / "candidate.json",
            "payload": {"cost_bps": 5},
        }
    ]


def test_v6_offline_facade_uses_a_distinct_candidate_contract(monkeypatch, tmp_path) -> None:
    module = _module()
    observed: dict[str, object] = {}

    def materialize(**kwargs):
        observed["contract"] = kwargs["p2_contract"]
        return {"v6": "ok"}

    monkeypatch.setattr(module, "materialize_soxl_core_only_free_split_close_p3_input", materialize)
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v6_longterm_compounding_p3_evidence_plan",
        lambda value: {"plan": "v6"},
    )
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v6_longterm_compounding_p3_evidence_summary",
        lambda **kwargs: {"status": "SUCCESS", "profile": kwargs["evidence_plan"]["plan"]},
    )

    result = module.run_soxl_core_only_free_split_close_p3_offline_evidence(
        binding={"binding": "v6"},
        manifest={"manifest": "v6"},
        closes_bytes=b"closes",
        assurance_bytes=b"assurance",
        ues_project=tmp_path / "ues",
        p2_candidate_path=tmp_path / "candidate.json",
        isolated_replay=lambda **_kwargs: {"isolated": "result"},
        p2_profile="v6_longterm_compounding",
    )

    assert observed["contract"] == module.P2_V6_LONGTERM_COMPOUNDING_CONTRACT
    assert result == {"status": "SUCCESS", "profile": "v6"}


def test_v7_offline_facade_uses_the_cash_reserve_candidate_contract(monkeypatch, tmp_path) -> None:
    module = _module()
    observed: dict[str, object] = {}

    def materialize(**kwargs):
        observed["contract"] = kwargs["p2_contract"]
        return {"v7": "ok"}

    monkeypatch.setattr(module, "materialize_soxl_core_only_free_split_close_p3_input", materialize)
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan",
        lambda value: {"plan": "v7"},
    )
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary",
        lambda **kwargs: {"status": "SUCCESS", "profile": kwargs["evidence_plan"]["plan"]},
    )

    result = module.run_soxl_core_only_free_split_close_p3_offline_evidence(
        binding={"binding": "v7"},
        manifest={"manifest": "v7"},
        closes_bytes=b"closes",
        assurance_bytes=b"assurance",
        ues_project=tmp_path / "ues",
        p2_candidate_path=tmp_path / "candidate.json",
        isolated_replay=lambda **_kwargs: {"isolated": "result"},
        p2_profile="v7_longterm_compounding_cash_reserve",
    )

    assert observed["contract"] == module.P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT
    assert result == {"status": "SUCCESS", "profile": "v7"}


def test_v7_facade_can_create_only_a_caller_selected_private_risk_observation(monkeypatch, tmp_path) -> None:
    module = _module()
    output = tmp_path / "private-risk-observation.json"
    monkeypatch.setattr(module, "materialize_soxl_core_only_free_split_close_p3_input", lambda **_kwargs: {"v7": "ok"})
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_plan",
        lambda _value: {"plan": "v7"},
    )
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_longterm_compounding_cash_reserve_p3_evidence_summary",
        lambda **_kwargs: {"status": "SUCCESS", "profile": "v7"},
    )
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_long_horizon_risk_observation",
        lambda **kwargs: {
            "schema": "qsl.long_horizon_risk_observation.v1",
            "summary": kwargs["evidence_summary"],
            "raw_return_paths": "private-only",
        },
    )

    result = module.run_soxl_core_only_free_split_close_p3_offline_evidence(
        binding={"binding": "v7"},
        manifest={"manifest": "v7"},
        closes_bytes=b"closes",
        assurance_bytes=b"assurance",
        ues_project=tmp_path / "ues",
        p2_candidate_path=tmp_path / "candidate.json",
        isolated_replay=lambda **_kwargs: {"isolated": "result"},
        p2_profile="v7_longterm_compounding_cash_reserve",
        risk_observation_output=output,
    )

    assert result == {"status": "SUCCESS", "profile": "v7"}
    assert json.loads(output.read_text(encoding="utf-8"))["raw_return_paths"] == "private-only"
    assert output.stat().st_mode & 0o777 == 0o600


def test_v7_forward_confirmation_facade_requires_and_binds_the_frozen_policy(monkeypatch, tmp_path) -> None:
    module = _module()
    observed: dict[str, object] = {}
    policy = {"p4": "frozen"}

    def materialize(**kwargs):
        observed["contract"] = kwargs["p2_contract"]
        return {"v7": "forward"}

    monkeypatch.setattr(module, "materialize_soxl_core_only_free_split_close_p3_input", materialize)
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan",
        lambda value, *, policy: observed.setdefault("plan_policy", policy) and {"plan": "p4"},
    )
    monkeypatch.setattr(
        module,
        "build_soxl_core_only_v7_forward_confirmation_p4_evidence_summary",
        lambda **kwargs: {"status": "SUCCESS", "policy": kwargs["policy"]},
    )

    result = module.run_soxl_core_only_free_split_close_p3_offline_evidence(
        binding={"binding": "v7"},
        manifest={"manifest": "v7"},
        closes_bytes=b"closes",
        assurance_bytes=b"assurance",
        ues_project=tmp_path / "ues",
        p2_candidate_path=tmp_path / "candidate.json",
        isolated_replay=lambda **_kwargs: {"isolated": "result"},
        p2_profile="v7_forward_confirmation",
        p4_policy=policy,
    )

    assert observed["contract"] == module.P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT
    assert observed["plan_policy"] == policy
    assert result == {"status": "SUCCESS", "policy": policy}


def test_v4_cli_parks_invalid_paths_without_echoing_them(capsys, tmp_path) -> None:
    module = _module()
    absent = tmp_path / "private-closes.json"

    assert module.main(
        [
            "--p1-binding",
            str(absent),
            "--input-manifest",
            str(absent),
            "--closes-member",
            str(absent),
            "--assurance-member",
            str(absent),
            "--ues-project",
            str(absent),
            "--p2-candidate",
            str(absent),
        ]
    ) == 0

    output = json.loads(capsys.readouterr().out)
    assert output == {
        "failure_class": "p1_p2_p3_contract_or_runtime_unavailable",
        "schema_version": module.RUN_SCHEMA,
        "status": "PARKED",
    }
    assert str(absent) not in json.dumps(output)


def test_cli_reports_an_incomplete_p4_window_as_expected_waiting(monkeypatch, capsys, tmp_path) -> None:
    module = _module()
    path = tmp_path / "input.json"
    args = SimpleNamespace(
        p1_binding=path,
        input_manifest=path,
        closes_member=path,
        assurance_member=path,
        ues_project=path,
        p2_candidate=path,
        p2_profile="v7_forward_confirmation",
        p4_policy=path,
        risk_observation_output=None,
    )
    monkeypatch.setattr(module, "_arguments", lambda _argv: args)
    monkeypatch.setattr(module, "_read_json", lambda _path: {})
    monkeypatch.setattr(module, "_read_member", lambda _path: b"{}")
    monkeypatch.setattr(module, "_load_isolated_replay", lambda **_kwargs: lambda **_inner: {})
    monkeypatch.setattr(
        module,
        "run_soxl_core_only_free_split_close_p3_offline_evidence",
        lambda **_kwargs: (_ for _ in ()).throw(SoxlCoreOnlyV7ForwardConfirmationP4WindowIncomplete("waiting")),
    )

    assert module.main([]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "failure_class": "p4_forward_window_not_complete",
        "schema_version": module.RUN_SCHEMA,
        "status": "PARKED",
    }
