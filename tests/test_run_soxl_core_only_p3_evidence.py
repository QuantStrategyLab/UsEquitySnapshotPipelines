from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_soxl_core_only_p3_evidence.py"


def _module():
    spec = importlib.util.spec_from_file_location("run_soxl_core_only_p3_evidence", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_offline_facade_binds_one_temp_replay_input_per_fixed_request(monkeypatch, tmp_path) -> None:
    module = _module()
    materialized = {"materialized": "verified"}
    plan = {"plan": "fixed"}
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(module, "materialize_soxl_core_only_p3_input", lambda **kwargs: materialized)
    monkeypatch.setattr(module, "build_soxl_core_only_p3_evidence_plan", lambda value: plan)

    def fake_summary(*, materialized, evidence_plan, replay_executor):
        assert materialized == {"materialized": "verified"}
        assert evidence_plan == {"plan": "fixed"}
        first = replay_executor({"session_dates": ["2026-08-20"], "cost_bps": 5})
        second = replay_executor({"session_dates": ["2026-08-21"], "cost_bps": 15})
        return {"schema_version": "summary.v1", "status": "SUCCESS", "runs": [first, second]}

    monkeypatch.setattr(module, "build_soxl_core_only_p3_evidence_summary", fake_summary)

    def isolated_replay(**kwargs):
        input_path = kwargs["input_path"]
        assert input_path.is_file()
        calls.append(
            {
                "ues_project": kwargs["ues_project"],
                "p2_candidate_path": kwargs["p2_candidate_path"],
                "payload": json.loads(input_path.read_text(encoding="utf-8")),
            }
        )
        return {"isolated": "result"}

    result = module.run_soxl_core_only_p3_offline_evidence(
        binding={"binding": "verified"},
        manifest={"manifest": "verified"},
        member_bytes=b"{}",
        ues_project=tmp_path / "ues",
        p2_candidate_path=tmp_path / "candidate.json",
        isolated_replay=isolated_replay,
    )

    assert result["status"] == "SUCCESS"
    assert [call["payload"]["cost_bps"] for call in calls] == [5, 15]
    assert all(call["ues_project"] == tmp_path / "ues" for call in calls)
    assert all(call["p2_candidate_path"] == tmp_path / "candidate.json" for call in calls)


def test_cli_parks_invalid_paths_without_echoing_them(capsys, tmp_path) -> None:
    module = _module()
    absent = tmp_path / "private-bars.json"

    assert module.main(
        [
            "--p1-binding",
            str(absent),
            "--input-manifest",
            str(absent),
            "--bars-member",
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
