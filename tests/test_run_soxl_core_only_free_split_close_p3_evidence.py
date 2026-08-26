from __future__ import annotations

import importlib.util
import json
from pathlib import Path

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
