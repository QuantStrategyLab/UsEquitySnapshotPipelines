from __future__ import annotations

import hashlib
import json
import runpy
import sys
from pathlib import Path

import pytest

from us_equity_snapshot_pipelines.lifecycle import soxl_isolated_research as research


def _scenario(cost: float) -> dict[str, object]:
    return {
        "total_cost_bps": cost,
        "total_return": 0.04,
        "cagr": 0.01,
        "sharpe_ratio": 0.2,
        "sortino_ratio": 0.3,
        "max_drawdown": -0.08,
        "turnover": 1.5,
        "costs_paid": cost * 2.0,
        "trade_count": 4,
        "risk_assessment_count": 5,
    }


def _document() -> dict[str, object]:
    trials = [
        {
            "status": "completed",
            "trend_entry_buffer": value,
            "cost_scenarios": [_scenario(cost) for cost in (5.0, 10.0, 15.0)],
        }
        for value in (0.08, 0.10, 0.12)
    ]
    return {
        "result": {
            "status": "development_completed",
            "failure_reason": None,
            "trial_count_requested": 3,
            "trial_count_started": 3,
            "trial_count_completed": 3,
            "cost_scenario_count_started": 9,
            "cost_scenario_count_completed": 9,
            "trials": trials,
            "window_class": "seen_development",
            "cost_scenarios_bps": [5.0, 10.0, 15.0],
            "loss_budget": 0.01,
            "stop_loss_distance": 0.05,
            "effective_exposure_cap": 0.50,
            "soxl_nominal_cap": 0.15,
            "account_drawdown_breaker": 0.10,
            "strategy_stop_breaker_count": 3,
            "learning_only": True,
            "promotion_eligible": False,
            "live_ready": False,
            "size_zero_required": True,
            "no_order": True,
            "real_backtest_executed": True,
        },
        "verification": {
            "wrapper_status": "entrypoint_returned",
            "entrypoint_exit_code": 0,
            "filevault_enabled": True,
            "original_tree_metadata_unchanged": True,
            "network_disabled": True,
        },
    }


def _write(tmp_path: Path, document: dict[str, object]) -> tuple[Path, str]:
    raw = json.dumps(document, allow_nan=False).encode()
    path = tmp_path / "completed.json"
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest()


def test_equal_completed_trials_return_hold_proposal() -> None:
    proposal = research.evaluate_soxl_completed_research_result(_document())

    assert proposal.recommendation == "hold"
    assert proposal.strategy_profile == "soxl_soxx_trend_income_isolated_learning"
    assert proposal.current_params == proposal.proposed_params == {
        "trend_entry_buffer": 0.08
    }
    assert proposal.search_iterations == 3
    assert proposal.walk_forward_passed is False
    assert proposal.current_metrics is proposal.proposed_metrics is None


def test_different_aggregate_requires_review_without_proposing_change() -> None:
    document = _document()
    document["result"]["trials"][1]["cost_scenarios"][0]["total_return"] = 0.05

    proposal = research.evaluate_soxl_completed_research_result(document)

    assert proposal.recommendation == "requires_review"
    assert proposal.improvement_score == 0.0
    assert proposal.current_params == proposal.proposed_params == {
        "trend_entry_buffer": 0.08
    }


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d["result"].update(status="development_incomplete"),
        lambda d: d["result"].update(trial_count_completed=2),
        lambda d: d["result"].update(cost_scenario_count_started=8),
        lambda d: d["result"]["trials"].pop(),
        lambda d: d["result"]["trials"][1].update(trend_entry_buffer=0.08),
        lambda d: d["result"]["trials"][1].update(trend_entry_buffer=0.13),
        lambda d: d["result"]["trials"][0]["cost_scenarios"].pop(),
        lambda d: d["result"]["trials"][0]["cost_scenarios"][1].update(
            total_cost_bps=5.0
        ),
        lambda d: d["result"]["trials"][0]["cost_scenarios"][0].update(
            total_return=float("nan")
        ),
        lambda d: d["result"]["trials"][0]["cost_scenarios"][0].update(
            trade_count=True
        ),
        lambda d: d["result"].update(learning_only=False),
        lambda d: d["result"].update(promotion_eligible=True),
        lambda d: d["verification"].update(network_disabled=False),
        lambda d: d["verification"].update(filevault_enabled=1),
        lambda d: d["verification"].update(network_disabled=1),
        lambda d: d["verification"].update(entrypoint_exit_code=False),
    ],
)
def test_completed_result_rejects_incomplete_or_unsafe_documents(mutation) -> None:
    document = _document()
    mutation(document)
    with pytest.raises(research.SoxlIsolatedResearchError, match="completed_result_invalid"):
        research.evaluate_soxl_completed_research_result(document)


def test_completed_result_reader_rejects_bad_hash_symlink_and_empty_file(tmp_path: Path) -> None:
    path, digest = _write(tmp_path, _document())
    with pytest.raises(research.SoxlIsolatedResearchError, match="completed_result_invalid"):
        research.load_soxl_completed_research_result(path, expected_sha256="0" * 64)

    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(research.SoxlIsolatedResearchError, match="completed_result_invalid"):
        research.load_soxl_completed_research_result(link, expected_sha256=digest)

    empty = tmp_path / "empty.json"
    empty.write_bytes(b"")
    with pytest.raises(research.SoxlIsolatedResearchError, match="completed_result_invalid"):
        research.load_soxl_completed_research_result(
            empty, expected_sha256=hashlib.sha256(b"").hexdigest()
        )


def test_completed_result_cli_is_result_only_and_emits_whitelist(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    path, digest = _write(tmp_path, _document())

    def forbidden(*_args, **_kwargs):
        raise AssertionError("development and preflight must not run")

    monkeypatch.setattr(research, "preflight_soxl_isolated_research", forbidden)
    monkeypatch.setattr(research, "run_soxl_isolated_development", forbidden)
    script = Path(__file__).parents[1] / "scripts/preflight_soxl_isolated_research.py"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--completed-result",
            str(path),
            "--completed-result-sha256",
            digest,
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        runpy.run_path(str(script), run_name="__main__")
    assert stopped.value.code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "completed"
    assert output["outcome"] == "no_improvement"
    assert output["proposal"]["recommendation"] == "hold"
    assert output["proposal"]["walk_forward_passed"] is False
    assert output["proposal"]["search_iterations"] == 3
    assert output["learning_only"] is True
    assert output["promotion_eligible"] is False
    assert output["live_ready"] is False
    assert output["real_backtest_executed"] is False
    assert output["completed_result_reused"] is True
    assert not {"path", "verification", "trials", "metrics"} & set(output)


def test_completed_result_cli_sanitizes_invalid_input(tmp_path: Path, monkeypatch, capsys) -> None:
    path = tmp_path / "bad.json"
    path.write_text("not-json")
    script = Path(__file__).parents[1] / "scripts/preflight_soxl_isolated_research.py"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--completed-result",
            str(path),
            "--completed-result-sha256",
            hashlib.sha256(b"not-json").hexdigest(),
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        runpy.run_path(str(script), run_name="__main__")
    assert stopped.value.code == 3
    assert json.loads(capsys.readouterr().out) == {
        "status": "unavailable",
        "reason": "completed_result_invalid",
    }


def test_completed_result_cli_marks_differences_for_review_without_promotion(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    document = _document()
    document["result"]["trials"][2]["cost_scenarios"][2]["sharpe_ratio"] = 0.21
    path, digest = _write(tmp_path, document)
    script = Path(__file__).parents[1] / "scripts/preflight_soxl_isolated_research.py"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script),
            "--completed-result",
            str(path),
            "--completed-result-sha256",
            digest,
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        runpy.run_path(str(script), run_name="__main__")
    assert stopped.value.code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["outcome"] == "requires_review"
    assert output["proposal"]["recommendation"] == "requires_review"
    assert output["proposal"]["current_params"] == output["proposal"]["proposed_params"]
    assert output["promotion_eligible"] is False
    assert output["real_backtest_executed"] is False
    assert output["completed_result_reused"] is True
