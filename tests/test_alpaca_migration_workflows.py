"""Retired acquisition must stop before credentials, fetches, or publications."""

import os
import re
import subprocess
from pathlib import Path

import pytest


WORKFLOWS = (
    "alpaca-sip-access-diagnostic",
    "batch-a-v2-alpaca-sip-inputs",
    "global-etf-learning-inputs",
    "qqqm-boxx-historical-sip-cause",
    "qqqm-boxx-raw-historical-inputs",
    "r9-raw-temporal-extension",
    "research-input-pilot",
    "rs04-cloud-archive",
    "russell-core-research-case",
    "soxl-p1-p3-daily-research",
    "tqqq-p1-p3-daily-research",
    "tqqq-p1-p3-one-shot",
    "tqqq-v7-p1-p3-manual-research",
)


def _jobs(name):
    workflow = Path(f".github/workflows/{name}.yml").read_text()
    jobs = dict(re.findall(r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)",
                           workflow.split("jobs:\n", 1)[1], flags=re.M | re.S))
    return workflow, jobs


@pytest.mark.parametrize("name", WORKFLOWS)
def test_acquisition_and_dependent_jobs_cannot_run(name):
    workflow, jobs = _jobs(name)
    assert "secrets.ALPACA_API_KEY_ID" not in workflow
    assert "secrets.ALPACA_API_SECRET_KEY" not in workflow
    notice = jobs.pop("alpaca-migration")
    assert "    permissions: {}\n" in notice
    assert "environment:" not in notice
    assert "uses:" not in notice
    assert "secrets." not in notice
    assert "needs:" not in notice
    assert jobs
    for job_name, body in jobs.items():
        conditions = re.findall(r"^    if: (.+)$", body, flags=re.M)
        if name == "rs04-cloud-archive":
            assert job_name == "archive"
            assert conditions == [
                "inputs.mode == 'inventory' && (github.ref == format('refs/heads/{0}', "
                "github.event.repository.default_branch) && github.run_attempt == 1)"
            ]
            assert "    if: inputs.mode != 'inventory'\n" in notice
            assert "if: inputs.mode == 'regenerate'" in body
        else:
            assert conditions == ["${{ false }}"]
            assert "    if:" not in notice


@pytest.mark.parametrize("name", WORKFLOWS)
def test_migration_notice_fails_with_a_summary_without_external_calls(name, tmp_path):
    _, jobs = _jobs(name)
    script = jobs["alpaca-migration"].split("        run: |\n", 1)[1]
    script = "\n".join(line.removeprefix("          ") for line in script.splitlines())
    summary = tmp_path / "summary.md"
    # Deliberately provide no inherited credentials or cloud environment.
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-c", script],
        env={"PATH": os.defpath, "GITHUB_STEP_SUMMARY": str(summary)},
        text=True, capture_output=True, check=False,
    )
    assert result.returncode == 1
    assert result.stderr == ""
    assert "::error::ALPACA_ACQUISITION_MOVED:" in result.stdout
    assert "QuantStrategyLab/QuantPlatformKit" in summary.read_text()
    assert "no fetch, replay, or publication was attempted" in summary.read_text()


def test_no_workflow_resolves_retired_alpaca_secrets():
    for workflow in Path(".github/workflows").glob("*.yml"):
        assert "secrets.ALPACA_API_KEY_ID" not in workflow.read_text(), workflow
        assert "secrets.ALPACA_API_SECRET_KEY" not in workflow.read_text(), workflow
