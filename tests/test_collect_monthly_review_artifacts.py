from __future__ import annotations

import importlib.util
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/collect_monthly_review_artifacts.py"
NOW = datetime(2026, 5, 5, tzinfo=UTC)
ETF = "global_etf_rotation"
RUSSELL = "russell_top50_leader_rotation"


def load():
    spec = importlib.util.spec_from_file_location("collector", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(number, date="2026-05-04T00:00:00Z", **kwargs):
    return dict(id=number, created_at=date, head_branch="main", status="completed",
                conclusion="success", path=".github/workflows/publish-snapshot-artifacts.yml", **kwargs)


def artifact(profile, number, **kwargs):
    return dict(id=number + 100, name=f"us-equity-snapshot-{profile}-{number}", expired=False, **kwargs)


class FakeGH:
    def __init__(self, runs, artifacts):
        self.runs = runs
        self.artifacts = artifacts
        self.downloads = []

    def pages(self, endpoint, key):
        if key == "workflow_runs":
            return self.runs
        return self.artifacts[int(endpoint.split("/runs/")[1].split("/")[0])]

    def download(self, run_id, name, target):
        self.downloads.append((run_id, name, target))
        target.mkdir()
        if name.startswith("us-equity-snapshot-"):
            profile = name[len("us-equity-snapshot-"):].rsplit("-", 1)[0]
            for filename in (f"{profile}_feature_snapshot_latest.csv",
                             f"{profile}_feature_snapshot_latest.csv.manifest.json",
                             f"{profile}_ranking_latest.csv"):
                (target / filename).write_text("fake evidence")
            (target / "release_status_summary.json").write_text(json.dumps({"strategy_profile": profile}))


def collect(tmp_path, gh, **kwargs):
    return load().collect(gh, tmp_path / "inputs", tmp_path / "report", now=NOW, **kwargs)


@pytest.mark.parametrize("trigger", [1, 2, None])
def test_collects_each_latest_profile_independently(tmp_path, trigger):
    gh = FakeGH([run(1), run(2, "2026-05-03T00:00:00Z")],
                {1: [artifact(ETF, 1)], 2: [artifact(RUSSELL, 2)]})
    assert collect(tmp_path, gh, source_run_id=trigger) == 0
    assert [item[0] for item in gh.downloads] == [1, 2]
    report = json.loads((tmp_path / "report/source_coverage.json").read_text())
    assert report["profiles"][RUSSELL]["run_id"] == 2
    assert all(item["status"] == "COLLECTED" for item in report["profiles"].values())


@pytest.mark.parametrize("condition,status", [("missing", "MISSING"), ("expired", "EXPIRED"), ("stale", "STALE")])
def test_missing_expired_stale_block_incomplete_review(tmp_path, condition, status):
    russell = artifact(RUSSELL, 2)
    russell["expired"] = condition == "expired"
    runs = [run(1), run(2, "2026-04-01T00:00:00Z" if condition == "stale" else "2026-05-03T00:00:00Z")]
    gh = FakeGH(runs, {1: [artifact(ETF, 1)], 2: [] if condition == "missing" else [russell]})
    assert collect(tmp_path, gh) == 1
    report = json.loads((tmp_path / "report/source_coverage.json").read_text())
    assert report["profiles"][RUSSELL]["status"] == status
    assert gh.downloads == []


def test_skips_wrong_branch_failed_workflow_and_expired_latest(tmp_path):
    bad = [run(9), run(8), run(7)]
    bad[0]["head_branch"] = "feature/fake"
    bad[1]["conclusion"] = "failure"
    bad[2]["path"] = ".github/workflows/fake.yml"
    expired = artifact(ETF, 3)
    expired["expired"] = True
    gh = FakeGH(bad + [run(3), run(1, "2026-05-03T00:00:00Z")],
                {3: [expired], 1: [artifact(ETF, 1), artifact(RUSSELL, 1)]})
    assert collect(tmp_path, gh) == 0
    assert all(item[0] == 1 for item in gh.downloads)


@pytest.mark.parametrize("field,value", [("expired", None), ("id", "fake-id"), ("name", None)])
def test_invalid_artifact_metadata_blocks(tmp_path, field, value):
    item = artifact(ETF, 1)
    item[field] = value
    gh = FakeGH([run(1)], {1: [item, artifact(RUSSELL, 1)]})
    assert collect(tmp_path, gh) == 1
    assert gh.downloads == []


def test_download_and_api_errors_remain_unknown(tmp_path):
    gh = FakeGH([run(1)], {1: [artifact(ETF, 1), artifact(RUSSELL, 1)]})

    def failure(*args):
        raise RuntimeError("SYNTHETIC_PRIVATE_DIAGNOSTIC")

    gh.download = failure
    assert collect(tmp_path, gh) == 1
    text = (tmp_path / "report/source_coverage.json").read_text()
    assert "SYNTHETIC_PRIVATE_DIAGNOSTIC" not in text
    assert "UNKNOWN" in text


def test_existing_output_blocks_restart_and_concurrent_writer(tmp_path):
    (tmp_path / "inputs").mkdir()
    (tmp_path / "inputs/old.json").write_text("fake prior evidence")
    gh = FakeGH([], {})
    with pytest.raises(FileExistsError):
        collect(tmp_path, gh)
    assert (tmp_path / "inputs/old.json").read_text() == "fake prior evidence"


def test_gh_errors_suppress_untrusted_diagnostics(monkeypatch):
    module = load()

    def failure(*args, **kwargs):
        raise subprocess.CalledProcessError(1, ["gh"], output="SYNTHETIC_PRIVATE_DIAGNOSTIC", stderr="SYNTHETIC_AUTH_DIAGNOSTIC")

    monkeypatch.setattr(module.subprocess, "run", failure)
    with pytest.raises(RuntimeError, match="GitHub read failed") as error:
        module.GitHub("example/fake").pages("actions/runs", "workflow_runs")
    assert "SYNTHETIC_PRIVATE_DIAGNOSTIC" not in str(error.value)


def test_pagination_combines_every_page(monkeypatch):
    module = load()
    calls = []

    def response(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps([
            {"workflow_runs": [run(1)]}, {"workflow_runs": [run(2)]}
        ]), "")

    monkeypatch.setattr(module.subprocess, "run", response)
    assert [item["id"] for item in module.GitHub("example/fake").pages("actions/runs", "workflow_runs")] == [1, 2]
    assert "--paginate" in calls[0]
    assert "--slurp" in calls[0]


def test_trigger_supplemental_artifacts_preserved_without_old_strategy(tmp_path):
    gh = FakeGH([run(1), run(2, "2026-05-03T00:00:00Z")], {
        1: [artifact(ETF, 1), artifact(RUSSELL, 1)],
        2: [artifact(RUSSELL, 2), dict(id=202, name="us-equity-market-history-2-1", expired=False)]})
    assert collect(tmp_path, gh, source_run_id=2) == 0
    assert len(gh.downloads) == 3
    assert gh.downloads[-1][1] == "us-equity-market-history-2-1"
    assert not any(item[1] == artifact(RUSSELL, 2)["name"] for item in gh.downloads)


@pytest.mark.parametrize("failure", ["missing", "empty", "wrong-profile"])
def test_incomplete_or_mismatched_download_blocks(tmp_path, failure):
    gh = FakeGH([run(1)], {1: [artifact(ETF, 1), artifact(RUSSELL, 1)]})
    download = gh.download

    def corrupt(run_id, name, target):
        download(run_id, name, target)
        if failure == "missing":
            (target / "release_status_summary.json").unlink()
        elif failure == "empty":
            (target / f"{ETF}_ranking_latest.csv").write_text("")
        else:
            (target / "release_status_summary.json").write_text('{"strategy_profile": "fake-profile"}')

    gh.download = corrupt
    assert collect(tmp_path, gh) == 1
    report = json.loads((tmp_path / "report/source_coverage.json").read_text())
    assert report["status"] == "UNKNOWN"
    assert report["profiles"][ETF]["status"] == "UNKNOWN"


def test_api_failure_never_becomes_missing_or_pass(tmp_path):
    gh = FakeGH([], {})

    def failure(*args):
        raise RuntimeError("SYNTHETIC_API_DIAGNOSTIC")

    gh.pages = failure
    assert collect(tmp_path, gh) == 1
    text = (tmp_path / "report/source_coverage.json").read_text()
    report = json.loads(text)
    assert report["status"] == "UNKNOWN"
    assert all(item["status"] == "UNKNOWN" for item in report["profiles"].values())
    assert "SYNTHETIC_API_DIAGNOSTIC" not in text


def test_newest_run_selected_even_when_api_order_differs(tmp_path):
    gh = FakeGH([run(1, "2026-05-02T00:00:00Z"), run(2)], {
        1: [artifact(ETF, 1), artifact(RUSSELL, 1)],
        2: [artifact(ETF, 2), artifact(RUSSELL, 2)]})
    assert collect(tmp_path, gh) == 0
    assert all(item[0] == 2 for item in gh.downloads)


def test_future_timestamp_and_invalid_trigger_block(tmp_path):
    gh = FakeGH([run(1, "2026-05-06T00:00:00Z")], {1: [artifact(ETF, 1), artifact(RUSSELL, 1)]})
    assert collect(tmp_path, gh, source_run_id=999) == 1
    assert gh.downloads == []
