"""Opt-in actual QRT Worker + QPK bridge. All market evidence is synthetic.

QRT_WORKER_PATH must identify an explicit local checkout; no checkout/network
or production credentials are discovered by the test.
"""
import io
import json
import os
from pathlib import Path
import subprocess
from urllib.error import HTTPError
from urllib.parse import urlsplit
import urllib.request

import pytest

cycle = pytest.importorskip("quant_platform_kit.strategy_lifecycle.research_promotion_cycle")
from scripts.run_soxl_v7_review_delivery import _console_request, deliver_once  # noqa: E402
from test_soxl_v7_review_delivery import Store  # noqa: E402
from test_soxl_v7_research_review import complete_record, materialized, _base_summary, _policy  # noqa: E402,F401
from us_equity_snapshot_pipelines.lifecycle import soxl_core_only_v7_forward_confirmation_p4_evidence as p4  # noqa: E402
from us_equity_snapshot_pipelines.lifecycle.soxl_v7_research_review import evaluate_soxl_v7_research_review  # noqa: E402

pytestmark = pytest.mark.skipif(not os.environ.get("QRT_WORKER_PATH"), reason="explicit local QRT checkout required")


@pytest.mark.parametrize("decision", ["accept", "reject"])
def test_same_candidate_financial_to_worker_to_durable_decision(monkeypatch, complete_record, decision):  # noqa: F811
    inputs = materialized(complete_record)
    plan = p4.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(inputs, policy=_policy())
    # Replace only expensive frozen replay output; use the real forward record
    # validator, financial benchmark/gates, ticket builder, Worker and QPK paths.
    monkeypatch.setattr(p4, "_build_base_summary", lambda **_: _base_summary(plan, rejected=False))
    _, prepared = evaluate_soxl_v7_research_review(
        record=complete_record, materialized=inputs, policy=_policy(), replay_executor=lambda _: {},
    )
    proc = subprocess.Popen(
        [os.environ.get("NODE_BINARY", "node"), str(Path(__file__).with_name("v7_review_console_bridge.mjs"))],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    requests = []

    def request(path, method="GET", payload=None, admin=False):
        requests.append((method, path))
        proc.stdin.write(json.dumps(dict(path=path, method=method, payload=payload, admin=admin)) + "\n")
        proc.stdin.flush()
        line = proc.stdout.readline()
        assert line, proc.stderr.read()
        result = json.loads(line)
        if result["status"] >= 400:
            raise HTTPError(path, result["status"], "synthetic worker response", {}, None)
        return result["body"]

    def open_request(req, timeout):
        assert req.get_header("User-agent") == "UsEquitySnapshotPipelines-V7Review/1.0"
        url = urlsplit(req.full_url)
        path = url.path + ("?" + url.query if url.query else "")
        body = request(path, req.get_method(), json.loads(req.data) if req.data else None)
        response = io.BytesIO(json.dumps(body).encode())
        response.status = 200
        return response

    monkeypatch.setattr(urllib.request, "urlopen", open_request)
    try:
        pull = cycle.make_console_research_promotion_pull(
            endpoint_url="https://switch.example/api/internal/research-promotion-ticket", sync_token="synthetic",
            raise_on_unavailable=True,
            get_json=_console_request,
        )
        sync = cycle.make_console_research_promotion_sync(
            endpoint_url="https://switch.example/api/internal/sync-research-promotion-ticket", sync_token="synthetic",
            pull_console=pull,
            post_json=_console_request,
        )
        store = Store()
        assert deliver_once(store=store, prepared=prepared, pull=pull, sync=sync) == "AWAITING_HUMAN"
        assert pull(prepared["ticket_id"])["notification_body"] == prepared["notification_body"]
        payload = {
            "ticket_id": prepared["ticket_id"], "decision": decision,
            "expected_proposed_params": prepared["proposed_params"],
            "expected_strategy_profile": prepared["strategy_profile"], "expected_domain": prepared["domain"],
            "confirmation": {"target_platform": "ibkr", "execution_mode": "live", "risk_profile": "CAPITAL_PRESERVATION"},
        }
        result = request("/api/research-promotion-decisions", "POST", payload, admin=True)
        assert result["live_authority_granted"] is False
        state = "HUMAN_ACCEPTED" if decision == "accept" else "HUMAN_REJECTED"
        assert deliver_once(store=store, prepared=None, pull=pull, sync=sync) == state
        assert deliver_once(store=store, prepared=None, pull=lambda _: pytest.fail(), sync=lambda _: pytest.fail()) == state
        assert store.read("terminal.json")["live_authority_granted"] is False
        assert sum(method == "POST" and path.endswith("sync-research-promotion-ticket") for method, path in requests) == 1
    finally:
        proc.stdin.close()
        proc.wait(timeout=10)
        assert proc.returncode == 0, proc.stderr.read()
