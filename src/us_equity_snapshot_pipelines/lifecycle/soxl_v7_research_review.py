"""Admit the frozen V7 checkpoint to research review, never to execution.

The existing evaluator runs here; callers cannot supply a PASS flag or a
summary in place of its assured input and isolated replay. The wire payload is
the existing QPK ResearchPromotionTicket format, consumed in a separate pinned
control runtime so the original V7 replay dependencies remain unchanged.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import exchange_calendars as xcals
import pandas as pd

from . import soxl_core_only_v7_forward_confirmation_p4_evidence as p4
from .soxl_core_only_p2_v7_longterm_compounding_cash_reserve_contract import (
    P2_V7_LONGTERM_COMPOUNDING_CASH_RESERVE_CONTRACT as P2,
)
from .soxl_core_only_p4_v7_forward_confirmation_contract import P4_V7_FORWARD_CONFIRMATION_CONTRACT as P4
from .soxl_v7_nonlive_forward_observation import build_soxl_v7_forward_control_plane_source


def evaluate_soxl_v7_research_review(
    *, record: Mapping[str, Any], materialized: Mapping[str, Any], policy: object,
    replay_executor: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Return (financial evidence, admitted ticket); waiting/rejection never admits."""
    build_soxl_v7_forward_control_plane_source(record, generated_at=record["observed_at"])
    if record["controller"]["state"] != "FORWARD_COMPLETE_HUMAN_REVIEW":
        return None, None
    plan = p4.build_soxl_core_only_v7_forward_confirmation_p4_evidence_plan(materialized, policy=policy)
    if (
        plan["p1_identity"]["input_manifest_sha256"] != record["p1_manifest_sha256"]
        or any(request["session_dates"] != record["observation_sessions"] for request in plan["requests"])
        or pd.Timestamp(record["observed_at"]) < xcals.get_calendar("XNYS").session_close(
            pd.Timestamp(record["last_observed_session"])
        )
    ):
        raise ValueError("V7 forward record and financial input must match")
    summary = p4.build_soxl_core_only_v7_forward_confirmation_p4_evidence_summary(
        materialized=materialized, evidence_plan=plan, replay_executor=replay_executor, policy=policy,
    )
    if summary["forward_confirmation_policy"]["forward_confirmation_satisfied"] is not True:
        return summary, None
    # Identity uses the frozen candidate and checkpoint, never a timestamp or
    # refreshed provider data. A rerun cannot create another human review ticket.
    ticket_id = "rpt_" + p4._sha256({"candidate": P2.candidate_id, "policy": P4.policy_config_sha256})
    notes = [
        "frozen_v7_forward_financial_gates_passed_research_only",
        f"forward_record_sha256={record['record_sha256']}",
        f"financial_evidence_sha256={summary['evidence_summary_sha256']}",
        f"p1_manifest_sha256={record['p1_manifest_sha256']}",
        f"p4_policy_sha256={P4.policy_config_sha256}",
        f"baseline_p3_evidence_sha256={P4.baseline_p3_evidence_summary_sha256}",
    ]
    lines = [
        "SOXL V7：冻结的首个 252 交易日窗口已完成，5/10/15 bps 成本下金融评价通过。",
        "这是非实盘 Shadow 和模拟 Paper 观察，不是券商模拟成交或 paired shadow。",
        "未进行漂移触发或新一轮调参；保持原候选、数据来源和风险政策。",
        "接受仅记录人工研究意图；不会启用策略、改变仓位或授予交易权限。",
    ]
    for run in summary["runs"]:
        metrics, benchmark = run["metrics"], run["benchmark"]
        lines.append(
            f"成本 {run['cost_bps']} bps：最大回撤 {metrics['max_drawdown']:.2%}，"
            f"SOXX {benchmark['max_drawdown']:.2%}；Calmar {metrics['calmar']}，"
            f"SOXX {benchmark['calmar']}。"
        )
    ticket = {
        "ticket_id": ticket_id, "strategy_profile": P2.candidate_id, "domain": "us_equity",
        "state": "awaiting_human", "drift_status": "not_applicable", "drift_score": 0.0,
        "created_at": record["observed_at"], "updated_at": record["observed_at"],
        "budget": {"allow_live_enablement": False, "max_search_iterations": 0},
        "proposed_params": {"candidate_id": P2.candidate_id, "config_sha256": P2.config_sha256},
        "search_iterations": 0, "shadow_evidence_kind": "v7_nonlive_shadow_and_simulated_paper",
        "shadow_passed": True, "notification_subject": "SOXL V7 固定窗口金融评价：待人工研究复核",
        "notification_body": "\n".join(lines), "notes": notes, "live_authority_granted": False,
    }
    return summary, ticket
