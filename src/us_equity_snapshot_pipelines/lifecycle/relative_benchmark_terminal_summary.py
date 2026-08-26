"""Publishable, no-market-data summaries for relative-benchmark evidence."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

_SCHEMA = "qsl.relative-benchmark-terminal-summary.v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_ID = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_VERDICTS = frozenset(
    {
        "PASS_PENDING_FORWARD_CONFIRMATION",
        "REJECT_NEGATIVE_STRATEGY_EVIDENCE",
        "INCONCLUSIVE_DATA_OR_EXECUTION",
    }
)
_CLAIMS = {
    "authority_scope": "RESEARCH_ONLY",
    "learning_only": True,
    "promotion_eligible": False,
    "live_ready": False,
    "size_zero_required": True,
    "no_order": True,
}
_FIELDS = frozenset(
    {
        "schema_version",
        "candidate",
        "input_manifest_sha256",
        "p3",
        "benchmark",
        "policy",
        "cost_scenarios",
        "lifecycle_claims",
    }
)


class RelativeBenchmarkTerminalSummaryError(ValueError):
    """Reject a summary that could leak raw market or account material."""


def _fail() -> None:
    raise RelativeBenchmarkTerminalSummaryError("invalid relative-benchmark terminal summary")


def _mapping(value: object, fields: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail()
    return dict(value)


def _digest(value: object) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        _fail()
    return value


def _candidate(value: object) -> dict[str, str]:
    candidate = _mapping(value, frozenset({"candidate_id", "config_sha256"}))
    candidate_id = candidate["candidate_id"]
    if not isinstance(candidate_id, str) or not _CANDIDATE_ID.fullmatch(candidate_id):
        _fail()
    return {"candidate_id": candidate_id, "config_sha256": _digest(candidate["config_sha256"])}


def _p3(value: object) -> dict[str, str]:
    p3 = _mapping(
        value,
        frozenset(
            {
                "evidence_sha256",
                "promotion_result_sha256",
                "relative_benchmark_policy_sha256",
                "status",
                "verdict",
            }
        ),
    )
    if p3["status"] != "EVIDENCE_V2_COMPLETE" or p3["verdict"] not in _VERDICTS:
        _fail()
    return {
        "evidence_sha256": _digest(p3["evidence_sha256"]),
        "promotion_result_sha256": _digest(p3["promotion_result_sha256"]),
        "relative_benchmark_policy_sha256": _digest(p3["relative_benchmark_policy_sha256"]),
        "status": "EVIDENCE_V2_COMPLETE",
        "verdict": str(p3["verdict"]),
    }


def _gate(value: object, fields: frozenset[str]) -> dict[str, bool]:
    gate = _mapping(value, fields)
    if not all(type(gate[field]) is bool for field in fields):
        _fail()
    return {field: bool(gate[field]) for field in fields}


def _policy_summary(value: object, *, candidate_id: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    if not isinstance(value, Mapping) or value.get("candidate_id") != candidate_id:
        _fail()
    benchmark_symbol = value.get("benchmark_symbol")
    benchmark_policy = value.get("benchmark_policy")
    if (
        not isinstance(benchmark_symbol, str)
        or not benchmark_symbol
        or not isinstance(benchmark_policy, str)
        or not benchmark_policy
    ):
        _fail()
    boolean_fields = (
        "short_window_drawdown_all_passed",
        "long_window_drawdown_all_passed",
        "long_window_incremental_calmar_all_passed",
        "forward_confirmation_satisfied",
        "automatic_promotion",
    )
    if (
        value.get("evidence_status") != "EVIDENCE_COMPLETE"
        or value.get("strategy_verdict") not in _VERDICTS
        or any(type(value.get(field)) is not bool for field in boolean_fields)
    ):
        _fail()
    scenarios = value.get("scenarios")
    if not isinstance(scenarios, Sequence) or isinstance(scenarios, (str, bytes)) or not scenarios:
        _fail()
    sanitized_scenarios: list[dict[str, object]] = []
    for scenario in scenarios:
        if not isinstance(scenario, Mapping):
            _fail()
        cost = scenario.get("total_cost_bps")
        short_windows = scenario.get("short_windows")
        long_horizon = scenario.get("continuous_long_horizon")
        if (
            type(cost) is not int
            or cost <= 0
            or not isinstance(short_windows, Sequence)
            or isinstance(short_windows, (str, bytes))
            or not short_windows
            or not isinstance(long_horizon, Mapping)
        ):
            _fail()
        short_gates = tuple(
            _gate(
                window.get("gate") if isinstance(window, Mapping) else None,
                frozenset({"max_drawdown_not_exceeding_benchmark", "passed"}),
            )
            for window in short_windows
        )
        long_gate = _gate(
            long_horizon.get("gate"),
            frozenset(
                {
                    "max_drawdown_not_exceeding_benchmark",
                    "incremental_calmar_after_cost",
                    "passed",
                }
            ),
        )
        if long_gate["passed"] is not (
            long_gate["max_drawdown_not_exceeding_benchmark"] and long_gate["incremental_calmar_after_cost"]
        ):
            _fail()
        sanitized_scenarios.append(
            {
                "total_cost_bps": cost,
                "short_window_drawdown_failed_count": sum(not gate["passed"] for gate in short_gates),
                "long_window_drawdown_passed": long_gate["max_drawdown_not_exceeding_benchmark"],
                "long_window_incremental_calmar_passed": long_gate["incremental_calmar_after_cost"],
                "long_window_passed": long_gate["passed"],
            }
        )
    short_drawdown_passed = all(scenario["short_window_drawdown_failed_count"] == 0 for scenario in sanitized_scenarios)
    long_drawdown_passed = all(scenario["long_window_drawdown_passed"] for scenario in sanitized_scenarios)
    long_calmar_passed = all(scenario["long_window_incremental_calmar_passed"] for scenario in sanitized_scenarios)
    if (
        value["short_window_drawdown_all_passed"] is not short_drawdown_passed
        or value["long_window_drawdown_all_passed"] is not long_drawdown_passed
        or value["long_window_incremental_calmar_all_passed"] is not long_calmar_passed
    ):
        _fail()
    expected_verdict = (
        "REJECT_NEGATIVE_STRATEGY_EVIDENCE"
        if not (short_drawdown_passed and long_drawdown_passed and long_calmar_passed)
        else "PASS_PENDING_FORWARD_CONFIRMATION"
    )
    if value["strategy_verdict"] != expected_verdict or value["forward_confirmation_satisfied"]:
        _fail()
    return (
        {
            "benchmark_symbol": benchmark_symbol,
            "benchmark_policy": benchmark_policy,
            "evidence_status": "EVIDENCE_COMPLETE",
            "short_window_drawdown_all_passed": value["short_window_drawdown_all_passed"],
            "long_window_drawdown_all_passed": value["long_window_drawdown_all_passed"],
            "long_window_incremental_calmar_all_passed": value["long_window_incremental_calmar_all_passed"],
            "forward_confirmation_satisfied": value["forward_confirmation_satisfied"],
            "strategy_verdict": value["strategy_verdict"],
            "automatic_promotion": value["automatic_promotion"],
        },
        sanitized_scenarios,
    )


def validate_relative_benchmark_terminal_summary(value: Mapping[str, object]) -> dict[str, object]:
    """Validate a portable summary that intentionally contains no market values."""
    summary = _mapping(value, _FIELDS)
    if summary["schema_version"] != _SCHEMA or summary["lifecycle_claims"] != _CLAIMS:
        _fail()
    candidate = _candidate(summary["candidate"])
    p3 = _p3(summary["p3"])
    benchmark = _mapping(summary["benchmark"], frozenset({"symbol", "policy"}))
    policy = _mapping(
        summary["policy"],
        frozenset(
            {
                "evidence_status",
                "short_window_drawdown_all_passed",
                "long_window_drawdown_all_passed",
                "long_window_incremental_calmar_all_passed",
                "forward_confirmation_satisfied",
                "strategy_verdict",
                "automatic_promotion",
            }
        ),
    )
    scenarios = summary["cost_scenarios"]
    if (
        not isinstance(benchmark["symbol"], str)
        or not benchmark["symbol"]
        or not isinstance(benchmark["policy"], str)
        or not benchmark["policy"]
        or policy["strategy_verdict"] != p3["verdict"]
        or not isinstance(scenarios, list)
        or not scenarios
    ):
        _fail()
    for field in (
        "short_window_drawdown_all_passed",
        "long_window_drawdown_all_passed",
        "long_window_incremental_calmar_all_passed",
        "forward_confirmation_satisfied",
        "automatic_promotion",
    ):
        if type(policy[field]) is not bool:
            _fail()
    scenario_records: list[dict[str, object]] = []
    scenario_fields = frozenset(
        {
            "total_cost_bps",
            "short_window_drawdown_failed_count",
            "long_window_drawdown_passed",
            "long_window_incremental_calmar_passed",
            "long_window_passed",
        }
    )
    for scenario in scenarios:
        record = _mapping(scenario, scenario_fields)
        if (
            type(record["total_cost_bps"]) is not int
            or record["total_cost_bps"] <= 0
            or type(record["short_window_drawdown_failed_count"]) is not int
            or record["short_window_drawdown_failed_count"] < 0
            or any(
                type(record[field]) is not bool
                for field in (
                    "long_window_drawdown_passed",
                    "long_window_incremental_calmar_passed",
                    "long_window_passed",
                )
            )
        ):
            _fail()
        if record["long_window_passed"] is not (
            record["long_window_drawdown_passed"] and record["long_window_incremental_calmar_passed"]
        ):
            _fail()
        scenario_records.append(record)
    if (
        policy["short_window_drawdown_all_passed"]
        is not all(record["short_window_drawdown_failed_count"] == 0 for record in scenario_records)
        or policy["long_window_drawdown_all_passed"]
        is not all(record["long_window_drawdown_passed"] for record in scenario_records)
        or policy["long_window_incremental_calmar_all_passed"]
        is not all(record["long_window_incremental_calmar_passed"] for record in scenario_records)
    ):
        _fail()
    return {
        "schema_version": _SCHEMA,
        "candidate": candidate,
        "input_manifest_sha256": _digest(summary["input_manifest_sha256"]),
        "p3": p3,
        "benchmark": {"symbol": benchmark["symbol"], "policy": benchmark["policy"]},
        "policy": policy,
        "cost_scenarios": scenario_records,
        "lifecycle_claims": dict(_CLAIMS),
    }


def build_relative_benchmark_terminal_summary(
    *,
    candidate: Mapping[str, object],
    input_manifest_sha256: str,
    p3_result: Mapping[str, object],
    relative_benchmark_policy: Mapping[str, object],
) -> dict[str, object]:
    """Derive the public-safe terminal view from a private policy artifact."""
    candidate_record = _candidate(candidate)
    policy_summary, scenarios = _policy_summary(
        relative_benchmark_policy, candidate_id=candidate_record["candidate_id"]
    )
    benchmark = {
        "symbol": policy_summary.pop("benchmark_symbol"),
        "policy": policy_summary.pop("benchmark_policy"),
    }
    return validate_relative_benchmark_terminal_summary(
        {
            "schema_version": _SCHEMA,
            "candidate": candidate_record,
            "input_manifest_sha256": input_manifest_sha256,
            "p3": p3_result,
            "benchmark": benchmark,
            "policy": policy_summary,
            "cost_scenarios": scenarios,
            "lifecycle_claims": _CLAIMS,
        }
    )


def canonical_relative_benchmark_terminal_summary_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        validate_relative_benchmark_terminal_summary(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


__all__ = [
    "RelativeBenchmarkTerminalSummaryError",
    "build_relative_benchmark_terminal_summary",
    "canonical_relative_benchmark_terminal_summary_bytes",
    "validate_relative_benchmark_terminal_summary",
]
