#!/usr/bin/env python3
"""Check one private SOXL package, with optional bounded development execution."""

from __future__ import annotations

import argparse
import json

from us_equity_snapshot_pipelines.lifecycle.soxl_isolated_research import (
    DEFAULT_ENTRY_BUFFERS,
    SoxlIsolatedResearchError,
    load_soxl_completed_research_result,
    preflight_soxl_isolated_research,
    run_soxl_isolated_development,
)


_PUBLIC_FAILURE_REASONS = frozenset(
    {"research_request_invalid", "research_dependency_mismatch", "snapshot_invalid"}
)
_PUBLIC_DEVELOPMENT_FAILURE_REASONS = frozenset(
    {
        "development_decision_failed",
        "development_input_invalid",
        "development_request_invalid",
        "development_risk_rejected",
        "development_timeout",
        "development_trial_failed",
    }
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot")
    parser.add_argument("--package-sha256")
    parser.add_argument("--input-manifest-sha256")
    parser.add_argument("--entry-buffers", type=float, nargs="+", default=DEFAULT_ENTRY_BUFFERS)
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--run-development", action="store_true")
    operation.add_argument("--completed-result")
    parser.add_argument("--completed-result-sha256")
    args = parser.parse_args(argv)
    if args.completed_result:
        if not args.completed_result_sha256:
            print(json.dumps({"status": "unavailable", "reason": "completed_result_invalid"}))
            return 3
        try:
            proposal = load_soxl_completed_research_result(
                args.completed_result,
                expected_sha256=args.completed_result_sha256,
            )
        except SoxlIsolatedResearchError:
            print(json.dumps({"status": "unavailable", "reason": "completed_result_invalid"}))
            return 3
        print(
            json.dumps(
                {
                    "status": "completed",
                    "outcome": (
                        "no_improvement"
                        if proposal.recommendation == "hold"
                        else "requires_review"
                    ),
                    "learning_only": True,
                    "promotion_eligible": False,
                    "live_ready": False,
                    "size_zero_required": True,
                    "no_order": True,
                    "real_backtest_executed": False,
                    "completed_result_reused": True,
                    "window_class": "seen_development",
                    "proposal": proposal.to_dict(),
                }
            )
        )
        return 0
    if not args.snapshot or not args.package_sha256 or not args.input_manifest_sha256:
        print(json.dumps({"status": "unavailable", "reason": "research_request_invalid"}))
        return 3
    try:
        operation = (
            run_soxl_isolated_development
            if args.run_development
            else preflight_soxl_isolated_research
        )
        result = operation(
            args.snapshot, expected_package_sha256=args.package_sha256,
            expected_input_manifest_sha256=args.input_manifest_sha256,
            entry_buffers=args.entry_buffers,
        )
    except SoxlIsolatedResearchError as exc:
        reason = str(exc)
        if reason not in _PUBLIC_FAILURE_REASONS:
            reason = "snapshot_invalid"
        print(json.dumps({"status": "unavailable", "reason": reason}))
        return 3
    if args.run_development:
        if result["failure_reason"] not in _PUBLIC_DEVELOPMENT_FAILURE_REASONS | {None}:
            result["failure_reason"] = "development_trial_failed"
        print(json.dumps(result))
        return 0 if result["status"] == "development_completed" else 4
    summary = {key: result[key] for key in (
        "execution_provider", "max_trials", "learning_only", "promotion_eligible",
        "live_ready", "size_zero_required", "no_order", "real_backtest_executed",
    )}
    print(json.dumps({"status": "preflight_passed", "trial_count": len(result["runtime_configs"]), **summary}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
