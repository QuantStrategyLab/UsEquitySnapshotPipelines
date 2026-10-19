#!/usr/bin/env python3
"""Bounded private archived no_plugin_core comparison; never a trading permit.

No provider, account, Kelly runner, optimizer or production configuration is read.
The only command that reads archive bytes requires the existing protected cloud
context and a separately approved immutable private control plan. Unit tests inject
an artificial archive contract and storage client; the command cannot select one.
"""
from __future__ import annotations

import argparse
import contextlib
import inspect
import json
import math
import os
import re
import signal
import sys
import time
from dataclasses import asdict
from datetime import date
from importlib.metadata import distribution, version
from pathlib import Path

from research_input_pilot import Stop, Store, bar_dates, coverage, encode, gcs_uri, now, sha

UES_COMMIT = "de8af865b89d3f21062c62aa8feb0b4773a6ec00"
QPK_COMMIT = "7840127862ea715e44c8f70b03155a399dc817de"
CORE_SHA = "37ad7ebe19d4ad9d6ff8c77294a084e7cd703a34105fa8ce011f0bddf9b35671"
CODE_VERSIONS = {"us-equity-strategies": "0.7.60", "quant-platform-kit": "1.0.0"}
START, END = date(2023, 3, 28), date(2026, 8, 25)
SYMBOLS = ("SOXL", "SOXX", "QQQ", "TQQQ", "QQQM", "BOXX")
STUDY = "rs04_archived_core_retrospective_v1"
NAMES = ("intent.json", "private_core_paths.json", "private_comparison_summary.json",
         "technical_summary.json", "terminal.json")
QUALIFICATION = {"PIT": "NOT_ESTABLISHED", "OOS": "SEEN_DEVELOPMENT_NOT_INDEPENDENT",
                 "financial": "NOT_READY", "adoption": "UNKNOWN"}
COSTS = {"effective_bps_per_side": [5, 10, 15], "role": "hypothetical_sensitivity",
         "weight_feedback": False, "self_financing_net_ledger": False,
         "actual_broker_costs": False, "residual_cash_return": "zero_model_assumption"}
# Immutable metadata only. Private locations enter exclusively through the plan.
ARCHIVE = {
    "inventory_uri_sha256": "16a9f778df1e6cd2512e6a0665325701cbfc0f2d1aa73570ea5ba233ab401fd0",
    "inventory": {"generation": "1792044148695461", "bytes": 73385,
                  "sha256": "31387418ffab9764fb8fb1615a15262fb1e48637b76ca61cf319379481a4bd29"},
    "leaves": (
        ("BOXX", "actions", "1792044141950872", 300), ("BOXX", "raw", "1792044141181623", 99725),
        ("QQQ", "actions", "1792044138178475", 9333), ("QQQ", "raw", "1792044137273444", 301489),
        ("QQQM", "actions", "1792044140793995", 5466), ("QQQM", "raw", "1792044140007402", 162306),
        ("SOXL", "actions", "1792044135160909", 6765), ("SOXL", "raw", "1792044134212797", 288592),
        ("SOXX", "actions", "1792044136703478", 9456), ("SOXX", "raw", "1792044135895920", 292875),
        ("TQQQ", "actions", "1792044139486627", 5633), ("TQQQ", "raw", "1792044138675597", 291632),
    ),
}


def require(condition, code):
    if not condition:
        raise Stop(code)


def object_ref(obj, max_bytes=2 * 1024**2):
    require(type(obj) is dict and {"uri", "generation", "bytes", "sha256"} <= obj.keys(), "OBJECT_REF_INVALID")
    bucket, _path = gcs_uri(obj["uri"])
    require(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,220}[a-z0-9]", bucket), "OBJECT_BUCKET_REJECTED")
    require(type(obj["generation"]) is str and re.fullmatch(r"[1-9][0-9]{0,24}", obj["generation"]),
            "OBJECT_GENERATION_INVALID")
    require(type(obj["bytes"]) is int and 0 < obj["bytes"] <= max_bytes, "OBJECT_SIZE_INVALID")
    require(type(obj["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", obj["sha256"]), "OBJECT_SHA_REQUIRED")
    return {k: obj[k] for k in ("uri", "generation", "bytes", "sha256")}


def identity(obj):
    return {k: obj[k] for k in ("generation", "bytes", "sha256")}


def decode(body):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "JSON_DUPLICATE_KEY")
            result[key] = value
        return result
    try:
        result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda _x: (_ for _ in ()).throw(
            Stop("NONFINITE_JSON")))
    except (ValueError, TypeError, UnicodeError):
        raise Stop("JSON_INVALID") from None
    require(type(result) is dict, "JSON_OBJECT_REQUIRED")
    return result


class Budget:
    """Small caller budget; the original pilot Budget is deliberately untouched."""
    def __init__(self):
        self.started = time.monotonic()
        self.storage_operations = self.storage_transfer_bytes = self.retained_bytes = 0
        self.input_bytes = self.output_objects = 0

    def check(self):
        require(time.monotonic() - self.started <= 1200, "WALL_TIME_LIMIT")

    def storage(self, transfer=0, retained=0):
        self.check()
        require(type(transfer) is int and transfer >= 0 and type(retained) is int and retained >= 0,
                "BUDGET_VALUE_INVALID")
        require(self.storage_operations < 64 and self.storage_transfer_bytes + transfer <= 64 * 1024**2,
                "STORAGE_BUDGET_LIMIT")
        require(self.retained_bytes + retained <= 16 * 1024**2, "OUTPUT_BYTE_LIMIT")
        self.storage_operations += 1
        self.storage_transfer_bytes += transfer
        self.retained_bytes += retained

    def input(self, size):
        self.check()
        require(self.input_bytes + size <= 2 * 1024**2, "INPUT_BYTE_LIMIT")
        self.input_bytes += size

    def summary(self):
        return {"storage_operations": self.storage_operations, "storage_transfer_bytes": self.storage_transfer_bytes,
                "private_output_bytes": self.retained_bytes, "private_output_objects": self.output_objects,
                "input_bytes": self.input_bytes, "provider_calls": 0, "automatic_retries": 0}


class ArchiveStore(Store):
    def read_input(self, ref, max_bytes=2 * 1024**2):
        ref = object_ref(ref, max_bytes)
        self.budget.input(ref["bytes"])
        return self.read(ref)

    def create(self, relative, body):
        require(relative in NAMES and self.budget.output_objects < 5, "OUTPUT_OBJECT_LIMIT")
        require(len(body) + self.budget.retained_bytes <= 16 * 1024**2, "OUTPUT_BYTE_LIMIT")
        # Reserve even on an uncertain result. Never retry or write a failure seal
        # after an unknown write; the operator must inspect that run privately.
        self.budget.output_objects += 1
        return super().create(relative, body)


def verify_runtime():
    from us_equity_strategies import manifests
    from us_equity_strategies.research import kelly_core_return_generator as core
    from us_equity_strategies.strategies import soxl_soxx_trend_income, tqqq_growth_income
    from quant_platform_kit.common.runtime_inputs import build_semiconductor_rotation_indicators_from_history
    from quant_platform_kit.research_stats.execution_models import CostModel
    from quant_platform_kit.research_stats.unified_report import build_unified_net_report
    require(version("exchange-calendars") == "4.13.2", "CALENDAR_VERSION_MISMATCH")
    result = {}
    for name, commit, module in (("us-equity-strategies", UES_COMMIT, core),
                                 ("quant-platform-kit", QPK_COMMIT, inspect.getmodule(CostModel))):
        dist = distribution(name)
        direct = json.loads(dist.read_text("direct_url.json") or "{}")
        require(direct.get("vcs_info", {}).get("commit_id") == commit, "RESEARCH_INSTALL_COMMIT_MISMATCH")
        require(direct.get("vcs_info", {}).get("vcs") == "git", "RESEARCH_INSTALL_NOT_GIT")
        require(Path(module.__file__).resolve().is_relative_to(Path(dist.locate_file("")).resolve()),
                "IMPORT_OUTSIDE_INSTALLED_DISTRIBUTION")
        require(dist.version == CODE_VERSIONS[name], "VERSION_INVALID")
        result[name] = {"commit": commit, "version": dist.version}
    require(sha(Path(core.__file__).read_bytes()) == CORE_SHA, "CORE_SOURCE_MISMATCH")
    require(callable(tqqq_growth_income.build_rebalance_plan) and callable(soxl_soxx_trend_income.build_rebalance_plan)
            and callable(build_semiconductor_rotation_indicators_from_history), "CORE_API_MISSING")
    require("signal_price_basis" in inspect.signature(core.generate_tqqq_core_path).parameters
            and "indicator_history_bars" in inspect.signature(core.generate_soxl_core_path).parameters
            and hasattr(core.CorePath, "measure_close_fill_costs")
            and "include_max_drawdown_recovery" in inspect.signature(build_unified_net_report).parameters,
            "RESEARCH_API_MISSING")
    configs = {"TQQQ": dict(manifests.tqqq_growth_income_manifest.default_config) | core.TQQQ_NO_PLUGIN_OVERRIDES,
               "SOXL": dict(manifests.soxl_soxx_trend_income_manifest.default_config) | core.SOXL_NO_PLUGIN_OVERRIDES}
    return result, sha(encode(configs))


def cloud_gate(env):
    require(env.get("GITHUB_EVENT_NAME") == "workflow_dispatch" and env.get("GITHUB_REF") == "refs/heads/main"
            and env.get("GITHUB_RUN_ATTEMPT") == "1" and env.get("RS04_ENVIRONMENT") == "market-data-nonlive"
            and re.fullmatch(r"[1-9][0-9]{0,24}", env.get("GITHUB_RUN_ID", "")), "CLOUD_CONTEXT_REJECTED")


def validate_plan(plan, root, run_id, contract=ARCHIVE):
    require(set(plan) == {"schema", "study_id", "run_id", "window", "inventory", "output_prefix",
                          "execution_approved", "cloud_cap_usd", "estimated_cloud_usd", "retention_estimate_days",
                          "soft_delete_days", "retention_conditions_unchanged"}, "PLAN_FIELDS_REJECTED")
    require(plan["schema"] == "qsl.archived_core_retrospective_plan.v1" and plan["study_id"] == STUDY,
            "PLAN_SCOPE_REJECTED")
    require(type(run_id) is str and re.fullmatch(r"[1-9][0-9]{0,24}", run_id) and plan["run_id"] == run_id,
            "RUN_ID_REJECTED")
    require(plan["window"] == {"start": START.isoformat(), "end": END.isoformat()}, "WINDOW_REJECTED")
    require(plan["execution_approved"] is True and type(plan["cloud_cap_usd"]) in (int, float)
            and plan["cloud_cap_usd"] == .10 and type(plan["estimated_cloud_usd"]) in (int, float)
            and 0 <= plan["estimated_cloud_usd"] <= .10, "EXECUTION_OR_COST_APPROVAL_REQUIRED")
    require(plan["retention_estimate_days"] == 90 and plan["soft_delete_days"] == 7
            and plan["retention_conditions_unchanged"] is True, "RETENTION_REESTIMATE_REQUIRED")
    bucket, prefix = gcs_uri(root)
    require(root.endswith("/"), "RESEARCH_ROOT_REJECTED")
    ref = object_ref(plan["inventory"])
    source_bucket, source_path = gcs_uri(ref["uri"])
    require(source_bucket == bucket and source_path.startswith(prefix)
            and source_path.endswith("/input-inventory.json") and identity(ref) == contract["inventory"]
            and sha(ref["uri"].encode()) == contract["inventory_uri_sha256"],
            "FIXED_INVENTORY_IDENTITY_MISMATCH")
    out_bucket, out_prefix = gcs_uri(plan["output_prefix"])
    require(out_bucket == bucket and out_prefix.startswith(prefix) and out_prefix.endswith(f"/{STUDY}/{run_id}/")
            and re.fullmatch(r"[A-Za-z0-9_./-]+", out_prefix)
            and not out_prefix.startswith(source_path.rsplit("/", 1)[0] + "/"), "OUTPUT_PREFIX_REJECTED")
    return bucket, out_prefix


def select_pages(inventory, ref, contract=ARCHIVE):
    require(inventory.get("schema_version") == "research_input_inventory.v1", "INVENTORY_SCHEMA_REJECTED")
    require(type(inventory.get("alpaca")) is list, "INVENTORY_ENTRIES_REJECTED")
    expected = {(s, k): (g, b) for s, k, g, b in contract["leaves"]}
    selected = {}
    base = ref["uri"].rsplit("/", 1)[0] + "/"
    for entry in inventory["alpaca"]:
        require(type(entry) is dict and entry.get("symbol") in SYMBOLS, "INVENTORY_SYMBOL_REJECTED")
        if entry.get("price_basis") == "split_dividend_adjusted" and entry.get("vendor_adjustment") == "all":
            continue
        kind = "actions" if entry.get("type") == "corporate_actions" else "raw"
        if kind == "raw":
            require(entry.get("price_basis") == "raw_unadjusted" and entry.get("vendor_adjustment") == "raw"
                    and entry.get("currency") == "USD" and entry.get("source_type") == "real"
                    and entry.get("calendar_id") == "XNYS" and entry.get("calendar_version") == "4.13.2",
                    "PRICE_BASIS_OR_IDENTITY_REJECTED")
        key = entry["symbol"], kind
        require(key in expected and key not in selected, "DUPLICATE_INVENTORY_ENTRY")
        pages = entry.get("pages")
        require(type(pages) is list and len(pages) == 1, "FIXED_PAGE_COUNT_MISMATCH")
        page = object_ref(pages[0])
        require((page["generation"], page["bytes"]) == expected[key]
                and page["uri"] == base + f"alpaca/{key[0]}/{kind}/page-001.json", "FIXED_PAGE_IDENTITY_MISMATCH")
        selected[key] = entry
    require(set(selected) == set(expected) and len(selected) == 12, "FIXED_PAGE_SET_MISMATCH")
    require(sum(e["pages"][0]["bytes"] for e in selected.values()) + ref["bytes"]
            == sum(x[3] for x in contract["leaves"]) + contract["inventory"]["bytes"], "INPUT_SIZE_MISMATCH")
    return selected


def calendar_sessions(start, end):
    import exchange_calendars as xcals
    require(version("exchange-calendars") == "4.13.2", "CALENDAR_VERSION_MISMATCH")
    calendar = xcals.get_calendar("XNYS", start=start, end=end)
    return tuple(s.date() for s in calendar.sessions_in_range(start, end))


def day(value):
    require(type(value) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value), "DATE_INVALID")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise Stop("DATE_INVALID") from None


def number(value, *, positive=False):
    require(not isinstance(value, bool) and isinstance(value, (int, float)), "ACTION_NUMBER_INVALID")
    require(math.isfinite(value) and (value > 0 if positive else value >= 0), "ACTION_NUMBER_INVALID")
    return float(value)


def normalize_actions(symbol, pages, start, end, *, seen_ids=None):
    from us_equity_strategies.research.kelly_core_return_generator import CorporateActions
    ids = set() if seen_ids is None else seen_ids
    records, groups = set(), {}
    splits, dividends = [], []
    for page in pages:
        require(page.get("next_page_token") in (None, ""), "PAGINATION_NOT_ARCHIVED")
        actions = page.get("corporate_actions")
        require(type(actions) is dict, "ACTION_SCHEMA_INVALID")
        for kind, rows in actions.items():
            require(type(rows) is list, "ACTION_SCHEMA_INVALID")
            require(kind in ("forward_splits", "cash_dividends") or not rows, "ACTION_KIND_UNSUPPORTED")
            for row in rows:
                require(type(row) is dict and row.get("symbol") == symbol, "ACTION_SYMBOL_INVALID")
                ex = day(row.get("ex_date"))
                require(start <= ex <= end, "ACTION_DATE_OUTSIDE_SOURCE")
                require(row.get("currency", "USD") == "USD", "ACTION_CURRENCY_REJECTED")
                require(not any(row.get(k) for k in ("cancelled", "canceled", "replaced_by", "replaces",
                                                     "superseded_by", "replacement_id", "revision"))
                        and row.get("status", "active") == "active", "ACTION_REVISION_REJECTED")
                native = row.get("id")
                require(native is None or (type(native) is str and 0 < len(native) <= 256
                                           and native == native.strip()), "ACTION_ID_INVALID")
                if native is not None:
                    require(native not in ids, "ACTION_DUPLICATE_ID_OR_CONFLICT")
                    ids.add(native)
                digest = sha(encode(row))
                require(digest not in records, "ACTION_DUPLICATE_RECORD")
                records.add(digest)
                group = kind, symbol, ex
                previous = groups.setdefault(group, [])
                require(not previous or (native is not None and all(x is not None for x in previous)),
                        "ACTION_SAME_DAY_AMBIGUOUS")
                previous.append(native)
                if kind == "forward_splits":
                    ratio = number(row.get("new_rate"), positive=True) / number(row.get("old_rate"), positive=True)
                    require(math.isfinite(ratio) and ratio > 0, "ACTION_RATIO_INVALID")
                    splits.append((ex, ratio))
                elif kind == "cash_dividends":
                    dividends.append((ex, number(row.get("rate"))))
    # Existing total_return_series, invoked below/by the real generators, rejects
    # repeated same-day splits and split/dividend collisions. Do not resolve them.
    return CorporateActions(tuple(splits), tuple(dividends))


def normalize(selected, bodies):
    from us_equity_strategies.research.kelly_core_return_generator import RawBar, total_return_series
    starts, bars, actions, seen_ids = {}, {}, {}, set()
    counts = {"raw_bars": 0, "cash_dividends": 0, "forward_splits": 0}
    for symbol in SYMBOLS:
        entry = selected[symbol, "raw"]
        cov = entry.get("coverage", {})
        start, end = day(cov.get("requested_start")), day(cov.get("requested_end"))
        require(start < START and end == END, "SOURCE_WINDOW_REJECTED")
        if symbol == "BOXX":
            require(start == date(2022, 12, 28) and entry.get("inception") == "2022-12-28", "BOXX_INCEPTION_REJECTED")
        starts[symbol] = start
        expected = calendar_sessions(start, end)
        page = bodies[symbol, "raw"]
        require(page.get("symbol") == symbol and page.get("next_page_token") in (None, ""),
                "BAR_SYMBOL_OR_PAGINATION_REJECTED")
        dates = bar_dates(page, start.isoformat(), end.isoformat())
        cov = coverage(dates, start.isoformat(), end.isoformat(), [s.isoformat() for s in expected])
        require(cov["coverage_complete"], "BAR_COVERAGE_INCOMPLETE")
        bars[symbol] = [RawBar(day(d), float(row["o"]), float(row["c"]))
                        for d, row in zip(dates, page["bars"], strict=True)]
        actions[symbol] = normalize_actions(symbol, [bodies[symbol, "actions"]], start, end, seen_ids=seen_ids)
        require(all(ex in set(expected) for ex, _value in (*actions[symbol].splits, *actions[symbol].dividends)),
                "ACTION_ON_NON_SESSION")
        total_return_series(bars[symbol], actions[symbol], expected)
        counts["raw_bars"] += len(bars[symbol])
        counts["cash_dividends"] += len(actions[symbol].dividends)
        counts["forward_splits"] += len(actions[symbol].splits)
    common_start = starts["QQQ"]
    require(all(starts[s] == common_start for s in ("SOXL", "SOXX", "TQQQ")), "SIGNAL_GRID_MISMATCH")
    sessions = calendar_sessions(common_start, END)
    require(sessions.index(START) >= 504 and len(calendar_sessions(START, END)) == 856,
            "WARMUP_OR_WINDOW_MISMATCH")
    return bars, actions, sessions, counts


def metrics(returns, gross, turnover):
    from quant_platform_kit.research_stats.unified_report import build_unified_net_report
    report = build_unified_net_report(returns, gross_returns=gross, periods_per_year=252,
                                     sharpe_basis="net_wealth_excess_sharpe_v2", calendar_id="XNYS",
                                     include_max_drawdown_recovery=True)
    recovery = report.metric("max_drawdown_peak_to_recovery_observations")
    details = recovery.details
    return {"periods": len(returns), "log_growth": math.fsum(math.log1p(x) for x in returns),
            "geometric_annualized_252": report.metric("net_cagr").value,
            "max_drawdown": report.metric("max_drawdown").value,
            "peak_to_recovery_periods": recovery.value,
            "trough_to_recovery_periods": report.metric("max_drawdown_trough_to_recovery_observations").value,
            "recovery_censored": details.get("right_censored"),
            "observed_recovery_lower_bound": {"peak_to_end": details.get("observed_peak_to_end_lower_bound"),
                                              "trough_to_end": details.get("observed_trough_to_end_lower_bound")},
            "close_turnover_sum": math.fsum(turnover),
            "model_fee_deduction_sum_fraction": math.fsum(a - b for a, b in zip(gross, returns, strict=True)),
            "cost_log_drag": report.metric("cost_log_drag_per_period").value * len(returns)}, report.to_dict()


def compare(bars, actions, sessions):
    from quant_platform_kit.research_stats.execution_models import CostModel
    from us_equity_strategies.research import kelly_core_return_generator as core
    # Each actual core is called exactly once per comparison. Cost tiers reuse
    # the same gross decisions and weights and never select a winning parameter.
    paths = (
        core.generate_tqqq_core_path(bars, actions, sessions, trade_start=START, signal_price_basis="split_adjusted"),
        core.generate_soxl_core_path(bars, actions, sessions, trade_start=START, signal_price_basis="split_adjusted",
                                     indicator_source="qpk_live_builder", indicator_history_bars=504),
    )
    daily, comparison = [], []
    for path in paths:
        require(path.variant == "no_plugin_core", "VARIANT_REJECTED")
        first_boxx = bars["BOXX"][0].session
        require(all(weights.get("BOXX", 0) == 0 for session, weights in zip(path.sessions, path.weights, strict=True)
                    if session <= first_boxx), "BOXX_PRE_INCEPTION_POSITION")
        window = path.window(START, END)
        require(window.sessions == calendar_sessions(START, END), "CORE_WINDOW_MISMATCH")
        sensitivity, nets = [], {}
        gross_metrics, _ = metrics(window.gross_returns, window.gross_returns, window.close_fill_turnover)
        outside = [i for i, d in enumerate(path.sessions) if d < START]
        pre_window = []
        for bps in COSTS["effective_bps_per_side"]:
            model = CostModel(commission_bps=bps)
            net = window.measure_close_fill_costs(model)
            nets[str(bps)] = net
            measured, report = metrics(net, window.gross_returns, window.close_fill_turnover)
            sensitivity.append({"bps_per_side": bps, "metrics": measured, "qpk_v2_report": report})
            pre_window.append({"bps_per_side": bps, "model_fee_deduction_sum_fraction": math.fsum(
                (1 + path.gross_returns[i]) * path.close_fill_turnover[i] * bps / 10000 for i in outside)})
        opening = {"earning_weights": dict(window.weights[0]), "first_signal": str(path.first_signal),
                   "trade_start": START.isoformat(), "preparation_sessions": 2}
        comparison.append({"strategy_key": path.strategy_key, "first_signal": str(path.first_signal),
                           "opening_state_digest": sha(encode(opening)),
                           "pre_window_entry_cost_diagnostic": pre_window,
                           "gross_metrics": gross_metrics,
                           "sensitivity": sensitivity})
        daily.append({"strategy_key": path.strategy_key, "path": asdict(window), "opening_state": opening,
                      "model_net_returns": nets})
    # Private serialization contains dates; public projection never sees daily.
    return json.loads(json.dumps(daily, default=str, allow_nan=False)), comparison


def technical_summary(*, run_id, started, inventory, pages, source_digest, installed, config_digest,
                      counts, outputs, budget, status="COMPLETED", reasons=()):
    require(status in ("PREPARED", "COMPLETED", "STOPPED")
            and all(r in ("CONTROLLED_STOP", "INTERNAL_FAILURE") for r in reasons),
            "SUMMARY_STATUS_REJECTED")
    require(type(run_id) is str and re.fullmatch(r"[1-9][0-9]{0,24}", run_id), "RUN_ID_REJECTED")
    require(set(installed) == {"us-equity-strategies", "quant-platform-kit"}, "SUMMARY_CODE_REJECTED")
    for name, commit in (("us-equity-strategies", UES_COMMIT), ("quant-platform-kit", QPK_COMMIT)):
        require(set(installed[name]) == {"commit", "version"} and installed[name]["commit"] == commit
                and installed[name]["version"] == CODE_VERSIONS[name], "SUMMARY_CODE_REJECTED")
    for digest in (config_digest, source_digest):
        require(re.fullmatch(r"[0-9a-f]{64}", digest), "SUMMARY_DIGEST_REJECTED")
    require(set(counts) == {"raw_bars", "cash_dividends", "forward_splits"}
            and all(type(x) is int and x >= 0 for x in counts.values()), "SUMMARY_COUNTS_REJECTED")
    safe_outputs = {}
    for name, ref in outputs.items():
        require(name in NAMES, "SUMMARY_OUTPUT_REJECTED")
        safe_outputs[name] = identity(object_ref(ref, 16 * 1024**2))
    result = {"schema": "qsl.archived_core_retrospective_technical.v1", "study_id": STUDY,
              "run_id": run_id, "attempt": 1, "status": status, "started_at": started, "finished_at": now(),
              "requested_window": {"start": str(START), "end": str(END)},
              "actual_window": {"start": str(START), "end": str(END)},
              "calendar_id": "XNYS", "calendar_version": "4.13.2", "sessions": 856,
              "source_inventory_identity": identity(object_ref(inventory)), "selected_page_count": 12,
              "selected_input_bytes": sum(e["pages"][0]["bytes"] for e in pages.values()) + inventory["bytes"],
              "source_digest": source_digest, "installed_code_identities": installed,
              "configuration_digest": config_digest, "variant": "no_plugin_core", "generator_version": "core_v2_parity",
              "price_basis": "raw_unadjusted", "signal_basis": "split_adjusted",
              "dividend_recognition": "retrospective_ex_date_not_available_cash",
              "execution_convention": "completed_t_close_signal_t1_close_fill_t2_earning",
              "cost_assumptions": COSTS, "action_counts": counts,
              "duplicate_conflict_counts": {"accepted_duplicates": 0, "accepted_conflicts": 0},
              "output_hashes_sizes_generations": safe_outputs, "operation_counters": budget.summary(),
              "qualification": QUALIFICATION, "reason_codes": list(reasons)}
    # Only a validated timestamp supplied by this application may be exported.
    require(type(started) is str and re.fullmatch(r"[0-9T:.Z+-]{20,40}", started), "SUMMARY_TIME_REJECTED")
    require(len(encode(result)) <= 64 * 1024, "SUMMARY_BYTE_LIMIT")
    return result


def run(plan, store, installed, config_digest, contract=ARCHIVE):
    started, outputs = now(), {}
    ref = object_ref(plan["inventory"])
    require(identity(ref) == contract["inventory"] and sha(ref["uri"].encode()) == contract["inventory_uri_sha256"],
            "FIXED_INVENTORY_IDENTITY_MISMATCH")
    inventory = decode(store.read_input(ref))
    selected = select_pages(inventory, ref, contract)
    total = ref["bytes"] + sum(e["pages"][0]["bytes"] for e in selected.values())
    require(store.budget.input_bytes + total - ref["bytes"] <= 2 * 1024**2, "INPUT_BYTE_LIMIT")
    source_digest = sha(encode([identity(ref)] + [object_ref(selected[k]["pages"][0]) for k in sorted(selected)]))
    outputs[NAMES[0]] = store.create(NAMES[0], encode({"schema": "qsl.archived_core_intent.v1", "run_id": plan["run_id"],
                                                     "source_digest": source_digest, "qualification": QUALIFICATION}))
    bodies = {key: decode(store.read_input(entry["pages"][0])) for key, entry in selected.items()}
    try:
        bars, actions, sessions, counts = normalize(selected, bodies)
        daily, comparison = compare(bars, actions, sessions)
    except Exception:
        # Computation failed after whole-byte source checks; retain a payload-free
        # private failure record. Storage/identity/write failures never get here.
        store.create(NAMES[4], encode({"schema": "qsl.archived_core_terminal.v1", "status": "STOPPED",
                                      "reason_codes": ["CONTROLLED_STOP"]}))
        raise Stop("COMPUTATION_STOPPED") from None
    store.budget.check()
    private = {"schema": "qsl.archived_core_comparison.v1", "cost_assumptions": COSTS,
               "configuration_digest": config_digest, "qualification": QUALIFICATION, "comparison": comparison,
               "configuration_basis": "manifest_defaults_plus_no_plugin_overrides_not_production_effective_config",
               "reference_equity": "generator_model_constant_not_account_funds",
               "boxx": "held_ETF_total_return_once_not_risk_free_or_residual_cash"}
    outputs[NAMES[1]] = store.create(NAMES[1], encode({"schema": "qsl.archived_core_paths.v1", "paths": daily}))
    outputs[NAMES[2]] = store.create(NAMES[2], encode(private))
    summary_args = dict(run_id=plan["run_id"], started=started, inventory=ref, pages=selected,
                        source_digest=source_digest, installed=installed, config_digest=config_digest,
                        counts=counts, outputs=outputs, budget=store.budget)
    summary = technical_summary(**summary_args, status="PREPARED")
    outputs[NAMES[3]] = store.create(NAMES[3], encode(summary))
    # Self hashes are deliberately absent; terminal seals only completed predecessors.
    outputs[NAMES[4]] = store.create(NAMES[4], encode({"schema": "qsl.archived_core_terminal.v1", "status": "COMPLETED",
                                                     "outputs": {k: identity(v) for k, v in outputs.items()}}))
    return technical_summary(**summary_args)


def dispatch_plan_ref(event_path):
    # GitHub prints step env/with values before a script can mask them. Read only
    # the supplied dispatch inputs from its local event file instead.
    path = Path(event_path)
    require(path.stat().st_size <= 64 * 1024, "DISPATCH_EVENT_SIZE_REJECTED")
    inputs = decode(path.read_bytes()).get("inputs")
    require(type(inputs) is dict and set(inputs) == {"plan_uri", "plan_generation", "plan_bytes", "plan_sha256"},
            "DISPATCH_INPUTS_REJECTED")
    return object_ref({"uri": inputs["plan_uri"], "generation": inputs["plan_generation"],
                       "bytes": int(inputs["plan_bytes"]), "sha256": inputs["plan_sha256"]}, 16 * 1024)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-runtime", action="store_true")
    args = parser.parse_args(argv)
    try:
        signal.signal(signal.SIGALRM, lambda *_args: (_ for _ in ()).throw(Stop("WALL_TIME_LIMIT")))
        signal.setitimer(signal.ITIMER_REAL, 1200)
        # Identity and real imports precede authentication and every input byte.
        installed, digest = verify_runtime()
        if args.verify_runtime:
            print(encode({"status": "RUNTIME_VERIFIED", "installed_code_identities": installed,
                          "configuration_digest": digest}).decode(), end="")
            return 0
        cloud_gate(os.environ)
        plan_ref = dispatch_plan_ref(os.environ["GITHUB_EVENT_PATH"])
        bucket, path = gcs_uri(plan_ref["uri"])
        from google.cloud import storage
        budget = Budget()
        # Environment OIDC is the sole credential source, as in the original pilot.
        with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            store = ArchiveStore(storage.Client(), budget, bucket, path.rsplit("/", 1)[0] + "/")
            plan = decode(store.read_input(plan_ref, 16 * 1024))
            source_bucket, source_path = gcs_uri(object_ref(plan["inventory"])["uri"])
            root = f"gs://{source_bucket}/" + source_path.rsplit("/", 2)[0] + "/"
            require(bucket == source_bucket and path.startswith(gcs_uri(root)[1]), "CONTROL_PLAN_PREFIX_REJECTED")
            bucket, output_prefix = validate_plan(plan, root, os.environ["GITHUB_RUN_ID"])
            store.bucket, store.prefix = bucket, output_prefix
            summary = run(plan, store, installed, digest)
        summary_file = os.environ.get("RS04_SUMMARY_FILE")
        if summary_file:
            Path(summary_file).write_bytes(encode(summary))
        print(encode(summary).decode(), end="")
        return 0
    except Exception:
        # Original exception bodies may contain credentials, object paths or data.
        # Do not print, interpolate, upload or retry them, even for known failures.
        print('{"schema":"qsl.archived_core_terminal.v1","status":"STOPPED","reason_codes":["CONTROLLED_STOP"]}')
        return 1
    finally:
        if hasattr(signal, "setitimer"):
            signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    sys.exit(main())
