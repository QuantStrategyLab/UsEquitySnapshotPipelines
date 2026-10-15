"""Aggregate paired cloud replay ledgers without exporting observations.

This does not generate signals, submit orders, or certify a replay producer.
The input must come from the frozen strategy/portfolio/cost replay in cloud.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlsplit


class ResearchStop(ValueError):
    """Fixed reason codes only; never include input values or provider errors."""


BASES = {"raw_unadjusted", "split_adjusted", "split_dividend_adjusted", "total_return_index"}
COST_COMPONENTS = {
    "commission", "spread_slippage", "impact", "borrow_margin", "cash_yield", "options", "fund_expenses",
}
SHA = re.compile(r"[0-9a-f]{64}")
MAX_OBJECT_BYTES = 64 * 1024 * 1024


def require(condition, code):
    if not condition:
        raise ResearchStop(code)


def number(value):
    require(type(value) in (int, float) and math.isfinite(value), "INVALID_NUMBER")
    return float(value)


def day(value):
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        raise ResearchStop("INVALID_DATE") from None


def reporting_dates(meta):
    as_of = day(meta.get("as_of"))
    value = meta.get("inception")
    inception = None if value in (None, "", "unknown") else day(value)
    require(inception is None or inception <= as_of, "INCEPTION_INVALID")
    try:
        two_years = as_of.replace(year=as_of.year - 2)
    except ValueError:  # Feb 29
        two_years = as_of.replace(year=as_of.year - 2, day=28)
    return as_of, inception, two_years


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"p50": None, "p95": None, "max": None}

    def percentile(q):
        x = (len(values) - 1) * q
        lo, hi = math.floor(x), math.ceil(x)
        return values[lo] + (values[hi] - values[lo]) * (x - lo)

    return {"p50": percentile(0.5), "p95": percentile(0.95), "max": values[-1]}


def bucket(value, edges):
    return next((i for i, edge in enumerate(edges) if value <= edge), len(edges))


def decisions(orders, edges):
    """Directions and size buckets use signed order notional / pre-fill NAV."""
    return {symbol: (1 if size > 0 else -1, bucket(abs(size), edges))
            for symbol, raw in orders.items() if (size := number(raw)) != 0}


def validate_arm(arm, limits):
    for field in ("signals", "weights", "orders", "threshold_distances"):
        require(isinstance(arm.get(field), dict), "REPLAY_FIELD_MISSING")
        for key, value in arm[field].items():
            require(isinstance(key, str) and len(key) <= 128, "REPLAY_KEY_INVALID")
            number(value)
    require(bool(arm["signals"]), "SIGNAL_SERIES_REQUIRED")
    weights = arm["weights"]
    require(all(w >= 0 for w in weights.values()), "NEGATIVE_WEIGHT")
    require(sum(weights.values()) <= limits["total"] + 1e-10, "TOTAL_CAP_EXCEEDED")
    for symbol, cap in limits["symbols"].items():
        require(weights.get(symbol, 0) <= cap + 1e-10, "SYMBOL_CAP_EXCEEDED")
    for key in ("gross_return", "net_return_1x", "net_return_2x"):
        require(number(arm.get(key)) > -1, "INVALID_RETURN")
    require(number(arm.get("turnover")) >= 0, "INVALID_TURNOVER")
    require(type(arm.get("trade_count")) is int and arm["trade_count"] >= 0, "TRADE_COUNT_REQUIRED")
    require(arm["net_return_1x"] <= arm["gross_return"] + 1e-10, "COST_ORDER_INVALID")
    require(arm["net_return_2x"] <= arm["net_return_1x"] + 1e-10, "COST_STRESS_INVALID")


def edges(values):
    require(isinstance(values, list) and bool(values), "BUCKET_EDGES_REQUIRED")
    out = [number(v) for v in values]
    require(out == sorted(set(out)) and out[0] >= 0, "BUCKET_EDGES_INVALID")
    return out


def validate_metadata(meta):
    require(meta.get("schema_version") == 1, "SCHEMA_VERSION_INVALID")
    require(meta.get("comparison_kind") == "signal_price_basis", "COMPARISON_KIND_UNSUPPORTED")
    arms = [meta.get(key, {}) for key in ("baseline", "candidate")]
    for arm in arms:
        require(arm.get("price_basis") in BASES, "EXPLICIT_PRICE_BASIS_REQUIRED")
        require(re.fullmatch(r"[0-9a-f]{40}", arm.get("platform_revision", "")), "PLATFORM_REVISION_REQUIRED")
        require(re.fullmatch(r"[0-9a-f]{40}", arm.get("strategy_revision", "")), "STRATEGY_REVISION_REQUIRED")
        require(SHA.fullmatch(arm.get("effective_config_sha256", "")), "EFFECTIVE_CONFIG_REQUIRED")
    require(arms[0]["price_basis"] != arms[1]["price_basis"], "BASES_MUST_DIFFER")
    # A basis experiment changes only the input series, not code/config/costs.
    for key in ("platform_revision", "strategy_revision", "effective_config_sha256"):
        require(arms[0][key] == arms[1][key], "BASIS_EXPERIMENT_CONFOUNDED")
    for key in ("data_manifest_sha256", "calendar_sha256", "replay_code_sha256", "cost_model_sha256",
                "plugin_binding_sha256", "initial_state_sha256", "external_flows_sha256"):
        require(SHA.fullmatch(meta.get("shared", {}).get(key, "")), "SHARED_REPLAY_IDENTITY_REQUIRED")
    require(meta.get("timing") in ("next_open", "next_session", "completed_session_before_fill"),
            "CAUSAL_FILL_CLOCK_REQUIRED")
    costs = meta.get("cost_components", {})
    require(set(costs) == COST_COMPONENTS, "UBPS_COST_COMPONENTS_REQUIRED")
    for value in costs.values():
        require(isinstance(value, dict) and value.get("status") in ("modeled", "not_applicable")
                and isinstance(value.get("reason_or_version"), str) and bool(value["reason_or_version"]),
                "UBPS_COST_EXPLANATION_REQUIRED")
    require(meta.get("unitized_twr") is True, "EXTERNAL_FLOW_NEUTRAL_RETURNS_REQUIRED")
    require(meta.get("cost_feedback") == "self_financing", "COST_FEEDBACK_REQUIRED")
    require(meta.get("turnover_definition") == "security_one_way_notional_over_prefill_nav",
            "TURNOVER_DEFINITION_REQUIRED")
    require(type(meta.get("thresholds_applicable")) is bool, "THRESHOLD_APPLICABILITY_REQUIRED")
    require(meta.get("corporate_action_coverage_complete") is True, "CORPORATE_ACTION_COVERAGE_REQUIRED")
    limits = meta.get("limits", {})
    require(0 < number(limits.get("total")) <= 1 and isinstance(limits.get("symbols"), dict),
            "LIMITS_REQUIRED")
    for cap in limits["symbols"].values():
        require(0 <= number(cap) <= limits["total"], "LIMITS_INVALID")
    edges(meta.get("size_bucket_edges"))
    edges(meta.get("threshold_bucket_edges"))


def daily_comparison(row, size_edges):
    a, b = row["baseline"], row["candidate"]
    wa, wb = a["weights"], b["weights"]
    delta = [abs(wa.get(s, 0) - wb.get(s, 0)) for s in set(wa) | set(wb)] or [0.0]
    da, db = decisions(a["orders"], size_edges), decisions(b["orders"], size_edges)
    symbols = set(da) | set(db)
    signal_keys = set(a["signals"]) | set(b["signals"])
    require(set(a["signals"]) == set(b["signals"]), "SIGNAL_COVERAGE_MISMATCH")
    position_diff = any(v != 0 for v in delta)
    order_diff = any(a["orders"].get(s, 0) != b["orders"].get(s, 0) for s in symbols)
    return {
        "weight_delta": max(delta), "weight_l1": sum(delta),
        "asset_day_deltas": delta,
        "cash_weight_delta": abs(sum(wa.values()) - sum(wb.values())),
        "decision_diff": position_diff or order_diff,
        "position_diff": position_diff,
        "order_size_diff": any(abs(a["orders"].get(s, 0)) != abs(b["orders"].get(s, 0)) for s in symbols),
        "trade_vs_no_trade_diff": bool(da) != bool(db),
        "direction_diff": any(da.get(s, (0, 0))[0] != db.get(s, (0, 0))[0] for s in symbols),
        "size_bucket_diff": any(da.get(s, (0, -1))[1] != db.get(s, (0, -1))[1] for s in symbols),
        "signal_diff": any(a["signals"][s] != b["signals"][s] for s in signal_keys),
    }


def summarize(rows, size_edges):
    comparisons = [daily_comparison(row, size_edges) for row in rows]
    out = {"sessions": len(rows)}
    for key in ("decision_diff", "position_diff", "order_size_diff", "trade_vs_no_trade_diff",
                "direction_diff", "size_bucket_diff", "signal_diff"):
        out[key + "_days"] = sum(c[key] for c in comparisons)
    out["daily_max_abs_weight_delta"] = quantiles([c["weight_delta"] for c in comparisons])
    out["daily_l1_weight_delta"] = quantiles([c["weight_l1"] for c in comparisons])
    out["cash_weight_delta"] = quantiles([c["cash_weight_delta"] for c in comparisons])
    out["asset_day_abs_weight_delta"] = quantiles([v for c in comparisons for v in c["asset_day_deltas"]])
    return out


def performance(rows, arm, field):
    # TWR unit values include initial NAV so the first loss affects drawdown.
    nav = peak = 1.0
    drawdown = 0.0
    for row in rows:
        nav *= 1 + row[arm][field]
        require(math.isfinite(nav) and nav > 0, "NAV_INVALID")
        peak = max(peak, nav)
        drawdown = min(drawdown, nav / peak - 1)
    years = ((day(rows[-1]["session"]) - day(rows[0]["session"])).days + 1) / 365.25
    return {"total_return": nav - 1, "trade_count": sum(row[arm]["trade_count"] for row in rows),
            "cagr": nav ** (1 / years) - 1, "max_drawdown": drawdown,
            "turnover_sum": sum(row[arm]["turnover"] for row in rows)}


def compare(meta, bundle, expected_sessions):
    validate_metadata(meta)
    require(isinstance(bundle, dict) and isinstance(bundle.get("rows"), list), "BUNDLE_INVALID")
    as_of, inception, two_years = reporting_dates(meta)
    start = min(inception, two_years) if inception else two_years
    expected = [s for s in expected_sessions if start <= day(s) <= as_of]
    require(bool(expected), "CALENDAR_EMPTY")
    require(expected == sorted(set(expected)), "CALENDAR_SESSIONS_INVALID")
    rows = bundle["rows"]
    require([r.get("session") for r in rows] == expected, "SESSION_COVERAGE_INCOMPLETE")
    for row in rows:
        for arm in ("baseline", "candidate"):
            validate_arm(row.get(arm, {}), meta["limits"])
            require(bool(row[arm]["threshold_distances"]) == meta["thresholds_applicable"],
                    "THRESHOLD_COVERAGE_MISMATCH")
    size_edges = meta["size_bucket_edges"]
    threshold_edges = meta["threshold_bucket_edges"]
    windows = {}
    for name, lower in (("trailing_two_years", two_years), ("since_inception", inception)):
        if lower is None:
            windows[name] = {"status": "NOT_COMPUTED", "reason": "INCEPTION_UNKNOWN",
                             "sessions": None, "performance": None}
            continue
        subset = [r for r in rows if day(r["session"]) >= lower]
        require(bool(subset), "WINDOW_EMPTY")
        stats = {"status": "AGGREGATED", **summarize(subset, size_edges)}
        stats["performance"] = {}
        for field in ("gross_return", "net_return_1x", "net_return_2x"):
            a, b = (performance(subset, arm, field) for arm in ("baseline", "candidate"))
            stats["performance"][field] = {
                "baseline": a, "candidate": b, "delta": {k: b[k] - a[k] for k in a},
            }
        events = bundle.get("corporate_actions")
        require(isinstance(events, list), "CORPORATE_ACTION_COVERAGE_REQUIRED")
        event_windows = {}
        for kind in ("dividend", "split"):
            dates = set()
            count = 0
            for event in events:
                require(event.get("kind") in ("dividend", "split") and event.get("session") in expected_sessions,
                        "CORPORATE_ACTION_INVALID")
                if event["kind"] != kind:
                    continue
                if day(event["session"]) > as_of:
                    continue
                index = expected_sessions.index(event["session"])
                dates.update(expected_sessions[max(0, index - 5):index + 6])
                count += lower <= day(event["session"]) <= as_of
            selected = [r for r in subset if r["session"] in dates]
            event_windows[kind] = {"events_in_window": count, **summarize(selected, size_edges)}
        stats["corporate_action_windows"] = event_windows
        grouped = [[] for _ in range(len(threshold_edges) + 1)]
        missing = 0
        for row in subset:
            distances = row["baseline"]["threshold_distances"]
            if not distances:
                missing += 1
            else:
                grouped[bucket(min(abs(v) for v in distances.values()), threshold_edges)].append(row)
        stats["threshold_buckets"] = [summarize(group, size_edges) for group in grouped]
        stats["threshold_not_applicable_days"] = missing
        windows[name] = stats
    return {"status": "AGGREGATED_REPLAY_INPUTS_NOT_EXECUTION_CERTIFICATION", "windows": windows}


def cloud_guard():
    require(platform.system() == "Linux" and os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_OS") == "Linux"
            and os.environ.get("GITHUB_REF") == "refs/heads/" + os.environ.get("GITHUB_DEFAULT_BRANCH", "")
            and bool(os.environ.get("GITHUB_DEFAULT_BRANCH")), "CLOUD_DEFAULT_BRANCH_REQUIRED")


def reference(ref, root):
    require(isinstance(ref, dict), "OBJECT_REFERENCE_REQUIRED")
    require(isinstance(root, str) and re.fullmatch(r"gs://[a-z0-9][a-z0-9._-]+/research/[A-Za-z0-9._/-]+", root),
            "APPROVED_RESEARCH_INPUT_ROOT_REQUIRED")
    parsed = urlsplit(root.rstrip("/"))
    require(all(p not in ("", ".", "..") for p in parsed.path[1:].split("/")), "OBJECT_PATH_INVALID")
    object_name = ref.get("object")
    require(isinstance(object_name, str) and all(p not in ("", ".", "..") for p in object_name.split("/")),
            "OBJECT_PATH_INVALID")
    require(object_name.startswith(parsed.path[1:] + "/"), "OBJECT_OUTSIDE_INPUT_ROOT")
    require(str(ref.get("generation", "")).isdigit() and int(ref["generation"]) > 0,
            "OBJECT_GENERATION_REQUIRED")
    require(isinstance(ref.get("sha256"), str) and SHA.fullmatch(ref["sha256"]), "OBJECT_DIGEST_REQUIRED")
    return parsed.netloc, object_name


def validate_manifest(manifest, root):
    require(isinstance(manifest, dict) and manifest.get("schema_version") == 1, "SCHEMA_VERSION_INVALID")
    targets = manifest.get("targets", [])
    require(isinstance(targets, list) and 1 <= len(targets) <= 32, "TARGET_COUNT_INVALID")
    ids = set()
    for target in targets:
        require(isinstance(target, dict), "TARGET_INVALID")
        alias = target.get("alias", "")
        require(isinstance(alias, str) and re.fullmatch(r"target-[1-9][0-9]?", alias) and alias not in ids,
                "TARGET_ALIAS_INVALID")
        ids.add(alias)
        require(isinstance(target.get("metadata"), dict), "REPLAY_METADATA_REQUIRED")
        validate_metadata(target["metadata"])
        reporting_dates(target["metadata"])
        reference(target.get("replay_bundle"), root)
    return targets


def read_object(ref, root, client):
    bucket_name, object_name = reference(ref, root)
    try:
        generation = int(ref["generation"])
        blob = client.bucket(bucket_name).blob(object_name, generation=generation)
        blob.reload(if_generation_match=generation, retry=None, timeout=30)
        require(0 < blob.size <= MAX_OBJECT_BYTES, "OBJECT_SIZE_LIMIT")
        body = blob.download_as_bytes(if_generation_match=generation, retry=None, timeout=60)
    except ResearchStop:
        raise
    except Exception:
        raise ResearchStop("CLOUD_INPUT_READ_FAILED_NO_RETRY") from None
    require(hashlib.sha256(body).hexdigest() == ref["sha256"], "OBJECT_DIGEST_MISMATCH")
    try:
        return json.loads(body)
    except (ValueError, UnicodeError):
        raise ResearchStop("INPUT_JSON_INVALID") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    try:
        cloud_guard()
        root = os.environ.get("PRICE_BASIS_INPUT_ROOT", "")
        ref = {"object": os.environ.get("PRICE_BASIS_MANIFEST_OBJECT", ""),
               "generation": os.environ.get("PRICE_BASIS_MANIFEST_GENERATION", ""),
               "sha256": os.environ.get("PRICE_BASIS_MANIFEST_SHA256", "")}
        reference(ref, root)
        if args.preflight:
            print('{"status":"PREFLIGHT_VALIDATED_NO_DATA_READ"}')
            return
        import exchange_calendars as xcals
        from google.cloud import storage
        from google.oauth2.credentials import Credentials

        # The approved auth action supplies a masked, short-lived token directly
        # to this client; no credential file, SDK login or token persistence.
        client = storage.Client(project=os.environ["GOOGLE_CLOUD_PROJECT"],
                                credentials=Credentials(os.environ["GOOGLE_OAUTH_ACCESS_TOKEN"]))
        manifest = read_object(ref, root, client)
        # Validate the complete reviewed read set before fetching any ledger.
        targets = validate_manifest(manifest, root)
        results = []
        for target in targets:
            # Public-safe aliases only; no private target names are echoed.
            alias = target["alias"]
            metadata = target["metadata"]
            as_of, inception, two_years = reporting_dates(metadata)
            lower = min(inception, two_years) if inception else two_years
            # Extend beyond both reporting boundaries for +-5 trading-day action windows.
            calendar = xcals.get_calendar(metadata["calendar"], start=lower - timedelta(days=15),
                                         end=as_of + timedelta(days=15))
            sessions = [s.date().isoformat() for s in calendar.sessions_in_range(
                lower - timedelta(days=15), as_of + timedelta(days=15))]
            result = compare(metadata, read_object(target["replay_bundle"], root, client), sessions)
            results.append({"alias": alias, "baseline_price_basis": metadata["baseline"]["price_basis"],
                            "candidate_price_basis": metadata["candidate"]["price_basis"],
                            "size_bucket_edges": metadata["size_bucket_edges"],
                            "threshold_bucket_edges": metadata["threshold_bucket_edges"], **result})
        output = {"schema_version": 1, "manifest_sha256": ref["sha256"],
                  "price_basis_vocabulary": sorted(BASES), "targets": results}
        destination = Path(os.environ["RUNNER_TEMP"]) / "price-basis-aggregate.json"
        destination.write_bytes(canonical(output))
        print(json.dumps({"status": "AGGREGATES_WRITTEN", "target_count": len(results)}))
    except ResearchStop as exc:
        print(json.dumps({"status": "STOP", "reason": str(exc)}))
        raise SystemExit(1) from None
    except Exception:
        print('{"status":"STOP","reason":"INPUT_OR_RUNTIME_INVALID"}')
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
