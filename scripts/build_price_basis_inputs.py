"""Local-only equity replay, paired bundles and review-bound input manifests.

Strategy adapters are explicit offline Python callables. No broker, provider,
credential or cloud client is imported here. Options require another qualified
joint-accounting engine; they cannot be silently switched off for this engine.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import inspect
import json
import math
import re
import sys
from pathlib import Path

import compare_price_basis as consumer

BASELINE = "split_dividend_adjusted"
CANDIDATE = "split_adjusted"


def require(condition, code):
    consumer.require(condition, code)


def digest(value):
    return hashlib.sha256(consumer.canonical(value)).hexdigest()


def complete(value):
    """Reject unresolved declarations recursively, including optional nulls."""
    require(value is not None, "UNRESOLVED_FIELD")
    if isinstance(value, str):
        require(value.strip().lower() not in ("", "unknown", "null", "none", "tbd"), "UNRESOLVED_FIELD")
    elif isinstance(value, dict):
        for item in value.values():
            complete(item)
    elif isinstance(value, list):
        for item in value:
            complete(item)
    elif type(value) in (int, float):
        consumer.number(value)
    else:
        require(type(value) is bool, "FIELD_TYPE_INVALID")


def positive(value):
    value = consumer.number(value)
    require(value > 0, "POSITIVE_NUMBER_REQUIRED")
    return value


def source_identity(adapter):
    path = inspect.getsourcefile(adapter)
    require(path is not None, "ADAPTER_SOURCE_REQUIRED")
    return {"engine_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "adapter_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
            "adapter_name": adapter.__qualname__, "python": sys.version.split()[0]}


def qualify(inputs, adapter):
    identity = source_identity(adapter)
    payload = {k: v for k, v in inputs.items() if k != "review"}
    identities = {**identity, "input_sha256": digest(payload)}
    if inputs["artifact_kind"] == "SYNTHETIC":
        return {"status": "SYNTHETIC_ONLY", **identities}
    require(inputs["artifact_kind"] == "REVIEWED_REAL_INPUTS", "INPUT_KIND_REQUIRED")
    review = inputs.get("review", {})
    require(review.get("status") == "PASS" and review.get("scope") == "equity_only"
            and isinstance(review.get("reviewer"), str) and bool(review["reviewer"].strip()),
            "PRODUCER_REVIEW_REQUIRED")
    require(all(review.get(k) == v for k, v in identities.items()), "PRODUCER_REVIEW_IDENTITY_MISMATCH")
    for key in ("dependency_lock_sha256", "target_evidence_sha256"):
        require(isinstance(review.get(key), str) and consumer.SHA.fullmatch(review[key]),
                "PRODUCER_REVIEW_EVIDENCE_REQUIRED")
        require(review[key] == digest(inputs["plugin_binding"][key.removesuffix("_sha256")]),
                "PRODUCER_REVIEW_EVIDENCE_MISMATCH")
    consumer.day(review.get("reviewed_at"))
    return {"status": "REVIEW_BOUND_EQUITY_ONLY", **identities, "review": copy.deepcopy(review)}


def validate_inputs(inputs):
    require(isinstance(inputs, dict), "INPUTS_REQUIRED")
    complete(inputs)
    require(inputs.get("corporate_action_coverage_complete") is True, "CORPORATE_ACTION_COVERAGE_REQUIRED")
    config = inputs.get("config", {})
    require(config.get("options_enabled") is False, "JOINT_OPTIONS_ENGINE_REQUIRED")
    require(config.get("whole_shares") is True, "WHOLE_SHARES_REQUIRED")
    warmup = config.get("warmup_sessions")
    require(type(warmup) is int and warmup >= 2, "WARMUP_REQUIRED")
    require(consumer.number(config.get("min_notional")) >= 0, "MIN_NOTIONAL_REQUIRED")
    sessions = inputs["calendar"]["sessions"]
    require(isinstance(sessions, list) and sessions == sorted(set(sessions)), "CALENDAR_SESSIONS_INVALID")
    require(len(sessions) > warmup, "WARMUP_REQUIRED")
    for s in sessions:
        consumer.day(s)
    require(inputs["as_of"] == sessions[-1], "COMPLETED_SESSION_REQUIRED")
    market = inputs["market"]
    require([r["session"] for r in market] == sessions, "SESSION_COVERAGE_INCOMPLETE")
    symbols = set(config["limits"]["symbols"])
    require(bool(symbols), "SYMBOL_UNIVERSE_REQUIRED")
    for row in market:
        require(consumer.day(row["available_session"]) <= consumer.day(row["session"]), "FUTURE_INPUT")
        require(isinstance(row["features"], dict), "HISTORICAL_PLUGIN_INPUTS_REQUIRED")
        for bars in [row["raw_ohlc"], *[row["signal_ohlc"][b] for b in (BASELINE, CANDIDATE)]]:
            require(set(bars) == symbols, "PRICE_COVERAGE_INCOMPLETE")
            for bar in bars.values():
                require(set(bar) == {"open", "high", "low", "close"}, "OHLC_REQUIRED")
                for p in bar.values():
                    positive(p)
                require(bar["low"] <= min(bar["open"], bar["close"])
                        and bar["high"] >= max(bar["open"], bar["close"]), "OHLC_INVALID")
    initial = inputs["initial_state"]
    require(set(initial["shares"]) == set(initial["marks"]) == symbols, "INITIAL_STATE_REQUIRED")
    require(consumer.number(initial["cash"]) >= 0, "CASH_INVALID")
    for symbol in symbols:
        require(type(initial["shares"][symbol]) is int and initial["shares"][symbol] >= 0,
                "WHOLE_SHARES_REQUIRED")
        require(positive(initial["marks"][symbol]) == market[warmup - 1]["raw_ohlc"][symbol]["close"],
                "INITIAL_MARK_MISMATCH")
    require(set(inputs["external_flows"]) == set(sessions[warmup:]), "EXTERNAL_FLOW_COVERAGE_REQUIRED")
    for flow in inputs["external_flows"].values():
        consumer.number(flow)
    costs = inputs["costs"]
    require(set(costs) == consumer.COST_COMPONENTS, "UBPS_COST_COMPONENTS_REQUIRED")
    for key, value in costs.items():
        require(set(value) == {"status", "reason_or_version", "bps", "model"}
                and value["model"] == "linear_bps", "COST_MODEL_UNSUPPORTED")
        require(value["status"] in ("modeled", "not_applicable"), "COST_STATUS_INVALID")
        require(consumer.number(value["bps"]) >= 0, "COST_RATE_INVALID")
        if value["status"] == "not_applicable":
            require(value["bps"] == 0, "INAPPLICABLE_COST_NONZERO")
        if key in ("options", "borrow_margin"):
            require(value["status"] == "not_applicable", "COST_MODEL_UNSUPPORTED")
    events = inputs["corporate_actions"]
    require(isinstance(events, list), "CORPORATE_ACTION_COVERAGE_REQUIRED")
    seen = set()
    for event in events:
        key = (event["session"], event["symbol"], event["kind"])
        require(key not in seen and event["session"] in sessions and event["symbol"] in symbols,
                "CORPORATE_ACTION_INVALID")
        seen.add(key)
        require(event["kind"] in ("split", "dividend"), "CORPORATE_ACTION_INVALID")
        field = "ratio" if event["kind"] == "split" else "cash_per_share"
        positive(event[field])
    return sessions, symbols, warmup


def nav(state, marks):
    return state["cash"] + sum(q * marks[s] for s, q in state["shares"].items())


def step(state, row, decision, inputs, stress):
    """One next-open equity session; flows occur before fills, at unit NAV."""
    config, costs = inputs["config"], inputs["costs"]
    pre_flow = positive(nav(state, state["marks"]))
    flow = inputs["external_flows"][row["session"]]
    elapsed = (consumer.day(row["session"]) - consumer.day(state["session"])).days / 365.25
    # Carry accrues on inherited closing balances, before today's flows/fills.
    # Charging the post-fill portfolio would charge new Monday holdings for a weekend.
    invested = sum(state["shares"][s] * state["marks"][s] for s in state["shares"])
    state["cash"] += max(0, state["cash"]) * costs["cash_yield"]["bps"] / 10000 * elapsed
    state["cash"] -= invested * costs["fund_expenses"]["bps"] / 10000 * elapsed * stress
    state["cash"] += flow
    denominator = positive(pre_flow + flow)
    # Splits precede ex-dividend cash, both on inherited pre-fill holdings.
    events = [e for e in inputs["corporate_actions"] if e["session"] == row["session"]]
    for event in sorted(events, key=lambda e: e["kind"] != "split"):
        symbol = event["symbol"]
        if event["kind"] == "split":
            quantity = state["shares"][symbol] * event["ratio"]
            require(float(quantity).is_integer(), "FRACTIONAL_SPLIT_CASH_IN_LIEU_REQUIRED")
            state["shares"][symbol] = int(quantity)
        else:
            state["cash"] += state["shares"][symbol] * event["cash_per_share"]
    opens = {s: b["open"] for s, b in row["raw_ohlc"].items()}
    prefill_nav = positive(nav(state, opens))
    weights = decision["weights"]
    target = {s: math.floor(prefill_nav * weights.get(s, 0) / opens[s]) for s in opens}
    orders, fills, notional = {}, 0, 0.0
    rates = sum(costs[k]["bps"] for k in ("commission", "spread_slippage", "impact")) / 10000 * stress
    # Sells first release cash. Buys use their own cost scenario's remaining cash.
    deltas = {s: target[s] - state["shares"][s] for s in opens}
    for symbol in sorted(opens, key=lambda s: (deltas[s] >= 0, s)):
        quantity = deltas[symbol]
        if quantity > 0:
            quantity = min(quantity, max(0, math.floor(state["cash"] / (opens[symbol] * (1 + rates)))))
        value = quantity * opens[symbol]
        if quantity == 0 or abs(value) < config["min_notional"]:
            continue
        state["cash"] -= value + abs(value) * rates
        state["shares"][symbol] += quantity
        orders[symbol] = value / prefill_nav
        notional += abs(value)
        fills += 1
    require(state["cash"] >= -1e-8, "SELF_FINANCING_CASH_INVALID")
    state["marks"] = {s: b["close"] for s, b in row["raw_ohlc"].items()}
    state["session"] = row["session"]
    closing_nav = positive(nav(state, state["marks"]))
    actual_weights = {s: state["shares"][s] * opens[s] / prefill_nav for s in opens}
    return {**copy.deepcopy(decision), "weights": actual_weights, "orders": orders, "turnover": notional / prefill_nav,
            "trade_count": fills, "return": closing_nav / denominator - 1}


def produce(inputs, adapter):
    sessions, symbols, warmup = validate_inputs(inputs)
    qualification = qualify(inputs, adapter)
    config = inputs["config"]
    shared = {"data_manifest_sha256": digest({k: inputs[k] for k in ("market", "corporate_actions")}),
              "calendar_sha256": digest(inputs["calendar"]),
              "replay_code_sha256": digest({k: qualification[k] for k in
                                            ("engine_sha256", "adapter_sha256", "adapter_name", "python")}),
              "cost_model_sha256": digest(inputs["costs"]),
              "plugin_binding_sha256": digest(inputs["plugin_binding"]),
              "initial_state_sha256": digest(inputs["initial_state"]),
              "external_flows_sha256": digest(inputs["external_flows"])}
    identity = {**inputs["identity"], "effective_config_sha256": digest(config)}
    metadata = {"schema_version": 1, "comparison_kind": "signal_price_basis", "as_of": inputs["as_of"],
                "calendar": inputs["calendar"]["name"], "baseline": {**identity, "price_basis": BASELINE},
                "candidate": {**identity, "price_basis": CANDIDATE}, "shared": shared, "timing": "next_open",
                "unitized_twr": True, "cost_feedback": "self_financing",
                "turnover_definition": "security_one_way_notional_over_prefill_nav",
                "cost_components": {k: {f: v[f] for f in ("status", "reason_or_version")}
                                    for k, v in inputs["costs"].items()},
                "corporate_action_coverage_complete": inputs["corporate_action_coverage_complete"],
                **{k: config[k] for k in ("limits", "size_bucket_edges", "threshold_bucket_edges",
                                          "thresholds_applicable")}}
    if "inception" in inputs:
        metadata["inception"] = inputs["inception"]
    consumer.validate_metadata(metadata)
    as_of, inception, two_years = consumer.reporting_dates(metadata)
    boundaries = {"trailing_two_years": two_years}
    if inception is not None:
        boundaries["since_inception"] = inception
    opening_states = {key: {} for key in boundaries}
    states, records = {}, {}
    for name, basis in (("baseline", BASELINE), ("candidate", CANDIDATE)):
        states[name] = [{**copy.deepcopy(inputs["initial_state"]), "session": sessions[warmup - 1]}
                        for _ in range(3)]
        records[name] = []
        for index in range(warmup, len(sessions)):
            row = inputs["market"][index]
            for window, lower in boundaries.items():
                if consumer.day(row["session"]) >= lower and name not in opening_states[window]:
                    opening_states[window][name] = [
                        {**copy.deepcopy(state), "opening_nav": positive(nav(state, state["marks"]))}
                        for state in states[name]]
            history = [{"session": r["session"], "signal_ohlc": r["signal_ohlc"][basis],
                        "features": r["features"]} for r in inputs["market"][index - warmup:index]]
            scenarios = []
            for stress, state in enumerate(states[name]):
                context = {"fill_session": row["session"], "history": copy.deepcopy(history),
                           "state": copy.deepcopy(state), "config": copy.deepcopy(config),
                           "plugin_binding": copy.deepcopy(inputs["plugin_binding"])}
                decision = adapter(context)
                require(isinstance(decision, dict) and set(decision) ==
                        {"signals", "weights", "threshold_distances"}, "ADAPTER_DECISION_INVALID")
                require(set(decision["weights"]) <= symbols, "ADAPTER_UNIVERSE_INVALID")
                consumer.validate_arm({**decision, "orders": {}, "gross_return": 0, "net_return_1x": 0,
                                       "net_return_2x": 0, "turnover": 0, "trade_count": 0}, config["limits"])
                scenarios.append(step(state, row, decision, inputs, stress))
            # Consumer exposes decisions/fills of 1x. All three full paths retain state.
            observation = {k: v for k, v in scenarios[1].items() if k != "return"}
            observation.update(gross_return=scenarios[0]["return"], net_return_1x=scenarios[1]["return"],
                               net_return_2x=scenarios[2]["return"],
                               cost_paths=dict(zip(("gross_return", "net_return_1x", "net_return_2x"), scenarios)))
            consumer.validate_arm(observation, config["limits"])
            records[name].append(observation)
    as_of, inception, two_years = consumer.reporting_dates(metadata)
    lower = min(inception, two_years) if inception else two_years
    rows = [{"session": s, "baseline": records["baseline"][i], "candidate": records["candidate"][i]}
            for i, s in enumerate(sessions[warmup:]) if lower <= consumer.day(s) <= as_of]
    bundle = {"rows": rows, "corporate_actions": copy.deepcopy(inputs["corporate_actions"])}
    # This also refuses incomplete two-year coverage instead of inventing history.
    consumer.compare(metadata, bundle, sessions)
    product = {"schema_version": 1, "artifact_kind": inputs["artifact_kind"], "metadata": metadata,
               "bundle": bundle, "bundle_sha256": digest(bundle), "qualification": qualification,
               "audit": states, "window_opening_states": opening_states, "replay_sessions": sessions}
    product["product_sha256"] = digest(product)
    return product


def validate_qualification(evidence):
    require(isinstance(evidence, dict), "PRODUCER_EVIDENCE_REQUIRED")
    for key in ("engine_sha256", "adapter_sha256", "input_sha256"):
        require(isinstance(evidence.get(key), str) and consumer.SHA.fullmatch(evidence[key]),
                "PRODUCER_EVIDENCE_REQUIRED")
    require(isinstance(evidence.get("adapter_name"), str) and isinstance(evidence.get("python"), str),
            "PRODUCER_EVIDENCE_REQUIRED")
    require(evidence.get("status") in ("SYNTHETIC_ONLY", "REVIEW_BOUND_EQUITY_ONLY"),
            "REAL_QUALIFICATION_REQUIRED")
    if evidence["status"] == "REVIEW_BOUND_EQUITY_ONLY":
        review = evidence.get("review", {})
        require(review.get("status") == "PASS" and review.get("scope") == "equity_only"
                and bool(review.get("reviewer")), "PRODUCER_REVIEW_REQUIRED")
        for key in ("engine_sha256", "adapter_sha256", "adapter_name", "python", "input_sha256"):
            require(review.get(key) == evidence[key], "PRODUCER_REVIEW_IDENTITY_MISMATCH")
        for key in ("dependency_lock_sha256", "target_evidence_sha256"):
            require(isinstance(review.get(key), str) and consumer.SHA.fullmatch(review[key]),
                    "PRODUCER_REVIEW_EVIDENCE_REQUIRED")
        consumer.day(review.get("reviewed_at"))


def validate_product(product):
    complete(product)
    validate_qualification(product.get("qualification"))
    evidence = product["qualification"]
    require(product["metadata"]["shared"]["replay_code_sha256"] == digest({k: evidence[k] for k in
            ("engine_sha256", "adapter_sha256", "adapter_name", "python")}), "REPLAY_CODE_IDENTITY_MISMATCH")
    require(product["bundle_sha256"] == digest(product["bundle"]), "BUNDLE_DIGEST_MISMATCH")
    require(product["product_sha256"] == digest({k: v for k, v in product.items() if k != "product_sha256"}),
            "PRODUCT_DIGEST_MISMATCH")
    return consumer.compare(product["metadata"], product["bundle"], product["replay_sessions"])


def build_manifest(entries, root, synthetic=False):
    complete(entries)
    targets, ready = [], 0
    for entry in entries:
        product = json.loads(Path(entry["product_path"]).read_bytes())
        result = validate_product(product)
        ref = entry["replay_bundle"]
        require(ref["sha256"] == product["bundle_sha256"], "BUNDLE_DIGEST_MISMATCH")
        consumer.reference(ref, root)
        if not synthetic:
            require(product["artifact_kind"] == "REVIEWED_REAL_INPUTS" and
                    product["qualification"]["status"] == "REVIEW_BOUND_EQUITY_ONLY", "REAL_QUALIFICATION_REQUIRED")
            # The review record binds the actual generated metadata, ledger and object reference.
            review = entry.get("review", {})
            require(review.get("status") == "PASS" and bool(review.get("reviewer"))
                    and review.get("product_sha256") == product["product_sha256"]
                    and review.get("metadata_sha256") == digest(product["metadata"])
                    and review.get("replay_bundle_reference_sha256") == digest(ref), "BUNDLE_REVIEW_REQUIRED")
            consumer.day(review.get("reviewed_at"))
        ready += sum(v["status"] == "AGGREGATED" for v in result["windows"].values())
        targets.append({"alias": entry["alias"], "metadata": product["metadata"], "replay_bundle": ref,
                        "producer_evidence": product["qualification"], "product_sha256": product["product_sha256"],
                        **({"bundle_review": entry["review"]} if not synthetic else {})})
    manifest = {"schema_version": 1, "targets": targets, "dispatch_ready": False,
                "artifact_kind": "SYNTHETIC_NO_DISPATCH" if synthetic else "REVIEWED_OFFLINE_MANIFEST",
                "structurally_ready_windows": ready}
    # Use the unchanged consumer contract; synthetic dispatch is rejected separately.
    consumer.validate_manifest({"schema_version": 1, "targets": targets}, root)
    complete(manifest)
    validate_reviewed_manifest(manifest, root, synthetic=synthetic)
    return manifest


def validate_reviewed_manifest(manifest, root, synthetic=False):
    complete(manifest)
    kind = "SYNTHETIC_NO_DISPATCH" if synthetic else "REVIEWED_OFFLINE_MANIFEST"
    require(manifest.get("artifact_kind") == kind and manifest.get("dispatch_ready") is False,
            "OFFLINE_MANIFEST_KIND_REQUIRED")
    targets = consumer.validate_manifest({"schema_version": manifest.get("schema_version"),
                                          "targets": manifest.get("targets")}, root)
    for target in targets:
        evidence = target.get("producer_evidence", {})
        validate_qualification(evidence)
        if not synthetic:
            require(evidence.get("status") == "REVIEW_BOUND_EQUITY_ONLY", "REAL_QUALIFICATION_REQUIRED")
            review = target.get("bundle_review", {})
            require(review.get("status") == "PASS" and bool(review.get("reviewer"))
                    and review.get("product_sha256") == target.get("product_sha256")
                    and review.get("metadata_sha256") == digest(target["metadata"])
                    and review.get("replay_bundle_reference_sha256") == digest(target["replay_bundle"]),
                    "BUNDLE_REVIEW_REQUIRED")
            consumer.day(review.get("reviewed_at"))
    expected_windows = sum(1 + (consumer.reporting_dates(t["metadata"])[1] is not None) for t in targets)
    require(type(manifest.get("structurally_ready_windows")) is int
            and manifest["structurally_ready_windows"] == expected_windows, "WINDOW_COUNT_INVALID")
    return targets


def load_adapter(path, name):
    spec = importlib.util.spec_from_file_location("offline_basis_adapter", path)
    require(spec is not None and spec.loader is not None and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name),
            "ADAPTER_REQUIRED")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    adapter = getattr(module, name)
    require(callable(adapter), "ADAPTER_REQUIRED")
    return adapter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("produce", "bundle", "manifest", "validate", "validate-manifest"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--input-root")
    parser.add_argument("--adapter", type=Path)
    parser.add_argument("--function", default="decide")
    parser.add_argument("--synthetic", action="store_true")
    args = parser.parse_args()
    try:
        payload = json.loads(args.input.read_bytes())
        if args.operation == "produce":
            require(args.adapter is not None, "ADAPTER_REQUIRED")
            output = produce(payload, load_adapter(args.adapter, args.function))
        elif args.operation == "bundle":
            validate_product(payload)
            output = payload["bundle"]
        elif args.operation == "validate-manifest":
            targets = validate_reviewed_manifest(payload, args.input_root, synthetic=args.synthetic)
            output = {"status": "OFFLINE_MANIFEST_VALID", "target_count": len(targets), "dispatch_ready": False}
        elif args.operation == "validate":
            output = validate_product(payload)
        else:
            output = build_manifest(payload["entries"], payload["input_root"], synthetic=args.synthetic)
        require(args.output is not None, "LOCAL_OUTPUT_REQUIRED")
        # Exclusive creation prevents accidental overwrite of reviewed inputs.
        with args.output.open("xb") as stream:
            stream.write(consumer.canonical(output))
        print('{"status":"OFFLINE_OUTPUT_WRITTEN","dispatch_ready":false}')
    except consumer.ResearchStop as exc:
        print(json.dumps({"status": "STOP", "reason": str(exc)}))
        raise SystemExit(1) from None
    except Exception:
        print('{"status":"STOP","reason":"LOCAL_INPUT_OR_RUNTIME_INVALID"}')
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
