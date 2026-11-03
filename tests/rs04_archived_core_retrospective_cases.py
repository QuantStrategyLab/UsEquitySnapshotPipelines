"""Explicit research-overlay acceptance. All market observations are artificial."""
import importlib.util
import copy
import math
import json
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta
from functools import wraps
from pathlib import Path
from threading import Barrier, Lock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import rs04_archived_core_retrospective as study  # noqa: E402


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*_args, **_kwargs):
        raise AssertionError("OFFLINE_NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket, "create_connection", denied)
    monkeypatch.setattr(socket.socket, "connect", denied)


def test_installed_overlay_is_real():
    identities, digest = study.verify_runtime()
    assert identities["us-equity-strategies"]["commit"] == study.UES_COMMIT
    assert identities["quant-platform-kit"]["commit"] == study.QPK_COMMIT
    assert len(digest) == 64
    assert importlib.util.find_spec("quant_platform_kit.research_stats.unified_report")


@pytest.mark.parametrize("event,ref,attempt", [
    ("pull_request", "refs/heads/main", "1"), ("push", "refs/heads/main", "1"),
    ("workflow_dispatch", "refs/heads/topic", "1"), ("workflow_dispatch", "refs/heads/main", "2"),
])
def test_cloud_gate_rejects_unapproved_context(event, ref, attempt):
    with pytest.raises(study.Stop):
        study.cloud_gate({"GITHUB_EVENT_NAME": event, "GITHUB_REF": ref, "GITHUB_RUN_ATTEMPT": attempt})


class FakeClient:
    """Exact-byte GCS-shaped port; no credentials, lists, providers or network."""
    object_lock = Lock()

    def __init__(self):
        self.objects, self.calls = {}, []
        self.fail_read = self.fail_write = False
        self.unknown_generation = False
        self.write_then_raise = self.corrupt_readback = False
        self.created = set()
        self.fail_control_body = False
        self.on_read = None

    def bucket(self, name):
        client = self

        class Bucket:
            def blob(self, path, generation=None):
                class Blob:
                    def __init__(self):
                        self.generation = generation
                        self.size = None

                    def reload(self, **kwargs):
                        with client.object_lock:
                            client.calls.append(("metadata", name, path, kwargs))
                            assert kwargs == {"if_generation_match": int(self.generation), "retry": None, "timeout": 30}
                            if client.fail_read:
                                raise PermissionError("PRIVATE_CONTROL_METADATA_SENTINEL")
                            body, gen = client.objects[name, path]
                            assert int(gen) == int(self.generation)
                            self.size = len(body)

                    def download_as_bytes(self, **kwargs):
                        with client.object_lock:
                            client.calls.append(("read", name, path, kwargs))
                            assert kwargs["retry"] is None
                            assert kwargs["if_generation_match"] == int(self.generation)
                            if client.fail_read or (client.fail_control_body and "/control/" in path):
                                raise PermissionError("PRIVATE_URI_SECRET_SENTINEL")
                            body, gen = client.objects[name, path]
                            assert int(gen) == int(self.generation)
                            if client.corrupt_readback and (name, path) in client.created:
                                return body[:-1] + b"X"
                            if client.on_read:
                                client.on_read(path)
                            return body

                    def upload_from_string(self, body, **kwargs):
                        with client.object_lock:
                            client.calls.append(("create", name, path, kwargs))
                            assert kwargs["if_generation_match"] == 0 and kwargs["retry"] is None
                            if (name, path) in client.objects or client.fail_write:
                                raise PermissionError("PRIVATE_WRITE_SECRET_SENTINEL")
                            client.objects[name, path] = body, "90000001"
                            client.created.add((name, path))
                            if client.write_then_raise:
                                raise OSError("PRIVATE_AMBIGUOUS_WRITE_SENTINEL")
                            self.generation = None if client.unknown_generation else "90000001"
                return Blob()
        return Bucket()

    def put(self, uri, body, generation):
        self.objects[study.gcs_uri(uri)] = body, generation
        return {"uri": uri, "generation": generation, "bytes": len(body), "sha256": study.sha(body)}


@pytest.fixture(scope="module")
def artificial_archive():
    client = FakeClient()
    root = "gs://synthetic-only/unit/"
    base = root + "archive/"
    entries, leaves = [], []
    sessions = study.calendar_sessions(date(2021, 1, 4), study.END)
    for symbol in study.SYMBOLS:
        start = date(2022, 12, 28) if symbol == "BOXX" else sessions[0]
        grid = [d for d in sessions if d >= start]
        # Smooth nonconstant artificial path; creates actual strategy fills.
        rows = []
        for i, d in enumerate(grid):
            close = round(100 + .03 * i + math.sin(i / 19) * 5, 4)
            rows.append({"t": d.isoformat() + "T21:00:00Z", "o": close, "h": close,
                         "l": close, "c": close, "v": 1})
        raw = study.encode({"symbol": symbol, "bars": rows, "next_page_token": None,
                            "private_raw_sentinel": "RAW_PRICE_ACCOUNT_SECRET_SENTINEL"})
        action = study.encode({"corporate_actions": {"cash_dividends": [
            {"id": symbol + "-div-1", "symbol": symbol, "ex_date": "2023-04-03", "rate": .25}]},
            "next_page_token": None})
        for kind, body in (("raw", raw), ("actions", action)):
            generation = str(1000 + len(leaves))
            ref = client.put(base + f"alpaca/{symbol}/{kind}/page-001.json", body, generation)
            leaves.append((symbol, kind, generation, len(body)))
            entry = {"symbol": symbol, "pages": [ref]}
            if kind == "raw":
                entry |= {"price_basis": "raw_unadjusted", "vendor_adjustment": "raw", "source_type": "real",
                          "currency": "USD", "calendar_id": "XNYS", "calendar_version": "4.13.2",
                          "inception": str(start), "coverage": {"requested_start": str(start), "requested_end": str(study.END)}}
            else:
                entry["type"] = "corporate_actions"
            entries.append(entry)
    inventory = {"schema_version": "research_input_inventory.v1", "alpaca": entries}
    ref = client.put(base + "input-inventory.json", study.encode(inventory), "9999")
    contract = {"inventory": study.identity(ref), "leaves": tuple(leaves),
                "inventory_uri_sha256": study.sha(ref["uri"].encode()),
                "control_scope": {"bucket_sha256": study.sha(b"synthetic-only"),
                                  "research_root_uri_sha256": study.sha(root.encode())}}
    plan = {"schema": "qsl.archived_core_retrospective_plan.v2", "study_id": study.STUDY, "plan_nonce": B1_NONCE,
            "expires_at": (datetime.now(UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window": {"start": str(study.START), "end": str(study.END)}, "inventory": ref,
            "output_prefix": study.nonce_output_prefix(root, B1_NONCE, contract), "execution_approved": True,
            "cloud_cap_usd": .1, "estimated_cloud_usd": .07835, "retention_estimate_days": 90,
            "soft_delete_days": 7, "retention_conditions_unchanged": True}
    return client, root, inventory, ref, contract, plan


@pytest.fixture
def archive(artificial_archive):
    return copy.deepcopy(artificial_archive)


def test_fixed_real_metadata_contract():
    assert len(study.ARCHIVE["leaves"]) == 12
    assert sum(row[3] for row in study.ARCHIVE["leaves"]) == 1473572
    assert sum(row[3] for row in study.ARCHIVE["leaves"]) + study.ARCHIVE["inventory"]["bytes"] == 1546957
    assert study.ARCHIVE["inventory"]["generation"] == "1792044148695461"


@pytest.mark.parametrize("mutation", ["generation", "bytes", "sha256", "prefix", "symbol", "basis", "duplicate", "missing"])
def test_inventory_and_page_identity_reject_before_leaf_reads(archive, mutation):
    client, _root, inv, ref, contract, _plan = archive
    entry = inv["alpaca"][0]
    if mutation in ("generation", "bytes", "sha256"):
        entry["pages"][0][mutation] = {"generation": "2000", "bytes": 1, "sha256": ""}[mutation]
    elif mutation == "prefix":
        entry["pages"][0]["uri"] = "gs://other/private/page-001.json"
    elif mutation == "symbol":
        entry["symbol"] = "PRIVATE_SENTINEL"
    elif mutation == "basis":
        entry["price_basis"] = "adjusted"
    elif mutation == "duplicate":
        inv["alpaca"].append(copy.deepcopy(entry))
    else:
        inv["alpaca"].pop()
    with pytest.raises(study.Stop):
        study.select_pages(inv, ref, contract)
    assert client.calls == []


@pytest.mark.parametrize("change", ["unapproved", "cost", "retention", "inventory", "location", "prefix", "window", "extra"])
def test_control_plan_rejects_scope_changes(archive, change):
    _client, root, _inv, _ref, contract, plan = archive
    if change == "unapproved":
        plan["execution_approved"] = False
    elif change == "cost":
        plan["cloud_cap_usd"] = .11
    elif change == "retention":
        plan["soft_delete_days"] = 8
    elif change == "inventory":
        plan["inventory"]["sha256"] = "f" * 64
    elif change == "location":
        plan["inventory"]["uri"] = root + "other/input-inventory.json"
    elif change == "prefix":
        plan["output_prefix"] = plan["inventory"]["uri"].rsplit("/", 1)[0] + f"/{study.STUDY}/12345/"
    elif change == "window":
        plan["window"]["start"] = "2020-01-01"
    else:
        plan["optimize"] = True
    with pytest.raises(study.Stop):
        study.validate_plan(plan, root, "12345", contract)


def bodies_for(client, selected):
    return {key: study.decode(client.objects[study.gcs_uri(entry["pages"][0]["uri"])][0])
            for key, entry in selected.items()}


@pytest.mark.parametrize("change", ["duplicate", "missing", "unordered", "timezone", "nonfinite", "negative"])
def test_raw_bars_fail_closed_without_fill(archive, change):
    client, _root, inv, ref, contract, _plan = archive
    selected = study.select_pages(inv, ref, contract)
    bodies = bodies_for(client, selected)
    rows = bodies["SOXL", "raw"]["bars"]
    if change == "duplicate":
        rows.insert(1, rows[0])
    elif change == "missing":
        rows.pop(10)
    elif change == "unordered":
        rows[0], rows[1] = rows[1], rows[0]
    elif change == "timezone":
        rows[0]["t"] = rows[0]["t"][:-1]
    else:
        rows[0]["c"] = math.inf if change == "nonfinite" else -1
    with pytest.raises(study.Stop):
        study.normalize(selected, bodies)


def dividend(native="one", **extra):
    row = {"symbol": "TQQQ", "ex_date": "2023-04-03", "rate": 1.0} | extra
    if native is not None:
        row["id"] = native
    return row


def action_page(rows, kind="cash_dividends"):
    return {"corporate_actions": {kind: rows}, "next_page_token": None}


@pytest.mark.parametrize("rows,kind", [
    ([dividend(), dividend()], "cash_dividends"),
    ([dividend(), dividend(rate=2)], "cash_dividends"),
    ([dividend(None), dividend("two")], "cash_dividends"),
    ([dividend(None), dividend(None)], "cash_dividends"),
    ([dividend(currency="EUR")], "cash_dividends"),
    ([dividend(cancelled=True)], "cash_dividends"),
    ([dividend(status="replaced")], "cash_dividends"),
    ([dividend(symbol="SOXL")], "cash_dividends"),
    ([dividend(rate=math.nan)], "cash_dividends"),
    ([dividend()], "reverse_splits"),
])
def test_event_identity_conflicts_rejected_across_pages(rows, kind):
    pages = [action_page([r], kind) for r in rows]
    with pytest.raises(study.Stop):
        study.normalize_actions("TQQQ", pages, date(2023, 1, 1), study.END)


def test_existing_action_operator_by_hand():
    from us_equity_strategies.research import kelly_core_return_generator as core
    days = (date(2023, 3, 30), date(2023, 3, 31), date(2023, 4, 3))
    bars = [core.RawBar(d, p, p) for d, p in zip(days, (100, 50, 48))]
    pages = [action_page([{"id": "split", "symbol": "TQQQ", "ex_date": str(days[1]),
                           "new_rate": 2, "old_rate": 1}], "forward_splits"),
             action_page([dividend("one"), dividend("two")])]
    actions = study.normalize_actions("TQQQ", pages, days[0], days[-1])
    # 50*2/100-1=0; (48+1+1)/50-1=0. Distinct trusted IDs sum once.
    assert core.total_return_series(bars, actions, days) == pytest.approx((0, 0, 0))
    collision = replace(actions, dividends=((days[1], 1),))
    with pytest.raises(core.CoreGeneratorError, match="SPLIT_AND_DIVIDEND"):
        core.total_return_series(bars, collision, days)
    with pytest.raises(core.CoreGeneratorError, match="SPLIT_DUPLICATE"):
        core.total_return_series(bars, replace(actions, splits=actions.splits * 2), days)


@pytest.mark.parametrize("new,old", [(0, 1), (1, 0), (-1, 1), (math.inf, 1), (True, 1)])
def test_invalid_split_ratio_rejected(new, old):
    row = {"id": "split", "symbol": "TQQQ", "ex_date": "2023-04-03", "new_rate": new, "old_rate": old}
    with pytest.raises(study.Stop):
        study.normalize_actions("TQQQ", [action_page([row], "forward_splits")], date(2023, 1, 1), study.END)


def test_close_fill_costs_hand_timing_and_terminal_fee():
    from quant_platform_kit.research_stats.execution_models import CostModel
    from us_equity_strategies.research import kelly_core_return_generator as core
    days = tuple(date(2023, 3, 27) + timedelta(days=i) for i in range(4))
    path = core._replay("X", days, {"A": (0, 0, .5, 0)}, ("A",),
                        lambda i, _w: {"A": (1, 1, 0, 1)[i]}, None)
    assert path.first_signal == days[0]
    assert path.weights[1] == {} and path.weights[2] == {"A": 1}
    assert path.gross_returns == (0, 0, .5, 0)
    assert path.close_fill_turnover == (0, 1, 0, 1)
    assert path.turnover[-1] == 0  # legacy cannot represent the terminal fee
    original = asdict(path)
    assert path.measure_close_fill_costs(CostModel(commission_bps=100)) == pytest.approx((0, -.01, .5, -.01))
    window = path.window(days[2], days[3])
    assert window.measure_close_fill_costs(CostModel(commission_bps=100)) == pytest.approx((.5, -.01))
    assert path.measure_close_fill_costs(CostModel(commission_bps=5))[1] == -.0005
    assert asdict(path) == original  # all weights, decisions, and gross state unchanged
    terminal_gain = core._replay("X", days[:3], {"A": (0, 0, .5)}, ("A",),
                                 lambda i, _w: {"A": (1, 0, 0)[i]}, None)
    assert terminal_gain.measure_close_fill_costs(CostModel(commission_bps=100)) == pytest.approx((0, -.01, .485))


def test_boxx_once_and_pre_inception_hold_rejected():
    from us_equity_strategies.research import kelly_core_return_generator as core
    days = tuple(date(2022, 12, 26) + timedelta(days=i) for i in range(5))
    bars = [core.RawBar(d, p, p) for d, p in zip(days[2:], (100, 99, 99))]
    actions = core.CorporateActions(dividends=((days[3], 1),))
    returns = core._optional_returns(bars, actions, days)
    assert returns == (0, 0, 0, 0, 0)  # (99+1)/100-1=0; no additional BOXX/rf layer
    with pytest.raises(core.CoreGeneratorError, match="ASSET_HELD_BEFORE_FIRST_BAR"):
        core._replay("X", days, {"BOXX": returns}, ("BOXX",), lambda _i, _w: {"BOXX": 1},
                     None, {"BOXX": 2})


def test_metrics_use_v2_and_censored_observation_periods():
    values, report = study.metrics((.1, -.2, 0), (.1, -.2, 0), (0, 0, 0))
    assert values["periods"] == 3
    assert values["log_growth"] == pytest.approx(math.log(.88))
    assert values["geometric_annualized_252"] == pytest.approx(.88 ** 84 - 1)
    assert values["max_drawdown"] == pytest.approx(-.2)
    assert values["recovery_censored"] is True and values["peak_to_recovery_periods"] is None
    assert values["observed_recovery_lower_bound"] == {"peak_to_end": 2, "trough_to_end": 1}
    assert all(m["value"] is None for m in report["metrics"]
               if m["name"] in ("sharpe_per_period", "deflated_sharpe_ratio"))


@pytest.mark.parametrize("failure", ["hash", "permission", "write", "generation", "overwrite"])
def test_store_identity_and_uncertain_writes_stop_once(archive, failure):
    client, _root, _inv, ref, _contract, _plan = archive
    store = study.ArchiveStore(client, study.Budget(), "synthetic-only", "outputs/")
    if failure == "hash":
        ref["sha256"] = "f" * 64
        def action():
            return store.read_input(ref)
    elif failure == "permission":
        client.fail_read = True
        def action():
            return store.read_input(ref)
    else:
        client.fail_write = failure == "write"
        client.unknown_generation = failure == "generation"
        if failure == "overwrite":
            client.put("gs://synthetic-only/outputs/intent.json", b"old", "1")
        def action():
            return store.create("intent.json", b"{}")
    with pytest.raises(study.Stop):
        action()
    assert len(client.calls) == 1


@pytest.mark.parametrize("cap", ["wall", "operations", "transfer", "input", "output_bytes", "output_objects"])
def test_hard_caps_before_io(archive, cap):
    client, _root, _inv, ref, _contract, _plan = archive
    budget = study.Budget()
    store = study.ArchiveStore(client, budget, "synthetic-only", "outputs/")
    if cap == "wall":
        budget.started -= 1201
    elif cap == "operations":
        budget.storage_operations = 64
    elif cap == "transfer":
        budget.storage_transfer_bytes = 64 * 1024**2
    elif cap == "input":
        budget.input_bytes = 2 * 1024**2
    elif cap == "output_bytes":
        budget.retained_bytes = 16 * 1024**2
    else:
        budget.output_objects = 5
    with pytest.raises(study.Stop):
        store.create("intent.json", b"{}") if cap.startswith("output") else store.read_input(ref)
    assert client.calls == []


def fake_dispatch(monkeypatch, tmp_path, plan_ref, *, run_id="12345"):
    for key, value in {"GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
                       "GITHUB_RUN_ATTEMPT": "1", "GITHUB_RUN_ID": run_id,
                       "RS04_ENVIRONMENT": "market-data-nonlive"}.items():
        monkeypatch.setenv(key, value)
    event = tmp_path / "dispatch.json"
    event.write_bytes(study.encode({"inputs": {"plan_" + key: str(value) for key, value in plan_ref.items()}}))
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
    monkeypatch.setenv("RS04_SUMMARY_FILE", str(tmp_path / "summary.json"))


def bound_control(client, root, plan, *, name="plan", generation="6001"):
    body = study.encode(plan)
    return body, client.put(root + "control/" + name + ".json", body, generation)


def test_real_cores_complete_offline_positive_and_private_outputs(archive, monkeypatch, tmp_path, capsys):
    from google.cloud import storage
    from us_equity_strategies.research import kelly_core_return_generator as core
    from us_equity_strategies.strategies import soxl_soxx_trend_income, tqqq_growth_income
    client, root, _inv, _ref, contract, plan = archive
    installed, digest = study.verify_runtime()
    bucket, prefix = study.validate_plan(plan, root, "12345", contract)
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    monkeypatch.setattr(storage, "Client", lambda: client)
    _body, plan_ref = bound_control(client, root, plan)
    fake_dispatch(monkeypatch, tmp_path, plan_ref)
    calls = {"TQQQ": 0, "SOXL": 0, "tqqq_builder": 0, "soxl_builder": 0}
    for owner, name, key in ((core, "generate_tqqq_core_path", "TQQQ"),
                             (core, "generate_soxl_core_path", "SOXL"),
                             (tqqq_growth_income, "build_rebalance_plan", "tqqq_builder"),
                             (soxl_soxx_trend_income, "build_rebalance_plan", "soxl_builder")):
        original = getattr(owner, name)

        @wraps(original)
        def wrapper(*args, _fn=original, _key=key, **kwargs):
            calls[_key] += 1
            return _fn(*args, **kwargs)
        monkeypatch.setattr(owner, name, wrapper)
    assert study.main([]) == 0
    output = capsys.readouterr()
    assert not output.err
    summary = study.decode(output.out)
    assert summary == study.decode((tmp_path / "summary.json").read_bytes())
    assert summary["installed_code_identities"] == installed and summary["configuration_digest"] == digest
    assert calls["TQQQ"] == calls["SOXL"] == 1
    assert calls["tqqq_builder"] > 0 and calls["soxl_builder"] > 0
    assert summary["status"] == "COMPLETED" and summary["sessions"] == 856
    assert len([c for c in client.calls if c[0] == "create"]) == 5
    assert len(client.calls) == 25  # control metadata/body + inventory + leaves + 5 create/readback pairs
    counters = summary["operation_counters"]
    assert counters["input_bytes"] < 2 * 1024**2 and counters["private_output_bytes"] < 16 * 1024**2
    assert counters["provider_calls"] == 0
    assert counters["control_input_bytes"] == plan_ref["bytes"]
    assert counters["control_storage_operations"] == 2
    assert counters["market_read_operations"] == 13
    assert counters["input_bytes"] == counters["control_input_bytes"] + counters["market_input_bytes"]
    assert [c[0] for c in client.calls[:4]] == ["metadata", "read", "create", "read"]
    intent = study.decode(client.objects[bucket, prefix + "intent.json"][0])
    assert intent["plan_nonce"] == B1_NONCE and intent["run_id"] == "12345"
    assert intent["control_plan_identity"] == study.identity(plan_ref)
    private = study.decode(client.objects[bucket, prefix + "private_comparison_summary.json"][0])
    daily = study.decode(client.objects[bucket, prefix + "private_core_paths.json"][0])
    prepared = study.decode(client.objects[bucket, prefix + "technical_summary.json"][0])
    assert prepared["status"] == "PREPARED"
    assert study.decode(client.objects[bucket, prefix + "terminal.json"][0])["status"] == "COMPLETED"
    for comparison, path in zip(private["comparison"], daily["paths"], strict=True):
        assert comparison["first_signal"] == "2023-03-24"
        assert comparison["pre_window_entry_cost_diagnostic"][0]["model_fee_deduction_sum_fraction"] > 0
        assert path["opening_state"]["earning_weights"]
        assert study.sha(study.encode(path["opening_state"])) == comparison["opening_state_digest"]
        assert set(comparison) == {"strategy_key", "first_signal", "opening_state_digest",
                                   "pre_window_entry_cost_diagnostic", "gross_metrics", "sensitivity"}
        assert comparison["gross_metrics"]["periods"] == 856
        for sensitivity in comparison["sensitivity"]:
            bps = sensitivity["bps_per_side"]
            gross = path["path"]["gross_returns"]
            turnover = path["path"]["close_fill_turnover"]
            assert path["model_net_returns"][str(bps)] == pytest.approx([
                g - (1 + g) * t * bps / 10000 for g, t in zip(gross, turnover, strict=True)])
    exported = study.encode(summary).decode()
    assert len(exported) <= 64 * 1024
    for forbidden in ("gs://", "earning_weights", "gross_metrics", "gross_returns", "model_net_returns",
                      "reference_equity_usd", "target_values", "PRIVATE_URI_SECRET_SENTINEL",
                      "RAW_PRICE_ACCOUNT_SECRET_SENTINEL"):
        assert forbidden not in exported
    assert private["cost_assumptions"]["weight_feedback"] is False
    assert private["qualification"]["financial"] == "NOT_READY"
    # Replaying the exact approved plan from a different run ID cannot consume
    # its nonce again. Other valid plan bytes and a different expiry cannot pick
    # another claim key either; versions/prefix overrides are rejected outright.
    original_outputs = {k: v for k, v in client.objects.items() if k in client.created}
    for change in ("run_id", "plan_bytes", "expiry", "version", "prefix"):
        replay_plan = copy.deepcopy(plan)
        if change == "plan_bytes":
            replay_plan["estimated_cloud_usd"] = .07
        elif change == "expiry":
            replay_plan["expires_at"] = (datetime.now(UTC) + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        elif change == "version":
            replay_plan["schema"] = "qsl.archived_core_retrospective_plan.v3"
        elif change == "prefix":
            replay_plan["output_prefix"] = root + "other/" + B1_NONCE + "/"
        _body, replay_ref = bound_control(client, root, replay_plan, name="replay_" + change, generation="6002")
        fake_dispatch(monkeypatch, tmp_path, replay_ref, run_id="67890")
        before = len(client.calls)
        assert study.main([]) == 1
        assert "STOPPED" in capsys.readouterr().out
        new_calls = client.calls[before:]
        assert [c[0] for c in new_calls] == (["metadata", "read"] if change in ("version", "prefix")
                                           else ["metadata", "read", "create"])
        assert all("/control/" in c[2] or c[2].endswith("intent.json") for c in new_calls)
    assert calls["TQQQ"] == calls["SOXL"] == 1
    assert {k: v for k, v in client.objects.items() if k in client.created} == original_outputs


def test_public_error_never_echoes_private_exception(monkeypatch, capsys):
    def broken():
        raise RuntimeError("gs://secret/private?account=999 PRICE_SENTINEL API_SECRET_SENTINEL")
    monkeypatch.setattr(study, "verify_runtime", broken)
    assert study.main([]) == 1
    output = capsys.readouterr()
    assert "STOPPED" in output.out
    assert all(s not in output.out + output.err for s in ("gs://", "999", "PRICE_SENTINEL", "API_SECRET_SENTINEL"))


@pytest.mark.parametrize("uri", ["gs://wrong-synthetic-bucket/control/plan.json",
                                  "gs://synthetic-only/outside-root/plan.json",
                                  "gs://synthetic-only/unit-other/control/plan.json"])
def test_control_plan_scope_rejects_before_client_or_read(archive, monkeypatch, tmp_path, capsys, uri):
    from google.cloud import storage
    client, _root, _inv, _ref, contract, plan = archive
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    constructions = []

    def client_factory():
        constructions.append(True)
        return client
    monkeypatch.setattr(storage, "Client", client_factory)
    fake_dispatch(monkeypatch, tmp_path, client.put(uri, study.encode(plan), "6001"))
    assert study.main([]) == 1
    output = capsys.readouterr()
    assert "STOPPED" in output.out and not output.err
    assert constructions == [] and client.calls == []


def test_workflow_is_offline_by_default_and_uses_existing_identity():
    body = (ROOT / ".github/workflows/rs04-archived-core-retrospective.yml").read_text()
    offline, cloud = body.split("  cloud:\n")
    assert "pull_request:" in offline and "branches: [main]" in offline
    assert "timeout-minutes: 20" in offline and "--junitxml=" in offline
    assert "counts['tests'] > 0" in offline and "root.iter('skipped')" in offline
    assert "id-token:" not in offline and "secrets." not in offline and "google-github-actions/auth" not in offline
    assert "needs: offline" in cloud and "github.event_name == 'workflow_dispatch'" in cloud
    assert "github.ref == 'refs/heads/main'" in cloud and "github.run_attempt == 1" in cloud
    assert "environment: market-data-nonlive" in cloud and "timeout-minutes: 30" in cloud
    assert cloud.index("--verify-runtime") < cloud.index("google-github-actions/auth")
    assert "secrets." not in cloud and "upload-artifact" not in body and "lifecycle" not in body
    assert "${{ inputs.plan_uri }}" not in body and "RS04_PLAN_URI:" not in body


@pytest.mark.parametrize("raw", [b'{"a":1,"a":2}', b'{"a":NaN}', b'[]'])
def test_json_is_unambiguous(raw):
    with pytest.raises(study.Stop):
        study.decode(raw)


def test_provider_ids_are_global_across_symbols(archive):
    client, _root, inv, ref, contract, _plan = archive
    selected = study.select_pages(inv, ref, contract)
    bodies = bodies_for(client, selected)
    bodies["SOXX", "actions"]["corporate_actions"]["cash_dividends"][0]["id"] = "SOXL-div-1"
    with pytest.raises(study.Stop, match="ACTION_DUPLICATE_ID_OR_CONFLICT"):
        study.normalize(selected, bodies)


def test_actions_on_non_session_are_rejected(archive):
    client, _root, inv, ref, contract, _plan = archive
    selected = study.select_pages(inv, ref, contract)
    bodies = bodies_for(client, selected)
    bodies["SOXL", "actions"]["corporate_actions"]["cash_dividends"][0]["ex_date"] = "2023-04-01"
    with pytest.raises(study.Stop, match="ACTION_ON_NON_SESSION"):
        study.normalize(selected, bodies)


def test_real_core_causality_under_future_price_perturbation(archive):
    from us_equity_strategies.research import kelly_core_return_generator as core
    client, _root, inv, ref, contract, _plan = archive
    selected = study.select_pages(inv, ref, contract)
    bars, actions, sessions, _counts = study.normalize(selected, bodies_for(client, selected))
    # A small window around the fixed start keeps the live indicator prehistory.
    cutoff = sessions.index(study.START) + 5
    sessions = sessions[:cutoff + 3]
    bars = {s: [b for b in rows if b.session <= sessions[-1]] for s, rows in bars.items()}
    actions = {s: core.CorporateActions() for s in bars}
    changed = copy.deepcopy(bars)
    for symbol in changed:
        changed[symbol][-1] = replace(changed[symbol][-1], open=777, close=777)
    for generator, extra in ((core.generate_tqqq_core_path, {}),
                              (core.generate_soxl_core_path, {"indicator_source": "qpk_live_builder",
                                                             "indicator_history_bars": 504})):
        before = generator(bars, actions, sessions, trade_start=study.START, signal_price_basis="split_adjusted", **extra)
        after = generator(changed, actions, sessions, trade_start=study.START, signal_price_basis="split_adjusted", **extra)
        assert before.first_signal == after.first_signal
        assert before.gross_returns[:-1] == after.gross_returns[:-1]
        assert before.weights == after.weights
        assert before.close_fill_turnover[:-1] == after.close_fill_turnover[:-1]


@pytest.mark.parametrize("change", ["uri", "returns", "prices", "account", "secret"])
def test_technical_projection_rejects_untrusted_nested_fields(archive, change):
    _client, _root, inv, ref, contract, _plan = archive
    installed, config_digest = study.verify_runtime()
    selected = study.select_pages(inv, ref, contract)
    args = dict(run_id="12345", started=study.now(), inventory=ref, pages=selected,
                source_digest="0" * 64, installed=installed, config_digest=config_digest,
                counts={"raw_bars": 1, "cash_dividends": 1, "forward_splits": 0}, outputs={}, budget=study.Budget())
    sentinel = "PRIVATE_SECRET_SENTINEL"
    if change == "uri":
        args["inventory"]["uri"] = "gs://synthetic-only/" + sentinel
        result = study.technical_summary(**args)
        assert sentinel not in study.encode(result).decode()
        return
    if change == "returns":
        args["counts"]["daily_returns"] = sentinel
    elif change == "prices":
        args["source_digest"] = sentinel
    elif change == "account":
        args["run_id"] = sentinel
    else:
        args["installed"]["quant-platform-kit"]["version"] = sentinel
    with pytest.raises(study.Stop):
        study.technical_summary(**args)


def test_cloud_gate_positive_without_cloud_io():
    study.cloud_gate({"GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
                      "GITHUB_RUN_ATTEMPT": "1", "GITHUB_RUN_ID": "12345", "RS04_ENVIRONMENT": "market-data-nonlive"})


def test_runtime_binding_rejects_wrong_install_source(monkeypatch):
    original = study.distribution

    class WrongSource:
        def read_text(self, _name):
            return '{"vcs_info":{"vcs":"git","commit_id":"0000000000000000000000000000000000000000"}}'
    monkeypatch.setattr(study, "distribution", lambda name: WrongSource() if name == "us-equity-strategies" else original(name))
    with pytest.raises(study.Stop, match="RESEARCH_INSTALL_COMMIT_MISMATCH"):
        study.verify_runtime()


def test_main_refuses_local_data_before_storage_client(monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "push")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out


def test_dispatch_inputs_are_read_privately_and_bounded(tmp_path, archive):
    _client, _root, _inv, ref, _contract, _plan = archive
    event = tmp_path / "dispatch.json"
    event.write_text(json.dumps({"inputs": {"plan_uri": ref["uri"], "plan_generation": ref["generation"],
                                           "plan_bytes": "1000", "plan_sha256": ref["sha256"]}}))
    assert study.dispatch_plan_ref(event)["uri"] == ref["uri"]
    body = json.loads(event.read_text())
    body["inputs"]["plan_bytes"] = str(16 * 1024 + 1)
    event.write_text(json.dumps(body))
    with pytest.raises(study.Stop):
        study.dispatch_plan_ref(event)
    event.write_text(" " * (64 * 1024 + 1))
    with pytest.raises(study.Stop, match="DISPATCH_EVENT_SIZE_REJECTED"):
        study.dispatch_plan_ref(event)


def test_computation_failure_records_private_terminal_without_exception_body(archive, monkeypatch):
    client, root, _inv, _ref, contract, plan = archive
    installed, digest = study.verify_runtime()
    bucket, prefix = study.validate_plan(plan, root, "12345", contract)
    store = study.ArchiveStore(client, study.Budget(), bucket, prefix)

    def broken(*_args):
        raise ValueError("PRIVATE_PRICE_DAILY_RETURN_ACCOUNT_SECRET")
    monkeypatch.setattr(study, "normalize", broken)
    body, plan_ref = bound_control(client, root, plan)
    with pytest.raises(study.Stop, match="COMPUTATION_STOPPED"):
        study.run(body, store, installed, digest, contract, plan_ref=plan_ref, run_id="12345")
    terminal = client.objects[bucket, prefix + "terminal.json"][0]
    assert b"STOPPED" in terminal and b"PRIVATE_PRICE" not in terminal
    assert len([c for c in client.calls if c[0] == "create"]) == 2


@pytest.mark.parametrize("failure", ["write_unknown", "leaf_hash"])
def test_pipeline_storage_failure_never_retries_or_writes_terminal(archive, failure):
    client, root, _inv, _ref, contract, plan = archive
    installed, digest = study.verify_runtime()
    bucket, prefix = study.validate_plan(plan, root, "12345", contract)
    store = study.ArchiveStore(client, study.Budget(), bucket, prefix)
    if failure == "write_unknown":
        client.fail_write = True
    else:
        key = ("synthetic-only", "unit/archive/alpaca/SOXL/raw/page-001.json")
        body, generation = client.objects[key]
        client.objects[key] = body[:-1] + b"X", generation
    body, plan_ref = bound_control(client, root, plan)
    with pytest.raises(study.Stop):
        study.run(body, store, installed, digest, contract, plan_ref=plan_ref, run_id="12345")
    assert not any(c[2].endswith("terminal.json") for c in client.calls)
    assert len([c for c in client.calls if c[0] == "create"]) == 1


B1_NONCE = "12345678-1234-4234-8234-123456789abc"


def test_b1_nonce_prefix_is_fixed_to_scope_and_canonical_nonce(archive):
    _client, root, _inv, _ref, contract, _plan = archive
    prefix = study.nonce_output_prefix(root, B1_NONCE, contract)
    assert prefix == root + study.STUDY + "/nonce/" + B1_NONCE + "/"
    with pytest.raises(study.Stop):
        study.nonce_output_prefix("gs://synthetic-only/outside/", B1_NONCE, contract)


@pytest.mark.parametrize("nonce", [None, True, "", B1_NONCE.upper(), B1_NONCE.replace("-", ""),
                                   "12345678-1234-1234-8234-123456789abc", "00000000-0000-0000-0000-000000000000"])
def test_b1_nonce_alias_or_wrong_version_rejected(archive, nonce):
    _client, root, _inv, _ref, contract, _plan = archive
    with pytest.raises(study.Stop):
        study.nonce_output_prefix(root, nonce, contract)


@pytest.mark.parametrize("expires", [None, "2026-10-19T11:59:59Z", "2026-10-19T12:00:00Z",
                                     "2026-10-20T12:00:01Z", "2026-10-19T13:00:00+00:00",
                                     "2026-10-19T13:00:00", "2026-13-19T13:00:00Z"])
def test_b1_expired_noncanonical_or_unbounded_plan_rejected(expires):
    instant = datetime(2026, 10, 19, 12, tzinfo=UTC)
    with pytest.raises(study.Stop):
        study.plan_deadline(expires, instant=instant)


def test_b1_validity_has_explicit_utc_bound():
    instant = datetime(2026, 10, 19, 12, tzinfo=UTC)
    assert study.plan_deadline("2026-10-19T13:00:00Z", instant=instant) == instant + timedelta(hours=1)


def test_b1_expiry_budget_rejects_before_io(archive):
    client, _root, _inv, ref, _contract, _plan = archive
    budget = study.Budget(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    store = study.ArchiveStore(client, budget, "synthetic-only", "outputs/")
    with pytest.raises(study.Stop):
        store.read_input(ref)
    assert client.calls == []


def test_b1_conditional_intent_port_has_one_atomic_winner(archive):
    client, root, _inv, _ref, contract, _plan = archive
    prefix = study.gcs_uri(study.nonce_output_prefix(root, B1_NONCE, contract))[1]
    ready = Barrier(2)

    def attempt(run_id):
        store = study.ArchiveStore(client, study.Budget(), "synthetic-only", prefix)
        ready.wait(timeout=5)
        try:
            return store.create("intent.json", study.encode({"run_id": run_id, "plan_nonce": B1_NONCE}))
        except study.Stop:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("12345", "67890")))
    assert sum(r is not None for r in results) == 1
    assert [c[0] for c in client.calls].count("create") == 2
    assert [c[0] for c in client.calls].count("read") == 1
    assert len(client.created) == 1
    assert all(c[2].endswith("intent.json") for c in client.calls)


@pytest.mark.parametrize("failure", ["write_then_raise", "unknown_generation", "corrupt_readback"])
def test_b1_intent_port_fault_stops_without_retry_and_existing_claim_blocks_replay(archive, failure):
    client, root, _inv, _ref, contract, _plan = archive
    prefix = study.gcs_uri(study.nonce_output_prefix(root, B1_NONCE, contract))[1]
    setattr(client, failure, True)
    store = study.ArchiveStore(client, study.Budget(), "synthetic-only", prefix)
    with pytest.raises(study.Stop):
        store.create("intent.json", b'{"run_id":"12345"}')
    first_calls = list(client.calls)
    assert [c[0] for c in first_calls] == (["create", "read"] if failure == "corrupt_readback" else ["create"])
    assert len(client.created) == 1
    setattr(client, failure, False)
    replay = study.ArchiveStore(client, study.Budget(), "synthetic-only", prefix)
    with pytest.raises(study.Stop):
        replay.create("intent.json", b'{"run_id":"67890"}')
    assert [c[0] for c in client.calls[len(first_calls):]] == ["create"]
    assert all(c[2].endswith("intent.json") for c in client.calls)


@pytest.mark.parametrize("failure", ["permission", "body_permission", "bytes", "hash", "too_large"])
def test_b1_control_failures_are_charged_separately_and_never_claim(archive, failure):
    client, root, _inv, _ref, contract, plan = archive
    _body, ref = bound_control(client, root, plan)
    if failure == "permission":
        client.fail_read = True
    elif failure == "body_permission":
        client.fail_control_body = True
    elif failure == "bytes":
        ref["bytes"] -= 1
    elif failure == "hash":
        ref["sha256"] = "f" * 64
    else:
        key = study.gcs_uri(ref["uri"])
        client.objects[key] = b"X" * (16 * 1024 + 1), ref["generation"]
    budget = study.Budget()
    store = study.ArchiveStore(client, budget, "synthetic-only", "unused/")
    with pytest.raises(study.Stop):
        store.read_control(ref, contract)
    summary = budget.summary()
    assert summary["control_input_bytes"] == ref["bytes"]
    assert summary["market_input_bytes"] == 0 and summary["market_read_operations"] == 0
    assert summary["storage_operations"] == len(client.calls) > 0
    assert summary["storage_transfer_bytes"] >= 64 * 1024
    assert not client.created and all("/control/" in c[2] for c in client.calls)


@pytest.mark.parametrize("mutation", ["v1", "v3", "expired", "far_future", "nonce_alias", "nonce_missing",
                                       "run_id_field", "prefix", "input_generation", "input_bytes", "input_hash",
                                       "input_uri", "unapproved", "over_budget", "retention"])
def test_b1_invalid_body_reads_only_control_and_never_claims(archive, monkeypatch, tmp_path, capsys, mutation):
    from google.cloud import storage
    client, root, _inv, _ref, contract, plan = archive
    if mutation in ("v1", "v3"):
        plan["schema"] = "qsl.archived_core_retrospective_plan." + mutation
    elif mutation == "expired":
        plan["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif mutation == "far_future":
        plan["expires_at"] = (datetime.now(UTC) + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif mutation == "nonce_alias":
        plan["plan_nonce"] = plan["plan_nonce"].upper()
    elif mutation == "nonce_missing":
        del plan["plan_nonce"]
    elif mutation == "run_id_field":
        plan["run_id"] = "12345"
    elif mutation == "prefix":
        plan["output_prefix"] = root + "outside/" + B1_NONCE + "/"
    elif mutation.startswith("input_"):
        field = mutation.removeprefix("input_")
        plan["inventory"][field if field != "hash" else "sha256"] = {
            "generation": "7777", "bytes": 1, "hash": "f" * 64, "uri": root + "other/input-inventory.json"}[field]
    elif mutation == "unapproved":
        plan["execution_approved"] = False
    elif mutation == "over_budget":
        plan["cloud_cap_usd"] = .11
    else:
        plan["retention_conditions_unchanged"] = False
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    monkeypatch.setattr(storage, "Client", lambda: client)
    _body, plan_ref = bound_control(client, root, plan)
    fake_dispatch(monkeypatch, tmp_path, plan_ref)
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out
    assert [c[0] for c in client.calls] == ["metadata", "read"]
    assert not client.created and all("/control/" in c[2] for c in client.calls)


@pytest.mark.parametrize("relative", ["archive/input-inventory.json", "control/nested/plan.json",
                                       "control/plan.txt", "control/.json"])
def test_b1_control_file_path_is_checked_before_client(archive, monkeypatch, tmp_path, capsys, relative):
    from google.cloud import storage
    client, root, _inv, _ref, contract, plan = archive
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    constructions = []

    def factory():
        constructions.append(True)
        return client
    monkeypatch.setattr(storage, "Client", factory)
    ref = client.put(root + relative, study.encode(plan), "6001")
    fake_dispatch(monkeypatch, tmp_path, ref)
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out
    assert not constructions and not client.calls


@pytest.mark.parametrize("fault", ["fail_write", "write_then_raise", "unknown_generation", "corrupt_readback"])
def test_b1_claim_faults_stop_before_market_reads_or_core(archive, monkeypatch, tmp_path, capsys, fault):
    from google.cloud import storage
    client, root, _inv, _ref, contract, plan = archive
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    monkeypatch.setattr(storage, "Client", lambda: client)
    core_calls = []

    def forbidden(*_args):
        core_calls.append(True)
        raise AssertionError("CORE_MUST_NOT_RUN")
    monkeypatch.setattr(study, "compare", forbidden)
    _body, plan_ref = bound_control(client, root, plan)
    setattr(client, fault, True)
    fake_dispatch(monkeypatch, tmp_path, plan_ref)
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out
    assert not core_calls
    expected = ["metadata", "read", "create"] + (["read"] if fault == "corrupt_readback" else [])
    assert [c[0] for c in client.calls] == expected
    assert all("/control/" in c[2] or c[2].endswith("intent.json") for c in client.calls)
    if fault != "fail_write":
        setattr(client, fault, False)
        fake_dispatch(monkeypatch, tmp_path, plan_ref, run_id="67890")
        before = len(client.calls)
        assert study.main([]) == 1
        assert "STOPPED" in capsys.readouterr().out
        assert [c[0] for c in client.calls[before:]] == ["metadata", "read", "create"]
        assert not core_calls


def test_b1_control_can_only_be_read_once_per_budget(archive):
    client, root, _inv, _ref, contract, plan = archive
    _body, ref = bound_control(client, root, plan)
    store = study.ArchiveStore(client, study.Budget(), "synthetic-only", "unused/")
    store.read_control(ref, contract)
    before = list(client.calls)
    with pytest.raises(study.Stop, match="CONTROL_READ_LIMIT"):
        store.read_control(ref, contract)
    assert client.calls == before


def test_b1_new_nonce_cannot_mutate_an_approved_plan_identity(archive):
    client, root, _inv, _ref, contract, plan = archive
    _original, plan_ref = bound_control(client, root, plan)
    plan["plan_nonce"] = "22345678-1234-4234-8234-123456789abc"
    plan["output_prefix"] = study.nonce_output_prefix(root, plan["plan_nonce"], contract)
    store = study.ArchiveStore(client, study.Budget(), "synthetic-only", "unused/")
    with pytest.raises(study.Stop, match="CONTROL_OBJECT_IDENTITY_MISMATCH"):
        study.run(study.encode(plan), store, *study.verify_runtime(), contract, plan_ref=plan_ref, run_id="67890")
    assert not client.calls and not client.created


def test_b1_new_nonce_does_not_reset_a_budget_after_failed_claim(archive):
    client, root, _inv, _ref, contract, plan = archive
    store = study.ArchiveStore(client, study.Budget(), "synthetic-only", "unused/")
    body, ref = bound_control(client, root, plan)
    client.fail_write = True
    with pytest.raises(study.Stop):
        study.run(body, store, *study.verify_runtime(), contract, plan_ref=ref, run_id="12345")
    before = list(client.calls)
    charged = store.budget.summary()
    client.fail_write = False
    plan["plan_nonce"] = "22345678-1234-4234-8234-123456789abc"
    plan["output_prefix"] = study.nonce_output_prefix(root, plan["plan_nonce"], contract)
    new_body, new_ref = bound_control(client, root, plan, name="new_nonce", generation="6002")
    with pytest.raises(study.Stop, match="CLAIM_ALREADY_ATTEMPTED"):
        study.run(new_body, store, *study.verify_runtime(), contract, plan_ref=new_ref, run_id="67890")
    assert client.calls == before and store.budget.summary() == charged


def test_b1_concurrent_complete_consumers_compute_real_core_pair_once(archive, monkeypatch):
    from us_equity_strategies.research import kelly_core_return_generator as core
    client, root, inventory, inv_ref, contract, plan = archive
    _body, plan_ref = bound_control(client, root, plan)
    installed, config_digest = study.verify_runtime()
    ready = Barrier(2)
    calls = {"TQQQ": 0, "SOXL": 0}
    for name, key in (("generate_tqqq_core_path", "TQQQ"), ("generate_soxl_core_path", "SOXL")):
        original = getattr(core, name)

        @wraps(original)
        def counted(*args, _fn=original, _key=key, **kwargs):
            calls[_key] += 1
            return _fn(*args, **kwargs)
        monkeypatch.setattr(core, name, counted)

    def attempt(run_id):
        store = study.ArchiveStore(client, study.Budget(), "synthetic-only", "unused/")
        body = store.read_control(plan_ref, contract)
        ready.wait(timeout=5)
        try:
            summary = study.run(body, store, installed, config_digest, contract, plan_ref=plan_ref, run_id=run_id)
            return True, summary, store.budget.summary()
        except study.Stop:
            return False, None, store.budget.summary()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, ("12345", "67890")))
    assert sum(success for success, _summary, _budget in results) == 1
    assert calls == {"TQQQ": 1, "SOXL": 1}
    winning = next((summary, budget) for success, summary, budget in results if success)
    losing = next(budget for success, _summary, budget in results if not success)
    assert winning[0]["status"] == "COMPLETED" and winning[1]["storage_operations"] == 25
    assert losing["storage_operations"] == 3 and losing["market_read_operations"] == 0
    assert losing["market_input_bytes"] == 0 and losing["private_output_objects"] == 1
    source_keys = {study.gcs_uri(inv_ref["uri"])} | {
        study.gcs_uri(entry["pages"][0]["uri"]) for entry in inventory["alpaca"]}
    reads = [c for c in client.calls if c[0] == "read" and (c[1], c[2]) in source_keys]
    assert len(reads) == len({(c[1], c[2]) for c in reads}) == 13
    assert len(client.created) == 5 and len(client.calls) == 28


@pytest.mark.parametrize("stage", ["control", "intent", "inventory"])
def test_b1_expiry_is_rechecked_between_read_stages(archive, monkeypatch, tmp_path, capsys, stage):
    from google.cloud import storage
    client, root, _inv, _ref, contract, plan = archive
    instant = datetime.now(UTC).replace(microsecond=0)
    deadline = instant + timedelta(minutes=1)
    clock = [instant]
    plan["expires_at"] = deadline.strftime("%Y-%m-%dT%H:%M:%SZ")
    monkeypatch.setattr(study, "utc_now", lambda: clock[0])
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    monkeypatch.setattr(storage, "Client", lambda: client)
    core_calls = []

    def forbidden(*_args):
        core_calls.append(True)
        raise AssertionError("EXPIRED_CORE_MUST_NOT_RUN")
    monkeypatch.setattr(study, "compare", forbidden)

    def expire(path):
        if ((stage == "control" and "/control/" in path)
                or (stage == "intent" and path.endswith("/intent.json"))
                or (stage == "inventory" and path.endswith("/input-inventory.json"))):
            clock[0] = deadline
    client.on_read = expire
    _body, ref = bound_control(client, root, plan)
    fake_dispatch(monkeypatch, tmp_path, ref)
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out and not core_calls
    expected = ["metadata", "read"] + ([] if stage == "control" else ["create", "read"])
    if stage == "inventory":
        expected += ["read"]
    assert [c[0] for c in client.calls] == expected
    assert len(client.created) == (0 if stage == "control" else 1)


@pytest.mark.parametrize("field,value", [("uri", "https://synthetic-only/control/plan.json"),
                                        ("generation", "latest"), ("generation", "0"),
                                        ("bytes", 0), ("bytes", 16385), ("sha256", "missing"),
                                        ("sha256", "F" * 64)])
def test_b1_control_reference_invalidity_has_no_client_or_budget_io(archive, monkeypatch, tmp_path, capsys, field, value):
    from google.cloud import storage
    client, root, _inv, _ref, contract, plan = archive
    for key, item in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, item)
    constructions = []

    def factory():
        constructions.append(True)
        return client
    monkeypatch.setattr(storage, "Client", factory)
    _body, ref = bound_control(client, root, plan)
    ref[field] = value
    fake_dispatch(monkeypatch, tmp_path, ref)
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out and not constructions and not client.calls


def test_b1_actual_legacy_run_id_plan_is_not_reinterpreted(archive, monkeypatch, tmp_path, capsys):
    from google.cloud import storage
    client, root, _inv, _ref, contract, plan = archive
    plan["schema"] = "qsl.archived_core_retrospective_plan.v1"
    plan["run_id"] = "12345"
    plan["output_prefix"] = root + "new/" + study.STUDY + "/12345/"
    del plan["plan_nonce"], plan["expires_at"]
    with pytest.raises(study.Stop, match="PLAN_VERSION_REJECTED"):
        study.validate_plan(plan, root, "12345", contract)
    for key, value in contract.items():
        monkeypatch.setitem(study.ARCHIVE, key, value)
    monkeypatch.setattr(storage, "Client", lambda: client)
    _body, ref = bound_control(client, root, plan)
    fake_dispatch(monkeypatch, tmp_path, ref)
    assert study.main([]) == 1
    assert "STOPPED" in capsys.readouterr().out
    assert [c[0] for c in client.calls] == ["metadata", "read"] and not client.created
