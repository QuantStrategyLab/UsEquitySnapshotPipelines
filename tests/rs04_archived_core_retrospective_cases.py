"""Explicit research-overlay acceptance. All market observations are artificial."""
import importlib.util
import copy
import math
import json
import socket
import sys
from dataclasses import asdict, replace
from datetime import date, timedelta
from functools import wraps
from pathlib import Path

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
    def __init__(self):
        self.objects, self.calls = {}, []
        self.fail_read = self.fail_write = False
        self.unknown_generation = False

    def bucket(self, name):
        client = self

        class Bucket:
            def blob(self, path, generation=None):
                class Blob:
                    def __init__(self):
                        self.generation = generation

                    def download_as_bytes(self, **kwargs):
                        client.calls.append(("read", name, path, kwargs))
                        assert kwargs["retry"] is None
                        assert kwargs["if_generation_match"] == int(self.generation)
                        if client.fail_read:
                            raise PermissionError("PRIVATE_URI_SECRET_SENTINEL")
                        body, gen = client.objects[name, path]
                        assert int(gen) == int(self.generation)
                        return body

                    def upload_from_string(self, body, **kwargs):
                        client.calls.append(("create", name, path, kwargs))
                        assert kwargs["if_generation_match"] == 0 and kwargs["retry"] is None
                        if (name, path) in client.objects or client.fail_write:
                            raise PermissionError("PRIVATE_WRITE_SECRET_SENTINEL")
                        client.objects[name, path] = body, "90000001"
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
                "inventory_uri_sha256": study.sha(ref["uri"].encode())}
    plan = {"schema": "qsl.archived_core_retrospective_plan.v1", "study_id": study.STUDY, "run_id": "12345",
            "window": {"start": str(study.START), "end": str(study.END)}, "inventory": ref,
            "output_prefix": root + "new/" + study.STUDY + "/12345/", "execution_approved": True,
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


def test_real_cores_complete_offline_positive_and_private_outputs(archive, monkeypatch):
    from us_equity_strategies.research import kelly_core_return_generator as core
    from us_equity_strategies.strategies import soxl_soxx_trend_income, tqqq_growth_income
    client, root, _inv, _ref, contract, plan = archive
    installed, digest = study.verify_runtime()
    bucket, prefix = study.validate_plan(plan, root, "12345", contract)
    budget = study.Budget()
    store = study.ArchiveStore(client, budget, bucket, prefix)
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
    summary = study.run(plan, store, installed, digest, contract)
    assert calls["TQQQ"] == calls["SOXL"] == 1
    assert calls["tqqq_builder"] > 0 and calls["soxl_builder"] > 0
    assert summary["status"] == "COMPLETED" and summary["sessions"] == 856
    assert len([c for c in client.calls if c[0] == "create"]) == 5
    assert len(client.calls) == 23  # 13 input reads, 5 create/readback pairs
    assert budget.input_bytes < 2 * 1024**2 and budget.retained_bytes < 16 * 1024**2
    assert budget.summary()["provider_calls"] == 0
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


def test_public_error_never_echoes_private_exception(monkeypatch, capsys):
    def broken():
        raise RuntimeError("gs://secret/private?account=999 PRICE_SENTINEL API_SECRET_SENTINEL")
    monkeypatch.setattr(study, "verify_runtime", broken)
    assert study.main([]) == 1
    output = capsys.readouterr()
    assert "STOPPED" in output.out
    assert all(s not in output.out + output.err for s in ("gs://", "999", "PRICE_SENTINEL", "API_SECRET_SENTINEL"))


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
    with pytest.raises(study.Stop, match="COMPUTATION_STOPPED"):
        study.run(plan, store, installed, digest, contract)
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
    with pytest.raises(study.Stop):
        study.run(plan, store, installed, digest, contract)
    assert not any(c[2].endswith("terminal.json") for c in client.calls)
    assert len([c for c in client.calls if c[0] == "create"]) == 1
