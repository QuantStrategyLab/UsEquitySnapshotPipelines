"""Run additionally in scripts/requirements-soxl-v7-review-control.txt runtime."""
from copy import deepcopy

import pytest

cycle = pytest.importorskip("quant_platform_kit.strategy_lifecycle.research_promotion_cycle")
from scripts.run_soxl_v7_review_delivery import CONFIG, PROFILE, deliver_once  # noqa: E402


class Store:
    def __init__(self):
        self.items = {}

    def read(self, name):
        return deepcopy(self.items.get(name))

    def create(self, name, value):
        assert name not in self.items or self.items[name] == value
        self.items[name] = deepcopy(value)


@pytest.fixture
def prepared():
    return cycle.ResearchPromotionTicket(
        ticket_id="rpt_synthetic_v7", strategy_profile=PROFILE, domain="us_equity",
        state=cycle.ResearchPromotionState.AWAITING_HUMAN, drift_status="not_applicable", drift_score=0,
        created_at="2027-09-01T00:00:00Z", updated_at="2027-09-01T00:00:00Z",
        proposed_params={"candidate_id": PROFILE, "config_sha256": CONFIG},
        shadow_evidence_kind="v7_nonlive_shadow_and_simulated_paper", shadow_passed=True,
        notes=("frozen_v7_forward_financial_gates_passed_research_only",),
    ).to_dict()


def test_absent_admission_does_not_contact_console():
    assert deliver_once(store=Store(), prepared=None, pull=lambda _: pytest.fail(), sync=lambda _: pytest.fail()) == "WAITING_FOR_FINANCIAL_ADMISSION"


def test_persist_before_post_and_never_repeat_uncertain_write(prepared):
    store = Store()
    calls = []

    def sync(ticket):
        assert store.read("attempted.json") == ticket.to_dict()
        calls.append("POST")
        return False

    for _ in range(2):
        assert deliver_once(store=store, prepared=prepared, pull=lambda _: None, sync=sync) == "PARKED_DELIVERY_UNCONFIRMED"
    assert calls == ["POST"]


def test_unavailable_read_does_not_post_or_record_attempt(prepared):
    store = Store()

    def unavailable(_):
        raise ValueError("unavailable")

    with pytest.raises(ValueError):
        deliver_once(store=store, prepared=prepared, pull=unavailable, sync=lambda _: pytest.fail())
    assert store.read("attempted.json") is None


@pytest.mark.parametrize("decision", ["accept", "reject"])
def test_actual_qpk_decision_recovered_and_persisted_across_restart(prepared, decision):
    store = Store()
    terminal = cycle.apply_human_promotion_decision(
        cycle.ResearchPromotionTicket.from_dict(prepared), decision=decision,
        confirmation={"target_platform": "ibkr", "execution_mode": "paper", "risk_profile": "CAPITAL_PRESERVATION"},
        paper_supported=True, decided_at="2027-09-02T00:00:00Z",
    ).to_dict()
    state = "HUMAN_ACCEPTED" if decision == "accept" else "HUMAN_REJECTED"
    assert deliver_once(store=store, prepared=prepared, pull=lambda _: terminal, sync=lambda _: pytest.fail()) == state
    assert store.read("terminal.json")["live_authority_granted"] is False
    assert deliver_once(store=store, prepared=None, pull=lambda _: pytest.fail(), sync=lambda _: pytest.fail()) == state


@pytest.mark.parametrize("key,value", [("shadow_evidence_kind", "paired_shadow"), ("live_authority_granted", True), ("drift_status", "critical")])
def test_changed_remote_never_overwrites_local_admission(prepared, key, value):
    store = Store()
    remote = {**prepared, key: value}
    with pytest.raises(ValueError):
        deliver_once(store=store, prepared=prepared, pull=lambda _: remote, sync=lambda _: pytest.fail())
    assert store.read("ticket.json") == prepared
    assert store.read("terminal.json") is None


def test_failed_attempt_checkpoint_write_prevents_post(prepared):
    store = Store()
    original = store.create

    def create(name, value):
        if name == "attempted.json":
            raise OSError("storage unavailable")
        original(name, value)

    store.create = create
    with pytest.raises(OSError):
        deliver_once(store=store, prepared=prepared, pull=lambda _: None, sync=lambda _: pytest.fail())


def test_cloud_checkpoint_is_create_only_and_unknown_write_is_not_retried():
    from scripts.soxl_v7_review_checkpoint import CreateOnlyStore
    calls = []

    class Blob:
        def upload_from_string(self, payload, **kwargs):
            calls.append(kwargs)
            raise TimeoutError("unknown upload outcome")

    class Bucket:
        def blob(self, name):
            return Blob()

    with pytest.raises(TimeoutError):
        CreateOnlyStore(Bucket()).create("attempted.json", {"synthetic": True})
    assert len(calls) == 1
    assert calls[0]["if_generation_match"] == 0
    assert calls[0]["retry"] is None


def test_only_confirmed_cloud_not_found_counts_as_absent():
    from google.api_core.exceptions import Forbidden, NotFound
    from scripts.soxl_v7_review_checkpoint import CreateOnlyStore

    class Blob:
        error = NotFound("synthetic")

        def download_as_bytes(self):
            raise self.error

    class Bucket:
        def blob(self, name):
            return Blob()

    store = CreateOnlyStore(Bucket())
    assert store.read("ticket.json") is None
    Blob.error = Forbidden("synthetic")
    with pytest.raises(Forbidden):
        store.read("ticket.json")
