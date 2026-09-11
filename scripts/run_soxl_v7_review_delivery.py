#!/usr/bin/env python3
"""One bounded publish/readback of an already-admitted V7 research ticket.

Run in the separate QPK736 control environment. It has no replay, AI, broker or
owner-decision call. Three create-only objects preserve admission, write intent
and the terminal human decision across ephemeral daily workflow runners.
"""
from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlencode, urlsplit
import urllib.request

from quant_platform_kit.strategy_lifecycle.research_promotion_cycle import (
    ResearchPromotionTicket, load_research_promotion_ticket,
    make_console_research_promotion_pull, make_console_research_promotion_sync,
    reconcile_saved_research_promotion_ticket, save_research_promotion_ticket,
)

from scripts.soxl_v7_review_checkpoint import BUCKET, CreateOnlyStore

PROFILE = "soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve"
CONFIG = "843ab4e93e81985c2b3becc61a2f0b971508ccf25afa59acf402e75f574514d1"
CONSOLE = "https://qsl-strategy-switch-console.pigbibi.workers.dev"


def _console_request(*, endpoint, bearer_token, timeout, ticket_id=None, payload=None):
    """Use QPK's existing transport hooks with this service's client identity."""
    headers = {"Authorization": "Bearer " + bearer_token, "Accept": "application/json",
               "User-Agent": "UsEquitySnapshotPipelines-V7Review/1.0"}
    if payload is None:
        request = urllib.request.Request(endpoint + "?" + urlencode({"ticket_id": ticket_id}), headers=headers)
    else:
        headers["Content-Type"] = "application/json"
        request = urllib.request.Request(endpoint, data=json.dumps(payload, sort_keys=True).encode(),
                                         method="POST", headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read()) if payload is None else response.status


def _admitted(raw):
    ticket = ResearchPromotionTicket.from_dict(raw)
    if (
        ticket.strategy_profile != PROFILE or ticket.domain != "us_equity"
        or ticket.state.value != "awaiting_human" or ticket.live_authority_granted
        or ticket.drift_status != "not_applicable" or ticket.drift_score != 0
        or ticket.search_iterations != 0 or ticket.shadow_passed is not True
        or ticket.shadow_evidence_kind != "v7_nonlive_shadow_and_simulated_paper"
        or ticket.proposed_params != {"candidate_id": PROFILE, "config_sha256": CONFIG}
        or not ticket.notes or ticket.notes[0] != "frozen_v7_forward_financial_gates_passed_research_only"
    ):
        raise ValueError("invalid admitted V7 research ticket")
    return ticket


def deliver_once(*, store, prepared, pull, sync):
    """Consume the trusted producer admission; persist before the sole POST."""
    saved = store.read("ticket.json")
    if saved is None:
        if prepared is None:
            return "WAITING_FOR_FINANCIAL_ADMISSION"
        ticket = _admitted(prepared)
        store.create("ticket.json", ticket.to_dict())
    else:
        ticket = _admitted(saved)
        if prepared is not None and _admitted(prepared).to_dict() != ticket.to_dict():
            raise ValueError("V7 admission changed; retain original checkpoint")

    terminal = store.read("terminal.json")
    remote = terminal if terminal is not None else pull(ticket.ticket_id)
    if remote is not None:
        with tempfile.TemporaryDirectory(prefix="v7-decision-") as raw:
            local = Path(raw) / "ticket.json"
            save_research_promotion_ticket(ticket, local)
            result = reconcile_saved_research_promotion_ticket(
                local, pull_console=lambda _: remote, domain="us_equity",
            )
            if result["status"] == "updated":
                # Store the validated original console payload, not extra local
                # reconciliation notes, so a restarted worker can validate again.
                store.create("terminal.json", remote)
                assert load_research_promotion_ticket(local).live_authority_granted is False
                return result["state"].upper()
            if result["status"] == "awaiting_human" and terminal is None:
                return "AWAITING_HUMAN"
            raise ValueError("V7 console decision mismatch")

    attempted = store.read("attempted.json")
    if attempted is not None:
        if attempted != ticket.to_dict():
            raise ValueError("V7 attempt identity mismatch")
        return "PARKED_DELIVERY_UNCONFIRMED"
    store.create("attempted.json", ticket.to_dict())
    # A crash immediately after this checkpoint safely leaves read-only recovery.
    return "AWAITING_HUMAN" if sync(ticket) else "PARKED_DELIVERY_UNCONFIRMED"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared-ticket", type=Path)
    args = parser.parse_args(argv)
    try:
        from google.cloud import storage
        url = os.environ.get("RESEARCH_PROMOTION_SYNC_URL", "")
        token = os.environ.get("RESEARCH_PROMOTION_SYNC_TOKEN", "")
        expected = CONSOLE + "/api/internal/sync-research-promotion-ticket"
        if url != expected or not token or urlsplit(url).query:
            raise ValueError("V7 research console binding unavailable")
        pull = make_console_research_promotion_pull(
            endpoint_url=CONSOLE + "/api/internal/research-promotion-ticket", sync_token=token,
            raise_on_unavailable=True, printer=lambda *_a, **_k: None, get_json=_console_request,
        )
        sync = make_console_research_promotion_sync(
            endpoint_url=url, sync_token=token, pull_console=pull, printer=lambda *_a, **_k: None,
            post_json=_console_request,
        )
        prepared = json.loads(args.prepared_ticket.read_text()) if args.prepared_ticket else None
        status = deliver_once(
            store=CreateOnlyStore(storage.Client().bucket(BUCKET)), prepared=prepared, pull=pull, sync=sync,
        )
        print(json.dumps({"status": status, "live_authority_granted": False}))
        return 2 if status.startswith("PARKED") else 0
    except Exception:
        print('{"status":"PARKED_REVIEW_DELIVERY","live_authority_granted":false}')
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
