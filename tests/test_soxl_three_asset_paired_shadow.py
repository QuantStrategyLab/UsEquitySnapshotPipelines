from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from us_equity_snapshot_pipelines.lifecycle.soxl_three_asset_paired_shadow import (  # noqa: E402
    SoxlThreeAssetPairedShadowError,
    advance_paired_shadow_session,
    input_snapshot_sha256,
)


def _state(*, pending: bool = False) -> dict[str, object]:
    return {
        "cash": 1_000.0,
        "quantities": {"SOXL": 0.0, "SOXX": 0.0, "BOXX": 0.0},
        "pending_target_weights": (
            {"SOXL": 0.65, "SOXX": 0.20, "BOXX": 0.10} if pending else None
        ),
        "pending_cash_weight": 0.05 if pending else None,
        "previous_equity": 1_000.0,
    }


def _session() -> dict[str, object]:
    value = {
        "as_of": "2026-09-10T20:00:00+00:00",
        "prices": {"SOXL": 100.0, "SOXX": 100.0, "BOXX": 100.0},
        "market_data": {"derived_indicators": {"SOXL": {"price": 100.0}}},
    }
    value["input_snapshot_sha256"] = input_snapshot_sha256(value)
    return value


def _decide(*, portfolio, market_data, as_of, mid_soxl_weight):
    del market_data
    assert as_of == "2026-09-10T20:00:00+00:00"
    assert portfolio["cash_balance"] == portfolio["buying_power"]
    return {
        "schema_version": "qsl.soxl-soxx-three-asset-learning-decision.v1",
        "as_of": as_of,
        "target_values": {
            "SOXL": portfolio["total_equity"] * mid_soxl_weight,
            "SOXX": portfolio["total_equity"] * (0.85 - mid_soxl_weight),
            "BOXX": portfolio["total_equity"] * 0.10,
        },
        "diagnostics": {"blend_gate_mid_soxl_weight": mid_soxl_weight},
        "output_sha256": "d" * 64,
    }


def test_one_session_keeps_independent_cash_share_ledgers_and_charges_next_session_cost() -> None:
    result = advance_paired_shadow_session(
        session=_session(),
        baseline_state=_state(pending=True),
        candidate_state={
            **_state(),
            "pending_target_weights": {"SOXL": 0.55, "SOXX": 0.30, "BOXX": 0.10},
            "pending_cash_weight": 0.05,
        },
        cost_bps=10.0,
        decide=_decide,
    )

    for name, soxl_quantity in (("baseline", 6.493825), ("candidate", 5.494775)):
        leg = result[name]
        state = result[f"{name}_state"]
        assert leg["cost"]["executed_one_way_turnover"] == pytest.approx(0.95)
        assert leg["cost"]["executed_cost"] == pytest.approx(0.95)
        assert leg["cost"]["model"] == "one_way_turnover_all_in_bps"
        assert leg["return"]["period_return"] == pytest.approx(-0.00095)
        assert state["quantities"]["SOXL"] == pytest.approx(soxl_quantity)
        assert state["cash"] == pytest.approx(49.9525)
        marked_equity = state["cash"] + sum(
            state["quantities"][symbol] * 100.0 for symbol in ("SOXL", "SOXX", "BOXX")
        )
        assert marked_equity == pytest.approx(999.05)
        assert state["previous_equity"] == pytest.approx(999.05)
    assert result["baseline_state"] is not result["candidate_state"]
    assert result["baseline"]["signal"]["as_of"] == result["candidate"]["signal"]["as_of"]
    assert result["baseline"]["hypothetical_order"]["execution_timing"] == "next_complete_trading_session"


def test_snapshot_is_frozen_before_decision_and_invalid_or_changed_input_fails_closed() -> None:
    session = _session()
    tampered = copy.deepcopy(session)
    tampered["prices"]["SOXL"] = 101.0

    with pytest.raises(SoxlThreeAssetPairedShadowError, match="input snapshot"):
        advance_paired_shadow_session(
            session=tampered,
            baseline_state=_state(),
            candidate_state=_state(),
            cost_bps=10.0,
            decide=_decide,
        )

    missing_price = _session()
    del missing_price["prices"]["BOXX"]
    missing_price["input_snapshot_sha256"] = input_snapshot_sha256(missing_price)
    with pytest.raises(SoxlThreeAssetPairedShadowError, match="session"):
        advance_paired_shadow_session(
            session=missing_price,
            baseline_state=_state(),
            candidate_state=_state(),
            cost_bps=10.0,
            decide=_decide,
        )
