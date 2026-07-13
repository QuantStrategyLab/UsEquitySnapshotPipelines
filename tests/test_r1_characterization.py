from __future__ import annotations

import json

import pytest

from us_equity_snapshot_pipelines.backtest.r1_characterization import characterize_profile


class RealOrchestrator:
    def __init__(self):
        self.calls = []

    def run(self, *, profile, params, execution_timing):
        self.calls.append((profile, params, execution_timing))
        return {"trades": 2, "final_equity": 101.5, "source": "fixture-input"}


def test_profiles_and_next_open_are_explicit(tmp_path):
    orchestrator = RealOrchestrator()
    artifact = characterize_profile(
        orchestrator, "soxl", params={"lookback": 20}, execution_timing="next_open", ephemeral_dir=tmp_path
    )
    assert artifact["profile"] == "SOXL"
    assert artifact["execution_timing"] == "next_open"
    assert orchestrator.calls == [("SOXL", {"lookback": 20}, "next_open")]
    assert json.loads((tmp_path / "soxl_next_open.json").read_text())["field_inventory"] == [
        "final_equity", "source", "trades"
    ]


def test_next_close_and_tqqq_are_explicit(tmp_path):
    artifact = characterize_profile(
        RealOrchestrator(), "TQQQ", params={}, execution_timing="next_close", ephemeral_dir=tmp_path
    )
    assert artifact["profile"] == "TQQQ"
    assert artifact["execution_timing"] == "next_close"


def test_placeholder_result_is_rejected(tmp_path):
    class Placeholder:
        def run(self, **_kwargs):
            return {"placeholder": True}

    with pytest.raises(ValueError, match="placeholder"):
        characterize_profile(Placeholder(), "SOXL", params={}, execution_timing="next_open", ephemeral_dir=tmp_path)


@pytest.mark.parametrize("profile", ["", "SPY", "soxl_soxx_trend_income"])
def test_only_target_profiles_are_allowed(profile, tmp_path):
    with pytest.raises(ValueError, match="unsupported R1 profile"):
        characterize_profile(RealOrchestrator(), profile, params={}, execution_timing="next_open", ephemeral_dir=tmp_path)
