from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from us_equity_snapshot_pipelines.backtest.r1_characterization import characterize_profile


SOURCE_SHA = "a" * 40


class RealOrchestrator:
    def __init__(self):
        self.calls = []

    def run(self, *, profile, params, execution_timing):
        self.calls.append((profile, params, execution_timing))
        return {"trades": 2, "final_equity": 101.5, "source": "fixture-input"}


def _characterize(orchestrator, tmp_path, **overrides):
    kwargs = {
        "params": {"lookback": 20},
        "execution_timing": "next_open",
        "ephemeral_dir": tmp_path,
        "source_sha": SOURCE_SHA,
    }
    kwargs.update(overrides)
    return characterize_profile(orchestrator, "SOXL", **kwargs)


def test_profiles_source_sha_and_next_open_are_explicit(tmp_path):
    orchestrator = RealOrchestrator()
    artifact = _characterize(orchestrator, tmp_path)

    assert artifact["profile"] == "SOXL"
    assert artifact["execution_timing"] == "next_open"
    assert artifact["source_sha"] == SOURCE_SHA
    assert orchestrator.calls == [("SOXL", {"lookback": 20}, "next_open")]
    persisted = json.loads(next(tmp_path.glob("soxl_next_open_*.json")).read_text())
    assert persisted["field_inventory"] == ["final_equity", "source", "trades"]
    assert len(artifact["artifact_sha256"]) == 64


def test_real_pandas_and_scalar_results_are_json_serializable(tmp_path):
    class PandasOrchestrator:
        def run(self, **_kwargs):
            index = pd.to_datetime(["2026-07-10", "2026-07-11"])
            return {
                "returns": pd.Series([np.float64(0.1), np.nan], index=index, name="return"),
                "weights": pd.DataFrame({"SOXL": [0.5, 0.0]}, index=index),
                "as_of": pd.Timestamp("2026-07-11", tz="UTC"),
                "generated_at": datetime(2026, 7, 11, tzinfo=timezone.utc),
                "session_date": date(2026, 7, 11),
                "capital": Decimal("100000.25"),
            }

    artifact = _characterize(PandasOrchestrator(), tmp_path)
    persisted = json.loads(open(artifact["artifact_path"], encoding="utf-8").read())

    assert persisted["result"]["returns"]["type"] == "pandas.Series"
    assert persisted["result"]["returns"]["data"] == [0.1, None]
    assert persisted["result"]["weights"]["type"] == "pandas.DataFrame"
    assert persisted["result"]["weights"]["columns"] == ["SOXL"]
    assert persisted["result"]["capital"] == {"type": "decimal", "value": "100000.25"}
    assert persisted["result"]["as_of"] == "2026-07-11T00:00:00+00:00"


def test_same_run_is_created_exclusively(tmp_path):
    kwargs = dict(params={"lookback": 20}, execution_timing="next_open", ephemeral_dir=tmp_path, source_sha=SOURCE_SHA)
    characterize_profile(RealOrchestrator(), "SOXL", **kwargs)
    with pytest.raises(FileExistsError, match="already exists"):
        characterize_profile(RealOrchestrator(), "SOXL", **kwargs)


def test_next_close_and_tqqq_are_explicit(tmp_path):
    artifact = characterize_profile(
        RealOrchestrator(), "TQQQ", params={}, execution_timing="next_close", ephemeral_dir=tmp_path, source_sha=SOURCE_SHA
    )
    assert artifact["profile"] == "TQQQ"
    assert artifact["execution_timing"] == "next_close"


def test_placeholder_result_is_rejected(tmp_path):
    class Placeholder:
        def run(self, **_kwargs):
            return {"placeholder": True}

    with pytest.raises(ValueError, match="placeholder"):
        _characterize(Placeholder(), tmp_path)


@pytest.mark.parametrize("profile", ["", "SPY", "soxl_soxx_trend_income"])
def test_only_target_profiles_are_allowed(profile, tmp_path):
    with pytest.raises(ValueError, match="unsupported R1 profile"):
        characterize_profile(
            RealOrchestrator(),
            profile,
            params={},
            execution_timing="next_open",
            ephemeral_dir=tmp_path,
            source_sha=SOURCE_SHA,
        )


@pytest.mark.parametrize("source_sha", ["", "A" * 40, "a" * 39, "g" * 40])
def test_source_sha_must_be_canonical(source_sha, tmp_path):
    with pytest.raises(ValueError, match="source_sha"):
        characterize_profile(
            RealOrchestrator(), "SOXL", params={}, execution_timing="next_open", ephemeral_dir=tmp_path, source_sha=source_sha
        )
