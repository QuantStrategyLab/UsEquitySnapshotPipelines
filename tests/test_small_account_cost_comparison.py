import math

import numpy as np
import pandas as pd
import pytest

from us_equity_snapshot_pipelines.research import small_account_cost_comparison as study


def _prices(seed=7, days=3400):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2010-01-04", periods=days)
    base = {"QQQ": 0.0005, "SOXX": 0.0006, "SPY": 0.0004, "BIL": 0.0001}
    rows = []
    paths = {}
    for symbol, drift in base.items():
        r = rng.normal(drift, 0.012 if symbol != "BIL" else 0.0002, len(dates))
        paths[symbol] = r
    paths["TQQQ"] = 3 * paths["QQQ"] - 0.0001
    paths["QLD"] = 2 * paths["QQQ"] - 0.00005
    paths["SOXL"] = 3 * paths["SOXX"] - 0.0001
    for symbol, r in paths.items():
        close = 100 * np.cumprod(1 + r)
        rows += [{"symbol": symbol, "as_of": d, "close": c} for d, c in zip(dates, close)]
    return pd.DataFrame(rows)


def test_twins_keep_signal_and_swap_only_instruments():
    variants = {v.label: v for v in study.build_variants()}
    tq = variants["live_tqqq_dual_drive_45_45_proxy (1x twin)"].spec
    assert dict(tq.risk_on_weights) == pytest.approx({"QQQ": 0.90, "BIL": 0.10})
    assert dict(tq.risk_off_weights) == {"BIL": 1.0}
    assert tq.signal_symbol == "QQQ"
    sx = variants["live_soxl_soxx_trend_57_proxy (1x twin)"].spec
    assert dict(sx.risk_on_weights) == pytest.approx({"SOXX": 0.57, "BIL": 0.43})
    assert dict(sx.risk_off_weights) == pytest.approx({"BIL": 1.0})
    assert sx.signal_symbol == variants["live_soxl_soxx_trend_57_proxy (3x)"].spec.signal_symbol
    assert study.spread_bps(sx) == study.SPREAD_BPS_1X
    assert study.spread_bps(variants["live_soxl_soxx_trend_57_proxy (3x)"].spec) == study.SPREAD_BPS_LEVERED


def test_commission_and_tax_reduce_returns():
    idx = pd.bdate_range("2020-12-28", periods=6)
    gross = pd.Series([0.01, 0.0, 0.0, 0.0, 0.0, 0.0], index=idx)
    weights = pd.DataFrame({"QQQ": [1, 1, 1, 0, 0, 0], "BIL": [0, 0, 0, 1, 1, 1]}, index=idx, dtype=float)
    net = study.apply_small_account_costs({"portfolio_returns": gross, "weights_history": weights},
                                          starting_equity=1000.0)
    equity = 1000.0 * (1.01) - 0.35  # first order on day one
    assert net.iloc[0] == pytest.approx(equity / 1000.0 - 1.0)
    # The QQQ->BIL switch on 2020-12-31 costs two orders; the 2020 gain is taxed on 2021-01-01.
    year_end = equity - 2 * 0.35
    assert (1 + net).prod() * 1000.0 == pytest.approx(year_end - 0.2 * (year_end - 1000.0), rel=1e-9)


def test_run_produces_every_trial_and_a_decision_per_pair():
    output = study.run(_prices())
    rows = output["rows"]
    assert len(output["registry"]) == 4 * len(study.STRESS_MULTIPLIERS)
    assert set(rows["period"]) == {"full", "oos"}
    assert {d["pair"] for d in output["decisions"]} == set(study.PROXY_IDS)
    assert {d["verdict"] for d in output["decisions"]} <= {"prefer_1x_twin", "rule_out_3x_on_balanced_tier",
                                                          "report_to_owner"}
    assert rows["dsr"].between(0, 1).all()
    assert "live_ready=False" in study.render_markdown(output)


def test_higher_spread_stress_never_improves_results():
    rows = study.run(_prices(seed=11))["rows"]
    full = rows.loc[rows["period"].eq("full")]
    for variant, group in full.groupby("variant"):
        cagr = group.sort_values("stress")["cagr"].tolist()
        assert all(a >= b - 1e-12 for a, b in zip(cagr, cagr[1:])), variant


def test_dsr_penalizes_many_trials():
    few = study.deflated_sharpe_probability(1.0, 2500, 0.0, 3.0, [1.0, 0.5])
    many = study.deflated_sharpe_probability(1.0, 2500, 0.0, 3.0, [1.0, 0.5] + [0.9, 0.2, 0.7, 0.4] * 10)
    assert not math.isnan(few) and many <= few
