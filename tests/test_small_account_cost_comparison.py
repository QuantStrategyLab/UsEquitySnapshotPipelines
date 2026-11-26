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
    assert study.symbol_spread_bps("SOXL") == study.SPREAD_BPS_LEVERED
    assert study.symbol_spread_bps("BIL") == study.SPREAD_BPS_1X


def _frame(rows, cols):
    idx = pd.bdate_range("2020-12-28", periods=len(rows))
    return pd.DataFrame(rows, index=idx, columns=cols, dtype=float)


def test_trades_pay_per_symbol_spread_and_minimum_commission():
    targets = _frame([[1, 0], [1, 0], [0, 1]], ["TQQQ", "BIL"])
    returns = _frame([[0, 0], [0, 0], [0, 0]], ["TQQQ", "BIL"])
    net = study.simulate_account(targets, returns, starting_equity=1000.0, tax_rate=0.0)
    first = 1000.0 * 3 / 10_000 + 0.35  # buy TQQQ: 3 bp + commission
    assert (1 + net.iloc[0]) * 1000.0 == pytest.approx(1000.0 - first)
    v = 1000.0 - first
    switch = v * 3 / 10_000 + 0.35 + v * 1 / 10_000 + 0.35  # sell TQQQ (3 bp) and buy BIL (1 bp)
    assert (1 + net).prod() * 1000.0 == pytest.approx(v - switch, rel=1e-9)


def test_holdings_drift_without_trading_between_signal_changes():
    targets = _frame([[0.5, 0.5]] * 3, ["QQQ", "BIL"])
    returns = _frame([[0, 0], [0.10, 0], [0.10, 0]], ["QQQ", "BIL"])
    net = study.simulate_account(targets, returns, starting_equity=1000.0, tax_rate=0.0)
    v0 = 1000.0 - (500 * 1e-4 + 0.35) * 2
    expected = v0 / 2 * 1.1 * 1.1 + v0 / 2  # drifted, never rebalanced
    assert (1 + net).prod() * 1000.0 == pytest.approx(expected, rel=1e-9)


def test_yearly_tax_on_positive_gain():
    targets = _frame([[1.0]] * 6, ["QQQ"])
    returns = _frame([[0.10], [0], [0], [0], [0], [0]], ["QQQ"])
    net = study.simulate_account(targets, returns, starting_equity=1000.0, min_commission=0.0,
                                 spread_multiplier=0.0)
    gain = 100.0
    assert (1 + net).prod() * 1000.0 == pytest.approx(1100.0 - 0.2 * gain, rel=1e-9)


def test_missing_symbol_fails_coverage():
    prices = _prices()
    with pytest.raises(ValueError, match="TQQQ"):
        study.validate_coverage(prices.loc[prices["symbol"] != "TQQQ"], study.FULL_START)


def test_run_produces_every_trial_and_a_decision_per_pair():
    output = study.run(_prices())
    rows = output["rows"]
    assert len(output["registry"]) == 4 * len(study.STRESS_MULTIPLIERS)
    assert set(rows["period"]) == {"full", "oos"}
    assert {d["pair"] for d in output["decisions"]} == set(study.PROXY_IDS)
    for d in output["decisions"]:
        assert ("rule_out_3x_on_balanced_tier" in d["verdicts"]) == d["three_x_out_on_balanced"]
    assert rows["dsr"].between(0, 1).all()
    assert "live_ready=False" in study.render_markdown(output)


def test_higher_spread_stress_never_improves_results():
    rows = study.run(_prices(seed=11))["rows"]
    full = rows.loc[rows["period"].eq("full")]
    for variant, group in full.groupby("variant"):
        cagr = group.sort_values("stress")["cagr"].tolist()
        assert all(a >= b - 1e-12 for a, b in zip(cagr, cagr[1:])), variant


def test_decision_rules_are_independent():
    def frame(three_dd, one_dd, three_calmar, one_calmar):
        return pd.DataFrame([
            {"pair": "p", "leverage": "3x", "stress": 1.0, "period": "full", "max_drawdown": three_dd, "calmar": three_calmar},
            {"pair": "p", "leverage": "1x", "stress": 1.0, "period": "full", "max_drawdown": one_dd, "calmar": one_calmar}])
    [both_breach] = study.decide(frame(-0.40, -0.35, 1.0, 0.5))
    assert both_breach["verdicts"] == ["rule_out_3x_on_balanced_tier", "report_to_owner"]
    [twin_wins] = study.decide(frame(-0.45, -0.20, 0.4, 0.6))
    assert twin_wins["verdicts"] == ["rule_out_3x_on_balanced_tier", "prefer_1x_twin"]


def test_drawdown_counts_from_opening_capital():
    idx = pd.bdate_range("2021-01-04", periods=3)
    assert study.metrics(pd.Series([-0.10, 0.0, 0.05], index=idx))["max_drawdown"] == pytest.approx(-0.10)


def test_main_requires_authorization(tmp_path):
    with pytest.raises(SystemExit):
        study.main(["--authorization", "wrong", "--output-dir", str(tmp_path)])


def test_dsr_penalizes_many_trials():
    few = study.deflated_sharpe_probability(1.0, 2500, 0.0, 3.0, [1.0, 0.5])
    many = study.deflated_sharpe_probability(1.0, 2500, 0.0, 3.0, [1.0, 0.5] + [0.9, 0.2, 0.7, 0.4] * 10)
    assert not math.isnan(few) and many <= few
