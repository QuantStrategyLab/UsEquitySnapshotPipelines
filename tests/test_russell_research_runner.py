from __future__ import annotations

import json
import socket
from datetime import date

import pandas as pd
import pytest

from us_equity_snapshot_pipelines.lifecycle.russell_research_runner import (
    PROFILE,
    RussellResearchRunner,
    _rebalance,
    run_via_orchestrator,
)


def _features(*, as_of: str = "2024-01-31", available_at: str = "2024-01-31 16:00:00-05:00") -> pd.DataFrame:
    rows = []
    for rank, symbol in enumerate(("AAA", "BBB", "CCC", "DDD", "QQQ", "SPY", "BOXX"), start=1):
        rows.append(
            {
                "as_of": as_of,
                "available_at": available_at,
                "symbol": symbol,
                "sector": "Technology" if rank < 3 else "Health Care",
                "close": 100.0 + rank,
                "adv20_usd": 100_000_000.0,
                "history_days": 300,
                "mom_3m": 0.20 - rank * 0.01,
                "mom_6m": 0.40 - rank * 0.01,
                "mom_12_1": 0.50 - rank * 0.01,
                "rel_mom_6m_vs_benchmark": 0.30 - rank * 0.01,
                "rel_mom_6m_vs_broad_benchmark": 0.25 - rank * 0.01,
                "high_252_gap": -0.02,
                "sma200_gap": 0.10,
                "vol_63": 0.20,
                "maxdd_126": -0.05,
                "eligible": symbol not in {"QQQ", "SPY", "BOXX"},
            }
        )
    return pd.DataFrame(rows)


def _prices() -> pd.DataFrame:
    rows = []
    for symbol in ("AAA", "BBB", "CCC", "DDD", "BOXX", "QQQ", "SPY"):
        for session, open_price, close_price in (
            ("2024-01-31", 100.0, 100.0),
            ("2024-02-01", 100.0, 110.0 if symbol == "AAA" else 100.0),
            ("2024-02-02", 100.0, 100.0),
        ):
            rows.append({"session": session, "symbol": symbol, "open": open_price, "close": close_price})
    return pd.DataFrame(rows)


def _params(**overrides):
    params = {
        "variant": "blend_top2_50_top4_50",
    }
    params.update(overrides)
    return params


def _runner(**overrides):
    return RussellResearchRunner(
        feature_snapshots=overrides.pop("feature_snapshots", _features()),
        prices=overrides.pop("prices", _prices()),
        data_kind=overrides.pop("data_kind", "synthetic"),
        initial_equity=overrides.pop("equity", 100_000.0),
        cost_bps=overrides.pop("cost_bps", 25.0),
    )


def test_runner_uses_core_signal_and_preserves_shares_on_noop(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network")))
    monkeypatch.setattr(socket, "create_connection", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network")))

    runner = _runner()
    result = runner.run(PROFILE, _params())

    assert result.params["runner_kind"] == "research_only"
    assert result.params["learning_only"] is True
    assert result.params["promotion_eligible"] is False
    assert result.params["live_ready"] is False
    assert result.params["size_zero_required"] is True
    assert result.params["no_order"] is True
    assert result.params["data_kind"] == "synthetic"
    daily = runner.last_artifacts["daily"]
    trades = runner.last_artifacts["trades"]
    assert len(daily) == 3
    assert not trades.empty
    assert (daily["shares_AAA"].diff().fillna(0.0).iloc[2:] == 0.0).all()
    assert float(daily["cash"].min()) >= -1e-8


def test_runner_rejects_delayed_snapshot_and_bad_prices():
    delayed = _features(available_at="2024-02-01 10:00:00-05:00")
    runner = _runner(feature_snapshots=delayed)
    runner.run(PROFILE, _params())
    assert (runner.last_artifacts["trades"]["session"] != "2024-02-01").all()
    assert runner.last_artifacts["daily"].loc[1, "no_op_reason"] == "no_snapshot_available_before_open"
    assert not runner.last_artifacts["trades"].empty
    bad_prices = _prices()
    bad_prices.loc[0, "open"] = 0.0
    with pytest.raises(ValueError, match="price"):
        _runner(prices=bad_prices).run(PROFILE, _params())


def test_runner_rejects_duplicate_snapshot_or_price_rows():
    with pytest.raises(ValueError, match="duplicate"):
        _runner(feature_snapshots=pd.concat([_features(), _features().iloc[[0]]])).run(PROFILE, _params())
    with pytest.raises(ValueError, match="duplicate"):
        _runner(prices=pd.concat([_prices(), _prices().iloc[[0]]])).run(PROFILE, _params())


def test_runner_calls_qpk_orchestrator_and_does_not_offer_promotion():
    class Store:
        def __init__(self):
            self.saved = []

        def save_backtest_result(self, result):
            self.saved.append(result)

    store = Store()
    runner = _runner()
    result = run_via_orchestrator(runner, _params(), store=store)
    assert result.strategy_profile == PROFILE
    assert result.params["promotion_eligible"] is False
    assert store.saved and store.saved[-1].params["runner_kind"] == "research_only"
    assert not hasattr(runner, "run_purged_fold")


def test_runner_forwards_named_variant_to_core_signal(monkeypatch):
    runner = _runner()
    captured = {}
    original = __import__("us_equity_strategies.strategies.mega_cap_leader_rotation", fromlist=["compute_signals"]).compute_signals

    def compute(snapshot, holdings, **kwargs):
        captured.update(kwargs)
        return original(snapshot, holdings, **kwargs)

    monkeypatch.setattr("us_equity_strategies.strategies.mega_cap_leader_rotation.compute_signals", compute)
    runner.run(PROFILE, _params(variant="top4_baseline"))
    assert captured["leader_rotation_profile_variant"] == "top4_baseline"


def test_same_day_snapshot_waits_until_next_session():
    snapshot = _features(as_of="2024-02-01", available_at="2024-01-31 16:00:00-05:00")
    runner = _runner(feature_snapshots=snapshot)
    runner.run(PROFILE, _params())
    daily = runner.last_artifacts["daily"]
    assert daily.loc[1, "no_op_reason"] == "no_snapshot_available_before_open"
    assert float(daily.loc[1, "equity"]) == pytest.approx(100_000.0)
    assert not runner.last_artifacts["trades"].empty


def test_transaction_cost_is_self_financed():
    runner = _runner(cost_bps=100.0)
    runner.run(PROFILE, _params())
    assert float(runner.last_artifacts["daily"]["cash"].min()) >= -1e-8


def test_first_day_loss_counts_initial_equity_in_drawdown():
    result = _runner().run(PROFILE, _params(), start_date=date(2024, 2, 2))
    assert result.total_return < 0
    assert result.max_drawdown == pytest.approx(result.total_return)


def test_open_gap_sizes_targets_from_open_equity():
    prices = pd.DataFrame([
        {"symbol": "AAA", "open": 200.0},
        {"symbol": "BBB", "open": 100.0},
    ])
    shares, cash, trades = _rebalance({"AAA": 100.0}, 0.0, {"AAA": 0.5, "BBB": 0.5}, prices, 10_000.0, 0.0)
    assert shares["AAA"] == pytest.approx(50.0)
    assert shares["BBB"] == pytest.approx(100.0)
    assert cash == pytest.approx(0.0)
    assert sum(row["notional"] for row in trades) == pytest.approx(20_000.0)


def test_each_snapshot_needs_benchmarks_and_named_variants_differ():
    missing = pd.concat([_features(), _features(as_of="2024-02-01").query("symbol != 'QQQ'")])
    with pytest.raises(ValueError, match="missing benchmark"):
        _runner(feature_snapshots=missing).run(PROFILE, _params())
    from us_equity_snapshot_pipelines.lifecycle.russell_research_runner import _target_weights
    expected = {
        "top4_baseline": {"AAA": 0.25, "BBB": 0.25, "CCC": 0.25, "DDD": 0.25},
        "blend_top2_25_top4_75": {"AAA": 0.3125, "BBB": 0.3125, "CCC": 0.1875, "DDD": 0.1875},
        "blend_top2_50_top4_50": {"AAA": 0.375, "BBB": 0.375, "CCC": 0.125, "DDD": 0.125},
    }
    for variant, weights in expected.items():
        actual, _ = _target_weights(_features(), pd.Timestamp("2024-02-01"), {}, 100_000.0, variant)
        assert actual == pytest.approx(weights)


def test_constructor_freezes_data_and_research_flags():
    runner = _runner()
    with pytest.raises(ValueError, match="promotion_eligible"):
        runner.run(PROFILE, {"variant": "top4_baseline", "promotion_eligible": True})
    with pytest.raises(ValueError, match="data_kind"):
        run_via_orchestrator(runner, {"variant": "top4_baseline", "data_kind": "research"})


def test_datetime_contract_and_latest_snapshot_no_fallback():
    naive = _features(available_at="2024-01-31 16:00:00")
    with pytest.raises(ValueError, match="timezone"):
        _runner(feature_snapshots=naive).run(PROFILE, _params())
    older = _features(as_of="2024-01-30")
    newest_delayed = _features(available_at="2024-02-01 10:00:00-05:00")
    baseline = _runner(feature_snapshots=older)
    baseline.run(PROFILE, _params())
    combined = _runner(feature_snapshots=pd.concat([older, newest_delayed], ignore_index=True))
    combined.run(PROFILE, _params())
    pd.testing.assert_frame_equal(
        baseline.last_artifacts["daily"].query("session <= '2024-02-01'").reset_index(drop=True),
        combined.last_artifacts["daily"].query("session <= '2024-02-01'").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        baseline.last_artifacts["trades"].query("session <= '2024-02-01'").reset_index(drop=True),
        combined.last_artifacts["trades"].query("session <= '2024-02-01'").reset_index(drop=True),
    )


def test_cli_is_offline_and_writes_qpk_result(tmp_path, monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("network")))
    features_path = tmp_path / "features.csv"
    prices_path = tmp_path / "prices.csv"
    _features().to_csv(features_path, index=False)
    _prices().to_csv(prices_path, index=False)
    from us_equity_snapshot_pipelines.lifecycle.russell_research_runner import main
    output = tmp_path / "out"
    main(["--features", str(features_path), "--prices", str(prices_path), "--data-kind", "synthetic", "--output", str(output)])
    result = json.loads((output / "result.json").read_text())
    assert result["params"]["research_scope"] == "core_signal_only"
    assert (output / "daily.csv").exists() and (output / "trades.csv").exists()


def test_qpk_performance_store_persists_scalar_research_params(tmp_path):
    from quant_platform_kit.strategy_lifecycle.performance_store import PerformanceStore
    store = PerformanceStore(local_root=tmp_path)
    result = run_via_orchestrator(_runner(), _params(), store=store)
    assert result.params["data_kind"] == "synthetic"
    assert result.params["equity"] == 100_000.0
    assert result.params["cost_bps"] == 25.0
    files = list(tmp_path.rglob("*.json"))
    assert files
    persisted = json.loads(files[0].read_text())
    assert persisted["params"]["promotion_eligible"] is False
    assert "feature_snapshots" not in persisted["params"]
