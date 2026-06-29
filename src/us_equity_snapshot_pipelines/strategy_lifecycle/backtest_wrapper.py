"""US Equity BacktestRunner — wraps existing backtest scripts for the lifecycle system."""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from quant_platform_kit.strategy_lifecycle.backtest_orchestrator import BacktestRunner
from quant_platform_kit.strategy_lifecycle.contracts import BacktestResult

# Known backtest entrypoints for US equity strategies
_US_EQUITY_BACKTEST_SCRIPTS: Mapping[str, str] = {
    "global_etf_rotation": "backtest_us_equity_strategy_candidates",
    "mega_cap_leader_rotation": "backtest_mega_cap_leader_rotation",
    "tqqq_growth_income": "backtest_leveraged_strategy_candidates",
    "soxl_soxx_trend_income": "backtest_leveraged_strategy_candidates",
}


class UsEquityBacktestRunner:
    """BacktestRunner for US Equity strategies.

    Wraps the existing backtest scripts in UsEquitySnapshotPipelines/scripts/.
    """

    def __init__(self, *, scripts_dir: str | None = None):
        import os
        from pathlib import Path

        self._scripts_dir = Path(scripts_dir) if scripts_dir else (
            Path(__file__).resolve().parents[3] / "scripts"
        )

    def run(
        self,
        strategy_profile: str,
        params: Mapping[str, Any],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> BacktestResult:
        """Run backtest for a US equity strategy.

        In production, this would subprocess or import the actual backtest script.
        For now, returns a structured placeholder that validates the interface.
        """
        script_name = _US_EQUITY_BACKTEST_SCRIPTS.get(
            strategy_profile, "backtest_us_equity_strategy_candidates"
        )

        # Placeholder: in production, this would call the actual backtest
        return BacktestResult(
            strategy_profile=strategy_profile,
            domain="us_equity",
            param_set_id="us_eq_1",
            params=dict(params),
            param_version=1,
            sharpe_ratio=1.2,
            calmar_ratio=0.8,
            max_drawdown=-0.15,
            cagr=0.18,
            volatility=0.22,
            win_rate=0.58,
            start_date=start_date or date(2020, 1, 1),
            end_date=end_date or date.today(),
            observation_count=1500,
            benchmark_symbol="buy_hold_SPY",
            source_script=f"scripts/{script_name}.py",
        )


def build_backtest_runner() -> UsEquityBacktestRunner:
    """Factory for the US Equity backtest runner."""
    return UsEquityBacktestRunner()
