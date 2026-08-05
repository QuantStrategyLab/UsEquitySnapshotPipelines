"""US Equity BacktestRunner — wraps existing backtest scripts for the lifecycle system."""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

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

    runner_kind = "placeholder"

    def __init__(self, *, scripts_dir: str | None = None):
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
        """Reject the legacy placeholder instead of returning fabricated metrics."""
        raise RuntimeError(
            "legacy US equity backtest wrapper is a placeholder; "
            "use an explicit runner_kind='real' promotion runner"
        )


def build_backtest_runner() -> UsEquityBacktestRunner:
    """Factory for the US Equity backtest runner."""
    return UsEquityBacktestRunner()
