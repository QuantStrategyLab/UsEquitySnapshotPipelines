from __future__ import annotations

import pytest

from us_equity_snapshot_pipelines.lifecycle.backtest_wrapper import (
    UsEquityBacktestRunner,
)


def test_legacy_wrapper_is_explicit_placeholder_and_cannot_run() -> None:
    runner = UsEquityBacktestRunner()

    assert runner.runner_kind == "placeholder"
    with pytest.raises(RuntimeError, match="placeholder"):
        runner.run("soxl_soxx_trend_income", {})
