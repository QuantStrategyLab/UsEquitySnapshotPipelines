# Tech/Communication Pullback Enhancement backtest refresh

Date: 2026-06-03

This note refreshes local, reproducible evidence for the existing
`tech_communication_pullback_enhancement` runtime profile. It is not a new
strategy candidate and should not be counted as a supplemental strategy.

## Command

```bash
PYTHONPATH=src:/Users/lisiyi/Projects/UsEquityStrategies/src:/Users/lisiyi/Projects/QuantPlatformKit/src \
.venv/bin/python scripts/backtest_tech_communication_pullback.py \
  --prices /Users/lisiyi/Projects/_local_runs/r1000_multifactor_defensive_20260403_official_monthly_v2_alias/r1000_price_history.csv \
  --extra-prices data/output/us_equity_strategy_candidate_research_20260603/downloaded_etf_price_history.csv \
  --universe /Users/lisiyi/Projects/_local_runs/r1000_multifactor_defensive_20260403_official_monthly_v2_alias/r1000_universe_history.csv \
  --config-path /Users/lisiyi/Projects/LongBridgePlatform/research/configs/growth_pullback_tech_communication_pullback_enhancement.json \
  --periods short:2025-06-01:2026-04-02,medium:2023-06-01:2026-04-02,long:2018-01-01:2026-04-02 \
  --output-dir data/output/tech_communication_pullback_enhancement_backtest_20260603
```

Outputs are written under:

```text
data/output/tech_communication_pullback_enhancement_backtest_20260603/
```

`data/output` is intentionally ignored by Git; rerun the command above to
regenerate the CSV evidence.

## Assumptions

- Uses the historical Russell 1000 universe history from the existing local
  Russell 1000 official monthly run.
- Adds QQQ price history from the existing US equity candidate research
  yfinance download because the Russell 1000 price history does not include QQQ.
- Uses the LongBridge tech/communication runtime config.
- Uses monthly rebalance, next-trading-day execution, 5 bps one-way turnover
  cost, and BOXX as the safe-haven sleeve.

## Period summary

| Period | Start | End | CAGR | QQQ CAGR | SPY CAGR | Max Drawdown | Sharpe |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| short | 2025-06-02 | 2026-04-02 | 79.94% | 16.11% | 15.26% | -15.23% | 2.29 |
| medium | 2023-06-01 | 2026-04-02 | 56.90% | 20.84% | 18.82% | -19.18% | 1.89 |
| long | 2018-01-02 | 2026-04-02 | 23.48% | 18.17% | 13.26% | -30.84% | 1.03 |

## Gate conclusion

The refreshed run confirms that `tech_communication_pullback_enhancement`
outperformed both QQQ and SPY across the short, medium, and long windows.

The long-window max drawdown is `-30.84%`, slightly worse than a hard 30% line.
Given the prior archive also had this profile around the same drawdown band and
the user explicitly accepted keeping it live, the profile should stay
`runtime_enabled` rather than being downgraded to research.
