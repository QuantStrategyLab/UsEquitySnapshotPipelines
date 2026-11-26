# Small-account cost comparison: pre-registration

Registered 2026-11-26, approved by the Owner before any run. Research only: no live
change, no orders, no frozen baseline rewritten.

## Question

For a USD 1,000–2,000 IBKR account of a China tax resident on the balanced tier (maximum
drawdown target about 30%), does each current 3x ETF strategy beat its 1x twin after every
cost?

## Rules fixed before running

1. **No tuning.** Each proxy (`live_tqqq_dual_drive_45_45_proxy`,
   `live_soxl_soxx_trend_57_proxy`) keeps its frozen signal and weights. The 1x twin swaps
   only the traded instruments: TQQQ and QLD become QQQ, SOXL becomes SOXX, and a risk-off
   leg that held SOXX moves one notch lower, to BIL.
2. **Data.** Public daily adjusted closes from 2010-04-01. Nothing is committed.
3. **Periods.** Full history, and out of sample from 2023-01-01, reported separately.
4. **Costs.**
   - Bid-ask spread on turnover: 1 bp for 1x ETFs, 3 bp for 2x/3x ETFs; stress x2 and x3.
   - IBKR minimum commission of USD 0.35 per order on a USD 1,500 equity path.
   - 20% China tax on each calendar year's positive return, paid at the start of the next
     year. This is an upper bound: it treats every gain as realized yearly; the 10% US
     dividend withholding is creditable against it.
   - Fund expense ratios and 3x financing and rebalancing costs are already inside adjusted
     prices and are not deducted again.
5. **Metrics.** CAGR, maximum drawdown, Calmar, Sharpe, and the deflated Sharpe ratio over
   every trial run.
6. **Trial registry.** Every variant at every cost tier is listed in the output, whether or
   not its result is good.
7. **Decision**, at base costs on the full period:
   - the 1x twin is preferred when its maximum drawdown is within 30% and its net Calmar is
     not lower than the 3x proxy's;
   - a 3x proxy whose drawdown exceeds 30% is ruled out on the balanced tier, whatever its
     return;
   - anything else is reported to the Owner, and no strategy is changed by this study.

## Running

`.github/workflows/small-account-cost-comparison.yml` (manual dispatch) runs
`us_equity_snapshot_pipelines.research.small_account_cost_comparison` and writes the report
to the run summary and a 30-day artifact. The result is evidence for the three-tier
selector and the capital ladder; it does not authorize any live change.
