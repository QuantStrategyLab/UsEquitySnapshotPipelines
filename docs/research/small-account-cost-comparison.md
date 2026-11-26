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
2. **Data (authorized source, dates and purpose).** Public daily adjusted closes from Yahoo
   Finance through the repository's existing `yfinance` helper, downloaded from 2009-01-01
   (warm-up for the 200-session trend filter) to the run's end date, for this comparison
   only. Nothing is committed. A run must pass the authorization id
   `small-account-cost-comparison-2026-11-26`, and fails if any required symbol does not
   cover the window.
3. **Periods.** Full history from 2010-04-01, and 2023-01-01 onward reported separately.
   The later period is a historical subperiod, not an untouched holdout. Each period starts
   a fresh USD 1,500 account.
4. **Costs.**
   - Holdings drift between signal changes. Every trade to a new target pays its own
     symbol's bid-ask spread (1 bp for 1x ETFs, 3 bp for 2x/3x ETFs; stress x2 and x3) and
     one IBKR minimum commission of USD 0.35.
   - 20% China tax on each calendar year's positive return, paid at the start of the next
     year. This is an upper bound: it treats every gain as realized yearly. US dividend
     withholding (10%) is creditable against it in gain years; in a year without a
     portfolio gain the withholding is not modelled separately, which slightly flatters
     dividend-heavy holdings such as BIL in loss years.
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
     return, independently of whether its twin qualifies;
   - anything else is reported to the Owner, and no strategy is changed by this study.

## Running

`.github/workflows/small-account-cost-comparison.yml` (manual dispatch) runs
`us_equity_snapshot_pipelines.research.small_account_cost_comparison` and writes the report
to the run summary and a 30-day artifact. The result is evidence for the three-tier
selector and the capital ladder; it does not authorize any live change.
