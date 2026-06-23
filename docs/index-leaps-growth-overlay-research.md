# Index LEAPS Growth Overlay Research

This module is a lightweight proxy research scaffold for `QQQ` / `SPY` LEAPS call overlays. It is
intentionally separate from runtime strategy code: strategy repositories generate option order intents,
while this repository produces research artifacts and review evidence.

Run example:

```bash
useq-research-index-leaps-growth-overlay \
  --prices data/prices.csv \
  --underlier QQQ \
  --output-dir artifacts/index-leaps-growth-overlay/qqq_proxy
```

The input CSV needs `symbol`, `as_of` or `date`, and `close` columns. Outputs are:

- `summary.csv`
- `daily_equity.csv`
- `trades.csv`
- `manifest.json`

The current implementation uses a Black-Scholes proxy: realized volatility is converted into estimated
IV with a floor/cap, the strike is solved from target delta, and the option is re-priced daily from the
remaining DTE. This is useful for screening strategy shape, but it is not live-promotion evidence.

For real option-chain evidence, pass a historical chain CSV:

```bash
useq-research-index-leaps-growth-overlay \
  --prices data/prices.csv \
  --option-chain data/option_chain.csv \
  --mode option-chain \
  --underlier QQQ \
  --output-dir artifacts/index-leaps-growth-overlay/qqq_chain
```

The chain CSV accepts common aliases for `as_of`, `underlier`, `expiration`, `right`, `strike`, `bid`,
`ask`, `delta`, `volume`, `open_interest`, and `option_symbol`. The backtest opens at ask, marks and
closes at bid, filters by target delta, DTE, spread, volume, and open interest, and marks
`promotion_evidence=false` if any held quote is missing.

Promotion still requires real historical option-chain data with bid/ask, expiration, strike, right,
volume, open interest, greeks or reproducible model greeks, dividends, rates, and transaction-cost assumptions.
