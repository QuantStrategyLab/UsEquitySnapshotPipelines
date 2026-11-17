English | [简体中文](twelve-data-shadow-source.zh-CN.md)

# Twelve Data Free Daily Shadow Source

`us_equity_snapshot_pipelines.twelve_data_daily` calls Twelve Data's fixed HTTPS `time_series` daily endpoint with `interval=1day`, `adjust=all`, and fixed start/end dates. The API key is sent solely in the `Authorization: apikey ...` header and never appears in URLs, logs, diagnostics, or artifacts.

It produces only an in-memory independent source observation:

- Success: `READY` with an immutable source root hash and normalized daily bar series;
- Missing key, permission error, rate limit, or network issue: `UNAVAILABLE` with a stable reason code;
- Invalid payload schema, symbol, or OHLCV structure: `INVALID` with a stable reason code.

By itself, the adapter never writes P1, runs backtests, places orders, or treats Twelve Data as a direct substitute for Alpaca SIP. Callers must pass both independent sources to QPK's `assess_multisource_daily_bars`: a publishable `VERIFIED` report is produced only when trading-day coverage, adjustment basis, and OHLCV all pass agreement checks.

Before enabling for any US equity strategy, configure `TWELVE_DATA_API_KEY` in the non-live `market-data-nonlive` environment. Never place the key in chat, source code, workflow parameters, or URLs. Even after enabling, run read-only diagnostics and cross-source agreement backtests first; on failure, keep status `DEGRADED` and block strategy publication.
