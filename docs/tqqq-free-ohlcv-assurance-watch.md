English | [简体中文](tqqq-free-ohlcv-assurance-watch.zh-CN.md)

# TQQQ Free OHLCV Data Source Calibration Watch

`TQQQ V9 Free OHLCV Assurance Calibration` performs three single-day, read-only cross-checks between Twelve Data and Yahoo split-adjusted daily bars after each completed US trading day: the latest completed trading day (T+0), the prior trading day (T+1), and two trading days prior (T+2). Each trading day therefore accumulates point-in-time evidence across three consecutive runs.

Each probe retains only UTC sample timestamp, requested date window, source response hashes, status, and per-OHLC-field divergence distributions; it uploads no prices, volume, raw responses, paths, or credentials. To stay within Twelve Data's free-tier per-minute request limit, the workflow waits 60 seconds after the eight Twelve Data requests for T+0/T+1 before probing T+2; if still rate-limited, it safely parks as `FREE_SOURCE_UNAVAILABLE` rather than drawing market-data conclusions. GitHub artifacts are retained for 35 days. Using GitHub Actions read-only permissions on its own prior sanitized artifacts, the workflow derives settlement states such as `PENDING_T_PLUS_2`, `REVISED_OR_UNSETTLED`, `SETTLED_VERIFIED`, or `PERSISTENT_CROSS_SOURCE_DISAGREEMENT`.

This is not a backtest, P3 run, shadow run, platform action, or trading job. Even when P1 sources agree completely, the workflow records `VERIFIED` only and never creates research inputs, modifies V9, relaxes tolerances, selects sources, switches strategies, or advances any rollout stage.

Data settlement state is not a strategy verdict. Once enough independent completed trading days are recorded, follow-up calibration must freeze a new candidate data contract separately and re-run backtesting and audits under that candidate; the historical V9 1bp full-OHLC rule remains unchanged. Single or isolated discrepancies never rewrite strategy rules or risk thresholds automatically.

V9 manual P1/P3 research avoids the latest completed trading day that may still be settling, instead pinning the third completed XNYS session back from the latest available completed session (T+2). This merely selects a timing cutoff shown by this watch to settle reliably without altering dual-source checks, the 1bp tolerance, or strategy parameters. If T+2 still fails, research parks safely rather than falling back to one source or relaxing tolerances.
