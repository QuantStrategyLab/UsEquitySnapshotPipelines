English | [简体中文](tqqq-free-ohlcv-assurance-calibration.zh-CN.md)

# TQQQ Free OHLCV Dual-Source Calibration Boundary

TQQQ's free P1 uses Twelve Data as canonical and Yahoo Finance as verifier. Both may return data successfully while exhibiting minor OHLC differences, which is distinct from service unavailability, missing keys, or insufficient coverage.

When P1 cannot verify, terminal states distinguish two causes without altering the strategy:

- `FREE_SOURCE_UNAVAILABLE`: at least one required source is unavailable, lacks coverage, or lacks enough diagnostic evidence to confirm price divergence;
- `FREE_SOURCE_DISAGREEMENT`: both sources are `READY`, but at least one symbol shows `daily_bar_price_divergence`.

For parked runs, `availability_diagnostic.price_agreement.field_delta_bps` reports only per-OHLC-field compared session counts, days exceeding existing tolerance, and nearest-rank p50/p95/p99/max differences in bps. It contains no raw prices, bar rows, URLs, upstream responses, paths, or credentials.

These statistics are **inputs to independent data-quality calibration, not automatic relaxation rules**. Creating a new candidate data contract requires pre-registering sample windows, field-level tolerance methodology, hard caps, anomaly/missing-data handling, dual-source READY requirements, and fail-park rules in a calibration record separate from strategy P1/P2/P3. Any new contract may neither reuse prior candidates' P3 results nor modify frozen candidates, strategy parameters, runtimes, platforms, or orders.
