English | [简体中文](r3-v2-free-source-assurance.zh-CN.md)

# R3-v2 Free Dual-Source Full OHLCV Assurance Diagnostic

R3-v2 is a standalone research diagnostic slice in this repository that compares split-adjusted daily bars between Twelve Data and Yahoo Finance for `QQQ`, `TQQQ`, `SOXX`, and `SOXL` only. It requires open, high, low, close, and volume to match across completed XNYS trading sessions; output is a sanitized diagnostic only, not a published research-input identity.

A separate [R3-v2 price-only](r3-v2-price-only-assurance.md) contract compares OHLC only with `volume_not_consumed`. Its schema, `contract_id`, and candidate identity are never shared with this contract, and full-OHLCV thresholds are never relaxed because of price-only.

It is isolated from R3-v1 (UsEquityStrategies private joint evidence / locked inputs): it never reuses, replaces, or auto-wires into R3-v1. Whether the diagnostic reports `VERIFIED`, `PARKED`, or `NOT_VERIFIED`, it never auto-promotes, rewrites existing TQQQ/SOXL candidates, or grants paper/live authority.

Manual entrypoint: workflow `r3-v2-free-source-assurance-diagnostic.yml` (`contents: read`, `market-data-nonlive`). No cloud access, no `id-token`, no artifact uploads, and no broker or order authority.

## Current data source roles

| Context | Current role | Rule |
| --- | --- | --- |
| Production trading | Each platform's own broker/exchange market data and account APIs | Never wire Yahoo or Twelve Data or place orders from research sources |
| R3-v1 | Legacy locked private Yahoo input | Keep original identity and remain `PARKED`; never mix with new sources |
| R3-v2 | Twelve Data canonical, Yahoo verifier | Research inputs may form only when both sources are READY, coverage is complete, and full OHLCV agrees; no auto-promotion |
| Alpaca SIP | Not adopted; parked | Free plan does not meet current SIP data requirements; do not subscribe, fall back, or switch sources without separate paid-plan approval |
| Other free providers | Not wired into candidates | Add no new keys, dependencies, workflows, or implicit fallbacks |

Retiring a path means clearing active runtime usage rather than deleting adapters still referenced by tests, audits, or future candidates. When any source is unavailable or disagrees, status stays `PARKED/NOT_VERIFIED` with no automatic source switching, mixing, or price averaging.
