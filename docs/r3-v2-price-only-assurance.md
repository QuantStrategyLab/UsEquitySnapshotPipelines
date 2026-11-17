English | [简体中文](r3-v2-price-only-assurance.zh-CN.md)

# R3-v2 Free Dual-Source Price-Only Assurance Diagnostic

R3-v2 price-only is a research diagnostic slice in this repository **independent of the full OHLCV contract**. It compares split-adjusted daily bars between Twelve Data and Yahoo Finance for `QQQ`, `TQQQ`, `SOXX`, and `SOXL`, consuming and comparing open, high, low, and close only across full completed XNYS trading sessions.

Source payloads may still carry volume, but this contract explicitly sets `volume_not_consumed=true`: volume **does not participate** in comparison results or strategy signals, and volume differences alone never approve or override price divergence. If price fields disagree, the outcome remains `NOT_VERIFIED` / `DEGRADED`.

It is isolated from the following paths and must never reuse their results as research inputs:

- R3-v1 (UsEquityStrategies private joint evidence / locked inputs)
- The existing R3-v2 full OHLCV contract (`qsl.r3_v2_free_source_assurance_diagnostic.v1`)

This slice uses an independent `contract_id=r3_v2_price_only` and schema `qsl.r3_v2_price_only_assurance_diagnostic.v1`. Whether the diagnostic returns `VERIFIED`, `PARKED`, or `NOT_VERIFIED`, it fixes `can_promote=false` and `auto_promote=false`: it never auto-promotes, publishes research inputs, rewrites existing TQQQ/SOXL candidates, or grants paper/live authority.

Manual entrypoint: workflow `r3-v2-price-only-assurance-diagnostic.yml` (`contents: read`, `market-data-nonlive`). No cloud access, no `id-token`, no artifact uploads, and no broker or order authority.

## Cutoff stability rule

The latest completed trading day is treated as a T+0 observation only; when unstable it stays parked rather than turning a single latest-bar result into a persistent source verdict. For example, on 2026-09-21 the T+0 diagnostic returned `NOT_VERIFIED`, whereas lookbacks on 2026-09-18 (T+1) and 2026-09-17 (T+2) passed price agreement and XNYS coverage checks across all four symbols:

- [T+0 run <run-id-t0>](#run-t0): price divergence on the latest bar; remains `NOT_VERIFIED`.
- [T+1 run <run-id-t1>](#run-t1): all four symbols `VERIFIED` with complete coverage.
- [T+2 run <run-id-t2>](#run-t2): all four symbols `VERIFIED` with complete coverage.

Research inputs therefore bind to a stable cutoff already cleared by dual-source checks, preferring T+1/T+2; T+0 only observes whether recent data has settled. This rule does not relax price tolerances, switch or mix sources automatically, or alter the `can_promote=false` / `auto_promote=false` research diagnostic boundary.

## Contract boundaries

| Item | Rule |
| --- | --- |
| Adjustment basis | split-adjusted |
| Compared fields | open / high / low / close |
| Volume | May exist in source payloads, but `volume_not_consumed`; `compare_volume=false` |
| Coverage | Complete XNYS completed trading sessions; cutoff must be a completed session |
| Sources | Twelve Data + Yahoo Finance both READY; unavailable or missing coverage fails closed |
| Prohibited | Automatic source switching, mixing sources, averaging prices, relaxing full-OHLCV thresholds, reusing full-OHLCV result identities |

This is a research diagnostic, not a promotion path or a live-recovery path.
