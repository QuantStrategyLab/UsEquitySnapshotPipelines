English | [简体中文](free-daily-source-crosscheck.zh-CN.md)

# Free Daily Source Cross-Check

Free daily shadow validation for SOXL, SOXX, and BOXX uses two independent acquisition paths: Twelve Data's authenticated daily endpoint and the project's existing Yahoo Finance chart download channel. To keep the adjustment basis verifiable, both paths request split-adjusted-only daily bars: Twelve Data sets `adjust=splits`, and Yahoo uses `close` without dividend total-return adjustment. Each produces an independent, immutable daily snapshot before `quant_platform_kit` evaluates multi-source agreement.

Diagnostics emit only status, hashes, and bounded trading-day coverage evidence (expected/actual counts and at most three missing or extra date samples), plus price-free divergence metadata (first divergent trading date, max relative delta, and affected field names). They never emit OHLCV rows, upstream payloads, request URLs, or secrets. If either source is unavailable, trading-day coverage differs, or OHLCV disagrees, the result is non-publishable rather than falling back to a single source.

This is not automatic failover or a free-data fallback mechanism: if either source is unavailable, adjustment basis differs, coverage disagrees, or any field declared for consumption by the candidate exceeds tolerance, the outcome is `NOT_VERIFIED`, blocking P1 writes, backtests, strategy publication, and broker positions. The default policy requires full OHLCV agreement; this free SOXL candidate declares consumption of split-adjusted `close` only, so its strategy-bound validation policy checks `close` alone without comparing unconsumed OHLC or volume. That field scope is bound into the policy hash and cannot be relaxed at runtime. Split-adjusted bars also differ from SOXL v3's total-return adjusted series; even when verified, they serve only as a P1 input for a new candidate that must complete P3 backtesting before review.

Twelve Data treats the end date as exclusive; the adapter extends the request end by one calendar day to cover the target completed session while still rejecting any returned row after `date_cutoff`. Extending the request boundary never admits an incomplete trading day into the snapshot.

Yahoo Finance requires no new key, but its public chart channel is not a production trading market-data API and is used here solely for independent cross-checking. Twelve Data's Basic plan provides a daily rate-limited quota, and its key lives only in the `TWELVE_DATA_API_KEY` secret under the non-live `market-data-nonlive` environment, never in code, logs, or chat.

Once the secret is configured, run **Multi-source Daily Assurance Diagnostic** manually from Actions for the latest closed XNYS trading day. The workflow outputs only source status, reason codes, and hashes; it retains no raw market bars, uploads no artifacts, uses no cloud permissions, and touches no broker.

Wiring this evidence into formal P1 requires separate review and only becomes eligible after all three symbols return `MULTISOURCE_DAILY_ASSURANCE_VERIFIED`.
