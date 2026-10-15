# Alpaca free-plan request windows

The manual Alpaca access diagnostic uses `scripts/alpaca_feed_windows.py` to plan
explicit, disjoint inclusive request intervals. The helper is reusable by research
acquisition callers; it does not change any scheduled snapshot or trading consumer.
Frozen archive producers retain their existing fixed historical contracts.

| Environment / GitHub variable | Default | Meaning |
| --- | --- | --- |
| `ALPACA_SIP_MIN_DELAY_MINUTES` | `16` | Historical SIP end is clamped to `now - delay`. Finite values of at least 15 minutes are accepted. |
| `ALPACA_FEED_HISTORICAL` | `sip` | Historical segment feed; accepts `sip` or explicitly selected `iex`. |
| `ALPACA_FEED_RECENT` | `iex` | Only the recent tail uses IEX; `none` explicitly omits that tail. SIP is rejected for this interval. |

A date cutoff means the end of that New York calendar day, capped at the captured
UTC collection time. Historical requests already outside the delay keep their
requested end. The recent segment begins one microsecond after the historical
inclusive end. Each output segment records its feed, bounds and HTTP status;
segments are never concatenated into an unlabeled price series. When integrating
this planner into a bar producer, preserve separate segment objects: an intrabar
boundary can yield overlapping daily bar timestamps, whose OHLCV must not be
merged across feeds or silently treated as completed daily bars.

The diagnostic requests one result per segment, at most 12 requests (six symbols,
two segments), with 30-second timeouts and no retries. It never reads successful
response bodies. The existing non-live environment consumes its protected provider
credentials. Redirects are disabled. Any HTTP or transport failure stops the run;
there is no SIP-to-IEX retry after a 401/403 or any other error. Status plus a bounded,
sanitized provider `message` go only into `diagnostic.json`, retained as a diagnostic
Actions artifact for 14 days. Headers, arbitrary envelopes, authentication, URLs
and successful market-data bodies are never included. The console prints only the
overall status. `adjustment=all` is labelled `price_basis=split_dividend_adjusted`.

These defaults follow the [Alpaca market-data FAQ](https://docs.alpaca.markets/us/docs/market-data-faq):
free-plan SIP historical queries require an end at least 15 minutes old.
The [bars reference](https://docs.alpaca.markets/us/reference/stockbars) declares
inclusive bounds. IEX is a different feed; successful access does not establish
price parity, historical completeness, or UBPS qualification.
