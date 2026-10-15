# Paired price-basis replay comparison (draft)

This is a cloud-only **aggregation stage**, not a strategy replay generator.
It compares two existing, frozen replay ledgers produced by the actual strategy,
portfolio, plugin and fill/cost implementation. Missing ledgers cannot be
replaced by synthetic prices, current holdings, or target-weight-only returns.
This draft has no real-market comparison results and grants no deployment authority.

## Price basis

The explicit `price_basis` values are `raw_unadjusted`, `split_adjusted`,
`split_dividend_adjusted`, and `total_return_index`. There is **no implicit default**
or fallback. Provider `Close` and `Adj Close` are column names, not universal basis
definitions. In particular, yfinance `Close` with `auto_adjust=False` is the
split-adjusted series used by the IBKR #602 path; it is not an unadjusted tape.
These draft names should be reconciled with the shared market-data standard before adoption.

## Workflow and access

`price-basis-comparison.yml` is manually dispatched on the repository default
branch and uses the existing `market-data-nonlive` environment and research
identity variables. Permissions match the existing research workflows:
`contents: read` and `id-token: write`. No secret inputs, new service identities,
roles, provider API calls, cloud writes, broker clients, or order APIs are added.
The auth client supplies a masked short-lived access token directly to the storage
client through the step environment; credential files are disabled. Stop on an
access denial; do not change permissions. Draft branches cannot run the real-data job.

Inputs have no private defaults: `input_root` must identify an already approved
`gs://.../research/...` prefix. `manifest_uri`, `manifest_generation`, and
`manifest_sha256` pin one existing manifest. Every replay bundle is similarly
generation- and digest-pinned inside that root. Each read is bounded to 64 MiB,
without retries. Only `price-basis-aggregate.json` is uploaded, with seven-day
retention. Input rows, prices, dates, symbols, holdings, order intents, private
paths and provider exception text are never written to logs or uploaded.
The manifest supports at most 32 public aliases (`target-1` etc.).

## Frozen input contract

The root manifest is `{"schema_version":1,"targets":[...]}`. Each target has an
`alias`, `metadata`, and `replay_bundle` reference (`uri`, `generation`, `sha256`).
The source of effective production configuration must be a non-secret cloud
projection; this workflow does not read secret stores or inspect credentials.

`metadata` requires:

- `schema_version: 1`, `comparison_kind: signal_price_basis`, `as_of`, actual
  `inception`, and `calendar` (an explicit exchange-calendars calendar such as `XNYS`).
  Calendar package version is fixed to 4.13.2; there is no calendar default.
- `baseline` and `candidate`: explicit `price_basis`, 40-character
  `platform_revision` and `strategy_revision`, and `effective_config_sha256`.
  Code revisions and effective-config digests must match between arms. The basis
  selector is supplied separately from that config so the experiment changes one
  input basis only. Compare dependency/valuation changes in separate experiments;
  this stage does not claim to isolate several PRs from one full-release delta.
- `shared`: SHA256 values for `data_manifest_sha256`, `calendar_sha256`,
  `replay_code_sha256`, `cost_model_sha256`, `plugin_binding_sha256`,
  `initial_state_sha256`, and `external_flows_sha256`. The producer must validate
  these identities against its actual installed packages and cloud objects.
- `timing`: `next_open`, `next_session`, or `completed_session_before_fill`.
  Signal warmup, availability times, corporate-action conventions and causal fill
  times belong in the frozen producer evidence. All OHLC fields must use a
  coherent basis; close-only total-return stress cannot stand in for OHLC fills.
- `limits: {"total": ..., "symbols": {...}}`: the unchanged existing research
  binding, never a newly optimized cap. This comparator validates the supplied
  limits but does not attest that they equal a private production risk policy.
- Positive `unitized_twr: true`, `cost_feedback: self_financing`, and
  `turnover_definition: security_one_way_notional_over_prefill_nav`.
- `cost_components`: exactly `commission`, `spread_slippage`, `impact`,
  `borrow_margin`, `cash_yield`, `options`, `fund_expenses`; each contains
  `status: modeled|not_applicable` and nonempty `reason_or_version`. Actual
  schedules, whole-share/minimum-order rules, option chains and joint accounting
  are required upstream where applicable. A zero-bps overlay is not UBPS evidence.
- `corporate_action_coverage_complete: true`, `thresholds_applicable: true|false`,
  ascending nonnegative `size_bucket_edges` and `threshold_bucket_edges`.
  Fix bucket boundaries before viewing results; bucket upper bounds are inclusive.

Each `replay_bundle` contains `rows` and `corporate_actions`. A row's baseline
excerpt is shown below; `candidate` is also required and uses the same object
schema, populated from its independently replayed state:

```json
{
  "session": "2025-01-02",
  "baseline": {
    "signals": {"synthetic_signal": 1.0},
    "weights": {"SYNTH": 0.5},
    "orders": {"SYNTH": 0.01},
    "threshold_distances": {"entry": 0.002},
    "gross_return": 0.001,
    "net_return_1x": 0.0009,
    "net_return_2x": 0.0008,
    "turnover": 0.01
  }
}
```

The example numbers and instrument are synthetic. `weights` are security weights,
excluding idle cash; cash yields still belong in the producer's return accounting.
`orders` are hypothetical
signed security notionals divided by pre-fill NAV, after the actual execution
decision and guards; never submitted orders. Empty means no trade. Whole shares,
minimum notional and no-trade filters must already be applied by the producer.
`turnover` is actual simulated one-way security fill notional divided by pre-fill
NAV, including entry and exit; it is not half-L1 target turnover.
`threshold_distances` contains signed dimensionless relative distances to relevant
decision boundaries, with the normalization defined in the producer evidence.
Both arms must provide thresholds on every day when applicable.
`corporate_actions` records have `session` (the exchange ex-session) and
`kind: dividend|split`; include events just outside reporting windows whose
five-session neighborhoods intersect a window. Verify the completeness assertion
against the pinned actions archive, including split/dividend corrections.

The producer must provide every calendar session from the earlier of actual
inception and two years before `as_of`, without duplicates or gaps. For each
reporting window, the inherited state and starting NAV must be documented.
Missing historical plugins, portfolio marks, option chains, cost schedules,
corporate actions or warmup leave the research **not comparable**; this aggregator
does not manufacture those inputs or infer inception from a deployment revision.

## Outputs and interpretation

For trailing two years and since inception, the report contains:

- Signal difference days; target-weight p50/p95/max for daily maximum absolute
  delta, daily L1 delta, and all asset-day absolute deltas (union of nonzero/explicit
  weight keys, not an invented full universe). Residual cash-weight deltas are
  reported separately. All weights use fractions of NAV.
- Decision difference days, trade/no-trade differences, direction differences
  (including entry/exit), and size-bucket differences. These counts can overlap.
- Dividend and split neighborhoods of ±5 **trading sessions**, deduplicated by
  session, plus baseline nearest-threshold buckets. Thresholds not applicable are
  counted separately; empty groups return null quantiles, never zero estimates.
- Calendar-day CAGR, signed maximum drawdown including initial NAV, cumulative
  one-way turnover, and candidate-minus-baseline differences for gross, 1× and
  2× cost paths. Returns must be unitized/TWR so external deposits are not profit.

The output status is `AGGREGATED_REPLAY_INPUTS_NOT_EXECUTION_CERTIFICATION`.
Digest/schema checks demonstrate input integrity, not the truth of producer
declarations or real production equivalence. Review producer evidence separately.
The workflow cannot run from a Draft PR while the default-branch gate is preserved;
this is an explicit handoff limitation, not permission to merge or bypass that gate.

## Synthetic verification

```bash
python -m unittest discover -s tests -p test_compare_price_basis.py -v
actionlint .github/workflows/price-basis-comparison.yml
```

These commands use fabricated ledgers only and perform no market-data acquisition.
