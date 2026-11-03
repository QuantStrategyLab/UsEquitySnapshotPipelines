# Frozen benchmark input readback

`scripts.benchmark_reference_readback.prepare_reference` validates one explicitly
selected window from an existing `research_input_inventory.v1` Alpaca archive.
The caller supplies an approved exact-object reader and retains the returned
private daily payload in its protected research environment. Only the separate
diagnostic summary may leave that environment. This seam has no default symbol,
window, percentage, account or selected policy, and no credential or storage writer.

The input inventory and every page require exact generation, byte count and SHA-256.
The caller also supplies `available_at_utc` from the exact inventory generation's
native object-creation metadata. The manifest's `created_at` is the producer's run
start, so it must not be relabelled as data availability. Pages must belong to the
selected symbol under the inventory's original dataset prefix, have complete
ordered pagination, and declare acquisition no later than availability.
Availability must precede the declared
freeze, which must not exceed the caller's declared observation time. Calendar
versions must match the installed locked version. The selected
daily sessions and preceding baseline session must be present exactly, with no
duplicates, interpolation or date intersections. QPK computes maximum drawdown
including initial wealth; the baseline close is used for the first return and is
not counted as an extra return observation.

Weekend and holiday end dates remain valid declared cutoffs. Calendar construction
includes a short trailing calendar buffer so its range parser can handle such
dates; the provider request, selected sessions and reported window keep the exact
original cutoff. No later market observations are fetched or added.

The result describes ratios of provider-adjusted closes. It is deliberately
labelled `PROVIDER_ADJUSTED_CLOSE_RATIO`: the archive does not prove independent
total-return reconstruction, account execution costs, taxes or an actual allocated
account wealth path. `account_costs_included`, `net_account_path_verified`,
`risk_policy_qualified`, `policy_selected`, `settings_applied` and
`execution_authorized` remain false. Historical point-in-time availability is not
asserted. A historical path acquired now may inform a future policy once separately
qualified; it must not be treated as information known at the historical decision.

This readback does not fetch new provider data, replay a strategy, select a better
window, issue an account policy or register a production consumer. Exact byte
verification proves that the declared objects match; it does not certify the
provider's financial accuracy or license rights. Those retain their original
source and deployment-specific evidence.
