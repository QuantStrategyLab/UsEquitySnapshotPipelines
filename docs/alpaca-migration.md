# Alpaca acquisition migration

Public market-data acquisition has moved to
[QuantPlatformKit](https://github.com/QuantStrategyLab/QuantPlatformKit).
The `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` secrets were removed from
this repository's `market-data-nonlive` environment. The same names now belong
to QuantPlatformKit repository Actions secrets; GitHub does not share them with
this repository. Do not recreate them here or add a fallback credential source.

The 13 legacy acquisition workflows now emit `ALPACA_ACQUISITION_MOVED` and
fail before acquisition. Their acquisition jobs and dependent replay/publication
jobs are explicitly skipped. The RS-04 archive workflow retains its
metadata-only `inventory` mode; `regenerate` stops before cloud authentication.
The notice job has no environment, checkout, secrets, or token permissions.

The TQQQ and SOXL daily schedules remain as visible migration notices, so their
runs fail and the schedule watchdog continues reporting unavailable research.
They do not publish new control-plane snapshots. Existing downstream data can
therefore become stale. Neither these notices nor a green migration PR check
means a replacement daily pipeline has run successfully.

Other workflows using `market-data-nonlive` for non-Alpaca inputs, private
archive readback, or offline replay remain unchanged. The environment itself
was not deleted. No new environment, secret, remote dispatch, or automatic
cross-repository handoff is introduced by this migration.

The old acquisition scripts and disabled job bodies remain as legacy adapters
for synthetic tests, frozen input contracts, and archive compatibility. Their
presence is not an instruction to run them or refill credentials. This change
blocks the GitHub Actions entry points; it does not remove every Python HTTP
adapter. Local plan-only helpers and archive consumers remain usable under
their existing constraints. New acquisition belongs in QuantPlatformKit;
consuming its outputs here still requires the existing provenance, licence,
input-contract, and authorization checks. A working replacement acquisition
or downstream handoff has not been verified by this cleanup.
