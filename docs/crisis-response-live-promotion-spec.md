# Crisis Response Notification-Only Spec

This document is the handoff contract for future Crisis Response work. It
defines how the deterministic research candidate remains observable without
silently changing V1 semantics or letting unreviewed logic control capital.

## Current Position

The current crisis-response work is a notification-only shadow plugin, not an
automatic live trading system.

- V1 remains frozen in `docs/crisis-response-v1.md`.
- V2 context features remain research-only unless explicitly promoted into the
  notification artifact.
- The plugin writes signals and audit logs, but does not place orders, maintain a
  paper ledger, or change live allocations.
- `shadow` is the only supported plugin mode. `paper`, `advisory`, and `live`
  plugin modes are retired.
- The route must come from deterministic, testable rules. Model-based review is
  not part of the trading path.
- Every promotion step must preserve the 1999-to-date, post-2010, post-2015,
  2020, 2022, 2018-2019, and 2025+ acceptance checks.

## Candidate Snapshot

The current research candidate is:

| Component | Candidate setting |
| --- | --- |
| Main backtest window | 1999-03-10 to present |
| Main route pack | V2 crisis context pack |
| 2000 dot-com defense | External valuation fragility plus severe valuation-bubble reduction |
| 2008 GFC defense | Financial / credit systemic context, including joint financial plus credit confirmation |
| 2020 COVID | Exogenous / policy-rescue context defaults to `no_action` |
| 2022 rate bear | Rate-bear context defaults to `no_action` unless financial-system stress appears |
| Policy shocks | `no_action` / watch-only inside the crisis plugin |
| TACO sleeve | Separate research only; no MAGS runtime mount |
| Live behavior | Not enabled; notification-only shadow mode |

Do not change this candidate while building the shadow plugin unless the user
explicitly asks for a new research experiment.

The notification contract is intentionally split. `crisis_response_shadow`
is a TQQQ black-swan defense plugin; it can recommend moving the main book to
cash or a money-market / Treasury-bill parking sleeve as manual review context
only after a true-crisis route. It must not recommend buying event rebounds.
Reversible policy, tariff, or geopolitical rebound research belongs outside
`crisis_response_shadow`. MAGS-style TACO usage remains research-only. The
preferred promoted direction is a separate TQQQ TACO notification artifact that
can be mounted for manual review without carrying allocation instructions.

The TACO notification artifact may emit an `event_rebound_break_bear` context
flag for high-confidence geopolitical de-escalation events. That flag does not
authorize this repository to trade and does not imply position size; it only
makes the manual-review reason visible in logs and notifications.

## Phase Ladder

| Phase | Name | Allowed behavior | Review role | Capital impact |
| --- | --- | --- | --- | --- |
| 0 | Research | Backtests and audit reports only | Analyze deterministic reports | None |
| 1 | Shadow plugin | Produce daily signal and evidence files | Inspect logs and data freshness | None |
| 2 | Evidence review | Review recent shadow signals in batches | Check route quality and data issues | None |

The approved engineering target is Phase 1 plus Phase 2 preparation. Automatic
plugin execution is outside this spec.

## Phase 1 Shadow Plugin Requirements

A shadow plugin must be production-facing but non-trading. It can read current
market data, external context, and event context. It must write outputs only.

Required properties:

- No order placement.
- No broker API write calls.
- No live allocation mutation.
- No implicit promotion to paper, advisory, or live plugin mode.
- Idempotent daily output for the same `as_of` date and inputs.
- Clear data-freshness and data-quality fields.
- Explicit kill switch when evidence is missing, stale, contradictory, or
  unavailable.

Runtime integration:

- Schedule the shadow builder on the same daily cadence as the TQQQ strategy
  artifact pipeline after prices and external context are refreshed.
- Keep the log namespace separate as `crisis_response_shadow`; it is an
  observation stream for defense-only crisis evidence, not a TACO trading
  stream.
- Downstream notification systems may include the latest shadow route in the
  TQQQ status message, but the notification must label it as `shadow_only`.
- This repository must not add Telegram, broker, or order-routing writes for
  the shadow plugin. It only writes artifacts for downstream readers.
- Prefer the platform-level plugin runner for deployment. The strategy should
  not hard-code whether Crisis Response, or any future plugin, is active.

Suggested output directory shape:

```text
data/output/crisis_response_shadow/
  latest_signal.json
  signals/YYYY-MM-DD.json
  signals/YYYY-MM-DD.csv
  audit/YYYY-MM-DD_evidence.csv
```

Runner configuration schema:

- Use `docs/examples/strategy_plugins.example.toml` as the checked-in example.
- Keep real runtime TOML with deployment or platform configuration.
- Keep top-level `default_mode = "shadow"`.
- Omit per-plugin `mode` unless a legacy config needs to spell out `shadow`.
- Keep input paths under `[strategy_plugins.inputs]`, output paths under
  `[strategy_plugins.outputs]`, and strategy/plugin/enabled at the plugin mount
  level.

Run it as a separate sidecar job:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/run_strategy_plugins.py --config /path/to/strategy_plugins.toml
```

The runner is a whitelist. A plugin can only run if the code explicitly
registers it. A plugin is mounted to a strategy through each
`[[strategy_plugins]]` entry; the strategy core must remain independent of the
runner and must not import plugin code. Plugins are strategy-limited in the
runner: a mount is rejected unless the plugin is explicitly declared compatible
with that strategy. This keeps `crisis_response_shadow` scoped to the TQQQ
compatibility mount, sends SOXL broad crisis/macro context through the general
`market_regime_notification` target, and keeps `taco_rebound_shadow` scoped to
a TQQQ-only manual-review notification artifact. It must not be wired into MAGS
rebound-budget inputs or any broker-facing allocation path.

The runner accepts only `mode = "shadow"` and writes that mode into each plugin
artifact. Downstream platform adapters must read the artifact as notification
context and must not run a second plugin mode-selection layer.

This repository remains artifact-only: it does not place orders, call broker
write APIs, maintain paper ledgers, or mutate live allocations. The payload
therefore keeps two concepts separate:

- Platform behavior fields, fixed for notification-only mode, such as
  `broker_order_allowed=false` and `live_allocation_mutation_allowed=false`.
- Repository capability fields, always false here, such as
  `repository_broker_write_allowed` and
  `repository_allocation_mutation_allowed`.

Mode meanings:

| Mode | Meaning | Capital impact |
| --- | --- | --- |
| `shadow` | Write signal, evidence, freshness, and optional notification context only | None |

## Shadow Signal Schema

The daily JSON must be easy for an operator or downstream process to read
without inspecting code. Use stable field names.

Required top-level fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `as_of` | string | Signal date in `YYYY-MM-DD` |
| `strategy` | string | Strategy profile this plugin artifact is mounted to |
| `plugin` | string | Plugin name, for example `crisis_response_shadow` |
| `mode` | string | Always `shadow` |
| `configured_mode` | string | Always `shadow` |
| `effective_mode` | string | Always `shadow` |
| `schema_version` | string | Start with `crisis_response_shadow.v1` |
| `canonical_route` | string | One of `true_crisis`, `no_action` |
| `watch_label` | string | Optional context label such as `systemic_stress_watch` or `rate_bear` |
| `suggested_action` | string | One of `defend`, `watch_only`, `no_action`, `blocked` |
| `risk_multiplier_suggestion` | number or null | Recommendation only inside this repository |
| `would_trade_if_enabled` | boolean | Whether the shadow signal would warrant manual review if enabled operationally |
| `price_scanner_active` | boolean | Confirmed price-crisis scanner state |
| `bubble_fragility_active` | boolean | Early valuation/fragility scanner state |
| `kill_switch_active` | boolean | True when the signal is blocked |
| `kill_switch_reason` | string | Why output is blocked or downgraded |
| `data_freshness` | object | Freshness by input family |
| `evidence` | object | Context evidence |
| `audit_summary` | object | Proposer/auditor/final route summary |
| `execution_controls` | object | Mode-derived platform behavior flags plus repository capability flags |

Required `evidence` fields:

| Field | Meaning |
| --- | --- |
| `valuation_context` | PE / CAPE / valuation-bubble evidence |
| `breadth_quality_context` | Breadth and earnings-quality evidence |
| `financial_context` | XLF / KRE / bank proxy evidence |
| `credit_context` | HYG / IEF, LQD / IEF, spread proxy evidence |
| `combined_financial_credit_context` | Whether financial and credit stress jointly confirm |
| `rate_context` | Rate, inflation, duration, real-yield evidence |
| `policy_context` | Tariff, trade-war, sanction, policy-shock evidence |
| `exogenous_context` | Pandemic, war, natural disaster, sudden-stop evidence |
| `policy_rescue_context` | Central-bank or fiscal rescue evidence |

Example JSON shape:

```json
{
  "as_of": "2026-04-17",
  "strategy": "tqqq_growth_income",
  "plugin": "crisis_response_shadow",
  "mode": "shadow",
  "schema_version": "crisis_response_shadow.v1",
  "canonical_route": "no_action",
  "watch_label": "rate_bear",
  "suggested_action": "watch_only",
  "risk_multiplier_suggestion": null,
  "would_trade_if_enabled": false,
  "price_scanner_active": true,
  "bubble_fragility_active": false,
  "kill_switch_active": false,
  "kill_switch_reason": "",
  "data_freshness": {
    "prices_as_of": "2026-04-17",
    "external_context_as_of": "2026-04-16",
    "events_as_of": "2026-04-17"
  },
  "evidence": {
    "valuation_context": false,
    "breadth_quality_context": false,
    "financial_context": false,
    "credit_context": false,
    "combined_financial_credit_context": false,
    "rate_context": true,
    "policy_context": false,
    "exogenous_context": false,
    "policy_rescue_context": false
  },
  "audit_summary": {
    "proposer_route": "no_action",
    "auditor_verdict": "approve_watch_only",
    "final_route": "no_action",
    "reason": "Rate-bear evidence is active without financial-system stress."
  }
}
```

## Evidence Review

Evidence review is a batch review of deterministic shadow logs. It should
happen after logs exist; it should not be used to invent missing evidence or
override the deterministic route.

Recommended cadence:

- Weekly while shadow signals are quiet.
- Daily during a market drawdown or policy shock.
- Ad hoc after any `would_trade_if_enabled=true` day.

Evidence review must answer:

1. Is the route historically coherent?
2. Is the evidence sufficient and point-in-time?
3. Is there a likely false-positive true-crisis risk?
4. Is there a likely false-negative true-crisis risk?
5. Is this closer to 2000, 2008, 2020, 2022, 2011, policy/no-action, or normal?
6. Should the notification remain visible, be downgraded, or remain blocked?

## Audit Windows

The shadow and evidence-review process must keep these windows visible:

| Window | Expected behavior |
| --- | --- |
| 2000-2002 dot-com bust | `true_crisis` after price confirmation, with valuation/fragility evidence |
| 2007-2009 GFC | `true_crisis` after price confirmation, with financial/credit evidence |
| 2011 debt / euro stress | `systemic_stress_watch`; no action unless price scanner confirms |
| 2020 COVID | Usually `no_action` because exogenous shock and policy rescue dominate |
| 2022 rate bear | `rate_bear` / `no_action`; do not treat as true crisis without financial stress |
| 2018-2019 trade war | Crisis plugin stays `no_action` / watch-only; separate TACO plugin may notify manual review context |
| 2025+ policy shocks | Crisis plugin stays `no_action` / watch-only unless systemic stress appears |

`2011_debt_euro_stress` is included in the audit reports as a
`systemic_stress_watch` control.

## Promotion Gates

Phase 1 shadow can start after:

- Local and CI tests pass.
- The plugin writes only logs and has tests proving no order path exists.
- Missing/stale data activates a kill switch.
- A daily output file can be inspected without code inspection.

Phase 2 evidence review can start after:

- At least 20 shadow trading days exist, or at least one high-volatility event
  has produced complete logs.
- Evidence files are complete and data freshness is visible.
- Review notes, if any, are stored separately from raw signals.
- Review cannot override the deterministic route.

No later plugin execution phases are approved here. Any future request for
automatic action must be designed as a new contract, not a mode flip in this
plugin.

## Kill Switch Rules

The shadow plugin must set `kill_switch_active=true` and `suggested_action=blocked`
when any of these occur:

- Price data is missing or stale.
- External valuation data needed for a valuation route is missing, stale, or not
  licensed for production use.
- Financial or credit data needed for a financial-crisis route is stale.
- Event classification required by a separate TACO rebound artifact is missing.
- The route depends on a newly added feature that has not passed historical
  audit-effectiveness checks.
- The code is running outside approved `shadow` mode.

## Rules For Future Agents

Future agents must follow these constraints:

- Do not change V1 parameters unless the user explicitly asks.
- Do not add a TACO sleeve back into `crisis_response_shadow`.
- Do not mount `taco_rebound_shadow` to MAGS runtime configs or any allocation
  input; keep it notification-only unless a future validation explicitly
  promotes a separate broker-facing mechanism.
- Do not promote V2 research into allocation changes in the same change that
  adds a new feature.
- Do not optimize thresholds against one crisis window without checking 2015+,
  2020, 2022, 2018-2019, and 2025+ controls.
- Do not treat high backtest CAGR as proof that an automatic plugin is ready.
- Do not add broker write calls to the shadow plugin.
- Do not store proxy credentials or API keys in the repository.
- Do not use provisional external data for production decisions.
- Always preserve audit CSV / JSON outputs when changing route logic.

Recommended next tasks:

1. Run `crisis_response_shadow_plugin` daily with current prices and authorized
   external context.
2. Run Phase 2 evidence review after at least 20 shadow trading days, or
   immediately after a high-volatility `would_trade_if_enabled=true` day with
   complete logs.
3. Keep review notes separate from raw shadow signals.
4. Keep the plugin shadow-only unless a new, explicitly approved contract
   replaces this notification-only design.
