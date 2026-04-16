# Crisis Response Live Promotion Spec

This document is the handoff contract for future AI / Codex work on the crisis
response research. It defines how the research candidate can move toward live
use without silently changing V1 semantics or letting AI control capital before
there is enough production evidence.

## Current Position

The current crisis-response work is a research candidate, not an automatic live
trading system.

- V1 remains frozen in `docs/crisis-response-v1.md`.
- V2 context features remain research-only until promoted through this spec.
- A future live plugin must start in shadow mode: write signals and audit logs,
  but do not place orders and do not change live allocations.
- AI may audit, explain, and challenge a route. AI must not directly submit
  trades or bypass risk gates.
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
| Policy shocks | TACO or `no_action`, small sleeve only |
| TACO sleeve | 5% research overlay |
| Live behavior | Not enabled |

Do not change this candidate while building the shadow plugin unless the user
explicitly asks for a new research experiment.

## Phase Ladder

| Phase | Name | Allowed behavior | AI role | Capital impact |
| --- | --- | --- | --- | --- |
| 0 | Research | Backtests and audit reports only | Analyze historical logs | None |
| 1 | Shadow plugin | Produce daily signal and evidence files | Replay and audit logs | None |
| 2 | AI audit replay | Review recent shadow signals in batches | Explain route quality and flag errors | None |
| 3 | Advisory | Produce a recommendation requiring human confirmation | Second-opinion auditor | Manual only |
| 4 | Limited live | Execute only inside small, explicit risk budget | Audit and kill-switch reviewer | Bounded |
| 5 | Full automation | Not approved by this spec | Still audited and kill-switched | Future decision |

The next approved engineering target is Phase 1 plus Phase 2 preparation.

## Phase 1 Shadow Plugin Requirements

A shadow plugin must be production-facing but non-trading. It can read current
market data, external context, and event context. It must write outputs only.

Required properties:

- No order placement.
- No broker API write calls.
- No live allocation mutation.
- No implicit promotion to advisory or live mode.
- Idempotent daily output for the same `as_of` date and inputs.
- Clear data-freshness and data-quality fields.
- Explicit kill switch when evidence is missing, stale, contradictory, or
  unavailable.

Suggested output directory shape:

```text
data/output/crisis_response_shadow/
  latest_signal.json
  signals/YYYY-MM-DD.json
  signals/YYYY-MM-DD.csv
  audit/YYYY-MM-DD_evidence.csv
  audit/YYYY-MM-DD_ai_replay_prompt.md
```

## Shadow Signal Schema

The daily JSON must be easy for another AI agent to read without inspecting
code. Use stable field names.

Required top-level fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `as_of` | string | Signal date in `YYYY-MM-DD` |
| `mode` | string | Must be `shadow` for Phase 1 |
| `schema_version` | string | Start with `crisis_response_shadow.v1` |
| `canonical_route` | string | One of `true_crisis`, `taco_fake_crisis`, `no_action` |
| `watch_label` | string | Optional context label such as `systemic_stress_watch` or `rate_bear` |
| `suggested_action` | string | One of `defend`, `small_taco`, `watch_only`, `no_action`, `blocked` |
| `risk_multiplier_suggestion` | number or null | Recommendation only; not executed in shadow mode |
| `would_trade_if_enabled` | boolean | Whether advisory/live mode would ask for action |
| `price_scanner_active` | boolean | Confirmed price-crisis scanner state |
| `bubble_fragility_active` | boolean | Early valuation/fragility scanner state |
| `kill_switch_active` | boolean | True when the signal is blocked |
| `kill_switch_reason` | string | Why output is blocked or downgraded |
| `data_freshness` | object | Freshness by input family |
| `evidence` | object | Context evidence |
| `audit_summary` | object | Proposer/auditor/final route summary |

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

## AI Audit Replay

AI replay is a batch review of shadow logs. It should happen after logs exist;
it should not be used to invent missing evidence.

Recommended cadence:

- Weekly while shadow signals are quiet.
- Daily during a market drawdown or policy shock.
- Ad hoc after any `would_trade_if_enabled=true` day.

The AI replay prompt should include:

- The latest shadow JSON.
- The previous 5 to 20 shadow JSONs.
- Current audit reports, especially false-positive and false-negative files.
- The historical taxonomy from `docs/crisis-response-research-roadmap.md`.
- The rule that AI cannot approve live trading by itself.

AI replay must answer:

1. Is the route historically coherent?
2. Is the evidence sufficient and point-in-time?
3. Is there a likely false-positive true-crisis risk?
4. Is there a likely false-negative true-crisis risk?
5. Is this closer to 2000, 2008, 2020, 2022, 2011, policy/TACO, or normal?
6. Should the system stay shadow, move to advisory for this case, or remain
   blocked?

## Audit Windows

The shadow and replay process must keep these windows visible:

| Window | Expected behavior |
| --- | --- |
| 2000-2002 dot-com bust | `true_crisis` after price confirmation, with valuation/fragility evidence |
| 2007-2009 GFC | `true_crisis` after price confirmation, with financial/credit evidence |
| 2011 debt / euro stress | `systemic_stress_watch`; no action unless price scanner confirms |
| 2020 COVID | Usually `no_action` because exogenous shock and policy rescue dominate |
| 2022 rate bear | `rate_bear` / `no_action`; do not treat as true crisis without financial stress |
| 2018-2019 trade war | TACO or `no_action`, not main-book defense |
| 2025+ policy shocks | TACO or `no_action`, with small-sleeve discipline |

Add `2011_debt_euro_stress` to audit reports before using shadow logs for
promotion decisions.

## Promotion Gates

Phase 1 shadow can start after:

- Local and CI tests pass.
- The plugin writes only logs and has tests proving no order path exists.
- Missing/stale data activates a kill switch.
- A daily output file can be replayed by AI without code inspection.

Phase 2 AI replay can start after:

- At least 20 shadow trading days exist, or at least one high-volatility event
  has produced complete logs.
- The replay prompt is stored with the evidence files.
- AI replay outputs are stored separately from raw signals.

Phase 3 advisory requires:

- At least 30 to 60 trading days of shadow logs.
- Zero unexplained false-positive `true_crisis` cases.
- Any `would_trade_if_enabled=true` days reviewed by AI and a human.
- Authorized point-in-time external valuation and credit context, or a kill
  switch that blocks decisions dependent on missing data.
- Written user approval to move from shadow to advisory.

Phase 4 limited live requires:

- Explicit user approval.
- Explicit maximum risk budget.
- Explicit maximum turnover and cooldown.
- Human override path.
- Daily audit logs.
- Automatic fallback to `no_action` or `blocked` on data errors.

Phase 5 full automation is not approved here.

## Kill Switch Rules

The shadow plugin must set `kill_switch_active=true` and `suggested_action=blocked`
when any of these occur:

- Price data is missing or stale.
- External valuation data needed for a valuation route is missing, stale, or not
  licensed for production use.
- Financial or credit data needed for a financial-crisis route is stale.
- Event classification is missing during a policy shock.
- AI replay contradicts the deterministic route without enough evidence.
- The route depends on a newly added feature that has not passed historical
  audit-effectiveness checks.
- The code is running outside approved shadow/advisory/live mode.

## Rules For Future AI Agents

Future AI agents must follow these constraints:

- Do not change V1 parameters unless the user explicitly asks.
- Do not promote V2 research into live allocation in the same change that adds a
  new feature.
- Do not optimize thresholds against one crisis window without checking 2015+,
  2020, 2022, 2018-2019, and 2025+ controls.
- Do not treat high backtest CAGR as proof that AI audit is effective.
- Do not add broker write calls to the shadow plugin.
- Do not store proxy credentials or API keys in the repository.
- Do not use provisional external data for production decisions.
- Always preserve audit CSV / JSON outputs when changing route logic.

Recommended next tasks:

1. Add `2011_debt_euro_stress` to the audit-effectiveness report.
2. Implement `crisis_response_shadow_plugin` as log-only.
3. Add tests proving the shadow plugin cannot place orders or mutate live
   allocation.
4. Add AI replay prompt generation from the shadow JSON.
5. Run shadow-only for 30 to 60 trading days before advisory mode.
