# Crisis Response AI Replay

This document defines the optional AI replay layer for the Crisis Response
shadow plugin. It is a review layer only. It must not place orders, mutate live
allocation, or promote the strategy to advisory/live mode.

## What It Does

`crisis_response_ai_replay` reads shadow artifacts from
`data/output/crisis_response_shadow/`, builds an AI review prompt, and writes
review artifacts under the shadow `audit/` directory.

Inputs:

- `latest_signal.json`
- recent `signals/YYYY-MM-DD.json` files
- latest `audit/YYYY-MM-DD_evidence.csv`, when present

Outputs:

- `audit/YYYY-MM-DD_ai_replay_gate.json`
- `audit/YYYY-MM-DD_ai_replay_request.md`
- `audit/YYYY-MM-DD_ai_review.json`
- `audit/YYYY-MM-DD_ai_review.md`

## Gates

The AI replay runner can be scheduled immediately, but it is gated by the
number of shadow signal days:

- AI replay gate: 20 shadow trading days. Before this, write
  `waiting_for_min_shadow_days`; do not call an AI API.
- Advisory review gate: 60 shadow trading days. After this, write
  `advisory_review_eligible=true`; still do not enable advisory/live.

The 20-day gate is for starting AI review of the shadow logs. It has no capital
impact.

The 60-day gate is only a reminder that there is enough shadow history to ask a
human whether advisory mode should be considered. It is not permission to
change allocations. The payload always keeps:

```json
{
  "advisory_auto_enable_allowed": false,
  "execution_controls": {
    "capital_impact": "none",
    "broker_order_allowed": false,
    "live_allocation_mutation_allowed": false,
    "advisory_auto_enable_allowed": false
  }
}
```

## API Key Configuration

Do not store API keys in the repository.

Set one of these environment variables on the runner host or secret store:

```bash
export OPENAI_API_KEY="..."
export OPENAI_MODEL="gpt-5.4-mini"
```

or:

```bash
export ANTHROPIC_API_KEY="..."
export ANTHROPIC_MODEL="claude-sonnet-4-6"
```

If the gate is open but the key is missing, the runner writes
`blocked_missing_api_key` and exits normally. This keeps daily schedules from
failing while the key is not configured.

## Commands

Dry run, useful before keys are configured:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/replay_crisis_response_shadow_ai.py \
  --shadow-dir data/output/crisis_response_shadow \
  --provider none \
  --dry-run
```

OpenAI replay after the 20-day gate:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/replay_crisis_response_shadow_ai.py \
  --shadow-dir data/output/crisis_response_shadow \
  --provider openai \
  --model "${OPENAI_MODEL:-gpt-5.4-mini}"
```

Anthropic replay after the 20-day gate:

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/replay_crisis_response_shadow_ai.py \
  --shadow-dir data/output/crisis_response_shadow \
  --provider anthropic \
  --model "${ANTHROPIC_MODEL:-claude-sonnet-4-6}"
```

## Scheduling

It is safe to schedule this now after the daily shadow signal builder. The
runner will wait until enough shadow signals exist.

Example VPS cron shape:

```cron
30 21 * * 1-5 cd /home/ubuntu/Projects/UsEquitySnapshotPipelines && \
  PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
  .venv/bin/python scripts/replay_crisis_response_shadow_ai.py \
  --shadow-dir data/output/crisis_response_shadow \
  --provider openai >> data/output/crisis_response_shadow/ai_replay.log 2>&1
```

Keep the API key in the host environment, systemd environment file, or secret
manager. Do not write it into the cron command if the command is committed or
shared.

## Future Promotion

AI replay can recommend `keep_shadow`, `human_review_advisory_candidate`, or
`blocked`. It cannot approve live trading.

A later advisory PR must still require:

- 30 to 60 shadow trading days, with 60 preferred for a stable read.
- Zero unexplained false-positive `true_crisis` cases.
- Human review of any `would_trade_if_enabled=true` day.
- Written user approval to move from shadow to advisory.
