English | [简体中文](batch-a-v2-alpaca-sip-inputs.zh-CN.md)

# Batch A v2 Alpaca SIP Acquisition Entrypoint

Audit §9.15.27 D1. This entrypoint serves `qsl.research.price_snapshot.v2` only and shares no state or compatibility layer with Global ETF learning, R3, Yahoo, `private-root`, or the v1 schema.

## Fixed scope

| Item | Specification |
|---|---|
| Sleeves | `soxl_soxx=(SOXX,SOXL)`, `tqqq_qqq=(QQQ,TQQQ)` |
| Calendar / timezone | `XNYS` / `America/New_York` |
| Request | Alpaca Stock Bars: `1Day`, `feed=sip`, `adjustment=all`, `currency=USD`, half-open window `2016-01-01` to `2025-01-01` (2016 warmup, 2017–2024 learning) |
| Per symbol | Exactly one request; rejects pagination, missing sessions, unsorted or duplicate bars, invalid OHLCV, 403/429, and errors; zero automatic retries or source fallback |
| Storage | Existing private bucket `<research-evidence-bucket>`; prefix `research/v2/input/<batch_id>/{soxl_soxx,tqqq_qqq}/` |
| Per-sleeve objects | `prices.csv` -> `object_identity.json` -> `prices.csv.manifest.json` (manifest written last) |
| Writes | Create-only (`if_generation_match=0`); full absence preflight before calling Alpaca; unknown upload outcome becomes `UNKNOWN` with no retry, cleanup, or overwrite |
| Authority flags | `no_order=true`, `research_only=true`, `execution_authorized=false` |

## CLI

```bash
# Default PLAN_ONLY: no credential reads, no network calls, no file writes
python scripts/acquire_batch_a_v2_price_snapshots_alpaca.py

# Allowed only in main GitHub Actions + market-data-nonlive; requires explicit batch id
python scripts/acquire_batch_a_v2_price_snapshots_alpaca.py --execute --batch-id <batch_id>
```

`batch_id` must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`, forbids path separators or `..`, and is never auto-generated.

## GitHub Actions

Manual workflow: `.github/workflows/batch-a-v2-alpaca-sip-inputs.yml`

- `workflow_dispatch` only
- `github.ref == refs/heads/main` guard
- Inputs: `execute` (boolean, default `false`) and explicit `batch_id`
- Default prints the plan only; `execute=true` uses existing WIF (`GCP_WORKLOAD_IDENTITY_*`) and Alpaca secrets in `market-data-nonlive`
- Adds no new cloud resources, secrets, or schedules; acquisition runs only when explicitly dispatched with `execute=true`

## Manifest

`prices.csv.manifest.json` conforms strictly to UES `qsl.research.price_snapshot.v2` (including actual GCS `generation` / `bytes` / SHA-256, request parameters, provider/feed, retention metadata, `code_version` / `source_revision`, `retrieved_at`, and counts/coverage). Downstream consumption lives in UsEquityStrategies `run_batch_a_from_gcs.py`.
