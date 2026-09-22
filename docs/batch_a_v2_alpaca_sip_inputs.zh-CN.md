# Batch A v2 Alpaca SIP 采集入口

审计 §9.15.27 D1。本入口只服务 `qsl.research.price_snapshot.v2`，与旧 Global ETF learning / R3 / Yahoo / `private-root` / v1 schema **互不复用、无兼容层**。

## 固定范围

| 项 | 约定 |
|---|---|
| Sleeves | `soxl_soxx=(SOXX,SOXL)`、`tqqq_qqq=(QQQ,TQQQ)` |
| 日历/时区 | `XNYS` / `America/New_York` |
| 请求 | Alpaca Stock Bars：`1Day`、`feed=sip`、`adjustment=all`、`currency=USD`、半开窗口 `2016-01-01`→`2025-01-01`（2016 warmup，2017–2024 learning） |
| 每标的 | 恰好一次请求；拒绝 pagination、缺日、乱序、重复、异常 OHLCV、403/429、错误；**零自动重试/换源** |
| 存储 | 既有私有桶 `qsl-research-evidence-831478360303`；前缀 `research/v2/input/<batch_id>/{soxl_soxx,tqqq_qqq}/` |
| 每 sleeve 对象 | `prices.csv` → `object_identity.json` → `prices.csv.manifest.json`（manifest 最后锁定） |
| 写入 | create-only（`if_generation_match=0`）；完整 preflight 不存在后才请求 Alpaca；上传结果未知即 `UNKNOWN`，不重试、不清理、不覆盖 |
| 权限标记 | `no_order=true`、`research_only=true`、`execution_authorized=false` |

## CLI

```bash
# 默认 PLAN_ONLY：不读凭据、不联网、不写文件
python scripts/acquire_batch_a_v2_price_snapshots_alpaca.py

# 仅允许 main GitHub Actions + market-data-nonlive；必须显式 batch id
python scripts/acquire_batch_a_v2_price_snapshots_alpaca.py --execute --batch-id <batch_id>
```

`batch_id` 必须匹配 `^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`，禁止路径分隔或 `..`；**不会自动生成**。

## GitHub Actions

手工 workflow：`.github/workflows/batch-a-v2-alpaca-sip-inputs.yml`

- 仅 `workflow_dispatch`
- `github.ref == refs/heads/main` guard
- 输入：`execute`（boolean，默认 false）与显式 `batch_id`
- 默认只打印 plan；`execute=true` 时使用既有 WIF（`GCP_WORKLOAD_IDENTITY_*`）与 `market-data-nonlive` 中的 Alpaca secrets
- **不新增**云资源、secret 或调度；本工程任务不触发真实采集

## Manifest

`prices.csv.manifest.json` 字段集合严格符合 UES `qsl.research.price_snapshot.v2`（含实际 GCS `generation` / `bytes` / SHA-256、request、provider/feed、许可保留、`code_version` / `source_revision`、`retrieved_at`、counts/coverage）。下游消费见 UsEquityStrategies `run_batch_a_from_gcs.py`。
