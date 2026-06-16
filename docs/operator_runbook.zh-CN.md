# 操作运行手册

[English](operator_runbook.md)

本仓库是 snapshot-backed 美股策略的上游 artifact 生产仓库。券商平台仓库仍然只是下游消费者。

## 本仓库生产的 Snapshot Profiles

- `russell_1000_multi_factor_defensive`
- `mega_cap_leader_rotation_top50_balanced`

`tech_communication_pullback_enhancement` 已随运行时移除降级为归档研究，不再出现在 publish 或 health workflow。`mega_cap_leader_rotation_dynamic_top20`、`mega_cap_leader_rotation_aggressive`、`dynamic_mega_leveraged_pullback` 也不再作为可发布 snapshot profile。Mega-cap 系列只保留 Top50 balanced 运行路线。

## 本地手动构建

归档科技通信回调增强研究构建：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_tech_communication_pullback_snapshot.py \
  --prices /path/to/price_history.csv \
  --universe /path/to/universe.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/tech_communication_pullback_enhancement
```

Russell 1000 多因子：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_1000_feature_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive
```

Mega-cap Top50 balanced：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_top50_balanced_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/mega_cap_leader_rotation_top50_balanced
```

## 手动 GitHub Actions 发布

使用 `Publish Snapshot Artifacts` workflow。

必填输入：

- `profile`

生产数据路径需要同时设置：

- `prices_path`
- `universe_path`

常用可选输入：

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `config_path`，仅仍需外部配置的 profile 使用
- `current_holdings`
- `portfolio_total_equity`
- `min_adv20_usd`

策略插件发布 workflow 在 `execute_publish=true` 时只接受位于
`gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/<scope>/plugins/<plugin>`
之下的手工 GCS prefix override。

workflow 每次都会把生成文件上传为 GitHub Actions artifact。

## 定时发布

`Update Source Input Data` 每月 1 日 `00:15 UTC` 自动运行，也就是 Asia/Shanghai 同日 `08:15`。它刷新月度 snapshot profiles 共用的 Russell 1000 输入数据：

```text
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_symbol_aliases.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_snapshot_metadata.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
```

`Publish Snapshot Artifacts` 每月 1 日 `00:45 UTC` 自动运行，构建：

```text
profiles=russell_1000_multi_factor_defensive,mega_cap_leader_rotation_top50_balanced
prices_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
russell_universe_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
mega_top50_balanced_universe_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
execute_publish=true
```

默认定时输出前缀：

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `russell_1000_multi_factor_defensive` | none | `gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/russell_1000_multi_factor_defensive_staging` |
| `mega_cap_leader_rotation_top50_balanced` | none | `gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/mega_cap_leader_rotation_top50_balanced_staging` |

workflow 仍保留月末交易日 guard：如果解析得到的 `snapshot_as_of` 不是该月份最后一个 NYSE 交易日，会写出 skip artifact，并且不会发布到 GCS。

## 月度 AI Review

定时 `Publish Snapshot Artifacts` 成功后，`Monthly Snapshot Review` 会下载该 run 的 artifacts，并组装：

```text
data/output/monthly_report_bundle/monthly_report_bundle.json
data/output/monthly_report_bundle/ai_review_input.md
data/output/monthly_report_bundle/job_summary.md
```

workflow 会创建或更新带 `monthly-review` label 的 GitHub issue。默认路线会 dispatch `QuantStrategyLab/CodexAuditBridge`，由 Quant HTTPS/443 service-backed Codex 路径执行审计，并把结果回帖到 issue。审计重点包括：

- 每个预期 snapshot profile 的 artifact 完整性
- contract version、snapshot date、row count 和 ranking-preview 健康度
- 会影响下游信心的缺失或过期证据
- 对 broker/runtime 仓库的下游影响

月度 review workflow 会 dispatch `CodexAuditBridge`，由 bridge 通过 `CODEX_AUDIT_PROVIDER` 统一决定 provider。

可选 provider：

- `auto`：默认值。先跑 service-backed Codex 路径；如果 Codex service 执行失败，再回落到已配置的 API reviewers。可用 `CODEX_AUDIT_BRIDGE_REF` pin bridge workflow ref，默认 `main`。
- `codex`：只跑 service-backed Codex，关闭 API fallback。
- `api`：汇总 API 审阅。要启用双 AI fallback，需要在 bridge 同时配置 `OPENAI_API_KEY` 和 `ANTHROPIC_API_KEY`。
- `openai` / `anthropic`：只回帖单一 API 审阅。

默认不会自动合并。只有确认 branch protection 和 CI gate 后，才设置 `CODEX_AUDIT_AUTO_MERGE=true`，让 bridge 显式请求 GitHub auto-merge。

## 排障规则

- 如果 source-input refresh 失败，snapshot publish 仍可能使用上一份成功发布的 source input 运行。
- 如果 snapshot-backed profile 加载失败，先确认 artifact manifest、schema、`as_of` 新鲜度。
- 如果定时发布跳过，先看 skip artifact 里的 resolved trading day，再判断是不是月末交易日 guard 生效。
