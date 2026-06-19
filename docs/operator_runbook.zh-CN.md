# 操作运行手册

[English](operator_runbook.md)

本仓库是 snapshot-backed 美股策略的上游 artifact 生产仓库。券商平台仓库仍然只是下游消费者。

## 本仓库生产的 Snapshot Profiles

当前唯一面向运行时发布的 snapshot profile 是：

- `russell_top50_leader_rotation`

`russell_1000_multi_factor_defensive` 因长期只小幅跑赢 SPY、回撤优势不足，已从本仓库运行时契约中退役。`tech_communication_pullback_enhancement` 以及已退役的 Russell Top50 dynamic/leveraged 研究变体均保留为归档研究，不再出现在 publish 或 health workflow。

## 本地手动构建

Russell Top50 leader rotation：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_top50_leader_rotation_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/russell_top50_leader_rotation
```

## 手动 GitHub Actions 发布

使用 `Publish Snapshot Artifacts` workflow。

必填输入：

- `profile`，当前只支持 `russell_top50_leader_rotation`

生产数据路径需要同时设置：

- `prices_path`
- `universe_path`

常用可选输入：

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `current_holdings`
- `portfolio_total_equity`
- `min_adv20_usd`，用于 Russell Top50 测试覆盖

策略插件发布 workflow 在 `execute_publish=true` 时只接受位于
`gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/<scope>/plugins/<plugin>`
之下的手工 GCS prefix override。

`Publish Strategy Plugins` 会同时构建 strategy artifact 和统一
`notification_targets.market_regime_notification` artifact。人工复核插件 bot
只应消费这个统一 notification target；TQQQ、SOXL 等 strategy artifact 仍供策略
runtime 自动消费，并在实际仓位变化时由策略运行通知承载。

统一通知使用 `STRATEGY_PLUGIN_ALERT_*` vars/secrets，默认中文
`STRATEGY_PLUGIN_ALERT_LANG=zh`，并通过
`STRATEGY_PLUGIN_ALERT_STATE_GCS_URI` 做跨 run 去重。未配置投递凭据时 workflow
会写出 skipped 诊断，不会影响 artifact 发布。

workflow 每次都会把生成文件上传为 GitHub Actions artifact。

## 定时发布

`Update Source Input Data` 每月 1 日 `00:15 UTC` 自动运行，也就是 Asia/Shanghai 同日 `08:15`。它刷新月度 snapshot profile 使用的 Russell 1000 输入数据：

```text
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_symbol_aliases.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_snapshot_metadata.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
```

`Publish Snapshot Artifacts` 会在 source-input refresh 后构建：

```text
profiles=russell_top50_leader_rotation
prices_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
universe_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
execute_publish=true
```

默认定时输出前缀：

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `russell_top50_leader_rotation` | none | `gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/russell_top50_leader_rotation_staging` |

workflow 仍保留月末交易日 guard：如果解析得到的 `snapshot_as_of` 不是该月份最后一个 NYSE 交易日，会写出 skip artifact，并且不会发布到 GCS。

## 月度 AI Review

定时 `Publish Snapshot Artifacts` 成功后，`Monthly Snapshot Review` 会下载该 run 的 artifacts，并组装：

```text
data/output/monthly_report_bundle/monthly_report_bundle.json
data/output/monthly_report_bundle/ai_review_input.md
data/output/monthly_report_bundle/job_summary.md
```

workflow 会创建或更新一个带 `monthly-review` label 的 GitHub issue。默认会 dispatch `QuantStrategyLab/CodexAuditBridge`，由该桥接 workflow 调用 Quant HTTPS/443 服务端 Codex 路径，并把 audit 结果回写到 issue。review 重点包括：

- 预期 snapshot profile 的 artifact 完整性；
- contract version、snapshot date、row count 和 ranking-preview 健康度；
- 会降低下游信心的缺失或陈旧证据；
- 对券商/runtime 下游仓库的影响。

`Monthly Snapshot Review` 通过 `CODEX_AUDIT_PROVIDER` 控制 reviewer provider。默认 `auto` 会优先调用 service-backed Codex，失败时回退到已配置的 API reviewer；没有 API fallback key 时会显式失败。可通过 `CODEX_AUDIT_BRIDGE_REF` pin 桥接 workflow ref，默认是 `main`。
