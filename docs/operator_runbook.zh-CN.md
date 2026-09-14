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

`Monthly Snapshot Review` 是只生成报告的 workflow：它构建既有 health、
promotion-readiness 与 review 证据包，上传该 bundle，并创建或更新
`monthly-review` issue。

AIAuditBridge 的 review、retry 和 merge dispatch 路径已经退役。GitHub Codex
App 是 PR 的唯一 AI reviewer。该 workflow 不会 dispatch AI reviewer、创建
remediation PR、重试反馈，或请求/执行自动合并。证据包仅作参考，不构成审批或
合并信号。

仓库 settings、旧变量和 secret 不属于此 workflow 的契约范围。不要从本文推断
其 runtime 状态；任何 settings 修改都需要独立的、已认证且明确授权的流程。

## Russell 离线 core-signal 研究回放

该 runner 只复用 `russell_top50_leader_rotation` 的 `compute_signals`，通过 QPK
普通 `BacktestOrchestrator` 运行。它是 `core_signal_only` 研究工具，不调用 AI、
不产生订单、不具备晋级资格，也不包含 income、option overlay、账户风控或实盘
成交层。输入的 feature snapshot 与价格必须使用统一的可比口径；股票拆分、分红
和复权来源在本工具中不会被推断或修正。

```bash
PYTHONPATH=src:$UES_SRC:$QPK_SRC \
python -m us_equity_snapshot_pipelines.lifecycle.russell_research_runner \
  --features features.csv --prices prices.csv --data-kind research \
  --equity 100000 --bps 25 --variant blend_top2_50_top4_50 \
  --output ./research-output
```

`data-kind` 只能是显式的 `synthetic` 或 `research`；synthetic 结果不得解释为
真实 PIT、OOS、验证或 live readiness 证据。该 runner 的 flags 固定为学习用途、
零规模和 no-order，不能通过参数反转。

## 固定 Russell core case

`.github/workflows/russell-core-research-case.yml` 仅支持手动、非 live 的一次性
研究接线。它从固定 generation 的 feature snapshot 读取一次内存数据，并请求
2026-09-01 至 2026-09-11 的 AMD、INTC、MU、PANW Alpaca SIP 全复权日线；只输出
聚合收益、回撤、费用、样本数和 research flags，不落 bars、daily、trades 或上传
工件。25 bps 每边是本例研究假设，不是券商实际成本。Alpaca SIP 全复权日线开/收盘
用于研究模拟，不代表券商成交价；本例不另计股息现金，避免与复权价格重复计算。
