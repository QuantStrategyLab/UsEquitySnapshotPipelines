# 操作运行手册

本仓库是 snapshot-backed 美股策略的上游 artifact 生产仓库。券商平台仓库仍然只是下游消费者。

这份公开文档只说明 artifact 如何构建、发布和验证，不记录任何平台或账户当前部署了哪条策略。

## 本仓库生产的 snapshot-backed profiles

- `tech_communication_pullback_enhancement`
- `russell_1000_multi_factor_defensive`
- `mega_cap_leader_rotation_dynamic_top20`
- `mega_cap_leader_rotation_top50_balanced`
- `dynamic_mega_leveraged_pullback`

这些 profile 的交易逻辑和运行频率以 `UsEquityStrategies` 为准；本仓库只负责生成下游运行时需要消费的 snapshot artifact。

## 本地手动构建

科技通信回调增强：

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

Mega-cap 动态 Top20：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_dynamic_top20_snapshot.py \
  --prices /path/to/mega_cap_leader_rotation_dynamic_top20_price_history.csv \
  --universe /path/to/mega_cap_leader_rotation_dynamic_top20_universe_history.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/mega_cap_leader_rotation_dynamic_top20
```

Top20 universe 输入必须已经是动态 Top20 历史，或者是带有 `mega_rank`、`source_weight`、`weight`、`source_market_value`、`market_value` 之一的已排名 Russell universe。定时 GitHub Actions 路径使用 source-input refresh 生成的 `r1000_latest_holdings_snapshot.csv`。

Mega-cap aggressive：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_mega_cap_leader_rotation_aggressive_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/mega_cap_leader_rotation_aggressive
```

这个命令发布独立的 `mega_cap_leader_rotation_aggressive` artifact contract，对应 top-3/no-defense 运行 profile。

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

这个命令发布独立的 `mega_cap_leader_rotation_top50_balanced` artifact contract，对应固定 `50% Top2 cap50 + 50% Top4 cap25` 袖子混合。

Dynamic mega leveraged pullback：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_dynamic_mega_leveraged_pullback_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --product-map /path/to/dynamic_mega_2x_product_map.csv \
  --as-of 2026-04-01 \
  --output-dir data/output/dynamic_mega_leveraged_pullback
```

`product_map` 是必需输入。builder 不会在缺少 2x 产品映射时自动退回买正股；未映射行会标记为 unavailable，可用行必须指向近似 2x long 产品。

Russell 1000 回测：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/backtest_russell_1000_multi_factor_defensive.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_universe_history.csv \
  --start 2019-01-01 \
  --output-dir data/output/russell_1000_multi_factor_defensive_backtest
```

## 手动 GitHub Actions 发布

使用 `Publish Snapshot Artifacts` workflow。

必填输入：

- `profile`

生产数据路径需要同时设置：

- `prices_path`
- `universe_path`

路径可以是仓库 checkout 后的本地路径、`gs://...` 或 `http(s)://...` URL。`tech_communication_pullback_enhancement` 的 `config_path` 也支持这些路径类型。

Smoke test 可以设置 `use_sample_data=true`，此时 `prices_path` 和 `universe_path` 可以留空。

常用可选输入：

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `config_path`
- `product_map_path`
- `current_holdings`
- `portfolio_total_equity`
- `min_adv20_usd`

workflow 每次都会把生成文件上传为 GitHub Actions artifact。

## 定时发布

`Update Source Input Data` 每月 1 日 `00:15 UTC` 自动运行，也就是 Asia/Shanghai 同日 `08:15`。它会刷新月度 snapshot profiles 共用的 Russell 1000 输入数据，包括：

- `r1000_price_history.csv`
- `r1000_universe_history.csv`
- `r1000_symbol_aliases.csv`
- `r1000_universe_snapshot_metadata.csv`
- `r1000_latest_holdings_snapshot.csv`

refresh workflow 会先下载已有价格历史，刷新最近重叠窗口，再为新增 symbol 下载完整历史，最后发布合并后的输入文件。它也会强制把 `QQQ`、`SPY`、`BOXX` 放进价格输入，保证 QQQ/SPY benchmark 和 BOXX 停泊逻辑有必要的参考 symbol。

`Publish Snapshot Artifacts` 每月 1 日 `00:45 UTC` 自动运行，也就是 Asia/Shanghai 同日 `08:45`。这个时间留给 source-input refresh 先完成。定时发布会从刷新后的输入构建所有定时 snapshot profiles：

```text
profiles=tech_communication_pullback_enhancement,russell_1000_multi_factor_defensive,mega_cap_leader_rotation_dynamic_top20,mega_cap_leader_rotation_top50_balanced,dynamic_mega_leveraged_pullback
prices_path=<shared Russell 1000 price history>
tech_and_russell_universe_path=<shared Russell 1000 universe history>
mega_dynamic_top20_universe_path=<latest weighted Russell 1000 holdings snapshot>
dynamic_mega_leveraged_product_map_path=<operator managed dynamic mega 2x product map>
execute_publish=true
```

定时发布路径刻意保持月频。它描述的是 artifact 发布频率，不是策略交易频率；策略层频率继续以 `UsEquityStrategies` 为准。

workflow 还保留一个防守性的月末交易日 guard：如果解析得到的 `snapshot_as_of` 不是该 snapshot 月份最后一个 NYSE 交易日，workflow 会写出 skip artifact，并且不会发布到 GCS。

## 排障规则

- 如果 source-input refresh 失败，snapshot publish 仍可能使用上一份成功发布的 source input 运行；数据源异常后要先检查 `Update Source Input Data`。
- 如果 snapshot-backed profile 加载失败，先确认 artifact manifest 是否存在、schema 是否匹配、`as_of` 是否在允许新鲜度窗口内。
- 如果 dynamic mega leveraged 构建失败，优先检查 `product_map_path` 是否可读，以及 universe 中的候选是否都有可用 2x 产品映射。
- 如果定时发布跳过，先看 skip artifact 里的 resolved trading day，再判断是不是月末交易日 guard 生效。

## 边界

- 本仓库不连接券商。
- 本仓库不做账户 / 持仓 reconciliation。
- 本仓库不下单。
- 本仓库不发送运行时 Telegram 交易通知。
- 本仓库不记录任何平台或账户当前部署了哪条策略。
