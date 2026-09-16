# Global ETF learning input adapter

本入口只定义受限的 Global ETF learning 输入边界：27 个固定标的、XNYS 交易日、2016-01-01 至 2024-12-31（2016 预热，2017–2024 learning）。Alpaca Stock Bars 请求固定为 `sip`、`1Day`、`adjustment=all`、`currency=USD`、`asof=2024-12-31`、升序和每请求 `limit=10000`；请求边界使用纽约时间，结束时间为 `2025-01-01T00:00:00-05:00`。

脚本默认只输出 plan，不读取环境凭据、不联网、不写文件。唯一执行入口是固定 main 分支上的手工 GitHub Actions `--execute-cloud`，使用 `market-data-nonlive` 环境、既有 GCP WIF 和固定 GCS `qsl-runtime-logs-shared/global-etf-learning/2017-2024/20260916/bars.json`；不提供 Mac 或常驻本地执行入口。执行器将通过普通形状的 `get_transport(url, params)` 在内存中逐标的执行最多 27 次请求，完整结果只一次以 generation-match=0 上传，运行前只确认目标对象不存在。分页、错误、缺日、重复、超窗、乱序、额外标的和非法数值立即 fail-closed，不重试，也不返回部分结果。

返回内容只在内存中保留统一 OHLCV 行和计数摘要，并固定标记 `no_order=true`、`promotion_eligible=false`、`live_ready=false`、`pit_verified=false`、`learning_only=true`、`size_zero_required=true`。`asof=2024-12-31` 只是固定证券名称映射日，不是 PIT 证明；`adjustment=all` 是请求参数，也不等于已验证公司行动完整性。本批真实采集前已确认 bucket 的 PAP、uniform access、`global-etf-learning/` 精确 prefix Delete age7、soft delete 7 天及其他保留边界；active 与 soft-delete 保留均为后台异步语义，不实现自动清理。历史成分、来源许可和实际账户权限仍需单独核验。参考 [Alpaca Stock Bars](https://docs.alpaca.markets/us/reference/stockbars)。

## 一次性 learning replay

已保存的唯一快照可由手工 main-only workflow `global-etf-learning-replay.yml` 做一次既有 runner 回放。输入固定为本对象 generation `1789552505861428`、size `6643030`；执行器在 GCS 云端内存下载并用 generation match 绑定，不把 bars 写入本机、GitHub artifact 或新的数据文件。输出只创建 `global-etf-learning/2017-2024/20260916/learning-replay.json`，目标已存在或上传结果不明都会停止，不重放。

回放固定使用 `UsEquityStrategies` commit `5f11fcfe8c5473de20e1b590e9aa3e87665b6108` 的 `UsEtfRotationBacktestRunner`，`global_etf_rotation`、`min_history_days=260`、2017-01-01 至 2024-12-31；2016 只作 warmup，保持 lag-one、默认 10 bps 费用和既有 runner 聚合 metrics。workflow 另外固定安装 `pandas-market-calendars==5.4.0`，并在读取 GCS 前确认 NYSE 2018-03 最后交易日为 2018-03-29，缺失即停止，避免静默回退工作日月末。

结果只报告来源 generation、runner revision、窗口、实际 params、既有聚合 metrics 和 `no_order=true`、`learning_only=true`、`pit_verified=false`、`promotion_eligible=false`、`live_ready=false`、`size_zero_required=true`。没有可比基准时明确未比较，不输出胜率、Sortino 或虚构盈利结论；回放成功也不代表真实 PIT、晋级、交易或自然观察完成。默认 CLI 仍 plan-only，`--execute-cloud` 只接受 main GitHub Actions 环境；不重采、不调用 AI、不交易。

## VOO/BIL 基准比较

同一 workflow 的 `operation=compare` 只读取固定 bars generation `1789552505861428` 和既有 replay 结果 generation `1789555099559060`（size `1601`），先检查目标 `global-etf-learning/2017-2024/20260916/learning-benchmarks.json` 不存在；任一来源身份、窗口、controls、UES/QPK revision、10 bps 或 replay metrics（含 2012 observations）不匹配即停止。比较器不调用 runner、不重采、不调用 AI，结果只在云端内存中计算并 create-only 上传聚合摘要。

VOO/BIL 均从 2017 年首个交易日收盘买入，初始本金费用 `c=0.001` 只在下一日计入：`r0=0`，`r1=(P1/P0)/(1+c)-1`，之后使用 `pct_change`，不做终末卖出。指标复用固定 `compute_backtest_metrics`，映射 `annual_return→cagr`、`annual_volatility→volatility`、`days→observation_count`；输出复制策略 metrics、两个基准 metrics 和 `strategy_minus_benchmark` 简单差值，不产生 alpha、晋级或盈利结论。
