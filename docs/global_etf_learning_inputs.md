# Global ETF learning input adapter

本入口只定义受限的 Global ETF learning 输入边界：27 个固定标的、XNYS 交易日、2016-01-01 至 2024-12-31（2016 预热，2017–2024 learning）。Alpaca Stock Bars 请求固定为 `sip`、`1Day`、`adjustment=all`、`currency=USD`、`asof=2024-12-31`、升序和每请求 `limit=10000`；请求边界使用纽约时间，结束时间为 `2025-01-01T00:00:00-05:00`。

脚本默认只输出 plan，不读取环境凭据、不联网、不写文件。唯一执行入口是固定 main 分支上的手工 GitHub Actions `--execute-cloud`，使用 `market-data-nonlive` 环境、既有 GCP WIF 和固定 GCS `qsl-runtime-logs-shared/global-etf-learning/2017-2024/20260916/bars.json`；不提供 Mac 或常驻本地执行入口。执行器将通过普通形状的 `get_transport(url, params)` 在内存中逐标的执行最多 27 次请求，完整结果只一次以 generation-match=0 上传，运行前只确认目标对象不存在。分页、错误、缺日、重复、超窗、乱序、额外标的和非法数值立即 fail-closed，不重试，也不返回部分结果。

返回内容只在内存中保留统一 OHLCV 行和计数摘要，并固定标记 `no_order=true`、`promotion_eligible=false`、`live_ready=false`、`pit_verified=false`、`learning_only=true`、`size_zero_required=true`。`asof=2024-12-31` 只是固定证券名称映射日，不是 PIT 证明；`adjustment=all` 是请求参数，也不等于已验证公司行动完整性。bucket 的 PAP、uniform access、精确 prefix 删除规则（active 7 天）、soft delete 7 天、versioning/hold/retention 边界需由管理员在唯一真实执行前核验；active 与 soft-delete 保留均为后台异步语义，不实现自动清理。历史成分、来源许可和实际账户权限仍需单独核验。参考 [Alpaca Stock Bars](https://docs.alpaca.markets/us/reference/stockbars)。
