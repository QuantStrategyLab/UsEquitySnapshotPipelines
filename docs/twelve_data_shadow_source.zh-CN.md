# Twelve Data 免费日线影子来源

`us_equity_snapshot_pipelines.twelve_data_daily` 使用 Twelve Data 的固定 HTTPS `time_series` 日线请求，并设置 `interval=1day`、`adjust=all`、固定起止日期。密钥只通过 `Authorization: apikey ...` 请求头传递，不会放入 URL、日志、诊断或产物。

它只产生一个内存中的独立来源观察：

- 成功：`READY`，包含不可变来源根哈希与标准日线序列；
- 密钥缺失、权限、限流或网络问题：`UNAVAILABLE` 加稳定原因码；
- 响应格式、标的或 OHLCV 结构异常：`INVALID` 加稳定原因码。

该适配器本身不写 P1、不回测、不下单，也不会将 Twelve Data 的结果直接当作 Alpaca SIP 的替代品。调用方必须把两个独立来源送入 QPK 的 `assess_multisource_daily_bars`：只有交易日覆盖、复权口径及 OHLCV 都通过一致性检查，才会得到可发布的 `VERIFIED` 报告。

为 SOXL 启用前，在仓库环境 `tqqq-p1-p3-nonlive` 中添加密钥 `TWELVE_DATA_API_KEY`。不要把密钥发到聊天、代码、工作流参数或 URL 中。启用后仍需先运行只读诊断和跨源一致性回测；未通过时保持 `DEGRADED` 并阻断策略发布。
