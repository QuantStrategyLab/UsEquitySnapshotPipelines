# TQQQ 免费 OHLCV 数据源校准观察

`TQQQ V9 Free OHLCV Assurance Calibration` 在每个已完成的美股交易日后，以 Twelve Data 与 Yahoo 的拆股调整日线做三次单日、只读交叉验证：最新完成交易日（T+0）、前一交易日（T+1）和前二交易日（T+2）。同一交易日因此会在连续三次任务中留下固定时间点的证据。

每个探针只保留 UTC 采样时间、请求日期窗口、来源响应哈希、状态和按 OHLC 字段的差异分布；不会上传价格、成交量、原始响应、路径或凭据。为遵守 Twelve Data 免费层的分钟级请求窗口，工作流完成 T+0/T+1 的八个 Twelve 请求后会固定等待 60 秒，再采样 T+2；若仍限流，结果会安全地停为 `FREE_SOURCE_UNAVAILABLE`，而不是得出市场数据结论。GitHub 工件保留 35 天。工作流仅以 GitHub Actions 的只读权限读取自己此前的脱敏工件，生成 `PENDING_T_PLUS_2`、`REVISED_OR_UNSETTLED`、`SETTLED_VERIFIED` 或 `PERSISTENT_CROSS_SOURCE_DISAGREEMENT` 等数据结算状态。

这不是回测、P3、影子、平台操作或交易任务。即使 P1 数据源完全一致，工作流也只记录 `VERIFIED`，不会自动创建研究输入、修改 V9、放宽容差、选择数据源、切换策略或推进任何上线阶段。

数据结算状态不是策略结论。当记录到足够多个独立已完成交易日后，后续校准必须单独冻结新的候选数据合约，并用该候选重新回测和审计；历史 V9 的 1bp 全 OHLC 规则始终保持不变。单次或少量差异绝不能自动改写策略或风险阈值。
