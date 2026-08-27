# SOXL V7 长期风险观察件

`soxl_core_only_v7_long_horizon_risk_observation.py` 把一次已经完成的、冻结的 SOXL V7 P3 回放转换为并行的 `qsl.long_horizon_risk_observation.v1` 与 `qsl.long_horizon_risk_observation.v2` 私有观察件，供 `QuantRuntimeSettings` 的长期复利风险 Composer 使用。

这不是策略参数、风险政策、P4/P5/P6 准入或平台重载。它不获取行情、不访问凭据、账户、券商或网络，也不创建云端对象。

## 证据口径

- `WALK_FORWARD`：固定 `trailing_252_xnys_session_oos` 的 10 bps 净成本滚动 OOS 回放；它是 P3 历史 OOS 证据，不是 V7 的 P4 前瞻确认。
- `STRESS`：同一冻结连续 756-session 窗口的 15 bps 净成本回放。
- `BOOTSTRAP`：连续窗口 10 bps 的策略/ SOXX 收益对，以固定 21-session 移动块、由 P3 摘要哈希导出的确定性种子产生 8 条路径。成对重采样保留策略与无杠杆基准之间的同期关系。

每条路径记录真实观测交易日数量 `session_count`；相邻日收益率数量必须刚好少一天。因而固定 252 XNYS-session OOS 路径含 251 个相邻日收益率，仍是完整一年的观察窗口。

## V2 的诚实能力声明

V2 不把 SOXL V7 当成可以按历史日收益直接线性缩放的普通 ETF 策略：日度杠杆、动态现金保留、交易成本和状态化回放意味着每个风险尺度都必须重新运行 P3。因此它固定声明 `return_evaluation=REPLAY_REQUIRED`、`portfolio_scope=SINGLE_CANDIDATE` 和无外部现金流；当前通用 Composer 会以 `RETURN_SCALE_REPLAY_REQUIRED` 停放，不输出可执行仓位或回撤阈值。

上游 P1 只提供**拆分复权收盘价**，不含可验证的股息总回报序列。V2 的 SOXX 基准因而明确标为 `SPLIT_ADJUSTED_PRICE_RETURN`，并绑定 `price_field=split_adjusted_close` 的定义摘要；绝不伪称为 `TOTAL_RETURN_NET_OF_COST`。在具有可比的总回报基准和专用 replay Composer 之前，这也是一个停放原因，而不是放宽门槛的理由。

## 私有边界

离线 P3 façade 仅在明确传入 `--risk-observation-output <new-private-v1-file>` 或 `--risk-observation-v2-output <new-private-v2-file>` 时创建观察件。该文件必须位于调用方预先准备好的受保护目录，且目标必须不存在；写入使用 create-only 和 `0600` 权限。默认工作流不传入这些参数，因此不会把收益路径上传到 Actions artifact、Job Summary、仓库、控制台或 AI 上下文。

只有同时显式请求两份私有观察件时，才可指定 `--risk-observation-comparison-output <new-private-receipt>`。它生成 `qsl.soxl_core_only_v7_long_horizon_risk_observation_comparison.v1`：只包含候选、证据和场景路径的摘要，以及 V1/V2 观察件摘要，证明两份路径完全一致；它不含原始收益路径，也不运行 Composer、比较建议、写风险政策或触发平台动作。

后续受限 ingress 必须校验观察件哈希，再由控制面显式绑定所有者选择的保守、均衡或增长偏好。Composer 只输出脱敏建议，不能写回 SOXL 配置、重载任何平台或提交订单。
