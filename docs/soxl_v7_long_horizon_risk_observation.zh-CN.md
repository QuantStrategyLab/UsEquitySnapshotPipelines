# SOXL V7 长期风险观察件

`soxl_core_only_v7_long_horizon_risk_observation.py` 把一次已经完成的、冻结的 SOXL V7 P3 回放转换为 `qsl.long_horizon_risk_observation.v1` 私有观察件，供 `QuantRuntimeSettings` 的长期复利风险 Composer 使用。

这不是策略参数、风险政策、P4/P5/P6 准入或平台重载。它不获取行情、不访问凭据、账户、券商或网络，也不创建云端对象。

## 证据口径

- `WALK_FORWARD`：固定 `trailing_252_xnys_session_oos` 的 10 bps 净成本滚动 OOS 回放；它是 P3 历史 OOS 证据，不是 V7 的 P4 前瞻确认。
- `STRESS`：同一冻结连续 756-session 窗口的 15 bps 净成本回放。
- `BOOTSTRAP`：连续窗口 10 bps 的策略/ SOXX 收益对，以固定 21-session 移动块、由 P3 摘要哈希导出的确定性种子产生 8 条路径。成对重采样保留策略与无杠杆基准之间的同期关系。

每条路径记录真实观测交易日数量 `session_count`；相邻日收益率数量必须刚好少一天。因而固定 252 XNYS-session OOS 路径含 251 个相邻日收益率，仍是完整一年的观察窗口。

## 私有边界

离线 P3 façade 仅在明确传入 `--risk-observation-output <new-private-file>` 时创建观察件。该文件必须位于调用方预先准备好的受保护目录，且目标必须不存在；写入使用 create-only 和 `0600` 权限。默认工作流不传入该参数，因此不会把收益路径上传到 Actions artifact、Job Summary、仓库、控制台或 AI 上下文。

后续受限 ingress 必须校验观察件哈希，再由控制面显式绑定所有者选择的保守、均衡或增长偏好。Composer 只输出脱敏建议，不能写回 SOXL 配置、重载任何平台或提交订单。
