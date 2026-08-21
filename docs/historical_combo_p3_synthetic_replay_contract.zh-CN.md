# 历史组合 P3 合成成本 / OOS 回放契约

`qsl.us-equity-historical-combo-p3-synthetic-replay.v1` 是既有组合 P1/P2/P3
契约之上的**离线测试切片**。它只接收调用方注入的
`synthetic_fixture`，用于验证冻结身份、分段 OOS、换手成本计算和指标摘要是否能
确定性地复现。

它不是历史行情回放器，更不是组合绩效、晋级、paper、shadow 或 live 的证据。

## 前置与严格绑定

每次执行先调用既有的
`verify_historical_combo_p3_inputs`。只有其返回 `READY_FOR_P3_REPLAY` 时，
回放器才接受夹具；否则直接 `PARKED`。夹具再自校验并绑定：

- 已冻结 P1/P2 摘要与 P3 preflight 摘要；
- P1 的共同 `common_cutoff`、PIT 声明摘要和成本声明摘要；
- P2 已冻结的 holdout 内、非重叠且按时间和 ID 排序的 OOS 分段；以及
- 每个分段内升序、仅含已冻结成分 `leg_id` 的合成日收益。

任何成分集合、摘要、PIT 声明或成本声明漂移都会 `PARKED`。任何分段或观察日期
晚于共同 cutoff 会以 `FUTURE_LEAKAGE_DETECTED` 停车；选择期中的观察也不被
接受。这样固定权重、策略 revision、成本假设和 OOS 边界后，夹具不能借由修改
输入悄悄使用未来信息。

## 合成成本与指标

成本情景只来自 P1 已冻结的 `turnover_cost_bps` 数组，不能由夹具增加、删除或
优化。P2 目前要求所有 sleeve 为正且总权重为 100%，所以每段从既有目标权重开始，
每日按收益漂移后才计算恢复到目标权重的单边换手；最后一个观察日不虚构下一期
再平衡或平仓成本。

输出按分段列出合成的 gross/net total return、net max drawdown、单边换手和已应用
的换手成本率，并给出跨段的均值 / 最差值 / 总换手摘要。借券和现金假设仍会原样
显示，但在这个满仓 long-only P2 模型里不适用，不会被伪造为已实现收益或成本。

输出状态只能是 `SYNTHETIC_REPLAY_COMPLETE_NOT_REAL_EVIDENCE` 或 `PARKED`，固定：

- `research_only=true`、`execution_authorized=false`、`real_market_evidence=false`；
- `promotion_recommendation=null`；
- `paper_authorized=false`、`shadow_authorized=false`、`live_authorized=false`。

该完成状态**不能**传给现有
`historical_combo_p3_evidence_index`，后者只接受真实、完整 P3 研究证据的
`HISTORICAL_COMBO_RESEARCH_EVIDENCE_COMPLETE`。本切片不读取行情、文件、网络、
凭证、GCS、workflow、scheduler、账户或 broker，也不写入任何工件。

## 接入真实历史数据前仍需的条件

未来独立的真实回放适配器必须另行提供：许可合规、可复现的共同 P1 原始输入；每个
观察的 as-of / revision / corporate-action 来源证明；冻结交易日历与信号到成交时点；
可审计的成本、借券、现金和流动性模型；完整 holdout / rolling OOS 覆盖；以及将真实
结果和其证据摘要写入既有 P3 evidence index 的受限工件路径。它不能复用或升级任何
这里的合成结果。
