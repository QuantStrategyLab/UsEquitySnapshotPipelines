# 历史组合 P1 输入与 P3 前置验证契约

这份切片把既有的组合研究契约接到一起，但**不运行回测**：

- UES 的 `qsl.us-equity-historical-combo-p2-candidate.v1` 仍是 P2 冻结候选的
  唯一所有者；
- UES 的 `qsl.us-equity-virtual-combo-target.v1` 仍是虚拟组合目标的唯一所有者；
- UESP 已有的 `qsl.us-equity-historical-combo-p3-evidence-index.v1` 仍是未来
  已完成 P3 证据的保留索引；以及
- 新增的 `qsl.us-equity-historical-combo-p1-input-binding.v1` 与
  `qsl.us-equity-historical-combo-p3-input-verification.v1` 只在 P3 回放之前
  固定和核对输入身份。

因此这里不会读取或下载行情，不访问凭证/GCS，不产生组合收益、成本结果、回撤、
晋级结论，也没有 scheduler、paper、shadow、broker 或订单能力。

## P1：共同历史输入身份

P1 binding 是一个自校验 SHA-256 的纯元数据对象，固定：

- 组合候选 ID、候选 revision 与配置摘要；
- 至少两个、按 `leg_id` 严格排序的成分策略；每个策略的 revision、配置摘要、
  自身 P1 输入摘要，以及**相同**的 `source_date_cutoff`；
- 共同 `common_cutoff`；
- 强制 `AS_OF_COMMON_CUTOFF` 的 PIT 声明：不允许未来数据、不允许事后修订数据，
  并固定信号与成交的时点；
- 非空、升序的显式成本压力数组以及借券/现金收益假设；和
- 冻结的 virtual-combo policy 摘要、`portfolio_risk_budget` policy 摘要，以及
  由既有虚拟组合目标投影出的摘要。

该摘要先验证上游虚拟目标自己的 SHA-256，再只保留其既有 schema、状态、policy、
input 和 target 摘要；不复制虚拟权重或风险指标。由于 P2 描述符本身要反向引用
P1 输入摘要，P1 不能直接内嵌 P2 描述符，否则会形成循环身份。P1 固定的是 P2
政策和虚拟目标，P3 前置验证再核对实际 P2 描述符与 P1 的双向引用。

## P3：前置验证，不是绩效结论

`verify_historical_combo_p3_inputs` 只接受 P1 binding 与既有 UES P2 descriptor。它在
任何未来 replay runner 被调用前验证：

- P2 的自摘要、研究限定和全部 P4/P5/P6 授权均为 false；
- P2 的 `p1_input_sha256` 与 P1 自摘要一致；
- 候选、每个成分策略 revision/配置摘要与 P1 一致；
- P2 风险预算摘要与 P1 冻结的预算摘要一致；以及
- 选择期和 holdout 期的结束日期都不晚于共同 cutoff。

完整时返回 `READY_FOR_P3_REPLAY`。这只表示“输入身份足以交给未来独立回放器”，
绝不表示策略有效、可以晋级、可以 paper/shadow 或可以 live。它会返回可复核的
摘要（P1/P2 摘要、共同 cutoff、成本声明摘要、虚拟目标摘要），而不会返回行情、
权重、绩效或订单。

缺字段、摘要不一致、成分 revision 漂移、风险政策漂移，或任一选择/holdout 日期
晚于共同 cutoff，都会直接返回 `PARKED`。`PARKED` 不能被此模块复位，也不会触发
工作流或任何外部写入。

## 仍然缺少什么

还没有真实、许可合规的共同历史 P1 输入；没有真实成本/OOS 历史回放器；没有 P3
绩效证据；也没有 P4/P5/P6 接线。现有
`historical_combo_p3_synthetic_replay` 只用注入的合成夹具验证这个输入契约的分段
OOS 与成本计算，不能代替或升级为真实历史绩效证据。未来实现真实回放时，必须先让它消费这个
`READY_FOR_P3_REPLAY` 摘要，再将完成结果交给既有 P3 evidence index；不能把
本契约的合成夹具或 `READY` 状态当作研究通过或执行资格。
