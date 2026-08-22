# 组合候选自动重评估（研究限定）

`portfolio-candidate-readiness` 是一个低频、脱敏的自动重评估器。它只读取
TQQQ v5 与 SOXL v3 日更工作流已经发布的 Actions P1/P3 terminal artifact，生成
`qsl.portfolio-candidate-readiness.v1`。

它不下载或读取 bars，不访问 GCS、凭证、账户或券商，也不会启动、重试或等待任一
日更工作流。

只有以下事实同时成立时，状态才是 `AI_RESEARCH_PROPOSAL_READY`：

- 两个已注册单策略候选都具有 `ACCEPTED` P1；
- 两个候选都具有完整的当前 P3 terminal record；以及
- 两个 P1 的 `date_cutoff` 相同。

在已有两个 P1 terminal 的前提下，其他未满足项会如实输出 `PARKED` 和原因码；缺少 P1
terminal 则不伪造状态记录，直接失败关闭且不会创建 Issue 或调用 AI。每天仍保留 35 天的
脱敏 readiness artifact，方便控制台或 AIAuditBridge 后续只读消费。

当且仅当出现 `AI_RESEARCH_PROPOSAL_READY` 时，工作流会在本仓库创建或更新一条
去重的 AI 研究任务。该任务只是“可以提出组合研究假设”的信号：它不选择权重、不冻结
P2、不封存共同 P1 root、不运行 P3、更不授予 P4 paper、P5 shadow、P6 live、账户或订单
权限。真实组合仍需独立冻结候选、共同历史 P1 输入、成本/风险政策和 P3 evidence。
