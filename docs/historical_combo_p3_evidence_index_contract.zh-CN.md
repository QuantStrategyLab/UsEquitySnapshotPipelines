# 历史组合 P3 证据索引契约

`qsl.us-equity-historical-combo-p3-evidence-index.v1` 是未来历史组合回放
完成后可保留的**摘要索引**。它只绑定：候选 ID、P1 输入 SHA-256、P2 冻结候选
SHA-256、完整 P3 证据 SHA-256、结果状态/结论，以及生成它的 GitHub Actions
运行身份。

索引不含原始行情、成分股内容、密钥、订单或资金信息。它强制
`RESEARCH_ONLY`，并固定拒绝 promotion、paper、shadow 和 live 授权。因此即使
结果结论为 `PASS_RESEARCH_EVIDENCE_NOT_PROMOTION`，含义也只是“研究证据完成”，
不是“策略已通过”或“可以执行”。

当前没有组合 P3 runner，也没有 P3 证据。这个索引只为未来 runner 准备可验证的
P1 → P2 → P3 身份链；它本身不读取数据、不运行回测、不写入远端，也不触发工作流。
