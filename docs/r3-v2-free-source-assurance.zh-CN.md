# R3-v2 免费双源全量 OHLCV 保障诊断

R3-v2 是本仓内独立的研究诊断切片，只对 `QQQ`、`TQQQ`、`SOXX`、`SOXL` 做 Twelve Data 与 Yahoo Finance 的 split-adjusted 日线比对。它要求 open/high/low/close 与 volume 全部一致，并用 XNYS 已完成交易日做覆盖校验；结果仅为脱敏诊断，不是研究输入发布身份。

它与 R3-v1（UsEquityStrategies 的私有联合证据 / 锁定输入路径）分离：不复用、不替代、不自动对接 R3-v1。无论诊断为 `VERIFIED`、`PARKED` 还是 `NOT_VERIFIED`，都不会自动晋级、不会改写既有 TQQQ/SOXL 候选，也不会授予 paper/live 权限。

手工入口：工作流 `r3-v2-free-source-assurance-diagnostic.yml`（`contents: read`、`market-data-nonlive`）。无云端、无 id-token、无 artifact 上传、无券商或下单权限。

## 当前数据源角色

| 场景 | 当前角色 | 规则 |
| --- | --- | --- |
| 生产交易 | 各平台自己的券商/交易所行情与账户接口 | 不接入 Yahoo 或 Twelve Data，不从研究源下单 |
| R3-v1 | 原锁定私有 Yahoo 输入 | 保持原身份并继续 `PARKED`，不与新源混用 |
| R3-v2 | Twelve Data canonical，Yahoo verifier | 两源都 READY、覆盖完整且全量 OHLCV 一致后才可形成研究输入；当前不自动晋级 |
| Alpaca SIP | 未采用，保持停车 | 免费计划不满足当前 SIP 数据要求；不订阅、不参与 fallback 或源切换，除非未来另行批准付费订阅 |
| 其他免费供应商 | 未接入候选 | 不新增密钥、依赖、workflow 或隐式 fallback |

因此“清理”指清理实际使用路径，而不是删除仍被测试、审计或未来候选引用的适配器。任何来源不可用或不一致时都保持 `PARKED/NOT_VERIFIED`，不得自动换源、混源或平均价格。
