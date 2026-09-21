# R3-v2 免费双源全量 OHLCV 保障诊断

R3-v2 是本仓内独立的研究诊断切片，只对 `QQQ`、`TQQQ`、`SOXX`、`SOXL` 做 Twelve Data 与 Yahoo Finance 的 split-adjusted 日线比对。它要求 open/high/low/close 与 volume 全部一致，并用 XNYS 已完成交易日做覆盖校验；结果仅为脱敏诊断，不是研究输入发布身份。

它与 R3-v1（UsEquityStrategies 的私有联合证据 / 锁定输入路径）分离：不复用、不替代、不自动对接 R3-v1。无论诊断为 `VERIFIED`、`PARKED` 还是 `NOT_VERIFIED`，都不会自动晋级、不会改写既有 TQQQ/SOXL 候选，也不会授予 paper/live 权限。

手工入口：工作流 `r3-v2-free-source-assurance-diagnostic.yml`（`contents: read`、`market-data-nonlive`）。无云端、无 id-token、无 artifact 上传、无券商或下单权限。
