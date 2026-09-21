# R3-v2 免费双源 price-only 保障诊断

R3-v2 price-only 是本仓内**独立于全量 OHLCV 合同**的研究诊断切片。它仍对 `QQQ`、`TQQQ`、`SOXX`、`SOXL` 做 Twelve Data 与 Yahoo Finance 的 split-adjusted 日线比对，但只消费并比较 open/high/low/close，并用完整 XNYS 已完成交易日做覆盖校验。

源载荷里可以仍带 volume，但本合同显式标记 `volume_not_consumed=true`：volume **不参与**比较结果、不参与策略信号，也不会单独因为 volume 差异而放行或封锁价格分歧。价格字段若不一致，结果仍为 `NOT_VERIFIED` / `DEGRADED`。

它与下列路径分离，且不得复用其结果作为研究输入：

- R3-v1（UsEquityStrategies 私有联合证据 / 锁定输入）
- 现有 R3-v2 full OHLCV 合同（`qsl.r3_v2_free_source_assurance_diagnostic.v1`）

本切片使用独立 `contract_id=r3_v2_price_only` 与 schema `qsl.r3_v2_price_only_assurance_diagnostic.v1`。无论诊断为 `VERIFIED`、`PARKED` 还是 `NOT_VERIFIED`，都固定 `can_promote=false`、`auto_promote=false`：不会自动晋级、不会发布研究输入、不会改写既有 TQQQ/SOXL 候选，也不会授予 paper/live 权限。

手工入口：工作流 `r3-v2-price-only-assurance-diagnostic.yml`（`contents: read`、`market-data-nonlive`）。无云端、无 id-token、无 artifact 上传、无券商或下单权限。

## 合同边界

| 项 | 规则 |
| --- | --- |
| 调整基准 | split-adjusted |
| 比较字段 | open / high / low / close |
| volume | 可保留在源数据中，但 `volume_not_consumed`；`compare_volume=false` |
| 覆盖 | 完整 XNYS 已完成交易日；cutoff 必须是已完成 session |
| 来源 | Twelve Data + Yahoo Finance 双源 READY；不可用或缺覆盖 fail-closed |
| 禁止 | 自动换源、混源、平均价格、放宽 full OHLCV 门槛、复用 full-OHLCV 结果身份 |

这是研究诊断，不是晋级路径，也不是实盘恢复路径。
