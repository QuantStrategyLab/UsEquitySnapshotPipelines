# 免费日线来源交叉校验

SOXL、SOXX、BOXX 的免费日线影子校验使用两份独立获取路径：Twelve Data 的有密钥日线接口，以及项目已存在的 Yahoo Finance chart 下载通道。两者都先产生独立、不可变的日线快照，再交给 `quant_platform_kit` 的多源一致性关卡比较。

诊断只输出状态、哈希和受限的交易日覆盖证据（预计/实际数量及最多三个缺失或多余日期样本），不会输出 OHLCV、上游响应、请求 URL 或密钥。任一来源不可用、交易日覆盖不同或 OHLCV 不一致时，结论均为不可发布；这不是自动降级到单源。

这不是自动切换或“免费数据兜底”机制：任何一个来源不可用、复权口径不同、交易日覆盖不同，或 OHLCV 超出允许偏差时，结果都会是 `NOT_VERIFIED`，不得写入 P1、不得触发回测/策略发布，也不得改变任何券商仓位。

Twelve Data 接口的结束日是排他边界；适配器会向后请求一个日历日以覆盖指定的已完成交易日，但仍会拒绝任何晚于 `date_cutoff` 的返回行。因此边界修复不会把未完成交易日纳入快照。

Yahoo Finance 不需要新密钥，但其公开图表通道不应被视为正式交易行情 API；本系统只将其用于独立交叉校验。Twelve Data 的 Basic 账户提供每日受限额度，密钥仅保存为通用非交易行情环境 `market-data-nonlive` 的 `TWELVE_DATA_API_KEY` secret，绝不放入代码、日志或聊天。

配置密钥后，从 Actions 手动运行 **Multi-source Daily Assurance Diagnostic**，输入最近一个已收盘的 XNYS 交易日。工作流只输出来源状态、原因码与哈希；不保存原始行情、不上传制品、不使用云权限，也不访问券商。

只有在三只标的都返回 `MULTISOURCE_DAILY_ASSURANCE_VERIFIED` 后，才可以另行审查是否把这份数据证据接入正式 P1。这个 PR 不做该接入。
