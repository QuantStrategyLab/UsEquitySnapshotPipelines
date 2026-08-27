# TQQQ 免费 OHLCV 双源校准边界

TQQQ 的免费 P1 固定使用 Twelve Data 为 canonical、Yahoo Finance 为 verifier。两者都成功返回数据，但其 OHLC 字段可能存在微小差异；这与“服务不可用、密钥缺失或覆盖不足”不同。

当 P1 无法验证时，终态会区分两种不改变策略的原因：

- `FREE_SOURCE_UNAVAILABLE`：至少一个必需来源不可用、覆盖不足，或诊断不足以确定为价格分歧；
- `FREE_SOURCE_DISAGREEMENT`：两源均为 `READY`，但至少一个标的出现 `daily_bar_price_divergence`。

停放状态的 `availability_diagnostic.price_agreement.field_delta_bps` 只会输出每个 OHLC 字段的比较交易日数、超出现有容差的天数、以及以 bps 表示的 p50/p95/p99/max 最近秩差异。它不含任意原始价格、K 线行、URL、来源响应、路径或凭据。

这些统计是**独立数据质量校准的输入，不是自动放宽规则**。若要创建一个新的候选数据合约，必须先在与策略 P1/P2/P3 分离的校准记录中预登记：样本区间、字段级容差方法、硬上限、异常/缺失处理、双源均为 READY 的要求以及不通过时的停放规则。新合约不得复用旧候选的 P3 结果，也不得改变已冻结候选、策略参数、运行时、平台或订单。
