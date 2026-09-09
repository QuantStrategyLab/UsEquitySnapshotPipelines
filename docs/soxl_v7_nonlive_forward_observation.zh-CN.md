# SOXL V7 自动前瞻观察

`SOXL V7 Non-Live Forward Observation` 在每个已完整结束的 XNYS 交易日后运行，针对冻结候选
`soxl_soxx_core_only_p2_v7_longterm_compounding_cash_reserve` 生成一条 create-only 的 P4 收据。
它同时运行候选 Shadow 决策和 10 bps 的**模拟** Paper 回放；两者都不连接券商、不修改平台运行目标，也不提交订单。

## 自动处理范围

工作流会从私有 P1 根构建脱敏输入，固定绑定 P1/P2/P3、风险政策、策略发布版本和前一条收据摘要。数据不完整、前序收据不可读、身份不匹配或风险/控制状态异常时，工作流输出 `PARKED` 或暂停记录，不能绕过条件继续。

收据的观察序号必须严格按 XNYS 交易日递增。若当天免费数据源暂时未就绪，工作流不会把较新的日期写进链中；后续运行会先选择**最早缺失的交易日**回补，一次只创建一条收据。这样数据恢复后会自动恢复连续观察，而不会因跳号把整个 252-session 窗口永久卡住。

当 P1 因免费双源未能验证而停车时，工作流会在摘要中自动写入脱敏原因码和处理方式。例如 `YAHOO_SETTLEMENT_LAG` 表示 Twelve Data 已覆盖完成日、Yahoo 仅缺同一最新收盘日；它仍不能单源发布，但会标记 `RETRY_NEXT_SCHEDULED_SESSION`。其他提供方、覆盖或价格一致性问题保持 `PARK_AND_DIAGNOSE`，不会自动换源、放宽容差或补造数据。

## 网站只读投影

每条已存在的 V7 record 会投影为独立的 `qsl_control_plane_source_snapshot.v1` 来源 `uesp-soxl-v7-nonlive-forward`，再通过既有 control-plane sync 接口展示。投影先验证 record 自身 SHA、冻结 candidate/config/P4 policy、固定 P3 与依赖摘要、252-session controller、收据 index、XNYS 会话列表，以及 `no_order=true`、无 broker 依赖、无 Live 权限。它只使用已签入 record 的 controller 状态，不用健康字符串补造 P3 或 paired 结果。

`FORWARD_ACTIVE` 显示为 P4 Shadow 观察并建议继续自动 Shadow 评估；暂停、停车或阻断状态显示为 P4 parked；完成 252 个会话后显示为 P4 evidence pending 并继续研究，不进入 P6。`computed_at`、`forward_observation.observed_at` 和最后观察日都保留原 record 时间；新 `generated_at` 只表示投影时间，freshness age 按原观察时间计算，网站仍会按既有交易日交付规则判定 stale。

人工重发网站投影时，使用 workflow dispatch 的 `date_cutoff` 和显式 `publish_only=true`。该模式只读取对应 GCS 路径下已经存在的 immutable record；缺记录会以 `PUBLISH_ONLY_RECORD_MISSING` 停止，不会回退到 P1 采集、Shadow 或模拟 Paper runner。普通自然周期在 create-only record 成功后也会执行相同投影。上传只发起一次 `urllib` POST，token 仅在环境变量和 Authorization header 中使用；结果不明时不自动重试。record 已先持久化，因此展示同步失败不会要求重跑观察。

## 明确不做的事

这条自动化只是 P4 的证据收集与安全停车/恢复机制：

- 不是券商 Paper 账户，也不启用任何平台的 `runtime target`；
- 不是自动策略推广，不能把候选变成实盘；
- 252 个交易日完成后仍会停在 `FORWARD_COMPLETE_HUMAN_REVIEW`；首次实盘、重启实盘、资金扩大或参数修改都需要独立的人为批准与新的验证。

因此，它能够无人值守地补齐和保护前瞻证据，但不会扩大交易权限。
