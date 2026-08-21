# 日更研究调度看门狗（仅控制面）

此看门狗检查两个既有、无人值守的研究工作流是否在预定 UTC 日期真正产生了一次**成功结束**的 `schedule` run：

- `TQQQ Daily P1-P3 Research`
- `SOXL Daily P1-P3 Research`

它在每个周二至周六 UTC `04:20` 运行，给前两条工作流留出延迟窗口。它仅通过 GitHub Actions 的只读元数据判断下列控制面状态：

- `OBSERVED / SCHEDULED_RUN_SUCCEEDED`：当天确有成功结束的定时工作流；
- `PARKED / SCHEDULED_RUN_MISSING`：当天没有定时 run；
- `PARKED / SCHEDULED_RUN_NOT_TERMINAL`：到检查窗口仍未结束；
- `PARKED / SCHEDULED_RUN_NOT_SUCCESSFUL`：定时 run 已结束但 Actions 失败或取消；
- `PARKED / WATCHDOG_INPUT_INVALID`：元数据不能被安全解析。

这不是行情、策略或绩效判断。特别是，`OBSERVED` 不表示 P1 数据已接受、P3 已成功、历史回放通过，或任何策略可进入 P4/P5/P6。P1 的 `DEFERRED`、P3 的 `PARKED` 仍由各自工作流的脱敏终态工件决定，正常的 fail-closed `DEFERRED` 不应被看门狗误报为“未执行”。

看门狗没有 `workflow_dispatch`、不调用 `workflow run`，不读取 Alpaca/GCP/券商/账户凭证，不创建 issue、不重试，也不改变候选、配置、仓位或交易权限。它的失败是供控制台或通知层消费的一个稳定、低频调度异常，而不是让 AI 自动修复或重新运行研究的许可。
