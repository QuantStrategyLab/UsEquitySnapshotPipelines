# 操作运行手册

[English](operator_runbook.md)

本仓库是 snapshot-backed 美股策略的上游 artifact 生产仓库。券商平台仓库仍然只是下游消费者。

## 本仓库生产的 Snapshot Profiles

当前唯一面向运行时发布的 snapshot profile 是：

- `russell_top50_leader_rotation`

`russell_1000_multi_factor_defensive` 因长期只小幅跑赢 SPY、回撤优势不足，已从本仓库运行时契约中退役。`tech_communication_pullback_enhancement` 以及已退役的 Russell Top50 dynamic/leveraged 研究变体均保留为归档研究，不再出现在 publish 或 health workflow。

## 本地手动构建

Russell Top50 leader rotation：

```bash
PYTHONPATH=src:../UsEquityStrategies/src:../QuantPlatformKit/src \
python scripts/build_russell_top50_leader_rotation_snapshot.py \
  --prices /path/to/r1000_price_history.csv \
  --universe /path/to/r1000_latest_holdings_snapshot.csv \
  --as-of 2026-04-01 \
  --dynamic-universe-size 50 \
  --output-dir data/output/russell_top50_leader_rotation
```

## 手动 GitHub Actions 发布

使用 `Publish Snapshot Artifacts` workflow。

必填输入：

- `profile`，当前只支持 `russell_top50_leader_rotation`

生产数据路径需要同时设置：

- `prices_path`
- `universe_path`

常用可选输入：

- `as_of_date`
- `artifact_dir`
- `gcs_prefix`
- `current_holdings`
- `portfolio_total_equity`
- `min_adv20_usd`，用于 Russell Top50 测试覆盖

策略插件发布 workflow 在 `execute_publish=true` 时只接受位于
`gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/<scope>/plugins/<plugin>`
之下的手工 GCS prefix override。

`Publish Strategy Plugins` 会同时构建 strategy artifact 和统一
`notification_targets.market_regime_notification` artifact。人工复核插件 bot
只应消费这个统一 notification target；TQQQ、SOXL 等 strategy artifact 仍供策略
runtime 自动消费，并在实际仓位变化时由策略运行通知承载。

统一通知使用 `STRATEGY_PLUGIN_ALERT_*` vars/secrets，默认中文
`STRATEGY_PLUGIN_ALERT_LANG=zh`，并通过
`STRATEGY_PLUGIN_ALERT_STATE_GCS_URI` 做跨 run 去重。未配置投递凭据时 workflow
会写出 skipped 诊断，不会影响 artifact 发布。

workflow 每次都会把生成文件上传为 GitHub Actions artifact。

## 定时发布

`Update Source Input Data` 每月 1 日 `00:15 UTC` 自动运行，也就是 Asia/Shanghai 同日 `08:15`。它刷新月度 snapshot profile 使用的 Russell 1000 输入数据：

```text
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_history.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_symbol_aliases.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_universe_snapshot_metadata.csv
gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
```

`Publish Snapshot Artifacts` 会在 source-input refresh 后构建：

```text
profiles=russell_top50_leader_rotation
prices_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_price_history.csv
universe_path=gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/inputs/r1000_official_monthly_v2_alias/r1000_latest_holdings_snapshot.csv
execute_publish=true
```

默认定时输出前缀：

| Profile | Extra config | GCS prefix |
| --- | --- | --- |
| `russell_top50_leader_rotation` | none | `gs://qsl-runtime-logs-shared/strategy-artifacts/us_equity/russell_top50_leader_rotation_staging` |

workflow 仍保留月末交易日 guard：如果解析得到的 `snapshot_as_of` 不是该月份最后一个 NYSE 交易日，会写出 skip artifact，并且不会发布到 GCS。

## 月度 AI Review

定时 `Publish Snapshot Artifacts` 成功后，`Monthly Snapshot Review` 会下载该 run 的 artifacts，并组装：

```text
data/output/monthly_report_bundle/monthly_report_bundle.json
data/output/monthly_report_bundle/ai_review_input.md
data/output/monthly_report_bundle/job_summary.md
```

workflow 会创建或更新一个带 `monthly-review` label 的 GitHub issue。默认会 dispatch `QuantStrategyLab/AIAuditBridge`，由该桥接 workflow 调用 Quant HTTPS/443 服务端 Codex 路径，并把 audit 结果回写到 issue。review 重点包括：

- 预期 snapshot profile 的 artifact 完整性；
- contract version、snapshot date、row count 和 ranking-preview 健康度；
- 会降低下游信心的缺失或陈旧证据；
- 对券商/runtime 下游仓库的影响。

`Monthly Snapshot Review` 通过 `CODEX_AUDIT_PROVIDER` 控制 reviewer provider。默认 `auto` 会优先调用 service-backed Codex，失败时回退到已配置的 API reviewer；没有 API fallback key 时会显式失败。可通过 `CODEX_AUDIT_BRIDGE_REF` pin 桥接 workflow ref，默认是 `main`。

桥接 PR 默认不自动合并。只有确认 branch protection 或 repository rulesets、CI、源仓库 `Auto Merge Codex Remediation PR` guard 以及 `Codex PR Feedback` 重试 workflow 后，才应设置 `CODEX_AUDIT_AUTO_MERGE=true`。当请求受控自动合并时，`Monthly Snapshot Review` 会先运行 `scripts/sync_codex_auto_merge_labels.py`，在缺失时创建配置的 `auto-merge-ok` / `human-review-required` label。真正打开变量前，应先运行 `scripts/plan_codex_auto_merge_enablement.py`，并逐项完成其中的 enablement preflight checklist，包括复核现有 branch protection 或 ruleset 设置，避免覆盖已有保护规则。源仓库 auto-merge workflow 自身也受 `CODEX_AUDIT_AUTO_MERGE` 硬开关控制；变量未设置或为 false 时，即使 PR 已带 `auto-merge-ok` 也会跳过。必需 CI check 默认是 `test`；如果 branch protection 使用不同 check 名称，应在启用前把仓库变量 `CODEX_AUDIT_REQUIRED_STATUS_CHECKS` 设置为逗号或换行分隔的列表。启用前可先运行 `scripts/plan_codex_auto_merge_enablement.py` 生成只读启用计划和 branch protection 命令；该脚本不会修改远端设置。`Monthly Snapshot Review` 会在 dispatch CodexAuditBridge 前运行 `scripts/check_codex_auto_merge_readiness.py`；如果 auto-merge 已开启但本地 guard、反馈重试 workflow、远端 label、protected branch/ruleset 或必需 CI status check 任一条件缺失，本次 dispatch 会自动降级为 `auto_merge=false`，继续执行 AI 审计和 PR 创建，但不会请求受控自动合并。如果部署仓库中的默认 workflow token 不能读取 branch protection、rulesets 或 label 设置，可配置源仓库 secret `CODEX_AUDIT_READINESS_TOKEN`，仅供 readiness 检查使用。`auto-merge-ok` label 默认由 CodexAuditBridge 在低/中风险 PR 打标前按需创建；如果 bridge token 权限不足，需要先手动创建该 label。无人值守路径应为：Codex 创建小范围修复 PR，CI 通过，PR 带 monthly remediation marker 和 `auto-merge-ok`，然后源仓库 merge guard 会确认当前没有 requested-changes review decision，再根据变更文件面、风险等级和 `max_changed_files`（默认 `20`）和 `max_changed_lines`（默认 `1200`）决定是否合并。auto-merge workflow 会跳过 run head SHA 已经不是当前 PR head 的过期成功 CI，最终 merge 也会绑定 CI 的 `workflow_run.head_sha`；同一 PR 分支如果之后又有新提交，必须重新通过 CI 才能合并。策略逻辑、live profile contract、依赖、密钥、broker/runtime 设置、live allocation、数据 artifact、策略删除或禁用等高风险变更必须保留人工复核；CodexAuditBridge 会给这类生成 PR 添加 `human-review-required`，并在源 issue 中记录风险文件。
月度 review bundle artifact 在 audit path 启用时也会包含 `codex_auto_merge_label_sync.md`、`codex_auto_merge_readiness.md`、`codex_auto_merge_enablement_plan.md` 以及渲染后的 `codex_auto_merge_preflight_comment.md`，workflow 还会在月度 review issue 上 upsert 这条简短的 guarded auto-merge preflight 评论，并包含 label sync、readiness 与 enablement checklist 摘要，用于留存本次受控自动合并 dispatch 决策前的 preflight 状态。

如果同仓 Codex remediation PR 的 CI 失败，或 reviewer 要求修改，`Codex PR Feedback` 会先跳过 run head SHA 已经不是当前 PR head 的过期 CI 失败事件。对当前失败或 requested changes，它会复用 readiness 的策略 label 校验，确认 policy 可读且 auto-merge / human-review label 不冲突后，再尽力移除该 PR 上旧的受控自动合并 label；如果 policy 无效或两个 label 冲突，则跳过 label mutation，避免误删人工复核 label。随后它会把失败或 review 摘要评论回源 `codex-bridge` issue，并再次 dispatch `CodexAuditBridge` 处理同一个 issue。反馈重试会运行同一套受控 auto-merge readiness 检查；如果 readiness 不完整，重试仍会执行，但传入 `auto_merge=false`。Bridge 会读取最新 feedback marker；只要原 PR 仍然 open、同仓、且属于同一个 monthly issue，就更新现有 PR 分支。workflow 允许 `CODEX_AUDIT_MAX_FEEDBACK_ROUNDS` 轮自动反馈，默认 `3` 轮；无效值会回退到 `3`，并被限制在 `1` 到 `10` 之间。达到上限后会移除 `codex-bridge`，并尽力先创建缺失的人工复核 label、再给 PR 加上该 label，然后交给人工处理。反馈 workflow 即使后续步骤失败，也会上传 `codex-pr-feedback-ci-<run_id>` 或 `codex-pr-feedback-review-<run_id>` 诊断 artifact；手动重跑前应先检查其中的 `comment.md`、`comment_pages.json`、`pr.json` 和 `codex_auto_merge_readiness.md`。

启用受控自动合并后，`Auto Merge Codex Remediation PR` 每次运行都会上传 `codex-auto-merge-<run_id>` 诊断 artifact。可用其中的 `decision.json`、`summary.md`、`pr.json`、`guard_decision_comment.md` 和 `readiness.md` 判断 PR 为什么被合并、跳过、readiness 失败或被 merge guard 阻止。如果 merge guard 跳过 PR，或 merge guard 已就绪但最终 merge-time readiness 检查失败，workflow 还会在月度 review issue 上 upsert 一条简短决策评论，让高风险或需人工复核的情况不必先打开 artifact 也能看到；同时会尽力从被跳过的 PR 上移除陈旧的受控自动合并 label，并在高风险决策时补上人工复核 label。决策评论会优先写入；label hygiene action 结果随后会 best-effort 追加到同一条评论和 artifact 中。如果后续评论更新失败，artifact 会记录该失败，避免权限问题掩盖无人值守合并停止的真实原因。

风险分层和无人值守维护边界见 [`snapshot-ai-audit-automation.md`](snapshot-ai-audit-automation.md)。
