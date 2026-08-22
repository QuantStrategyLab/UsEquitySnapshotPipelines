# TQQQ P2 v5 Daily Research Contract

`tqqq_core_only_p2_v5` is a frozen, research-only candidate for the daily
P1-to-P3 lane.  It does not alter the TQQQ strategy rule, source revision,
runtime configuration, tradable universe, costs, or parameter values used by
P2 v4.

The only new variable is the completed `date_cutoff` in a verified P1 binding.
Each immutable daily root carries that date, its binding digest, the exact four
canonical Alpaca-SIP symbol payloads, and a manifest.  P3 derives its OOS
window as the final 252 XNYS sessions ending at that bound cutoff.  The three
historical evidence folds remain fixed and no daily outcome may change them.

This candidate is deliberately bounded as follows:

- P1 and P3 may acquire observed bars, assess input health, publish one
  immutable private research root, and run an offline no-order replay.
- A missing or malformed input produces `DEFERRED` or `QUARANTINED`; it does
  not substitute a provider, fill a gap, change a strategy parameter, or
  invalidate prior roots.
- P4 paper, P5 shadow, broker orders, capital allocation, and P6 live remain
  outside this daily-controller contract. This says nothing about deployments
  of the same frozen strategy on other platforms: each strategy-version,
  platform, and execution-lane tuple has its own evidence and authority. P6 is
  explicitly user-activated.

The initial P2/P3 foundation was deliberately pure and synthetic. The current
main branch now adds the separately reviewed daily controller described below;
the candidate itself remains frozen and research-only.

## Daily controller

The controller is `.github/workflows/tqqq-p1-p3-daily-research.yml`.  It runs
at 02:35 UTC from Tuesday through Saturday, after the preceding XNYS weekday
close.  At runtime it derives the latest completed XNYS session rather than
assuming that every calendar day is tradable.

The frozen P2 v5 candidate itself is the checked-in personal automation
policy: its canonical digest is carried into P3 as the no-order research
receipt.  There is no per-run mandate, reviewer, paper, shadow, order, or live
activation step.  A failure to acquire or validate input produces a visible
`DEFERRED` or `QUARANTINED` status and stops the P3 branch for that day.

P1 acquisition never changes its source, parameter, cutoff, or input.  The
fixed Alpaca SIP transport makes one narrowly bounded availability recovery:
when its first request receives HTTP `403`, it waits 60 seconds and submits
that exact same request once more.  A second `403`, or a first failure of any
other class, keeps the existing sanitized `DEFERRED` outcome; it does not
switch provider, search for an alternate input, or repeat indefinitely.

Each P1 terminal record now also carries one sanitized provider-retry state:
`NOT_TRIGGERED`, `SIP_403_RECOVERED`, or `SIP_403_EXHAUSTED`.  The short-lived
P1 Actions artifact retains this state alongside the existing terminal status;
the TQQQ control-plane snapshot renders the same fact in its existing
recommendation text.  This reports transport availability only, not strategy
performance, P3 evidence, or P4--P6 authority.

The only later recovery candidate is narrower: if the exact accepted root
already reached P3 and its sanitized terminal state is
`runtime_internal_failure` after replay started, the controller may plan one
additional offline replay of that same immutable root while the short-term
store still contains it. A create-only recovery record consumes that one
attempt whether it completes or parks. Input, configuration, evidence, and
contract failures are not retried automatically; they remain `PARKED` for
diagnosis. This recovery is P3 research only and does not create a delayed P5
forward observation, paper/shadow action, or live authority.

For an accepted run, the four-bar input root, daily health record, and a
sanitized P3 terminal-status record are create-only objects under the same
short-term private snapshot prefix.  When P3 has complete evidence, that same
P3 runner also produces one digest-bound, sanitized forward observation in a
separate `forward-observations` child prefix keyed by its completed session.
It contains only the frozen candidate identity, evidence digests, the
next-session virtual allocation, and its own digest; it contains no bars,
credentials, account data, order, or capital instruction.  This separation
lets a later P5 identity read the exact observation without access to the raw
P1-root prefix.  The publication is a P3-derived input for a later
independently-authorized P5 scheduler, not P5 execution or permission.  No raw
bars are copied into GitHub artifacts and the controller does not extend the
configured retention period.

After the create-only P3 status write succeeds, the same sanitized terminal
record is also retained as a short-lived Actions artifact. It contains only
the candidate identity, immutable input/config digests, date cutoff, and P3
terminal outcome; it is not a P1 root, raw bars, credentials, a retry trigger,
or P4--P6 authority.

## 全局控制台来源快照

P1/P3 结束后，同一 scheduled workflow 会生成一份
`qsl_control_plane_source_snapshot.v1` 并提交给 Settings Worker。它只包含：

- 固定来源 ID `uesp.tqqq_daily_research` 与 workflow revision；
- 单个候选的 P1/P3 阶段、脱敏状态和数据新鲜度；
- P1 manifest、冻结 P2 config、P3 evidence 的 digest（如已产生）。
- P1 provider 的脱敏有界 403 重试结果，作为既有 recommendation 文本的一部分。

不会发送 bars、GCS 路径、Alpaca 凭证、账户、订单、资金或 P4–P6 权限。
`DEFERRED`、`QUARANTINED`、`PARKED` 也会如实发布。P1 的延期会附带闭合、
脱敏的原因码：一般输入缺失为 `input_unavailable`，数据覆盖不足为
`missing_sessions`，Alpaca 的限流、服务/网络不可用分别为
`alpaca_rate_limited`、`alpaca_service_unavailable`、
`alpaca_transport_unavailable`。鉴权/套餐或请求被拒绝会产生
`alpaca_auth_or_entitlement` 或 `alpaca_request_rejected`，控制台应提示检查
账户或请求配置，而不是把它当作无休止的自动重试。隔离输入只记录
`p1_contract_failure`。这些码不会暴露 provider 原始报错、bars、路径或凭据。发布只使用此 Environment 的
`CONTROL_PLANE_SYNC_TOKEN` 与 `QSL_CONTROL_PLANE_SYNC_URL`，两者缺失时该
发布 job 失败可见，但不会回写、重跑或篡改已经完成的 P1/P3 数据动作。
