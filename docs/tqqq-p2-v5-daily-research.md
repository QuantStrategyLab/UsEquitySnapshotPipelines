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
  outside this contract.  They require separately implemented policies; P6 is
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

## 全局控制台来源快照

P1/P3 结束后，同一 scheduled workflow 会生成一份
`qsl_control_plane_source_snapshot.v1` 并提交给 Settings Worker。它只包含：

- 固定来源 ID `uesp.tqqq_daily_research` 与 workflow revision；
- 单个候选的 P1/P3 阶段、脱敏状态和数据新鲜度；
- P1 manifest、冻结 P2 config、P3 evidence 的 digest（如已产生）。

不会发送 bars、GCS 路径、Alpaca 凭证、账户、订单、资金或 P4–P6 权限。
`DEFERRED`、`QUARANTINED`、`PARKED` 也会如实发布，因此控制台不会将一次
provider 故障误显示为成功。发布只使用此 Environment 的
`CONTROL_PLANE_SYNC_TOKEN` 与 `QSL_CONTROL_PLANE_SYNC_URL`，两者缺失时该
发布 job 失败可见，但不会回写、重跑或篡改已经完成的 P1/P3 数据动作。
