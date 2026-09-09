# SOXL 九输入独立研究预检

本入口为新的隔离开发研究读取既有私有输入包。默认模式只做预检；显式 development 模式执行回测，但不执行外部模型、订单或晋级，不消耗旧候选的 mandate，不更新旧包或旧账本。

已有 development 聚合结果可用互斥的 `--completed-result <path> --completed-result-sha256 <sha256>` 离线消费；此模式不需要 `--snapshot`，也不调用预检、runner 或模型。输出因此标记 `real_backtest_executed=false`、`completed_result_reused=true`；原结果中的 true 仍是必须校验的历史执行事实。入口用单次文件读取同时校验预期字节 SHA 和 JSON，拒绝 symlink、非普通文件、空文件及超过 1 MiB 的输入。

实际调用入口是 `scripts/preflight_soxl_isolated_research.py`，由它调用 `lifecycle.soxl_isolated_research.preflight_soxl_isolated_research`。调用方必须提供已批准的原目录、package manifest SHA256 和 input manifest SHA256；不能由入口扫描选择输入或把现场计算的摘要自授为可信根。

```text
python scripts/preflight_soxl_isolated_research.py \
  --snapshot <已批准的原私有快照目录> \
  --package-sha256 <已批准的清单根> \
  --input-manifest-sha256 <已批准的输入根>
```

预检保留原四文件集合及 0700/0600 权限，拒绝 symlink；检查三个成员的字节、长度，使用当前 UTC 时钟检查原来源留存期限。原来源必须仍为 `provider_observed`、原加密本机内部研究保留范围，不接受 synthetic、代理替换或放宽的保留范围。已有纯 `prepare_soxl_pit_input` 在内存复验来源、日历、上市可用性和 prefix provenance，并与原规范字节比较；不调用 `publish_soxl_pit_input`，不落盘任何行情或新回执。原 package/source identity 保留生成时的 UES `15df2a42df5d230cfb03a7cb655fd4b226956681` 与 QPK `730ad9f3983bd90cd75adecb67fcf483ffb96736`，不会重写成新 consumer 版本。

唯一参数为 `trend_entry_buffer`，范围 0.08–0.12，默认 0.08 / 0.10 / 0.12。0.08 基线必须保留且计数；列表严格递增，最多 25 值。新 runtime config 独立来自 consumer UES `33d8c09a9aa517cde94f36d2f67e526c340ea6e9` 与 QPK `5c916917626707c4ee798c6b45a5d43609019816`，这两个运行依赖不会倒灌或替换上述 source lineage。该参数确实参与原策略计算：SOXX 价格高于 140 日趋势均线乘以 `(1 + trend_entry_buffer)` 时进入最高趋势档。它没有修改持仓权重、止损、风险预算或 mandate；提高门槛也不代表已证明收益或风险改善。

Python 返回原 `input_path` / `input_manifest_path`、原来源身份以及互相独立的完整 `runtime_configs`，供后续实际 runner 消费。CLI 只输出状态、计数、Codex 渠道和研究权限标志，不输出路径、来源日期、行情、底层异常或配置全文。所有配置均为 `learning_only`、`promotion_eligible=false`、`live_ready=false`、`size_zero_required=true`、`no_order=true`；配置列表长度限制不替代执行阶段对全部尝试累计最多 25 次的限制。

默认 CLI 仍只做预检。显式增加 `--run-development` 后，入口先完整重做同一输入门，并把第二次读取的 `input.json` 字节绑定到 package 已验证的成员 SHA；文件在两次读取之间被替换时不会进入编排器。随后只使用实际 `BacktestOrchestrator.run` 和无落盘 sink，依次执行三个参数 trial。每个 trial 内固定计算 5 / 10 / 15 bps 三个费用场景；费用不是新增可调参数。整个窗口从原首会话开始，到旧 WFA 最后一段结束为止，明确标记 `seen_development`。完整性与来源验证仍读取原包全文件，但计算和调参不纳入其后的旧 locked OOS。

新 learning adapter 只提供普通 `run`，不实现 promotion runner 的 fold 或 locked-OOS 能力。它复用旧 runner 已验证的 lot、现金、half-L1 费用、open-gap stop、同日 low stop、close 决策和指标计算；逐 prefix 指标只在本次 job 的同一输入内按 index 缓存，交给策略前使用独立副本，不落盘也不暴露未来会话。策略决策使用 pinned UES 的纯 builder，不调用普通入口的 decision record，也不调用旧 promotion authority。0.01 模拟 loss budget、5% 可执行止损、0.50 有效敞口、SOXL 三倍因子与 0.15 名义上限、5% 回撤减半、10% account breaker 和 3 次 stop park 都保持。真实 `RiskEngine.assess` 是额外 sanity check；close 决策会消费其 approve scalar，异常、非有限或大于 1 的 scalar 一律拒绝。开盘止损 assessment 只用当日 open 估值和前一会话 regime；由于共享止损执行无法消费进一步缩减，scalar 小于 1 时 fail closed。它不生成 evidence 或权限。

开发输出只含每个 trial/费用场景的聚合收益、风险、成本、turnover、成交与 assessment 计数，以及固定研究边界。第二个或后续费用场景失败时立即停止后续工作，保留已经完成的聚合结果，并分别报告 trial/scenario 的 started 与 completed 计数。状态仅使用 `development_completed` 或 `development_incomplete`，不能写成 PASS、ready、promotion 或自然 shadow。旧 runner 的 `_validate_config` / `_require_run_contract` 继续严格绑定旧候选，不能覆盖旧身份、消费旧 ledger，或把本开发窗口称为新的独立 OOS。新候选的严格晋级和 forward 证据仍需独立产生。

completed-result 模式只接受完整、安全标志一致且包含真实比较的 trial 结构：参数必须唯一递增且含 0.08 基准，每个参数必须具有固定 5 / 10 / 15 bps 场景，started/completed 计数必须与内容一致，所有聚合值必须为有限数且不能用 bool 冒充数值。若各参数在相同费用下的聚合值完全相同，输出 `status=completed`、`outcome=no_improvement`，并为隔离 learning profile 返回 QPK `OptimizationProposal(recommendation="hold")`；current/proposed 参数都保持 0.08，不挂到原运行策略身份。任何聚合差异只输出 `outcome=requires_review`，仍不提出参数变更且 `promotion_eligible=false`。该 proposal 的 `walk_forward_passed=false`，不把 seen-development 写成 OOS，也不声称历史自动任务、production drift 或 job 已绑定。

本片验证采用生成的 synthetic 行情 fixture、隔离解释器和网络阻断。本子任务不访问真实输入；真实输入由主任务在当前明确授权内执行。真实预检结果、测试命令和日志由本轮交接提供，不能把单元测试当成真实优化成功。
