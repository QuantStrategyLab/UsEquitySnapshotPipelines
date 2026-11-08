# SOXL 固定历史探索候选

该路径是独立的历史开发探索。独立来源尚未证明 SOXL、SOXX、BOXX 全窗口正反拆股全集；可能存在未观察事件，进而改变价格调整、份额、净值和结论。结果必须标识 `EXPLORATORY`、`independent_full_split_coverage=UNPROVEN`、`eligible_for_promotion=false`。归档不证明严格 PIT，研究默认配置不代表实际生产配置。探索结果不适合作生产采用或前瞻结论。

`exploratory.run_local_status` 仅接受调用方注入的精确读取器，返回内存聚合。`protected_exploratory.py` 和独立手动工作流是待审查的受保护候选，限定首次 attempt、main、精确 SHA、原身份及固定目的地。计划需要 `rs04.soxl.exploratory-plan.v1`、固定 manifest 和明确的 `UNPROVEN` 标记，冻结配置和发行方事件表必须字节匹配，失败先于读取。归档 37 笔现金和一笔 SOXX 拆分仍须与发行方逐项一致，包括 BOXX 经单事件确认的 USD 0.290590；归档事件、支付日、比例、完整分页、原始价格、交易日历、现金和费用原有检查均保留。真实事件不匹配、字段缺失、拆股导致碎股而无现金补偿均停止。现金及费率和已知拆股的份额与价格口径由合成账本验证；未知拆股无法由价格连续性本身证明。

使用工作流固定并校验的 UES、补丁树和 QPK，Python 3.11 与锁定依赖；SOXL 总名义上限 0.97、单腿 0.679，BOXX 为现金替代资产。开发自 2023-03-28，主窗 2024-04-05 至 2026-07-10，尾窗至 2026-08-25；全部 21 情景，包括注册敏感性，D10 单边主档 7 bps、报告档 0 和 15 bps，费用覆盖 BOXX，保留失败状态。主比较为 B1.4−B1.2 和 B2.2−B2.1。不得改窗、调参或筛掉失败。

## 后续具体审批范围与准备条件

拟复用 HIST-01 固定登记与代次、原 `market-data-nonlive` 环境及 WIF，仅读 registration、manifest 和 SOXL／SOXX／BOXX 的 actions 与 bars 各一页：8 个对象，每个至多一次元数据与一次内容读取，共至多 16 次对象请求，关闭重试。身份需对这 8 个精确对象具备 `storage.objects.get`。身份、代次、大小、摘要、页码、事件、价格、日历、参数或成本不符以及权限拒绝均停止，不切换数据源或重跑。真实数据只在受保护临时目录处理，不记录或输出原始页、逐日账与订单。

拟在原受限桶创建独立对象 `research/v2/soxl-exploratory/rs04e-b1b2-v1/result.json`，仅允许 `storage.objects.create`，条件 `ifGenerationMatch=0`，无覆盖和重试；指定接收者仅需对该对象 `storage.objects.get`，不授予列表、覆盖或删除。结果拟保存七个自然日，精确代次清理与桶软删除、实际保留政策和接收者权限需事先确认并单独授权；目标不存在性及私有性尚未核实。写入结果未知时停止，由获准接收者核对对象，不重复运行。

候选已具备独立的本地受保护入口及手动工作流；仍未绑定最终源提交、计划或身份，亦未核实读取、创建或接收权限。计划必须在受保护位置绑定 `rs04.soxl.exploratory-plan.v1`、`independent_full_split_coverage=UNPROVEN`、固定 manifest、最终 `source_sha`、完整原 `archive_binding` 和上述唯一 `result_uri`，以独立受保护计划及其 SHA-256 原样提供。新增计划 secret 名称为 `RS04_SOXL_EXPLORATORY_PLAN` 和 `RS04_SOXL_EXPLORATORY_PLAN_SHA256`；候选不创建或修改 secret。后续须按仓库规则完成完整差异严格扫描、精确处置、审查、集成，绑定最终 SHA，核实对象权限与私有保留，再请求对应具体写入和一次运行授权；此前不执行读取、回放、secret/IAM 变更或写入。先前两次对账的一次性授权已消耗，不能作为本次运行授权。
