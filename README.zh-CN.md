# UsEquitySnapshotPipelines

[English README](README.md)

> 投资有风险。本项目不构成投资建议，仅用于学习、研究和工程审阅。

## 这个仓库是什么

UsEquitySnapshotPipelines 是 QuantStrategyLab 的美股 snapshot 与证据流水线。为美股策略生成 feature snapshot、回测摘要、ranking 和提升证据。

这是一个产出证据的仓库，不直接下单，也不应该被当作执行平台。

## 策略和证据边界

### 普通 runtime 策略

普通 runtime 策略代码，例如 TQQQ/SOXL profile 和 Smart DCA，实现在 UsEquityStrategies；本仓库负责需要受控上游数据的 snapshot artifact、universe audit 和候选评审证据。Global ETF Rotation 现在 runtime 侧基于 feature snapshot，ranking pool 治理由本仓库 snapshot 侧管理。

### 本仓库处理的 Snapshot-backed 工作

- Russell Top50 Leader Rotation
- Global ETF Rotation feature snapshot 和 universe audit 产物
- Tech/Communication Pullback Enhancement 研究产物
- 候选策略 ranking 和 replacement-review 研究输出

### 下游如何使用

UsEquityStrategies 和美股执行平台只消费已经提升的产物和 runtime-enabled profile。

## 这些产物用来做什么

Snapshot artifact 的作用是让策略判断可复现：包括 ranking 输入、feature snapshot、manifest、validation summary 和提升证据。它们不是宣传式收益承诺。下游仓库提升 profile 前，应在适用场景下检查最新短、中、长周期产物。

## 仓库结构

- `src/`：库代码和运行时代码。
- `tests/`：单元测试、契约测试和回归测试。
- `docs/`：运行手册、设计说明、证据和集成契约。
- `.github/workflows/`：CI、定时任务、发布或部署 workflow。
- `scripts/`：运维脚本和本地辅助工具。

## 快速开始

```bash
python -m pip install -e .
python -m pytest -q
```

## 延伸文档

- [`docs/artifact_contract.md`](docs/artifact_contract.md)
- [`docs/crisis-context-research-v2.md`](docs/crisis-context-research-v2.md)
- [`docs/crisis-response-live-promotion-spec.md`](docs/crisis-response-live-promotion-spec.md)
- [`docs/crisis-response-research-roadmap.md`](docs/crisis-response-research-roadmap.md)
- [`docs/crisis-response-v1.md`](docs/crisis-response-v1.md)
- [`docs/global-etf-rotation-snapshot-management.md`](docs/global-etf-rotation-snapshot-management.md)
- [`docs/leveraged-strategy-candidate-research.md`](docs/leveraged-strategy-candidate-research.md)
- [`docs/live-strategy-optimization-feedback-20260603.md`](docs/live-strategy-optimization-feedback-20260603.md)
- [`docs/mega-cap-leader-rotation-dynamic-validation.md`](docs/mega-cap-leader-rotation-dynamic-validation.md)

## 安全和贡献说明

- 除非产物明确设计为公开且已有文档说明，否则不要把生成数据、凭据或私人账户信息提交到 Git。
- 优先提供可复现命令，并显式指定输出目录。
- 没有完整验证证据时，不要把研究产物提升到 live 使用。

## 社区和安全

- 贡献前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)，确认 PR 范围、本地校验和文档要求。
- 讨论、issue 和 review 请遵守 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
- 涉及密钥、自动化、券商/交易所或云资源的漏洞请按 [SECURITY.md](SECURITY.md) 私密报告；不要为 secret 或实盘风险开公开 issue。

## 许可证

详见 [LICENSE](LICENSE)。
