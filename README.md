# UsEquitySnapshotPipelines

[Chinese README](README.zh-CN.md)

> Investing involves risk. This project does not provide investment advice and is for education, research, and engineering review only.

## What this repository is

UsEquitySnapshotPipelines is the QuantStrategyLab US equity snapshot and evidence pipeline. It builds feature snapshots, backtest summaries, rankings, and promotion evidence for US equity strategies.

It is an evidence-producing repository. It does not place trades and should not be treated as an execution platform.

## Strategy and evidence boundary

### Direct runtime strategies

Direct runtime strategy code such as TQQQ/SOXL profiles and Smart DCA lives in UsEquityStrategies; this repository owns snapshot artifacts, universe audits, and candidate-review evidence for strategies that need controlled upstream data. Global ETF Rotation is now feature-snapshot backed at runtime, and its ranking-pool governance is snapshot-managed here.

### Snapshot-backed work handled here

- Russell Top50 Leader Rotation
- Global ETF Rotation feature snapshot and universe audit artifacts
- Tech/Communication Pullback Enhancement research artifacts
- candidate ranking and replacement-review research outputs

### Downstream use

UsEquityStrategies and US execution platforms consume only promoted artifacts and runtime-enabled profiles.

## What the artifacts are for

Snapshot artifacts are used to make strategy decisions reproducible: ranking inputs, feature snapshots, manifests, validation summaries, and promotion evidence. They are not marketing claims. Before a downstream repository promotes a profile, review the latest artifacts across short, medium, and long windows where applicable.

## Repository layout

- `src/`: library and runtime code.
- `tests/`: unit, contract, and regression tests.
- `docs/`: runbooks, design notes, evidence, and integration contracts.
- `.github/workflows/`: CI, scheduled jobs, release, or deployment workflows.
- `scripts/`: operator scripts and local helpers.

## Quick start

```bash
python -m pip install -e .
python -m pytest -q
```

## Useful docs

- [`docs/artifact_contract.md`](docs/artifact_contract.md)
- [`docs/crisis-context-research-v2.md`](docs/crisis-context-research-v2.md)
- [`docs/crisis-response-live-promotion-spec.md`](docs/crisis-response-live-promotion-spec.md)
- [`docs/crisis-response-research-roadmap.md`](docs/crisis-response-research-roadmap.md)
- [`docs/crisis-response-v1.md`](docs/crisis-response-v1.md)
- [`docs/global-etf-rotation-snapshot-management.md`](docs/global-etf-rotation-snapshot-management.md)
- [`docs/leveraged-strategy-candidate-research.md`](docs/leveraged-strategy-candidate-research.md)
- [`docs/live-strategy-health-report.md`](docs/live-strategy-health-report.md)
- [`docs/live-strategy-optimization-feedback-20260603.md`](docs/live-strategy-optimization-feedback-20260603.md)
- [`docs/mega-cap-leader-rotation-dynamic-validation.md`](docs/mega-cap-leader-rotation-dynamic-validation.md)
- [`docs/snapshot-ai-audit-automation.md`](docs/snapshot-ai-audit-automation.md)
- [`docs/tecl-xlk-optimization-research.md`](docs/tecl-xlk-optimization-research.md) ([简体中文](docs/tecl-xlk-optimization-research.zh-CN.md))

## Safety and contribution notes

- Keep generated data, credentials, and private account details out of Git unless the artifact is intentionally public and documented.
- Prefer reproducible commands and explicit output directories.
- Do not promote a research artifact to live use without the documented validation evidence.

## Community and security

- See [CONTRIBUTING.md](CONTRIBUTING.md) for pull request scope, local verification, and documentation expectations.
- Follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for maintainer and contributor conduct.
- Report credential, automation, broker, exchange, or cloud-resource vulnerabilities through [SECURITY.md](SECURITY.md); do not open public issues for secrets or live-execution risk.

## License

See [LICENSE](LICENSE).
