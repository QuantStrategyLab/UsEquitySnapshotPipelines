# UsEquitySnapshotPipelines

[Chinese README](README.zh-CN.md)

> ⚠️ Investing involves risk. This project does not provide investment advice and is for educational and research purposes only.

## What this project does

UsEquitySnapshotPipelines is a **Snapshot and research pipeline** in the QuantStrategyLab ecosystem. It builds upstream feature snapshots, candidate research artifacts, and release evidence for US equity strategies.

## Who this is for

- Engineers and researchers who want to inspect, reproduce, or extend this part of the QuantStrategyLab stack.
- Operators who need a clear entry point before reading the deeper runbooks or workflow files.
- Reviewers who need to understand the repository purpose, safety boundary, and evidence requirements before enabling automation.

## Current status

Research and gating repository; it should promote evidence, not directly trade.

## Repository layout

- `src/`: main library and runtime code.
- `tests/`: unit and contract tests.
- `docs/`: detailed design notes, runbooks, and evidence docs.
- `.github/workflows/`: CI, scheduled jobs, and deployment workflows.
- `scripts/`: operator scripts and local helpers.

## Quick start

From a fresh clone:

```bash
python -m pip install -e .
python -m pytest -q
```

If a command requires credentials, run it only after reading the relevant workflow or runbook and configuring secrets outside Git.

## Deployment and operation

Run the pipeline locally or through CI with explicit output directories. Review generated period summaries, rankings, and artifact manifests before any downstream live enablement.

Prefer manual or dry-run execution first. Enable schedules or live execution only after logs, artifacts, permissions, and rollback steps are reviewed.

## Strategy performance and evidence

This repository is the place to produce performance evidence for candidates. Only candidates that pass short, medium, and long-window backtests, benchmark comparison, and drawdown controls should be considered for live profiles.

README files are intentionally not a source of dated performance promises. Re-run the relevant tests, backtests, or pipeline jobs before relying on any result.

## Safety notes

- Never commit API keys, broker credentials, OAuth tokens, cookies, or account identifiers.
- Run new strategies and platform changes in dry-run or paper mode before any live execution.
- Review generated orders, artifacts, and logs manually before enabling schedules.

## Contributing

Keep changes small, reproducible, and covered by the narrowest useful tests. For strategy-facing changes, include the evidence artifact or command used to validate behavior.

## License

See [LICENSE](LICENSE) if present in this repository.
