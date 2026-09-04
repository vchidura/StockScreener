# Stock Screener Documentation

The portal uses immutable canonical equity storage, reproducible analysis
evidence, and worker-published views. Browser requests read PostgreSQL only;
they do not call market-data providers or perform full-universe analysis.

Start with the root [README](../README.md) for local development and
[DEPLOYMENT.md](DEPLOYMENT.md) for the complete post-cutover environment,
database role, Compose profile, validation, backup, and restore contract.

## Architecture

- [EQUITY_ANALYSIS_MATERIALIZATION_DESIGN.md](EQUITY_ANALYSIS_MATERIALIZATION_DESIGN.md): canonical equity storage and analysis contracts
- [OPTION_CHAIN_SCANNER_DESIGN.md](OPTION_CHAIN_SCANNER_DESIGN.md): option platform boundaries and data flow
- [SCALING_STRATEGY.md](SCALING_STRATEGY.md): capacity and scaling guidance
- [MODEL_REGISTRY.md](MODEL_REGISTRY.md): scanner and model versions
- [FRESH_DATABASE_SETUP.md](FRESH_DATABASE_SETUP.md): baseline initialization and Polygon ingestion

## Equity Research

- [FEATURES.md](FEATURES.md): product capabilities
- [STRATEGIES.md](STRATEGIES.md): strategy behavior
- [SIGNAL_RESEARCH.md](SIGNAL_RESEARCH.md): signal validation methodology
- [BACKTEST_WALKTHROUGH.md](BACKTEST_WALKTHROUGH.md): how a study runs end to end, followed on one real signal
- [SCANNER_LITERATURE_REVIEW.md](SCANNER_LITERATURE_REVIEW.md): research basis
- [SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md): event and outcome evaluation
- [SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md](SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md): scanner study summaries, recent signal retention, return qualification, and legacy cleanup
- [SCANNER_ENHANCEMENTS_BACKLOG.md](SCANNER_ENHANCEMENTS_BACKLOG.md): deferred enhancements
- [INTRADAY_STRATEGIES_DESIGN.md](INTRADAY_STRATEGIES_DESIGN.md): intraday strategy design
- [EXTENDED_HOURS_RESEARCH_DESIGN.md](EXTENDED_HOURS_RESEARCH_DESIGN.md): isolated extended-hours design
- [SCHEDULER_EXECUTION.md](SCHEDULER_EXECUTION.md): execution timing and lane semantics

## Options Research

- [OPTION_PIPELINE_CURRENT_STATE.md](OPTION_PIPELINE_CURRENT_STATE.md): current implementation state
- [OPTION_RESEARCH_DESIGN.md](OPTION_RESEARCH_DESIGN.md): what equity research must add for option relevance, and the option-native demand study
- [OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md](OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md): phased build order
- [OPTION_CHAIN_SCANNER_DESIGN.md](OPTION_CHAIN_SCANNER_DESIGN.md): normative design
- [OPTION_PHASE0_VALIDATION_2026-08-29.md](OPTION_PHASE0_VALIDATION_2026-08-29.md): provider entitlement evidence
- [OPTION_PLATFORM_CAPACITY_DECISION_2026-08-29.md](OPTION_PLATFORM_CAPACITY_DECISION_2026-08-29.md): measured capacity decision
- [optionchain-scanners.md](optionchain-scanners.md): scanner reference

Options remain read-only. Equity context, raw option archival, broker
execution, and Advanced streaming remain disabled until separately promoted.

## Operational Baseline

The active ticker universe contains 386 symbols. Canonical publication covers
`1m`, `5m`, `15m`, `30m`, `1h`, `1d`, `1wk`, and `1mo`; analysis materializes
all except `1m`. Twenty portal snapshot types must match the current source
generation before production readiness reports healthy.

Legacy `stock_prices_*`, scanner-event, and cutover-only relations are absent
from the final baseline.

Run the secret-safe environment and storage checks from the repository root:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_cutover_environment.py
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
```