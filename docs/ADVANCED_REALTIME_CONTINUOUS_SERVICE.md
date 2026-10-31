# Advanced Realtime Continuous Service

> **Legacy target-local Developer-forward service reference.** This is not the
> current Advanced V3 lifecycle owner. The consolidated continuous V3 design and
> implementation status are in
> [REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md).

Status: **SOURCE_QUALIFIED; NOT ACTIVATED**.

The dedicated owner is
[`run_realtime_developer_forward_service.py`](../backend/scripts/run_realtime_developer_forward_service.py).
It does not launch, repoint or share runtime state with `start_workers.ps1` or any
canonical resident worker.

## Pipeline Order

One advisory leader schedules these target-local stages:

1. `REFERENCE_REFRESH`: every six hours, run the reviewed nine-name reference
   bootstrap with fundamentals. It covers securities, corporate actions, company
  fundamentals, earnings/FOMC, Treasury rates, option catalog and settled OI.
  The resulting seed/action-set hashes bind every later window.
2. `CURRENT_INGESTION`: for each eligible Developer forward window, persist exact
   raw equity spot/context inputs under INGRESS/COMPUTE authority.
3. `CYCLE_MATERIALIZATION`: acquire complete option matrices and materialize
   normalized matrices, target-local context and package evidence.
4. `CYCLE_DETECTORS`: run the native detector/strategy adapters and seal ALERTS,
   FLOW and SCREENER artifacts.
5. `IMMUTABLE_PUBLICATION`: publish one atomic release/current pointer under the
   PUBLICATION role.

The service plan exposes a data-coverage matrix mapping these stages to canonical
Equity, CorporateActions, MarketEvents, OptionInputs, Options, StockAlerts,
Screening, Portal and UIProjections data families. This is contract-equivalent
target-local data coverage, not reuse of canonical databases, workers or mutable
publications.

## Retained Portal Browsing

Realtime ticker workspaces, their summaries, Pattern Watch, daily/hourly strategy
pages, Persistence and exact-release strategy research use explicit
`read_mode=retained` for portal browsing. These stored-only GETs validate the
publication's original reference, cohort, member snapshot IDs, checksums and
persisted evidence. Original values and market/publication timestamps remain
unchanged. No provider request, refresh, recomputation or publication occurs.

Ordinary ticker navigation, including All Tickers links, opens `/ticker/SYMBOL`
without requiring URL cohort or snapshot IDs. The deployment's configured cohort
selects the latest published workspace; its snapshot ID binds section reads
internally. Retained ticker reads do not require `screening_current` or a ready
producer. Entry checks and manual refresh resolve the latest publication while
failed refreshes leave the displayed data intact. Explicit historical pins still
select the exact requested snapshot without substitution.

Ordinary strategy and Persistence browsing also selects its release internally
without injecting cohort/snapshot IDs into the URL. Hourly views retain
`interval=1h`; historical URLs and bounded research still honor exact release and
evaluation-session pins. `/api/strategies/daily?read_mode=retained` validates the
original reference at publication time and keeps durable scanner, source-window,
action, setup dependency and rolling-history evidence checks intact. Later
reference expiry/supersession or new action evidence does not block stored reads.

Strategy scanner, evaluation and Persistence page loads obtain the selected
publication and view in one atomic request. Successful immutable validation is
cached per API reader for up to 16 cohort/snapshot/checksum identities, with
single-flight validation per identity. Earlier visibility cutoffs revalidate;
failed validation is never reused. This relies on the database's immutable
publication/evidence rows. Every read still checks publication hashes, scope,
clocks, original/current references and new action evidence; cache hits also
check dependency membership with a bounded aggregate query. Current-mode reads
continue to perform full validation. The cache contains validation metadata, not
rewritten scanner results or freshness status.

Later reference expiry or corporate-action changes produce freshness metadata and
`current_data_available=false`, rather than hiding valid retained content.
Routine reference-expiry notices are not displayed in ticker sections. Market
freshness still reports the actual stale/unavailable state. Retained
responses have `execution_permission=false`; they are not current trading data.
Missing publications, invalid original references, corrupt evidence, substituted
pins and future clocks still fail closed.

The default `read_mode=current` remains strict. Retained browsing does not alter
reference-refresh cadence, ingestion/detector/publication gates, Stock Alerts,
Options permissions or service activation requirements.

## Lifecycle

- Modes remain `--plan`, `--validate-session`, `--once`, `--continuous`, `--status`.
- Reference refresh cadence defaults to 21,600 seconds and is pinned into service
  identity with the manifest SHA-256 and fundamentals requirement.
- Reference/action hashes are generated from the completed refresh and passed to
  both `current` and `cycle`; operators do not provide static per-service hashes.
- Reference failure writes `BLOCKED_REFERENCE_REFRESH` and prevents all later
  stages. Completed reports are reused within cadence; ambiguous reports require
  manual inspection.
- Current ingestion must complete before cycle work. A cycle commits materialized
  artifacts before immutable publication. Missed windows remain MISSED and are
  never backdated.
- PostgreSQL advisory leadership, atomic state reports and stop-on-first-failure
  behavior remain unchanged.
- The service never enrolls, migrates, changes grants, accepts readiness or unlocks
  roles. Continuous mode still requires an exact enrollment receipt and reviewed
  monitoring acceptance with manual UI/data review.

## Guarded Launcher

[`start_realtime_advanced_service.cmd`](../backend/scripts/start_realtime_advanced_service.cmd)
invokes the reviewed PowerShell wrapper with process-local execution-policy bypass,
then runs the Python service in `--plan` mode first. It does not change machine or
user execution policy. Planning requires an enrollment
file but not monitoring evidence. Starting requires both a workspace-local
`-MonitoringAcceptance` file and explicit `-Start`. The launcher validates evidence
paths and connection bounds, but never changes principal state or invokes
`start_workers.ps1`.

The current 2026-10-09 fallback receipt is BLOCKED and the plan rejects it before
provider, target-write or service construction. A READY cohort and accepted
monitoring artifact are required for an actual continuous start. No
`ready-enrollment.json` artifact currently exists; readiness must come from a
successful staged finalization, never a copied or renamed blocked receipt.

Fundamentals-inclusive reference mode has a source ceiling of 104 requests; normal
reference mode remains 68. These are code ceilings, not standing provider approval.

Validation: focused supervisor/reference tests **13 passed**; reference bootstrap
**64 passed**; full forward ingestion/materialization/detection/publication gate
**390 passed**, all with the mandatory offline I/O guard.

Activation remains blocked by the immutable 2026-10-09 selection verdict documented
in [canonical-fallback validation](ADVANCED_REALTIME_FALLBACK_VALIDATION_2026-10-09.md).

## Scaling Boundary

The current service is a correct nine-name coordinator but still executes stages
inline. It is not a 400-equity/50-option capacity claim. Broader scale requires the
coordinator/worker extraction, four 50-name detector shards, shared account ledger,
connection/storage headroom and measured recovery gates in the
[Advanced Realtime scaling plan](ADVANCED_REALTIME_SCALING_PLAN.md).