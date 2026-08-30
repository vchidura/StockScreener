# Option Platform Capacity and Deployment Decision

Status: accepted planning baseline

Recorded: 2026-08-29

Related design: `docs/OPTION_CHAIN_SCANNER_DESIGN.md`

Purpose: preserve the measured workstation and PostgreSQL capacity assessment used to
decide where Polygon Options Developer research, paper simulation, Polygon Options
Advanced shadow testing, and automated execution may run. Re-run the measurements in
section 13 before relying on this record after material hardware, database, provider,
universe, retention, or deployment changes.

## 1. Decision

| Workload | Decision on this machine |
|---|---|
| Developer delayed scanning for 13 initial underlyings | Approved after the prerequisites in section 8 |
| Developer delayed scanning for 18 expanded underlyings | Approved after the 13-name soak and expansion gates pass |
| Developer delayed paper-proxy account simulation | Approved after durable ledger, restart reconciliation, and backup tests pass |
| Advanced real-time shadow testing on filtered contracts | Conditionally approved after the controls in sections 9 and 10 |
| Full-chain storage of every Advanced quote tick | Rejected for local PostgreSQL |
| Unattended automated live trading on the current workstation | Not approved |
| Automated live trading on an always-on managed host | Future target after Advanced shadow and broker gates pass |

The hardware has ample compute, memory, and burst database throughput for the planned
Developer workload. The current blockers are operational reliability and durability,
not CPU performance. Advanced can also run on this machine for bounded shadow testing,
but automatic execution should move to an always-on Linux server or cloud VM with a
managed or independently recoverable PostgreSQL service and redundant connectivity.

## 2. Scope

Initial universe:

- Stocks: `AAPL`, `AMD`, `AMZN`, `GOOGL`, `META`, `MSFT`, `NVDA`, `PLTR`, `SOFI`,
  `TSLA`
- ETFs: `SPY`, `QQQ`, `IWM`
- Total: 13 underlyings

Expansion candidates:

- `AVGO`, `COIN`, `INTC`, `MSTR`, `MU`
- Expanded total: 15 stocks plus 3 ETFs, or 18 underlyings

Developer mode uses 15-minute-delayed option chains, aggregates, daily open interest,
and individual option trades. It does not have option quotes. All simulated fills are
labeled `RESEARCH_DELAYED_PROXY` and cannot be presented as broker-like execution.

Advanced mode adds real-time option trades and NBBO quotes. Automated execution also
requires a real-time underlying stock/ETF feed from Stocks Advanced or the broker;
Options Advanced alone does not guarantee an aligned real-time underlying mark.

## 3. Measured Host

Measurements taken on 2026-08-29:

| Resource | Measured value |
|---|---:|
| Operating system | Windows 11 Pro |
| Processor | Intel Core i7-1270P |
| Physical cores / logical threads | 12 / 16 |
| Installed memory | 47.7 GB |
| Free memory during assessment | 32.2 GB |
| Storage | 953.9 GB SK hynix NVMe SSD |
| Free storage | 865.4 GB, 90.8% |
| Storage health | Healthy / operational |
| Uptime at assessment | 121.5 hours |
| Active network | Intel Wi-Fi 6E, negotiated at 144.4 Mbps |
| Battery charge | 86% |
| AC sleep timer | 3,600 seconds, or 1 hour |

CPU, memory, and disk capacity are substantially above the requirements of delayed
13-to-18-underlying scanning. Wi-Fi bandwidth is also sufficient, but a single wireless
link is not a reliable dependency for unattended order execution. The one-hour AC
sleep timer prevents continuous market-hours operation until it is disabled.

## 4. Measured PostgreSQL State

The active database server was PostgreSQL 17.11 on 64-bit Windows.

| Item | Measured value |
|---|---:|
| PostgreSQL | 17.11, 64-bit Windows |
| Service | `postgresql-x64-17`, running, automatic startup |
| Database | `stocks_db` |
| Database size | 2,612 MB |
| Database backends during assessment | 2 |
| Cache hit rate | 95.24% |
| Recorded deadlocks | 0 |
| Committed / rolled-back transactions | 157,134 / 13 |
| Historical temporary files | 1,656 files, 11 GB |
| Current application pool | 2 minimum, 20 maximum connections per process |

Largest relations at assessment:

| Relation | Approximate size |
|---|---:|
| `market_discovery_states` | 1,707 MB |
| `stock_prices_hourly` | 239 MB |
| `scanner_event_outcomes` | 200 MB |
| `scanner_events` | 132 MB |
| `stock_prices_intraday` | 128 MB |
| `stock_prices_daily` | 114 MB |
| `scanner_event_occurrences` | 82 MB |

The initial PostgreSQL settings were close to installation defaults rather than
settings chosen for a 48 GB analytical workstation. Conservative tuning was applied
and activated on 2026-08-29:

| Setting | Before | Effective after tuning |
|---|---:|---:|
| `shared_buffers` | 128 MB | 8 GB |
| `effective_cache_size` | 4 GB | 32 GB |
| `work_mem` | 4 MB | 8 MB globally; 64 MB only for the ticker-overview transaction |
| `maintenance_work_mem` | 64 MB | 512 MB |
| `max_connections` | 100 | Adequate only if application pools are bounded collectively |
| `min_wal_size` | 80 MB | 1 GB |
| `max_wal_size` | 1 GB | 4 GB |
| `checkpoint_timeout` | 5 minutes | 15 minutes |
| `random_page_cost` | 4.0 | 1.25 for the local NVMe benchmark baseline |
| `wal_level` | `replica` | Compatible with replication/PITR foundations |
| `fsync` | On | Correct; retain |
| `synchronous_commit` | On | Correct for ledger writes; retain |
| `full_page_writes` | On | Correct; retain |
| `autovacuum` | On, 3 workers | Correct baseline; table-specific tuning will be needed |
| `archive_mode` | Off | No point-in-time recovery |
| Data checksums | Off | Corruption detection is weaker |
| `track_io_timing` / WAL I/O timing | Off | On |
| Temporary-file logging | Disabled | Files at least 64 MB |
| Slow-query logging | Disabled | Statements taking at least 2 seconds |
| `pg_stat_statements` | Not loaded | Version 1.11 loaded and collecting |

The cache hit rate and absence of deadlocks are healthy. The original 11 GB figure was
cumulative temporary-file output, not current disk occupancy. Past statements cannot
be identified retroactively because query and temp-file diagnostics were disabled.
The investigation itself increased the cumulative database counter, including one
discarded synthetic plan that was not equivalent to production SQL; use
`pg_stat_statements` from its clean 2026-08-29 baseline for future attribution rather
than comparing the global cumulative counter directly.

Exact production-path measurements identified and fixed two repeatable sources:

| Repository workload | Before | Optimization | After |
|---|---:|---|---:|
| Scanner-confidence observation load | 272.5 MB temp/call | Removed redundant SQL `ORDER BY`; downstream operations already group and sort explicitly | 0 temp blocks; 341,480 rows planned/executed in 273 ms server time |
| Ticker overview for 386 active symbols | 78.3 MB temp/call | `SET LOCAL work_mem='64MB'` for this transaction only | 0 temp blocks; 386 rows returned in 2.64 seconds end to end |

Repeating these workloads approximately 30 times is enough to explain most of the
historical 11 GB. Global `work_mem` was not raised to 64 MB because each parallel
query node and concurrent connection can receive a separate allocation.

Reusable read-only monitoring command:

```powershell
cd backend
.\.venv\Scripts\python.exe scripts\analyze_postgres_temp_spill.py --limit 15 --min-temp-mb 64
```

The report shows the statistics baseline, query ID, call count, execution time,
temporary blocks/bytes, cache behavior, normalized SQL, and tables with the highest
cumulative sequential rows read.

## 5. Measured Throughput

### Numerical workload

A synthetic NumPy/SciPy workload processed one million option rows through 20
vectorized Black-Scholes/Newton iterations:

| Metric | Result |
|---|---:|
| Rows | 1,000,000 |
| Newton iterations per row | 20 |
| Elapsed time | 3.403 seconds |
| Row-iterations per second | 5.88 million |
| Approximate working arrays | 76.3 MB |

This is not a model-correctness test. It demonstrates that local vectorized IV/Gamma
calculation is not a capacity bottleneck for the proposed filtered universe.

### Database workload

Two synthetic PostgreSQL tests were run using the existing psycopg2 connection path:

| Test | Result |
|---|---:|
| Temporary 100,000-row insert | 57,889 rows/second |
| Temporary index plus grouped aggregate | 0.448 seconds |
| WAL-backed 50,000-row insert/index/aggregate | 74,913 rows/second |
| WAL generated by second test | 7.2 MB |

The WAL-backed test was executed inside a transaction and rolled back. It exercised
WAL generation but did not measure long-duration commit latency, checkpoint pressure,
autovacuum, partition churn, concurrent readers, network ingestion, or production
retention. These results establish burst headroom, not a promise of sustained quote-
tick throughput.

## 6. Complexity Assessment

The scanner formulas are not the most difficult part. Correct market-time handling,
paper-account accounting, multi-leg order state, restart recovery, provider
corrections, and live broker reconciliation dominate the risk.

Approximate effort for one experienced engineer, including focused tests but excluding
the time needed to accumulate market evidence:

| Delivery area | Complexity | Planning range |
|---|---:|---:|
| Developer ingestion, filters, local IV/Gamma, durable raw work | Medium-high | 2-4 weeks |
| Six scanners, context gates, replay, and APIs | High | 3-6 weeks |
| Paper ledger, limits, assignment, margin, and reconciliation | Very high | 4-8 weeks |
| Advanced WebSockets, NBBO, and shadow execution | Very high | 4-8 weeks |
| Broker adapter, live reconciliation, controls, and soak | Critical | 4-8+ weeks |

A useful Developer scanning MVP is plausible in 6-10 engineering weeks. A system that
is technically and operationally ready for unattended automatic execution is more
realistically a 4-6 month effort plus the required forward-testing period. These are
planning ranges, not delivery commitments.

## 7. Account and Transaction Controls Required

PostgreSQL is appropriate for durable paper-account and execution state. The paper
engine must track and transactionally enforce:

- Starting cash, settled cash, reserved cash, buying power, margin, and total equity
- Per-order quantity and notional limits
- Maximum contracts per order and orders per cycle/session/day
- Per-strategy, per-underlying, per-expiration, and per-account position limits
- Per-trade, per-underlying, aggregate, and 0-DTE maximum risk
- Minimum unreserved cash after a proposed order
- Atomic all-or-none package treatment for simulated multi-leg orders
- Duplicate signal, order, fill, assignment, dividend, and ledger-effect prevention
- Realized/unrealized P&L, fees, slippage class, and data-quality class
- Stops, profit targets, trailing stops, 21-DTE exits, and technical exits
- Expiration exercise/assignment and assigned stock lots
- Daily loss and maximum drawdown kill switches
- Win rate, profit factor, drawdown, activation frequency, and rejected-order metrics
- Immutable cash/fill ledger and reconstruction of materialized balances after restart

The design's durable inbox/outbox, advisory leadership lock, deterministic idempotency
keys, and startup reconciliation remain required even at the initial 13-name scale.
They protect correctness rather than increase throughput.

## 8. Prerequisites for Developer Market-Hours Operation

All of the following are required before calling the workstation an all-session paper
environment:

1. Keep Polygon Options Developer chain and delayed-trade entitlement under automated
   startup verification. It was activated and returned HTTP 200 on 2026-08-29; option
   quotes returned the expected Developer-tier HTTP 403 denial.
2. Verify the separately configured stock provider supplies delayed one-minute
   underlying bars aligned with option marks.
3. Disable Windows sleep and hibernation while connected to AC power.
4. Install and pin `polygon-api-client`. The backend virtual environment currently has
   NumPy 1.26.4, pandas 2.2.0, SciPy 1.17.1, psycopg2 2.9.9, and FastAPI 0.109.0, but
   the `polygon` module is missing.
5. Add a separately supervised option-pipeline process. The current Docker Compose
   file has stock database, API, frontend, and stock scheduler services only.
6. Bound aggregate PostgreSQL connections across all processes. The current pool can
   allocate 20 connections per process, so API, stock scheduler, options ingestion,
   and execution processes can approach the server's 100-connection limit.
7. Apply option migrations, create monthly partitions ahead of time, and test retention.
8. Run the complete 13-underlying cycle and eight-hour 18-underlying stress soak from
   the architecture acceptance gates.
9. Verify leadership-lock, crash-recovery, partial-pagination, clock-skew, provider-
   correction, and ledger-rebuild tests.
10. Expose health alerts for stale feeds, durable work age, disk/WAL growth, IV
    convergence, missing management marks, and ledger mismatch.

## 9. PostgreSQL Hardening Before Sustained Options Ingestion

Required work:

- Establish automated, encrypted full backups outside this workstation.
- Add continuous WAL archiving if the RPO=0 committed-ledger goal is retained.
- Perform and time an isolated restore; a backup that has not restored successfully is
  not an accepted recovery control.
- Reduce per-process connection pools or introduce PgBouncer. Keep the total normal
  allocation materially below 100 so migrations, maintenance, and incident access
  retain headroom.
- Create time-partitioned normalized snapshots/trades and keep indexes aligned with
  actual read paths.
- Use batched `COPY` or `execute_values`, not one transaction per tick.
- Tune autovacuum by write-heavy partition and monitor dead tuples, WAL, checkpoints,
  temp spill, transaction age, and partition size.
- Enable useful production diagnostics, including I/O timing and bounded slow-query
  logging, after evaluating overhead.

Applied local baseline values, subject to workload monitoring:

| Setting | Applied value |
|---|---:|
| `shared_buffers` | 8 GB |
| `effective_cache_size` | 32 GB |
| `work_mem` | 8 MB global |
| `maintenance_work_mem` | 512 MB |
| `min_wal_size` / `max_wal_size` | 1 GB / 4 GB |
| `checkpoint_timeout` | 15 minutes |
| `random_page_cost` | 1.25 |

Do not multiply `work_mem` by connection count alone; one query can allocate it for
multiple sort/hash nodes. Keep `fsync`, `full_page_writes`, and `synchronous_commit`
enabled for execution-ledger transactions.

Current recovery gap:

- `archive_mode` is off.
- Data checksums are off.
- Only a 0.1 MB schema-only dump dated 2026-08-21 was present.
- A validated backup script exists at `backend/scripts/backup_database.ps1`, but no
  matching application/database backup task was found in Windows Task Scheduler.

This gap blocks unattended paper-account trust and all live execution. Schema backup
alone cannot recover cash, orders, fills, positions, or option market history.

### 9.1 Configuration backup and rollback

Pre-tuning configuration files were copied to:

```text
C:\Program Files\PostgreSQL\17\data\tuning-backup-20260829-112755
```

Tuning was persisted through `ALTER SYSTEM` in `postgresql.auto.conf`. Rollback should
be performed deliberately with `ALTER SYSTEM RESET` for the changed settings, followed
by `SELECT pg_reload_conf()` and a controlled restart for `shared_buffers` and
`shared_preload_libraries`. Copying old files over a live configuration is not the
preferred first response.

### 9.2 Windows service incident

The controlled restart exposed a pre-existing deployment incompatibility: Smart App
Control is On and user-mode Code Integrity policy
`{0283AC0F-FFF1-49AE-ADA1-8A933130CAD6}` blocks the unsigned PostgreSQL
`pg_ctl.exe` service wrapper. Code Integrity events 3033 and 3077 and Service Control
Manager event 7000 identify the wrapper explicitly. The tuning itself parsed correctly
and did not cause the failure.

PostgreSQL was recovered temporarily by launching the permitted `postgres.exe`
directly. It is listening on port 5432 with the tuned settings, passed an application-
table smoke test, and loaded `pg_stat_statements` 1.11. The Windows service remains
stopped, so this process is tied to its persistent terminal and is not an accepted
automatic-start configuration.

For local development while this policy remains enabled, use the guarded helper:

```powershell
cd backend
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
.\scripts\start_postgres_direct.ps1
```

It exits without action when port 5432 is already listening, refuses to remove a stale
`postmaster.pid`, and otherwise launches `postgres.exe` in a separate minimized
console. Keep that console open and use Ctrl+C there for a controlled shutdown. This
is a development workaround, not automatic service recovery or approved live-trading
supervision. The execution-policy override applies only to the current PowerShell
process; it does not disable Smart App Control or change machine-wide script policy.

Microsoft documents that Smart App Control has no per-application bypass. Turning it
off permits untrusted applications more broadly and does not make `pg_ctl.exe` signed;
if Smart App Control is re-enabled, a later service restart may be blocked again.
Recent Windows builds may allow Smart App Control to be re-enabled without reinstalling
Windows, but that does not solve PostgreSQL's trust decision. Durable remediation is
one of:

- replace the PostgreSQL distribution/service wrapper with an organization-approved,
   signed or Smart-App-Control-trusted package;
- migrate PostgreSQL to an approved WSL/container runtime; or
- migrate to managed PostgreSQL, which is already the target for automated trading.

On an enterprise-managed device using a custom App Control for Business policy, the IT
administrator may be able to deploy an approved policy. Smart App Control itself does
not expose an individual-file allow rule.

Until one option is implemented and restart-tested, a reboot or terminal closure can
stop the local database and therefore blocks unattended market-hours claims.

## 10. Advanced Real-Time Operating Envelope

The workstation can perform bounded Advanced shadow testing if subscriptions are
filtered before data reaches durable storage.

Recommended stream split:

1. Discovery lane: real-time option trades and compact one-second or one-minute
   aggregates for the filtered 13-to-18-underlying universe.
2. Execution lane: NBBO quotes only for shortlisted candidates, every proposed spread
   leg, every working order, and every open position.
3. Underlying lane: real-time stock/ETF quotes for moneyness, IV alignment, context,
   stops, and risk.

PostgreSQL should retain:

- Normalized filtered snapshots and aggregate bars
- Signal-relevant trade windows and corrections
- Context snapshots and signal suppressions
- Signals, orders, fills, positions, stock lots, and immutable ledger effects
- Raw event slices around signals and incidents where needed for audit

PostgreSQL should not retain every quote update for every contract in the complete
SPY, QQQ, IWM, and stock chains. That workload can reach tens of millions of rows per
session and creates unnecessary WAL, index, vacuum, and backup pressure. High-volume
raw ticks belong in compressed Parquet or provider Flat Files/object storage with
short, explicit retention. PostgreSQL stores the compact operational truth and file
manifests/checksums.

Advanced capacity must be proven using captured or realistically generated peak-rate
replay, not the burst benchmarks in section 5. Pass conditions include:

- Zero lost or duplicated business effects during disconnect and restart injection
- Bounded queue and durable-work age through open, news, and close peaks
- Database commit latency and replication/archive lag within policy
- No unbounded RSS, WAL, table, index, or temporary-file growth
- Real-time option and underlying timestamps satisfy the five-second alignment gate
- NBBO-backed shadow fills reconcile against broker-paper results

## 11. Why Live Automation Is Not Approved on This Workstation

The current machine is powerful enough, but its failure domain is unsuitable for
unattended order routing:

- Windows sleeps after one hour on AC under the active power plan.
- The only active physical network link is Wi-Fi.
- The database, application, scheduler, and proposed execution engine share one host.
- No point-in-time recovery or automated full backup is active.
- There is no documented redundant network, UPS response, remote watchdog, or
  independent execution kill switch.
- Docker CLI was unavailable during assessment, and the Compose definition has no
  option-pipeline service or resource limits.

A laptop can fail, reboot, sleep, lose Wi-Fi, install updates, exhaust disk, or become
unreachable while positions remain open. A local process restart policy does not
remove the shared host, power, network, and database failure domain.

Recommended live target:

- Always-on Linux VM or dedicated server in a region appropriate for the broker and
  provider
- Managed PostgreSQL or primary plus independently recoverable standby
- Automated encrypted backups and tested point-in-time restore
- Wired/redundant network path and infrastructure health monitoring
- Separate ingestion, strategy, execution, and API process supervision
- Broker-side risk limits and kill capability independent of the strategy process
- Central logs/metrics/alerts available when the trading host is down
- Secure secret manager and explicit paper/shadow/live environment separation

The workstation should remain the development, historical replay, incident-analysis,
and recovery environment after live workloads move.

## 12. Go/No-Go Gates

### Developer scanner

Go only when:

- Developer snapshot/trade and underlying-stock entitlements pass probes
- Sleep is disabled and the option service restarts automatically
- One full session completes with no missing durable batch or stale backlog
- IV convergence is at least 95% per underlying batch
- Partitions, retention, alerts, and storage forecasts are validated

### Developer paper proxy

Go only when:

- Every scanner event and order is idempotent
- Multi-leg package effects are atomic
- Cash, margin, positions, and P&L reconstruct exactly after forced restart
- Daily loss, drawdown, concentration, and transaction limits pass boundary tests
- Full off-host backup and successful restore are documented
- All output is labeled `RESEARCH_DELAYED_PROXY`

### Advanced shadow

Go only when:

- Options Advanced and real-time underlying entitlements pass
- WebSocket gaps reconcile from REST without duplicated effects
- Quote subscriptions are limited to candidates/orders/positions
- Peak-rate replay and a multi-session shadow soak pass
- Every leg satisfies quote spread, age, and option/underlying alignment gates

### Automated broker execution

Go only when:

- The runtime has moved to the approved always-on deployment class
- At least 100 closed quote-backed shadow trades and 60 eligible sessions exist for
  each promoted strategy/version
- Net expectancy after measured costs is positive, profit factor is at least 1.20,
  and drawdown remains inside policy
- Broker-paper reconciliation and live startup reconciliation are exact
- Broker-side limits, application kill switch, stale-data circuit, duplicate-order
  prevention, and human authorization are independently tested
- A reviewed deployment change, not an API call, enables the approved live adapter

## 13. Reassessment Triggers and Procedure

Reassess this decision when any of the following changes:

- Universe grows beyond 18 underlyings
- DTE or moneyness filters widen
- Quote subscriptions expand beyond candidates, working orders, and positions
- Raw-event retention or snapshot cadence changes
- Database size doubles or free disk falls below 30%
- p95 cycle duration exceeds 10 minutes in Developer mode
- Durable-work age exceeds five minutes
- PostgreSQL cache hit rate falls below 95% or temp/WAL growth accelerates
- A new service increases aggregate connection-pool allocation
- Provider plan, API schema, entitlement, delay, or history changes
- Workload moves between Windows, containers, Linux, or cloud infrastructure
- A live broker or strategy version is added

Reassessment procedure:

1. Record hardware, free RAM/disk, network path, sleep/power policy, and active services.
2. Record PostgreSQL version, database size, relation sizes, cache/temp/deadlock/WAL
   metrics, pool allocations, backup age, and restore result.
3. Replay representative peak Developer or Advanced data through the full durable
   pipeline, not isolated math only.
4. Measure p50/p95/p99 ingest, normalization, strategy, transaction, and end-to-end
   latency plus RSS, queue age, WAL, temp, table, and index growth.
5. Inject provider disconnect, partial pagination, database restart, duplicate leader,
   full queue, stale marks, and process crashes.
6. Compare results with the go/no-go gates and append a dated superseding decision.

## 14. Summary

The current machine is comfortably capable of Polygon Options Developer scanning and
delayed paper-proxy simulation for 13 underlyings, with a credible path to 18. Local
PostgreSQL is fast enough for normalized option snapshots, trades, signals, and an
immutable paper ledger.

The system is not ready today for continuous unattended operation because sleep,
backup/PITR, dependency, service supervision, pool sizing, and entitlement gaps remain.
Advanced shadow testing is feasible only with filtered quote subscriptions and compact
storage. Full-chain quote-tick retention is out of scope for PostgreSQL. Automated live
execution should run on independently recoverable always-on infrastructure after all
Advanced shadow and broker gates pass.