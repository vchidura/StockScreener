# Stock Screener Portal

A FastAPI and React research portal backed by immutable, revision-aware equity
bars, reproducible analysis evidence, and worker-published scanner views.

## Current Architecture

- PostgreSQL stores immutable bar revisions, publications, analysis runs,
  evidence, contexts, and current pointers.
- The equity materialization worker ingests Polygon REST bars and publishes
  `5m`, `15m`, `30m`, `1h`, `1d`, `1wk`, and `1mo` cohorts.
- The portal worker publishes 20 generation-aware snapshots so expensive GET
  routes remain read-only and fast.
- Stock Screener uses worker-published immutable screening snapshots, pairing the
  last completed daily anchor with newly published completed hourly context.
  These are delayed observations, not live quotes.
- Stock Alerts distinguishes frozen backtested results, forward shadow records,
  and legacy daily discovery. The native launcher starts the multi-model forward
  shadow worker; this is unqualified paper monitoring, not brokerage execution.
- A single transactional baseline creates the final schema for fresh databases;
  legacy price and scanner relations are absent.
- Options remain read-only with equity context and raw archival disabled.
- The Polygon Advanced stream worker is defined but must remain disabled.

See [Equity Materialization Design](docs/EQUITY_ANALYSIS_MATERIALIZATION_DESIGN.md)
and [Option Chain Scanner Design](docs/OPTION_CHAIN_SCANNER_DESIGN.md) for system
design when the local documentation archive is present. The local production
runbook is [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md); the startup commands below
remain in the tracked README.

## Local Documentation And Research Inputs

The root `docs/` directory is local-only and ignored by Git, including existing
research reports and operator notes. Untracking it does not delete local files.
Documentation links into that directory require the local archive; a fresh checkout
does not include it. Runtime and test dependencies are retained under
`backend/research/inputs/`, with exact frozen bytes protected by Git attributes.
The reviewed ETF split configuration remains tracked separately at
[backend/research/stock_sector_split_reviews.json](backend/research/stock_sector_split_reviews.json).
Backups, source ledgers, database files and worker-generated views remain ignored
and must not be treated as disposable cleanup artifacts.

## Prerequisites

- Python 3.11
- Node.js 20+
- PostgreSQL 17 client and server tools
- Docker with Compose for container deployment only
- A Polygon/Massive API key

## Environment

Create the ignored active environment from the complete tracked template:

```powershell
Copy-Item .\backend\.env.example .\backend\.env
```

At minimum, provide database connection values, `POLYGON_API_KEY`, and exact
`CORS_ORIGINS`. Canonical reads and worker cadence use tested defaults when
omitted. Keep these option gates explicit and unchanged:

```env
OPTION_START_READ_ONLY=true
OPTION_EQUITY_CONTEXT_ENABLED=false
OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED=false
OPTION_RAW_ARCHIVE_ENABLED=false
```

Use `APP_ENV=development` for the native local database. Production requires
`APP_ENV=production` and a restricted, non-owner, non-superuser `DB_USER`.
Fresh native or Compose initialization also requires separate
`POSTGRES_ADMIN_*` credentials. Database dumps are external operator media and
are intentionally not tracked or required for a fresh deployment.

Validate the active configuration without displaying secrets:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_cutover_environment.py
```

## Native Development

Install dependencies from the repository root:

```powershell
python -m venv .\backend\.venv
.\backend\.venv\Scripts\python.exe -m pip install -r .\backend\requirements.txt
Set-Location frontend
npm.cmd install
Set-Location ..
```

After PostgreSQL is running and `backend/.env` is configured, initialize the
database and selected universe once:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\initialize_database.py
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\discover_universe_polygon.py `
  --if-empty --target-size 350 --lookback-days 20
```

For a complete historical bootstrap before starting workers, follow
[Fresh Database Setup](docs/FRESH_DATABASE_SETUP.md). Its plan-first
`bootstrap_fresh_data.py` driver can execute the same ingestion phases manually
without Docker.

### Daily Market-Day Startup

After the one-time bootstrap, start PostgreSQL and run the following commands
from the repository root on each market day. Start before the XNYS opening bell
(normally 09:30 America/New_York). Keep workers running through the actual exchange
close, including early-close sessions, plus the configured provider delay
(15 minutes by default) **and until final publication/analysis completes**.
Normally that means after 16:15 ET, not stopping exactly at 16:00 or 16:15.
Schedulers determine due sessions and slots; the launcher does not stop workers
at the closing bell. Local time zones and daylight-saving changes do not change
the exchange schedule.

#### Resident Worker Checklist

The native launcher's default `-Only All` starts eight worker windows. Use
`-Only Equity` for the five equity-side workers or `-Only Options` for the three
options/calendar workers. Use `-Worker` for a single worker from the table below.
Provider refresh cadences below are defaults, not a
promise that every cycle produces a new publication.

| Worker | Launch Set | `-Worker` Value | Purpose And Cadence |
|---|---|---|---|
| [run_equity_worker.py](backend/scripts/run_equity_worker.py) | Equity | `Equity` | REST bar ingestion, canonical publication, v16 analysis and daily signal context; checks due watermarks every 15 seconds with the configured provider delay |
| [run_corporate_action_worker.py](backend/scripts/run_corporate_action_worker.py) | Equity | `CorporateActions` | Upcoming/recent split and dividend observations; refreshes every 6 hours, retries failures after 5 minutes |
| [run_stock_idea_worker.py](backend/scripts/run_stock_idea_worker.py) | Equity | `StockAlerts` | Multi-model forward shadow alerts after completed source ingestion; bounded XNYS 30m publication windows, shared 30m/1h/daily decisions, actual input times and isolated paper plans |
| [refresh_equity_portal_snapshots.py](backend/scripts/refresh_equity_portal_snapshots.py) | Equity | `Portal` | Refreshes the 20 portal/scanner snapshots when missing or stale; checks every 60 seconds with `--continuous`; does not publish the separate Stock Screener snapshot |
| [run_screening_worker.py](backend/scripts/run_screening_worker.py) | Equity | `Screening` | Checks retained daily/hourly publications every 60 seconds; prepares a daily anchor when a new complete daily cohort arrives and appends hourly-context revisions without provider calls |
| [run_market_event_worker.py](backend/scripts/run_market_event_worker.py) | Options | `MarketEvents` | Shared Finnhub earnings and official FOMC calendar coverage; refreshes every 6 hours |
| [run_option_model_input_worker.py](backend/scripts/run_option_model_input_worker.py) | Options | `OptionInputs` | Official Treasury curve observations; refreshes every 6 hours, retries failures after 5 minutes; does not itself change option valuation policy |
| [run_option_worker.py](backend/scripts/run_option_worker.py) | Options | `Options` | Delayed option ingestion, analysis, strategy/recommendation publication and paper-outcome processing; due XNYS-open-anchored 15-minute slots |

The options worker always targets the latest observable delayed slot. A newer slot
supersedes an older retry and its cooldown; missed slots and terminal failed batches
are retained, not replayed with later snapshots. Same-slot operational failures may
retry after five minutes, but a failed/quarantined batch is never reopened. Outcome
processing cannot erase the completed ingestion checkpoint.

Options worker diagnostics are written to
[worker.log](backend/backups/options-worker/worker.log), with three rotating backups
of up to 2 MB each. Log timestamps use the host timezone; slot identities include UTC.
Configured secrets and common credential parameters are redacted, including tracebacks.
`run_option_worker.py --log-file <path>` overrides the path. Logging and scheduler
progress do not certify data freshness, full-session coverage or execution readiness.

All workers require the configured application database and installed schema.
Equity ingestion/corporate actions/options need their Polygon/Massive entitlements;
earnings coverage needs `FINNHUB_API_KEY` (without it, earnings are unavailable,
not cleared). Treasury/FOMC refresh needs access to the official public sources.
The default **All** set includes calendar support. For an equity-only deployment
that also needs shared earnings/FOMC coverage, run the calendar worker separately:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker MarketEvents
```

Do not start a second calendar worker if the Options/All set is already running.
Do not run native and Compose copies against the same database. An Options-only
launch does not supply equity ingestion: retain/start the equity side separately
when fresh underlying prices are needed, without enabling the separate
`OPTION_EQUITY_CONTEXT_ENABLED` gate.

The native Equity launcher enables the equity-owned `--behavior-shadow` producer for
the fixed 13 configured option underlyers. At startup it checks the latest 10 exchange
sessions and fetches split-adjusted grouped-daily data only for missing sessions with
exact canonical identities; normal cycles check only the latest completed session. It then
assembles and persists behavior after the legacy analysis run is terminal. Missing, stale or invalid behavior
inputs skip independently and never fail legacy materialization. This does not enable an
option detector gate, alert publication or execution permission. Check the real producer
path without writes:

Persisted option stock-behavior assessments are separately gated by
`OPTION_STOCK_BEHAVIOR_SHADOW_ENABLED=false`. Enabling it requires migration 046 and
launch-identity migration 047 plus a reviewed bounded or continuous-development manifest supplied through
`OPTION_STOCK_BEHAVIOR_SHADOW_LAUNCH_FILE`. It records assessment-only evidence after original
candidate persistence; it does not change admission, alerts, reservations, paper positions,
or execution permission, and failures do not retry original strategy work. Continuous mode
enforces row, payload, unavailable-rate and latency checks over a rolling usage window and
refreshes its approved status artifact after each completed Options cycle.

WP6 package-term assessments are separately gated by
`OPTION_PACKAGE_ASSESSMENTS_ENABLED=false` and require migrations 048 and 050. They reconcile
ordered candidate legs and signed reward/risk economics against the existing payoff engine,
record READY or explicit UNAVAILABLE research evidence, and never alter outcome maturation,
qualification, publication, probability or execution permission.

Terminal missing-package outcome evidence is independently gated by
`OPTION_OUTCOME_UNAVAILABLE_EVIDENCE_ENABLED=false` and requires migration 049. It leaves
missing coherent marks transiently pending until the versioned delayed-data deadline, then
records immutable UNAVAILABLE evidence without changing existing available outcomes.

```powershell
.\backend\.venv\Scripts\python.exe -u `
  .\backend\scripts\report_stock_behavior_production_readiness.py
```

`BLOCKED` with only `FEATURE_EVIDENCE_EXPIRED_AT_RECEIPT` before the session is expected;
recheck after fresh 30-minute and hourly analyses. `READY` means all 13 snapshots can be
assembled, not that any option strategy or publisher is approved.

Options Alerts dry-run v2 attaches an assessment-only stock behavior profile for
directional long-premium and debit-spread candidates. It records exact snapshot/policy/
candidate identities and structural directional/liquidity gates, but does not change
candidate selection, blockers, publication, paper positions or execution permission.
Other strategy families are explicitly not applicable. No empirical ADX, extension or
RVOL threshold is activated by this assessment.

#### Start And Preview

Inspect the commands first without launching workers, inspecting current processes,
reading the database or making provider requests:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Plan
```

`-Plan` also accepts `-Only` or `-Worker`, `-Once`, and the opt-in switches described below.
It checks command selection and script paths, not provider credentials, enrollment
or runtime health. ExecutionPolicy Bypass applies only to that PowerShell process.

Terminal 1 starts the API and explicitly loads the backend environment file:

```powershell
.\backend\.venv\Scripts\python.exe -m uvicorn main:app `
  --app-dir backend --env-file .\backend\.env `
  --reload --host 127.0.0.1 --port 8001
```

Terminal 2 launches the complete resident set in separate worker windows:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1
```

Do not launch the checklist scripts manually as well. Continuous startup skips
already-running script names; one-shot recovery reports them as failures rather
than claiming a completed pass. The major materialization/publisher workers also
use database leadership locks, but those are not a replacement for one operator
owning startup. Close each worker window or use Ctrl+C to stop it; closing the
launcher terminal does not stop its child worker windows.

To start the three independently in separate worker windows, run these from the
repository root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker MarketEvents
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker Screening
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker StockAlerts
```

Append `-Plan` to preview any one command without launching it. Append
`-NoNewWindow` to run a **single** selected worker in the current terminal (use a
different terminal per resident worker). Append `-Once` for a single cycle;
the launcher supplies the appropriate entry-point flags automatically, except
MarketContext, which is continuous-only through this launcher.

The provider-free alert-context annotator is an additional **opt-in** worker,
excluded from the default All/Equity sets:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker AlertContext
.\backend\.venv\Scripts\python.exe backend/scripts/run_stock_alert_context_worker.py --status
```

It checks every 60 seconds and annotates at most one selected publication per cycle,
using only retained cutoff-eligible inputs. It follows `STOCK_ALERT_SHADOW_VIEW` and
pins its first activation to that ledger's enrollment/policy. Earlier publications
are not automatically annotated; current/previous trading sessions form the bounded
catch-up window. Missing financials cannot block alerts. It never fetches providers,
writes the alert ledger or changes plans. A new enrollment/policy requires explicit
annotation reactivation; do not delete the activation record to bypass a mismatch.
The separate one-shot financial pilot and its access limitations are documented in
[the context runbook](docs/STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#automatic-annotation-and-financial-pilot).

For the separately approved event shadow study, use `-Worker AlertContext -ShadowEvents`.
This adds `--shadow-events` to the annotator, not Stock Alerts. It pins a separate
prospective activation and records all retained candidates from completed publications,
including suppressions, under `event-shadow`. Confirmed events inside the planned
holding window produce WOULD_BLOCK; missing coverage or uncertain timing stays UNKNOWN.
No live gate, selection change or quota-refill simulation is enabled. Shared market
facts are pinned to a verified archived snapshot completed before the original cutoff,
with matching session/universe and stock identity. Old alert context is never replaced.
See [progression results](docs/STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#context-progression-milestone)
for remaining coverage gaps and the prerequisites for any live gate proposal.

The separately approved Market Conditions split basis is opt-in via
`prepare_stock_alert_context.py --rotation-only --split-review backend/research/stock_sector_split_reviews.json`.
The four exact issuer-reviewed ETF actions adjust copied context history only;
raw bars and alert plans are untouched, and unreviewed actions still block.
The approved continuous refresher retains its prior state directory, publish and
FRED arguments with this flag added. See [reviewed split results](docs/STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#reviewed-sector-split-milestone)
for the12/12coverage check and still-pending prospective/disjoint evaluation.

Start this refresher independently in its own `market-context-worker` PowerShell
window, without restarting any already-running worker group:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker MarketContext
```

This opt-in entry retains `--rotation-only`, the original v2 state directory,
the pinned split review, `--publish-rotation-view --fetch-macro --continuous`.
It is excluded from default `All`/`Equity` sets and does not start dependencies
or the separate AlertContext annotator. An existing preparation script is skipped,
including one already running through the **Watch stored market conditions** task.
It does not transfer or restart that process. Append `-Plan` for a no-side-effect
preview or `-NoNewWindow` to run in the calling terminal. `-Once` is rejected;
use the bounded capture CLI with an explicit new output for one-shot work.
The **Start standalone market context worker** VS Code task calls this same entry.

The daily-owned swing alert policy is a separate, inactive-by-default experiment:

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/run_stock_idea_worker.py --swing --plan
```

A completed daily trigger owns its original stop, target, ATR and maximum holding
period: reversal5, acceptance10, resumption21 **trading sessions**, with the entry
session counting as one. Same-model, same-direction 30m or hourly confirmation is
required in the next trading session; it times entry without replacing the daily
bracket. Missed daily publications cannot seed setups. Missing/invalid preconfirmation
bars, bracket breaches, identity/action risk, late signals and no remaining entry
slot fail closed. Stops/targets remain active overnight, including gap losses.

`--swing` ignores `STOCK_ALERT_SHADOW_VIEW` and uses the separate
`backend/backups/equity-shadow/stock-ideas-swing-v1` store/view. It cannot reuse the
v1/v2 default stores and is not started by the default worker groups. Actual writer
execution additionally requires `--activate-swing-shadow`; no activation or reader
cutover is performed by preview. Starting this experiment enrolls a new dated cohort,
not a retroactive clone of old alerts or their recovery overlays. Existing plans and
closed/no-fill outcomes retain their original policies. The shared results reader
below presents both streams without merging their checkpoints. Prospective collection
and disjoint outcome evaluation are still required; this is not a qualified strategy.

After explicit activation approval, start only the swing worker in its own window:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker SwingAlerts
.\backend\.venv\Scripts\python.exe backend/scripts/run_stock_idea_worker.py --swing --status
```

`-Worker SwingAlerts` explicitly supplies `--swing --activate-swing-shadow` and
opens `stock-swing-shadow-worker`; it is excluded from the default All/Equity groups.
Duplicate checks distinguish swing and intraday modes, so the two can run independently.
`-Plan` remains side-effect free. A separately approved exact-identity quarantine
can use the normal quarantine flags together with the swing activation flags;
it preserves the enrollment and existing records, taking effect only at a future
boundary. Read-only `--swing --check-readiness` may report an identity-exclusion
diagnostic, but never activates one. The existing annotator retains its original
source; absent swing context remains explicitly unavailable.

### Combined Alert Results

The Stock Alerts page now reads a shared PostgreSQL results repository for forward
results. Both workers keep their separate SQLite checkpoints, policy manifests,
enrollments, risk rules and positions. The results-only projector checks each stream
every60seconds, with an independent lock and no source-ledger writes or provider calls.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Worker AlertResults
```

This opens `stock-alert-results-projector` separately; it is opt-in, not part of
All/Equity, and does not start or restart strategy workers. Durable status is at
`backend/backups/stock-alert-results/projector-status.json`; API stream statuses
show independent source and import timestamps, errors and projector staleness.

- Trade type: All / Intraday / Swing. Plan IDs, original policies and outcomes
  stay distinct even for the same stock, direction or publication timestamp.
- Latest Run selects the latest publication per strategy, including empty/missed
  publications; it never falls back to an older nonempty run. Day History excludes
  each strategy's latest publication and combines the remaining selected-session rows.
- Open Positions includes pending/open/unresolved plans outside the21-session date
  navigator. Sorting/pagination are global; recurrence remains strategy-specific.
- Opposing exposure is an annotation, not netting or a new selection gate. Returns
  are per-plan, not a combined portfolio return. Replay/legacy remain explicit sources.
- Publications/plans/context are immutable; row/mark updates are append-only revisions.
  Known entry/exit clocks and settled outcomes cannot be rewritten. Imports retain
  source payloads and hashes; corrupt/missing streams do not erase other results.
- Discovery WATCH records have no paper position. The importer validates their
  selected publication disposition, candidate identity, watch-only fields and clocks;
  TRADE records still require their exact retained position and plan. A valid closing
  WATCH must not prevent later trade publications from reaching the shared reader.
- Each stream's capture timestamp is taken after reading its atomic source file;
  a snapshot published during the other stream's import is not falsely future-dated.
  Genuinely future-dated snapshots are still rejected.

Deploy schema042 with the existing `apply_incremental_migration.py` administrator
runner. `project_stock_alert_results.py --verify` imports and reconciles both streams,
then repeats the import to check idempotency. Add `--publish-reader` only for an
approved cutover after successful reconciliation. `--check-storage` tests database
immutability guards in rolled-back transactions. GET never imports or detects.
The activation marker is `backend/backups/stock-alert-results/reader.json`.
`?combined=false` explicitly reads the original intraday snapshot for comparison;
an approved rollback removes that marker without deleting shared or original evidence.

`-Worker Equity` means ingestion only, unlike `-Only Equity`, which starts all
five equity-side workers. A single-worker launch does **not** start dependencies:
Screening, StockAlerts and Portal need ongoing equity ingestion for fresh facts.
Calendar and Treasury refresh should remain resident when their freshness is
required, although they poll only every six hours by default.

Do not combine `-Worker` with `-Only`, `-IncludePaperStudy` or `-PublishScreening`.
Bounded `-RepairSessions` is allowed only for `-Worker Equity` or an equity-containing
set. Ordinary StockAlerts restarts resume the retained policy and checkpoint;
do not repeat one-time policy-migration or retry-window flags.

For a worker **set**, `-NoNewWindow` still requires `-Once`: a resident worker
never returns to start its siblings. Single-worker selection is the exception.

The corporate-action worker polls Polygon every six hours by default and retains
live-observed splits and dividends for the union of portal and ranked-universe
securities. It looks back 30 days and forward 365 days. This supplies future
ex-date facts but does not yet activate dividend-aware option valuation: option
snapshots continue to carry `DIVIDEND_YIELD_DEFAULTED` until causal coverage,
discrete cash-flow treatment, and early-assignment gates are implemented.

The shared Finnhub earnings and Federal Reserve calendar worker starts with the
Options/All worker set. Its provider contract and rollback-only persistence path
can be revalidated without retaining another observation:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\probe_finnhub_earnings_contract.py
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\probe_market_event_materialization.py
```

Both commands do not retain data; the materialization probe always rolls back.
The 12-hour expiry on calendar coverage makes stale observations return
`UNAVAILABLE` rather than continuing to report `CLEAR` if the resident worker
fails. Date revisions and removals remain append-only observations. See
[Calendar deployment details](docs/DEPLOYMENT.md) for the measured request budget
and Compose `calendar` profile.

Terminal 3 starts the frontend:

```powershell
Set-Location frontend
npm.cmd run dev
```

Open `http://127.0.0.1:5174`. The Vite development server proxies `/api` to
`http://127.0.0.1:8001`.

#### Screening, Alerts And Optional Research

**Stock Screener:** the screening worker is now included in Equity/All. During the
session it pairs the last completed daily screening snapshot with the latest
published completed hour. A new complete daily source automatically produces the
next daily anchor. Existing daily facts, ranks, gap episodes and source cutoffs are
copied unchanged into each hourly revision; both source times are displayed.
The active page refreshes retained results every 60 seconds. Publication depends
on ingestion completing, not just on the clock reaching an hourly boundary.

If the other workers are already running, start only the new publisher; do not
stop or duplicate ingestion:

```powershell
.\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_screening_worker.py
```

Use `--status` to inspect the retained publication, `--measure` for a read-only
preparation, or `--once` to publish one due update and exit. Status reports the
retained snapshot, not a worker heartbeat. `SCREENING_WORKER_POLL_SECONDS` defaults
to 60 (minimum 15). The worker uses advisory leadership and an idempotent daily/hourly
pair key; no duplicate snapshots are written on idle polls. It performs no provider
backfill or research/alert-engine work. Delayed or missing source data stays stale
or UNKNOWN, including corporate actions between the daily and hourly dates.

An explicitly approved partial daily anchor can be published for one session without
relaxing the worker's general complete-source requirement. The approval binds the
source publication, session, selected count and unavailable identities. Missing
members remain in the universe as UNKNOWN, never assigned substitute prices.
The worker can refresh hourly context against this exact anchor until a complete
source for the same or a later session becomes available. Other degraded sessions
are not automatically approved.

September 16, 2026 was approved with385available of386members and OKE unavailable:

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/prepare_stock_screening.py `
  --session 2026-09-16 --allow-degraded --approve-partial-current `
  --expected-unavailable OKE --expected-selected 385
```

The command above measures without writing. Adding `--publish` creates and promotes
the approved anchor; it is a one-time operator action, not a routine restart command.
Historical `--session --allow-degraded --publish` without the explicit approval flag
still never promotes a current pointer. Only Screening was reloaded for this change;
original source publications, all280prior snapshots and other workers were retained.
The page displays partial source coverage separately from freshness and field-level
availability. Current session does not imply all indicators are available.

The original [prepare_stock_screening.py](backend/scripts/prepare_stock_screening.py)
remains available for explicit daily rebuilds and bounded historical work. Normal
market-day operation no longer requires running it after close. For a forced
daily rebuild during sequential recovery, stop resident workers first and use:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Only Equity -Once -PublishScreening
```

`-PublishScreening` still requires `-Once` and Equity/All; it now forces the daily
rebuild before the normal screening-worker pass. The pass is skipped if an earlier
equity step failed or was already running. Inspect
the Stock Screener session/publication details or the read-only
`GET /api/stocks/screening/catalog`; a successful worker launch is not proof of
fresh screening data.

**Stock Alerts:** [run_stock_idea_worker.py](backend/scripts/run_stock_idea_worker.py)
replaces the legacy daily discovery worker in Equity/All launches. It enrolls the
latest complete tracked daily cohort once, warms up from already-stored canonical
bars, and publishes only post-enrollment resumption/acceptance/failure plans.
The initial cohort is 386 tracked instruments, including ETFs, not the full market.
The active `stock_ideas_source_ready_v2` timing policy waits for a COMPLETE native
30m publication covering the enrolled security IDs, with both publication/creation
timestamps visible. Checking begins after the provider delay (15 minutes); the
intraday dispatch deadline is bar end +29m55s. The closing window also requires the
completed daily publication and is bounded at close +59m55s. Actual input-read and
publication times control decisions and paper entries; setup expiry is unchanged.
Unavailable inputs at the final deadline produce a missed run, not backdated alerts.
Daily context refreshes every five minutes, including late post-close arrivals.
Paper marks refresh from stored bars when positions are active; no quotes are fetched.

The original retained state and view remain under
`backend/backups/equity-shadow/stock-ideas-forward-v1/`. The corrected quality-v2
worker now defaults to a SEPARATE `stock-ideas-forward-v2/` directory, gates on
current eligible native prices and remaining entry slots, and strengthens intraday
breakout confirmation. It is implemented but not enrolled or activated. No old
checkpoint, published plan or paper result is migrated. A mismatched existing
store/view is rejected, not overwritten. The reader continues serving the old
view unless `STOCK_ALERT_SHADOW_VIEW` is explicitly changed during an approved
cutover. Keep that variable pointed at the NEW view for both worker and reader;
pointing v2 at the v1 view is rejected. The application is currently paused.
Offline inspection:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_worker.py --plan
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_worker.py --status
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_worker.py --quality-version 1 --status
```

`--plan` is offline and `--status` reads the retained view, not a process heartbeat.
The final command inspects the original v1 directory; the default status may be
NOT_ENROLLED for v2. See [the working enhancement plan](docs/STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md)
for the exact confirmation rules, isolated rollout and staged context work.
`--once` performs one cycle and exits; it does not wait for the next scheduled run.
New enrollments use source-readiness by default. Existing fixed-cutoff checkpoints
require explicit `--enable-source-readiness`; the approved activation is recorded
without rewriting their earlier runs. An explicitly requested `--retry-window`
appends one current-time retry of an empty incomplete run, never changes its
original record or extends setup expiry, and leaves the next normal boundary intact.
Use the ordinary command without retry flags for subsequent resident starts.
For an explicitly approved identity quarantine, stop only the Stock Alerts worker
and use the one-shot `--quarantine-security-id <enrolled-id> --quarantine-ticker
<enrolled-ticker>` operation. It requires a recorded `IDENTITY_CHANGED` break,
creates a consistent SQLite backup, and appends a hash-linked alert-only cohort
revision starting at the next strictly future market boundary. Original enrollment,
past publications, input checkpoints and paper positions remain retained; shared
universe/identity tables and other page readers are not changed. Existing unresolved
positions stay identity-gated. Resume the ordinary worker command without quarantine
flags; the exclusion persists in the checkpoint. Do not delete a member or substitute
its new security ID by ticker. `--check-readiness` inspects the pending cohort and a
retained-source probe without writes, but does not guarantee future alerts. Current
OKE activation and verification are recorded in
[the downstream publication checkpoint](docs/agent-context/downstream-publications.md).
Incremental alert reads revisit seven calendar days of retained native history
(up to 800 bars per ticker), keeping actual observed/created cutoffs. Missing bars
inserted behind a detector cursor reset only that security's intraday setup state;
evaluation resumes at the next strictly future boundary with the same 200-bar
warmup. Existing bar revisions still trigger correction quarantine, never overwrite.
For an explicitly approved repair of an existing checkpoint, stop only Stock Alerts,
run `run_stock_idea_worker.py --quality-version 2 --reconcile-history`, then verify
with `audit_stock_alert_session.py --session YYYY-MM-DD --state-dir <v2-state-dir>
--verify-history-reconciliation` **before restarting**. This is a bounded stored-data
repair, not a provider backfill or alert retry. A consistent SQLite backup and report
are retained under the state directory's `history-backups`; original bars, enrollment,
quarantine, positions, publications, outbox and scheduled boundary are preserved.
Intraday pending setups for affected securities are discarded rather than replayed;
recovery revision IDs and the future effective boundary are retained. Resume the
ordinary standalone StockAlerts command without maintenance flags after verification.
Older gaps outside seven days need a separately scoped audit; this is not unlimited
historical coverage or a guarantee of qualifying alerts.
For an empty live run, use `audit_stock_alert_session.py --session YYYY-MM-DD
--state-dir <v2-state-dir> --publication-summary`. Optional `--inspect-corrections`
compares at most 1,000 flagged revision pairs with bounded read-only database queries.
The summary separates immutable publication/candidate counts from current model-input
blocks and watch states. Neither option retries publications or changes worker state.
Reviewed same-security corrections can be admitted for future detectors using
`run_stock_idea_worker.py --quality-version 2 --recover-corrections
--expected-correction-hash <correction_inventory_sha256>` only after approval and
stopping Stock Alerts. The inventory hash comes from the read-only summary; a
changed inventory, identity/clock change, invalid/future bar, older-than-seven-day
correction or ambiguous multiple revision chain is refused. No tolerance threshold
or volume-only exemption is used. Only the exact current canonical revision pairs
are retained in a hash-checked overlay under `correction_recovery_history`.
The operation backs up SQLite under `correction-backups` and resets affected pending
setups at the next strictly future boundary. Original bars, corrections, identity
flags, positions, enrollment, prior publications and outbox remain unchanged.
Run the same audit script with `--verify-correction-recovery` **before restarting**
to verify preservation and prospective model-input coverage, then resume the ordinary
standalone worker. New alerts/positions record the recovery generation; pre-recovery
positions remain under their original correction hold. An additional unreviewed
revision or identity change blocks the security again and needs a new scoped review.
This does not replay old signals or promise that a pending setup will confirm.
The old worker script and records remain available but are no longer launched.
Backtested history stays frozen; neither [run_stock_idea_replay.py](backend/scripts/run_stock_idea_replay.py)
nor [prepare_stock_alert_view.py](backend/scripts/prepare_stock_alert_view.py) is a
market-hours service. An empty run or degraded coverage is not evidence of no setups
in the full market. Continuous cadence and strategy effectiveness are separate checks.
No automatic alerts across saved screeners, AI alerts, email delivery or broker
orders are enabled by this launcher. See the
[agentic roadmap](docs/STOCK_SCREENER_WORKSPACE_DESIGN.md#agentic-screener-roadmap)
for future work, not startup prerequisites.

**Optional enrolled paper study:** [run_equity_paper_tracker.py](backend/scripts/run_equity_paper_tracker.py)
observes the fixed ridge/momentum/SPY study, not general stock alerts. Check its
state before adding the watcher:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_paper_tracker.py --status
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_paper_tracker.py --check-inputs
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Only Equity -IncludePaperStudy
```

This switch requires an existing default study manifest; it never enrolls or
changes the study. It passes `--watch` normally or `--once` with launcher `-Once`.
The observer uses retained inputs and writes research records, not broker orders;
its frozen deadlines cannot be repaired by starting late. Review the
[paper-study policy](docs/EQUITY_FORWARD_PAPER_STUDY.md) before opting in.
Advanced streaming, historical replays/backtests, universe bootstraps and source
repairs are not ordinary resident market-hours workers and remain excluded.

### Same-Session Recovery And Verification

If the continuous workers were interrupted or were not running during the
current session, stop any remaining worker windows and run one catch-up pass
from the repository root after the configured 15-minute provider delay:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Once
```

This re-reads all available native equity bars for the current session, processes
only the latest due equity analysis watermark and latest observable option slot,
then exits. It does not reconstruct skipped intraday equity analyses or option
matrices. Verify both pipelines without starting additional work:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_equity_materialization.py --coverage-report
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_option_pipeline.py --status
```

`run_equity_worker.py` performs ingestion, canonical publication, and the v16
analysis run for every due interval; do not launch a separate analysis job.
Monitor run/member status from another terminal without starting work:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\report_equity_analysis_status.py
```

One-shot mode runs equity materialization to completion before refreshing portal
snapshots, so a newly published daily or weekly cohort cannot leave the portal
generation stale after a successful pass. Corporate actions and daily discovery
run between those steps; market events and Treasury inputs run before the option
cycle. A failed step is reported while independent steps are still attempted;
the launcher exits with an error if any failed. Continuous workers start
independently, so check their first successful cycles rather than assuming a
completed dependency chain from the order windows opened.

One-shot invocation is safe for intermittent same-session price-bar capture. Each
native `5m`, `15m`, and `30m` request re-reads the available session-to-date range
and persists bars idempotently, so repeated runs do not duplicate data and an
after-close run fills the complete equity session. Run the close pass after the
configured provider delay (15 minutes by default), not exactly at the closing
bell. Use continuous mode when every 5m/15m/30m equity signal checkpoint or every
15-minute option matrix is required.

### Recent-Session Repair

There is no need to query the database for each missing equity bar before
repairing a short recent gap. Provider fetches and canonical persistence are
idempotent: existing revisions are retained and only missing revisions are
inserted. Stop the continuous workers, choose how many recent XNYS sessions to
repair, and run one command from the repository root:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\start_workers.ps1 -Once -RepairSessions 3
```

`-RepairSessions 3` re-fetches `5m`, `15m`, and `30m` bars for the latest three
exchange sessions, rebuilds recent `1h`, `1d`, `1wk`, and `1mo` derivations from
bounded source tails, then runs the normal latest-watermark equity publication,
analysis, portal snapshot refresh, and latest delayed option cycle. The accepted
range is 1-30 sessions. Restart the continuous workers afterward.

This repairs equity price history but does not retroactively create `ORIGINAL`
analysis runs for every skipped intraday checkpoint. Use continuous mode when
every signal checkpoint is required.

The delayed option worker and `run_option_pipeline.py` accept no historical date;
they can process only the latest observable current-session slot and durable
current-session retries. A missed prior option session, or its skipped 15-minute
matrices, cannot be reconstructed by the current daily commands. Do not represent
a later option snapshot as if it had been observed at the missed watermark.

Ticker charts keep canonical history unchanged and may add current-session
`15m`, `30m`, `1h`, or `1d` display candles composed directly from fresh,
finalized `5m` bars. The latest candle is labeled **Developing candle** until
its target window closes and is replaced by the canonical bar when publication
catches up. These rows are response-only: scanners, patterns, setups, evidence,
outcomes, and current projections continue to use canonical finalized bars.
Missing or stale `5m` input disables the display fold rather than fabricating a
candle. A developing `5m` candle is unavailable while the Advanced `1m` stream
remains disabled.

`run_option_worker.py` performs delayed option-chain ingestion, normalization,
local IV/Greeks, chain and expiration analysis, six strategy modules, payoff and
scenario analysis, and atomic candidate/recommendation persistence. It runs on
XNYS-open-anchored 15-minute slots after the configured Developer delay. The
worker remains read-only for execution; only backend-published rows appear in the
Opportunity Board and Decisions views.

The default `/options` route opens the all-universe Opportunity Board. It presents
the leading persisted structure from each strategy and underlying separately from
research-only detector highlights. Ranking remains the backend strategy rank; the
portal does not fabricate a score across strategies. Raw contract research remains
available at `/options/research`. Decisions combines Candidate Audit and Signal Ledger;
the full matrix Explorer remains available contextually from Research rather than as a
primary tab.

Stocks and options have separate primary navigation sections. **Stocks / Tickers** links
Overview to `/ticker/SPY` by default and keeps that item active for any `/ticker/{symbol}`
workspace; global ticker search continues to open the searched symbol on the same Overview
tab. Stock Research lives beside Overview at `/stock-research`. There is no separate global
Research section.

The **Options** section exposes Options Research, Option Activity, and Options Flow as
equal-level pages. `/options/activity` compares cumulative call
and put volume, open interest, estimated premium activity, and matched settlement-to-
settlement OI change across the 13 configured underlyings. The shared header session
selector reads each underlying's last complete 15-minute matrix within that exchange
session. `/options/flow` drills into one ticker selected in-page with expiry concentration,
strike concentration, and high-activity contracts. Developer data has no contemporaneous
option NBBO, so neither surface infers buyer/seller aggressor direction; estimated premium
activity remains distinct from executable or net premium flow.

Do not start a legacy scheduler or the Advanced stream worker.

## Historical Signal Research Inputs

The portal snapshot worker publishes current UI payloads only. Historical signal
studies use a separate signal-agnostic input builder that reconstructs the universe
effective on each trading session, applies a causal prior-session liquidity policy,
and records that the data was downloaded later for replay.

Run a non-persistent 100-session pilot first:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --dry-run --sessions 100 --end 2026-08-31
```

Persist resumable universe checkpoints and split/dividend facts:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --persist --sessions 100 --end 2026-08-31 `
  --backfill-actions `
  --output .\backend\.cache\historical-signal-research\pilot-100-report.json
```

The default `liquid_us_common_stocks_v2` policy admits historically active US common
stocks whose latest prior-session close is at least `$5`, whose median dollar volume
over the preceding 20 sessions is at least `$20M`, and whose lookback coverage is at
least 90%. Provider responses are checksummed under the ignored `backend/.cache`
directory, and identical reruns resume completed session checkpoints.

Version 2 preserves Massive ticker case while joining grouped bars to uppercase common-stock
reference symbols. This prevents preferred-share notation such as `BCpC` from colliding with the
different common stock `BCPC`. Version 1 is retained only as superseded audit evidence and its
grouped bars are excluded from canonical selection.

`--backfill-bars` is an optional second phase. It reuses the checksummed grouped-daily
cache to persist provider-native, unadjusted reconstructed daily bars for the eligible
union with exact XNYS session bounds and replay availability. It makes no additional
provider requests. Capacity-plan that phase after reviewing `eligible_union` in the
pilot report. The builder prepares reusable inputs; signal-specific event replay,
outcomes, and qualification remain separate versioned jobs.

Verify reconstructed-universe, corporate-action, daily-bar, and live-pointer
boundaries without starting work:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py --status
```

Run the bounded gap adapter against the qualification-sized 300-session input:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal gap-formation-v2 --start 2025-06-23 --end 2026-08-31 `
  --output .\backend\.cache\historical-signal-research\gap-formation-300-v2-summary.json `
  --events-output .\backend\.cache\historical-signal-research\gap-formation-300-v2-events.jsonl

# Future adapters implement HistoricalSignalAdapter and can be loaded as:
# --adapter package.module:adapter_instance
```

Adapters receive exact session membership, reconstructed daily bars, corporate-action
context, and deterministic event identity. Their JSONL output is immutable research
input, not a qualified outcome or a live recommendation. Gap adapter v2 evaluates the
formation bar plus exactly 20 prior bars so extending the research range cannot change
an overlapping event. Gap adapter v1 remains audit evidence only.

Persist event evidence, evaluate the predeclared 5/10/21-session next-open policy,
and run the 18-cell FDR family with an explicit reviewed timestamp:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\gap-formation-300-v2-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version gap_formation_daily_qualification_v5_portfolio_metrics `
  --qualification-effective-from 2026-09-01T06:13:45Z

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --status `
  --source-version gap_formation_v2 `
  --evaluation-version gap_formation_daily_qualification_v5_portfolio_metrics
```

The three primary source lanes are breakaway hold, continuation hold, and fade
reversal. Direction and horizon create 18 qualification cells. Common, exhaustion-watch, and
unclassified formation controls are persisted as evidence but receive no outcome
policy. An identical run inserts no duplicate evidence, outcomes, or qualification
revisions. Qualification reads only evidence IDs declared by the supplied event file;
retained subjects from superseded replays cannot enter a newer cohort.

Run the separately versioned confirmation and first-entry fill studies against the same inputs:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal gap-breakaway-confirmation-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\gap-breakaway-confirmation-300-events.jsonl

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal gap-entry-fill-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\gap-entry-fill-300-events.jsonl
```

Confirmation evaluates 5/10/21 sessions after the first close beyond the formation extreme. Gap
entry evaluates 1/3/5/10/21 sessions after the first close inside a still-unfilled gap. Both retain
explicit stop/target path evidence and remain research-only.

Exact product-strategy studies keep the 20-session universe liquidity policy unchanged while
extending only the indicator bar/action warm-up:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\prepare_historical_signal_research.py `
  --persist --sessions 300 --end 2026-08-31 `
  --bar-warmup-sessions 210 --backfill-actions --backfill-bars `
  --output .\backend\.cache\historical-signal-research\pilot-300-strategy-warmup.json

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal ma-crossover-9-21-v1 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\ma-crossover-9-21-300-events.jsonl

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal momentum-pullback-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\momentum-pullback-300-events.jsonl

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_research.py `
  --signal bearish-bounce-v2 --start 2025-06-23 --end 2026-08-31 `
  --events-output .\backend\.cache\historical-signal-research\bearish-bounce-300-events.jsonl
```

Momentum Pullback and Bearish Bounce reuse the exact page scanners on deterministic 210-bar daily
windows. Version 2 emits only the first session of each contiguous match episode. Grade and score
are retained as diagnostics; the primary studies do not select a grade after observing outcomes.

Evaluate the exact product cohorts as separate FDR families:

```powershell
.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\ma-crossover-9-21-300-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version ma_crossover_9_21_qualification_v2_bracket_aware `
  --qualification-effective-from 2026-09-01T14:34:05Z

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\momentum-pullback-v2-300-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version momentum_pullback_episode_qualification_v3_bracket_aware `
  --qualification-effective-from 2026-09-01T14:34:10Z

.\backend\.venv\Scripts\python.exe `
  .\backend\scripts\run_historical_signal_outcomes.py --all `
  --events .\backend\.cache\historical-signal-research\bearish-bounce-v2-300-events.jsonl `
  --horizon-sessions 5 10 21 --round-trip-cost-bps 4 `
  --evaluation-version bearish_bounce_episode_qualification_v3_bracket_aware `
  --qualification-effective-from 2026-09-01T14:34:15Z
```

## Production Compose

Set the production values documented in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md), then run:

```powershell
docker compose --env-file backend/.env --profile equity up -d --build
docker compose --env-file backend/.env ps
```

The `equity` profile starts both canonical workers. Do not enable the
`equity-stream` profile.

## Verification

Run the storage validator after schema installation, ingestion, restoration, or
worker recovery:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
```

Expected state includes:

- Eight current interval cohorts with 386 members each.
- Twenty fresh portal snapshot types.
- No invalid bars, unresolved derived lineage, publication-count mismatch,
  analysis evidence mismatch, or current projection/run mismatch.
- No retired legacy price or scanner relations.

Run regression checks:

```powershell
.\backend\.venv\Scripts\python.exe -m pytest .\backend\tests -q
Set-Location frontend
npm.cmd run build
```

The API readiness endpoint is `GET /api/health`. In production it is healthy
only when canonical storage is ready, all 20 snapshot pointers match the source
generation, and the database login is restricted.

## Backup

Create and isolated-restore a versioned full backup:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\backup_database.ps1 -Version fresh-canonical -Mode Full

powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\backend\scripts\verify_database_backup.ps1 `
  -BackupPath C:\Backups\StockScreener\stocks_db_backup_current.dump
```

Keep the previous known-good backup until the replacement database passes restore
verification. The verifier must report both `CANONICAL_RESTORE_VALIDATED` and
`RESTORE_VERIFIED`.

## Documentation

- [Deployment](docs/DEPLOYMENT.md)
- [Fresh database setup](docs/FRESH_DATABASE_SETUP.md)
- [Feature catalog](docs/FEATURES.md)
- [Strategies](docs/STRATEGIES.md)
- [Scheduler execution contract](docs/SCHEDULER_EXECUTION.md)
- [Scanner evaluation](docs/SCANNER_EVENT_EVALUATION.md)
- [Scanner research consolidation](docs/SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md)
- [Options pipeline state](docs/OPTION_PIPELINE_CURRENT_STATE.md)
- [Option chain implementation guide](docs/OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md)
- [Model registry](docs/MODEL_REGISTRY.md)