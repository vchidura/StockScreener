# Forward Equity Paper Observation

Status at setup: **ENROLLED, WATCHER RUNNING, WAITING FOR FIRST DECISION**.

The user explicitly retained the **20% prospective drawdown research limit**.
The historical 20% no-go assessment remains unchanged. This is a separately scoped
forward observation study, not a reopening of the historical parameter experiments.

## Mandate

- Track three portfolios: frozen ridge, ordinary 12-1 momentum, and scheduled SPY.
- Use 75% intended equity exposure, 25% zero-return cash, equal stock weights in
  the top decile, and the original 21-XNYS-session next-open/exit-close convention.
- Charge 10 bps round-trip on invested initial notional, half at entry and half at
  exit. No leverage, automatic risk resizing, replacement stocks or dividend income.
- Save prospective scores and intended allocations before the next entry window.
  No historical signal backfills or retrospective basket replacement.
- Record daily simulated fills and marks; never send brokerage orders or update
  live screener rankings. No existing worker was restarted or reconfigured.
- Review after six completed cohorts, with SPY-relative compounded return and
  drawdown reported separately. The checkpoint is not automatic deployment approval.
  Breaching 20% is a research flag, not a liquidation instruction.

## Frozen Model And Forward Population

The final saved ridge fit from the closed experiment is frozen, including its feature
means/scales, coefficients and intercept. Its training decision was **2026-07-16**.
No automatic refitting is performed in this forward phase. This is explicitly a fixed
coefficient observation study, not continued expanding-window monthly retraining.

Model SHA256:
`244a3c797f73640afe9825b24ae152b0f5f209ea07e21cfba5c3c47b86071176`.

The forward population is the existing complete `LIVE_OBSERVED` universe with policy
`polygon_reference_v1`, visible at capture time, effective no later than the decision
close and no more than seven calendar days old. It is **not** the original historical
`liquid_us_common_stocks_v2` population or the two fixed 300-ticker subsets. Those
sample lists and source hashes remain in the manifest as provenance, not forward
eligibility. Both stock portfolios use the same current eligible population.

This choice reuses the retained operational feed rather than expanding ingestion.
The setup check found 386 universe members and 375 feature-ready candidates, with 11
excluded for unavailable/invalid historical inputs. At least 50 eligible names and
90% coverage of the observed universe are required. Failure blocks that decision;
it does not silently relax eligibility or lower the required coverage.

Features preserve `mom_12_1 = close(t-21)/close(t-252)-1` and
`rev_5 = -(close(t)/close(t-5)-1)`. Retained raw prices are adjusted for recorded
splits through the decision date, with exact consecutive sessions, security identity,
OHLCV and actual observation/creation clocks checked. The current decision bar must
be live observed. Historical reconstructed prices may supply history only if already
observed before capture; replay clocks never substitute for actual availability.

## Schedule

Enrollment was recorded at **2026-09-13 01:26:21 UTC**. No prior signal was written.

| Cohort | Decision Close | Scheduled Entry | Scheduled Exit |
|---|---|---|---|
| 1 | 2026-09-14 | 2026-09-15 | 2026-10-13 |
| 2 | 2026-10-13 | 2026-10-14 | 2026-11-11 |
| 3 | 2026-11-11 | 2026-11-12 | 2026-12-11 |
| 4 | 2026-12-11 | 2026-12-14 | 2027-01-13 |
| 5 | 2027-01-13 | 2027-01-14 | 2027-02-12 |
| 6 | 2027-02-12 | 2027-02-16 | 2027-03-16 |

Capture starts 15 minutes after the scheduled decision close and ends five minutes
before the next session opens. Both input-observation time and actual record time
are saved. If the watcher misses that window or inputs never become usable, the
cohort is permanently recorded as missed; it is not reconstructed after entry.

March 16, 2027 is the planned checkpoint if all six cohorts complete. Missing data
or missed cohorts prevent a six-completed-cohort conclusion. The watcher also stops
for manual review seven days after the final scheduled exit if completion remains
blocked. It never extends the experiment or adds new cohorts automatically.

## Simulated Fills And Valuation

The tracker is an observation ledger, **not a broker paper account**. A signal is
frozen before entry, but the daily-open fill proxy is recorded only after that day's
final live-observed bar becomes available. It does not establish auction access,
liquidity, bid/ask spread or an achievable execution price. Its actual observation
timestamp and source bar are retained.

A valid zero-volume daily bar produces an explicitly labelled no-fill under this
paper policy, leaving that allocation in cash without fees. A missing entry bar
remains **unresolved**, not a fabricated fill or a presumed halt. Other positions
retain their weights; unresolved values never become zero-return holdings by default.

Daily marks use recorded closes and fixed entry share quantities, multiplied by
validated split ratios when applicable. Known non-split corporate events, identity
changes, incomplete prices or unavailable split evidence block the aggregate mark
and create a first-seen issue record. There is no automatic merger settlement or
ticker substitution. Missing marks can be observed later with their actual acquisition
time, but an already recorded mark is not rewritten. A data revision affecting such
a mark requires a separately reviewed correction procedure, not a silent overwrite.

Portfolio reporting uses only the contiguous marked path. It does not join later
complete cohorts across a missing earlier portfolio value. Performance is unavailable
before observations exist, and incomplete observations remain explicitly partial.
Daily-close drawdown still misses intraday extremes.

## Evidence And Storage

The bounded local ledger is at
[backend/backups/equity-paper/ridge_forward_v1/manifest.json](../backend/backups/equity-paper/ridge_forward_v1/manifest.json).
It stores the enrolled mandate, six-cohort calendar, model and historical source
provenance. Additional files contain immutable signals, simulated fills, daily marks,
missed windows and unresolved issues. Signals retain the exact input prices/actions,
scores, exclusions, universe and implementation hash. These can be sizable artifacts
because all 253-session candidate histories are retained for each of six decisions.

Each ledger document has a checksum and is written without replacing an existing
file. A local lock prevents concurrent writers. This is reproducible local research
storage, not an externally secured or tamper-proof audit system. Do not manually edit
the manifest or overwrite observations to repair data.

Database queries use `REPEATABLE READ, READ ONLY`. The initial readiness check found
that canonical response-bound split history was insufficient. The tracker therefore
also uses the existing provider client for bounded split queries and stores those
responses in the local `.cache/equity-paper` evidence cache, not in production tables.
Responses are keyed by query interval and UTC observation date; their first acquired
timestamp is preserved on cache reuse. Only evidence observed before signal capture
may affect a score.

Ticker-scoped split responses are associated with the current frozen position identity;
this is not independent certification of all historical identities. Split coverage
does not certify absence of every merger, suspension or delisting. Known such events
block automatic marks, and unresolved valuations remain a research limitation.

## Operation

The VS Code task **Watch prospective equity paper study** was started at setup and
checks once per minute. Keep the machine awake, the terminal task running, and the
existing market-data ingestion operational. This is not a Windows service and does
not restart automatically after a reboot or VS Code shutdown. Stop it with Ctrl+C
in its task terminal; restarting resumes the existing ledger without re-enrollment.

From the workspace root in PowerShell:

```powershell
# Read status without changing observations.
& .\backend\.venv\Scripts\python.exe .\backend\scripts\run_equity_paper_tracker.py --status

# Restart continuous observation of the already enrolled study.
& .\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_equity_paper_tracker.py --watch

# Run one observation cycle, or diagnose an observer error.
& .\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_equity_paper_tracker.py --once

# Read-only readiness check; never writes a past candidate signal.
& .\backend\.venv\Scripts\python.exe -u .\backend\scripts\run_equity_paper_tracker.py --check-inputs
```

Use only one watcher for this ledger. A stale lock after a crash requires operator
review that no observer process remains before removing it. The CLI deliberately
provides no historical `--now`, signal backfill, model-refit or trading command.

## Setup Validation

**110 tests passed** across the new tracker and existing price-diagnostic suite.
Coverage includes future-only scheduling, late-capture rejection, actual source
cutoffs, weighted feature parity through a split, immutable/restart-safe recording,
simulated fill fees, zero-volume no-fill policy, missing entries, split-aware shares,
known-event blockers and missing coverage.

The current-input probe passed on the last closed session, September 11: **375/386
eligible**, without writing a historical signal. This does not guarantee future input
availability. The running watcher reported all six cohorts waiting, zero signals,
zero fills and no measured returns. No real orders, database writes or live rank
changes occurred. The historical no-go decision remains in the
[research closeout](EQUITY_RESEARCH_CLOSEOUT_2026-09-12.md).