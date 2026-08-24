# Scheduler Execution Specification

Purpose: provide a precise, operations-oriented description of what the scheduler executes, when it executes, and why outcomes appear when they do.

Scope: current behavior implemented by backend/scripts/run_scheduler.py, backend/scripts/run_scanner_event_pipeline.py, and backend/research/scanner_events.py.

## 1) Process lifecycle

1. Scheduler starts with provider and ticker set resolved from CLI args.
2. It runs one startup backfill pass to repair recent data gaps before entering the main loop.
3. It enters an infinite loop with a 30-second sleep cadence.
4. The loop is partitioned into three time regimes:
   - Market hours (weekday 09:30:00 through 16:00:00 ET, inclusive)
   - Post-close window (weekday 16:15:00 through 17:59:59 ET)
   - Closed market (all other times)

## 2) Scheduler cadence constants

- Intraday 5m price job: every 300 seconds
- Running daily candle job: every 300 seconds
- Hourly price job: every 3600 seconds
- Intraday 1h scanner lane: every 3600 seconds
- Main loop check cadence: every 30 seconds
- Closed-market heartbeat log: every 600 seconds

Important: these are elapsed-time triggers, not wall-clock cron anchors.

## 3) Market-hours execution order

During each market-hours loop pass, jobs are checked and potentially run in this order:

1. Intraday 5m candles
2. Running daily candle
3. Hourly candles
4. Scanner events lane for interval set (1h only, intraday)

Each job updates its own last-run timestamp only after the function call returns.

Operational implication:

- If a job takes time, downstream jobs in the same pass are delayed.
- The 1h scanner lane is effectively hourly but can drift by loop latency and upstream job runtime.

## 4) First run after scheduler start

The per-job last-run timestamps initialize to zero. Therefore, on the first loop pass that falls inside market hours, all eligible market-hours jobs run immediately (not after waiting 5 or 60 minutes).

Operational implication:

- If scheduler starts at 11:17 ET on a weekday, 5m, daily-running, hourly, and intraday 1h scanner checks are all immediately eligible.

## 5) Intraday 1h scanner lane behavior

The market-hours scanner lane calls scanner pipeline with interval set (1h) only.

Pipeline order for each interval is strict:

1. evaluate_outcomes(interval)
2. capture_events(interval)

Reasoning:

- Evaluate-first guarantees that only already-matured prior events are evaluated.
- Newly captured events from this pass are not evaluated in the same pass.

## 6) What makes a 1h outcome "due"

For each 1h scanner event and each configured horizon (7, 21, 35 bars):

1. The system checks scanner_event_outcomes for an existing row for that (event_id, horizon_bars).
2. If missing, it counts how many hourly bars exist after signal_time for that ticker.
3. The pair is due only when count >= horizon_bars.
4. Due rows are evaluated and inserted with ON CONFLICT DO NOTHING idempotency.

Operational implication:

- 1h outcomes appear progressively as bars mature; they are not all created at capture time.

## 7) 1h capture universe selection

For interval 1h capture, tickers are not the full active universe by default. The pipeline selects tickers from latest market_discovery_states restricted to active discovery cohorts:

- CONTINUATION
- REVERSAL_WATCH
- EMERGING_REVERSAL
- REVERSAL_CONFIRMED
- CONFLICT
- LAGGARD

Operational implication:

- Intraday 1h scanning load is intentionally bounded to cohort-relevant names.

## 8) End-of-day (post-close) run behavior

In the post-close window, once per trading day, scheduler runs run_eod_once sequence:

1. Final daily close update
2. Final hourly update
3. Final 5m intraday update (non-blocking for later steps if it fails)
4. EOD data validation
5. Conditional backfill + retry of daily gaps if validation is not clean
6. Re-validation
7. If validation is clean: cross-sectional signal, market discovery
8. Hourly scanner repair (accuracy pass):
   - Delete recent 1h outcomes for a bounded number of sessions
   - Re-evaluate 1h outcomes in batches until due queue drains
9. Scanner events interval set at EOD:
   - Monday-Thursday: (1d, 1h)
   - Friday: (1d, 1h, 1wk)
10. Saturday-only deep hourly repair for Yahoo provider

Operational implication:

- 1h scanner pipeline runs both intraday and in EOD pass.
- EOD pass can catch any missed intraday evaluation/capture work.
- The explicit hourly repair step replays recent 1h outcomes from refreshed bars,
  improving end-of-day accuracy when intraday bars or late updates changed.

## 9) Daily close guard and duplicate prevention

- The daily_close_done_today marker ensures run_eod_once is executed at most once per ET date.
- The marker resets when a new ET date is observed in market-hours branch.

## 10) Timezone and session semantics

- Scheduler market-hour checks are based on America/New_York.
- 1h scanner signal_time values are stored as UTC-converted ET wall-clock times.
- Daily/weekly signal_time values are pinned to 16:00 ET close.

## 11) Failure handling and resilience

- Any exception inside main loop is caught; scheduler logs and retries loop after 60 seconds.
- Specific intraday scanner errors are caught and logged without terminating the process.
- Startup backfill failure does not stop scheduler; loop continues.

## 12) Idempotency and data integrity patterns

- scanner_event_outcomes unique key: (event_id, horizon_bars)
- Outcome inserts use ON CONFLICT DO NOTHING
- Event capture upserts by stable event_key
- Occurrence history pruning runs in pipeline to cap detail volume while preserving recent and latest-per-ticker/interval rows

## 13) Practical timing expectations

Given current implementation, "hourly" means elapsed-hour cadence under a cooperative loop, not strict bar-close wall-clock triggers.

Expected behavior during market hours:

1. First pass after entering market hours: 1h scanner lane is immediately eligible.
2. Subsequent passes: 1h scanner lane runs when approximately 3600 seconds have elapsed since last completion.
3. Real trigger time may be late by:
   - Up to one loop interval (about 30 seconds)
   - Plus runtime of earlier jobs in the same pass

## 14) CLI modes for controlled operations

- Normal continuous mode: run_scheduler
- EOD-only one-shot: --eod-once
- Daily failure queue retry one-shot: --retry-daily-failures
- Deep hourly repair one-shot: --hourly-deep-once [--force]

These one-shot modes run and exit without entering continuous loop.

## 15) Current interval separation policy

- Intraday scanner lane in scheduler: 1h only
- EOD scanner lane: 1d + 1h (and 1wk on Fridays)

This separation avoids mixing close-confirmed weekly logic into intraday run cadence while still preserving end-of-day completeness.
