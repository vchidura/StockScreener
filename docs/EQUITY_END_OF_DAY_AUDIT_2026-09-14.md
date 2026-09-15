# End-of-Day Data Audit: September 14, 2026

## Verdict

**REVIEW REQUIRED.** Closing equity prices and current analysis are usable and
internally consistent for the tracked universe, but the whole day's pipeline is
not complete. The main outstanding issues are intraday analysis continuity,
missing lower-interval bars, absent downstream publishers, historical accounting
exceptions, and incomplete closing option processing.

Read-only checks ran approximately 21:05-21:21 UTC (17:05-17:21 Eastern). Results
are checkpoints, not a guarantee about later worker activity. No ingestion,
repair, replay, publication, worker restart, or broker operation was requested.

## Equity Coverage

Scope: 386 currently active tracked instruments; regular session 09:30-16:00 ET.
This is not the whole market or a reconstructed historical universe.

| Interval | Stored Session Bars | Expected Bars | Closing Analysis Members | Published Checkpoints |
|---|---:|---:|---|---:|
| 5m | 30,080 | 30,108 | 386 complete | 38 / 78 |
| 15m | 10,034 | 10,036 | 386 complete | 22 / 26 |
| 30m | 5,018 | 5,018 | 386 complete | 12 / 13 |
| 1h | 2,702 | 2,702 | 386 complete | 7 / 7 |
| 1d | 386 | 386 | 385 complete, 1 insufficient history | 1 / 1 |

- Every tracked ticker has a closing bar on all five intervals.
- ARKW is missing 27 five-minute windows and the 12:15 and 12:30 ET fifteen-minute
  windows. HUM is missing the 10:15 ET five-minute window. Times identify bar starts.
  No provider request was made to distinguish provider omissions, no-trade windows,
  or ingestion defects; missing bars were not fabricated from neighboring prices.
- VMRK has 19 daily source bars, below the setup's 50-bar minimum. Its daily setup
  is correctly unavailable; today's daily price itself is present.
- All 386 daily and 2,702 hourly bars matched OHLCV recomputed from their recorded
  30m source revisions, with matching ticker/security identity and source bounds.
- All 385 daily setup prices match their exact source closes within the documented
  cent rounding. Maximum rounding difference is $0.005; source times also match.
- Current RBLX 30m/hourly/daily setups all read $51.28 and report READY.

The missed 30m analysis checkpoint ends at **13:00 ET**. Its retained run failed
with 266 members marked `ANALYSIS_RUN_LEASE_EXPIRED`. The four absent 15m analysis
checkpoints end at 13:15, 13:45, 14:15, and 14:45 ET. Forty 5m analysis checkpoints
were not published. Stored price history and published analysis history are
different coverage measures; the closing success does not restore missed signals.

## Storage And Analysis Integrity

The full storage validator found zero invalid OHLCV rows, derived bars without
lineage, unresolved source-bar links, publication-count mismatches, or current
projection/evidence run mismatches. All five closing analysis cohorts have exact
member-to-evidence counts: 5m 432, 15m 463, 30m 2,777, hourly 2,832, daily 3,178.

The validator nevertheless returns **FAIL** for 33 older analysis runs: eight
from September 10 and 25 from September 11, with 88 more persisted evidence rows
than the failed-member counters record. All affected members are PNC. Their
recorded errors include PostgreSQL LZ4/PGLZ decompression and allocation failures.

This is not a newly demonstrated corruption incident. The damaged PNC feature row
was repaired on September 13, and the retained post-repair audit decoded 595,488
evidence rows with no affected rows/pages. Today's PNC closing members are all
COMPLETE. See [the repair verification](EQUITY_SIGNAL_EVIDENCE_SCORECARD.md#repair-verification).
The historical partial-evidence/member-counter discrepancy remains unresolved.
Do not delete evidence or rewrite failed outcomes merely to make a counter pass.
The original storage damage cause remains unknown; this audit is not a physical
database, disk, memory, or backup-restoration certification.

## Portal And Downstream Publications

- Core materialization API: READY for all scheduled intervals, no stale intervals.
- The 20 standard portal snapshots match their source generation. The additional
  `SCREENING_DAILY_V1` snapshot follows its separate publication contract.
- Stock Screener still uses the September 11 daily anchor, despite having
  September 14 16:00 ET hourly context. Latest snapshot:
  `8f228b25-86e1-55dd-9516-beb53b873b08`, generated at 16:25:24 ET.
- Forward Stock Alerts last published the 15:30 ET boundary at 15:58:46 ET, selecting
  two plans. Its last worker check was 16:14:34 ET, and its next boundary remains
  16:00 ET. No closing run was retained by this audit, after the closing deadline.
- Process inspection found the equity, portal, corporate-action, market-event,
  option-input, options, API, and Vite services, plus legacy stock discovery.
  The separate Screening and forward Stock Alerts worker processes were absent.
  Their absence explains why current core data does not imply updated downstream views.
- The screening verifier stops at the frozen replay source-hash assertion:
  `research/stock_idea_models.py` differs from the earlier replay manifest. This
  establishes code-version drift, not changed replay input data or incorrect current
  prices. The verifier did not reach all subsequent source-lineage checks, so a full
  screening verification PASS is not claimed. The frozen manifest was not edited.

## Options Check

The 13 configured underlyings have retained COMPLETE matrices under the current
configuration/policy, but they are not a complete closing cohort. Latest scheduled
cycles are 15:00 ET for MSFT/NVDA/PLTR/SOFI/TSLA and 15:15 ET for the other eight.
SPY, QQQ, and IWM also have `EXECUTION_LAG_EXCEEDED` on those late-processed matrices.
Their newer market timestamps do not turn the older scheduled cycles into closing runs.

There are 16 TERMINAL_FAILED normalization work items created today, including
the 16:00 ET items for all ten configured individual stocks. Today also has 283
completed normalization items, 283 completed strategy items, and 4,245 pending
trade-classification items. Pending classification is reported as inventory, not
proof that every item should have been consumed by the currently enabled engine.
Option failure causes and complete matrix valuation integrity require separate
investigation; this audit does not certify option recommendations or execution.

## Priorities

1. Restore the intended Screening and forward Stock Alerts service lineup under
   explicit operator approval; preserve missed closing alert history without backdating.
2. Investigate closing option normalization failures and late cycles.
3. Resolve intraday scheduling throughput/checkpoint gaps, then investigate the
   bounded ARKW/HUM price omissions without conflating prices with past analyses.
4. Reconcile historical partial-evidence accounting and the frozen/current code
   verification boundary without rewriting historical facts.

## Reproduce

From the repository root, these commands only read retained data:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\validate_equity_storage.py
.\backend\.venv\Scripts\python.exe .\backend\scripts\audit_equity_session.py --session 2026-09-14
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_screening_worker.py --status
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_stock_idea_worker.py --status
.\backend\.venv\Scripts\python.exe .\backend\scripts\run_option_pipeline.py --status
```

The session audit prints `PASS` or `REVIEW_REQUIRED`; exit code zero means the
report completed, not that every data-quality check passed. It compares against
the current tracked universe and does not independently download or verify
provider prices, re-run detector algorithms, or repair any reported gap.