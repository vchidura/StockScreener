# Backtest Walkthrough

How a scanner goes from a price bar to a published verdict, followed end to end on one real
signal. This is the explanatory companion to
[SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md](SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md), which states
the architecture and retention contract, and
[SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md), which holds the registry and gates.

Every number below is taken from the `level_retest_rejection` daily study run on 2026-09-03/04
over 1,034 sessions, not from a constructed illustration.

## The Question A Study Answers

Not "did this trade make money" but "does this setup earn more than holding the same sector over
the same window, often enough that the result is unlikely to be luck". A scanner that returns 16%
in a sector that returned 10% has found 6%, and a scanner that fires 171,804 times has to be
judged on the distribution, not the best case.

## The Pipeline

Seven stages, driven by `scripts/run_composite_study.ps1`. Only stage 1 and stage 3 call Polygon;
everything after reads the database, and the outcome runner is constructed with a `None` client so
a network call would raise rather than silently succeed.

| Stage | Script | Produces |
|---|---|---|
| 1 | `prepare_historical_signal_research.py --persist --backfill-actions` | Point-in-time universes, corporate actions |
| 2 | `prepare_historical_signal_research.py --backfill-sector-references` | Sector classification for members |
| 3 | `ingest_adjusted_daily_bars.py --from-reconstructed-universes` | Split-adjusted daily bars, including benchmarks |
| 4 | `run_historical_signal_research.py --adjusted` | Signal events as JSONL |
| 5 | filter step in the runner | Events for the one studied scanner |
| 6 | `run_historical_signal_outcomes.py --all --adjusted` | Evidence, outcomes, qualification |
| 7 | `run_historical_signal_outcomes.py --status` | Verification of the published record |

## Step 1: The Universe Decides Eligibility

Before a scanner looks at anything, each session asks whether the ticker was tradable *at that
time*: above $5, median dollar volume above $20M over the prior 20 sessions, common stock, active.
Membership is rebuilt per session from the reference data as it stood then, so a company that was
illiquid in 2022 and liquid in 2025 is only eligible in 2025.

This is the defence against survivorship bias, and it is not cosmetic. The 2026-09-03 replay
rejected **147,651** candidate ticker-days as `NOT_IN_UNIVERSE`. A fixed present-day cohort would
have silently accepted all of them, and would also have excluded every company that has since
delisted.

## Step 2: The Scanner Reads One Bar

Ticker `PI` (Impinj), session 2023-01-06:

```
trigger            fvg:bullish_strong_close
reference level    109.59        fair-value gap left 37 bars earlier
close at signal    112.31
atr at signal      5.30
volume_ratio       1.11          range_ratio 1.32
prior_level_tests  11
```

PI had left an unfilled gap near 109.59. Price returned to that level and closed strongly upward
off it, on above-average volume and range: the level was tested and held. That is the
`level_retest_rejection` hypothesis.

The scanner also records where it would be wrong (stop 104.74) and what it targets (127.46). Both
come from the ATR at that instant; nothing about the future is used.

Detectors are verified for exactly this property. `scripts/verify_composite_detectors.py`
recomputes each signal on history truncated at the signal bar and requires an identical result -
240 sampled bars, zero differences.

## Step 3: Entry Cannot Use The Signal Bar

The signal is stamped at the close of 2023-01-06, 21:00 UTC. Entry is:

```
entry   2023-01-09 14:30 UTC  @ 113.60
```

The next session's open, because 7-8 January was a weekend. Not 112.31, the close it was detected
on: that price is what *created* the signal, so it was not purchasable. Entering at 113.60 makes
the study pay the overnight gap, which here moved against the trade before it started.

This is `NEXT_ACTIONABLE_BAR_OPEN_V1`, enforced in three independent places - the SQL path filter,
a Python guard, and a `CHECK (entry_time > signal_time)` constraint in the schema.

## Step 4: Hold The Declared Horizon

Horizons are fixed before the run. For daily composites they are 5, 10 and 21 sessions. On the
`10d` lane:

```
exit              2023-01-23 21:00 UTC  @ 132.34
gross return      +16.50%
estimated cost     -0.04%     4 bps round trip
net return        +16.46%
```

Signals too recent to have completed a horizon are held back by a maturity cutoff rather than
scored early, which is why the 21d lane has fewer rows than the 5d lane.

## Step 5: Subtract What You Would Have Earned Anyway

```
SPY  returned  +2.63%   ->  market alpha  +13.83%
SMH  returned +10.43%   ->  SECTOR ALPHA   +6.02%   <- scored
```

January 2023 was a large semiconductor rally. SMH, correctly matched to PI's sector, gained 10.4%
over the same window, so most of the 16% was owed to being a semiconductor stock rather than to the
setup. The study credits **+6.02%**.

Sector-primary benchmarking is the whole point of the exercise, and it fails silently if the
benchmark series is absent. In the first attempt at this study the benchmark ETFs had been excluded
from the adjusted lineage by a common-stock filter, every alpha column was null, and qualification
published zero revisions. Before committing to a long evaluation, confirm alpha is actually
populated on the first few thousand rows.

Tickers with no sector - typically foreign private issuers, who file 20-F and so carry no SIC code -
fall back to SPY for the sector leg. In this cohort that is 388 of 3,094 tickers, about 5.8% of
scored rows.

## Step 6: One Trade Proves Nothing

PI's +6.02% is a single observation among 171,804. Aggregation is where a naive backtest usually
goes wrong, and two rules matter most.

**Same-instant signals collapse to one observation.** If 40 stocks trigger on 2023-01-06, that is
one period, not 40, because they share the same market news and would rise or fall together.
Counting them separately inflates the sample roughly 40-fold and makes noise look significant. The
study's ~171,804 signals reduce to about **1,034 independent periods**.

**The mean is then tested against zero** with a t-statistic on those independent periods.

## Step 7: The Gates

A lane is `ROBUST_PASS` only if all of the following hold:

- at least 100 events and 40 independent periods;
- mean net return positive with t > 2;
- mean alpha positive with t > 2;
- alpha positive in **both** the first and second half of the window - an edge that only worked in
  one regime does not count;
- survives Benjamini-Hochberg correction at q <= 0.05.

`MONITOR_ONLY` means every gate passed except FDR. Everything else is `UNRANKED`.

The FDR family must be fixed before the run. This study declares 16 lanes: one scanner, two
directions, three horizons, two exit models. The daily adapter emits all seven composite scanners,
so stage 5 filters the events file; qualifying the unfiltered file would silently create a
112-lane family and make a real effect roughly seven times harder to detect.

## What Is Kept

One row per lane in `equity_qualification_revisions`, carrying the verdict, sample size, alpha,
t-statistic, q-value, calibration curve, ticker breadth, top-5 concentration, the Wilson hit-rate
interval and the tested window. The 171,804 evidence rows and ~1.02M outcome rows beneath it are
working data and can be discarded:

```powershell
.\backend\.venv\Scripts\python.exe .\backend\scripts\purge_research_scanner_data.py `
  --source-prefix level_retest_rejection --exclude-production --apply
```

Keep `--drop-qualification` off unless the study itself is being discarded as invalid. Because the
supporting rows go away, anything a report needs must be computed at qualification time; after the
purge it cannot be recovered.

## Why A Positive Example Proves Little

PI returned +6.02% sector alpha and the setup looks convincing in isolation. That is exactly the
trap the machinery exists to avoid. The four intraday scanners tested with this same harness were
all `UNRANKED` - statistically indistinguishable from a random-direction control that was run
alongside them, which produced gross returns of about +/-5bps and negative net returns. A
lookahead-oracle control run through the same harness scored `ROBUST_PASS` on all six lanes with
t-statistics from 8 to 39, confirming the harness can detect a real edge when one exists.

A single good trade is fully compatible with a scanner that has no edge at all.

## This Verdict Is Not An Option Verdict

Qualification scores the mean sector-adjusted return over a fixed horizon. An option position
depends on the distribution and on timing, so a `ROBUST_PASS` here does not imply option
profitability and an `UNRANKED` does not rule it out. Only `qualified_direction` - a three-valued
enum, from the `5d` lane alone - ever reaches the option engine.

What equity research must add to become option-relevant, and how option demand should be studied in
its own right, are set out in [OPTION_RESEARCH_DESIGN.md](OPTION_RESEARCH_DESIGN.md).
