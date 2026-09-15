# Equity Context For Option Contract Alerts

Status: **READ-ONLY SHADOW REPORT IMPLEMENTED. NO MEASURED PERFORMANCE CONCLUSION YET.**

This separately authorized diagnostic connects stock setup evidence to existing
option-contract candidates. It does not change production detection, candidate ranks,
execution gates or the running 21-session equity paper study. No alerts or orders
are sent by the report, and there is no new frontend view or background worker.

## What Is Connected

Each retained alert record includes its ordered contracts, strikes/expirations, entry
IV and Greeks, premium/payoff economics, original rank, event state and complete
execution-gate ledger. It also includes the latest decision-visible daily equity
setup's direction, evidence ID, source version and market/observation/creation times.
The full equity evidence payload is retained separately for source inspection.

Equity context is classified as **ALIGNED, OPPOSED, NEUTRAL, CONFLICTED, UNAVAILABLE**
or **NOT_DIRECTIONAL**. Raw volume/OI, flow and other research-only detector findings
remain in the report rather than being converted into bullish/bearish trade advice.
An option activity alert may be a hedge; stock-direction disagreement is not a reason
to erase it.

The initial evidence source is the existing daily `TRADE_SETUP`, not the experimental
ridge score. All context is labelled **unqualified research evidence**, regardless
of whether a source has a qualification reference. In particular, `ALIGNED` never
changes an `EQUITY_DIRECTION UNAVAILABLE` execution gate into PASS.

This first version tests a direction-alignment filter and abstention, not a new
strike-ranking algorithm. Existing option economics decide the package ordering.
Candidates of the same direction commonly share the same equity alignment, so the
equity arm often either keeps the option-only selection or abstains.

## Frozen Comparison

- Read selected candidates from the current strategy policy over the last 60 calendar
  days. Runtime strategy and valuation hashes are recorded with every report.
- Keep the first observed matrix per underlyer, strategy, structure and exchange
  session. Retain its original candidate ordering; later snapshots cannot improve
  the chosen decision retrospectively. This limits repetition but does not make
  overlapping periods or recurring contracts independent observations.
- For each **5, 10 and 21 subsequent XNYS-session close**, require every leg to expire
  strictly after the planned exit date. This is not DTE matching by calendar days,
  a 0-DTE study, or a shorter-horizon refit of an equity model.
- Option-only arm: the first horizon-eligible package in the unchanged ordering.
  Equity arm: the first ALIGNED horizon-eligible package, otherwise an explicit
  zero-return cash abstention. Directional long-premium and debit-spread strategies
  are the initial comparison scope; other candidates remain contextual records.
- Use only daily equity evidence whose market time is no later than option market
  time, and whose actual observation and creation times are no later than candidate
  observation. Reject reconstructed, stale, expired, invalidated and conflicted
  directional evidence. Maximum age is one exchange-session boundary.
- Preserve the original option gates, including quote, event, risk-engine and
  read-only restrictions. Existing short-window event checks do not establish full
  5/10/21-session event coverage, which is separately labelled UNVERIFIED.

The 60-day window follows the existing option outcome retention window and keeps
entries available after a 21-session exit matures. It was corrected from an initial
30-day implementation before any measured outcome existed; selection and valuation
rules did not change. Repeated matrices are reduced in SQL before the 50,000 retained
candidate safety bound. Exceeding that bound aborts rather than truncating a cohort.

This is an as-of retrospective join of persisted records, **not proof that the
combined alert was issued live at that historical moment**. Candidate creation times
are exposed. The option-only arm is an existing candidate-set baseline, not a replay
of detection with all original equity filtering removed.

## Measuring Option Outcomes

The report reuses the existing versioned option payoff/commission calculation,
without writing to production outcome tables or extending their measurement enums.
Each measurement is explicitly identified by its shadow holding-session count.

Entry uses the saved, policy-compatible candidate leg marks whose source observation
was visible at decision. Legacy entries without valuation provenance remain missing.
Exit requires one coherent saved batch **scheduled at the target close**. Individual
snapshot quote/mark times may precede that close within the valuation policy's source
age and option/spot skew limits. A different 15-minute cycle, later session, changed
contract terms or incompatible policy is not substituted. The narrow implementation
does not support nonstandard contract multipliers/deliverables.

P/L uses actual persisted option marks, ordered leg sides/ratios/multipliers and the
existing commission policy. It includes recorded entry and exit IV for interpretation.
These remain **indicative delayed-mark outcomes**, not executable fills; bid/ask and
slippage limitations remain visible. Stock returns and theoretical option scenarios
are never used to invent missing option outcomes. American early exercise/assignment
is not simulated, and no realized trade or portfolio drawdown is claimed.

Summaries stay separate by strategy, structure and horizon. They report:

- All opportunity counts, expiry exclusions, missing states and equity abstentions.
- Paired measured outcomes where both arms are available.
- A separate same-opportunity comparison allowing explicit equity-arm cash abstention.
- Equal-entry-session mean return differences, alongside descriptive candidate means.
- Distinct entry-session and recurring-package counts, without p-values or calibrated
  win probabilities. Missing option outcomes never count as zero-return trades.

## First Run

The first report found **17,342 candidate snapshot rows**, reduced to **2,308 retained
candidate records** across the first decision matrices. Of these, **761 are raw
research-only detector alerts**. Context classifications are 147 ALIGNED, 142 OPPOSED,
216 CONFLICTED and 1,803 NOT_DIRECTIONAL. These counts include contextual strategies,
not just the directional measurement arms.

The measured strategy set spans only **two entry sessions**, September 10 and 11,
2026. There are 312 horizon-pair rows, not 312 independent experiments:

| Horizon | Cohort Opportunities | Expiry/Package-Eligible Option-Only Choices | Equity-Aligned Choices |
|---|---:|---:|---:|
| 5 sessions | 104 | 98 | 46 |
| 10 sessions | 104 | 96 | 45 |
| 21 sessions | 104 | 71 | 33 |

Eligibility counts above precede valuation-provenance checks. Among 265 distinct
selected candidate/horizon measurements, **138 lack the current entry valuation
provenance and 127 are not mature**. There are **zero measured return comparisons**.
The first policy-compatible target closes are September 18, September 25 and
October 12, respectively, subject to retained price coverage. This does not establish
that the equity filter improves alerts, nor favor one holding period over another.

The alignment join used 104 retained daily `equity_setup_v13` records. All 13,848
retained gate rows remain unchanged in the output. This demonstrates evidence linkage,
not execution qualification. Horizon-specific predictive calibration and richer
trigger/participation/risk features are not implemented by this first diagnostic.

## Operation And Verification

From the workspace root:

```powershell
& .\backend\.venv\Scripts\python.exe -u .\backend\scripts\report_option_equity_shadow.py
```

Use `--no-write` for a read-only preview. `--output` can keep a dated report rather
than replace the default derived report. This command is on demand; it has not been
added to the option worker or equity paper watcher. Existing ingestion must continue
retaining the relevant contract marks for later outcomes to become measurable.

Artifacts:

- [option_equity_shadow_config.json](option_equity_shadow_config.json)
- [option_equity_shadow_results.json](option_equity_shadow_results.json)
- Implementation: [equity_shadow.py](../backend/options/analytics/equity_shadow.py)
- Runner: [report_option_equity_shadow.py](../backend/scripts/report_option_equity_shadow.py)

The exact configuration, valuation-policy and input/code SHA256 hashes are recorded
in the generated result artifact.

**50 focused tests passed** across the new shadow functions, existing option outcomes
and execution gates. Tests cover future evidence, stale/conflicted setups, horizon
and expiry boundaries, coherent spread P/L, commission, changed terms, late snapshots,
scheduled-close batch selection, missing/abstention handling and read-only SQL.
The report records an actual `REPEATABLE READ, READ ONLY` transaction and input/code
hashes. Artifact checks verified causal joins, unique paired cohorts, unchanged gate
values and expiry-safe choices. No live rankings, historical equity results, running
paper-tracker records or database contents were modified by this diagnostic.

The next evaluation is to rerun this **same report** after policy-compatible target
closes become available, not to select new filters from the current empty outcome set.