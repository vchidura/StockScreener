# Equity models: offline Model Lab, new live Stock Alerts rules, stock quotes

Prepared November 8, 2026 (updated 08:00 UTC). Status: design for review; nothing built; uncommitted. Companions:
[research roadmap](EQUITY_MODEL_DETECTION_AND_RESEARCH_ROADMAP_20261024.md) (model cards, method) and
[V2 detector implementation contract](ADVANCED_REALTIME_EQUITY_V2_DETECTOR_IMPLEMENTATION_PLAN_20261024.md)
(full portal contract).

## 1. Decisions (user, Nov 8)

- **Model Lab is fully offline.** Evaluating the models, collecting their results and passing them happen on history
  files, outside the realtime system, with no portal page (section 5).
- **The new model rules replace the current rule in live Stock Alerts**, independent of the evaluation, to observe how
  they run and what alerts they raise, reviewed on the existing Stock Alerts page. No separate worker or page is built
  for this (section 6). Target: Monday Nov 9 before the open, behind a gate.
- **Live stock quotes and trades** come later through the same Equity pipeline (section 8); not on Monday.
- The legacy database is the primary research source; provider downloads fill gaps (approved).
- P1/P2 (Position) wait for the 400-ticker stage and point-in-time fundamentals.
- Alert-only, as in the roadmap: no paper entries, simulated fills, positions or broker orders.
- Changes stay uncommitted until you review them.

### 1.1 How the pieces fit

```
 MODEL LAB (offline, on demand)                         LIVE STOCK ALERTS (every window)
 legacy DB (read-only) + provider reference data         realtime DB: 30m/1h/1d bars (rt.bar_head), actions
            │                                                         │
            v                                                         v
 history files ──> research input ──> new models + controls   adapter ──> same research model code (29 rules)
            │                                                         │
            v                                                         v
 evaluation report + labels (CANDIDATE / REJECTED / ...)      evaluations, states, alerts ──> Stock Alerts page
            │
            └──> your decisions: which rules stay live, which are switched off
```

- Both sides run the same model code (`research/equity_models`), so what you see live is what Model Lab evaluates.
  Nothing from live feeds the offline evaluation.
- Qualification is per model, not per alert. Each live alert is checked inside its own evaluation (inputs complete,
  every gate passing, valid stop/target, evidence stored); if anything is missing the result is UNAVAILABLE, not an
  alert. Until Model Lab reports, live alerts from these rules are unvalidated.

## 2. What exists and what is reused

| Code | State (checked Nov 8) | Role |
| --- | --- | --- |
| `research/stock_idea_*`, `strategy_v2.py` | The legacy three models (resumption, acceptance, reversal; Quality V2). Measured Sep 15-Oct 23: net -4 to -9 bps per intraday plan at 10 bps cost. `stock_idea_replay.py` replays them over a bundle of bars | Offline controls |
| `research/equity_models/` + `scripts/run_equity_detection_backtest.py` (Oct 25) | All roadmap cards implemented for detection and gross forward returns: 37 registrations. Stateful per security and model (`evaluate_research(snapshot, model_id, previous_state)`), causal input contract, `--macro-policy none` supported. 33 + 8 tests pass. Never run on real data | **The model code**, offline and live |
| `scripts/prepare_equity_detection_history.py`, `report_equity_detection_readiness.py` | Read-only legacy inventory and readiness evidence; does not export bars | Reused for inventory |
| `equity/model_detection/` + `realtime/canonical_stock_alert_*` | Live Stock Alerts: built by the dedicated service `realtime-v2-canonical-stock-alerts` (`scripts/run_realtime_v2_canonical_stock_alerts.py`), one batch per window receipt, versioned detector identity, per-security state, evidence, publication. Runs one placeholder policy, `completed_bar_breakout_reference` v2 on current-session 5m bars. 50 + 19 tests pass | Host for the new rules; placeholder kept in code for rollback and as an offline control |
| Branch `agents/wp3-canonical-stock-alert-detector` | Nov 4 snapshot, superseded | Not used |

## 3. Data

### 3.1 Provider facts (verified Nov 8)

| Data | Our access (Stocks Advanced) | Notes |
| --- | --- | --- |
| Minute aggregates, 2+ years back | Yes, with per-bar `vw` VWAP | Plan lists 20+ years |
| Stock NBBO quotes, historical and last | Yes | Bid/ask price, size, exchange, sequence, SIP and participant timestamps |
| Stock trades, historical | Yes | Price, size, exchange, conditions, sequence, timestamps |
| Stock WebSocket channels | `AM`, `A`, `T`, `Q`, `LULD`, `NOI` | Real-time |
| Depth of book (Level 2) | **Not offered**; quotes are NBBO only | C2 cannot be built on this provider |

### 3.2 Legacy database (primary research source)

The legacy platform (`stocks_db_fresh`, other repository, Developer plan, 15-minute delayed) holds, per its Oct 25
inventory (`backups/equity-detection-history-20261025/data-profile-expanded.json`):

| Data | Coverage | Use |
| --- | --- | --- |
| 30m and 1h bars with VWAP | 2021-09-03 to 2026-10-23, about 385 stocks | R2/B3/F2, SW3, SW1 |
| Daily bars, raw and adjusted | Since 2021-09 | Context, warmup, 5/10/21-session returns |
| SPY and sector ETF daily bars; sector mapping | Since 2021 / 2022 | Market and sector context |
| 5m / 15m / 1m bars | Since Jul / Jun / Aug 2026 | D1/D2 only |
| Earnings calendar, Fed dates | From Sep 2026 | SW1 only for recent periods |
| Fundamentals | None | P1/P2 later |
| Split/dividend coverage | Certified only recently | **The Oct 25 blocker**; certify from provider reference data |

Intraday legacy bars labeled `LIVE_OBSERVED` back to 2021 are treated as `HISTORICAL_RECONSTRUCTED`. Research reads
the legacy database read-only and never writes to it.

## 4. Models and where they run

| Models | Model Lab (offline) | Live Stock Alerts |
| --- | --- | --- |
| R2/B3/F2 at 30m and 1h (structure, context, participation, combined), B3 retest | Yes | Yes, 26 rules |
| SW3-R/B/F (hourly Swing) | Yes | Yes, 3 rules, with earnings coverage optional (live earnings calendar is N/A after Nov 9, as decided) |
| SW1 | Recent periods only | No (needs the earnings calendar) |
| Legacy three, placeholder breakout | Yes, as controls | Placeholder switched off (kept for rollback) |
| D1, D2 | 5m from Jul 2026; quote gate needs historical NBBO | No, until live stock quotes (section 8) |
| C1 | Bounded historical NBBO/trades samples | No |
| SW2, P1/P2 | Later | No |
| C2 | Not feasible (needs depth of book) | No |

## 5. Model Lab: offline evaluation

### 5.1 Protocol (frozen before any result is seen)

- **Splits** on the legacy history: development 2021-09 to 2024-12, validation 2025-01 to 2025-12, holdout
  2026-01 to 2026-10 (opened once, at the end). The 5m wave (from Jul 2026 only) gets its own declared split.
- **Comparisons** on common eligible opportunities: R2 vs legacy resumption, B3 vs acceptance, F2 vs reversal; SW3
  vs daily-direct swing; all also vs the placeholder breakout. Every variant is reported; the four variants of a family
  are one multiple-testing family.
- **Measures:** episodes, coverage (UNAVAILABLE share), gross signed return at the intended hold (60/120/240 minutes
  intraday) and at 5/10/21 sessions, net at 4/10/25 bps, hit rate, per-ticker and per-month breakdowns,
  block-bootstrap intervals by session.
- **Labels** (research labels, not investment qualification):
  - CANDIDATE: in both development and validation, net at 10 bps (intraday) or the 5/10-session return (swing) is
    above zero and above its control; at least 30 validation episodes; the sign holds when any one ticker or month is
    left out.
  - REJECTED: below zero or below its control in both development and validation.
  - RESEARCH: anything else. UNAVAILABLE: inputs missing.

### 5.2 What is needed to run it

| Need | Detail |
| --- | --- |
| Legacy database, read-only | Credentials from the legacy repository's `backend/.env` (as `prepare_equity_detection_history.py` uses); session `default_transaction_read_only=on`, repeatable read, statement timeout |
| Provider key (Advanced) | Only for split/dividend reference data and gap downloads; request counts recorded in the run manifest |
| Python environment | The backend image or `.venv`; no network except the export and certification steps |
| Disk | About 2 GB of history files for 385 names over five years; runs a few hundred MB each |
| Output root | `backend/backups/model-lab/` (not committed): `history/<snapshot>/`, `datasets/<id>/`, `runs/<id>/` |

### 5.3 How to run (commands; status of each script)

| # | Step | Command | Status |
| --- | --- | --- | --- |
| 1 | Export legacy history to files | `scripts/model_lab_export.py --env-file <legacy .env> --universe live25 or legacy-all --start 2021-09-01 --end 2026-11-06 --output history/<snapshot>` | **To build** |
| 2 | Certify splits and dividends | `scripts/model_lab_certify_actions.py --history history/<snapshot> --key-file <provider key>` | **To build** |
| 3 | Build the research input | `scripts/model_lab_build_dataset.py --history history/<snapshot> --output datasets/<id>/input.json.gz` (bars plus eligibility, benchmarks, relative strength, events and reversal facts; shares the fact builder with the live adapter, section 6.3) | **To build** |
| 4 | Readiness per model | `scripts/report_equity_detection_readiness.py` against the new input | Exists; adapt input |
| 5 | Freeze the protocol | `runs/<id>/protocol.json`: splits, models, controls, measures, label rules; its hash goes into every later artifact | **To build** (small) |
| 6 | New models | `scripts/run_equity_detection_backtest.py --input datasets/<id>/input.json.gz --models <list> --macro-policy none --output runs/<id>/challengers` | Exists |
| 7 | Controls | `scripts/model_lab_controls.py --input ... --output runs/<id>/controls` (legacy replay detections without paper fills; placeholder breakout through `equity/model_detection`) | **To build** |
| 8 | Evaluate | `scripts/model_lab_evaluate.py --run runs/<id>` writes `evaluation.json`, `labels.json`, `REPORT.md`; refuses the holdout unless `--open-holdout`, which it records | **To build** |

Start with the 25 live tickers, then all about 385.

### 5.4 Results and passing

- A run folder holds `manifest.json` (inputs, code and protocol hashes), the challengers' and controls'
  `evaluations.jsonl` and `outcomes.jsonl`, `evaluation.json`, `labels.json` and `REPORT.md`. Identical reruns
  reproduce it; different artifacts are refused rather than overwritten.
- You review `REPORT.md` and record a decision per model in `runs/<id>/decisions.json` (model, label, decision,
  reviewer, date): keep it live, switch it off, or (for a model not yet live) switch it on.
- Rerun when a model changes, the history grows (for example monthly) or the protocol changes; a new protocol is a
  new run, never an edit of an old one.

## 6. New rules in live Stock Alerts (target Monday Nov 9)

### 6.1 Approach

The dedicated Stock Alerts service runs the research model code directly, through an adapter, in place of the
placeholder breakout. No port into `equity/model_detection` is needed to go live, and the live code is the evaluated
code. The change is confined to that one service; Equity, Options and every other service stay on the Monday bundle
(r55).

### 6.2 Rules and defaults

- **29 rules:** R2/B3/F2 (four variants each at 30m and 1h), B3 retest at 30m and 1h, SW3-R/B/F.
- **Securities:** the 16 live stocks. The 9 ETFs (SPY, QQQ, IWM, SMH, XLC, XLE, XLF, XLK, XLY) are market and sector
  benchmarks.
- **Events:** macro gate off (`macro_policy=none`; only Fed dates exist). SW3's earnings-coverage requirement is
  optional, as live Stock Alerts made events optional on Nov 5; this is a distinct registration (its policy hash
  differs from the research one), and the gate reads NOT_APPLICABLE.
- **Known gap:** the software sector ETF (IGV) is not in the live universe, so context and combined variants read
  UNAVAILABLE for MSFT, ORCL, CRWD and PLTR (and GOOGL, whose retained sector has no mapped ETF). Adding IGV is a
  universe change and needs your yes. F2 context and combined also need a reversal-context fact (whether a breakout
  or a scheduled event explains the move) that nothing defines yet, so they read UNAVAILABLE when they reach it.
- **Cadence:** a rule is evaluated when its own bar completes (30m rules every 30 minutes, 1h and SW3 rules on full
  hours anchored to the open); other windows skip it quickly.

### 6.3 Changes (Stock Alerts service code only)

| Piece | Change |
| --- | --- |
| Input | Load completed 30m, full-hour and daily bars by instrument from `rt.bar_head` (history: at least 200 trigger bars and 253 daily bars), plus SPY and sector ETF bars, at the window's cutoff and known-by time |
| Fact builder | Eligibility (membership, corporate-action coverage, price basis), benchmarks (SPY; sector ETF from `research/gics_sectors.py`), relative strength (rs63, percentile63 among the live stocks, sector_rs20/63), events, reversal context; one builder shared with Model Lab step 3 |
| Evaluation | One batch per rule per window, each with its own detector version and per-security state (the research state stored as the state payload); one rule's failure never blocks another |
| Alerts | Each MATCH becomes a Stock Alerts row: model and variant, style, direction, trigger, stop, target, suggested hold, entry deadline (Swing: next session close), gates as evidence |
| Publication | Same tables and product publication; the batch and publication hash the same inputs (the B18 lesson) |
| Placeholder | Switched off, kept in code |

### 6.4 Verification before going live

- Unit tests: fact builder, state round trip, per-rule isolation, alert mapping, cadence.
- Replay Friday Nov 6 windows through the new service code on a copy of live: alerts per rule, UNAVAILABLE reasons,
  and **parity**: the same decisions as the research backtest run on the same bars.
- Read the alerts back through the Stock Alerts API on the copy; check the page fields the frontend uses.
- **Time budget:** added time per window measured on the copy; the service must finish inside its window deadline
  without delaying Equity or Options.

### 6.5 Deploy, gate and rollback

- Sunday evening (after 20:00 UTC): the Monday bundle r55 deploys unchanged.
- **Gate: Monday 12:00 UTC.** If 6.4 is green, the Stock Alerts service alone moves to the new image by hotfix before
  the 13:30 UTC open. If not, Monday runs the placeholder and the new rules go in after Monday's close.
- Rollback at any time: hotfix that service back to r55.
- These rules are unvalidated: Monday shows how they run and what they raise, not whether they make money.

### 6.6 As built and rehearsed (Nov 8, uncommitted)

**Code.** `realtime/research_rule_input.py` (input adapter: point-in-time bars from `rt.bar_head`, the 15:30-16:00
hourly fragment dropped, split-based continuity segments, prior-day facts, feature frames cached per security and
time, bars indexed per security and interval); `realtime/canonical_stock_alert_research.py` (the batch);
`canonical_stock_alert_repository.build_canonical_stock_alert_batch_cursor` switches to it unless
`REALTIME_V2_STOCK_ALERT_RULES=PLACEHOLDER`; `research/equity_models` gains `earnings_policy` (left out of the hash
when "required", so existing registrations keep their hashes); tests `tests/realtime/test_research_rules_live.py`.

**Shape forced by the schema.** `rt.canonical_stock_alert_evaluations` is unique per batch and security, so each
window has **one evaluation per member**: every rule due for that stock is evaluated in rule and bar order and
summarized in it. A match makes it the alert, named after the rule; another rule matching the same stock in the same
window is listed with it (`also_matched`, for example TSLA B3_1h_structure with SW3_B). Otherwise it is NO_MATCH with
counts of rules evaluated and unavailable, or UNAVAILABLE only when every due rule lacked inputs (which, like the
placeholder, writes a non-actionable observation row). One state per stock holds each rule's research state and how
far it has processed, so an unavailable bar is not evaluated again; each cohort starts at the session open.

**Rehearsal on a copy of live** (service code as the app role: claim receipts, build, persist, then the Stock Alerts
page snapshot):

| Session | Windows | Result |
| --- | --- | --- |
| Nov 4 (9 members) | 78 | 3 alerts: NVDA B3_30m_structure short 15:30 UTC, TSLA B3_1h_structure short 16:30 (also SW3_B), MSFT R2_30m_structure long 20:00 (also R2_30m_participation); identical to the research code on the same bars; page rows show rule, style, interval, suggested hold, entry deadline; image r56 code, 1.5 s per window typical |
| Nov 6 (25 members) | 94 | No alerts: after the missed 14:40 window the 30m/1h bars did not exist until the 23:46 fill, so every rule read TRIGGER_WARMUP_REQUIRED (the research contract never bridges a gap); 2.5 s typical, 4.0 s max |

**What to expect Monday.** The rules are selective: over 21 sessions on the 16 stocks they raised 6 alerts, so
Monday may have none or a few. A gap in today's bars (a missed window) makes the rules unavailable until it is
filled; B20 now redoes a missed window within about 10 minutes.

**Watch items.** The service holds the claimed window row while it builds (about 1.5-4 s, against about 1.4 s
average for the placeholder); compare Equity window times with Friday's. The page's Hits column stays empty for this
source (the canonical snapshot reports no recurrence data); repeats show as separate rows (see 6.7).

### 6.7 Repeats on the Stock Alerts page

- **Same rule, same setup:** alerted once. A rule remembers the setup it alerted (its anchor) and never alerts it
  again, in later windows or later bars.
- **Same rule, a new setup later:** a new row with its own trigger time.
- **Different rules, same stock, same window:** one row named after the first rule, the others listed with it.
- **Different windows, same stock, side and style (user, Nov 8):** one row per session. The first alert stays the
  row (its trigger, stop and target); later matches fold into it as plan variants ("rule1 + rule2 / N plans", listed
  in the details), and **Hits** counts the windows in which any rule matched that stock and side, with first and last
  seen and the rules that matched. Built: each research alert carries `episode_group` (session, stock, side, style);
  `research.stock_alerts.collapse_alert_rows` folds by it, keeping the earliest alert as the row; the canonical
  snapshot (`stock_alert_projection.canonical_hit_observations`) reports every matched window as a hit, for the
  placeholder rule too. No frontend change: the page already shows folded rows, variants and hits (the Hits column
  is in the column picker, hidden by default; the details show the count).
- **Tabs:** Latest shows only the newest window's alerts (unchanged page behavior); a session's alerts stay in
  History and Open. An alert's status stays OPEN after its entry deadline (also unchanged; the deadline is in the
  row's details).
- **Rehearsed Nov 4 (image r56):** History and Open show the 3 alerts with Hits 1 and first/last seen.

## 7. Native port (later, optional)

When Model Lab labels a rule CANDIDATE and you keep it, it can be reimplemented natively in `equity/model_detection`
(new feature version with Wilder ATR14, ADX14/DI, slot and cumulative RVOL20, close location, session VWAP), with a
parity test against the research code, for speed and long-term maintenance. The model registry and activations (the
Equity part of G102 A3) then switch rules on and off at a window boundary, off-hours, after the previous close is
COMPLETE.

## 8. Live stock quotes and trades

### 8.1 What exists

- The preflight design (`ADVANCED_REALTIME_PREFLIGHT_DESIGN.md` section 3) already targets stock `T`, `Q` and `AM` for
  the admitted tickers "subject to verified access and measured capacity", with quotes needed because minute bars
  alone cannot show stock-side aggression.
- The stream machinery handles trades and quotes today for Options (socket, ingress, journal, flow and market-state
  consumers), and `MarketChannel` has `TRADE` and `QUOTE`.
- For stocks it is locked to minute bars: the Equity ingress (`advanced_equity_ingress.py`) and the runtime profile
  (`advanced_ingress_runtime_profile.py`) accept only `("STOCKS", ("AM",))`; there is no stock-quote consumer or table.
- Our account is entitled to stock NBBO quotes and trades in real time (verified Nov 8).

### 8.2 Plan

| Step | Work |
| --- | --- |
| Q1 Capacity | Measure one session of NBBO (and separately trades) for the live names from provider history: events per second at peak, journal bytes per day, CPU to journal and derive. Decide names and channels from the numbers |
| Q2 Connection | Confirm the account's stock connection allowance; decide same socket as `AM` or a separate stocks connection so quote bursts cannot delay minute bars |
| Q3 Pipeline | Allow `("STOCKS", ("AM", "Q"))` (then `T`) in the runtime profile and Equity ingress; quotes go to the journal; the bars bridge ignores them; a new consumer derives per-second and per-minute quote features (spread, best sizes and imbalance, quote age) into a new table; each evaluation that uses a quote stores that exact quote as evidence; retention for the new table |
| Q4 Rehearsal | Replay recorded quotes through the pipeline on a copy; verify minute bars and Equity windows are unaffected |
| Q5 Live | Enable off-hours for a few names (for example SPY, QQQ and five stocks), measure, then widen |
| Q6 Consumers | D1/D2 live (quote freshness and spread gates), spread-based costs, C1 research |

**Not for Monday.** It changes the Equity ingest path that Monday is testing, and Q1-Q4 are several days of work. No
rule going live on Monday needs quotes. Estimated about one week after Monday, then a staged enable.

## 9. Implementation path

| Step | Deliverable | When | Touches live | Needs your yes |
| --- | --- | --- | --- | --- |
| S1 | New rules in the Stock Alerts service (6.3) with tests | Today | No | No |
| S2 | Copy rehearsal: replay, parity, API read, time budget (6.4) | By Monday 12:00 UTC | No (copy) | No |
| S3 | Hotfix the Stock Alerts service to the new image (6.5) | Monday before 13:30 UTC, if S2 is green | Yes | Yes |
| M1 | Model Lab export, action certification, research input, readiness (5.3 steps 1-4) | After S3 | No | Granted (read-only legacy, provider reference data) |
| M2 | Protocol, controls, evaluation and report (steps 5-8) | After M1 | No | No |
| M3 | Your decisions per rule | After M2 | Switching rules | Yours |
| Q1-Q6 | Live stock quotes (8.2) | From about a week after Monday | Yes, staged | Yes |
| Later | Native port (7), SW1 live, D1/D2 live, SW2, P1/P2 | - | - | - |

## 10. Risks

- **Unvalidated rules publishing live.** Monday's alerts come from rules that Model Lab has not judged yet; they are
  for observation, and the Stock Alerts rows name the rule that raised them.
- **Speed.** The research code is plain Python built for offline runs; 29 rules on 18 stocks must fit the window
  deadline. Shared features per security and evaluation only when a rule's bar completes keep it bounded; S2 measures
  it, and the service stays isolated from Equity and Options.
- **Alert volume.** 29 rules may raise many alerts; the per-rule counts in S2 show what to expect.
- **Small universe live.** Percentiles among 18 stocks are coarse; Model Lab's 385 names carry the statistics.
- **Multiple testing offline.** Frozen splits, a one-time holdout and reporting every variant are the guard.
- **Reconstructed vs observed.** Legacy history is complete in hindsight; live data arrives late or with gaps.
