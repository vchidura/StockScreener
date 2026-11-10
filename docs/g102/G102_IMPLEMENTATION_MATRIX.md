# G102 implementation matrix

One row per design item, with one status. Design: `G102_TARGET_DESIGN.md` (section or invariant in the "Design"
column). Evidence: `G102_EVIDENCE.md` (dated entries). Procedures: `G102_RUNBOOK.md`.

**Statuses**

- **DESIGNED**: specified in the target design; no code yet.
- **IMPLEMENTED**: code exists (commit or worktree, image named) but the item is not yet proven by a live session or
  a rehearsal that exercises it.
- **VERIFIED**: proven by a live session or a rehearsal on a copy of live; the evidence entry is named.
- **DEFERRED**: consciously postponed; the reason and the trigger are given.

An item can be VERIFIED and still carry a known defect; the defect is its own row.

**This matrix is the only tracker.** Since Nov 10 the gap register (`docs/agent-context/advanced-realtime-integration-gaps.md`,
G1-G105) and the Nov 6 blocker register (`docs/BLOCKERS_NOV6.md`, B1-B25) are frozen history: their open items are
rows here (section 3 or the rows that name them), and new findings get a row here only. **Ids:** G-numbers come from
the gap register; B3 and higher (and B14a, B14b, B9c ...) from the blocker register; B0, B1/B2 in section 1 are G102
phase B steps, unrelated to the blocker register's B1 and B2 (which are A2.5 and the G93 bridge fix).

Last updated: 2026-11-10 09:40 UTC. Live images (since Nov 10 09:15-09:32, user's go): backend r66
`sha256:81e236e83b9e` (r63 + r64 + r65's Financials + A4.3, A4.6, R7) on all 19 services, frontend f21
`sha256:b4d3c1210837`; rehearsed on copies of live Nov 10 04:15 and 08:11. Previous images: `rollout/pre-r66-images-20261110.txt`
(r64, f19). First session on r66: Nov 10.

## 1. G102 items

| ID | Item | Design | Status | Code / image | Evidence | Next |
| --- | --- | --- | --- | --- | --- | --- |
| A0 | Options Alerts session list (G100) | 3 | IMPLEMENTED | live since r55 (Nov 8) | Nov 7 replay | First use with packages Nov 10 (O1 selection on in r63); VERIFIED after that session shows session rows |
| A0.1 | O1 packages valued under the session policy, unmarkable package recorded without a mark | 3 | IMPLEMENTED | r63 | Nov 10 unit tests | Live Nov 10; switch `REALTIME_V3_O1_PACKAGE_SELECTION` |
| A4.1 | `rt.bar_head` schema, migration 051 | 4.1 | VERIFIED | live Nov 8 | Nov 7 rehearsal; Nov 9 session; Nov 10 preflight | |
| A4.2 | Writer is head-only | 4.4 | VERIFIED | live Nov 8 | Nov 9 session; Nov 10 preflight (0 mismatched heads) | |
| A4.3 | Winner tuple, arrival-order independent (INV-2) | 4.3 | VERIFIED (copy) | r66 `sha256:81e236e83b9e`; live (r64) has the defect | Nov 10 DB tests 9 of 9 (12 of 12 at 07:40); replay A/B on two copies (04:15, 08:20): r65 moves 1 head and rewrites 3,000, r66 none (8,257 revisions) | Deploy r66 before the Nov 10 open (user's go); then runbook 8 (manual correction) is usable |
| A4.4 | Central read operations with as-of fallback | 4.5 | VERIFIED (range, heads, groups) | live Nov 8 | Nov 7 rehearsal (200 of 200 chart groups) | latest, stored coordinates, coverage counts still per module (A4.6) |
| A4.5 | One known-by clock contract (INV-3) | 4.5 | DESIGNED | named clock sets in `equity_bar_repository.py` (r66) | Nov 10 write delay: 5m up to 54 s, 1d reconstructed 77-85 s | Decide how a live known-by time relates to the write time (wait for the write, or stamp known-by from it); then one set for every reader, including `equity_history` |
| A4.6 | Live readers on the central read (INV-4) | 4.5 | VERIFIED (groups 1-4 live; Options equity input, P8 spots, interval derivation on a copy) | groups live Nov 8; three readers r66 | Nov 10 DB tests; parity on a copy: 491 of 491 identical to the r65 SQL, Options input and spots 10-20 times faster | Live with r66; then the 36 allowlisted modules (the interval derivation stays listed: it still counts `rt.bar_catalog`) |
| A4.7 | Strict read boundary test (no allowlist) | 4.5, INV-4 | DESIGNED | `test_bar_head.py` (36-module allowlist) | | After A4.6 |
| A4.8 | Retire history bindings | 4.6 | DESIGNED | written by 3 modules, read by `equity_history.py` | Nov 10 review R5 | Move provenance into the window attempt / configuration manifest |
| A4.9 | Writer refuses non-production cohorts (INV-5) | 4.4 | DESIGNED | | Nov 10: cohort `mode` cannot tell (all heads written by SHADOW cohorts) | Process flag `REALTIME_V2_BAR_WRITER=PRODUCTION` on the bridge, finalizer, controller and preparation services; the writer refuses the live database without it (compose change, next migration-free deploy) |
| A4.10 | Manual correction procedure | 4.7 | IMPLEMENTED (procedure) | runbook 8 | | Usable once A4.3 is live |
| 4.11-1 | v3 scripts off `rt.bar_current` (`audit_advanced_options_v3_operations.py` and three more) | 4.6 | DEFERRED | | | Before the first v3 one-shot run; `_history_coverage` needs a redesign, not a rename |
| 4.11-2 | Operator scripts off `rt.bar_current` (`runner/recovery_steps_nov6.py`, `runner/os_replay.py`); retire `runner/fix_aapl_live.ps1` | 4.6 | IMPLEMENTED | runner (local, no image), Nov 10 | Nov 10 | The fix script refuses to run; VERIFIED on the next use of either script |
| 4.11-3 | Ten remaining scripts off `rt.bar_current`; retire `bind_realtime_v2_advanced_history.py` | 4.6 | DEFERRED | | | With A4.8; none runs in a live service |
| 4.11-4 | Two-writer database test; bridge recovery from a serialization error | 4.3 | IMPLEMENTED (test) | `test_bar_head_v2_db.py` (r66) | Nov 10: READ COMMITTED and SERIALIZABLE both end on the canonical winner | The bridge's own retry on a serialization error is not tested (it runs READ COMMITTED, where the upsert waits instead) |
| 4.11-5 | No test or replay cohort writes bars to live | INV-5 | IMPLEMENTED (rule) | runbook 1 | Nov 10: no configured job writes bars | Guard: A4.9 |
| 4.11-6 | `equity_portal_projection_worker.py`, `equity_v2_backfill_source.py` off the old table | 4.6 | DEFERRED | | not imported by any live service | With 4.11-3 |
| 4.11-7 | Manual correction procedure | 4.7 | IMPLEMENTED (procedure) | runbook 8 | | = A4.10 |
| 4.11-8 | Retention cascade keeps the next-best revision (INV-6) | INV-6 | DEFERRED | | nothing deletes catalog rows today | Before any catalog cleanup or the 15-month retention drop |
| 4.11-9 | Drop `rt.bar_current` | 4.6 | DEFERRED | | the A4 rollback fill still writes it | After 4.11-1, -3, -6 and once the A4 rollback is no longer wanted |
| G90 | Preparation supersede | 5 | IMPLEMENTED | live Nov 8 | Nov 7 tests and EXPLAIN | Not exercised (no revision change since) |
| G90.1 | Supersede serialization (advisory lock, bounded retry) (INV-10) | 5 | DESIGNED | | Nov 10 review R6 | With B2; one lifecycle worker holds the role lease today |
| G91 | Carry readiness | 5 | IMPLEMENTED | live Nov 8 | Nov 7 tests | Not exercised (no list change since) |
| G99 | Statistics step | 5 | VERIFIED | live Nov 8 | Nov 10: watched tables analyzed 14:02 Nov 9 | |
| B24 | Bridge journal index | 5 | VERIFIED | live Nov 8 | Nov 9 session | |
| G94 | Split lookback (preparation) | 5 | VERIFIED | live Nov 8 | Nov 9 25 of 25 | |
| G94.1 | Split lookback and dividend validity in the nightly rollover | 5 | VERIFIED | r63 | Nov 10 01:29: all 25 refreshed, CRWD drift cleared | |
| A2.1 | Per-member outcomes v1 (manifests, placeholders) | 6 | VERIFIED | live Nov 8; Pattern Watch gap fixed r58 | Nov 9 session | |
| A2.2 | Typed `MemberUnavailable`, outcome rows, PARTIAL barrier, last good entity STALE (INV-7); includes G23 (one Options ticker's failed capacity stage holds every ticker) | 6 | DESIGNED | | Nov 10 review R3 | Before more members or fan-out |
| A2.3 | Read labels instead of 503 (INV-8) | 6.1 | VERIFIED | live Nov 8 | Nov 7 check; Nov 9 pages | CRWD 1m history still 503 (provider-empty minute, outside G102) |
| A2.4 | Window attempts that supersede (B14b, B17); includes G5 (a retry can start seconds before its deadline and overrun the next slot), G14 (recovering a window projected by an earlier image fails) and G44 (after an Options window role crash the retry can never succeed) | 7 | DESIGNED | | | With the P8-or-V3 work |
| A2.5 | Ingress plan revisions, audited reopen (B2) | 7 | DESIGNED | | | Next after A2.2 |
| R7 | Fallbacks visible (INV-9) | 2 | VERIFIED (copy; response and log) | r66: `data.optional_reads` on session alerts, Screener breadth, Flow breadth, current marks | Nov 10 unit tests (FALLBACK path); r66 rehearsal: all four AVAILABLE on the Nov 9 session | A page label when a read fell back (next frontend build) |
| A3 | Detector activations, policy versions | 7 | DEFERRED | | Nov 7 benefit check: no failure needs it | When a configuration change needs it |
| A5 | Page pointers and URLs; includes G70 (cohort ids in page URLs and API calls) | 7 | DEFERRED | | | Before B1 (consistent member sets) |
| A6 | Universe at a window boundary | 7 | DEFERRED | | optional | |
| B0 | Preparation speed, budget at 400; includes G89 (16 new members took about 87 minutes, mostly per-chunk overhead) | 7 | DESIGNED | | | Before 50+ members |
| B1/B2 | Fan-out, warm restart | 7 | DESIGNED (outline) | | Nov 10 review R4 | Full specification in target 7 before code |

## 2. Platform items tracked beside G102

| ID | Item | Status | Evidence / image | Next |
| --- | --- | --- | --- | --- |
| B14a | Stream session ends at the decision stop | VERIFIED | Nov 9: `d42b2d84` ended by the next session | |
| B20 | Missed window's 5m bars redone (with the stream gap repair, covers G35: a feed drop left a permanent 5m hole) | IMPLEMENTED | r55 | |
| B3/B11/B9c | Crash to wait with a status | VERIFIED | Nov 9 | |
| G75 | Options breadth (Screener, Flow, Activity) | VERIFIED | Nov 8 rehearsal; Nov 9 session; Nov 10 r63 | |
| G75 V4 | Options depth 24 per name | DEFERRED | Nov 10: market-state at one core with 72 contracts | Market-state process split first |
| V3-1 | First V3 runtime slice: Options market-state (and flow) split across processes, replacing the P8 market-state service; then V4 depth on it; each remaining P8 service (ingress, flow, window) retired as its V3 replacement proves itself (user, Nov 9: V3 in the fewest cycles, no throwaway P8 work) | DESIGNED (outline) | Nov 9 measurement: market-state p50 42%, p90 86%, max 99% of one core at 72 contracts; 216 would exceed it | Write the design (partitioning by underlying or contract, state handoff, window input contract, capacity target for 216) before code; after the Nov 10 session |
| G81 | OS detectors | VERIFIED | Nov 9 | |
| Quiet contract | Snapshot validity ignores a quiet minority | IMPLEMENTED | r63; Nov 10 replay 103 of 103 | Live Nov 10 |
| G92 | Earnings per symbol, N/A without Finnhub, swing gate reads stored dates | IMPLEMENTED | r64 | Live Nov 10 |
| G37 | Financials statements (income, balance sheet, cash flow; quarterly, annual, TTM) | IMPLEMENTED | r65 + f21, copy rehearsals (Nov 10 08:24: 1,314 reports, projection 38 ms); carried in r66 | Live with r66 since Nov 10 09:17: first refresh 16 companies, 1,314 reports (R10: at the role's first idle slot); VERIFIED when a ticker workspace published Nov 10 shows statements |
| G37.1 | Financials: key stats (ratios), float and short interest, dividends section | DESIGNED | `FINANCIALS_TAB_TASK.md` sections 1, 5, 6 | Needs new `ref.*` tables (a migration, user's yes), refresh, projection and page sections; not in r66 |
| Stack stop | Controller (waiting for activation) exits 137 and Options market-state 241 when all services stop together; includes G33 (a clean stop does not exit during a window) | DESIGNED | Nov 10 r63 and r66 rehearsals; alone, r64 and r66 both exit 0 in 1-2 s; Nov 10 08:20 (r66): controller 0, finalizer and Options lifecycle 241 within 0.3 s (child ended by SIGTERM, returncode -15 passed up by the wrapper) | Find what the controller waits on during a whole-stack stop, and why the wrapper's child sees SIGTERM unhandled (image-independent) |
| Public domain | `alpha-screener-pro.com` serves the realtime portal through the stack's own Cloudflare Tunnel, behind Cloudflare Access (runbook 10) | VERIFIED | Nov 10: 4 tunnel connections; pages and `/api` answer 302 to the Access login; legacy connector retired | Move the connector onto `rollout/compose.edge.yml` (pinned image, token file); delete the old tunnel in the dashboard; check the domain after tonight's frontend deploy |
| Stock quotes | Live stock quotes and trades by purpose (`EQUITY_MODEL_LAB_DESIGN.md` 8.2): not needed today | DEFERRED | Nov 10 review: pages, rules and Options run on bars or snapshots | Research on provider history first; first live use (one quote per candidate per decision) when D1/D2 are approved; never in the minute-bar journal |
| Execution | Manual trade execution from alerts through a configured broker (`TRADE_EXECUTION_FUTURE_DESIGN.md`) | DESIGNED (future) | Nov 10, user | X0 decisions (broker, quote source, limits, users); needs the exact live quote of the ticket's instrument at execution, not a stream for all names |
| R8 | `test_equity_interval_derivation_worker.py` does not collect (imports `_load_current_five_minute_facts`, renamed before r63), so r66's change to `load_current_five_minute_facts_cursor` has no unit test in its own file | DESIGNED | Nov 10 review (07:40): covered meanwhile by the central read's DB tests (12 of 12) and copy parity (100 of 100) | Fix the import and the fakes (test-only, no image change) |
| R9 | Pre-open audit tool `coverage_after.sql` hard-codes 25 tickers with XLV instead of SMH (the desired sector ETFs are SMH, XLC, XLE, XLF, XLK, XLY), so SMH's dividend and split coverage is not checked | DESIGNED | Nov 10 review: the member-driven version (session scratchpad `ready3.sql`) shows 25 of 25 | Move the member-driven query into `runner/` and point runbook 7 at it |
| R10 | The Financials refresh runs in the preparation role's first idle slot after a start and then every 30 min while due, not only after the close: a pre-open deploy fetches statements at once | DESIGNED (docs) | Nov 10 review: `run_realtime_v2_advanced_equity_preparation.py` (slot at start, `FED_CALENDAR_CHECK_INTERVAL` 30 min); live preparation IDLE (`NO_DUE_PREPARATION_WORK`) | Say so in `FINANCIALS_TAB_TASK.md` and the module docstring |
| CRWD empty minute | Provider-confirmed empty minute accepted | DESIGNED | Nov 9 17:01 | Repair, input gate, history completeness |
| Sector benchmarks | Stock Alert context rules for stocks without a benchmark | DESIGNED | Nov 10 replay | Member-to-benchmark mapping |
| G103 | Close waits about an hour for daily bars | DESIGNED | Nov 9: 21:00 to 22:01 | |
| G84/B25 | Official close; one basis for Chg% | DESIGNED | Nov 9 validator | |
| G38/B19 | Daily strategies producer | DESIGNED | | |
| B10 | SPY/QQQ literals | DESIGNED | | Before more Options underlyings |

## 3. Carried from the frozen registers (open on Nov 10)

The gap register's items that were neither addressed nor already tracked, triaged Nov 10 (evidence: Nov 10 entry
"Register triage"). Wording is today's state, not the original report. G12 (legacy stack) was closed the same day by
the user: its worker services are off and it runs only outside market hours, for Model Lab.

| ID | Item | Status | Evidence | Next |
| --- | --- | --- | --- | --- |
| G4 | A timed-out stage keeps running in its thread beside the next window, in the same process | DESIGNED | Register Nov 4; no fix recorded | Cancel inside the stage, or one process per window attempt (with A2.4) |
| G13, G40 | First window of a session fails its first attempt (G13); Equity windows publish one 5m bar behind and read `STALE` (G40) | DESIGNED (verify) | Nov 9 openings failed for another cause (Pattern Watch required every member, fixed r58) | Check at the Nov 10 open; close the rows or fix |
| G20 | Rolling back the first Options read pin fails (the role may not delete it) | DESIGNED | Register; rollback path never exercised | Disable the pin instead of deleting it |
| G21 | A transient failure in an Options capture costs a whole capture round and a long wait | DESIGNED | Register Nov 4 (QQQ, 14.5 hours) | Retry the failed page promptly instead of spending a round |
| G22 | Portal releases name an input receipt from a different cutoff | DESIGNED (verify) | Register Nov 3: 118 of 146 releases, pages correct | Trace on a recent session's releases; with A5 |
| G7, G8, G41 | Options feed: unexplained drops and a keepalive timeout (Nov 4-5), the 12-attempt ceiling, acquisition status spin | DEFERRED (watch) | Not seen since: Nov 9 Options stream on one connection all day | On the next drop, record close code and cause before changing keepalive or ceilings |
| G57 | `REALTIME_V2_BACKEND_IMAGE` names an old image | DESIGNED | Nov 10: the API's label says `84ffaace`, it runs `881c0c72` | Set the label from the deployed image in `deploy_candidate.ps1` |
| G60 | The stock stream role reports healthy after its reconnect budget is spent | DESIGNED | Register Nov 5 (limit raised, health unchanged) | Health fails when the feed is out of budget |
| G65 | `/api/stocks/context` returns 503 (`ValueError`) | DESIGNED | Nov 10 smoke: 503 on live and on r66; Nov 10 08:16: the ValueError is the Dashboard retained read refusing (`dashboard_repository.py:176`, "identity/checksum/clock drift"), which also 503s `/latest-price-date`, `/tickers/overview`, `/tickers/setup-summary` and `/scan/all` (since at least 00:54) | Find which of the identity checks fails (candidate: the source-manifest identity recomputed from the history binding); see whether the first Nov 10 Dashboard publication clears it; serve with a label (INV-8) |
| G77 | Sector Intelligence (1 week to 3 months) and Rotation are blank; Pattern Watch and the ticker page use different ATRs | DESIGNED | Register Nov 5 | A producer for those periods; one ATR definition |
| G79, G101 | Options detector models: O2 and O3 not built; O1 strategies for same-day expiries and repeated trades | DESIGNED | O1 alerts live since Nov 6; OS1/OS2 VERIFIED Nov 9 | Model work after G75 breadth |
| G86 | A ticker renamed in the last 5 years cannot finish preparation (META, Nov 6) | DESIGNED | Register Nov 6 | Follow the provider's ticker changes, or accept shorter coverage with a recorded reason |
| G87 | SPX and NDX options | DESIGNED | Target was Nov 10; not built | Index member kind, SPXW/NDXP roots, index level from put-call parity |
