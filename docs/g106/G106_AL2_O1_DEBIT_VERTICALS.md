# G106 AL2: O1 debit verticals on the live Options window (Nov 16 2026)

Matrix: G106 AL2 (package builder structures), first structure = the directional debit vertical for O1; D7 (OS1/OS2
debit-spread variant) can reuse the same rule later. Branch `al2-o1-debit` (from `v3-live-fixes` d3ade41 = r120 code +
docs), image **r121** `sha256:601cc71f9352ebcc38c2a30d6d0429b9dca792d3843457690c34fe41957ce993` (source
e56ad7182d4fb950) = r120 + this change only. Deploy note: `HANDOVER-20261116-LIVE.md` section 13.

## 1. What changes

Today every O1 detection is packaged as one bought contract (`options_alert_v1_event_package`: BUY 1 at the event's
NBBO midpoint, capital at risk = premium, exit at the session close). With the switch, the same event can be packaged
as a debit vertical instead.

Switch `REALTIME_V3_O1_DEBIT_VERTICAL` on `realtime-v3-options-p8-window`:

| Value | Behaviour |
|---|---|
| unset / `OFF` | today's single contract, byte for byte (no extra read; same input identity) |
| `RECORD` | builds the vertical for every O1 event without a stored package, checks that its 2-leg package can be constructed, logs one `O1_DEBIT_VERTICAL_RECORD {json}` line per window; publishes today's single contract unchanged |
| `ON` | the O1 package is the debit vertical when one qualifies, otherwise today's single contract; same record line |
| anything else | the window service refuses to start (`run_advanced_options_v3_production.py`, WINDOW kind) and every window that reads it fails: never silently OFF |

## 2. The rule (`options/policies/o1_debit_vertical_v1.json`, version `o1-debit-vertical-v1`)

- Long leg = the O1 event's contract (BUY 1). Short leg = same underlying, expiry, type and multiplier, further out of
  the money (call: higher strike; put: lower), SELL 1, among the window's watched contracts (slot set), nearest strike
  first. Strikes closer than the minimum width are skipped; the search stops at the maximum width; the first strike
  that passes every check is the short leg.
- Both legs priced at the window's cutoff marks (`rt.option_mark_evidence` ADVANCED_NBBO, the marks the window values
  packages with), so the two prices come from one snapshot: net debit = long mark - short mark > 0; capital at risk =
  net debit x multiplier; maximum profit = (width - net debit) x multiplier; breakeven = long strike +/- net debit;
  exit at the session close (as today). Package entry time = the later leg mark; each leg keeps its own mark time.
- Both legs must be marked at this cutoff: not in the window's `unmarked_instrument_ids` (R34: no fresh quote; R31:
  incomplete volume baseline) and a usable two-sided mark at or before the cutoff. Otherwise rejected with a reason.
- Limits from the legacy `DIRECTIONAL_DEBIT_SPREAD` block (`strategy_o3_credit_v2.json` `debit_spread`, the block the V4
  profile's debit-spread groups use): width 0.5%-10% of spot (spot = the window's underlying snapshot tick), reward/risk
  = max profit / net debit >= 1.0, each leg open interest >= 250 (session reference state) and day volume >= 100
  (window rolling state), DTE <= 60.
- Not applied from the legacy block (stated in the policy file): DTE floor 1 -> 0 (O1 exits at the session close; a
  0DTE vertical exits at its expiry, as today's single contract does; the legacy floor is for multi-day holds), the long
  delta band (the long leg is the O1 event's contract, not chosen), the expected-move ratios (need an IV solve at the
  cutoff: v2), DTE lanes and per-lane counts (one vertical per O1 event).
- Rejection reasons (fallback = today's single contract): `O1DV_LONG_LEG_NOT_WATCHED`, `O1DV_DTE_OUTSIDE_POLICY`,
  `O1DV_LONG_LEG_UNMARKED`, `O1DV_LONG_LEG_LIQUIDITY_BELOW_POLICY`, `O1DV_SPOT_UNAVAILABLE`,
  `O1DV_NO_FURTHER_OTM_WATCHED_CONTRACT`, `O1DV_NO_SHORT_WITHIN_WIDTH_POLICY`, and for the nearest width-eligible strike
  `O1DV_SHORT_LEG_UNMARKED`, `O1DV_SHORT_LEG_LIQUIDITY_BELOW_POLICY`, `O1DV_NON_POSITIVE_DEBIT`,
  `O1DV_DEBIT_NOT_BELOW_WIDTH`, `O1DV_REWARD_RISK_BELOW_POLICY`; `O1DV_INPUTS_UNAVAILABLE:<error>` (the vertical
  inputs read failed under its savepoint), `O1DV_PACKAGE_INVALID:<error>`.

## 3. Code (5 files + policy + tests)

| File | Change |
|---|---|
| `realtime/advanced_options_o1_debit_vertical.py` (new) | switch, policy, rule, 2-leg package (candidate id `uuid5(o1-debit-vertical-v1, cohort, event)`), inputs read (reuses O3's readers: watched contracts, cutoff marks, leg activity; spot from the rolling state's snapshot ticks), record line |
| `realtime/advanced_options_native_loader.py` | `_load_o1`: OFF path unchanged; RECORD/ON build the vertical when the event has no stored package; a stored vertical with a leg unmarked at a later window is never replaced by a new package (one package per event, else "mapping is ambiguous"); `_packages_for_events` can report the packages it left out |
| `realtime/advanced_options_native_adapters.py` | `AdvancedO1Invocation.debit_vertical` (not hashed); the O1 executor logs the record line once per window |
| `realtime/advanced_options_projection.py` | O1 debit vertical: structure `CALL_DEBIT_VERTICAL` / `PUT_DEBIT_VERTICAL`, risk class PREMIUM_AT_RISK_DEBIT, maximum profit, reward/risk, breakeven in the candidate detail and alert row; direction from the bought leg (the legs cancel by count, so R30's leg sum read +1 for every put vertical); session alert of its own (`options_session_alert_id(..., variant="O1_DEBIT_VERTICAL:<short>")`) so a single contract and a vertical on one contract never share a row and mark. Every other package projects as before (O3 untouched) |
| `scripts/run_advanced_options_v3_production.py` | the WINDOW service validates the switch at start |

Places that assumed one leg: package evidence (`advanced_options_package_evidence.py`) and the window's package mark
(`_package_mark`, `materialize_realtime_option_package_mark`) already take up to 8 legs and judge R31 baselines per
leg (`threshold_for`); `option_package_context.py` (legs[0] direction) is the developer/forward path, not the live
window: unchanged. API: `option_alert_valuation.py` values every leg (OA1) and gives the OA2 model exit only to a single
bought contract (a vertical shows no model exit); the API's read-time current mark loops every leg. **No API change is
needed** (r120's API serves the 2-leg alert, section 5).

Known limit: the platform marks a multi-leg package only when its legs' quotes are within 250 ms of each other
(`MAX_PACKAGE_QUOTE_SKEW_MS`, shared with O3) and each is received within 1 s of the cutoff; at the entry window the
earlier leg's quote precedes the package entry time, so the window's own mark of a new vertical is UNAVAILABLE there.
The API's read-time mark (stored quotes, GOOD and two-sided for every leg, STALE after 15 minutes) fills the Options
Alerts page; a far-OTM short leg with no bid (0DTE near the close) leaves it unavailable.

## 4. Tests

- `tests/realtime/test_o1_debit_vertical.py` (21): switch values and loud failure, start-up check, policy vs legacy
  block, call and put picks, every rejection, next strike when the nearest fails, 0DTE, package economics and identity,
  the window's package mark of a vertical (and the 250 ms skew refusal), `_load_o1` OFF (no extra read, today's
  package) / RECORD (same package and input identity as OFF, outcome recorded) / ON (vertical; unmarked short -> single
  contract; inputs unavailable -> single contract), invalid switch, stored vertical never replaced, the record line,
  projection (structure, payoff, direction of a put vertical, separate session alert), spot from snapshot ticks.
- Targeted suites (15 files incl. the above): 245 passed, 1 failed = the same test failing on 84e04b2
  (`test_compose_has_exact_four_p8_services...`: compose file not mounted in the test container).
- Full `tests/realtime`: 158 failed / 5,278 passed vs 158 / 5,257 on 84e04b2, the identical failure set (0 new).

## 5. Replay and end-to-end (isolated test server `g102-test-pg`, copy `test_realtime_v2_recovery_20261104r120svc`)

Replay (`runner/o1dv_replay.py`, read-only, outputs in `evidence/o1dv-20261116/`): per stored Options window the AL2
rule on that window's stored marks (marked = newest mark within 2 s before the cutoff), rolling checkpoint (volumes,
spot) and reference states (open interest), for each detected O1 event at its first window.

| Session | Windows | Watched | Detected events | Of them alerts | Verticals built (alerts) | Fallbacks (detected events) |
|---|---|---|---|---|---|---|
| Nov 12 (V2) | 77 | 72 | 591 | 397 | 0 (0) | no further-OTM watched strike 431, long leg unmarked 115, reward/risk < 1 44, short unmarked 1 |
| Nov 13 (V4, Friday; 8 windows 18:15-19:58 in the copy) | 8 | 216 | 103 | 31 | 18 (17) | long leg unmarked 70, short leg unmarked 11, no further-OTM 3, no short within width 1 |

- Nov 13 verticals: NVDA 16, TSLA 2, all calls, all 0DTE (Friday): net debit 0.04-0.86 (median 0.15), width 2.5-5,
  width 0.6%-3.1% of spot, reward/risk 1.9-61.5 (median 32), capital at risk $4-$86; legs' quote skew 49-575 ms.
- Sensitivity: with the legacy DTE floor (1) Nov 13 builds 0 (57 of the 103 events were 0DTE, Friday's weekly expiry);
  with reward/risk >= 0.5 Nov 12 builds 43 NVDA in-the-money call verticals (reward/risk 0.55-0.64), which the policy
  correctly leaves as single contracts.
- V2 (72 contracts, mostly one contract per expiry and side) rarely has a further-OTM partner; V4 streams adjacent
  debit-spread pairs, so on a normal weekday the long leg being marked at the cutoff is the main limit (contracts are
  marked only with a quote received within 1 s of the cutoff).

End-to-end (`runner/o1dv_e2e.py`, AL2 code, the app login with SET ROLE app, switch ON) at the stored Nov 13 19:58:40
cutoff: the real `_load_o1` (flow-event SQL, stored-package lookup, vertical inputs under the savepoint) gave 156
invocations: 2 verticals, 32 single contracts (stored or rebuilt), 122 without a package; the real O1 executor
(detector + selection + record line): lane COMPLETE, 15 selected, 1 a vertical (event 6feb957b, NVDA 13 Nov 165C/170C,
debit 0.10, max profit $490, breakeven 165.10); `persist_realtime_option_package_cursor` stored it with leg_count 2 /
net premium -10.00 / capital at risk 10.00; the projection's alert row and `persist_options_session_alerts_cursor` wrote
its session alert (direction +1, CALL_DEBIT_VERTICAL, 2 legs). API read (`/api/options/alerts/detector-runs?
scope=HISTORY&session_rollup=true&detector=O1`) on an API container of **r120** and of **r121**: status 200, 12 rows,
the vertical row with both legs, OA1 IV/delta per leg, structure, max profit and breakeven; current mark UNAVAILABLE
because the short leg's last stored quote (Nov 13 close) is one-sided. The same path from the r121 image in RECORD at
18:54:22: 0 verticals used, 94 single contracts unchanged, 50 verticals built and logged. The three test rows were
removed afterwards, the copy renamed back, the test logins set NOLOGIN and the test server stopped as found.

Not exercised: a whole Options window cycle with the switch (package evidence batch and the Alerts page publication
with a 2-leg O1 package; the same code serves O3's 2-leg packages live since today), and a live quote stream.
