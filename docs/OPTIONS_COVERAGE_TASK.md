# Task: wide Options coverage (breadth + moderate depth), for Monday Nov 9

Status: decided by the user on Nov 6 ~18:50 UTC ("Breadth + moderate depth"). Tracks G75 in
`docs/agent-context/advanced-realtime-integration-gaps.md` and C16 in `PLATFORM_STUDY.md`.

## Why

Option Flow, the Options Screener and volume/OI detector models need a wide view of the market. On Nov 6 the system
tracked **72 contracts (8 per underlying) out of 9,264 active contracts within 60 days** (0.8%): AAPL 419, AMZN 321,
IWM 1,164, MSFT 626, NFLX 126, NVDA 260, QQQ 3,306, SPY 2,611, TSLA 431. The day's 1,966 flow events were on 52
contracts. O1 (day volume / prior OI >= 3) can only ever see the tracked contracts.

Where the 8 comes from: the C6 activity-momentum profile (`realtime/policies/advanced_options_activity_momentum_c6_v2.json`):
`quote_contract_ceiling` 1,000, `configured_total_budget` 900, `quote_slot_reserve` 100, `maximum_contracts_per_underlying`
12, one connection; the selection fills 8 per underlying. Scope is fixed at preparation (C1), so it cannot change
mid-session.

## Decision

1. **Breadth layer (new):** REST chain snapshots of every in-scope contract at each 5-minute Options window: day
   volume, open interest, last trade, NBBO, IV and Greeks. Scope: contracts expiring within 60 days, within a moneyness
   band (to agree; for example +/-20% of spot). Feeds Option Flow (volume/OI by strike and expiry), the Options
   Screener (all contracts) and volume/OI detectors (O1 over all contracts).
2. **Moderate depth (configuration):** stream trades and quotes for more contracts, from 8 toward about 36 per
   underlying within the 900-contract budget (23 underlyings x 36 = 828), chosen by activity from the breadth layer at
   preparation. Measured on a copy first (CPU of the Options services, socket, Equity window times alongside).

## Budget (estimate, to confirm with a measured capture)

- Breadth: about 250 contracts per request; 9 underlyings ~38 requests per window, ~3,000 per session; 23 underlyings an
  estimated 6,000-8,000 per session. The system's own account ceiling is 64,000 requests per day
  (`provider_budget.MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING`); no ceiling change is needed at this size. Any raise
  needs the user's yes.
- Depth: no extra REST; CPU and socket load measured before Monday.

## Build (weekend)

1. **Capture.** A breadth capture in the Options window cycle, independent of the frozen contract set: paged chain
   snapshots per underlying, bounded by the request budget, recorded through the provider ledger. A failed or late page
   leaves that underlying's breadth stale with a label; it never fails the Options window (per-item degrade, C3).
2. **Storage.** Per window, one row per contract (volume, OI, NBBO, IV, Greeks, observed time), point in time, with
   retention like the other Options facts.
3. **Projection and reads.** Option Flow by strike/expiry and the Options Screener read the breadth rows; current
   prices for Options alerts can also use them (today they read `rt.quote_current`).
4. **Detectors.** O1 evaluates breadth rows (volume vs prior OI) for all in-scope contracts; packages are built by the
   Options alerts v1 rule (G79).
5. **Depth.** Raise `per_underlying_contract_limit` / the profile's per-underlying cap and the active quote budget for
   Monday's preparation; selection by breadth activity.
6. **Underlyings.** Monday's list: Equity 25 minus XLE and XLC (23), as agreed.

## Validation

- Unit tests with recorded chain pages (no provider calls in tests).
- One measured breadth capture for 2-3 underlyings (request count, bytes, latency) before the full build is enabled.
- Rehearsal on a copy with the real Monday universe: Options window time with breadth, CPU, Equity windows alongside.
- After Monday's first windows: compare page values (volume, OI, NBBO) with the provider for a few contracts.

## Open questions for the user

- Moneyness band (proposal +/-20% of spot) and DTE window (proposal 60 days).
- Whether 0DTE contracts are included for index ETFs (high volume, high request count).
  Nov 6 evidence: 26 of 28 O1 detections were same-day contracts (24 trades on NVDA 192.5C alone). The user's
  direction is to keep them and evolve the detector strategies to handle that scenario (G101), not to filter them.

## Status and decisions, Nov 8 (user)

**Built for Monday** (worktree, not deployed): breadth capture each Options window (0-60 DTE, strikes within 20% of
spot, same-day included; migration 052); our IV and Greeks computed for every breadth contract with the solver the
streamed contracts use, beside the provider's IV, delta, gamma, theta and vega (`local_*` is always ours,
`provider_*` always the provider's); the Options Screener merges breadth rows under the published ones; built-in
Unusual-activity screens defined in the backend (`options/policies/option_screens_v1.json`).

**Unusual activity is an observation, not a tradable alert** (ADVANCED_OPTIONS_FLOW_POLICY.md, OPTION_PACKAGE
_STRUCTURE_AND_STRATEGY_EVALUATION.md: volume above OI does not prove opening flow or direction). It is a built-in
screen: prior OI >= 500, Vol/OI >= 3, 3-4 DTE excluded, tier minimums (ETF 5,000 contracts and $1M day premium;
core mega-cap 1,500 / $500k; high-beta 750 / $500k), ranked by day premium, 3 per underlying and 20 in all.
Friday's chains: 45 contracts outside the 3-4 DTE lane across the 9 underlyings. Tradable alerts need print
evidence (aggression side) and package rules; depth (below) widens the contracts that can produce them.

**Depth: C6 profile v4, live Tuesday Nov 10** after a replay against Monday's real preparation inputs and breadth
activity (Monday keeps v2, 8 per underlying). 24 contracts per underlying, 0DTE included for every product:

| Product (recognised from listings) | Buckets |
| --- | --- |
| Index ETFs (daily expiries listed) | 0DTE 6, 1-2 DTE 6, weeklys 8, core 6-21 DTE 4 |
| Single stocks (Friday weeklys) | weeklys 8, core 6-21 DTE 12, extension 22-60 DTE 4 |

Weekly ladder, from the listed expiries (holiday weeks included): Monday to Wednesday cover this week's weekly;
Thursday covers this week's (1 DTE) and next week's; Friday covers next week's weekly as the primary one, beside
0DTE. Calls and puts compete in one quality ranking with no side quota (quote present, prior OI present, nearness to
the delta anchor, previous session volume and notional, spread). Budget 216 of 900 at 9 underlyings. Open risk:
Options CPU at three times the stream (G72); stop rule on Monday-Tuesday.

**Later:** the detection models (O1, O2, O3, OS1, OS2) and the flow profiles (Ultra-short, Block, Sweep,
Accumulation) toward fewer, higher-quality tradable alerts (model plan after Monday); multi-user screens, one
definition language and natural-language screens (`OPTIONS_SCREENS_FUTURE_DESIGN.md`).
