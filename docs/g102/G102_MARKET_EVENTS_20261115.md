# G102 R80: market, security and data events: current behaviour and handling plan

- **Status:** ANALYSIS, Nov 15 2026 (user: "document them for future handling"). Nothing here is built unless marked
  BUILT. Each item names what happens today, the impact, the planned handling and how to verify it.
- **Principle (user, Nov 14-15):** one name never blocks the others; a name that cannot be served is left out or
  labelled with its reason, and the rest of the session runs.
- **Related:** `G102_UNIVERSE_SIZING_20261114.md` (R82: exclusions, confirmed-empty minutes, Options readiness),
  matrix rows R58, R59, R66, R76, R82, G86, G105.
- **Code facts used below (checked Nov 15):** no realtime module knows about trading halts (no halt, LULD or
  circuit-breaker handling); the market calendar (`options/calendar.py`, `exchange_calendars` XNYS) knows early
  closes (Nov 27 and Dec 24 2026 close at 18:00 UTC).

## 1. Market structure events

### 1.1 Single-stock trading halt (LULD pause 5-10 min; news or regulatory halt, minutes to days)

- **What happens today:**
  - The stream sends no minute bar for the halted name (no trades). The Equity input gate needs every member's bars
    for the window, so the window waits for the gap repair; the provider has no bar either.
  - Nov 13 evidence: one GS no-trade minute (18:04) failed E:18:05 for **all 45 names** (`ADVANCED_INPUT_NOT_READY`)
    until the repair gave up (WAIVED); E:18:10 passed. A halt repeats this for each window it spans (expected, to
    verify).
  - Options windows attach to Equity windows, so they are skipped for the same cutoffs.
  - A 5-minute slot without trades has no 5m bar, so the 15m/30m/1h bars and the RAW daily bar (derived from 5m) are
    incomplete for that name; the close may not complete (as G105 Nov 6 and R76 Nov 13 for whole-universe holes).
  - Stock Alerts: the research history restarts its warm-up after a missing bar (R66): one halt can leave that name
    without alerts for ~15 (30m) to ~33 (1h) sessions.
  - Options on a halted underlying halt too: its contracts' quotes go stale (the quiet-contract rule keeps windows
    running); the OS1/OS2 Equity context for it is incomplete.
- **Impact:** high. A routine 5-minute LULD pause in one of 45 names fails windows for everyone.
- **Handling plan:**
  1. Per-member degradation in the Equity input gate: a member without its minute after the repair confirms the
     provider has none (`REALTIME_V2_CONFIRMED_EMPTY_MINUTES`, built behind a switch in R82, today capped at 5 minutes
     per hole) is served as "no new bar" and the window proceeds for everyone. Remove the 5-minute cap for a
     provider-confirmed halt.
  2. Halt awareness: read the provider's halt/resume status (or infer: no trades + repair confirms empty) and label
     the name `HALTED` on its ticker page and in the window report.
  3. Derived bars and the daily bar: build them from the minutes that exist and mark the slots as confirmed empty, so
     the RAW daily bar and the close complete (the same rule as R58 in preparation).
  4. Stock Alerts: treat confirmed-empty or halted slots as continuous history (no warm-up restart), flagged in the
     evidence.
  5. Options: the underlying's lanes report `UNDERLYING_HALTED` for those windows; other underlyings unaffected.
- **Verify:** replay a Nov 13 window with one member's minutes removed (gate 6 of R82), then a 30-minute and a
  2-hour gap; check windows, derived bars, the close and Stock Alert continuity.

### 1.2 Market-wide circuit breaker (Level 1/2: all names halt 15 min; Level 3: closed for the day)

- **What happens today:** every member misses its minutes; every window fails until the repairs give up; the REST
  repair runs for every member (45 requests per missing minute at 45 names, 400 at 400). Level 3 ends trading early
  with no calendar change, so the remaining windows wait, fail or are missed, and the close runs at the normal time on
  an incomplete day.
- **Impact:** rare, severe.
- **Handling plan:** detect "no member traded" for consecutive minutes as a market-wide halt (and/or read the
  provider's market status); pause window processing and repairs during the halt; resume on the first trades. For
  Level 3, close the session early from the last traded minute (like an early close).
- **Verify:** replay with all members' minutes removed for 15 minutes; a Level 3 replay with an early stop.

### 1.3 Early-close days (Nov 27 and Dec 24 2026, close 13:00 ET = 18:00 UTC)

- **What happens today:** the calendar returns the early close; window counts in the code are bounded at 78 (a short
  day has 42). The pipeline has never run an early-close session: the close window, the after-close daily products
  job (R78), Options decision stop and acquisition stop, the after-close refreshes and the self-heal timing all
  derive from `session_close` and have not been exercised with 18:00 UTC.
- **Impact:** high if any step assumes 21:00 UTC (a missed close is a Monday-style blocker for the next session).
- **Handling plan:** before Nov 27: grep for 21:00 / 16:00 ET / 390 / 78 assumptions in schedules and checks
  (scheduler, watcher, runner scripts, crons in the live prompts), then rehearse a full early-close day on a copy with
  the clock shifted (windows to 17:55, close at 18:00, daily products after the close).
- **Verify:** that copy rehearsal; the live session's crons for Nov 27 use 18:00 UTC.

### 1.4 Unscheduled market closure (e.g. a national day of mourning, an exchange outage)

- **What happens today:** the calendar package does not know it until updated: the system treats the day as a session,
  prepares, activates at 14:15, and every window fails (no data); the "previous session" for the next day may be
  wrong.
- **Handling plan:** an operator override file (closed dates and special closes) read by the calendar wrapper, plus a
  pre-open check (provider market status) that refuses activation on a closed day.
- **Verify:** unit test with an override date; dry run on a copy.

### 1.5 Late or delayed open of one name (news pending, IPO first trade mid-morning)

- **What happens today:** like a halt from the open (1.1); for an IPO the name is not READY (no history) and is left
  out (R82).
- **Handling plan:** as 1.1; an IPO joins from the next session after its backfill (R82).

## 2. Security lifecycle events

| Event | Today | Plan |
|---|---|---|
| Delisting (merger completed, exchange delisting) | BUILT (R82): excluded before the session from the provider's delisting date; history kept; returns if active again | Delisting effective mid-day: behaves like a halt for the rest of the day (1.1) |
| Ticker change (FB -> META) | Preparation cannot finish 5-year weekly/monthly history (G86 rename part, not built) | Follow the provider's ticker events; stitch history under the old ticker by security identity |
| Spin-off / when-issued (SNDK, Feb 2025) | Listing date = when-issued day; bars start at regular trading; the gap blocks weekly history (found Nov 15) | Accept the gap between the listing date and the provider's first bar (follow-up in R82) |
| New listing / IPO | BUILT (R82): history from the listing date, recorded as `history_shortened`; READY with shorter history | Ticker page shows the shorter history; indicators needing more bars marked unavailable |
| Splits / reverse splits | Corporate actions refreshed by the rollover; adjusted history rebuilt (CRWD split drift fixed Nov 9-10) | Verify an intraday-announced split effective next open on a copy |
| Symbol reuse (old SanDisk vs new) | Identity by security, not ticker; G86 listing date avoids the old history | Keep; check the provider's FIGI on rename/reuse |

## 3. Data and provider events

| Event | Today | Plan |
|---|---|---|
| No-trade minutes in quiet names | BUILT: accepted in preparation (R58); in session behind `REALTIME_V2_CONFIRMED_EMPTY_MINUTES` (off) | Rehearse and switch on (R82 gate 6); same rule as 1.1 |
| Stream disconnect / network outage | Gap repair by REST for the hole; Nov 10 DNS outage left Options 460 s behind (R28); a host sleep restarts services (Nov 15) | Keep; host sleep disabled; repairs of derived bars after a hole (R66 follow-up) |
| One member missing from the stream while others flow | Treated as a gap; repaired by REST if the provider has bars | As 1.1 when the provider has none |
| REST outage or 404 | 404 for an inactive ticker handled (R59); other failures retry with backoff | A per-name deadline: left out for the day with the reason |
| Late or corrected bars | Bar revisions keep the newest by precedence (A4 bar head) | Keep |

## 4. Options-specific events

| Event | Today | Plan |
|---|---|---|
| Underlying halted | Options on it halt; quotes stale; quiet-contract rule keeps windows running | Label the underlying's lanes `UNDERLYING_HALTED` (1.1) |
| One underlying's preparation keeps failing (e.g. a provider error on its chain) | Session activation waits for every underlying's work: Options down for all (pre-existing) | Per-underlying cut-off a few minutes after the open: dropped for the day as UNAVAILABLE (user, Nov 15; R82 follow-up) |
| Contract adjustments after a corporate action (non-standard deliverables) | Not reviewed | Check how the catalog and marks treat adjusted contracts; exclude non-standard ones from selection if needed |
| Expiration day (0DTE at the close; early-close expirations) | Selection by DTE; early-close expirations untested | Include in the early-close rehearsal (1.3) |

## 5. Platform events (seen in the live runs)

| Event | Seen | Plan |
|---|---|---|
| Host sleep (Modern Standby) | Nov 15 01:15-01:20 UTC, services restarted | Disable sleep on AC; pre-open check of the power plan |
| Host clock skew | Nov 10 (W32Time stopped) | Pre-open clock check (exists) |
| Docker VM stall / memory exhaustion | Nov 13 (my parity replay, R76) | No heavy jobs in session (rule); VM memory headroom check |
| Postgres lock-table exhaustion | Nov 13 (R71, Stock Alerts) | R71 fix before Stock Alerts restart |

## 6. Order (recommendation)

1. Before Nov 27: early-close review and copy rehearsal (1.3).
2. Single-stock halt handling (1.1): per-member gate degradation, derived/daily completion over confirmed-empty
   slots, Stock Alert continuity; rehearse with gate 6 and switch on.
3. Options per-underlying cut-off (section 4) and `UNDERLYING_HALTED` labels.
4. Market-wide halt (1.2), unscheduled closures (1.4).
5. Lifecycle items (section 2): SNDK first-trade gap, META rename, adjusted contracts.
