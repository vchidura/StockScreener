# Downstream Publication Recovery

Updated: 2026-09-14 21:52 UTC, user-requested application shutdown verified
State: COMPLETE

## Objective And Authority

- Latest request: stop all running StockScreener application processes.
- Earlier scope: recover Screening/forward Alerts, then validate them in the portal.
- Approved shutdown: application workers, API/reloader, frontend/npm/esbuild.
  PostgreSQL, VS Code, and unrelated applications are preserved.
- Out of scope: other worker activation, options, price repair, historical replay,
  freshness-policy changes, backdating missed alerts, frozen-study hash changes.
- Done when: September 14 screening is published; the retained forward enrollment
  resumes with explicit missed-window accounting and a future next boundary.

## Context And Verified State

- Owners: [launcher](../../backend/scripts/start_workers.ps1),
  [Screening](../../backend/scripts/run_screening_worker.py),
  [Stock Alerts](../../backend/scripts/run_stock_idea_worker.py).
- Startup contract: [README](../../README.md#resident-worker-checklist).
- Initial current process check found only Vite, not the API or any worker.
  The earlier audit's process inventory is no longer current.
- Screening initially retained September 11 daily / September 14 closing hourly.
- Forward checkpoint initially retained 386 members, source-readiness v2,
  last check 20:14:34Z, next boundary September 14 20:00Z.
- The original closing deadline has passed. Resume must record MISSED_PUBLICATION,
  not manufacture a closing signal at a historical publication time.
- Launcher was initially an older version; it changed concurrently during work.
  Preserved the current documented single-worker/Plan implementation and validated
  it instead of overwriting the concurrent version. Manifest path is correct.
- Current launcher and forward tests: 61 passed, no real workers in tests.

## Recovery Results

- Screening published September 14 daily / September 14 20:00Z hourly at
  21:39:26Z. Generation `e69af963370036509f22cb14161412004ce09404bccd2a1674c9f901e088c12d`,
  snapshot `d8ce51b1-ba69-5660-b178-12e258258d86`. Default query: 386 members,
  386 matches, 0 unknown; daily/hourly stale flags false. Generation hash passed.
- Forward resumed the original enrollment from 14:50:39Z with the same 386 members
  and `stock_ideas_source_ready_v2`. Retains 12 publications after recovery.
- September 14 20:00Z closing window recorded MISSED_PUBLICATION at 21:39:55Z,
  after its original 20:59:55Z deadline: zero selected plans and zero closing outbox
  entries. The 15:30 ET run was not replaced with a backdated closing signal.
- Next forward boundary: September 15 14:00Z (10:00 ET). It still requires retained
  completed source inputs; restarting these publishers does not start ingestion.
- Read-only [verification script](../../backend/scripts/verify_downstream_updates.py):
  `backend/.venv/Scripts/python.exe backend/scripts/verify_downstream_updates.py --session 2026-09-14`
  completed with both integrity checks PASS. It reads current retained stores;
  it does not replay detectors, certify older research, or modify evidence.

## Runtime State

- PAUSED by the user after successful recovery and portal validation.
- Screening, forward Alerts, and API terminal trees stopped. Vite/npm/esbuild
  stopped only after verifying their current process ancestry.
- Verified at 21:52:08Z: zero Python/Node/esbuild processes and zero listeners on
  application ports 8001, 8002, 5173, 5174, 5175 (IPv4/IPv6 listener inventory).
- PostgreSQL and VS Code remain running. No market records or configuration deleted.
- No active application execution remains from this task. Do not reuse historical
  terminal IDs/PIDs or automatically restart services without a new user request.

## Boundaries And Next Step

- No downstream activation step remains. Future market-session cadence is not
  yet reverified; do not infer it from a successful after-hours resume.
- Browser: Screener September 14 / 386 matches / 0 unknown / 100 visible rows;
  HTTP 200, daily and hourly stale flags false. Saved screens/filters not edited.
- Alerts Latest Run: HTTP 200, zero new plans, Run Details says missed publication;
  next window September 15 10:15-10:29:55 ET. Day History renders 42 earlier SHADOW
  alerts; none from the missed closing run. Browser left on September 14 history.
- Display caveat deferred: missed run's Published time shows deadline 20:59:55Z,
  while actual recovery recording was 21:39:55Z. Stored missed status is correct;
  no presentation/model changes were made during validation.
- The successful browser checks above preceded the requested shutdown; the portal
  is now offline. Future new-session updates need separately authorized service
  startup, including equity ingestion. Browser validation covered responses,
  rendered text/rows, and DOM activation, not layout/pointer certification.
- Options, price gaps, historical accounting, and frozen-source drift remain
  deferred per the user. No fixes or broad audits were attempted for those items.