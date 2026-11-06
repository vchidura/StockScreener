# Advanced Realtime V3 - Current Runtime Checkpoint

Updated: 2026-11-06 09:35 UTC - **Ready for the open at 25.** All 16 new members READY at 09:25 under revision 1626d44f (25 READY in total). Final check on copy `test_realtime_v2_recovery_20261104s25c` (live backup 09:25): activation at a simulated 14:16 starts 25 members, manifest 25, plan target 25, profile `advanced-equity-ingress-25-v5`. Options lifecycle healthy, Nov 6 Options preparation for 9 underlyings waits for capture at 14:31:30. Copies s25a and s25b dropped. Wake-ups at 14:20 and 14:45 UTC.

Updated: 2026-11-06 08:20 UTC - **Options on 9 underlyings for Friday; G90 worked around; Options roles started; stop-rule watcher running.** User asked for Options on the 9 liquid names (SPY QQQ AAPL AMZN IWM MSFT NFLX NVDA TSLA) and approved deleting the stale Nov 6 Options preparation (G90). Desired revision now `1626d44f-67b5-5ef7-ac36-84bdeaa60319` (25 Equity, Options 9; `recovery_steps_nov5.py desired25o9`); Equity preparation of the 16 restarted under it (READY expected ~09:45). The five stopped Options roles were started ~08:00 (idle until the session); Options lifecycle healthy since 08:10. Policy: Options stays on v2 (env unset); v3 logic has 20 passing tests but the env wiring and a replay on real chains are untested, so v3 moves to Monday with the 23. `runner/live_watch.ps1` restarted 07:57 with the Options scheduler added to its stop list (G80). Wake-ups at 14:20 and 14:45 UTC. **Fallback before 14:05:** `desired9` (Equity 9; Options would then need the G90 cleanup again).

Updated: 2026-11-06 07:50 UTC - **G88 fixed and deployed; rung-25 profile active; Check 2 passed on a copy.** New-member preparation crash-looped in DERIVED_INTERVALS (G88). Preparation role on r34 (`sha256:24299ab89f7d0849db05390e0b2bc11b80e59bdffdc6c93766663a2237abeb6a` = r19 plus `advanced_equity_preparation.py`, `equity_interval_derivation_worker.py`; rollback r19) since 07:42; live preparation of the 16 resumes (rehearsal: 3107 s, all 25 READY, AMD bar counts exactly as targeted). Activation at 25 needed the reviewed ladder profile: registered and activated on live at ~07:48 with `prepare_realtime_v2_advanced_ingress.py sync-profiles --universe-target 25` and `activate-profile` (via `runner/run_ingress_tool.py`), profile `84af4b94864b6bcfaae30da3b2d1b305d6b60b20bfa00972157f9559ceddef57`, stability evidence `ca09b30d…` (`evidence/scale25/stability-waiver-20261106.json`, the user's waiver), capacity evidence `fedb9faf…` (`evidence/scale25/capacity-20261106.json`), approval `user-approved-v3-target25-activation-20261106`. Check 2 on copy `test_realtime_v2_recovery_20261104s25b` (live backup 06:43 plus rehearsed stages 4 and 5 plus the same activation): simulated activation at 14:16 starts 25 members, manifest 25, plan target 25, profile 25-v5. **Next:** live preparation to READY (watcher `bq9b5841v`), a final activation check on a fresh copy of live, start the stopped Options roles ~13:45, watch 14:15.

**Monday Nov 9 plan, agreed with the user Nov 6 ~06:20 UTC: Options on 23 underlyings.** The user's 25 Equity minus XLE and XLC (includes MSTR, not META): AAPL AMZN MSFT NVDA TSLA NFLX GOOGL MSTR AMD AVGO MU INTC PLTR ORCL CRWD JPM SPY QQQ IWM XLK SMH XLY XLF. SPX and NDX are development (index price source, SPXW and NDXP roots, AM settlement), not Monday. Weekend gates: (1) G74 item 2 chain freshness on one unified Options image for all 7 roles; (2) G72 label instead of block; (3) CPU limits on Options roles; (4) copy rehearsal replaying an Options session at about 11.5 times Nov 5's rate with Equity windows running; (5) after Friday's close apply a desired revision effective Monday with the 23 `OPTION_UNDERLYING` members (Friday's 25 are source members then, so no Equity re-preparation). Any gate failing: Monday runs the 9 liquid names (SPY QQQ AAPL AMZN IWM MSFT NFLX NVDA TSLA) and the user is told which gate failed. Also on the weekend list: G84, G86, cohort design review.

Updated: 2026-11-06 06:10 UTC - **G74 item 1 and the Dashboard cadence deployed.** `realtime-v3-options-scheduler` on r32 (`sha256:ca57d43d329cc1015e70eb9f6ac247fe9da385dd217812ee1a9223f9d953b77f` = r19 plus `advanced_options_continuous_scheduler.py` and `run_advanced_options_v3_scheduler.py`; rollback r19 `sha256:e2bbac18…`): an exhausted snapshot retry budget, or a missed E/O deadline, returns `SNAPSHOT_STALE_SKIPPED` for that Equity window and the process waits for the next one instead of crash-looping; running since 06:01, `WAITING_FOR_SESSION`. Equity scheduler on r33 (`sha256:2ff25c9cae439c080a87f78a4386045121254bf539bc069c4543a2f4168809ec` = r31 plus `runner_environment.py`, `stage_runners_equity.py`, window script; rollback r31 `sha256:65991138…`): the Dashboard is rebuilt only on a process's first window for a cohort, on :35 windows and at the close (user, Nov 6: the Dashboard may stay stale; it is a sample awaiting redesign); other windows carry the previous one forward (current releases only). Rehearsal of 9 windows on the copy: 72 s cold, 6.5 to 14 s without the Dashboard, 27.8 s with it. Tests: 138 window/runner/orchestrator/scheduler and 24 release tests pass.

**Options scope decision, Nov 6 ~06:15 UTC.** User: widen Options to the 9 liquid names on Friday only if I recommend it, and to all 23 from their list on Monday once the G74 fix lands. My call: Friday stays SPY and QQQ (a list change would restart the 16 members' preparation; first day at 25 Equity on r31; G74 unfixed). Monday: apply the 23 Options underlyings in a revision effective Monday after Friday's close (Friday's 25 are then source members, so no Equity re-preparation), only after the G74 fix is built, rehearsed and deployed. Weekend list: G74, G84, G86, Dashboard produce (G85 follow-up), cohort design review.

Update 06:00 UTC: META could not finish preparation (G86, traded as FB until June 2022). On the user's choice MSTR replaces it: revision `c9e10b36-ea37-57b5-bbd2-c9c1e36e24f4` (effective Nov 6) replaces `33e96266`; preparation of all 16 new members restarted under it (work is per revision; the preparation role claims only the current revision's work). `runner/recovery_steps_nov5.py` `SCALE25_EQUITY` now lists MSTR.

Updated: 2026-11-06 05:40 UTC - **25 Equity members applied for Nov 6; preparation of the 16 running; Equity scheduler on r31 (G85 warm windows).** Equity scheduler `sha256:65991138102e2caca8db516c68af9f11b1f4ccbf742cd53dfdc072d46403b198` (r31 = r30 plus `run_realtime_v2_advanced_equity_window.py`, `dashboard_repository.py`, `stage_runners_equity.py`); rollback r30 `sha256:bca2afd71a18…` then r19. Rehearsal on copy `test_realtime_v2_recovery_20261104s25a`: 9 consecutive windows in one process, first 62.6 s (cold), then 19.9 to 26.8 s; every PORTAL_READY identical to the original; screening rows identical (only `projection_code_sha256` and the generation digest differ, from G82). Desired state: revision `33e96266-1ac6-5e81-9356-e0f11c4f8166` (19 EQUITY incl. SPY/QQQ/IWM, 6 SECTOR_ETF XLK SMH XLC XLY XLF XLE, Options SPY QQQ), effective Nov 6, applied with `runner/recovery_steps_nov5.py desired25`. The preparation role promotes each member to READY after `TECHNICAL_WARMUP`. Rehearsed on the copy: activation at a simulated 14:16 with the 16 still WARMING starts with the 9 READY (partial activation); `desired9` reselects the original revision `062ec2af` and activates 9. **Fallback before 14:05:** `run_step.ps1 -Mode LIVE -Step desired9 -Script recovery_steps_nov5.py`. Options on SPY and QQQ needs no code change at 25 (live preparation checks containment; the 9-pins are on paths the continuous package skips). Watcher: `runner/watch_prep.ps1`. **Next:** preparation complete, then Check 2 at 12:30 on a fresh copy (activation at 25), start the stopped Options roles before 14:05, watch activation at 14:15.

**Authorization, 2026-11-06 ~05:10 UTC.** The user replied "yes, go ahead" to: (1) deploy G85 Fix 1 (warm loader and projection state kept between windows) and Fix 2 (Dashboard from the warm loader) to the Equity scheduler role before the open, only if the copy rehearsal passes, even at 9 tickers; (2) apply the 25-member desired state and its preparation (about 450 provider requests) if Check 1 passes; (3) go live at 25 at Check 2 (12:30 UTC), otherwise revert the desired state to the 9 before 14:05; (4) Options off on Nov 6 if Equity runs at 25; (5) waive the scale policy's 5-stable-sessions rule for this step. Still not covered: raising ceilings or budgets, clearing the ledger, READY rows, watermarks, deadlines or clocks, commit or push.

Updated: 2026-11-06 04:10 UTC - **Nov 5 closed (78/78) after the G82 fix; Equity scheduler on r30 for Nov 6.** The 21:00 window failed 5 times on a 0.12% closing-auction difference (AMZN, QQQ, TSLA) and the scheduler stopped retrying. The same check would have failed every Nov 6 window. User's "go 1": endpoint tolerance 1% outside `SCREENING_BASIS` (G82). Live close at 04:03 via `runner/recovery_steps_nov5.py windows` on r30 after a passing rehearsal on copy `test_realtime_v2_recovery_20261104g82a`; API status READY. `realtime-v2-advanced-scheduler` now r30 (`sha256:bca2afd71a18c997aaef9ea17c64c309b32c49ba344fb7aa05fb16df791c53b6`, r19 plus two files); **rollback:** `deploy_candidate.ps1 -Step hotfix -Service realtime-v2-advanced-scheduler -Image sha256:e2bbac18d7e7515d962a1efc9653137eb051a67541929e51bc0b6320ab65747b`. All other roles unchanged from the 18:15 entry below; the six stopped Options roles stay stopped until the Nov 6 morning step. New: G83 (overnight Screener current read 404, pre-existing), G84 (weekend: official close for raw daily bars, then 0.1% again). Backup before the close: `backend/backups/stocks_db_backup_v3-g82-nov5close-20261106T034441Z.dump` (sha256 `4B6FAF12…AEAFD8`). **Next:** before 14:05 UTC start the stopped Options roles; watch the first Nov 6 window (about 14:35) pass `EQUITY_PROJECTION`; v3 policy, G81 OS fix and crash-loop fixes still wait for the user's yes.

Previous: 2026-11-05 18:15 UTC - **Equity and Options both publishing; one Options scheduler crash cleared by itself (G74).** Live images: backend r19 (`sha256:e2bbac18d7e7515d962a1efc9653137eb051a67541929e51bc0b6320ab65747b`) everywhere except `realtime-v3-options-acquisition` r22 (`sha256:0b659311f67f8947d22e1a2343d3a9c1a00180010eb1904652955d0d02ca7d6d`), `realtime-v3-options-p8-market-state` and `realtime-v3-options-p8-flow` r28 (`sha256:c0ea8fe39b9f58987259b7667d21fa0a6ccb4f46be832b313a33da183b295b5b`), `realtime-v3-options-p8-window` r29 (`sha256:4e0872d8423f9fd0819069f2aff4330677ed15dca2e2eaa881e57f7c2c730f24`); frontend f17 (`sha256:413943b85018f76b09015a88bca4f96a77d576b936bde18d664743ecadb912ce`, rollback f16 `sha256:73d71b5f117c3dca2d6936d2598f600d087f5dc8557420a82c28dafed72369c4`). Run validations only after the Equity window finishes (position 135 s or later): one started at 118 s and the 18:55 window took 127 s. Feed attempt 6 of 12: do not restart acquisition unless needed. Validation tools added: `runner/tech_validate.py`, `runner/strategy_validate.py`, `sim/pages_survey.py`, results in `data-audit/`. r29 adds provider IV and gamma to Options Screener rows from the stored snapshots (`rt.raw_payloads`), display only. All changes after `9418820` are uncommitted. **Next:** confirm G73 on live, finish portal validation, monitor to the close near 21:07, then the after-close list in the gap register. **Rollback:** `-Step hotfix -Service NAME -Image` with the previous image: window r27 `sha256:1ab7908cc84730d5bf2fcd5b88cfe01caffcfb0003957c5f88a13812d9438f14`, market-state and flow r19 (the image above), frontend `frontend-candidates.txt`.

Earlier: 2026-11-05 16:02 UTC - **Options publishing on the real feed with a labelled hole, user's decision (G63, G68, G69).** Live images: backend r19 everywhere except `realtime-v3-options-acquisition` and `realtime-v3-options-p8-window` on r22 (`sha256:0b659311f67f8947d22e1a2343d3a9c1a00180010eb1904652955d0d02ca7d6d`), frontend f9 (`sha256:6bf3b1e3f2bf50ad12fa680d5edfc8c15fa12cc8416d676c819730c64bb27590`). Windows 15:50, 15:55, 16:00 complete, lag 1 s, feed attempt 6 of 12. Do not restart acquisition again today unless needed: each restart costs an attempt. Portal validation runs through `runner/run_portal_validation.sh`, results in `data-audit/portal-validation.json`. All changes after `9418820` are uncommitted. **Next:** act on validation findings, monitor to the close near 21:07. **Rollback:** roles with `-Step hotfix -Service NAME -Image` and the previous image from the tool's output: acquisition r21 `sha256:1299cfe073f22737bfc42d04bcc833d90727744b0d8c981d719c02c15b089698`, window r19 `sha256:e2bbac18d7e7515d962a1efc9653137eb051a67541929e51bc0b6320ab65747b`, frontend f7.

Earlier: 2026-11-05 15:40 UTC - **Options root cause fixed, publication still blocked, waiting on the user (G63, G68).** Live images: backend r19 everywhere except `realtime-v3-options-acquisition` on r21 (`sha256:1299cfe073f22737bfc42d04bcc833d90727744b0d8c981d719c02c15b089698`), frontend f7 (`sha256:86f8c90950edd95ba787f2dfa8e1c9433287fffa3cbf0b6614d81b5e9a0e3b60`). Options feed at real time, lag 1 s. A 941 s gap from the restart blocks later windows under the 120 s rule. User asked on Nov 5 that, once Options is resolved, every price and technical value on the portal be validated against the database; a database that lags the market is acceptable, a page that differs from the database is not. Changes after `9418820` are uncommitted. **Next:** the user's choice on G68, then the portal validation. **Rollback:** acquisition `-Step hotfix -Service realtime-v3-options-acquisition -Image` with r19 `sha256:e2bbac18d7e7515d962a1efc9653137eb051a67541929e51bc0b6320ab65747b`, which brings the throttle back; frontend as below.

Earlier: 2026-11-05 15:18 UTC - **Equity healthy, Options not publishing since 14:50 (G63).** Equity windows 75 to 90 s, stock alerts firing. Options feed is capturing but about 12 minutes behind and running at about 0.7 times real time, so windows pass their 5-minute deadline and stay PENDING. Options roles left running: no hotfix, because the limiter is not proven and a restart loses the backlog. Frontend f5 live (ticker page reads today, G64), f7 building for All Tickers, Screener and Market Conditions. Worktree changes after `9418820` are uncommitted. **Next:** deploy f7 if tests are clean, watch for Options recovery, close near 21:07, then G63 with a real-rate replay. **Rollback:** frontend `-Step hotfix -Service realtime-v2-advanced-frontend -Image` with f3 `sha256:e372fb88783586165edb274e3911628c3f657d0b653412b175049d7dd8f514b4`.

Earlier: 2026-11-05 14:42 UTC - **Session running on the real feed.** The scheduler cleared by itself at 14:05:27 UTC as predicted (G61, r20 not needed). Plan READY, stream limit 12, connection attempts 12, REST budget 72, cohort `dc98cb4f` at 14:15:08. Stock feed LIVE 14:30:03, Options feed LIVE 14:32:30. First Equity window 14:35 COMPLETE in 67 s, first Options window 14:40 COMPLETE in 20 s, all 20 containers up. No open stock alerts yet: the 9 rows per window with lifecycle `UNAVAILABLE` are not alerts. Watcher fault found and fixed, G62: it wrote nothing from 11:52 to 14:40 UTC. New watcher pid 17292; attention events come from `runner/watch_events.sh`. **Next:** monitor to the close near 21:07, then read the mid-session and post-close pages. **Rollback:** unchanged, see below.

Earlier: 2026-11-05 13:50 UTC - Candidate source committed locally at the user's request: `9418820` on branch `agents/v3-stable-nov5` in worktree `StockScreener.worktrees/v3-stable-nov5`, 137 files, not pushed, not merged to `main`. Live still runs r19, which lacks only the scheduler guard for G61 (in r20, held as a fallback). These docs and the git-ignored recovery tools are not in that commit. At 13:54 UTC, also at the user's request, the six other `agents/*` worktrees were saved as unreviewed checkpoints on their own branches, not pushed: `advanced-equity-scheduler-reliability` `df3e469`, `advanced-realtime-gap-fixes-implementation` `fcbf8a6`, `v3-integration-holistic-review-fixes` `468f8a1`, `wp1-options-portal-read-correctness` `a7ccfe6`, `wp2-advanced-options-rollback-implementation` `650a525`, `wp3-canonical-stock-alert-detector` `2cf1446`. After the close: compare their differing files with `9418820`, then move `main`'s staged work and these docs to a new branch without the 6 test-output XML files. Controller hotfix at 13:41 UTC raised `--stream-sessions` to 12, approved by the user. **Next:** scheduler at 14:05, plan READY at 14:15, monitor the session and close. **Rollback:** unchanged, see below; the commit changes nothing on live.

Earlier: 2026-11-05 11:55 UTC - **Gate D done, live is on backend r19 and frontend f3.** All 20 containers replaced one at a time, migration 048 and the two grants applied, security audit passing. Backup `stocks_db_backup_v3-pre-stable-rollout-20261105T114159Z.dump`, SHA256 `1DAEDE4003A52555F42A21B78C2C9D08CE48A10A24C3CC807F54D1856C15377A`, volumes in `backend/backups/stable-rollout-*`. Nov 2 to 4 regenerated on live: 52, 75, 83 alerts. Real Fed calendar and Finnhub refreshed. Browser run on the live portal: 11 of 18 pages clean, the 7 strategy-backed pages have no publication (G38). The scheduler restarts until 14:05 UTC (G61), expected to clear by itself. Safety watcher `runner/live_watch.ps1` is running detached, pid in `data-audit/live-watch.pid`, log `data-audit/live-watch-20261105.log`. **Next:** confirm the scheduler at 14:05, plan READY at 14:15, then monitor the session and close. **Rollback:** `rollout/stable-deployment-plan.json` has each previous image, and after new writes stop writers and verify the snapshot on an isolated target first. Undo regeneration: `DELETE FROM rt.alert_current WHERE projection->>'regenerated'='true'` as admin.

Earlier: 2026-11-05 11:40 UTC - Rehearsal r18 passed: Equity 12 of 12 with two stock drops, automatic close at retry 4, 15:37, pointer 15:30, daily bars in both bases, both gaps closed by themselves, 4 of 4 forecast stock alerts, O1 709 evaluations across 9 datasets, Options 9 of 11 published. Suite: 112 failed against 148 for the unchanged package, 0 new. Rolling out backend r19 (`sha256:e2bbac18d7e7`) and frontend f3 (`sha256:e372fb887835`) now. **Next:** `deploy_candidate.ps1` steps, regeneration, page checks, session monitoring with `runner/live_watch.ps1`. **Rollback:** `rollout/stable-deployment-plan.json` has each previous image; after new writes, stop writers and verify the snapshot on an isolated target first.

Earlier: 2026-11-05 08:55 UTC - Nov 4 is closed on live. Producing alerts is now in scope for Nov 5 (user, Nov 5). Rehearsal r5, quiet feed, Options window role swapped three times for fixes, Equity untouched: the 4 forecast stock alerts fired on the forecast bars (IWM and MSFT 15:15, IWM 15:20, AAPL 15:25), appeared on the real Stock Alerts page, and the session closed itself at retry 4, 15:37. Final rehearsal r11 is running until about 10:25 UTC on `alpha-research-realtime-v3:stable-nov5-r11-11ff25bdb33a535d` (`sha256:22fd7145d338`) and frontend `stable-nov5-f3` (`sha256:e372fb887835`): trades on, drops at 14:52:30 and 15:03:30 (`sim/inject_drops.sh`), first real test of O1 with flow, mid-session pages (G49, G53, G55). Nothing new is on live yet. **Next:** read r11, full suite on r11, then gate D with `deploy_candidate.ps1`, then `-Step regenerate-plan` and `regenerate-alerts` (G56), before 14:00 UTC. **Rollback:** nothing new is on live. Working to `advanced-realtime-stabilization-plan.md`, gates C and D.

## Nov 4 closed, 21:16 UTC

78 of 78 windows, daily bars for all 9, pointer on 21:00, AAPL 1m head fixed (G36, one row, `runner/fix_aapl_live.ps1 -Rollback` undoes it). 77 windows ran
live with no loss; the 21:00 window needed the rollover, done by hand on today's images after a rehearsal on a copy (`runner/recover_nov4.ps1`). Rollover refuses until
about 5 minutes after the close. Backups: `stocks_db_backup_v3-nov4-close-20261104T210212Z.dump` (before the close steps), `stocks_db_backup_v3-nov4-closed-20261104T212253Z.dump`
(SHA256 `4B55163592F18FBC6D48232C785E9F4DE8CEB26BE0D372C2949EC23F0F0FD479`, after them). Options stayed stopped and its Nov 4 journal is sealed.

**Candidate:** worktree `StockScreener.worktrees/v3-stable-nov5`, uncommitted. Package source plus: consumers hold at each cutoff and a stop no longer seals (G27, G28, G31, unit tested only),
checkpoint gate reads latest heads with a digest that follows bars (Nov 3 retry failure, G2, G3), bar head tie goes to the later write (G36), selection ranking. In progress: tolerate short closed Options gaps, one bad quote no
longer blocks a window, far-future stamps, retried Options windows, keepalive (G41). Retry timing: keep the package's `_retry_completion_budget`, it already caps at the next window (G5).

**Next:** finish the candidate, build one image (`runner/build_candidate.sh`), derive `rollout/compose.stable.json` (`runner/make_stable_compose.py`), then dress rehearsal in the sim. The sim now runs a one-hour Nov 5 session through a sim-only calendar overlay, so the automatic close is reached at real speed without jumping the clock. **Rollback:** nothing new is on live; `docker inspect` shows the same images as this morning.

## Dress rehearsal 1, passed Nov 5 04:05 UTC

Sim only, one image `alpha-research-realtime-v3:stable-nov5-r1-005b64cadcc264cf` (`sha256:bbbfbb4b8c7e`) on all roles, no edits or swaps during the run, one-hour session by a sim-only calendar overlay, feed at live rate. Equity 12 of 12: 99 to 148 s with Options on and a full test suite running beside it (the sim's own Postgres at about 90% of a core, peak 209%, loaded by synthetic flow far above the real feed's). Close without a person: `preparation` ran the rollover, retry 4 completed the closing window at 15:40, pointer on 15:30, daily bars both bases for all 9. Options 10 of 11 cutoffs published, each in 8 to 13 s, first read pin at 14:45, last at 15:30. The other cutoff, the session's first, stayed deferred on `OPTION_VOLUME_CHECKPOINT_INCOMPLETE` while the baseline warmed. Equity's first window failed once at 14:35 and retried, as on live. Average and peak CPU % of a core: scheduler 48/135, market-state 30/101, alerts 17/153, flow 15/52, acquisition 9/26, options-ingress 5/27, Postgres 89/209.

Real provider behaviour is not proven by any of this: the feed is synthetic, with about 490 flow events a minute against 1 in 7 minutes on the real Nov 4 feed.

Permission rule: `deploy_candidate.ps1 -Step preflight` ran without a block at 03:10 UTC. Result: all 20 services dry-run, 048 pending, audit fails only on the two DELETE grants that `migrate` applies. That does not prove later steps will be allowed.

## Nov 5 candidate, measured so far (00:47 UTC)

Realtime suite on image `84ffaaceba33`: package source 453 failed, 4,072 passed; candidate 417 failed, 4,216 passed, no new failure names (read-only mount and missing services cause most). Canary `test_v3_equity_provider_free_close_restart_retry_and_next_slot` passes on the candidate. `test_p8_provider_free_ingestion_to_canary_alert_publication` fails at `assert flow_rows["events"]`, the same on the unchanged package image tonight, though the package recorded it passing on Nov 4, so it depends on date or time and is not evidence either way.

Sim, Nov 5 session from the closed Nov 4 backup, candidate image, migration 048 and grants through the live commands, one-hour session by a sim-only calendar overlay. Equity alone: 12 of 12 windows, about 105 to 124 s, first attempt each, 15 s start delay, `preparation` ran the rollover by itself by 15:36 and the closing window completed at retry 4 at 15:38, pointer on the close, daily bars in both bases. No person involved. This is the first time the automatic close has run. With Options: Equity 103 to 123 s. Ingress, market-state and flow all reach the same offset at each cutoff within 6 s (for example 66043 at 14:45), so the cutoff hold works on a moving feed. The Options window has not published yet; see G43 for each stop so far.

Live deploy tools ready, not run: `recovery-20261104/deploy_candidate.ps1 -Step ...`, `rollout/deploy_stable.py`, `rollout/compose.stable.json`. `prepare` dry-ran all 20 services on live with no change.

## Nov 4 supervised Options collection, ended 18:19 UTC

Measured on live, QQQ and SPY, 16 contracts. Feed connected at 18:11:29 on the candidate image, stopped at 18:18:58. Equity windows:
about 68 s before, then 136 s (18:10) and 194 s (18:15, projection 107, publish 80). Stopped early, below the 200 s limit, because the
trend would have crossed it. `acquisition`, `p8-ingress`, `p8-flow`, `p8-market-state` all exited 0 by 18:19:09. CPU while running:
acquisition 50 to 280%, each processing role 80 to 190%, health probes included. Collected: 27,698 raw messages, 46,300 events,
5.9 MB, about 6,500 events a minute, one drop (G41), 1 flow event, volume `INCOMPLETE` after the reconnect, residents complete for the
18:15 and 18:20 Options cutoffs. `options-scheduler` crashed once at 18:12:26 on a provider `ConnectionError` and Docker restarted it
(1 restart). `acquisition` stays stopped on candidate `5b0e8517`; do not restart it during this session. Plan for tonight is
unchanged. The approval and plan for this run follow.

The user approved at about 18:05 UTC: run the Options feed and middle roles to measure load with Equity, for 60-90 minutes, no
publishing, stopping by 20:00 UTC at the latest. `p8-ingress` (18:07:10), `p8-market-state` (18:07:37) and `p8-flow` (18:08:05) are
started on their existing images, by `docker start`. `p8-window` stays stopped because it cannot publish (G26 to G28, G39).
`acquisition` is still stopped on its old image: the auto-mode classifier blocked `rollout_candidate.py start` [Production Deploy],
so the user must run `recovery-20261104/deploy_acquisition_now.ps1` (result in `rollout/deploy-result-acquisition.txt`). Watcher:
`runner/opt_measure.sh`, CPU samples in `data-audit/opt-measure-cpu.log`. Stop rule: stop the Options roles if an Equity window
passes 200 s. Stop order: `acquisition`, `p8-ingress`, `p8-flow`, `p8-market-state`, each `docker stop --timeout 30`. If the feed has
not started by about 19:00 UTC, stop the three waiting roles, since their health probes cost about 4 s in every 9 each (G34).
Rollback: stopping those roles returns Options to how it was, apart from the new acquisition image if the script ran.

## Nov 4 integrated deployment preparation

The user now authorizes both pipelines end-to-end with monitoring, migration 048, and the
two required app-role DELETE grants on `rt.advanced_options_read_pins` and
`rt.portal_release_current`. Existing Equity nine / Options QQQ+SPY membership, budgets,
deadlines and immutable evidence must remain unchanged. No Git commit or push is authorized.

After the 14:00 UTC rollout cutoff, the user explicitly chose a supervised attempt using
the existing containers. All seven Options containers were started individually, without
changing container IDs or images. Acquisition repeated the completed-session spin and was
safety-stopped at 14:08 UTC: status `COMPLETE`, 12 old connection attempts, 108,466 decoded
messages, 29.48% CPU on the second observation. Six Options roles remain running, but no
Nov 4 Options session exists. Do not equate 19/20 running containers with end-to-end readiness.
No live schema, grants, images, universe, budgets, clocks or READY rows were manually changed.
`nov4-watch` is running and reported the acquisition stop and the Nov 4 Equity ingress plan
`152478ab` becoming `READY` at 14:15:32 UTC. Its stop time is 21:25 UTC; no external alert
channel was configured. Equity has nine READY members; Options QQQ/SPY remain WARMING.

Fresh audit: Nov 3 closed 78/78; latest retained pointer is 21:00. All nine tickers have
history and feature/setup outputs, but unfiltered 30m history still rejects divergent
sources. Fundamentals and condition/exchange lookup tables are empty; earnings coverage
expired October 30. Treasury observations reach November 3. Options preparation still
retries QQQ catalog and SPY capacity. These are not a blanket all-inputs-ready verdict.

Preparation artifacts are in session
`759a9489-ebe9-4065-81e2-a68f86754f19/files/`: immutable source snapshot/hash
`0cc6500624faa43a235ef07638215a1e6eb2fd8c7a5d63dc66b1e38202b6db71`,
backend `alpha-research-realtime-v3:package-20261104-0cc6500624fa` (`84ffaaceba33`), frontend
`alpha-research-realtime-v2-frontend:integrated-20261104-0cc6500624fa`, and scoped package
scripts / `compose.package.json`. Frontend digest is `ec1bba637d21`. Source is the holistic worktree plus the main worktree's already-rehearsed
acquisition session-status/spin fixes and regression test. Neither source worktree changed.
The final image stores code at `/backend`, with `/app` compatibility, so repository source
audits work in containers. Final-image checks: 217 passed / 20 skipped; both disposable
Equity close/retry and Options ingestion/publication/API/rollback canaries passed; frontend
31 passed and TypeScript/Vite build passed; all 20 service dry runs passed. Isolated httpx
0.27.2 fixes the test harness only; production dependencies were unchanged. Earlier failed
packaging/test-environment attempts are retained as failures, not counted as passing runs.

Verified dump: `backend/backups/stocks_db_backup_v3-integrated-predeploy-20261104-20261104T133444Z.dump`,
284,587,466 bytes, 2,794 entries, SHA256
`745728AEFD0A49B902C89170DB7B453FA3C1ABC5516A2EF8C5B3ADA3744C56F3`.
Fresh copy `test_realtime_v2_recovery_20261104integrateddeploy` restored successfully,
migration 048 applied there only, all 78 cutoffs retained, and hardened security audit passed.
Live audit finds exactly the two approved missing DELETE grants; no live grants applied yet.

The missing Options-window image was preserved by exact container filesystem export
(`backend/backups/options-window-filesystem-20261104.tar`, 1,003,786,240 bytes) and imported
as `alpha-research-realtime-v3:rollback-options-window-20261104` (`4481efdb221a`).

Portal: SPY/AAPL daily charts each render 283 bars and retained/stale notifications in the
live browser. AAPL 1m still returns 503; Options Flow remains unavailable. Latest-package
API smoke on the migrated copy returns 131/153 HTTP 200: 13 retired-route 404s, eight
missing-identity/argument 422s, one real missing retained Strategy release 503. This is not
a blanket portal pass or live acceptance of the undeployed frontend.

Next: watch the first Equity release after 14:35:05 UTC and protect Equity from Options
load. Full deployment remains pending the next permitted closed-market window after
21:10 UTC: fresh writer-stopped DB/volume snapshots, migration 048, the approved two grants,
fresh security verification, one-role-at-a-time rollout and live browser acceptance. The
rollout helper refuses protected hours and missing fresh migration/security verification.
After new-version writes, rollback requires verified DB/volume restore to isolated targets;
image-only rollback is not assumed safe. Do not restart the old spinning acquisition blindly.

## Nov 4 holistic source-integration checkpoint

The clean integration candidate is in worktree `v3-integration-holistic-review-fixes`, based on
`0ecd5d9d397d3f0c376e5d316048b326a2eb0112`. It combines WP1-WP4 and the non-overlapping gap
remediation with manual owner-first overlap resolution. The candidate has 83 changed files and
remains uncommitted. No live image, service, database, schema, budget, readiness row, provider
state, delivery path or trading path changed.

The holistic-review blockers are corrected in source:

- Options rolling `_volumes` no longer references undefined `event_rows`; its production path runs
  in regression coverage.
- Positional admitted-contract cursors are normalized, and Flow uses bounded full-session grouped
  recomputation so late inserts and corrections are visible.
- Volume baselines remain pending per contract/snapshot instead of being replaced by later windows.
- Flow, Screener and Alerts preserve exact release/session pins and consistently serve retained
  stale data with stale metadata.
- Canonical Stock Alerts keep the real nine-member product membership while SMH/IGV/etc are
  context-only benchmark owners with exact reference/bar/hash/clock evidence and member-local
  `UNAVAILABLE`.
- Equity `INPUT_READY` persists an exact `known_by` visibility seal in migration 048. Projection,
  publication, Stock Alerts, Strategies, scheduler recovery and rehydration reuse that seal rather
  than a window deadline or mutable heads.
- WP2 executor tests create their owned artifact parents in a fresh worktree; production preflight
  remains strict. Ticker cache tests validate unique materialization identities.

Provider-free validation is complete for the candidate: WP1 backend 205 passed; four frontend
suites 31 passed and the production TypeScript/Vite build passed; WP2 executor/read-pin/rollback/
security 94 passed plus its full disposable ingestion-to-publication/API/rollback canary; WP3
product/publication/portal 66 passed plus 3 disposable persistence/replay tests; WP4 combined
non-DB 275 passed and its owned disposable recovery scenarios passed; corrected resident/window/
Flow coverage is 392 green checks. The provider-free Equity close -> interrupted publication ->
restart retry -> next-slot pointer rehearsal also passed. `git diff --check`, conflict-marker scan
and backend compilation pass.

Remaining evidence is live-only: commit/build immutable images, separately approve and apply
migration 048, render/audit the intended services, perform the live first publication and close/
drain observation, and complete live browser acceptance. A broader legacy mixed rehearsal still
contains unrelated Strategy-generation and retired synthetic Stock Alert fixture failures; it is
not the passing focused Equity integration above. Current execution and blocker dispositions are
recorded in `docs/ADVANCED_REALTIME_V3_FIRST_RUN_READINESS_2026-11-02.md` sections 8 and 11.

## Nov 4 session checkpoint

**08:00 UTC: Options windows cannot publish in their current design, so the go/no-go is heading to no for Options.** Found by
running the real window role in the simulated world (gaps G26 to G31). The rolling producer expects the state to stand still at
the cutoff, as it did in the bounded single-window tests, but with a live feed its consumers carry on. The volume baseline also
never completes and quiet windows have no flow event. This is the first step of the window, so later stages are still unrun. On
live on Nov 3, all 10 Options windows stayed `PENDING` and no Options stage ever ran. Equity is unaffected. The prompt's default
applies unless the user decides otherwise: Equity runs, the universe does not change, and Options roles stop before the open.
Stopping `options-lifecycle` and `options-acquisition` is the action at about 13:30 UTC. The acquisition deploy then matters less;
the scheduler deploy still matters. Next action: the G25 swap test, then the close in the sim. Rollback: nothing on live changed.

08:25 UTC: G25 swap test done for Equity, no check changed (register G25, G32, G33). `deploy_nov4.ps1` now moves the scheduler
first, so an acquisition failure cannot hold back the Equity fix; both plans still pass and the user has been told it is ready.
Live has 15 of 20 containers running; the 5 stopped ones are `options-scheduler` and `options-p8-{ingress,market-state,flow,window}`,
stopped at about 21:06 UTC on Nov 3. No live image has changed. Next action: injected feed drops and role restarts, then the
close in the sim with the stuck rows from the swaps. Suite baselines are running: scratchpad `suite-head.txt`, `suite-current.txt`.

09:15 UTC: image generation, scoped deployment and the sim are documented in
`docs/ADVANCED_REALTIME_V3_FIRST_RUN_READINESS_2026-11-02.md` section 13. Generating an image does not need the sim. Feed drops
and role crashes were tested (G35: a stock drop costs a window). The clock-jump close test was dropped as invalid, so the G5
retry budget still has only a unit test and G32's effect on the close is unknown. The sim has been down since 09:08 UTC with its
volumes kept. `deploy_nov4.ps1` has still not been run. Next action: the go/no-go report by 12:00 UTC, then pre-open checks at
13:30 and stopping `options-lifecycle` and `options-acquisition` if the answer is no or there is no reply. Rollback: nothing on
live changed.

09:35 UTC: data audit of all 9 tickers, read-only, in `recovery-20261104/data-audit/` (`runner/q_data_audit.py`, `q_audit_*.py`).
Against the portal history plan and the exchange calendar: 1m 5 sessions, 5m, 15m, 30m, 1h and raw 1d 90 sessions, adjusted 1d
283, 1wk 261, 1mo 60, with no bar missing. 5m agrees with 1d on all 90 sessions. No unapplied split. The stock gaps are repaired;
the pre-open check's 81 open gaps are Options on Nov 3. Open: G36 (AAPL 1m chart, fix proven in the sim and on copies, needs the
user's yes: `runner/fix_aapl_live.ps1`, with `-Rollback`), G37 (calendar expired, fundamentals never loaded), G38 (unbuilt pages).
The strict history reader refuses 30m for all 9 because 884 older bars have two copies differing only in VWAP rounding. No live
caller reads 30m without a source filter (read from code), and pages serve 1,170 bars, so no action. Nothing on live changed.

09:40 UTC: whole realtime suite, both on image `7921bc32`. Commit `0ecd5d9`: 419 failed, 4,043 passed. Working tree: 371 failed,
4,103 passed. 4 tests fail only on the working tree: 3 pass when rerun alone, and 1 fails because the read-only test mount blocks
a lock file in `.realtime-canary`, which the commit export lacks. So no regression was found from these changes, but the 367
tests failing in both runs cannot show one. Waiting on the user: `deploy_nov4.ps1`, Equity only, the AAPL fix, calendar timing.

**Nov 3 is closed on live, finished 04:11 UTC: 78 of 78, pointer on 21:00, all 9 members with raw and adjusted daily bars, last
15m/30m/1h present.** Rollover used 36 real requests. Real pages improved: session-close reads went from 503 to 200, 12 of 15
page calls and 126 of 153 routes return 200. SPY `HISTORY` went `COMPLETE` by the live role at 04:10:47. QQQ stays behind its
`CATALOG_QUOTE` retry at 14:31:30 (gap G21); not edited. No live image has changed.

**Waiting on the user: `recovery-20261104/deploy_nov4.ps1`.** The candidate acquisition and scheduler images passed their
rehearsal on a fresh pre-recovery copy, and both plans pass, but the auto-mode classifier blocked the live deploy, so the user
must run it. It must run before 14:00 UTC. Its output goes to `rollout/deploy-result.txt`; check that file, then the two images.
If it has not run by about 13:30 UTC, acquisition must be stopped instead, or activation can crash-loop it (gap G1). Next
action otherwise: the simulated world in `recovery-20261104/sim/` to prove the Options chain, then the go/no-go at 12:00.

User decision 07:05 UTC on gap G25: **loosen the checks that tie saved work to a code version.** Method agreed: in the sim, swap a
role to changed code mid-session (`overlay-swap/` holds the real Nov 3 projection change, set through `sim/image_overrides.json`,
then `sim_up.py role NAME`), record what refuses to run, change only those checks to record the difference and continue, and
leave every other check alone. Nothing on live changes. Selection ranking is fixed in source with regression tests
(SPY picks 4,755 to 175,755 traded, QQQ 44,287 to 149,800, flow-excluded picks 3 to 0, dry run on the stored Nov 3 chain); not
in any image. Tests: `runner/run_tests.sh IMAGE paths`, which mounts the repo root and pytest read-only.

Simulated world, started 06:50 UTC, see `sim/README.md`: real roles and their real images on an internal Docker network with
no route out, its own Postgres restored from a live dump, one shifted clock, and `sim/fake_provider.py` answering at the real
hostnames with synthetic data. Driven by `sim/sim_up.py`. Copying Nov 3's data cannot prove Options publishes: the real-data
windows have no stored binding, and later ones truthfully block on stale watermarks and gaps. Remove with `sim_up.py down --all`.
It must be down before 14:00 UTC so it cannot compete with the session.

Live recovery started 03:51 UTC. Rehearsal on clean copy `test_realtime_v2_recovery_20261104c` passed: 78 of 78,
pointer 21:00, both daily bases for 9, `HISTORY` READY for QQQ and SPY, API 126 of 153. Backup:
`backend/backups/stocks_db_backup_v3-pre-nov3-recovery-20261104T035026Z.dump`, SHA256
`78F4CB26926DE885598D34CC83BB762A7A6F5F0FFE0D695DA10D1B74EF348FF5`. Run with
`recovery-20261104/runner/recover_nov3.ps1 -Mode LIVE -Part X`, in order: `old-windows`, `new-windows`, `rollover`, `close`,
`check`. To see where it got to, `runner/show_window.py stocks_realtime_v2 HH:MM`; a part can be rerun, as it skips complete
windows. Rollback is restoring that backup, which needs a yes. No live image has changed. Task file: `advanced-realtime-next-session-prompt.md`. Gaps: `advanced-realtime-integration-gaps.md`.
New evidence goes in `backend/.realtime-canary/v3-supervised-20261102/recovery-20261104/`. Fresh copy:
`test_realtime_v2_recovery_20261104a`. New read-only probes in `recovery-20261103/runner/`: `q1_gate_race.py`,
`q3_valueerror_predict.py`, `q2_idle_speed.py`. Corrections so far: source is committed at `0ecd5d9` (the "nothing
committed" line above is stale); the acquisition spin ended at the 21:05 decision stop, and it now idles with bursts,
averaging about 65% of a core.

Retry chain, proven on the copy at 03:13 UTC (`recovery-20261104/patch_e2.py` applied to sources from `7921bc32`, mounted
read-only by `recovery-20261104/runner/run_step.ps1 -Overlay`): 20:45 and 20:50 completed on `7921bc32`, 17:50 on
`35808528`, and the pointer stayed on 20:55. Windows projected before the 20:24 scheduler change need `35808528`. Nov 3
needs no journal replay or gap repair by the audit: 1m and 5m are whole. Next action: fresh copy, rehearse all 14 windows,
rollover and the close, then back up and run on live. Before the open, fix or stop acquisition (gap G1), because closing
Nov 3 unblocks Options activation. No rollback needed yet: nothing on live has changed.

### Phase 1 findings: what went wrong on Nov 3

The chain, in short: the host slowed for reasons not yet known, a window overran its 300 seconds, its thread kept running
beside the next window, the next window started late, and a late start is itself fatal to the gate. Retries could never
succeed, so each slow window was lost for good, and at the end a retry held the serial loop through the close.

1. Retries after a timeout. **Proven on a copy.** The stock retry fails `ADVANCED_INPUT_GATE_DIGEST_CHANGED` even with the feed
   stopped (E1). The hypothesis was right but only the first of three layers: with the digest fixed the failure moved to
   `INPUT_READY_REUSE_MISMATCH` (`input_ready_identity` also hashes moving heads, E2), then to `CYCLE_REHYDRATION_MISMATCH`
   (E3). That one, undiagnosed until now, has two causes. Rehydration read at the retry's `deadline`, while the projection
   read at the first `release_at`: the stored hash matched at `release_at` in 5 of 5 windows and at `deadline` in 0 of 5. And
   evidence identity depends on `ticker_projection.py`, so a window projected before the 20:24 scheduler change only
   rehydrates on `35808528` (E5 failed, E7 passed). With the patch, 20:45, 17:50 and 20:50 all completed on the copy.
2. Why windows slowed. **Partly explained, cause not found.** Acquisition's status loop, which spun from 15:53, is an accidental
   speedometer: its rate against projection seconds gives r = -0.82 over 61 windows, and in minutes with a stage running it
   fell from about 10,500 lines a minute (15:55-17:25) to 3,000-5,500 in the slow periods. So the slowdown reached outside
   the scheduler, either host CPU or Postgres. Market-state and flow logged only at 18:40-18:44 and after 20:30, and ingress
   only after 20:46, so those roles do not explain 18:44-20:30. Options scheduler and window activity lines up with 17:33-17:52
   and with 20:35-21:00. Only 3 idle minutes exist in the slow periods and they disagree (19:14 full speed, 19:29 and 19:34
   slow), so cause and effect are not fully separated. Windows Update ran 18:21-18:36 only, and nothing in the system log
   covers 19:00-20:05. Postgres logs for Nov 3 are empty. Host facts: ThinkPad, i7-1270P (28W, mixed P and E cores), Balanced
   plan, `% performance limit` reads 76 at this moment, on AC. Not yet measured: a window alone against a window with
   Options roles, which goes in the dress rehearsal.
3. The 8 ValueErrors. **Proven for 2, strongly supported for the rest.** Log: `Advanced input gate checkpoint coverage is
   incomplete` at 20:52:03 (`stage_runners_equity.py:650`). `rt.consumer_checkpoints` holds only the latest head per shard,
   and the gate filters `committed_at<=release_at`, so the first bridge write after `release_at` hides the row. Timing fits
   7 of 8: each ended right after that write, and 76 of 77 other attempts with no such write passed. The eighth, 14:35, is a
   different cause, proven by count: 0 of 9 5m heads are visible at the first `release_at`, because materialize writes
   them afterwards. Nov 2 shows the same pattern, so the first window of every session fails once.
4. The 16 `ADVANCED_INPUT_NOT_READY`. **Not the bridge falling behind.** 4 were the real 15:25 gap (43 of 45 bars, repaired by
   REST at 16:21). 10 had all 45 bars and started up to 4.5 minutes late, so the same hidden head blocked them. 2 were true
   bar lag past `release_at` (20:35 by 7.9s, 20:50 by 0.3s) during a slow period. A retry passes these, so waiting inside the
   window is cheap but secondary: fix the clock filter first.
5. The close. **Proven from stage rows.** 20:55-retry1 started at 20:59:11 with a fresh 5-minute budget and held the serial loop
   until 21:04:41. So the 21:00 window, due at 21:00:05, started at 21:04:48. Materialize's 28-second "timeout" was the window
   budget running out at 21:05:18, not a slow stage. No attempt followed because retries stop at the drain deadline, and
   `_recover_completed_session_window` is not in the deployed image. It should have run a post-session attempt.
6. Feed drops. **Largely unknown.** Only the exception type is stored, never a close code. The first 8 Options drops, about every
   40 seconds from 15:21 to 15:29, fit keepalive starvation, and only 2 more came in the 11 minutes after the 15:42 GIL and ping
   change. That is inferred from the rhythm. The Stocks drop at 15:23 coincides with the crash loop, also inferred. The
   drops at 15:48:29 and 15:53:01 are unexplained.
7. The acquisition spin. **Proven from code and logs.** `run_acquisition` returns at once when attempts are used up, and `main` has
   no wait on that path, so it produced about 21 status documents a second, each with a database write, until the decision
   stop. It now polls every 2 seconds, yet still averages about 65% of a core with bursts to 404%, cause unknown. **It cannot
   take the next session:** deployed `_load_status` raises `ValueError` on the real status file with another session's binding
   (run), and `main` does not catch it (read), so the role should crash-loop at the next activation.
8. The OI block. **Fixed on stored data.** Deployed coalesce gives 16 of 16 `AVAILABLE` at the Nov 3 20:35 known time. At Nov 4
   14:15, 11 of those 16 have no baseline. What follows the block has never run anywhere.
9. The coupling. **Proven from code and logs.** The check is named for materialize, but it requires `window_run.state='COMPLETE'`,
   meaning Equity has published. That gave `WAITING_FOR_MATERIALIZED_E` on 1,091 of 1,234 scheduler statuses.

Also found: overrun stages keep running as daemon threads, up to 3 minutes past their timeout, beside the next window. The
window role's image `cfd20e71` no longer exists, though its stopped container names it, so it has no rollback. The legacy
stack restarted at 01:58 and uses a full core on the same Postgres server.

## Nov 3 portal recovery and staged Options resume outcome

Migration `047_advanced_journal_plan_authority` is applied live with no pending migrations. It removes the
single-table journal-plan FK; registry writes now require the plan to exist in either `advanced_ingress_plans`
or `service_plans`. The bridge image is `e4ab5e9dcf93` (base `d3198c1b`) and changes only
`advanced_journal_registry.py`: Equity replay discovers `STOCKS` streams only. It is healthy with zero restarts.

The falsely quarantined Options journal `d629048a-2187-5bab-abfd-a12787b2df1a` was repaired under exact
plan/stream/cohort/manifest/reason/detail/offset predicates from `QUARANTINED` to `DRAINING`; the quarantine
reason, detail and timestamp remain stored. The stream is now truthfully `STOPPED`, ended at the original
`2026-11-03T15:53:01.329202Z`, with watermark `15:51:50.577Z`; raw/event offsets are `107257/170196`.
MARKET_STATE is checkpointed at `170196`; all nine FLOW shards are checkpointed at `107257`.

The live Options release count and pointer remain zero. Failed 17:35 and expired 17:50 windows were not cleared,
backdated or published. A staged resume exposed old ingress journal code; ingress alone was rebuilt as `075cf391`
from `b9c2e021` plus the exact `advanced_journal_registry.py` already deployed on the healthy bridge. The 20:35
target now has durable COMPLETE INGRESS/MARKET_STATE/FLOW receipts at exact offsets `170196/170196/107257`.
The target deadline elapsed during diagnosis; WINDOW started after the deadline and correctly stayed
`READY_WAITING_FOR_SESSION`, with no stage attempt, release or pin through the 21:05 decision stop. It was not
forced, cleared or backdated. All five processing roles are stopped again. Acquisition `25336afa` remains
unhealthy but its durable status is COMPLETE; lifecycle `b61d80cc` remains healthy.

The portal fix was scoped onto the running images: scheduler `7921bc32` from `35808528` plus only
`ticker_projection.py`, API `a1f37347` from `41f819b8` plus only `ticker_repository.py`, and frontend `cad91f69`
from `060adb55`. The frontend now retries active errored reads and uses current retained ticker publications unless
an explicit historical session is selected; session-close mode is no longer forced for current ticker routes.
API and frontend are healthy with zero restarts.
The scheduler was recreated only after a 20:22:31 UTC read proved zero RUNNING windows/stages. Its old durable
lease caused eight fenced startup attempts before epoch 26 acquired normally; it is stable on the new image, while
the known health probe still reports unhealthy because it contends for the scheduler lease.

The first post-rollout 20:25 Equity window is COMPLETE: INPUT_GATE, MATERIALIZE, PROJECTION and PORTAL_PUBLISH
all completed with fresh receipts. SPY 1m/5m/15m/30m/1h/1d/1wk/1mo APIs return 200. Fresh-browser acceptance
proved visible retained charts for 5m, 15m and 30m with stale banners and the correct nearest/pinned analysis.
The real page-call matrix is 12/15: canonical replay remains unimplemented, independent daily Strategies has no
release, and the old diagnostic's session-close chart call omits its required snapshot. No non-target container
changed. Do not retry the old 17:35/17:50 windows or force the expired 20:35 target. Resume publication only on a
new session/cutoff: keep WINDOW stopped, start ingress/market-state/flow and scheduler, prove a fresh unexpired
three-member barrier, then start WINDOW and verify release plus current pointer.

## Live state after the Nov 2 recovery - approved by the user 2026-11-03

Live recovery ran 10:00-10:29 UTC: journals +117/+514 bars, gap repair 9 requests, 15 windows, rollover 36
requests, close attempt 5 COMPLETE. Windows 78/78; all 9 members exact on 1m/5m/15m/30m/1h/raw 1d/adjusted 1d;
real HISTORY stage READY for QQQ and SPY; Treasury Nov 2 VERIFIED (fetched automatically at 21:00:01).
Deployed by `scoped_rollout.py`, other 16 containers unchanged, rollback images in `scoped-rollout-plan.json`:
bridge `d3198c1b` (was `226c8e1c`), scheduler hotfix `ef5fdfb9` (was `61b942e6`), API `41f819b8` (source
`36145771`, was `65d36a00`), frontend `060adb55` (source `49af6ddc`, was `1f319b3f`).

API source fixes in that image: empty `known_by` crash on ticker reads; Pattern Watch session fields; frozen
closing release no longer overwritten by the hot one (All Tickers/Pattern Watch `session_close` reads); positional
cursor for `/realtime/equity-preparation`; `action_evidence_until` ends the action-drift period at the session's
close for current reads, so a nightly refresh no longer takes the portal down. This is a deliberate behaviour change.
Routes through the frontend: 131/153. Remaining: 13 absent paths, 8 needing parameters, `/strategies/daily` with
no release. Browser acceptance is the user's check. `runner/ui_calls.py` holds the calls real pages make;
`preopen_smoke.py` checks stored live state; `api_smoke.py` alone misses `read_mode=session_close`.

Still not deployed, for after the Nov 3 close: controller, ingress, preparation (rollover), lifecycle, full scheduler,
sleep gate. Without preparation's rollover, Nov 4 Options blocks again. Every image except the bridge and API still caps the
history binding chain at 4 scopes (source: 64). The chain gains one per session, so Nov 3 has 3, Nov 4 has 4, and Nov 5
would exceed the cap, which makes this rollout due before the Nov 5 open at the latest. The Options roles also lack the
15-session native context in `advanced_options_equity_input.py`, so OS1/OS2 context may be incomplete today; not verified.

Prior-session stream-derived 5m: the deployed scheduler `ef5fdfb9` fails every Nov 3 window, from the first one, with
`Ticker 5m history source drift`, because Nov 2's 5m are all DERIVED and the check allowed DERIVED for the current
session only. Reproduced read-only by `runner/repro_nov3_window.py`, which runs the projection on real history with
in-memory Nov 3 bars and the binding activation will write. The source fix accepts Advanced DERIVED 5m from any retained
session and verifies each against its 1m parents, using the source closure for sessions older than the 1m chart window.
Image `35808528` (source `1c73c27d`) is built and passes the simulation for 9 tickers at 14:35 and 21:00, including summary,
Pattern Watch and dashboard. Not deployed until the user approves: `scheduler_hotfix_switch.py plan|start|verify --to drift`;
`--to previous` is the rollback. Publishing and real Nov 3 bars stay unproven until the first window.
`test_ticker_projection_state.py` has 2 failures with or without the fix: hard-coded materialization counts of 24 and 22
against 20 and 18.

Options has never activated on live: 2 underlyings (QQQ, SPY), `option_flow_policies` 0 rows until first activation.

Options pre-open validation, 2026-11-03, approved by the user. One websocket handshake at 11:43 UTC: auth accepted, T and
Q subscriptions accepted for one contract, no market events, no retry. `REALTIME_V2_ADVANCED_OPTIONS_READS=ENABLED` since
11:45 UTC on the same API image, recreated by `options_reads_switch.py` (`plan`/`start`/`verify --value`; `--value DISABLED`
is the rollback). `scoped_rollout.py` does not carry the flag, so an API rollout through it turns reads off. Pages read a
published release, never stored T/Q, and show nothing until the first Options window publishes.
`runner/options_selection_dryrun.py` runs the deployed selection on the stored real chain with no writes or provider calls;
`runner/options_open_check.py` tracks capture through release, for 14:32-14:45 UTC. QQQ has one capture round left (64/96
slots), and activation is revision-wide, so a failed QQQ capture leaves Options unactivated for the session. Host
`node.exe` crashes on any script (`0xC0000096`), and 4 of 8 tests in `optionsFlowRealtime`, `optionsScreenerRealtime` and
`optionsAlertsRealtime` cannot load `api.ts` since it imports `./portalIdentity`, so page requests are unverified.

Selection works but favours expensive in-the-money contracts: shortlist and group ranks sort by relative spread first and
use delta only as a late tie-break. On the stored Nov 2 chain, SPY's 8 picks traded 4,755 contracts against 533,230 for its
busiest 8, and 3 of the 16 picks were 3 DTE, which the Flow policy excludes. Expect sparse Flow and Activity. Fix after the
close; volume-first ranking at the open needs prior-session volume, because the chain then holds about 90 seconds of it.

Product gaps the portal correctly reports, not portal bugs. Stock Alerts cannot fire: all 558 Nov 2 evaluations are
UNAVAILABLE, because `canonical_stock_alert_product.py` hard-codes event context to None and has no sector context, and
the detector computes nothing when context is missing. The earlier "0 matches, truthful no-signal" note was wrong;
no signal was evaluated. Batches also stop at 20:00, since recovered windows do not run alerts. Strategies has no
continuous producer: the scheduler never passes `--include-strategies`, which needs a BOUND history report (283
adjusted, 90 raw sessions); the newest release is Oct 27 in an older cohort. So Strategies Overview and Stocks
Persistence stay 503. Both need a user decision and are not planned before the Nov 3 open.

## Nov 2 recovery and Nov 3 readiness - history of the approval

Everything below predates the live run. Evidence and scripts: `backend/.realtime-canary/v3-supervised-20261102/recovery-20261103/`
(`runner/`, `scoped_rollout.py`, `rehearsal/`). Run recovery in containers only: host Python crashed twice
(`0xc0000005`). Backup: `backend/backups/stocks_db_backup_v3-pre-nov2-recovery-20261103_2026-11-02.dump`,
SHA256 `FCAD888E0AAB8C38EB4B36A0809602086640FDFBDB04EDC71CB6AE1C4E0ED3B6`.

Nov 3 Options: `CATALOG_QUOTE` and `REFERENCE_OI` are COMPLETE; `HISTORY` retries with no provider calls
until Nov 2 adjusted and raw daily bars exist. Dividends also need the rollover. QQQ has 64/96 budget slots
used (one capture round left), SPY 32/96. Do not reset them.

New defect, found by rehearsal: a closing window could never complete. Derivation read 5m bars at the stage's
start clock, so the last 15m/30m/1h and raw 1d bars were never derived, and a retry reuses the first
INPUT_READY `known_by`, which predates the daily bars. Fixed in `stage_runners_equity.py`
(`CLOSING_DAILY_INPUTS_PENDING`). Deployed code still has it, so without the hotfix today's close would fail too.

Images, pinned; do not rebuild, because a rebuild changes the ids: recovery `sha256:d3198c1b...` (source `ad1b8476`)
for journals, gap, rollover and bridge. Hotfix `sha256:ef5fdfb9...` (source `74a4a884`) is deployed scheduler
`61b942e6` plus `stage_runners_equity.py` and `ticker_projection.py`, for all windows and the scheduler.

Final rehearsal, finished 09:56 UTC on the disposable copy with these images (fake provider, no live effect):
- PASS: closing window attempt 4 BLOCKED, attempt 5 COMPLETE in 77s; pointer at 21:00. Real HISTORY stage READY for
  QQQ and SPY on lifecycle image `d9dea05d`; dividends valid; all 9 members have both daily bases.
- Windows 76/78: 20:10 and 20:25 timed out in publish and every retry hit `CYCLE_REHYDRATION_MISMATCH`, so they stay
  incomplete. Later windows, the close and Options were unaffected. Cause of the mismatch is not diagnosed; it has
  followed a publish failure each time it has been seen. A timeout in projection recovers on retry.
- Timeouts come from CPU speed swings, not the hotfix: identical work took 57-228 CPU-seconds on the deployed image
  and 34-153 on the hotfix, and a fixed loop took 3-20s. Windows reports a 76% performance limit on the Balanced
  plan, with no thermal throttling. Live windows share the 300s limit, so this also threatens today's session.
- Deployed API: 22/153 routes 200 (baseline 21). 10 routes newly 200 (`/stock/*/sessions`, `/trading-sessions`).
  9 routes stay 503 `action evidence changed; republish required`, and a completed close does not clear them: hot
  releases set `generated_at` to the market cutoff, so any coverage row received later trips the reader until a
  newer release publishes. Expect them back at the first Nov 3 publish, about 14:35-14:40 UTC; not proven. After
  the rollover deploys, this would repeat every night unless the reader changes.
- The working-tree API still returns 503 on 112 routes (`AttributeError` on 100); do not deploy it.

Second rehearsal, finished 06:14 UTC on the disposable copy (fake provider, no live effect):
- PASS: journals, gap repair, all 15 windows, rollover. All 9 members have both daily bases. SPY raw bars are
  15m 26, 30m 13, 1h 7, 5m 78, 1m 390, 1d 1. The real HISTORY stage from lifecycle image `d9dea05d` returned READY
  for QQQ and SPY at a simulated 14:32 UTC, and dividends read `KNOWN_NO_ANNOUNCED_DIVIDEND`. So Options is unblocked.
- FAIL: the 21:00 window is still not COMPLETE (77/78). Attempt 4 BLOCKED as designed and derived the bars, but
  attempt 5 failed in projection with `Ticker 5m history source drift`. `ticker_projection.py:898-909` allows DERIVED
  own-session 5m bars only while `partial_session`, so at the close it demands native bars nothing writes.
- REGRESSION: on the deployed API, 200 routes fell from 21 to 13 of 153 and 9 baseline-200 routes went 503 with
  `action evidence changed; republish required` (`/tickers/overview`, `/scan/all`, `/stocks/alert-view` and others;
  `/trading-sessions` became 200). Readers reject a release once corporate-action rows arrive after it was generated,
  and the rollover's dividend refresh does that until a newer release publishes. A completed closing window after
  the rollover should clear it, and otherwise the first Nov 3 window should. Neither is proven.
- Do not deploy the working-tree API yet: on this copy the recovery image returned 503 `AttributeError` on 100 routes
  (ticker pages) and `KeyError` on 2. Not diagnosed.
- The one unresolved gap is the stream-end marker at 21:00, which the input gate passes. It is not missing data.

The source now validates legitimate DERIVED session 5m at the close; see the source-only checkpoint below.
The hotfix image contains it, `scoped_rollout.py` is pinned to these images, and the final rehearsal above used them.
No live step, deployment or changed live order is authorized by any of this; live still needs explicit approval.

Live order after approval: copy journals afresh and verify hashes; replay both journals (no provider calls);
gap repair 16:35 (9 requests); 15 windows on the hotfix; rollover (36 requests); closing window on the hotfix
(attempt 4 BLOCKED and derives raw daily, which Options needs; attempt 5 COMPLETE); scoped rollout of bridge and
scheduler (re-run `scoped_rollout.py prepare` first; it refuses at or after 14:15 UTC); verify with the `check`
step on lifecycle image `d9dea05d` and compare the API with `baseline/`
(21/153 routes 200 before recovery). If not clean by about 13:00 UTC, deploy only the bridge and scheduler fixes;
Options then stays blocked for Nov 3. Drop `test_realtime_v2_rehearsal_20261103` when finished.

Do not redeploy lifecycle before the Nov 3 close: K=12 changes the budget scope hash and would act like a budget
bypass. Deploy controller, ingress, preparation, lifecycle, the full scheduler and the sleep gate after that close.
Follow-ups: history binding depth 64 needs compaction within about 3 months; `test_equity_interval_derivation_worker.py`
has a pre-existing collection error; the wider regression has not run.

## H1 source-only fixes and remaining automatic readiness

The completed 45-stage/nine-member bootstrap is preserved; do not repeat it or restore the archive wholesale.
The user selected existing PostgreSQL leases/request-ledger ownership rather than a new C4 migration.

- [Native Options context](../../backend/realtime/advanced_options_equity_input.py) now resolves exact
  15-session XNYS raw daily history through bounded bindings, retaining each actual source cohort/bar/hash.
  Incomplete stock-first requirement declarations block their lanes instead of entering ATR with empty inputs.
- [Recurring rollover](../../backend/realtime/advanced_equity_rollover.py) uses a durable revision lease,
  owner/epoch and current-desired-scope fences before I/O and at completion. Every real HTTP request checks
  lease/open-time headroom and live priority. Facts and ledger outcomes commit atomically; ledger completion
  clocks drive restart-safe backoff. Expired reservations are recovered only with matching prior-owner hashes.
  Unknown/legacy unfenced reservations stay BLOCKED for controlled reconciliation; never reset them.
  Quiesce old unfenced rollover callers before deploying this protocol. Lease duration is 10-3600 seconds.
  Manual reports expose BUSY/backoff and only return success with an empty verified remaining work plan.
- [Closing projection](../../backend/realtime/ticker_projection.py) accepts current Advanced derived 5m
  bars at close only after checking exact native minute parents and aggregates. Missing unrelated older
  minute-chart history does not invalidate those current 5m bars.
- Disposable DB checks passed for overlap, completed-work restart, persistent backoff, expired-owner recovery,
  unproven reservations, stale completion, complete mixed-source native assembly/frozen replay, bootstrap/HV
  identity preservation, reference production and Equity input binding. In distinct focused runs, 125 offline
  and 12 disposable DB tests passed, including existing hardened app-role rollover permissions and
  normal/SESSION_CLOSE publication with the real closing projection.
  The full recovery rehearsal remains unrepeated. No live/provider effects, new schema/grants or budget resets.

Broader H1 is still deferred, not completed by these fixes:

1. Extract ordinary verified adoption from the bootstrap-only adapter. Normal C4 residents still disable it;
   never enable its unconditional whole-weekly revalidation for routine onboarding. Preserve native/derived,
   adjustment, causal source and closure checks, and commit reuse through ordinary C4 chunks.
2. Derive a per-product history inventory from enabled detector lookbacks plus portal/Options requirements;
   the current initial targets use a portal plan with fixed 210-daily/200-hourly warmup minima.
3. Rollover now plans absent, split-stale and invalid-coordinate adjusted history over the existing portal
   targets, using the last completed session as the adjustment horizon even for weekly/monthly bars.
   General policy-driven lookbacks beyond these existing targets remain future work.
4. Qualify retained stream-derived 5m across the next session's bindings and minute-parent retention. The close
   fix accepts current-session derived 5m only; prior-session 5m still has the native-source contract. Define
   supported retained derivation evidence explicitly, without relabeling bars or refetching merely for a label.
5. Replace coverage-only technical assurance with durable product/checkpoint evidence bound to exact bar IDs,
   parameter/code versions and through-clocks. Recompute only dependent tails from the earliest changed input
   through existing materialization/evidence owners, then validate real screening/HV/OS/chart/publication reads.
6. Qualify interrupted adoption/repair and rollover with zero repeat spend, unchanged earlier checkpoints,
   bounded requests/bytes/DB work, live-priority deferral and source/adjustment divergence tests. Compact bounded
   history chains before the existing depth-64 limit. These are implementation acceptance, not a daily manual gate.

Refresh only due catalog/OI/dividend/action context; missing OI and deferred sector dependencies stay UNAVAILABLE.
Stored Nov 2 Treasury was separately VERIFIED for Nov 3 (14 tenors, zero fetches); reuse it, not Oct 30 evidence.

## Residual-finding fixes - source only, Nov 3

- Screening's shared depth-64/identity checks were left unchanged.
- Rollover repairs only the first contiguous invalid range, bounded by the configured fact ceiling. It
  supplies that exact range to the existing preparation adapter, including ranges larger than its ordinary
  20-period default. References run before historical work. Lease fencing, request/byte limits and the
  existing per-action attempt/budget scope remain unchanged; exhaustion is still BLOCKED, never reset.
- The user selected a resident overnight Equity scheduler. It is excluded from session-bound sleep roles;
  the open-minus-25-minute recovery stop and other workers' wake windows were not relaxed.
- Legacy Options calls no longer require runtime publication identity. Realtime Options still requires its
  own cohort and never borrows the Equity cohort.
- Retained context reports SMA50 explicitly; EMA50 remains unavailable. Market Conditions labels and
  distances follow the declared basis, while legacy EMA calculations stay unchanged.
- Focused validation: 41 offline backend tests, 3 frontend regressions and 4 disposable DB checks passed.
  The DB repair uses the real preparation source, fixes stale history beyond 20 sessions, preserves valid
  bar IDs and proves both requests retain one budget scope. TypeScript completed cleanly in cached
  `node:20-alpine` with no network/read-only source; host Node again exited -1073741674.
- Recovery-runner editing/rehearsal belongs to the other session by explicit user choice after concurrent
  runner changes were detected. Its fake-provider fence hook was preserved; this session left no runner edits.
  Do not claim runner approval gates or a full recovery rehearsal passed based on these source checks.

Candidate backend/frontend images must include these latest edits before rehearsal; older pins may predate
them. No live repair, provider fetch, deployment, grant/migration, budget reset or bootstrap rerun occurred here.

## Authority and safety

Target `stocks_realtime_v2` only. Supervised approval V3-FIRST-SUPERVISED-20261102
through Nov3 21:15 UTC. Current live pass remains READ-ONLY; the source remediation is not a rollout.
No unscoped Compose up, live-role recreation, deliberate outage, active restore,
legacy runtime, trading/delivery, budget reset or manual READY/watermark edits.
Preserve dirty source, immutable facts/bindings, original clocks and quarantines.
Do not rerun completed bootstrap or infer authority for a bridge fix.
Evidence: [readiness section12, Independent live monitoring](../ADVANCED_REALTIME_V3_FIRST_RUN_READINESS_2026-11-02.md).

## Current observed deployment

- Another session deployed API, scheduler, canonical-stock-alerts and Options
  WINDOW on `alpha-research-realtime-v3@sha256:9ce840961a554612e055cda9d737d2b8a0bbabf508b1c2804454ff4657cd62e9`.
  Frontend `sha256:1f319b3fa2b7cf9f2dae495895806f85c54f6fbfc9ab4143c774cb67c452d04d`.
  Scoped recreation17:36-17:37; deployment evidence:
  `backend/.realtime-canary/v3-supervised-20261102/portal-publication-deploy/`.
- Other15 containers were unchanged by that rollout. Bridge/ingress still226c8e1c;
  Options lifecycle stilld9dea05d. No monitored container restarts17:55-17:59.
- Publication uses continuous-publication and C7 batch publication. No migration
  or new grants; Options reads remain DISABLED. Current identity resolves cohort
  `52b9fef3-beeb-5181-8fe8-e28fb6f2de48`; no build-time cohort required.
- 45/45 C4 prep COMPLETE14:04:13, nine Equity READY before14:15. Schema/security
  verified; 045/046 only, original backups preserved. No new bootstrap/adoption.
  October30 Treasury cachedVERIFIED14tenors, zero monitoring provider fetches.
- Member160requests/2GiB, account64k/512GiB, Options40M/8GiB/12attempts unchanged.

## Five-sample live observations, 17:55-17:59 UTC

1. Real PORTAL_PUBLISH writes completed:17:35retry,17:40,17:45,17:50,17:55.
   Four normal durations187/235/184/166s; all within their300s release budgets.
   Minimum observed deadline headroom~59s. Overrun recovery not demonstrated.
2.42 releases,37 distinct cutoffs; five dashboard/pattern/summary snapshots each,
   45 individual ticker snapshots,37 Stock Alerts snapshots. Pointer17:55 CURRENT.
   37 alert batches,0compute errors,0matches (truthful no-signal).
3.**Serving still FAILS**, including explicit-cohort controls18:01:50:
   workspace summary422 `identity/checksum drift`; dashboard503
   `identity/checksum/clock drift`; Stock Alerts503 `identity/checksum drift`.
   Frontend proxy also422. Correct cohort header proves this is not just routing.
   Do not treat successful writes/current pointer as successful product reads.
4. Same LIVE Stocks sessionf3c9060f, epoch36, remained active; bars/journal advanced.
   Options192-slot/150MB/one-scope ledger unchanged, no new REST reservations;
   API returns enabled=false/ADVANCED_OPTIONS_V2_DISABLED, read pins0 as expected.
5.19 ticker-test failures/baseline are being investigated elsewhere; this monitor
   did not rerun them or assert that they were pre-existing.

## Serving-integrity source remediation

Read-only predicate audit of current releasebaf814b8 at18:10 proved stored
manifest/payload checksums valid. Aggregate source identity589a46c5 exactly matches
the resolved two-scope history chain, but Dashboard/summary/Stock Alerts manifests
omit `history_scope_chain`; readers recompute obsolete single-binding153932b4.
Immutable snapshots/pointers/clocks were not changed. The fix centralizes
bounded history/reference continuity fields across aggregate producers and makes
Stock Alert/strategy readers use the shared exact resolver. Final focused offline
acceptance:10 passed. Scoped rollout changed only API/scheduler/canonical-alerts:
images65d36a00/61b942e6/a1559bbf. Non-target IDs, LIVE Stocks owner epoch36,
Options192/150MB/one-scope ledger and immutable rows stayed unchanged. New18:40
Dashboard/summary/pattern/alert manifests each carry chain length2 and resolve
exactly. Six implicit/explicit controls now return200.

## Confirmed historical gap and remaining live risk

Old journalb0c80d14 DRAINED16:23:02 while its stream481bccd6 ended16:36:17
with CONNECTIONCLOSEDERROR; replacement began16:36:21. Missing ALL9 ticker bars
at bar_start16:22-16:35 inclusive (126 coordinates); adjacent minutes have9.
16:40 all4 attempts correctly blockedADVANCED_INPUT_NOT_READY; not timeouts.
Deployed bridge226c8e1c AND portal image9ce84096 have identical heartbeat-only
orphan-drain logic, no connection-owner lease check. The AM/60s race remains live.
Working-tree changes are not evidence of deployment. No repair/waiver was made.
Any bridge update/recreation requires a separate controlled scope decision.

## Remaining Equity portal source remediation

Source-only remediation now keeps lag separate from corruption. Per-ticker identity
resolution carries the stored `history_scope_chain`; ticker session lists use the
immutable quote trade date/clock and include the current retained session without
mislabeling it as a frozen close. Screening has explicit `current` and `retained`
controls: current still fails on live-head drift, while retained verifies stored
hashes and serves the saved generation with stale metadata. Market Conditions,
Sector Intelligence and Market Regime routes derive truthful degraded views from
the validated retained workspace summary; they do not call providers or legacy
runtime. Retained strategy reads may select an earlier checksum-valid released
entry, and the UI falls back to an exact pinned Dashboard only for an explicit503
missing strategy release, never for identity/shape errors.

The session selector retains prior data across refresh failures. The shared banner
now covers latest ticker, cross-ticker, screening, strategy and enabled Options
surfaces and reports `STALE DATA`, sessions behind, oldest published timestamp,
affected timeframes and retained setup age. Explicit historical selections are not
misreported as live lag. Options reads remain disabled today; enabled-mode status
uses the retained Options release and never an Equity cohort.

Focused offline evidence: the four retained-reader files first ran461pass/3fail;
all three local failures were corrected, then affected screening/ticker selectors
passed. The final combined remediation/identity selection passed33/33. VS Code
diagnostics report no Python/TypeScript errors. The host Node test/build processes and Playwright MCP
both terminate before diagnostics with Windows exit-1073741674, so browser rendering
could not be certified.

No rollout was authorized or performed. The19:51 read-only deployed matrix therefore
still reflects the old images: eight direct-entry HTML routes return200, but ticker
current/retained charts return422 old identity drift, ticker sessions503 clock
mismatch, screening current/retained404 lineage drift, Market/Sector/status routes
404, and strategies503 absent release. Cross-ticker current/retained remain200;
Options disabled status remains200. This is baseline evidence, not a result of the
source changes. Full serving/browser acceptance remains NO until a separately
authorized scoped rollout and post-rollout direct-entry/network/banner matrix.

Existing malformed old manifests remain fail-closed; no snapshot, clock, pointer,
service, container, database, grant, provider data or live worker was changed.
Current monitoring evidence: observing session files/portal-monitor-20261102T1744/,
portal-serving-evidence.json,1640-gap-evidence.json,deployed-drain-source-metadata.json.
No services, code or budgets changed by monitoring. Read-only helpers completed.
Original backups/quarantines retained; volume restore files identical but historical
DB recovery INVALID (3 boundary/2 codec rows). Member reference refresh fuseNov9;
calendars/dividend rollover/daily catch-up and next-session continuity unvalidated.
Close21:00UTC/13:00Pacific, final21:05:05/13:05:05; next XNYS sessionNov3.
Options real publication, full browser/UI, close/drain and accepted recovery remain
UNVALIDATED. No recurring manual bootstrap or new daily admission gate.
