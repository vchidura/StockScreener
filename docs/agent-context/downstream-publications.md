# Downstream Publication Recovery

Updated: 2026-09-16 17:49:23 UTC, correction recovery and reload verified
State: COMPLETE; FIRST CORRECTION-RECOVERED PUBLICATION NOT YET OBSERVED

## Objective And Authority

- Latest request approved prospective recovery of the reviewed historical bar
  corrections, preserving original evidence and past alerts/positions.
- Authorized only Stock Alerts maintenance/reload. No provider calls, global source
  writes, OKE remapping, historical retry/replay, deadline or warmup relaxation.
- Owners: [worker](../../backend/scripts/run_stock_idea_worker.py),
  [forward state](../../backend/research/stock_idea_forward.py),
  [read-only verifier](../../backend/scripts/audit_stock_alert_session.py).
- Runtime authority: [README](../../README.md#resident-worker-checklist).
  PIDs below are dated evidence, never reusable stop/start permission.

## Current Correction Recovery

- Exact inventory213pairs/116securities rechecked17:39Z:212volume-only,1open+volume,
  no identity/clock changes. InventorySHA256
  cb07f0d2fedec9b9024591f0ab21a0836c065d405079f9d7fe2bc5d45d59e981.
- --recover-corrections requires that hash, exclusive alert advisory lock, exact
  retained/current canonical revisions, stable security/ticker/bar clock, valid
  observed/created inputs within7days. Ambiguous multirevision slots are refused.
- Applied17:42:31.092933Z, effective18:00Z (14:00ET). Overlay generation
  08157bcee88bdb230d056af0a0fad0d89dae6ab95ad253299aa452098fd3da83.
- V2 state backup/report: correction-backups/
  086114bfdb97780059a7cb4eda865556a82a1eb83c9c6cf830c0574fefac262b.
  BackupSHA256 96bd8ac85accbab19ecafc74961787cbad7adf141b61ec1ca6f0293db822e4b0.
- Original bars/corrections/identity flags stay intact; corrected revisions are a
  hash-checked future detector overlay. Only affected pending setups/packets reset.
  New publications/positions carry recovery generation; old positions keep original
  correction holds. Any new unreviewed revision or identity change reblocks; identity
  changes take precedence even in mixed batches. No automatic broad acceptance.
- Independent CORRECTION_PRESERVATION_VERIFIED before restart: all20publications,
  15positions,outbox,manifest,originalbars,enrollment,OKE quarantine and protected
  checkpoint fields unchanged. Verifier must run BEFORE resumed worker writes.
- Retained-input prospective model-ready375/38530m,374/385hourly; indicator-only
  383/381 is NOT full eligibility. Remaining30m: CBRS,MRSH,SPCX,APH,SUNB,P,FISV,
  VMRK,HONA,ARKW.200bar warmup unchanged. Future publication was not simulated.
- Stopped verified12028/12244 at17:42:16Z; all20 other worker/API processes remained.
  Restarted only standalone StockAlerts (ordinary quality2, no maintenance flags).
  New observed ancestry24672 ->23796 ->2352. Runtime hashes match current files;
  saved-view17:45:08.366582Z confirms restored cache and loaded recovery generation.
  Browser/API HTTP200 at17:49:23Z; old17:00Z empty run remains unchanged.
- 187focused tests pass: causal exact revisions, wrong hash, identity precedence,
  future frame readiness/selection, old-position isolation, preservation/idempotence,
  reblocking and audit labels. Diagnostics clear; temporary PID-stop task removed.
- NEXT: observe18:00Z publication after18:15Z source delay,
  hard deadline18:29:55Z. No guarantee of a confirmed setup. Do not bypass guards to
  populate the page; current/past empty records remain authoritative.

## Earlier Verified Milestones

- OKE oldID c11cb5a6-797e-5214-894a-278b6fa75ec9 changed to
  1453cded-aa60-51c4-8139-43a8cd205237 in provider reference. Independent continuity
  remains unresolved. Alert-only exclusion activated15:53:58Z/effective16:00Z;
  original enrollment386 retained, selection385; other page universes unchanged.
  Cohort/backup generation f2401949dec1d733c21525989fbae56783092a5dfdf7142120580c7e776f549f
  under quarantine-backups in V2. Original source was timely; identity gate caused
  four morning MISSED records, not absence of386ticker bars.
- Missing-history recovery16:43Z restored2310bars (sixSeptember15 afternoon slots
  x385) from existing source storage, effective17:00Z. Original one-hour overlap
  missed overnight late arrivals; reader now revisits7days/800bar cap. Missing older
  slots reset detectors prospectively, never replay past lifecycle/alert decisions.
  History backup/report directory8772be46431eb6ef89625013dcea787f5a290dcac6dd8a259d52aa29ae42b9bc;
  backupSHA256 a3810074337cbf8f0199dadafb6d601847c826cc959daaf9f9b4f93a1e8634aa.
- First history-recovered17:00Z run PUBLISHED17:21:28Z:262/385ready,123missing,
  zero candidates/alerts,4current30mWATCH states.123=116correction-review+7other
  prerequisites. Wider reads exposed real existing-bar corrections; this prompted
  the separately approved overlay above. Publication != full model eligibility.
- Active browser d19082e8 (today Latest) and API both showed0alerts at17:24Z;
  no hidden rows. Day History excludes global newest run until another publication.
  Missed Published timestamp can show deadline instead of actual record time.
- Related contracts: [quality/context checkpoint](stock-alert-review.md),
  [downstream verifier](../../backend/scripts/verify_downstream_updates.py).
  Historical September14 closing recovery/61tests passed before that day's shutdown;
  those shutdown notes do not describe current runtime or authorize other restarts.