# Stock Alert Quality Review

Updated: 2026-09-15, authorized quality-V2 shadow cutover
State: COMPLETE (enrolled and waiting for first market-session publication)

## Objective And Authority
- Latest user request authorized V2 workers and UI reading the V2 store, with all
  backup files retained locally and outside Git. Normal equity-side startup approved;
  no options pipeline, new study, paid acquisition, historical repair or context gate.
- Earlier shutdown/pre-cutover findings are dated evidence. The September15
  [operational check](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#september-15-operational-check)
  at08:48UTC was superseded by the explicitly authorized cutover below.

## Current V2 Cutover
- Launcher explicitly uses --quality-version2; SHADOW reader default changed to
  stock-ideas-forward-v2/alerts-view.json. No env override present. MissingV2doesnot
  fallbackV1. Full4source/test originals archived/hashverified55,900bytes before edits.
- Enrollment08:54:46.169672UTC,386members,warmupcomplete08:58:16UTC,noidentitybreaks.
  API200 source_idstock_ideas_forward_quality_v2,statusWAITING_FOR_PUBLICATION,total0.
  NextboundarySep15T14:00Z;window14:15..14:29:55Z=10:15..10:29:55ET.
  UI LatestRun displays that window; no old alerts/backdated publications created.
- Started5resident workers; existingAPI/Vite untouched. Corporate firstrefresh
  completed; equity resumed retained intervals; Screening ALREADY_PUBLISHED.
  No Windows service/autostart installed; do not infer future uptime from startup.
- Terminals (recheck exact process state before reuse/stop):
  StockAlerts df991133-510e-4e2d-b16d-805eeaa6d960;
  Equity fcd2e499-eb55-4f66-972e-b080be68ad80;
  CorporateActions 55706915-b017-4199-b8d5-81b523e2dd7b;
  Screening d30a74d2-0b49-4811-8e72-cba34400ebdd;
  Portal 25a0926e-338d-43c6-865f-2a40b0f2c96c.
- V1view/SQLite byte hashes unchanged before/after; receipt in local ignored
  backend/backups/v2-cutover-v1-preservation-2026-09-15.json.14backup paths had
  reappeared in Git index; approved cached-only removal reapplied,all14diskhashes
  unchanged,0tracked. No data deletion/commit. Runtime data must be restored or
  regenerated on another machine,not ignored portable configs.
- Cutovertests117PASS,archiveVERIFY,diffcheckPASS,liveAPI/UIverificationPASS.
  First in-session cycle/selection/fill stillNOTOBSERVED;zeroideasvalid,notfailure.

## Evidence
- [Readable review](../STOCK_ALERT_REVIEW_2026-09-14.md) includes all 42 alert rows.
- [Final evidence](../stock_alert_session_review_2026-09-14_final.json) contains
  per-plan geometry, entry gates, same-source outcome checks and focus candles.
- [Utility](../../backend/scripts/audit_stock_alert_session.py) reads isolated
  SQLite via mode=ro/query_only/BEGIN, never instantiates the writer store.
- Retained cutoff21:49:57Z;12publications,42plans/40tickers,18x30m/24x1h,
  15resumption/9acceptance/18failure,26long/16short. Enrollment10:50ET,notfullopen.
- OBSERVED in original review: all42 original-price entry/availability/expiry gates
  and reconstructed model geometry checks pass; then-current hashes matched. No duplicate
  episode IDs or retained-vs-recomputed paper outcome differences. Input/checkpoint
  hashes unchanged before/after. No independent vendor/quote/action certification.

## Findings
- OBSERVED: HWM/LQD/VEA already fail entry_gate at newer30m close available BEFORE
  publication input cutoff, but dispatch passes stale candidate.price to selection.
  HWM226.665->228.11 breaks227.57short boundary;LQD104.41->104.545 R/R.73;
  VEA71.985->72.12 R/R.62. Recommendation: separate decision-price gate beforequota.
- OBSERVED: RCL/MA published15:58:46 with no later30m entry open beforeclose;
  bothNO_FILL/SESSION_CLOSED_BEFORE_ENTRY. Gate actionability beforepublication.
- OBSERVED: acceptance next-barconfirmation only requiresclosebeyondboundary;
  no favorablebody/closinglocation/volume/trend gate.6of9 confirmationsmoveagainst
  direction. Range-widthtarget vs localepisodestop makesretestsmallriskhighR/R.
- SPGI417.50/stop416.2607/target425.32=>6.31x,stop.297%/.412activationATR.
  Known12:00price418.04 atpublication=>4.09x;12:30entry418.67=>2.76x;
  stop14:00barboundary,net-.6755%. Downconfirmationatlow,belowhourly/dailyEMA50.
- RMD221.355/stop220.7376/target226.61=>8.51x,stop.279%/.386activationATR.
  Closeonly.065above221.29boundary,downconfirmation;12:00entry222.6106=>2.14x;
  timeexit14:00at223.09,net+.1154%.Neitherreachedtarget; ratiosnotprobabilities.
- 20no-fills (14room/3chase/1bracket/2noentry);22closed(10wins/12losses),
  18timeexits/4stops/0targets,meanenterednet-.0259%,median-.0915% after10bps.
  Per-model enteredmeansresumption+.303%/acceptance-.078%/failure-.230%.
  Single-day descriptive only; no portfolio or qualification/probability claim.

## Implemented Quality V2
- `forward_config(quality_version=2)` identifies stock_ideas_forward_quality_v2.
  Decision-time native30m price must match security/ticker/currentboundary,
  actual observed/created cutoff, providerlag, validOHLCV and positivevolume.
  `execution_times` rejects no remaining entry slot before quota. Suppressions
  retain reasons; acceptance/reversal priorities use decisionroom/risk+extension.
  Original candidate price/bracket/expiry restored in positions/publication;
  separate decision_prices/decision_price_evidence retains clock/revision/risk.
- `range_breakout_acceptance_intraday_v2` requires nextbarclose>=.15activationATR
  beyondboundary, favorablebody, improvementoverpreviousclose, directionalhalfof
  nonzerorange. Symmetricshort. Dailycontracts/resumption/reversalunchanged.
  v1 config defaults preserve the old detector for baseline fixtures.
- CLI defaults quality2 +separate stock-ideas-forward-v2 directory; --quality-version1
  selects olddirectory for inspection. Differentpolicy store/view refused;
  V2 is now separately enrolled and reader points toV2. No V1migration occurred.
- Existing v1 manifests contain older code hashes: do not alter them to resume
  changedcode. Source-aware baseline fixtures are not same-version runtime evidence.

## Validation And Next Step
- 147 focused forward/engine/replay/reader/launcher tests pass (14 newqualitytests).
  Actual incremental detector positive/negative controls, long/short symmetry,
  noentry/currentpricegates, beforequotaordering, immutableplan/store/view tested.
- [Retained-case verification](../stock_alert_quality_v2_regression_2026-09-14.json)
  created by audit_stock_alert_session.py --session2026-09-14 --verify-quality-fixes.
  All42examined; HWM/LQD/VEA rejectedbyknownprice, RCL/MA bynoentry;
  SPGI/RMD failconfirmation. No revisedwinners/returns computed, allcheckpoint,
  input andpublication hashesunchanged. This is NOTperformanceevaluation.
- [Working delivery plan](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md): detailed
  manifestcontract, Stage1read-onlyaudit, Stage2storedannotations/nochangedwinners,
  one model-specific continuation challenger, separateevent/optionsstudies,
  matchedoptionsstockcohort, calibratedprobabilityonlyafterprospectivevalidation.
- NEXT: implement Stage1read-only readiness manifest on enrolled386 + reviewed
  windows using current storeddata. Reaudit comparableIV (notrawmarkcounts),
  rates/dividends/quotes/flow/events, datedsectorcoverage and priorVIXreadiness.
  No manifestproducer or freshIVaudit has been implemented/executed yet.
  PriorIVreports are datedevidence, nottodayreadiness. Noacquisitionrequiredforaudit.
- NOT_CHECKED: independentprices, revisedperformance/multisessionsoak orfirstV2marketpublication.
  NewV2ledger/view created under authorizedcutover;V1recordsunchanged.
- Agreed post-commit order is in the design's Post-Commit Delivery Contract:
  finish baseline/readiness/parity, frozen comparison including exact xsmom,
  separate incremental context trials, prospective gate, then page migration.
  No xsmom retirement, extra acquisition or study execution. When using the existing
  end-of-day verify_downstream_updates.py, pass --state-dir pointing toV2 explicitly;
  its historical default state path isV1. Do not inspectV1checkpointagainstV2view.