# Stock Alert Quality Review

Updated: 2026-09-17 context progression deployed, shadow activated05:40:41Z
State: CONTEXT/SHADOW DIAGNOSTICS DEPLOYED; live gates disabled; sector adjustments unresolved

## Current Progression

- User approved reviewed progression; financial/moat/squeeze acquisition deferred.
  No live gate, quota/fill/outcome change or historical alert rewrite. No provider
  calls by manual audit/capture; existing market refresher FRED policy unchanged.
- [Canonical milestone](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#context-progression-milestone)
  has contracts, evidence, CLI, deployment and gate prerequisites; supersedes prior
  next-financial-integration notes below. Runtime observations are dated, not authority.
- Score/UI fixes preserve stale reason/dates; missing components not reweighted.
  Event timingv2:confirmed hold-window/upcoming-before-entry/recent30m/same-dayuncertain.
  New alert facts reuse cutoff-eligible archive(session/universe/security/reference),
  snapshot+lineage hashes; late snapshots rejected,laggedintradayvolumestale.
- [Proxy audit](../stock_sector_action_review_2026-09-17.json):four2-for-1splits2025-12-05,
  adjacent raw prices~half. Not independent adjustment certification; guards remain
  8/12proxies,245stocksectorready/105unavailable/36N/A. UI exposes exact terms/source.
- [New snapshot](../stock_rotation_2026-09-17.progression.json)1301facts/939lineagesverified,
  originalpublicationsunchanged,APIhash34e347b79c6682516aba6488b871679b814fd2d28416460236b354a036a71048.
- [Retrospective diagnostic](../stock_alert_context_progression_2026-09-17.json):3candidate
  occurrences/2unique/2selected,archives3/3,UNKNOWN3(FOMCestimated);originalpubhashunchanged.
  Includes expired candidate; no held-out/economic/latency qualification or quota refill.
- Standalone annotator enabled by -Worker AlertContext -ShadowEvents; separate
  shadowactivation2026-09-17T05:40:41.596108Z;oldannotationactivationunchanged. Stores all
  retained candidates(PUBLISHEDruns) in immutable event-shadow files,60s/onepub/current+prior
  XNYSsession. WOULD_BLOCK onlyconfirmedholdingevent;missing/uncertainUNKNOWN. No live gate.
- Reloaded only verified marketcontext27960/10324 using previous task command; shadow
  annotator started separately. Heartbeat05:40:41Zshadowenabled/livegatefalse/0pending.
  Inspection returned no StockAlerts process; NOT restarted by this task, recheck before
  prospective sample collection. API and existingbars/enrollment/OKEoverlay left alone.
- VALIDATED240backendtests,tsc/build,artifactverifier,APIanddesktop1440/mobile390browser
  pointer/keyboard/screenshots,nooverflow/clippedtext. Isolated fixture removed. Original
  CME/LYVsidecars keep oldfields;LYVhash571f6c77... explicitly unchanged.
- NEXT: uninterrupted prospective samples then disjoint model-specific evaluation;
  actual preselection availability/latency and explicit approval before any gate.
  FourETFsplit-adjustedbasis still pending approved evidence, never bypassrawguard.
  Shared-shelloutputunreliable:usedprocess tasks;temporaryPIDstoptaskremoved.

## Earlier Finnhub Probe

- User authorized only a secret-safe Finnhub access/usefulness probe. No DB writes,
  source switch, integration, worker changes or paid entitlement changes.
- scripts/probe_finnhub_financials.py:3requests total for AAPL/MSFT/AAL, quarterly
  endDate range2026-03-21..2026-09-17,1MB/response,no retries/pagination/redirects.
  Header-only local key; no key/errors/URLs or raw responses printed/saved. Console
  contains sanitized selected facts only. Runtime clock, not chat date, recorded.
- OBSERVED all3HTTP200 with matching ticker/CIK,one10-Q each,allthree statement
  sections present. AAPL end2026-03-28/filedMay1,MSFT endMar31/filedApr29,AAL
  endMar31/filedApr23. No later filing returned within this request window; cause
  unknown. Endpoint access is not current/full-history coverage or plan certification.
- Narrow exact-tag matches: AAPL12/12,MSFT12/12,AAL8/12 (cash,capex,liabilities,R&D
  unmatched, not necessarily absent under other tags). Revenue/earnings,debt and
  operating cash flow available. AAPL/MSFT capex and R&D also matched.
- Duration warning: AAPL report start2025-09-28,MSFT2025-07-01,AAL2026-01-01;
  quarterly request does not imply standalone-quarter amounts. Unit labels include
  usd,u_usd and differing EPS units; acceptance timestamps have no explicit timezone.
  Retain raw-fact interpretation, no derived FCF/ratios or historical availability claim.
- VALIDATED3focusedtests for header-only secrets,denial stop,byte limit,exception
  redaction,identity and tagged-fact parsing; editor diagnostics clear.
- NEXT: user decision on a bounded freshness/tag/unit/period validation before any
  Finnhub integration. Do not refetch automatically or replace the blocked Polygon
  source. Earlier annotator runtime notes below are dated, not rechecked by this probe.

## Objective And Authority
- Latest request: "lets go ahead" on bounded financial acquisition and automatic
  annotation production/deployment. No paid upgrade, alternate source, broad backfill,
  identity repair or existing alert/market/ingestion worker restart authorized here.
- [Current runbook](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#automatic-annotation-and-financial-pilot):
  financial pilot first5active enrolledCS alphabetically,15request ceiling/twoquarterly
  rows/statement/1MB, no pagination/retries. First AALincome request17:00:14Z HTTP403;
  stopped after1request/0reports. Report+sanitizedreceipt retained, no secrets. Exact
  entitlement cause/successful provider schema unverified; no more probes until resolved.
- Automatic scripts/run_stock_alert_context_worker.py activated17:05:51.948595Z;
  provider-free,60secondchecks,one selected publication/cycle,current+priorXNYSsessions,
  original cutoff-eligible facts only. Activation binds reader directory/enrollment/
  policy, independent advisory lock. Earlier publications excluded. Changes fail closed.
- Evidence write-once, originalpublication/manifestfactsverified, immutable sidecars;
  trade/watch paths covered. Never mutates alert ledger, plans, deadlines or OKE overlay.
- Deployed explicitly via start_workers.ps1 -Worker AlertContext in standalone
  stock-alert-context-worker; not added to All/Equity defaults. Latest heartbeat
  17:10:34Z WAITING_FOR_SELECTED_PUBLICATION,19observed/0pending. Ancestry23820->23960->7704,
  StockAlerts12028/12244 unchanged. IDs dated, inspect before any future stop.
- VALIDATED155context/reader/Polygon/launcher tests; diagnostics clear. Nonempty
  trade/watch cycle tested with isolated facts; first live nonempty attachment remains
  unobserved. No frontend changes this increment. Original activation preserved on
  annotator-only reload after watch-only fix. Other workers untouched by this work.
- NEXT: observe first selected prospective attachment; resolve existing financial
  endpoint403 or separately approve alternative source. No automatic financial retry,
  subscription change, invented values or retroactive availability. Below is earlier
  audit evidence, not current worker ownership or standing acquisition authority.
- [Implementation and runbook](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#company-context-implementation-milestone)
  records the contract, source paths, CLI and verification. Existing winners/plans,
  original enrollment, OKE quarantine and other services were not changed by this work.
- OBSERVED final capture16:42:24.523641Z: original386/selection385; daily363ready,
  sector245ready/105unavailable/36N/A, native385ready/1unavailable, earnings350ready+36N/A,
  market/FOMCready. No retained financial reports for cohort, not just filtered/stale.
  [Final manifest](../stock_alert_company_context_2026-09-16.final.json) and
  [.annotations](../stock_alert_company_context_2026-09-16.final.annotations.json):
  zero selected alert rows; no genuine nonempty sidecars published. Both DB reads
  read-only, original publications unchanged,6951facts/causal clocks/hashesverified.
- Pure financial_context preserves source units/signs and quality; unknown units and
  cash-flow period basis block ratios/growth, not alerts. Empty reports unavailable.
- research/stock_alert_annotations.py: immutable per-run sidecars, source publication
  hash, exact source/run/security/plan/cutoff binding, atomic no-replace publication;
  corrupt/missing context isolated. SHADOW reader attaches after pagination,2MB/file,
  10MB/page. REPLAY/LEGACY unchanged. Existing alert details show facts/provenance.
- prepare_stock_alert_context.py --company-context captures to NEW output; separate
  --freeze-alert-context verifies retained artifact and ledger then writes sidecars.
  No provider/ledger writes; final freeze completed with zero rows. Nonempty flow
  verified using isolated fixtures, not live-delivery evidence.
- VALIDATED final process task Test stock alert context builders:171passed;14frontend
  navigation tests,tsc/build,artifactverifier pass. Browser fixture pointer/keyboard,
  partial/missing context and1440/390screenshots pass; nooverflow/clippedtext. Fixture
  removed, actual API200/zeroalerts at that earlier milestone; deployment now above.
- Execution: shared shell parallel calls interleaved once; confirmed no job still
  running and recovered via sequential direct process tasks. A task output also
  showed another terminal's diff; durable artifact and exact process checks avoided
  duplicate capture. Prefer process tasks for final checks; no unverified exit claims.
- Company catalyst/moat/ownership and short/gamma studies remain later backlog;
  missing context never changes winners. Financial units/period normalization still
  requires real eligible source evidence; synthetic success tests do not certify it.

## Earlier Dated Milestones

- Development score/ETF activity/NAV calculator/bond results remain unchanged.
- September16 current inspection found API/Vite plus ingestion/alert/other workers.
  Only the original context refresher was reloaded, preserving its command/settings.
- Old context terminal0804af8c-9903-43c9-9c04-dc8204a4e8fe was stopped. Active
  replacement is process task `Watch stored market conditions`; startup2026-09-16
  15:29:01Z WAITING_FOR_NEXT_SOURCE_WINDOW, advisory lock held,300secondchecks.
  No other workers touched. Recheck ownership before operating; no duplicate launch.
- [Development details](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md): score44.0,
  PARTIAL trackedbreadth331/349, fixed25%momentum/participation/VIX/credit, no missing
  reweighting. Component scores12.069/49.245/49.206/65.476. Not FearGreed/probability.
- [Development artifact](../stock_rotation_2026-09-16.development.json): capture
  15:25:02Z,1301facts/940lineagesverified,14.1MB,24.531sec,originalpubsunchanged.
  Option activitySPY/QQQavailable,12sectorETFsnoeligiblematrix. NAV/sharesnone;14rows
  NeedsSource. Approved retained artifact contract in design; not a live issuer feed.
- VALIDATED37tests,tsc/bundle/artifactverifiers. New browser tab visible: realpointer,
  keyboard4tabs,disagreementdetail,desktop1440/mobile390screenshots,nooverflow.
  Prior hidden/shared-tab visual limitation now resolved for this workspace slice.
- Computed/provider improvement order documented, not run: choose HYvsIG versus
  HYOAS exposure; distributions/totalreturn; dated duration matching; disjoint tests.
  Frozen study unchanged; details-only projection retains10disagreements and hashes.
- [Bond implementation and results](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md):
  new versioned additional_context.bond_etf_relative_performance, independent of
  genuine credit/OAS field;1/5/20-session raw-price differences, absolute returns,
  strict identity/time/basis gates. New study mode reuses opt-in daily/FRED history.
- [Frozen study](../bond_etf_oas_comparison_2026-09-16.json):253 price sessions
  2025-09-12..2026-09-15; capture15:02:40Z; retained OAS receipt14:49:02Z.
  Existing credit approval was enabled at run time; no env changes/new requests.
  733windows verified:252/248/233paired, Pearson vs tightening .384/.204/.277,
  directional agreement67.0/59.0/66.5% (nonzero denominator explicit).
  20-session first/second-half Pearson .573/-.100; not a stable OAS substitute.
- CLI: --bond-study --session <completed-date> --output <NEW-path>, optional
  --oas-reference/--publish-bond-comparison; --verify offline, --bond-from offline
  recomputation. Existing process task Verify frozen bond ETF OAS comparison.
- Separate file-only summary stock-rotation/bond-comparison.json; GET never reruns
  comparison. New [context snapshot](../stock_rotation_2026-09-16.bond.json) verifies
  1269facts/936lineages/originalpubsunchanged. Incomplete terminal capture output
  recovered by durable artifact/reader verification; no duplicate capture launched.
- VALIDATED32backendtests,tsc/bundle,realretainedstudyofflineverification,HTTP200,
  DOM sample-switching/values/1440+390overflowchecks. Browser hidden: pointer/key
  activation stalled and screenshots clipped; manual visual/pointercheck pending.
- [Current implementation/setup](../STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md):
  September16 section supersedes older daily-only notes below. Snapshot
  [stock_rotation_2026-09-16.context.json](../stock_rotation_2026-09-16.context.json):
  capture09:07:48Z, sourceSeptember15,1268facts/934lineagesverified,13.1MB.
  Breadth332/350,101up/228down/3flat,41.87%aboveSMA50/56.93%aboveSMA200;
  SPYrealizedvol8.8075%;14/14proxyvolumeratiosready. FREDno-key/creditapprovalblocked.
- VALIDATED22contexttests (mockHTTP,configurationreload/revoke,credentialredaction,
  exactvolume,missinghistory,breadth,clock/units/coverage,duplicateleader),tsc/bundle,
  desktoprealvalues/statuses; no liveFREDrequest made beforekeyconfiguration.
- Remaining: per-alertfrozenlinks, intradaypricedivergence, sectoroptionscohort and
  NAVsharesfeeds, calibrated sentiment/independent outcomeevaluation. Development
  conditions score is enabled descriptively; no winnerpolicy activated.

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
  no migration/reenrollment done. Reader stays on v1 untilexplicitcutover.
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
  scenario-aware evaluation then at most one model-specific challenger,
  separateevent/optionsstudies,
  matchedoptionsstockcohort, calibratedprobabilityonlyafterprospectivevalidation.
- IMPLEMENTED: research/stock_alert_context.py pure daily/market/sector/event/IV
  gates; scripts/prepare_stock_alert_context.py one DBrepeatable-read/read-only
  snapshot +separateSQLite snapshot, boundedcapture, write-once artifacts/--verify.
- [Readiness results](../STOCK_ALERT_CONTEXT_READINESS_2026-09-15.md): finalmanifest
  stock_alert_context_readiness_2026-09-15.final.json + .final.annotations.json.
  Current386: marketREADY/MIXED, daily363ready/15actioninvalid/8insufficient,
  sector245ready/105unavailable/36nonapplicable, native386, earnings350+36N/A,
  FOMC386. Options10stocksmatrixactivity;0matchedIVcontextsactivepolicy.
  VIX/NBBO/flow/borrow/halts/CPIjobsuncovered, OIknowledge/allactioncoverageunknown.
- Capture25.34sec,7focusedtestsPASS;offlineverifier7306facts/42annotationrows,
  causalobserved/created/availabletimestamps, hashes andoriginalpublicationsunchanged.
  Retrospectiveannotations explicitlyRECONSTRUCTED_FROM_RETAINED_ASOF_INPUTS.
  Nonfinaldiagnosticpair remains; do not use its provisionaloriginal_context flag.
- Existingtasks process: Prepare stored stock alert context readiness (newoutput
  required forrerun), Test stock alert context builders, Verify stored stock alert
  context artifacts. SharedPowerShellfailed basiccmdlets; namedprocess taskswork.
- PLANNING DECISION: withdraw the earlier all-agree market/sector continuation gate.
  Market down/Mixed must not automatically exclude longs or sector-weakness shorts.
  Distinguish absolute direction, paired relative leadership and its change;
  outperformance while falling is resilience, not an automatically profitablelong.
- Rotation implemented: sector-minusSPY and stock-minussector1/5/20session returns;
  recent5relative minus precedingNONOVERLAPPING5relative. LeadershipaxisRS20,
  changeaxisdelta5 =>leadingstrengthening/leadingweakening/laggingimproving/
  laggingdeteriorating; absolutepricesseparate, two-completeddailystateconfirmation
  implemented on reconstructed5sessiontrails, boundary/unknownexplicit. No forecast
  of the nextwinningsector. Confirmation requiresconsecutive>=2evenonreversion.
- Workspace implemented: `/market-conditions`, price-firstMarketConditions with
  sectorhistory, searchable/sortablesectors, stockdivergence, indicators/coverage.
  Fixedper-alertlinks andcontinuousrefresh NOTimplemented. Later
  composite owntransparentcontract, not screenshotvendor'sformula; proposed5equal
  categoryweights20%each, fixedwithinweights, no missingdatareweighting/50fill.
  VIXseparatestress/direction; scoreUnavailuntilrequiredinputhistorypasses.
- Flow distinction: price/volumeleadership !=ETFfundflows !=optionspremiumactivity.
  Existing optionspremium=dayvolume*retainedmark*multiplier, not traded/netpremium.
  RealETFcreation/redemptionestimatesneeddatedNAV/sharesout/splits/distributions;
  sectorETFoptionscoverage notprovided by10stockoptionscohort. No fabricatedIn/Out.
- OBSERVED: capture2026-09-15T17:07:45.731323Z, sourcecloseSeptember14,
  99162dailyrevisions/386references/1254actions,42.39sec,read-onlyrepeatableread.
  8/12proxiesready; XLY/XLE/XLU/XLB blockedbyknownactions despite253bars.
  245/350commonstocksready,105unavailable,36ETFnotapplicable; originalpubsunchanged.
- Owningcode: research/stock_rotation.py; prepare_stock_alert_context.py
  --rotation-only/--publish-rotation-view; stock_discovery_api.py file-onlyendpoint;
  frontend MarketConditionsPage. Dailycontextadds previous5,volume,252sessionrange.
- Canonicalartifact: [final rotation snapshot](../stock_rotation_2026-09-15.final.json).
  1245facts/918lineagesverified; sharedupdatesstock-rotation/latest.jsononly.
  Original24.2MBexceeded20MBreadguard; catalogprojection11.7MBretainsguard.
  Originaland.compactartifactsretaineddiagnostics; .finalcorrectsConfirmed(1)
  viaofflineprojection, no newDBreads/changedreturns/cutoffs. Hashancestryretained.
- VALIDATED:13contexttests,69browsermodelregressions,typecheck/bundle;realHTTP200,
  pointersearch/sectorselection/stocklookup/emptyresults/indicators,keyboardtabs;
  screenshots1440/390,nohorizontalpageoverflow/clippedcontrols.
- Tasks: Capture stored sector rotation snapshot (newoutputpathrequired), Verify
  stored sector rotation snapshot (offline.final), Test stock alert context builders.
  GETreloadonlyreadsstoredsnapshot; nocontinuouscontextproducerstarted.
- NEXT: user sets localFREDkey/creditapproval; collector auto-detects nextcheck.
  Immutableper-alertlinks remain pending, preservingexactsource/run/cutoffandwinners.
  Earlier readiness result above unchanged; latestrotationdataisdated,notlivefeed.
  Strict matchedIVdryrun+boundedrepair-vs-licensecomparison stillneeded; current
  rawinventory256-259datesisNOTmatchedseriesreadiness/historicalavailability.
- NOT_CHECKED: independentprices, newqualityperformance/multisessionsoak/licensing.
  No appworker/readerpointer/sourcepublication changes bythismilestone.