# Polygon Options Developer Phase 0 Validation

Status: conditional GO for Phase 1 implementation

Validated: 2026-08-29, weekend after the 2026-08-28 session

Related documents:

- `OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md`
- `OPTION_CHAIN_SCANNER_DESIGN.md`
- `OPTION_PLATFORM_CAPACITY_DECISION_2026-08-29.md`

## 1. Decision

Proceed with Phase 1 market-data implementation. The configured account now has the
Developer capabilities required by the design, and representative/all-universe REST
data passed the weekend checks below.

This is not approval for unattended paper operation or automatic execution. The first
market-hours validation remains mandatory because a weekend cannot prove the live
15-minute delay cadence, incremental cursor behavior, current-session volume evolution,
or full-cycle timing under active market traffic.

## 2. Entitlement Results

| Capability | Expected Developer result | Observed result | Status |
|---|---|---|---|
| Option-chain snapshot | 15-minute delayed | HTTP 200, status `OK` | Pass |
| Option reference contracts | Daily reference | HTTP 200 | Pass |
| Individual option trades | 15-minute delayed | HTTP 200, status `DELAYED` | Pass |
| Option minute aggregates | 15-minute delayed | HTTP 200 | Pass |
| Daily open interest | Included | Present for every filtered contract | Pass |
| Polygon diagnostic IV/Greeks | Included but nullable | Present for most, absent on sparse contracts | Pass |
| Option quotes | Not included | HTTP 403, `NOT_AUTHORIZED` | Expected denial |
| Underlying minute aggregates | Depends on Stocks plan | HTTP 200, status `DELAYED` | Pass |

No credential or market price was printed or persisted by the probes.

## 3. Filtered Chain and Reference Audit

Request scope for each underlying:

- Expiration from 2026-08-29 through 45 calendar days later
- Strike from 85% through 115% of the latest completed underlying close
- Complete chain pagination at 250 rows per page
- Complete reference pagination at 1,000 rows per page
- Maximum 40 pages, 10,000 rows, and 64 MiB per underlying path

| Underlying | Chain rows | Chain pages | Reference rows | Expirations | Day data | IV/Greeks | Missing references |
|---|---:|---:|---:|---:|---:|---:|---:|
| AAPL | 512 | 3 | 512 | 9 | 472 | 490 | 0 |
| AMD | 800 | 4 | 800 | 9 | 715 | 779 | 0 |
| AMZN | 448 | 2 | 448 | 9 | 409 | 397 | 0 |
| GOOGL | 486 | 2 | 486 | 9 | 449 | 436 | 0 |
| META | 952 | 4 | 952 | 9 | 860 | 916 | 0 |
| MSFT | 762 | 4 | 762 | 9 | 681 | 682 | 0 |
| NVDA | 404 | 2 | 404 | 9 | 404 | 378 | 0 |
| PLTR | 240 | 1 | 240 | 6 | 231 | 228 | 0 |
| SOFI | 132 | 1 | 132 | 6 | 129 | 129 | 0 |
| TSLA | 612 | 3 | 612 | 9 | 583 | 559 | 0 |
| SPY | 4,038 | 17 | 4,038 | 14 | 3,400 | 3,940 | 0 |
| QQQ | 3,776 | 16 | 3,776 | 14 | 3,076 | 3,557 | 0 |
| IWM | 2,146 | 9 | 2,146 | 14 | 1,464 | 1,985 | 0 |
| **Total** | **15,308** | **68** | **15,308** | - | **12,873** | **14,476** | **0** |

All 15,308 audited snapshot contracts were standard American calls or puts with a
100-share multiplier. Calls and puts were balanced within each underlying. No option
quote appeared in any Developer chain row.

Interpretation:

- Reference matching is complete for the measured weekend state.
- The largest chain, SPY at 17 pages, is below the 40-page design cap.
- Aggregate chain/reference transfer was approximately 14.6 MiB for all 13 names.
- Day/trade fields are sparse by contract, which is expected when a contract did not
  trade in the latest session.
- Provider IV/Greeks coverage was 14,476 / 15,308, approximately 94.6%. These values
  remain nullable diagnostics and are not the local-IV acceptance gate.

## 4. Representative Trade and Alignment Probe

A liquid near-spot standard SPY contract was selected without persisting or printing
its ticker, strike, premium, or underlying price.

| Check | Result |
|---|---:|
| Delayed trades returned for latest session | 7,704 |
| Trade pages at 50,000-row limit | 1 |
| Observed trade fields | `conditions`, `decimal_size`, `exchange`, `id`, `price`, `sequence_number`, `sip_timestamp`, `size` |
| Option one-minute aggregates | 385 |
| Corresponding underlying minute bar found | Yes |
| Option/underlying source-time skew | 0 seconds |
| Within Developer 60-second alignment gate | Yes |
| Local IV solved within `[0.001, 5.0]` | Yes |

The actual payload includes an `id` field even though implementation does not depend
on a portable generic trade-ID contract. Preserve it as optional provider metadata;
the durable event key and deduplication rules in the design continue to use contract,
SIP/participant timestamp, sequence, correction semantics, and payload hash.

## 5. Cross-Universe Local-IV Smoke Sample

The smoke test selected up to 100 contracts per underlying that were:

- Within 5% of the latest completed underlying close
- Liquid under the volume/OI floor
- Traded during the latest completed session
- Alignable to an underlying minute bar within 60 seconds
- Inside European Black-Scholes no-arbitrage bounds under the smoke assumptions

| Underlying | Eligible/aligned sample | Bounds-valid | Converged |
|---|---:|---:|---:|
| AAPL | 100 | 100 | 100 |
| AMD | 100 | 100 | 100 |
| AMZN | 100 | 100 | 100 |
| GOOGL | 100 | 100 | 100 |
| META | 100 | 100 | 100 |
| MSFT | 100 | 100 | 100 |
| NVDA | 100 | 100 | 100 |
| PLTR | 78 | 78 | 78 |
| SOFI | 34 | 34 | 34 |
| TSLA | 100 | 100 | 100 |
| SPY | 100 | 100 | 100 |
| QQQ | 100 | 100 | 100 |
| IWM | 100 | 100 | 100 |
| **Total** | **1,212** | **1,212** | **1,212** |

This result establishes broad viability of SIP-time-aligned local IV solving. It is not
the formal 95% production acceptance result because:

- It sampled the near-spot 5% corridor rather than every otherwise eligible contract
  in the design's 15% corridor.
- It used a 4% risk-free rate and zero dividend yield for a smoke test rather than the
  versioned point-in-time rate/dividend inputs required by production.
- It ran after the market closed and did not test incremental matrix timing.
- It solved IV but did not validate every full local Greek or scenario output.

## 6. Conditional Passes and Open Gates

### Passed now

- Developer chain, reference, aggregate, trade, OI, and delayed underlying access
- Expected Developer option-quote denial
- Complete pagination within current design caps
- Canonical reference match for every filtered snapshot contract
- Standard contract/multiplier/exercise-style assumptions for the measured universe
- Representative trade history and option aggregate availability
- Same-minute option/underlying alignment
- Representative and cross-universe local-IV solvability
- Workstation and PostgreSQL capacity for Phase 1 development

### Must pass during the next open session

- Actual 15-minute delayed snapshot/trade cadence and source/observation timestamps
- Incremental per-contract trade watermark plus overlap/deduplication behavior
- Late trade and correction handling when observed
- Current-session cumulative volume evolution and prior-session OI semantics
- Intraday newly listed strike detection and catalog admission
- Full 13-underlying ingestion/normalization/analysis cycle p95 below 10 minutes
- Formal local-IV convergence at least 95% per underlying over the full eligible 15%
  corridor using versioned rate and dividend inputs
- Queue/work age, memory, PostgreSQL WAL/temp growth, and restart recovery

### Blocks unattended paper operation, but not Phase 1 coding

- PostgreSQL Windows service auto-start remains blocked by Smart App Control; the
  database currently runs through direct `postgres.exe` startup.
- Windows AC sleep remains enabled unless changed after the capacity assessment.
- Automated full off-host backup and point-in-time recovery are not active.
- The option-pipeline service, alerts, partitions, and retention jobs do not exist yet.

### Dependencies/configuration still required during implementation

- Install and pin `polygon-api-client`.
- Install/pin the PyArrow archive extra before raw trade Parquet archiving is enabled.
- Select and configure the point-in-time event-calendar provider before Phase 2 signals.
- Implement versioned risk-free-rate and dividend inputs before production local-IV
  acceptance.

## 7. Final Phase 0 Verdict

**Conditional GO for Phase 1 implementation.**

Do not wait for Monday to begin the typed domain, migration 015, catalog, provider,
normalization, filter, local-analysis, persistence, and fixture work. Do not enable
unattended paper simulation or describe Phase 0 as fully accepted until the open-session
gates in section 6 pass and the workstation reliability blockers are resolved.