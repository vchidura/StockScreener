# G106 implementation plan

Order follows the user's objective priority (directional intraday, directional swing, income, volatility/event) and
the capacity gates. Rows: `G106_IMPLEMENTATION_MATRIX.md`. Design: `G106_TARGET_DESIGN.md`. Nothing reaches live
without its rehearsal and the user's go; G102 rows named here stay tracked in G102.

## P0 - this week (after the Nov 12 close and the Nov 13 night)

| Order | Rows | Work | Done when |
|---|---|---|---|
| 1 | C1 | Read-only provider REST checks (trades/quotes per contract, index snapshot, rate) | Results in evidence; B4 and D16 inputs confirmed or ruled out |
| 2 | H1 | Daily close chain snapshot table + writer (migration: user's yes) | Rehearsed on a copy; first close captured |
| 3 | D1, AL1 | O1 per-name cap behind a switch; coverage metric in the watcher | Replay Nov 10-12: per-name counts before/after; switch off by default |
| 4 | B1 (V4.1) | C6 profile v4 with buckets, weekday ladder, stock 0DTE on expiry day | Unit tests on recorded chains; Nov 12 preparation re-run offline |
| 5 | G102 V3-1.3 | Capacity replay at 3x on a copy (gate for B1) | market-state p90 under ~60% of a core; Options window under 50% of budget |

G102 tonight in parallel: O3 night 2 (R44), OA1/OA2 rehearsal if the user wants it.

## P1 - next week (9 names)

| Order | Rows | Work |
|---|---|---|
| 1 | B1 (V4.3) | V4 preparation for a session (user's yes) |
| 2 | A2, A3 | IV summary per name/tenor; adapters for GEX, OI walls, variance risk premium on chain rows |
| 3 | A4, D17, D15, D11 | Observations section: unusual activity, GEX/OI walls, term structure (portal batch) |
| 4 | H2, H3, H4 | Research datasets nightly; journal retention and archive; promotion ledger |
| 5 | B3, B4 | Promotion pool design + replayed-day copy rehearsal; on-demand REST trades for flagged contracts |
| 6 | D8 | Start using OI change once H1 has a few sessions |

## P2 - 25 names

A1 parallel chain pages; U1 universe (user's yes, G102 B10 first); G102 V3-1.4 shards; B3 live; intraday detectors D3,
D4, D5, D2; swing D6, D7; income D9 with O3; event D13; regime D16; AL2 structures.

## P3 - 50 names

U2 list (Q1); B2 stream split 600/250/150; D10, D12 rank, D14 with 20+ sessions of history; D18 calibration.

## P4 - continuous

AL3 Model Lab grading per detector; retire weak detectors; accept models on out-of-sample evidence.
