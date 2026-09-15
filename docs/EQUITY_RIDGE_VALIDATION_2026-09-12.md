# Ridge Contributor And Missing-Path Validation

Closeout: the [Final Sensitivity And Decision](EQUITY_RESEARCH_CLOSEOUT_2026-09-12.md)
is complete under subsequent user approval. It preserves this review and the
original model/results; no new strategy is qualified or deployed.

## Conclusion

**The large CAR gain is corroborated, not a discovered split-adjustment error.** All
ten selected large-contributor paths match the original provider's native daily data.
CAR also matches a separate Yahoo Finance price feed within rounding tolerance.

The five unresolved selections are not interchangeable random data gaps. Primary
filings and provider identity evidence identify three symbol continuations, a trading
suspension during an existing holding, and a halt that prevents the specified entry.
These require different execution and valuation treatment.

The frozen ridge study has **not been changed or rerun**. Its original later returns
remain 89.40% / 79.81% under the zero-missing-return illustration, with 22.56% / 23.72%
scenario drawdown. This review does not make the strategy pass the 20% research target.

## Bounded Scope

Selected from the saved ridge predictions: the five largest observed later gross
contributors per sample, plus every unresolved selected position. This gives 15 paths,
without selecting new portfolios or fitting models. Initial portfolio weights include
the unresolved names when measuring contribution.

The original-cutoff database prices were compared with native adjusted daily opens
and closes. Unadjusted prices, in-window splits/dividends and dated ticker references
were checked separately. Provider responses were retrieved through the existing
checksum-validated cache; some responses were reused rather than fetched anew.

## Large Contributors

| Sample | Reviewed Tickers | Outcome |
|---|---|---|
| 1 | CAR, OKLO, INTC, MRVL, MU | All 21 daily opens/closes match for each path |
| 2 | RGTI, EOSE, SGML, AXTI, CIFR | All 21 daily opens/closes match for each path |

Across these ten paths, all 210 daily sessions match. Adjusted and unadjusted prices
also match, no splits or dividends were returned for the holding windows, and the
decision, entry and exit issuer/share-class references agree. These are same-provider
checks, not independent confirmation of every ticker or every possible corporate event.

### CAR Cross-Source Check

The saved entry is **$99.34 at the 2026-03-17 open** and exit is **$395.77 at the
2026-04-15 close**, a 298.399% gross stock return. Yahoo's corresponding values are
99.3399963 and 395.7699890. All 21 opens and closes agree within an explicit one-cent
tolerance, which allows binary-float and sub-cent open-price rounding. No split or
dividend event is returned by that chart response.

The separate feed is available through the
[Yahoo CAR daily chart query](https://query1.finance.yahoo.com/v8/finance/chart/CAR?period1=1773705600&period2=1776297600&interval=1d&events=splits%2Cdiv).
The full checked prices, response hash and tolerance are saved in the follow-up artifact.
Separate vendors may still share upstream sources; this is price corroboration, not
proof of the mechanism behind the move or of future strategy performance.

The [Avis Budget March 27 Form 8-K](https://www.sec.gov/Archives/edgar/data/723612/000119312526129481/d34396d8k.htm)
describes an at-the-market offering agreement for up to five million common shares.
It does not supply a share-conversion adjustment to apply to this return. The review
does not infer that the offering caused the price rise. CAR remains a large economic
contributor: 13.16 percentage points of gross return in one portfolio window, not an
additive attribution of the whole compounded evaluation.

## Five Unresolved Selections

| Original Ticker / Decision | Finding | Consequence For The Frozen Study |
|---|---|---|
| QMMM / 2025-09-12 | SEC suspension covers September 29 through October 10; prices remain absent at October 13 exit | Existing holding cannot be assigned an observed terminal value |
| KAR / 2025-12-11 | Same issuer/share class continues as OPLN; provider changes symbol on December 26 | Recorded old/new prices can form a complete review-only path |
| CLSK / 2024-11-07 | Nasdaq halt spans the intended November 8 entry; common-stock halt lifted November 11 | No executable price at the frozen entry; not an ordinary missing held-position mark |
| MNMD / 2026-01-13 | Rebranded as Definium, DFTX; official effective date January 13, provider handover January 15 | Same-share identity is supported, but provider symbol dates lag the filing |
| SATS / 2026-06-15 | Same issuer/share class continues as ECHO; provider handover June 24 | Recorded old/new prices can form a complete review-only path |

### Suspension And Unavailable Entry

QMMM's [October 7 Form 6-K](https://www.sec.gov/Archives/edgar/data/1971542/000149315225017183/form6-k.htm)
reports receipt of the SEC order on September 26, with suspension from 4:00 a.m. EDT
September 29 through 11:59 p.m. EDT October 10, 2025. The SEC's
[trading-suspension index](https://www.sec.gov/enforcement-litigation/trading-suspensions)
lists QMMM release 34-104113. The filing also reports a Nasdaq information request.
This establishes the suspension interval, **not an observed October 13 exit or proof
of the exact exchange status after the SEC interval**. Neither a zero return nor a
total loss is established by the suspension. The provider still marks the ticker
active in its exit-date reference, which is not evidence that trading was executable.

CleanSpark's [November 15 Form 8-K](https://www.sec.gov/Archives/edgar/data/827876/000095017024127680/clsk-20241115.htm)
states that Nasdaq halted its securities beginning November 7 and lifted the
common-stock halt on November 11, 2024. The issue concerned warrant terms following
the GRIID acquisition. The specified November 8 open was therefore unavailable.
Keeping that allocation in cash or waiting until November 11 would be a different,
explicit execution rule. This review does not backdate a fill or remove the selection.

### Symbol Continuation Evidence

The new-symbol exit-date references match the original CIK and share-class FIGI for
all three continuations. Combining only recorded old-symbol and new-symbol prices
covers every one of the 21 expected sessions. The original raw prices match the stored
prefix, there are no conflicting overlapping prices, and no split is returned for
either symbol in the holding window. No interpolation or replacement stock is used.

| Original / Successor | Original Entry Open | Successor Exit Close | Review-Only Gross Return |
|---|---:|---:|---:|
| KAR / OPLN | $28.74 | $30.75 | +6.99% |
| MNMD / DFTX | $14.86 | $16.23 | +9.22% |
| SATS / ECHO | $118.865 | $91.54 | -22.99% |

These are **candidate same-share price returns, not corrected backtest results**.
The artifact classifies them as `PROVIDER_ALIAS_JOIN_REVIEW_ONLY`.

- OPENLANE's [January 30 Form 8-K](https://www.sec.gov/Archives/edgar/data/1395942/000139594226000002/kar-20260129.htm)
  lists OPLN as its common-stock symbol. Together with matching CIK/FIGI this supports
  identity continuity, but the exact official transition date was not established
  from the retrieved primary filing. December 26 is the observed provider handover.
- Definium's [January 12 Form 8-K](https://www.sec.gov/Archives/edgar/data/1813814/000119312526009854/mnmd-20260109.htm)
  explicitly says the CUSIP is unchanged, no shareholder action is required, and
  DFTX trading starts at market open January 13. That precedes the January 15
  handover in the provider's daily series; the latter must not replace the official
  effective date. The original MNMD label at the decision/entry is stale.
- EchoStar's [June 25 Form 8-K](https://www.sec.gov/Archives/edgar/data/1415404/000141540426000030/sats-20260623x8k.htm)
  lists ECHO, whereas its [June 18 Form 8-K](https://www.sec.gov/Archives/edgar/data/1415404/000141540426000027/sats-20260617x8k.htm)
  lists SATS. Exact primary-source transition timing remains unverified here;
  June 24 is the observed provider handover, corroborated by the later cover page.

## Next Decision

The largest new gain no longer looks like an isolated data-adjustment artifact.
The stock-selection result remains worth examining, but it is not yet qualified.
The next bounded experiment should be a **separate event/execution sensitivity**, not
another fitted model: retain the saved scores, consistently apply any accepted
same-share continuations to every affected portfolio, and state the halt/no-fill
treatment before recomputing performance.

QMMM still needs a defensible terminal valuation or an explicit unresolved scenario.
CLSK needs an explicit execution rule. The known provider/official symbol-date
differences must remain visible. Any evaluation-only sensitivity would still leave
the original model's omitted training labels unchanged; a refit would be a separate
experiment, not a silent update to this one. No such sensitivity or refit was run here.

## Artifacts And Checks

- [equity_ridge_path_validation.json](equity_ridge_path_validation.json): all 15 original/provider path and corporate-action checks.
- [equity_ridge_validation_followup.json](equity_ridge_validation_followup.json): independent CAR prices and three reviewed symbol joins.
- [EQUITY_RIDGE_CHALLENGER_2026-09-12.md](EQUITY_RIDGE_CHALLENGER_2026-09-12.md): unchanged original experiment findings.

All 83 focused tests passed, including incomplete/interior-path mismatches, explicit
independent-price tolerance, selected missing weights, symbol identity mismatches,
intervening splits, conflicting prices and incomplete old/new joins. The first
successor-only check was intentionally fail-closed but too restrictive for a split
ticker history; its corrected join still requires every recorded day and preserves
all identity and price checks. No original price or return was changed to make a
test pass.

Artifact checks verified 15 unique targets, 210 same-provider contributor sessions,
21 independent CAR sessions, three complete review-only joins, and unchanged original
study, prediction and source-file hashes. Original source cutoffs remain pinned.
Only separate research review files and local response caches were written; database
access was read-only. No model fit, production data correction, live ranking change
or trading order occurred.