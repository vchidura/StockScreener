# Manual trade execution from alerts (future design)

Recorded November 10, 2026 at the user's request. Status: future design; nothing built; no date. Companions:
[research roadmap section 11.5](EQUITY_MODEL_DETECTION_AND_RESEARCH_ROADMAP_20261024.md) (review now, broker later),
[option package evaluation](OPTION_PACKAGE_STRUCTURE_AND_STRATEGY_EVALUATION.md) (execution stays false until a separate
execution ledger and its real-time inputs exist), [Model Lab design section 8.2](EQUITY_MODEL_LAB_DESIGN.md) (when the
platform needs live stock quotes) and the [G102 target design](g102/G102_TARGET_DESIGN.md) (invariants reused here).

## 1. What is wanted

An execute button (view icon) on a Stock Alert or an Options Alert opens an order ticket for that alert's instrument:
a stock, one option contract, or an option package. The user reviews the ticket and submits it, and the order goes to
a configured broker. Execution is manual: nothing reaches a broker without the user's click and confirmation.

## 2. What market data execution needs

Executing an order needs the **exact live quote of the instrument being executed, at the moment of execution**:

- a stock: its best bid and offer across exchanges (NBBO) with sizes and the exchange time;
- an option package: the NBBO of every leg, and the package's net bid, ask and mid from them.

It does **not** need:

- **live quotes for every detected ticker or contract.** Only the instrument on an open ticket is quoted: when the
  ticket opens, refreshed while it stays open, and again immediately before submit;
- **the trade stream.** The last sale is context on the ticket, not an input to the order;
- **page prices.** The 1-minute bars and the 5-minute publications never price an order.

After submission the broker is the source of truth: order status, fills, positions and buying power come from the
broker's order updates, not from market data.

| Data | Source | Used for |
| --- | --- | --- |
| Live NBBO of the ticket's instrument (each leg) | Market-data provider on demand (REST), the broker's quote API, or both (decision 10) | Proposed limit price, spread and staleness checks, evidence |
| Last sale | Provider, optional | Context on the ticket |
| Alert evidence (alert price, model, time) | Platform alert record | Showing how far the price moved since the alert |
| Order status, fills, positions, buying power | Broker | Everything after submit |

## 3. Flow

Alert, then ticket, then checks, then the user's confirmation, then submit, then broker updates, then fills, then
positions. Pending or unfilled orders are not positions; a partial fill creates only the filled exposure. The alert
record never changes: the ticket, order and fills link to it by id (roadmap 11.5).

## 4. The order ticket

- Instrument or legs, side, quantity, account (paper or live), time in force (day by default).
- Order type: **limit by default**, with a proposed price from the fresh quote (mid or the touching side, the user's
  choice). Market orders only for stocks and only by explicit choice; never for options (wide spreads).
- Shown beside it: the alert price against the live quote, the spread, the quote's age, the estimated cost and the
  buying-power effect read from the broker.

## 5. Checks before submit

Each check refuses or warns, and every result is recorded with the ticket.

- **Quote fresh:** the quote's age at submit is within a limit (to decide; for example 2 s for stocks, 5 s for
  options); a stale quote refuses.
- **Spread:** within a limit (basis points for stocks, a share of the mid for options).
- **Price moved since the alert:** beyond a limit, warn and require a new confirmation.
- **Market state:** the session is open (extended hours only if enabled), the instrument is not halted, the option has
  not expired.
- **Broker-side:** buying power, options approval level, pattern-day-trader status and borrow for a short sale are read
  from the broker and shown, not re-implemented.
- **Platform limits** (configured): maximum notional per order, maximum contracts, maximum open orders, daily loss
  limit; a kill switch disables all submits.
- **One order per click:** each ticket carries an idempotency key sent to the broker as the client order id, so a
  repeated click or a retry cannot send a second order.
- **Multi-leg:** an option package goes as one broker complex order with a net limit, never leg by leg; if the broker
  cannot take it, the ticket refuses.

## 6. Records (execution ledger)

Append-only, nothing overwritten (as G102 INV-6):

- **Ticket:** alert id and model version, instrument or legs, the quote evidence used for each leg (bid, ask, sizes,
  exchange time, receive time), check results, proposed and chosen price, quantity, user and time.
- **Order:** broker order id, client order id (the idempotency key), account, the request as sent and the broker's
  answer.
- **Order events:** every broker update (accepted, partial fill, filled, cancelled, rejected, expired) as its own row
  with the broker's time and our receive time.
- **Fills:** price, quantity, fees and time. **Positions** are derived from fills only.
- Alert price performance stays separate from broker P/L (roadmap 11.5).

## 7. Architecture

- **A separate execution service**, outside the research pipeline: its own process, its own credentials (mounted
  secrets; never in the database, logs or the portal), its own tables and its own failure domain. Research services
  never call the broker; the execution service never writes alerts or bars.
- **Quotes for tickets** come on demand for the ticket's instruments only; no continuous stream is added for this
  (Model Lab 8.2). If a quote service is shared with D1/D2 later, it serves both.
- **Broker adapter:** account, positions, buying power, quote (optional), submit, cancel, replace and an order-update
  stream; one adapter per broker; a paper account first.
- **Portal:** the execute button appears only for an authenticated user, only when execution is enabled in
  configuration, and only on alerts whose instrument is still tradable. The portal is read-only today, so
  authentication and authorization come before any execute button.
- **Audit:** every action and every refusal is recorded.

## 8. What this does not change

- Alerts stay alert-only: no automatic orders; targets, stops and invalidations stay advisory. Automatic exits need a
  separately approved policy.
- No continuous quote stream for all names, and nothing changes in the minute-bar journal (Model Lab 8.2).

## 9. Phases

| Phase | Work | Account |
| --- | --- | --- |
| X0 | Decisions in section 10 | - |
| X1 | Broker adapter, read-only (account, positions, order history); quote-at-ticket service; no submit | Paper |
| X2 | Stock orders, limit only; ledger, fills, positions page | Paper |
| X3 | Single-leg options; then multi-leg as broker complex orders | Paper |
| X4 | Live account with small limits and the kill switch, staged | Live |
| X5 (optional) | Close tickets from advisory targets or stops, each confirmed by the user | Paper, then live |

Each phase is rehearsed against the broker's paper account and recorded as evidence before the next.

## 10. Open decisions

- Broker. Criteria: API for stocks and multi-leg options, a paper account, an order-update stream, fees, account types.
- Quote source on the ticket: the market-data provider's NBBO, the broker's quote, or both (recorded side by side).
- Thresholds: quote age, spread, price move since the alert, platform limits.
- Extended-hours orders: allowed or not.
- Users: single user or several (affects authentication, accounts and limits).
- Short sales of stocks: allowed or not.
