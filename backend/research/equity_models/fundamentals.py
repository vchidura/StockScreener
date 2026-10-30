"""Point-in-time rank construction for P1/P2; no imputation or provider fetching."""
from __future__ import annotations

import pandas as pd

from .catalog import digest
from .data import ResearchData, Unavailable, number, stamp, ready_value


def build_rank_facts(data: ResearchData, securities, at):
    """Build ranks over an explicit, complete dated candidate population.

    Caller supplies the dated full population, not today's surviving symbols.
    Missing eligible evidence fails the cohort instead of inflating survivor ranks.
    total_return_close must be an exporter-certified total-return index on every
    daily row used by the 12-minus-1 calculation; adjusted price is not a substitute.
    """
    at = stamp(at)
    securities = tuple(sorted(securities))
    if not securities or len(set(securities)) != len(securities):
        raise ValueError("unique explicit dated population required")
    rows, lineage, sessions = [], set(), set()
    for security in securities:
        eligibility = data.fact(security, "eligibility", at)
        lineage.add(f"{eligibility['source_id']}:{eligibility['revision']}")
        if eligibility["values"].get("member") is not True:
            continue
        if eligibility["values"].get("actions_covered") is not True:
            raise Unavailable(f"ACTIONS_UNCOVERED:{security}")
        frame = data.history(security, "1d", at)
        if len(frame) < 273:
            raise Unavailable(f"INSUFFICIENT_RANK_HISTORY:{security}")
        latest = frame.iloc[-1]
        sessions.add(latest.session)
        if latest.close < 5 or ready_value(latest, "liquidity60") < 50e6:
            continue
        report = data.fact(security, "fundamentals", at)
        values = report["values"]
        if values.get("common_stock") is not True or values.get("sector") in {"FINANCIALS", "REIT"}:
            continue
        required = ("sector", "assets", "gross_profit", "operating_cash_flow", "net_debt",
                    "capital_expenditure", "enterprise_value", "equity_market_value", "ebit")
        if any(k not in values or values[k] is None for k in required):
            raise Unavailable(f"INCOMPLETE_RANK_FUNDAMENTALS:{security}")
        assets, gp, cfo, debt, capex, ev, market, ebit = (number(values[k]) for k in required[1:])
        if min(assets, ev, market) <= 0 or capex < 0:
            raise Unavailable(f"NONCOMPARABLE_FUNDAMENTALS:{security}")
        if "total_return_close" not in frame or "total_return_basis_id" not in frame:
            raise Unavailable(f"TOTAL_RETURN_INDEX_REQUIRED:{security}")
        if frame.total_return_basis_id.iloc[-253:].isna().any() or frame.total_return_basis_id.iloc[-253:].nunique() != 1:
            raise Unavailable(f"TOTAL_RETURN_BASIS_MISMATCH:{security}")
        recent = number(frame.total_return_close.iloc[-22])
        old = number(frame.total_return_close.iloc[-253])
        if min(recent, old) <= 0:
            raise Unavailable(f"INVALID_TOTAL_RETURN_INDEX:{security}")
        rows.append(dict(security_id=security, sector=values["sector"], momentum=recent / old - 1,
                         gross_profit_assets=gp / assets, cash_flow_assets=cfo / assets,
                         negative_net_debt_assets=-debt / assets, fcf_yield=(cfo - capex) / market, ebit_ev=ebit / ev))
        lineage.add(f"{report['source_id']}:{report['revision']}")
        lineage.update(report["input_ids"])
        lineage.update(f"{r.source_id}:{r.revision}" for r in frame.itertuples())
    if len(sessions) != 1 or not rows:
        raise Unavailable("RANK_COHORT_SESSION_MISMATCH_OR_EMPTY")
    session = next(iter(sessions))
    if data.session_bounds(session)[1] > at:
        raise Unavailable("RANK_SESSION_NOT_COMPLETE")
    frame = pd.DataFrame(rows)
    frame["momentum_rank"] = frame.momentum.rank(method="average", pct=True)
    by_sector = frame.groupby("sector")
    quality_columns = ["gross_profit_assets", "cash_flow_assets", "negative_net_debt_assets"]
    value_columns = ["fcf_yield", "ebit_ev"]
    frame["quality"] = by_sector[quality_columns].rank(method="average", pct=True).mean(axis=1)
    frame["value"] = by_sector[value_columns].rank(method="average", pct=True).mean(axis=1)
    frame["quality_rank"] = frame.groupby("sector").quality.rank(method="average", pct=True)
    frame["value_rank"] = frame.groupby("sector").value.rank(method="average", pct=True)
    cohort = digest(dict(session=session, requested_population=securities,
                         eligible=sorted(frame.security_id), input_ids=sorted(lineage)))
    valid_until = data.session_bounds(data.session_offset(session, 1))[1]
    return [dict(security_id=row.security_id, source_id=f"fundamental-rank:{cohort}:{row.security_id}",
                 revision="1", kind="fundamental_ranks", market_at=data.session_bounds(session)[1].isoformat(),
                 available_at=at.isoformat(), received_at=at.isoformat(), valid_until=valid_until.isoformat(),
                 methodology="eligible_cross_section_sector_average_rank_v1", input_ids=sorted(lineage),
                 values=dict(cohort_id=cohort, coverage_complete=True, eligible_count=len(frame),
                             sector_count=int((frame.sector == row.sector).sum()),
                             total_return_12_minus_1_percentile=float(row.momentum_rank),
                             sector_quality_percentile=float(row.quality_rank), sector_value_percentile=float(row.value_rank)))
            for row in frame.itertuples()]
