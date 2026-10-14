from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Mapping
from uuid import UUID
from zoneinfo import ZoneInfo

from options.discovery import EligibleChainQuery
from options.domain import ContractType, OptionContractSnapshot


def eligible_chain_snapshot_row(
    snapshot: OptionContractSnapshot, *, matrix_id: UUID, market_time: datetime, observed_time: datetime,
    asset_type: str, policy_version: str, policy_sha256: str,
) -> dict | None:
    greeks = (snapshot.local_iv, snapshot.local_delta, snapshot.local_gamma,
        snapshot.local_theta_per_day, snapshot.local_vega_per_vol_point, snapshot.local_rho_per_rate_point)
    if (snapshot.model_mark is None or snapshot.model_mark <= 0 or snapshot.spot <= 0
            or not snapshot.iv_converged or any(value is None for value in greeks)
            or snapshot.market_data_time > market_time or snapshot.first_observed_at > observed_time
            or (snapshot.revised_observed_at or snapshot.first_observed_at) > observed_time
            or snapshot.expiration_cutoff <= market_time):
        return None
    row = asdict(snapshot)
    row["underlying"] = row.pop("underlyer")
    row.update(asset_type=asset_type, policy_version=policy_version, policy_sha256=policy_sha256,
        data_delay_seconds=snapshot.data_delay_seconds, matrix_id=matrix_id,
        absolute_delta=abs(snapshot.local_delta), mark=snapshot.model_mark,
        volume_open_interest_ratio=snapshot.day_volume / snapshot.open_interest
            if snapshot.day_volume is not None and snapshot.open_interest else None,
        otm_fraction=(snapshot.strike - snapshot.spot) / snapshot.spot if snapshot.contract_type is ContractType.CALL
            else (snapshot.spot - snapshot.strike) / snapshot.spot)
    return row


def query_stored_eligible_chain(
    request: EligibleChainQuery, *, rows: tuple[Mapping, ...], coverage: tuple[Mapping, ...],
    underlyers: tuple[str, ...], filter_values: Mapping[str, Mapping] | None = None,
) -> dict:
    request = EligibleChainQuery.model_validate(request.model_dump())
    requested = [request.underlyer] if request.underlyer else list(underlyers)
    if any(symbol not in underlyers for symbol in requested):
        return dict(available=False, reason="UNDERLYING_NOT_CONFIGURED",
            as_of=None, observed_at=None, data=dict(rows=[], total=0, coverage=[]))
    retained = [deepcopy(dict(row)) for row in coverage if row["underlying"] in requested
        and (request.session_date is None or datetime.fromisoformat(row["market_time"])
            .astimezone(ZoneInfo("America/New_York")).date() == request.session_date)]
    covered = {row["underlying"] for row in retained}

    def numeric_value(row, field):
        exact = (filter_values or {}).get(str(row.get("snapshot_id")), {})
        value = exact.get(field, row[field])
        return Decimal(str(value)) if value is not None else None

    filtered = []
    for stored in rows:
        if stored["underlying"] not in covered or request.contract_type and stored["contract_type"] != request.contract_type:
            continue
        matches = True
        for condition in request.filters:
            value = numeric_value(stored, condition.field)
            if value is None:
                matches = False
                break
            minimum = Decimal(str(condition.minimum)) if condition.minimum is not None else None
            maximum = Decimal(str(condition.maximum)) if condition.maximum is not None else None
            if (minimum is not None and value < minimum) or (maximum is not None and value > maximum):
                matches = False
                break
        if matches:
            filtered.append(deepcopy(dict(stored)))
    filtered.sort(key=lambda row: (row["underlying"], row["expiration_date"], Decimal(str(row["strike"])),
        row["contract_type"], row["contract_id"]))
    nonnull = [row for row in filtered if numeric_value(row, request.sort) is not None]
    nulls = [row for row in filtered if numeric_value(row, request.sort) is None]
    nonnull.sort(key=lambda row: numeric_value(row, request.sort), reverse=request.descending)
    ordered = nonnull + nulls
    missing = [symbol for symbol in requested if symbol not in covered]
    return dict(available=bool(retained), reason=None if retained else "NO_ACTIVE_POLICY_ELIGIBLE_CHAIN",
        as_of=max((row["market_time"] for row in retained), default=None),
        observed_at=max((row["observed_time"] for row in retained), default=None),
        data=dict(rows=ordered[request.offset:request.offset + request.limit], total=len(filtered), coverage=retained,
            requested_underlyers=requested, missing_underlyers=missing,
            coverage_status="MISSING" if not retained else "PARTIAL" if missing else "COVERED",
            selection_basis="LATEST_COMPLETE_MATRIX_PER_UNDERLYING", serving_mode="CURRENT_POLICY",
            session_date=request.session_date.isoformat() if request.session_date else None,
            limit=request.limit, offset=request.offset, filters=[item.model_dump() for item in request.filters],
            quote_liquidity="NOT_AVAILABLE", execution_mode="READ_ONLY_RESEARCH"))