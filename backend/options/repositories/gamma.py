from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from options.analytics.gamma_exposure import (
    GammaFlipPoint,
    GammaProfile,
    ScopedGammaProfile,
    StrikeGammaExposure,
)
from options.domain import (
    DealerConvention,
    GammaRegime,
    GammaScope,
    VolatilityAssumption,
)

from .base import PostgresRepository


SQL_INSERT_PROFILE = """
INSERT INTO option_gamma_profiles (
    gamma_profile_id, matrix_id, underlying, scope,
    market_data_time, first_observed_at, spot,
    dealer_convention, volatility_assumption, shares_per_contract,
    gamma_policy_version, gamma_policy_sha256,
    net_gamma_shares_per_point, net_gamma_notional_per_percent,
    absolute_gamma_notional_per_percent,
    call_gamma_notional_per_percent, put_gamma_notional_per_percent,
    flip_spot, regime_at_spot, sign_change_count,
    flip_search_low_spot, flip_search_high_spot, flip_grid_points, flip_reasons,
    peak_gamma_strike, strike_count, contributing_contract_count,
    eligible_contract_count, coverage_fraction,
    strike_profile, quality_reasons
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s,
    %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s
)
ON CONFLICT (matrix_id, scope, gamma_policy_sha256) DO NOTHING
"""

# Portal read. Served by idx_option_gamma_profiles_latest; excludes the JSONB
# curve so the common board query never touches TOAST.
SQL_LATEST_BY_UNDERLYING = """
SELECT DISTINCT ON (underlying, scope)
       gamma_profile_id, matrix_id, underlying, scope,
       market_data_time, first_observed_at, spot,
       dealer_convention, volatility_assumption,
       net_gamma_shares_per_point, net_gamma_notional_per_percent,
       absolute_gamma_notional_per_percent,
       call_gamma_notional_per_percent, put_gamma_notional_per_percent,
       flip_spot, regime_at_spot, sign_change_count,
       peak_gamma_strike, strike_count, contributing_contract_count,
       eligible_contract_count, coverage_fraction, quality_reasons
FROM option_gamma_profiles
WHERE gamma_policy_sha256 = %s
  AND scope = %s
  AND (%s::text IS NULL OR underlying = %s)
ORDER BY underlying, scope, market_data_time DESC
"""

# Chart read. The only query that intentionally loads the curve.
SQL_CURVE_BY_MATRIX = """
SELECT gamma_profile_id, matrix_id, underlying, scope, market_data_time,
       spot, dealer_convention, flip_spot, regime_at_spot,
       strike_count, strike_profile
FROM option_gamma_profiles
WHERE matrix_id = %s
  AND scope = %s
  AND gamma_policy_sha256 = %s
"""

# Replay read: every scope for one matrix, including the curve, so a persisted
# matrix can be rescanned without recomputing gamma.
SQL_PROFILES_BY_MATRIX = """
SELECT scope, spot, dealer_convention, volatility_assumption, shares_per_contract,
       net_gamma_shares_per_point, net_gamma_notional_per_percent,
       absolute_gamma_notional_per_percent,
       call_gamma_notional_per_percent, put_gamma_notional_per_percent,
       flip_spot, regime_at_spot, sign_change_count,
       flip_search_low_spot, flip_search_high_spot, flip_grid_points, flip_reasons,
       peak_gamma_strike, strike_count, contributing_contract_count,
       eligible_contract_count, coverage_fraction, strike_profile, quality_reasons
FROM option_gamma_profiles
WHERE matrix_id = %s
  AND gamma_policy_sha256 = %s
ORDER BY scope
"""

# Research cohort read. Served by idx_option_gamma_profiles_regime.
SQL_REGIME_HISTORY = """
SELECT underlying, scope, market_data_time, regime_at_spot,
       net_gamma_notional_per_percent, flip_spot, spot, coverage_fraction
FROM option_gamma_profiles
WHERE gamma_policy_sha256 = %s
  AND market_data_time >= %s
  AND market_data_time < %s
ORDER BY market_data_time DESC
LIMIT %s
"""

# Catalogued so a measurement script can EXPLAIN every read path by name
# without duplicating SQL. Keep write statements out of this mapping.
READ_QUERIES: Mapping[str, str] = {
    "latest_by_underlying": SQL_LATEST_BY_UNDERLYING,
    "curve_by_matrix": SQL_CURVE_BY_MATRIX,
    "profiles_by_matrix": SQL_PROFILES_BY_MATRIX,
    "regime_history": SQL_REGIME_HISTORY,
}


@dataclass(frozen=True, slots=True)
class GammaProfileRecord:
    matrix_id: UUID
    underlying: str
    scope: GammaScope
    market_data_time: datetime
    first_observed_at: datetime
    profile: GammaProfile
    gamma_policy_version: str
    gamma_policy_sha256: str

    @property
    def gamma_profile_id(self) -> UUID:
        return uuid5(
            NAMESPACE_URL,
            "option-gamma-profile:"
            f"{self.matrix_id}:{self.scope.value}:{self.gamma_policy_sha256}",
        )


class OptionGammaProfileRepository(PostgresRepository):
    def persist(self, records: Sequence[GammaProfileRecord]) -> int:
        if not records:
            return 0
        with self._cursor() as cursor:
            for record in records:
                cursor.execute(SQL_INSERT_PROFILE, _insert_parameters(record))
            return len(records)

    def latest_by_underlying(
        self,
        gamma_policy_sha256: str,
        *,
        scope: GammaScope = GammaScope.TOTAL,
        underlying: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        with self._cursor() as cursor:
            cursor.execute(
                SQL_LATEST_BY_UNDERLYING,
                (gamma_policy_sha256, scope.value, underlying, underlying),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def curve_by_matrix(
        self,
        matrix_id: UUID,
        gamma_policy_sha256: str,
        *,
        scope: GammaScope = GammaScope.TOTAL,
    ) -> dict[str, Any] | None:
        with self._cursor() as cursor:
            cursor.execute(
                SQL_CURVE_BY_MATRIX, (matrix_id, scope.value, gamma_policy_sha256)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def regime_history(
        self,
        gamma_policy_sha256: str,
        start: datetime,
        end: datetime,
        *,
        limit: int = 1000,
    ) -> tuple[dict[str, Any], ...]:
        with self._cursor() as cursor:
            cursor.execute(SQL_REGIME_HISTORY, (gamma_policy_sha256, start, end, limit))
            return tuple(dict(row) for row in cursor.fetchall())

    def profiles_by_matrix(
        self,
        matrix_id: UUID,
        gamma_policy_sha256: str,
    ) -> tuple[ScopedGammaProfile, ...]:
        with self._cursor() as cursor:
            cursor.execute(SQL_PROFILES_BY_MATRIX, (matrix_id, gamma_policy_sha256))
            return tuple(rehydrate_profile(dict(row)) for row in cursor.fetchall())

    def explain(
        self,
        query_name: str,
        parameters: tuple[Any, ...],
        *,
        analyze: bool = True,
    ) -> dict[str, Any]:
        """Return the planner output for a catalogued read query.

        ANALYZE executes the statement, so only catalogued reads are allowed.
        """
        if query_name not in READ_QUERIES:
            raise KeyError(f"unknown gamma read query: {query_name}")
        options = "ANALYZE, BUFFERS, FORMAT JSON" if analyze else "FORMAT JSON"
        statement = f"EXPLAIN ({options}) {READ_QUERIES[query_name]}"
        with self._cursor(dict_rows=False) as cursor:
            cursor.execute(statement, parameters)
            plan = cursor.fetchone()[0]
        if isinstance(plan, str):
            plan = json.loads(plan)
        return {"query_name": query_name, "plan": plan[0]}


def _insert_parameters(record: GammaProfileRecord) -> tuple[Any, ...]:
    profile = record.profile
    flip = profile.flip
    return (
        record.gamma_profile_id,
        record.matrix_id,
        record.underlying,
        record.scope.value,
        record.market_data_time,
        record.first_observed_at,
        profile.spot,
        profile.convention.value,
        profile.volatility_assumption.value,
        profile.shares_per_contract,
        record.gamma_policy_version,
        record.gamma_policy_sha256,
        profile.net_gamma_shares_per_point,
        profile.net_gamma_notional_per_percent,
        profile.absolute_gamma_notional_per_percent,
        profile.call_gamma_notional_per_percent,
        profile.put_gamma_notional_per_percent,
        flip.flip_spot,
        flip.regime_at_spot.value,
        flip.sign_change_count,
        flip.searched_low_spot,
        flip.searched_high_spot,
        flip.grid_points,
        list(flip.reasons),
        profile.peak_gamma_strike,
        len(profile.strikes),
        profile.contributing_contract_count,
        profile.eligible_contract_count,
        profile.coverage_fraction,
        Json(curve_payload(profile.strikes)),
        list(profile.quality_reasons),
    )


def curve_payload(
    strikes: Sequence[StrikeGammaExposure],
) -> list[dict[str, Any]]:
    """Serialize the per-strike curve, keeping strikes exact as strings."""
    return [
        {
            "strike": _decimal_text(row.strike),
            "call_open_interest": row.call_open_interest,
            "put_open_interest": row.put_open_interest,
            "call_contract_count": row.call_contract_count,
            "put_contract_count": row.put_contract_count,
            "call_gamma_shares_per_point": row.call_gamma_shares_per_point,
            "put_gamma_shares_per_point": row.put_gamma_shares_per_point,
            "call_gamma_notional_per_percent": row.call_gamma_notional_per_percent,
            "put_gamma_notional_per_percent": row.put_gamma_notional_per_percent,
        }
        for row in strikes
    ]


def rehydrate_profile(row: Mapping[str, Any]) -> ScopedGammaProfile:
    """Rebuild a scoped profile from a persisted row so replay never recomputes gamma."""
    curve = row["strike_profile"]
    if isinstance(curve, str):
        curve = json.loads(curve)
    strikes = tuple(
        StrikeGammaExposure(
            strike=Decimal(str(item["strike"])),
            call_open_interest=int(item["call_open_interest"]),
            put_open_interest=int(item["put_open_interest"]),
            call_contract_count=int(item["call_contract_count"]),
            put_contract_count=int(item["put_contract_count"]),
            call_gamma_shares_per_point=float(item["call_gamma_shares_per_point"]),
            put_gamma_shares_per_point=float(item["put_gamma_shares_per_point"]),
            call_gamma_notional_per_percent=float(
                item["call_gamma_notional_per_percent"]
            ),
            put_gamma_notional_per_percent=float(
                item["put_gamma_notional_per_percent"]
            ),
        )
        for item in curve
    )
    flip = GammaFlipPoint(
        flip_spot=row["flip_spot"],
        regime_at_spot=GammaRegime(row["regime_at_spot"]),
        searched_low_spot=row["flip_search_low_spot"],
        searched_high_spot=row["flip_search_high_spot"],
        grid_points=int(row["flip_grid_points"]),
        sign_change_count=int(row["sign_change_count"]),
        reasons=tuple(row["flip_reasons"] or ()),
    )
    profile = GammaProfile(
        spot=row["spot"],
        convention=DealerConvention(row["dealer_convention"]),
        volatility_assumption=VolatilityAssumption(row["volatility_assumption"]),
        shares_per_contract=int(row["shares_per_contract"]),
        strikes=strikes,
        net_gamma_shares_per_point=float(row["net_gamma_shares_per_point"]),
        net_gamma_notional_per_percent=float(row["net_gamma_notional_per_percent"]),
        absolute_gamma_notional_per_percent=float(
            row["absolute_gamma_notional_per_percent"]
        ),
        call_gamma_notional_per_percent=float(row["call_gamma_notional_per_percent"]),
        put_gamma_notional_per_percent=float(row["put_gamma_notional_per_percent"]),
        contributing_contract_count=int(row["contributing_contract_count"]),
        eligible_contract_count=int(row["eligible_contract_count"]),
        coverage_fraction=float(row["coverage_fraction"]),
        peak_gamma_strike=row["peak_gamma_strike"],
        flip=flip,
        quality_reasons=tuple(row["quality_reasons"] or ()),
    )
    return ScopedGammaProfile(scope=GammaScope(row["scope"]), profile=profile)


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")
