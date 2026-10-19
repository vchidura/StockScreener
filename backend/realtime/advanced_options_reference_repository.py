"""PostgreSQL adapter for immutable Advanced Options Tier A reference state."""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping
from uuid import UUID

from psycopg2.extras import Json

from .advanced_options_reference_state import (
    AdvancedOptionsAdjustedDailyBarIdentity,
    AdvancedOptionsCorporateActionIdentity,
    AdvancedOptionsDividendCoverageReference,
    AdvancedOptionsEquityMetric,
    AdvancedOptionsHistoricalVolatilityFact,
    AdvancedOptionsOpenInterestReference,
    AdvancedOptionsRateReference,
    AdvancedOptionsReferenceState,
    AdvancedOptionsReferenceStateRepository,
    AdvancedOptionsValuationReference,
    EquityMetricKind,
    ReferenceAvailability,
)
from .contracts import canonical_json
from .database_access_v2 import V2ConnectionProvider


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _date(value: str) -> date:
    return date.fromisoformat(value)


def _bar(value: Mapping[str, object]) -> AdvancedOptionsAdjustedDailyBarIdentity:
    return AdvancedOptionsAdjustedDailyBarIdentity(
        bar_revision_id=UUID(value["bar_revision_id"]),
        security_id=UUID(value["security_id"]),
        ticker=value["ticker"],
        session_date=_date(value["session_date"]),
        adjusted_close=Decimal(value["adjusted_close"]),
        market_close_at=_datetime(value["market_close_at"]),
        known_at=_datetime(value["known_at"]),
        adjustment_lineage_sha256=value["adjustment_lineage_sha256"],
        finality_sha256=value["finality_sha256"],
        payload_sha256=value["payload_sha256"],
        finalized=value["finalized"],
    )


def _open_interest(
    value: Mapping[str, object],
) -> AdvancedOptionsOpenInterestReference:
    return AdvancedOptionsOpenInterestReference(
        contract_id=value["contract_id"],
        contract_ticker=value["contract_ticker"],
        underlyer=value["underlyer"],
        availability=ReferenceAvailability(value["availability"]),
        known_at=_datetime(value["known_at"]),
        baseline_id=(
            None
            if value["baseline_id"] is None
            else UUID(value["baseline_id"])
        ),
        settlement_session=(
            None
            if value["settlement_session"] is None
            else _date(value["settlement_session"])
        ),
        observed_session=(
            None
            if value["observed_session"] is None
            else _date(value["observed_session"])
        ),
        open_interest=value["open_interest"],
        source=value["source"],
        date_basis=value["date_basis"],
        observed_at=(
            None
            if value["observed_at"] is None
            else _datetime(value["observed_at"])
        ),
        received_at=(
            None
            if value["received_at"] is None
            else _datetime(value["received_at"])
        ),
        payload_sha256=value["payload_sha256"],
        supersedes_baseline_id=(
            None
            if value["supersedes_baseline_id"] is None
            else UUID(value["supersedes_baseline_id"])
        ),
        reason_codes=tuple(value["reason_codes"]),
    )


def _volatility(
    value: Mapping[str, object],
) -> AdvancedOptionsHistoricalVolatilityFact:
    return AdvancedOptionsHistoricalVolatilityFact(
        security_id=UUID(value["security_id"]),
        ticker=value["ticker"],
        window_sessions=value["window_sessions"],
        annualized_volatility=Decimal(value["annualized_volatility"]),
        estimator=value["estimator"],
        estimator_version=value["estimator_version"],
        market_cutoff=_datetime(value["market_cutoff"]),
        known_at=_datetime(value["known_at"]),
        payload_sha256=value["payload_sha256"],
        bars=tuple(_bar(row) for row in value["bars"]),
    )


def _rate(value: Mapping[str, object]) -> AdvancedOptionsRateReference:
    return AdvancedOptionsRateReference(
        observation_id=UUID(value["observation_id"]),
        rate_date=_date(value["rate_date"]),
        tenor_days=value["tenor_days"],
        annual_rate=Decimal(value["annual_rate"]),
        source=value["source"],
        source_key=value["source_key"],
        observed_at=_datetime(value["observed_at"]),
        received_at=_datetime(value["received_at"]),
        payload_sha256=value["payload_sha256"],
    )


def _action(
    value: Mapping[str, object],
) -> AdvancedOptionsCorporateActionIdentity:
    return AdvancedOptionsCorporateActionIdentity(
        action_id=UUID(value["action_id"]),
        payload_sha256=value["payload_sha256"],
        observed_at=_datetime(value["observed_at"]),
        received_at=_datetime(value["received_at"]),
    )


def _dividend(
    value: Mapping[str, object],
) -> AdvancedOptionsDividendCoverageReference:
    return AdvancedOptionsDividendCoverageReference(
        coverage_id=UUID(value["coverage_id"]),
        security_id=UUID(value["security_id"]),
        ticker=value["ticker"],
        window_start=_date(value["window_start"]),
        window_end=_date(value["window_end"]),
        status=value["status"],
        observed_at=_datetime(value["observed_at"]),
        received_at=_datetime(value["received_at"]),
        payload_sha256=value["payload_sha256"],
        response_sha256=value["response_sha256"],
        actions=tuple(_action(row) for row in value["actions"]),
    )


def _valuation(
    value: Mapping[str, object],
) -> AdvancedOptionsValuationReference:
    return AdvancedOptionsValuationReference(
        security_id=UUID(value["security_id"]),
        ticker=value["ticker"],
        valuation_date=_date(value["valuation_date"]),
        expiration_date=_date(value["expiration_date"]),
        risk_free_rate=Decimal(value["risk_free_rate"]),
        dividend_yield=Decimal(value["dividend_yield"]),
        rate_observations=tuple(
            _rate(row) for row in value["rate_observations"]
        ),
        dividend_coverage=_dividend(value["dividend_coverage"]),
        model_version=value["model_version"],
        model_bundle_sha256=value["model_bundle_sha256"],
        valuation_policy_sha256=value["valuation_policy_sha256"],
        known_at=_datetime(value["known_at"]),
    )


def _metric(value: Mapping[str, object]) -> AdvancedOptionsEquityMetric:
    return AdvancedOptionsEquityMetric(
        security_id=UUID(value["security_id"]),
        ticker=value["ticker"],
        metric_kind=EquityMetricKind(value["metric_kind"]),
        as_of_session=_date(value["as_of_session"]),
        availability=ReferenceAvailability(value["availability"]),
        known_at=_datetime(value["known_at"]),
        value=(
            None if value["value"] is None else Decimal(value["value"])
        ),
        source=value["source"],
        source_key=value["source_key"],
        observed_at=(
            None
            if value["observed_at"] is None
            else _datetime(value["observed_at"])
        ),
        received_at=(
            None
            if value["received_at"] is None
            else _datetime(value["received_at"])
        ),
        payload_sha256=value["payload_sha256"],
        calculation_version=value["calculation_version"],
        member_sessions=tuple(_date(row) for row in value["member_sessions"]),
        member_sha256=value["member_sha256"],
        reason_codes=tuple(value["reason_codes"]),
    )


def decode_advanced_options_reference_state(
    *,
    state_id: UUID,
    state_sha256: str,
    document: Mapping[str, object],
) -> AdvancedOptionsReferenceState:
    if document.get("contract") != "advanced-options-reference-state-v1":
        raise ValueError("stored reference state version changed")
    return AdvancedOptionsReferenceState(
        state_id=state_id,
        state_sha256=state_sha256,
        version=document["contract"],
        reference_generation=document["reference_generation"],
        session_date=_date(document["session_date"]),
        known_at=_datetime(document["known_at"]),
        valid_until=_datetime(document["valid_until"]),
        reference_policy_sha256=document["reference_policy_sha256"],
        contract_scope_sha256=document["contract_scope_sha256"],
        underlyings=tuple(document["underlyings"]),
        open_interest=tuple(
            _open_interest(row) for row in document["open_interest"]
        ),
        historical_volatility=tuple(
            _volatility(row)
            for row in document["historical_volatility"]
        ),
        valuation_inputs=tuple(
            _valuation(row) for row in document["valuation_inputs"]
        ),
        equity_metrics=tuple(
            _metric(row) for row in document["equity_metrics"]
        ),
    )


def _member_rows(
    state: AdvancedOptionsReferenceState,
) -> tuple[tuple[str, str, str, str, dict[str, object]], ...]:
    rows = []
    for row in state.open_interest:
        rows.append((
            "OPEN_INTEREST",
            str(row.contract_id),
            row.sha256,
            row.availability.value,
            json.loads(canonical_json(row)),
        ))
    for row in state.historical_volatility:
        rows.append((
            "HISTORICAL_VOLATILITY",
            f"{row.ticker}:HV{row.window_sessions}",
            row.sha256,
            ReferenceAvailability.AVAILABLE.value,
            json.loads(canonical_json(row)),
        ))
    for row in state.valuation_inputs:
        rows.append((
            "VALUATION_INPUT",
            f"{row.ticker}:{row.expiration_date.isoformat()}",
            row.sha256,
            ReferenceAvailability.AVAILABLE.value,
            json.loads(canonical_json(row)),
        ))
    for row in state.equity_metrics:
        rows.append((
            "EQUITY_METRIC",
            f"{row.ticker}:{row.metric_kind.value}",
            row.sha256,
            row.availability.value,
            json.loads(canonical_json(row)),
        ))
    return tuple(sorted(rows))


class PostgresAdvancedOptionsReferenceStateRepository(
    AdvancedOptionsReferenceStateRepository
):
    def __init__(self, provider: V2ConnectionProvider) -> None:
        self._provider = provider

    def put(
        self,
        state: AdvancedOptionsReferenceState,
    ) -> AdvancedOptionsReferenceState:
        if not isinstance(state, AdvancedOptionsReferenceState):
            raise TypeError("reference state is invalid")
        document = json.loads(canonical_json(state.identity))
        members = _member_rows(state)
        with self._provider.transaction() as unit:
            cursor = unit.cursor
            cursor.execute(
                "INSERT INTO rt.advanced_options_reference_states ("
                "state_id,state_sha256,state_version,reference_generation,"
                "session_date,known_at,valid_until,reference_policy_sha256,"
                "contract_scope_sha256,underlyings,member_count,state_document"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (
                    str(state.state_id),
                    state.state_sha256,
                    state.version,
                    state.reference_generation,
                    state.session_date,
                    state.known_at,
                    state.valid_until,
                    state.reference_policy_sha256,
                    state.contract_scope_sha256,
                    list(state.underlyings),
                    len(members),
                    Json(document),
                ),
            )
            cursor.execute(
                "SELECT state_id,state_sha256,state_document,member_count "
                "FROM rt.advanced_options_reference_states "
                "WHERE reference_generation=%s",
                (state.reference_generation,),
            )
            stored = cursor.fetchone()
            if (
                stored is None
                or UUID(str(stored[0])) != state.state_id
                or str(stored[1]) != state.state_sha256
                or canonical_json(stored[2]) != canonical_json(document)
                or int(stored[3]) != len(members)
            ):
                raise ValueError("persisted reference state conflicts")
            cursor.executemany(
                "INSERT INTO rt.advanced_options_reference_state_members ("
                "state_id,member_kind,member_key,member_sha256,availability,"
                "member_document) VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                tuple(
                    (
                        str(state.state_id),
                        kind,
                        key,
                        digest,
                        availability,
                        Json(member_document),
                    )
                    for kind, key, digest, availability, member_document
                    in members
                ),
            )
            cursor.execute(
                "SELECT member_kind,member_key,member_sha256,availability,"
                "member_document FROM "
                "rt.advanced_options_reference_state_members "
                "WHERE state_id=%s ORDER BY member_kind,member_key",
                (str(state.state_id),),
            )
            stored_members = tuple(
                (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]),
                    row[4],
                )
                for row in cursor.fetchall()
            )
            if len(stored_members) != len(members) or any(
                actual[:4] != expected[:4]
                or canonical_json(actual[4])
                != canonical_json(expected[4])
                for actual, expected in zip(stored_members, members)
            ):
                raise ValueError("persisted reference state members conflict")
        return state

    @staticmethod
    def _decode(row) -> AdvancedOptionsReferenceState | None:
        if row is None:
            return None
        return decode_advanced_options_reference_state(
            state_id=UUID(str(row[0])),
            state_sha256=str(row[1]),
            document=row[2],
        )

    def load_generation(
        self,
        reference_generation: int,
    ) -> AdvancedOptionsReferenceState | None:
        with self._provider.transaction(read_only=True) as unit:
            unit.cursor.execute(
                "SELECT state_id,state_sha256,state_document "
                "FROM rt.advanced_options_reference_states "
                "WHERE reference_generation=%s",
                (reference_generation,),
            )
            return self._decode(unit.cursor.fetchone())

    def load_for_session(
        self,
        session_date: date,
        *,
        known_at: datetime,
    ) -> AdvancedOptionsReferenceState | None:
        with self._provider.transaction(read_only=True) as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT state.state_id,state.state_sha256,"
                "state.state_document FROM "
                "rt.advanced_options_reference_session_freezes "
                "AS session_freeze "
                "JOIN rt.advanced_options_reference_states AS state "
                "USING(state_id) WHERE session_freeze.session_date=%s "
                "AND state.known_at<=%s AND state.valid_until>%s",
                (session_date, known_at, known_at),
            )
            row = cursor.fetchone()
            if row is None:
                cursor.execute(
                    "SELECT state_id,state_sha256,state_document FROM "
                    "rt.advanced_options_reference_states "
                    "WHERE session_date=%s AND known_at<=%s "
                    "AND valid_until>%s ORDER BY reference_generation DESC "
                    "LIMIT 1",
                    (session_date, known_at, known_at),
                )
                row = cursor.fetchone()
            return self._decode(row)

    def freeze_for_cycle(
        self,
        state: AdvancedOptionsReferenceState,
        *,
        cycle_id: UUID,
        consumed_at: datetime,
    ) -> None:
        with self._provider.transaction() as unit:
            cursor = unit.cursor
            cursor.execute(
                "SELECT state_id,state_sha256 FROM "
                "rt.advanced_options_reference_states "
                "WHERE reference_generation=%s AND known_at<=%s "
                "AND valid_until>%s FOR SHARE",
                (state.reference_generation, consumed_at, consumed_at),
            )
            stored = cursor.fetchone()
            if (
                stored is None
                or UUID(str(stored[0])) != state.state_id
                or str(stored[1]) != state.state_sha256
            ):
                raise ValueError("reference state cannot be consumed")
            cursor.execute(
                "INSERT INTO rt.advanced_options_reference_session_freezes ("
                "session_date,state_id,first_cycle_id,consumed_at"
                ") VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    state.session_date,
                    str(state.state_id),
                    str(cycle_id),
                    consumed_at,
                ),
            )
            cursor.execute(
                "SELECT state_id FROM "
                "rt.advanced_options_reference_session_freezes "
                "WHERE session_date=%s",
                (state.session_date,),
            )
            frozen = cursor.fetchone()
            if frozen is None or UUID(str(frozen[0])) != state.state_id:
                raise ValueError("consumed session reference state is frozen")
            cursor.execute(
                "INSERT INTO rt.advanced_options_reference_cycle_bindings ("
                "cycle_id,session_date,state_id,consumed_at"
                ") VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    str(cycle_id),
                    state.session_date,
                    str(state.state_id),
                    consumed_at,
                ),
            )
            cursor.execute(
                "SELECT state_id FROM "
                "rt.advanced_options_reference_cycle_bindings "
                "WHERE cycle_id=%s",
                (str(cycle_id),),
            )
            bound = cursor.fetchone()
            if bound is None or UUID(str(bound[0])) != state.state_id:
                raise ValueError("cycle reference state is immutable")
