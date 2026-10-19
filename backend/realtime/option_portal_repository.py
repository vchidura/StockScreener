from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import nullcontext
from uuid import NAMESPACE_URL, UUID, uuid5

from options.domain import ContractType
from options.strategies.domain import OptionSide

from .option_portal_adapter import (
    RealtimeModelMarkEvidence,
    RealtimeOptionPackage,
    RealtimeOptionPackageLeg,
    RealtimeOptionPackageMarkResult,
    RealtimePackageMarkStatus,
    RealtimeQuoteEvidence,
)
from .preflight import ProviderTier


def _database_identity(row) -> tuple[object, ...] | None:
    if row is None:
        return None
    return tuple(str(value) if isinstance(value, UUID) else value for value in row)


def _package_policy_sha256(package: RealtimeOptionPackage) -> str:
    policies = {leg.valuation_policy_sha256 for leg in package.legs}
    if len(policies) != 1:
        raise ValueError("option package legs must bind one valuation policy")
    return next(iter(policies))


def _persist_package(cursor, package: RealtimeOptionPackage) -> None:
    policy_sha256 = _package_policy_sha256(package)
    cursor.execute(
        "INSERT INTO rt.option_packages ("
        "candidate_id, cohort_id, event_id, matrix_id, candidate_identity_sha256, "
        "provider_tier, "
        "entry_market_time, entry_net_premium, capital_at_risk, exit_deadline, "
        "valuation_policy_sha256, leg_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s) ON CONFLICT (candidate_id) DO NOTHING",
        (
            str(package.candidate_id),
            str(package.cohort_id),
            str(package.event_id) if package.event_id is not None else None,
            str(package.matrix_id),
            package.candidate_identity_sha256,
            package.provider_tier.value,
            package.entry_market_time,
            package.entry_net_premium,
            package.capital_at_risk,
            package.exit_deadline,
            policy_sha256,
            len(package.legs),
        ),
    )
    cursor.execute(
        "SELECT cohort_id, event_id, matrix_id, candidate_identity_sha256, provider_tier, "
        "entry_market_time, entry_net_premium, capital_at_risk, exit_deadline, "
        "valuation_policy_sha256, leg_count FROM rt.option_packages WHERE candidate_id=%s",
        (str(package.candidate_id),),
    )
    expected = (
        str(package.cohort_id),
        str(package.event_id) if package.event_id is not None else None,
        str(package.matrix_id),
        package.candidate_identity_sha256,
        package.provider_tier.value,
        package.entry_market_time,
        package.entry_net_premium,
        package.capital_at_risk,
        package.exit_deadline,
        policy_sha256,
        len(package.legs),
    )
    if _database_identity(cursor.fetchone()) != expected:
        raise ValueError("persisted option package identity conflict")
    for leg in package.legs:
        cursor.execute(
            "INSERT INTO rt.option_package_legs ("
            "candidate_id, leg_index, instrument_id, contract_id, catalog_version_id, "
            "contract_ticker, side, ratio, multiplier, strike, expiration_date, "
            "contract_type, entry_mark, entry_market_time, valuation_policy_sha256"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (candidate_id, leg_index) DO NOTHING",
            (
                str(package.candidate_id),
                leg.leg_index,
                leg.instrument_id,
                leg.contract_id,
                leg.catalog_version_id,
                leg.contract_ticker,
                leg.side.value,
                leg.ratio,
                leg.multiplier,
                leg.strike,
                leg.expiration_date,
                leg.contract_type.value,
                leg.entry_mark,
                leg.entry_market_time,
                leg.valuation_policy_sha256,
            ),
        )
        cursor.execute(
            "SELECT instrument_id, contract_id, catalog_version_id, contract_ticker, "
            "side, ratio, multiplier, strike, expiration_date, contract_type, entry_mark, "
            "entry_market_time, valuation_policy_sha256 FROM rt.option_package_legs "
            "WHERE candidate_id=%s AND leg_index=%s",
            (str(package.candidate_id), leg.leg_index),
        )
        expected_leg = (
            leg.instrument_id,
            leg.contract_id,
            leg.catalog_version_id,
            leg.contract_ticker,
            leg.side.value,
            leg.ratio,
            leg.multiplier,
            leg.strike,
            leg.expiration_date,
            leg.contract_type.value,
            leg.entry_mark,
            leg.entry_market_time,
            leg.valuation_policy_sha256,
        )
        if _database_identity(cursor.fetchone()) != expected_leg:
            raise ValueError("persisted option package leg identity conflict")


def persist_realtime_option_package_cursor(
    cursor,
    package: RealtimeOptionPackage,
) -> None:
    _persist_package(cursor, package)


def _persist_realtime_option_package_mark(
    connection,
    result: RealtimeOptionPackageMarkResult,
    *,
    repository_cursor,
) -> UUID:
    if (connection is None) == (repository_cursor is None):
        raise ValueError("option mark persistence requires one transaction owner")
    if result.status is not RealtimePackageMarkStatus.READY or result.mark is None:
        raise ValueError("only a ready realtime option package mark can be persisted")
    mark = result.mark
    package = mark.package
    outcome = mark.outcome
    mark_id = uuid5(
        NAMESPACE_URL,
        f"realtime-option-package-mark:{package.candidate_id}:"
        f"{outcome.valuation_policy_version}:{outcome.valuation_policy_sha256}:"
        f"{mark.adapter_version}:{mark.evidence_batch_id}",
    )
    try:
        with (
            connection.cursor()
            if connection is not None
            else nullcontext(repository_cursor)
        ) as cursor:
            _persist_package(cursor, package)
            evidence_by_instrument = {}
            for evidence in mark.mark_evidence:
                evidence_by_instrument[evidence.instrument_id] = evidence
                advanced = isinstance(evidence, RealtimeQuoteEvidence)
                if not advanced and not isinstance(evidence, RealtimeModelMarkEvidence):
                    raise TypeError("unsupported realtime option mark evidence")
                cursor.execute(
                    "INSERT INTO rt.option_mark_evidence ("
                    "evidence_id, instrument_id, contract_id, catalog_version_id, "
                    "provider_tier, source_kind, mark_source, source_batch_id, "
                    "source_revision_id, journal_generation, stream_session_id, source_offset, "
                    "market_time, observed_time, bid, ask, mark, payload_sha256) VALUES ("
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                    "%s, %s, %s) "
                    "ON CONFLICT (evidence_id) DO NOTHING",
                    (
                        str(evidence.evidence_id),
                        evidence.instrument_id,
                        evidence.contract_id,
                        evidence.catalog_version_id,
                        package.provider_tier.value,
                        "ADVANCED_NBBO" if advanced else "DEVELOPER_ALIGNED_AGGREGATE",
                        evidence.mark_source.value,
                        None if advanced else str(evidence.source_batch_id),
                        None if advanced else str(evidence.source_revision_id),
                        str(evidence.journal_generation) if advanced else None,
                        str(evidence.stream_session_id) if advanced else None,
                        evidence.source_offset if advanced else None,
                        evidence.market_time,
                        evidence.observed_time,
                        evidence.bid if advanced else None,
                        evidence.ask if advanced else None,
                        evidence.mark,
                        evidence.payload_sha256,
                    ),
                )
                cursor.execute(
                    "SELECT instrument_id, contract_id, catalog_version_id, "
                    "provider_tier, source_kind, mark_source, source_batch_id, "
                    "source_revision_id, journal_generation, stream_session_id, source_offset, "
                    "market_time, observed_time, bid, ask, mark, payload_sha256 "
                    "FROM rt.option_mark_evidence WHERE evidence_id=%s",
                    (str(evidence.evidence_id),),
                )
                expected = (
                    evidence.instrument_id,
                    evidence.contract_id,
                    evidence.catalog_version_id,
                    package.provider_tier.value,
                    "ADVANCED_NBBO" if advanced else "DEVELOPER_ALIGNED_AGGREGATE",
                    evidence.mark_source.value,
                    None if advanced else str(evidence.source_batch_id),
                    None if advanced else str(evidence.source_revision_id),
                    str(evidence.journal_generation) if advanced else None,
                    str(evidence.stream_session_id) if advanced else None,
                    evidence.source_offset if advanced else None,
                    evidence.market_time,
                    evidence.observed_time,
                    evidence.bid if advanced else None,
                    evidence.ask if advanced else None,
                    evidence.mark,
                    evidence.payload_sha256,
                )
                if _database_identity(cursor.fetchone()) != expected:
                    raise ValueError("persisted option quote evidence identity conflict")
            cursor.execute(
                "INSERT INTO rt.option_package_mark_revisions ("
                "mark_id, candidate_id, event_id, provider_tier, valuation_policy_version, "
                "valuation_policy_sha256, adapter_version, computed_at, market_time, "
                "observed_time, entry_net_premium, exit_net_premium, gross_pnl, "
                "estimated_cost, net_pnl, capital_at_risk, net_return, availability_flag, "
                "quality_flags, evidence_batch_id, evidence_count, management_status"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (mark_id) DO NOTHING",
                (
                    str(mark_id),
                    str(package.candidate_id),
                    str(package.event_id) if package.event_id is not None else None,
                    package.provider_tier.value,
                    outcome.valuation_policy_version,
                    outcome.valuation_policy_sha256,
                    mark.adapter_version,
                    mark.computed_at,
                    outcome.market_time,
                    outcome.observed_time,
                    outcome.entry_net_premium,
                    outcome.exit_net_premium,
                    outcome.gross_pnl,
                    outcome.estimated_cost,
                    outcome.net_pnl,
                    outcome.capital_at_risk,
                    outcome.net_return,
                    outcome.availability_flag,
                    list(outcome.quality_flags),
                    str(mark.evidence_batch_id),
                    len(mark.mark_evidence),
                    None,
                ),
            )
            cursor.execute(
                "SELECT candidate_id, event_id, provider_tier, valuation_policy_version, "
                "valuation_policy_sha256, adapter_version, computed_at, market_time, "
                "observed_time, entry_net_premium, exit_net_premium, gross_pnl, "
                "estimated_cost, net_pnl, capital_at_risk, net_return, availability_flag, "
                "quality_flags, evidence_batch_id, evidence_count, management_status "
                "FROM rt.option_package_mark_revisions WHERE mark_id=%s",
                (str(mark_id),),
            )
            expected_revision = (
                str(package.candidate_id),
                str(package.event_id) if package.event_id is not None else None,
                package.provider_tier.value,
                outcome.valuation_policy_version,
                outcome.valuation_policy_sha256,
                mark.adapter_version,
                mark.computed_at,
                outcome.market_time,
                outcome.observed_time,
                outcome.entry_net_premium,
                outcome.exit_net_premium,
                outcome.gross_pnl,
                outcome.estimated_cost,
                outcome.net_pnl,
                outcome.capital_at_risk,
                outcome.net_return,
                outcome.availability_flag,
                list(outcome.quality_flags),
                str(mark.evidence_batch_id),
                len(mark.mark_evidence),
                None,
            )
            if _database_identity(cursor.fetchone()) != expected_revision:
                raise ValueError("persisted option package mark identity conflict")
            for leg in package.legs:
                evidence = evidence_by_instrument[leg.instrument_id]
                cursor.execute(
                    "INSERT INTO rt.option_package_mark_inputs (mark_id, leg_index, evidence_id) "
                    "VALUES (%s, %s, %s) ON CONFLICT (mark_id, leg_index) DO NOTHING",
                    (str(mark_id), leg.leg_index, str(evidence.evidence_id)),
                )
                cursor.execute(
                    "SELECT evidence_id FROM rt.option_package_mark_inputs "
                    "WHERE mark_id=%s AND leg_index=%s",
                    (str(mark_id), leg.leg_index),
                )
                input_row = cursor.fetchone()
                if input_row is None or str(input_row[0]) != str(evidence.evidence_id):
                    raise ValueError("persisted option package mark input conflict")
            cursor.execute(
                "SELECT mark_id, market_time, observed_time FROM rt.option_package_mark_current "
                "WHERE candidate_id=%s AND valuation_policy_sha256=%s FOR UPDATE",
                (str(package.candidate_id), outcome.valuation_policy_sha256),
            )
            current = cursor.fetchone()
            current_identity = _database_identity(current)
            coordinate = (outcome.market_time, outcome.observed_time)
            if current_identity is not None:
                current_coordinate = (current_identity[1], current_identity[2])
                if current_coordinate == coordinate and current_identity[0] != str(mark_id):
                    raise ValueError("option package mark coordinate has conflicting evidence")
            cursor.execute(
                "INSERT INTO rt.option_package_mark_current ("
                "candidate_id, valuation_policy_sha256, mark_id, market_time, observed_time"
                ") VALUES (%s, %s, %s, %s, %s) ON CONFLICT (candidate_id, "
                "valuation_policy_sha256) DO UPDATE SET mark_id=EXCLUDED.mark_id, "
                "market_time=EXCLUDED.market_time, observed_time=EXCLUDED.observed_time, "
                "updated_at=clock_timestamp() WHERE "
                "rt.option_package_mark_current.market_time < EXCLUDED.market_time OR ("
                "rt.option_package_mark_current.market_time = EXCLUDED.market_time AND "
                "rt.option_package_mark_current.observed_time < EXCLUDED.observed_time) "
                "RETURNING mark_id",
                (
                    str(package.candidate_id),
                    outcome.valuation_policy_sha256,
                    str(mark_id),
                    outcome.market_time,
                    outcome.observed_time,
                ),
            )
            persisted = cursor.fetchone()
            if persisted is None and not (
                current_identity is not None and current_identity[0] == str(mark_id)
            ):
                if current_identity is None or (current_identity[1], current_identity[2]) < coordinate:
                    raise ValueError("option package current mark advancement was rejected")
        if connection is not None:
            connection.commit()
        return mark_id
    except Exception:
        if connection is not None:
            connection.rollback()
        raise


def persist_realtime_option_package_mark_cursor(
    cursor,
    result: RealtimeOptionPackageMarkResult,
) -> UUID:
    return _persist_realtime_option_package_mark(
        None,
        result,
        repository_cursor=cursor,
    )


def persist_realtime_option_package_mark(
    connection,
    result: RealtimeOptionPackageMarkResult,
) -> UUID:
    return _persist_realtime_option_package_mark(
        connection,
        result,
        repository_cursor=None,
    )


def select_developer_option_packages_cursor(
    cursor,
    *,
    cohort_id: UUID,
    valuation_policy_sha256: str,
    source_batch_id: UUID,
    instrument_ids: Sequence[int],
    source_cutoff,
    limit: int,
) -> tuple[RealtimeOptionPackage, ...]:
    instruments = tuple(sorted(set(instrument_ids)))
    if not instruments or any(
        type(instrument_id) is not int or instrument_id <= 0
        for instrument_id in instruments
    ):
        raise ValueError("Developer option selection requires positive instruments")
    if len(valuation_policy_sha256) != 64 or any(
        character not in "0123456789abcdef"
        for character in valuation_policy_sha256
    ):
        raise ValueError("Developer option selection policy must be a SHA-256 digest")
    if source_cutoff.utcoffset() is None:
        raise ValueError("Developer option selection cutoff must be timezone-aware")
    if not 1 <= limit <= 100:
        raise ValueError("Developer option selection limit must be in [1, 100]")
    cursor.execute(
        "WITH eligible AS MATERIALIZED ("
        "SELECT package.candidate_id FROM rt.option_packages AS package "
        "WHERE package.cohort_id=%s AND package.provider_tier='MASSIVE_DEVELOPER' "
        "AND package.valuation_policy_sha256=%s "
        "AND package.entry_market_time <= %s "
        "AND (package.exit_deadline IS NULL OR package.exit_deadline >= %s) "
        "AND NOT EXISTS (SELECT 1 FROM rt.option_package_legs AS missing "
        "WHERE missing.candidate_id=package.candidate_id "
        "AND NOT (missing.instrument_id=ANY(%s::bigint[]))) "
        "AND NOT EXISTS (SELECT 1 FROM rt.option_package_mark_revisions AS mark "
        "WHERE mark.candidate_id=package.candidate_id "
        "AND mark.valuation_policy_sha256=package.valuation_policy_sha256 "
        "AND mark.evidence_batch_id=%s) "
        "ORDER BY package.entry_market_time, package.candidate_id LIMIT %s"
        ") SELECT package.candidate_id, package.event_id, package.matrix_id, "
        "package.candidate_identity_sha256, package.cohort_id, package.provider_tier, "
        "package.entry_market_time, package.entry_net_premium, package.capital_at_risk, "
        "package.exit_deadline, package.leg_count, leg.leg_index, leg.instrument_id, "
        "leg.contract_id, leg.catalog_version_id, leg.contract_ticker, leg.side, "
        "leg.ratio, leg.multiplier, leg.strike, leg.expiration_date, leg.contract_type, "
        "leg.entry_mark, leg.entry_market_time, leg.valuation_policy_sha256 "
        "FROM eligible JOIN rt.option_packages AS package USING (candidate_id) "
        "JOIN rt.option_package_legs AS leg USING (candidate_id) "
        "ORDER BY package.entry_market_time, package.candidate_id, leg.leg_index",
        (
            str(cohort_id),
            valuation_policy_sha256,
            source_cutoff,
            source_cutoff,
            list(instruments),
            str(source_batch_id),
            limit,
        ),
    )
    grouped: dict[UUID, list[tuple[object, ...]]] = {}
    for row in cursor.fetchall():
        grouped.setdefault(UUID(str(row[0])), []).append(tuple(row))
    packages = []
    for candidate_id, rows in grouped.items():
        header = rows[0]
        if len(rows) != int(header[10]):
            raise ValueError("Developer option package leg set is incomplete")
        legs = tuple(
            RealtimeOptionPackageLeg(
                leg_index=int(row[11]),
                instrument_id=int(row[12]),
                contract_id=int(row[13]),
                catalog_version_id=int(row[14]),
                contract_ticker=str(row[15]),
                side=OptionSide(str(row[16])),
                ratio=int(row[17]),
                multiplier=int(row[18]),
                strike=row[19],
                expiration_date=row[20],
                contract_type=ContractType(str(row[21])),
                entry_mark=row[22],
                entry_market_time=row[23],
                valuation_policy_sha256=str(row[24]),
            )
            for row in rows
        )
        packages.append(RealtimeOptionPackage(
            candidate_id=candidate_id,
            event_id=UUID(str(header[1])) if header[1] is not None else None,
            matrix_id=UUID(str(header[2])),
            candidate_identity_sha256=str(header[3]),
            cohort_id=UUID(str(header[4])),
            provider_tier=ProviderTier(str(header[5])),
            entry_market_time=header[6],
            entry_net_premium=header[7],
            capital_at_risk=header[8],
            exit_deadline=header[9],
            legs=legs,
        ))
    return tuple(packages)


class RealtimeOptionPortalMarkRepository:
    def __init__(self, cursor_factory: Callable, *, transaction_configured=False):
        self._cursor_factory = cursor_factory
        self._transaction_configured = transaction_configured

    def retained_plan_marks(
        self,
        candidate_ids: Sequence[UUID],
        *,
        available_by,
        valuation_policy_sha256: str,
        cohort_id: UUID | None = None,
        provider_tier: ProviderTier | None = None,
    ) -> dict[str, object]:
        ids = tuple(sorted({UUID(str(value)) for value in candidate_ids}, key=str))
        if not 1 <= len(ids) <= 200 or available_by.utcoffset() is None:
            raise ValueError("realtime history marks require 1-200 candidates and an aware cutoff")
        with self._cursor_factory() as cursor:
            if not self._transaction_configured:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                "SELECT to_regclass('rt.option_package_mark_current') IS NOT NULL AS ready"
            )
            ready_row = cursor.fetchone()
            ready = bool(ready_row["ready"] if isinstance(ready_row, dict) else ready_row[0])
            if not ready:
                return {"ready": False, "management_status_ready": False, "rows": {}}
            cursor.execute(
                "SELECT revision.*, package.candidate_identity_sha256 AS candidate_identity, "
                "package.matrix_id, current.updated_at, "
                "array_agg(evidence.evidence_id ORDER BY input.leg_index) AS source_snapshot_ids, "
                "jsonb_agg(jsonb_build_object("
                "'contract_id', leg.contract_id, 'contract_ticker', leg.contract_ticker, "
                "'side', leg.side, 'ratio', leg.ratio, 'multiplier', leg.multiplier, "
                "'strike', leg.strike, 'expiration_date', leg.expiration_date, "
                "'contract_type', leg.contract_type, 'entry_mark', leg.entry_mark, "
                "'entry_valuation_policy_sha256', leg.valuation_policy_sha256, "
                "'snapshot_multiplier', leg.multiplier, "
                "'entry_mark_source', evidence.mark_source, "
                "'exit_mark', evidence.mark, "
                "'exit_mark_source', evidence.mark_source, "
                "'valuation_policy_sha256', revision.valuation_policy_sha256, "
                "'snapshot_id', evidence.evidence_id, "
                "'batch_id', revision.evidence_batch_id, "
                "'source_market_time', evidence.market_time, "
                "'source_observed_time', evidence.observed_time, "
                "'revised_observed_at', NULL) ORDER BY input.leg_index) AS legs, "
                "revision.evidence_batch_id AS source_batch_id "
                "FROM rt.option_package_mark_current AS current "
                "JOIN rt.option_package_mark_revisions AS revision USING (mark_id) "
                "JOIN rt.option_packages AS package USING (candidate_id) "
                "JOIN rt.option_package_mark_inputs AS input USING (mark_id) "
                "JOIN rt.option_package_legs AS leg ON leg.candidate_id=package.candidate_id "
                "AND leg.leg_index=input.leg_index "
                "JOIN rt.option_mark_evidence AS evidence ON evidence.evidence_id=input.evidence_id "
                "AND evidence.instrument_id=leg.instrument_id "
                "AND evidence.contract_id=leg.contract_id "
                "AND evidence.catalog_version_id=leg.catalog_version_id "
                "AND evidence.provider_tier=package.provider_tier "
                "WHERE current.candidate_id=ANY(%s::uuid[]) "
                "AND current.valuation_policy_sha256=%s "
                "AND (%s::uuid IS NULL OR package.cohort_id=%s::uuid) "
                "AND (%s::text IS NULL OR package.provider_tier=%s::text) "
                "AND revision.observed_time<=%s AND current.updated_at<=%s "
                "GROUP BY revision.mark_id, package.candidate_identity_sha256, "
                "package.matrix_id, current.updated_at, revision.evidence_batch_id "
                "ORDER BY revision.candidate_id",
                (list(ids), valuation_policy_sha256, str(cohort_id) if cohort_id else None, str(cohort_id) if cohort_id else None,
                    provider_tier.value if provider_tier else None, provider_tier.value if provider_tier else None,
                    available_by, available_by),
            )
            rows = tuple(dict(row) for row in cursor.fetchall())
        return {
            "ready": True,
            "management_status_ready": False,
            "rows": {str(row["candidate_id"]): row for row in rows},
        }


class RealtimeOptionCurrentQuoteRepository:
    def __init__(self, cursor_factory: Callable):
        self._cursor_factory = cursor_factory

    def current_quotes(
        self,
        contract_ids: Sequence[int],
        *,
        available_by,
    ) -> dict[str, object]:
        ids = tuple(dict.fromkeys(contract_ids))
        if not 1 <= len(ids) <= 8 or any(type(value) is not int or value <= 0 for value in ids):
            raise ValueError("current option quotes require 1-8 positive contract IDs")
        if available_by.utcoffset() is None:
            raise ValueError("current option quote cutoff must be timezone-aware")
        with self._cursor_factory() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                "SELECT provider_tier FROM rt.portal_cohort_current "
                "WHERE scope='PLATFORM' AND effective_at<=%s",
                (available_by,),
            )
            active = cursor.fetchone()
            active_tier = (
                active["provider_tier"] if isinstance(active, dict)
                else active[0] if active is not None else None
            )
            if active_tier != "MASSIVE_ADVANCED":
                reason = (
                    "DEVELOPER_OPTION_QUOTES_UNAVAILABLE"
                    if active_tier == "MASSIVE_DEVELOPER"
                    else "ACTIVE_PROVIDER_TIER_UNAVAILABLE"
                )
                return {
                    "available": False,
                    "reason": reason,
                    "as_of": None,
                    "observed_at": None,
                    "data": {
                        "quotes": [],
                        "requested_contract_ids": list(ids),
                        "missing_contract_ids": list(ids),
                        "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
                        "provider_fetch_performed": False,
                        "execution_permission": False,
                    },
                }
            cursor.execute(
                "SELECT contract.contract_id, contract.contract_ticker, "
                "quote.event_at AS quote_time, quote.received_at, quote.bid, quote.ask, "
                "quote.bid_size, quote.ask_size, quote.updated_at "
                "FROM rt.quote_current AS quote "
                "JOIN rt.instruments AS instrument USING (instrument_id) "
                "JOIN ref.option_contracts AS contract USING (contract_id) "
                "WHERE quote.provider='massive' AND quote.quality='GOOD' "
                "AND contract.contract_id=ANY(%s::bigint[]) "
                "AND quote.event_at<=%s AND quote.received_at<=%s "
                "ORDER BY contract.contract_id",
                (list(ids), available_by, available_by),
            )
            rows = tuple(dict(row) for row in cursor.fetchall())
        found = {row["contract_id"] for row in rows}
        return {
            "available": bool(rows),
            "reason": None if rows else "ADVANCED_QUOTES_UNAVAILABLE",
            "as_of": max((row["quote_time"] for row in rows), default=None),
            "observed_at": max((row["received_at"] for row in rows), default=None),
            "data": {
                "quotes": list(rows),
                "requested_contract_ids": list(ids),
                "missing_contract_ids": [value for value in ids if value not in found],
                "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
                "provider_fetch_performed": False,
                "execution_permission": False,
            },
        }