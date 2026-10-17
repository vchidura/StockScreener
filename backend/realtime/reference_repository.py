from __future__ import annotations

from collections.abc import Mapping

from psycopg2.extras import Json

from .reference_facts import (
    ReferenceFact,
    ReferenceFactRelation,
    REFERENCE_FACT_FIELDS,
    validate_reference_facts,
)
from .reference_seed import ReferenceSeedManifest, SeedStatus
from .reference_service import ReferenceServiceResult


_RELATION_ORDER = {
    ReferenceFactRelation.SECURITY: 0,
    ReferenceFactRelation.FUNDAMENTAL_REPORT: 1,
    ReferenceFactRelation.FUNDAMENTAL_COVERAGE: 2,
    ReferenceFactRelation.OPTION_CONTRACT: 3,
    ReferenceFactRelation.CORPORATE_ACTION: 4,
    ReferenceFactRelation.CORPORATE_ACTION_COVERAGE: 5,
    ReferenceFactRelation.OI_BASELINE: 6,
    ReferenceFactRelation.MARKET_EVENT: 7,
    ReferenceFactRelation.MARKET_EVENT_COVERAGE: 8,
    ReferenceFactRelation.RATE_OBSERVATION: 9,
}


def _persist_reference_fact(cursor, fact: ReferenceFact) -> None:
    row = dict(fact.values)
    if fact.relation is ReferenceFactRelation.SECURITY:
        cursor.execute(
            "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (security_id) DO NOTHING",
            (row["security_id"], row["asset_type"], row["source_origin"], row["observed_at"]),
        )
        cursor.execute(
            "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, exchange, "
            "calendar_id, effective_from, effective_to, observed_at, payload_sha256, "
            "company_name, security_type, active, cik, composite_figi, share_class_figi, "
            "sic_code, sic_description, sector, industry, list_date, delisted_date, "
            "weighted_shares, free_float, free_float_percent, market_cap) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (revision_id) DO NOTHING",
            (
                row["revision_id"], row["security_id"], row["ticker"], row["exchange"],
                row["calendar_id"], row["effective_from"], row["effective_to"],
                row["observed_at"], row["payload_sha256"], row["company_name"], row["security_type"],
                row["active"], row["cik"], row["composite_figi"], row["share_class_figi"],
                row["sic_code"], row["sic_description"], row["sector"], row["industry"],
                row["list_date"], row["delisted_date"], row["weighted_shares"], row["free_float"],
                row["free_float_percent"], row["market_cap"],
            ),
        )
        cursor.execute(
            "SELECT asset_type, source_origin FROM ref.securities WHERE security_id=%s",
            (row["security_id"],),
        )
        stored = cursor.fetchone()
        expected = (row["asset_type"], row["source_origin"])
        if stored is None or tuple(stored) != expected:
            raise ValueError("persisted security identity conflict")
    elif fact.relation is ReferenceFactRelation.FUNDAMENTAL_REPORT:
        cursor.execute(
            "INSERT INTO ref.fundamental_reports (report_id, security_id, security_revision_id, "
            "cik, accession_number, form_type, timeframe, fiscal_year, fiscal_quarter, period_end, "
            "filing_date, availability_time, observed_at, source, source_key, metrics, raw_payload, "
            "payload_sha256, quality_codes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (report_id) DO NOTHING",
            (*fact.ordered_values()[:15], Json(row["metrics"]), Json(row["raw_payload"]),
             row["payload_sha256"], list(row["quality_codes"])),
        )
    elif fact.relation is ReferenceFactRelation.FUNDAMENTAL_COVERAGE:
        cursor.execute(
            "INSERT INTO ref.fundamental_report_coverage (coverage_id, security_id, window_start, "
            "window_end, source, report_count, response_sha256, checked_at, valid_until) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (coverage_id) DO NOTHING",
            fact.ordered_values(),
        )
    elif fact.relation is ReferenceFactRelation.OPTION_CONTRACT:
        cursor.execute(
            "INSERT INTO ref.option_contracts (contract_id, underlying_security_id, provider, "
            "contract_ticker, first_observed_at, source_origin) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (contract_id) DO NOTHING",
            (
                row["contract_id"], row["underlying_security_id"], row["provider"],
                row["contract_ticker"], row["first_observed_at"], row["source_origin"],
            ),
        )
        cursor.execute(
            "SELECT underlying_security_id, provider, contract_ticker, source_origin "
            "FROM ref.option_contracts WHERE contract_id=%s",
            (row["contract_id"],),
        )
        stored = cursor.fetchone()
        expected = (
            row["underlying_security_id"], row["provider"],
            row["contract_ticker"], row["source_origin"],
        )
        if stored is None or tuple(stored) != expected:
            raise ValueError("persisted option contract identity conflict")
        cursor.execute(
            "INSERT INTO ref.option_contract_versions (version_id, contract_id, call_put, "
            "expiration, strike, exercise_style, multiplier, deliverables, eligibility, "
            "effective_from, effective_to, observed_at, payload_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (version_id) DO NOTHING",
            (
                row["version_id"], row["contract_id"], row["call_put"], row["expiration"],
                row["strike"], row["exercise_style"], row["multiplier"],
                Json(row["deliverables"]), row["eligibility"], row["effective_from"],
                row["effective_to"], row["observed_at"], row["payload_sha256"],
            ),
        )
    elif fact.relation is ReferenceFactRelation.CORPORATE_ACTION:
        cursor.execute(
            "INSERT INTO ref.corporate_action_revisions (action_id, security_id, ticker, "
            "action_type, effective_date, declaration_date, ex_date, record_date, pay_date, "
            "cash_amount, split_from, split_to, new_ticker, source, source_key, observed_at, "
            "received_at, payload_sha256, raw_payload, supersedes_action_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s) ON CONFLICT (action_id) DO NOTHING",
            (*fact.ordered_values()[:-2], Json(row["raw_payload"]), row["supersedes_action_id"]),
        )
        cursor.execute(
            "SELECT action_id,security_id,ticker,action_type,effective_date,declaration_date,ex_date,record_date,pay_date,"
            "cash_amount,split_from,split_to,new_ticker,source,source_key,observed_at,received_at,payload_sha256,raw_payload,"
            "supersedes_action_id FROM ref.corporate_action_revisions WHERE action_id=%s", (row["action_id"],))
        _verify_reference_fact(cursor.fetchone(), fact)
    elif fact.relation is ReferenceFactRelation.CORPORATE_ACTION_COVERAGE:
        cursor.execute(
            "INSERT INTO ref.corporate_action_coverage (coverage_id, security_id, action_type, "
            "window_start, window_end, source, source_key, response_action_count, "
            "response_sha256, observed_at, received_at, payload_sha256) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (coverage_id) DO NOTHING",
            fact.ordered_values(),
        )
        cursor.execute(
            "SELECT coverage_id,security_id,action_type,window_start,window_end,source,source_key,response_action_count,"
            "response_sha256,observed_at,received_at,payload_sha256 FROM ref.corporate_action_coverage WHERE coverage_id=%s",
            (row["coverage_id"],))
        _verify_reference_fact(cursor.fetchone(), fact)
    elif fact.relation is ReferenceFactRelation.OI_BASELINE:
        cursor.execute(
            "INSERT INTO ref.oi_baselines (baseline_id, contract_id, settlement_session, "
            "open_interest, source, date_basis, observed_session, observed_at, received_at, "
            "payload_sha256, supersedes_baseline_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (baseline_id) DO NOTHING",
            fact.ordered_values(),
        )
    elif fact.relation is ReferenceFactRelation.MARKET_EVENT:
        cursor.execute(
            "INSERT INTO ref.market_event_revisions (revision_id, security_id, event_key, "
            "event_type, event_date, announced_at, event_timezone, time_precision, status, "
            "source, observed_at, received_at, valid_until, payload_sha256, "
            "supersedes_revision_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s) ON CONFLICT (revision_id) DO NOTHING",
            fact.ordered_values(),
        )
    elif fact.relation is ReferenceFactRelation.MARKET_EVENT_COVERAGE:
        cursor.execute(
            "INSERT INTO ref.market_event_coverage (coverage_id, security_id, event_type, "
            "window_start, window_end, source, verdict, checked_at, valid_until, "
            "evidence_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (coverage_id) DO NOTHING",
            fact.ordered_values(),
        )
    elif fact.relation is ReferenceFactRelation.RATE_OBSERVATION:
        cursor.execute(
            "INSERT INTO ref.risk_free_rate_observations (observation_id, rate_date, "
            "tenor_days, annual_rate, source, source_key, source_observed_at, received_at, "
            "payload_sha256) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (observation_id) DO NOTHING",
            fact.ordered_values(),
        )


def _verify_reference_fact(stored, fact):
    values = tuple(stored[key] for key in REFERENCE_FACT_FIELDS[fact.relation]) if isinstance(stored, Mapping) else stored
    if values is None or tuple(values) != fact.ordered_values():
        raise ValueError("Persisted corporate-action fact identity/metadata conflict")


def load_dividend_reference_cursor(cursor, *, security_id, ticker, window_start, window_end, known_by, coverage_id=None):
    from .dividend_reference import _utc, dividend_reference_window
    cutoff = _utc(known_by, "known_by")
    cursor.execute(
        "SELECT * FROM ref.corporate_action_coverage WHERE security_id=%s AND action_type='DIVIDEND' "
        "AND window_start<=%s AND window_end>=%s AND observed_at<=%s AND received_at<=%s "
        "AND (%s::uuid IS NULL OR coverage_id=%s::uuid) ORDER BY received_at DESC,observed_at DESC,coverage_id LIMIT 1",
        (str(security_id), window_start, window_end, cutoff, cutoff, str(coverage_id) if coverage_id else None,
         str(coverage_id) if coverage_id else None))
    coverage = cursor.fetchone()
    rows = ()
    if coverage is not None:
        if not isinstance(coverage, Mapping):
            raise TypeError("Dividend reference reader requires a dictionary cursor")
        cursor.execute(
            "SELECT * FROM ref.corporate_action_revisions WHERE security_id=%s AND ticker=%s AND action_type='DIVIDEND' "
            "AND source=%s AND observed_at=%s AND received_at=%s AND effective_date BETWEEN %s AND %s "
            "ORDER BY effective_date,ticker,source_key,action_id",
            (str(security_id), ticker, coverage["source"], coverage["observed_at"], coverage["received_at"],
             coverage["window_start"], coverage["window_end"]))
        rows = tuple(cursor.fetchall())
    return dividend_reference_window(security_id=security_id, ticker=ticker, window_start=window_start,
        window_end=window_end, known_by=cutoff, coverage=coverage, rows=rows, coverage_id=coverage_id)


def persist_reference_facts_cursor(cursor, facts: tuple[ReferenceFact, ...]) -> None:
    validate_reference_facts(facts)
    for fact in sorted(facts, key=lambda row: (_RELATION_ORDER[row.relation], str(row.identity[1]))):
        _persist_reference_fact(cursor, fact)


def _check_reference_result(result: ReferenceServiceResult) -> None:
    member_identities = {
        (member.kind.value, member.key) for member in result.manifest.members
    }
    ready_member_identities = {
        (member.kind.value, member.key)
        for member in result.manifest.members
        if member.status.value == "READY"
    }
    fact_identities = {
        (fact.member_kind, fact.member_key) for fact in result.facts
    }
    extras = sorted(fact_identities - member_identities)
    if extras:
        raise ValueError(f"reference facts contain unreviewed member identities: {extras}")
    if result.manifest.status is SeedStatus.READY:
        missing = sorted(ready_member_identities - fact_identities)
        if missing:
            raise ValueError(f"ready reference seed is missing durable facts: {missing}")


def persist_reference_result(
    connection,
    result: ReferenceServiceResult,
    *,
    generation: int,
    scope: str = "MARKET_REFERENCES",
) -> None:
    _check_reference_result(result)
    persist_reference_seed(
        connection,
        result.manifest,
        generation=generation,
        facts=result.facts,
        scope=scope,
    )


def persist_reference_result_cursor(
    cursor,
    result: ReferenceServiceResult,
    *,
    generation: int,
    scope: str = "MARKET_REFERENCES",
) -> None:
    """Persist in the caller's transaction (a stage's fenced unit of work); the caller commits."""
    _check_reference_result(result)
    persist_reference_seed_cursor(
        cursor,
        result.manifest,
        generation=generation,
        facts=result.facts,
        scope=scope,
    )


REVIEWED_REFERENCE_SCOPES = frozenset({
    "MARKET_REFERENCES",
    "EQUITY_VALIDATION_REFERENCES",
    "ADVANCED_SPY_REFERENCES",
})


def _check_reference_seed(generation: int, scope: str, facts: tuple[ReferenceFact, ...]) -> None:
    if generation <= 0:
        raise ValueError("seed generation must be positive")
    if scope not in REVIEWED_REFERENCE_SCOPES:
        raise ValueError("reference seed scope is not reviewed")
    validate_reference_facts(facts)


def persist_reference_seed(
    connection,
    manifest: ReferenceSeedManifest,
    *,
    generation: int,
    facts: tuple[ReferenceFact, ...] = (),
    scope: str = "MARKET_REFERENCES",
) -> None:
    _check_reference_seed(generation, scope, facts)
    try:
        with connection.cursor() as cursor:
            _persist_reference_seed(cursor, manifest, generation, facts, scope)
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def persist_reference_seed_cursor(
    cursor,
    manifest: ReferenceSeedManifest,
    *,
    generation: int,
    facts: tuple[ReferenceFact, ...] = (),
    scope: str = "MARKET_REFERENCES",
) -> None:
    """Persist in the caller's transaction (a stage's fenced unit of work); the caller commits."""
    _check_reference_seed(generation, scope, facts)
    _persist_reference_seed(cursor, manifest, generation, facts, scope)


def _persist_reference_seed(cursor, manifest, generation, facts, scope) -> None:
    persist_reference_facts_cursor(cursor, facts)
    cursor.execute(
        "INSERT INTO rt.seed_manifests ("
        "seed_id, seed_sha256, universe_sha256, session_date, status, "
        "reason_codes, member_count, created_at, effective_at, valid_until, "
        "source_generations) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s) ON CONFLICT (seed_id) DO NOTHING",
        (
            str(manifest.seed_id),
            manifest.seed_sha256,
            manifest.universe_sha256,
            manifest.session_date,
            manifest.status.value,
            list(manifest.reason_codes),
            len(manifest.members),
            manifest.created_at,
            manifest.effective_at,
            manifest.valid_until,
            Json(dict(manifest.source_generations)),
        ),
    )
    cursor.execute(
        "SELECT seed_sha256 FROM rt.seed_manifests WHERE seed_id=%s",
        (str(manifest.seed_id),),
    )
    row = cursor.fetchone()
    if row is None or row[0] != manifest.seed_sha256:
        raise ValueError("persisted reference seed identity conflict")
    for member in manifest.members:
        cursor.execute(
            "INSERT INTO rt.seed_manifest_members ("
            "seed_id, reference_kind, reference_key, source_id, "
            "payload_sha256, status, reason_codes, observed_at, received_at"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (seed_id, reference_kind, reference_key) DO NOTHING",
            (
                str(manifest.seed_id),
                member.kind.value,
                member.key,
                member.source_id,
                member.payload_sha256,
                member.status.value,
                list(member.reason_codes),
                member.observed_at,
                member.received_at,
            ),
        )
    if manifest.status is SeedStatus.READY:
        cursor.execute(
            "SELECT generation FROM rt.seed_current "
            "WHERE scope=%s FOR UPDATE",
            (scope,),
        )
        current = cursor.fetchone()
        if current is not None and current[0] >= generation:
            raise ValueError("seed generation must advance the current pointer")
        cursor.execute(
            "INSERT INTO rt.seed_current (scope, seed_id, generation, effective_at) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (scope) DO UPDATE SET seed_id=EXCLUDED.seed_id, "
            "generation=EXCLUDED.generation, effective_at=EXCLUDED.effective_at, "
            "updated_at=clock_timestamp() "
            "WHERE rt.seed_current.generation < EXCLUDED.generation",
            (scope, str(manifest.seed_id), generation, manifest.effective_at),
        )