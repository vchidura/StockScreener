from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from psycopg2.extras import execute_values

from options.stock_behavior_gates import StockBehaviorGateAssessment

from .base import ConnectionFactory, PostgresRepository


@dataclass(frozen=True, slots=True)
class StockBehaviorAssessmentPersistResult:
    inserted: int
    existing: int


@dataclass(frozen=True, slots=True)
class OptionMatrixAssessmentLineage:
    matrix_id: UUID
    underlying: str
    market_time: datetime
    observed_time: datetime
    scheduled_cycle: datetime
    configuration_sha256: str
    market_policy_sha256: str
    analysis_policy_sha256: str


@dataclass(frozen=True, slots=True)
class StockBehaviorAssessmentStatus:
    since: datetime
    until: datetime
    assessment_count: int
    candidate_count: int
    eligible_count: int
    blocked_count: int
    unavailable_count: int
    latest_decision_at: datetime | None
    latest_recorded_at: datetime | None


@dataclass(frozen=True, slots=True)
class StockBehaviorShadowLaunchUsage:
    assessment_count: int
    payload_bytes: int
    unavailable_count: int
    p95_decision_lag_seconds: float | None


def stock_behavior_assessment_id(assessment: StockBehaviorGateAssessment) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"option-stock-behavior-assessment:{assessment.sha256}",
    )


class OptionStockBehaviorAssessmentRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def detector_cycle_sources(self, *, configuration, scheduled_cycle, completed_matrices, as_of):
        if (as_of.utcoffset() is None or scheduled_cycle.utcoffset() is None or scheduled_cycle > as_of
                or not 1 <= len(completed_matrices) <= 13
                or set(completed_matrices) != set(configuration.settings.underlyers)
                or len(set(completed_matrices.values())) != len(completed_matrices)):
            raise ValueError("detector sources require an exact complete causal cycle")
        matrix_ids = list(completed_matrices.values())
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT analysis.matrix_id,analysis.batch_id,analysis.underlying,
                    analysis.market_time,analysis.observed_time,analysis.created_at,
                    ingestion.scheduled_cycle,ingestion.configuration_sha256,
                    ingestion.policy_sha256 AS market_policy_sha256,analysis.policy_sha256 AS analysis_policy_sha256
                FROM option_analysis_runs AS analysis JOIN option_ingestion_runs AS ingestion USING(batch_id)
                JOIN option_work_items AS work ON work.subject_id=analysis.matrix_id::text AND work.stage='STRATEGY'
                  AND work.business_key='strategy:'||analysis.matrix_id::text||':'||%s
                WHERE analysis.matrix_id=ANY(%s::uuid[]) AND ingestion.scheduled_cycle=%s
                  AND ingestion.configuration_sha256=%s AND ingestion.policy_sha256=%s AND analysis.policy_sha256=%s
                  AND ingestion.status='COMPLETE' AND analysis.status='COMPLETE' AND work.status='COMPLETED'
                  AND ingestion.completed_at<=%s AND analysis.completed_at<=%s AND work.completed_at<=%s
                  AND analysis.observed_time<=%s AND analysis.created_at<=%s
                ORDER BY analysis.underlying LIMIT 14""",
                (configuration.strategy_policy.strategy_version, matrix_ids, scheduled_cycle,
                 configuration.configuration_sha256, configuration.policy_sha256, configuration.policy_sha256,
                 as_of, as_of, as_of, as_of, as_of))
            matrices = tuple(dict(row) for row in cursor.fetchall())
            if (len(matrices) != len(completed_matrices)
                    or {row["underlying"]: row["matrix_id"] for row in matrices} != completed_matrices):
                raise ValueError("detector source matrices are not exactly complete")
            cursor.execute(
                "SELECT snapshot.* FROM option_chain_snapshots AS snapshot "
                "JOIN option_analysis_runs AS analysis USING(batch_id) "
                "WHERE analysis.matrix_id=ANY(%s::uuid[]) AND snapshot.first_observed_at<=%s "
                "AND snapshot.created_at<=%s AND snapshot.market_data_time<=analysis.market_time "
                "AND snapshot.first_observed_at<=analysis.observed_time "
                "ORDER BY snapshot.batch_id,snapshot.contract_id,snapshot.snapshot_id LIMIT 20001",
                (matrix_ids, as_of, as_of),
            )
            snapshots = tuple(dict(row) for row in cursor.fetchall())
            if len(snapshots) > 20000:
                raise ValueError("detector snapshot bound exceeded; no truncated source pool")
            contract_ids = sorted({row["contract_id"] for row in snapshots})
            cursor.execute("""SELECT * FROM option_daily_contract_facts
                WHERE contract_id=ANY(%s::bigint[]) AND open_interest_observed_session=%s
                                    AND open_interest_observed_at<=%s AND created_at<=%s AND updated_at<=%s
                ORDER BY contract_id,settlement_session LIMIT 20001""",
                                (contract_ids, scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date(), as_of, as_of, as_of))
            facts = tuple(dict(row) for row in cursor.fetchall())
            if len(facts) > 20000:
                raise ValueError("detector dated OI bound exceeded")
            cursor.execute("""SELECT candidate_id,matrix_id,underlying FROM option_strategy_candidates
                WHERE matrix_id=ANY(%s::uuid[]) AND policy_sha256=%s AND strategy_version=%s
                  AND status='SELECTED' AND candidate_kind IN ('SINGLE_CONTRACT','MULTI_LEG')
                  AND observed_time<=%s AND created_at<=%s
                ORDER BY matrix_id,candidate_rank,candidate_id LIMIT 5001""",
                (matrix_ids, configuration.strategy_policy_sha256, configuration.strategy_policy.strategy_version, as_of, as_of))
            candidates = tuple(dict(row) for row in cursor.fetchall())
            if len(candidates) > 5000:
                raise ValueError("detector candidate bound exceeded")
        return dict(matrices=matrices, snapshots=snapshots, open_interest=facts, candidates=candidates)

    def detector_package_sources(self, *, configuration, candidate_ids, as_of):
        if as_of.utcoffset() is None or len(candidate_ids) > 5000 or len(set(candidate_ids)) != len(candidate_ids):
            raise ValueError("package source lookup requires bounded distinct candidates and cutoff")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT * FROM option_strategy_candidates WHERE candidate_id=ANY(%s::uuid[])
                AND strategy_name IN ('DIRECTIONAL_LONG_PREMIUM','DIRECTIONAL_DEBIT_SPREAD')
                AND policy_sha256=%s AND strategy_version=%s AND status='SELECTED'
                AND observed_time<=%s AND created_at<=%s AND valid_until>%s
                ORDER BY candidate_id""", (list(candidate_ids), configuration.strategy_policy_sha256,
                    configuration.strategy_policy.strategy_version, as_of, as_of, as_of))
            candidates = tuple(dict(row) for row in cursor.fetchall())
            ids = [row["candidate_id"] for row in candidates]
            cursor.execute("SELECT * FROM option_candidate_legs WHERE candidate_id=ANY(%s::uuid[]) ORDER BY candidate_id,leg_index LIMIT 10001", (ids,))
            legs = tuple(dict(row) for row in cursor.fetchall())
            if len(legs) > 10000:
                raise ValueError("detector leg source bound exceeded")
            contracts = sorted({row["contract_id"] for row in legs})
            cursor.execute("""SELECT catalog.contract_id,catalog.contract_ticker,catalog.underlying,catalog.asset_type,version.*
                FROM option_contract_catalog AS catalog JOIN option_contract_catalog_versions AS version USING(contract_id)
                WHERE catalog.contract_id=ANY(%s::bigint[]) AND catalog.catalog_admitted_at<=%s
                  AND version.first_observed_at<=%s AND COALESCE(version.revised_observed_at,version.first_observed_at)<=%s
                  AND version.eligibility_status='VALIDATED_ACTIVE' ORDER BY catalog.contract_id,version.valid_from DESC LIMIT 20001""",
                (contracts, as_of, as_of, as_of))
            references = tuple(dict(row) for row in cursor.fetchall())
            if len(references) > 20000:
                raise ValueError("detector reference source bound exceeded")
            cursor.execute("""SELECT DISTINCT bar.* FROM equity_bar_revisions AS bar
                JOIN option_chain_snapshots AS snapshot ON snapshot.underlying=bar.ticker
                  AND snapshot.spot_market_data_time=bar.bar_end AND snapshot.spot=bar.close_price
                JOIN option_candidate_legs AS leg ON leg.snapshot_id=snapshot.snapshot_id
                WHERE leg.candidate_id=ANY(%s::uuid[]) AND bar.interval='1m' AND NOT bar.adjusted
                  AND bar.is_final AND bar.source_kind='NATIVE_REST' AND bar.availability_mode='LIVE_OBSERVED'
                  AND bar.session_scope='RTH' AND bar.quality_codes=ARRAY[]::text[]
                  AND bar.system_observed_at<=%s AND bar.created_at<=%s
                ORDER BY bar.ticker,bar.bar_end,bar.created_at DESC LIMIT 10001""", (ids, as_of, as_of))
            bars = tuple(dict(row) for row in cursor.fetchall())
            if len(bars) > 10000:
                raise ValueError("detector spot source bound exceeded")
        return dict(candidates=candidates, legs=legs, references=references, raw_bars=bars)

    def detector_technical_sources(self, *, underlyers, market_cutoff, as_of):
        from equity.materialization import SETUP_VERSION

        if not 1 <= len(underlyers) <= 13 or market_cutoff.utcoffset() is None or as_of.utcoffset() is None or market_cutoff > as_of:
            raise ValueError("technical sources require bounded causal scope")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT DISTINCT ON(setup.ticker,setup.interval) setup.*,
                    feature.source_revision_ids AS feature_bar_ids
                FROM equity_evidence AS setup JOIN equity_evidence AS feature
                    ON feature.evidence_id=(setup.payload->>'feature_evidence_id')::uuid
                WHERE setup.ticker=ANY(%s) AND setup.evidence_type='TRADE_SETUP' AND setup.source_name='EQUITY_SETUP'
                  AND setup.source_version=%s AND setup.interval IN ('30m','1h')
                  AND setup.quality_state IN ('COMPLETE','RESEARCH_ONLY') AND setup.lifecycle_status<>'CONFLICTED'
                  AND setup.market_time<=%s AND setup.observed_at<=%s AND setup.created_at<=%s AND setup.valid_until>%s
                  AND feature.evidence_type='FEATURE_SNAPSHOT' AND feature.security_id=setup.security_id
                  AND feature.interval=setup.interval AND feature.market_time=setup.market_time
                  AND feature.observed_at<=%s AND feature.created_at<=%s
                ORDER BY setup.ticker,setup.interval,setup.market_time DESC,setup.observed_at DESC LIMIT 27""",
                (list(underlyers), SETUP_VERSION, market_cutoff, as_of, as_of, as_of, as_of, as_of))
            sources = tuple(dict(row) for row in cursor.fetchall())
            ids = sorted({identity for row in sources for identity in row["feature_bar_ids"]}, key=str)
            if len(sources) > 26 or len(ids) > 12000:
                raise ValueError("technical source bar budget exceeded")
            cursor.execute("""SELECT * FROM equity_bar_revisions WHERE bar_revision_id=ANY(%s::uuid[])
                AND NOT adjusted AND is_final AND session_scope='RTH' AND availability_mode='LIVE_OBSERVED'
                AND quality_codes=ARRAY[]::text[] AND system_observed_at<=%s AND created_at<=%s
                ORDER BY bar_start""", (ids, as_of, as_of))
            bars = tuple(dict(row) for row in cursor.fetchall())
            cursor.execute("""SELECT DISTINCT ON(source,source_key) * FROM option_market_events
                WHERE (affected_underlying=ANY(%s) OR affected_underlying IS NULL)
                  AND first_observed_at<=%s AND COALESCE(revised_observed_at,first_observed_at)<=%s
                  AND scheduled_time>=%s AND scheduled_time<=%s
                ORDER BY source,source_key,COALESCE(revised_observed_at,first_observed_at) DESC LIMIT 1001""",
                (list(underlyers), as_of, as_of, as_of - timedelta(days=1), as_of + timedelta(days=1)))
            events = tuple(dict(row) for row in cursor.fetchall())
            if len(events) > 1000:
                raise ValueError("technical event source budget exceeded")
            cursor.execute(
                "SELECT * FROM option_event_calendar_coverage "
                "WHERE (affected_underlying=ANY(%s) OR affected_underlying IS NULL) "
                "AND first_observed_at<=%s AND first_observed_at>=%s "
                "ORDER BY first_observed_at DESC LIMIT 1001",
                (list(underlyers), as_of, as_of - timedelta(days=1)),
            )
            coverage = tuple(dict(row) for row in cursor.fetchall())
            if len(coverage) > 1000:
                raise ValueError("technical event coverage budget exceeded")
        return dict(sources=sources, bars=bars, events=events, coverage=coverage)

    def technical_replay_preflight(self, *, configuration, session_date, as_of):
        from equity.behavior import DEFINITION_V1_SHA256, OPTIONS_SWING_PROFILE

        today = as_of.astimezone(ZoneInfo("America/New_York")).date()
        if as_of.utcoffset() is None or not today - timedelta(days=60) <= session_date <= today:
            raise ValueError("technical replay preflight requires one retained session within sixty days")
        start = datetime.combine(session_date, datetime.min.time(), ZoneInfo("America/New_York"))
        end = start + timedelta(days=1)
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""WITH sources AS (
                    SELECT analysis.matrix_id,analysis.underlying,ingestion.scheduled_cycle,
                        GREATEST(ingestion.completed_at,analysis.completed_at,work.completed_at,
                            analysis.created_at,analysis.observed_time) AS available_at
                    FROM option_analysis_runs AS analysis JOIN option_ingestion_runs AS ingestion USING(batch_id)
                    JOIN option_work_items AS work ON work.subject_id=analysis.matrix_id::text AND work.stage='STRATEGY'
                      AND work.business_key='strategy:'||analysis.matrix_id::text||':'||%s
                    WHERE ingestion.configuration_sha256=%s AND ingestion.policy_sha256=%s AND analysis.policy_sha256=%s
                      AND analysis.underlying=ANY(%s) AND ingestion.scheduled_cycle>=%s AND ingestion.scheduled_cycle<%s
                      AND ingestion.status='COMPLETE' AND analysis.status='COMPLETE' AND work.status='COMPLETED'
                      AND ingestion.completed_at<=%s AND analysis.completed_at<=%s AND work.completed_at<=%s
                ), cycles AS (
                    SELECT scheduled_cycle,MAX(available_at) AS decision_cutoff,COUNT(*) AS matrix_count,
                        COUNT(DISTINCT underlying) AS covered FROM sources GROUP BY scheduled_cycle
                )
                SELECT sources.*,cycles.decision_cutoff,cycles.matrix_count,cycles.covered,
                    candidates.candidate_count,candidates.timely_candidates,candidates.first_entry_deadline,candidates.last_entry_deadline,
                    EXISTS(SELECT 1 FROM equity_context_snapshots AS stock
                        WHERE stock.context_kind='STOCK_BEHAVIOR' AND stock.ticker=sources.underlying
                          AND stock.strategy_horizon=%s AND stock.behavior_definition_sha256=%s AND stock.context_policy_sha256=%s
                          AND stock.market_time<=sources.scheduled_cycle AND stock.observed_at<=cycles.decision_cutoff
                          AND stock.created_at<=cycles.decision_cutoff AND stock.valid_until>cycles.decision_cutoff
                          AND stock.behavior_payload_text::jsonb->>'availability_mode'='PROSPECTIVE_RECEIPT') AS has_stock_behavior,
                    EXISTS(SELECT 1 FROM equity_evidence AS evidence
                        WHERE evidence.ticker=sources.underlying AND evidence.evidence_type='TRADE_SETUP'
                          AND evidence.interval IN ('30m','1h') AND evidence.quality_state='COMPLETE'
                          AND evidence.market_time<=sources.scheduled_cycle AND evidence.observed_at<=cycles.decision_cutoff
                          AND evidence.created_at<=cycles.decision_cutoff AND evidence.valid_until>cycles.decision_cutoff
                          AND cardinality(evidence.source_revision_ids)>0) AS has_potential_technical_evidence
                FROM sources JOIN cycles USING(scheduled_cycle)
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) AS candidate_count,MIN(candidate.valid_until) AS first_entry_deadline,
                        MAX(candidate.valid_until) AS last_entry_deadline,COUNT(*) FILTER(WHERE candidate.valid_until>cycles.decision_cutoff
                        AND candidate.observed_time<=cycles.decision_cutoff AND candidate.created_at<=cycles.decision_cutoff) AS timely_candidates
                    FROM option_strategy_candidates AS candidate WHERE candidate.matrix_id=sources.matrix_id
                      AND candidate.policy_sha256=%s AND candidate.strategy_version=%s AND candidate.status='SELECTED'
                      AND candidate.strategy_name IN ('DIRECTIONAL_LONG_PREMIUM','DIRECTIONAL_DEBIT_SPREAD')
                ) AS candidates ON TRUE
                ORDER BY sources.scheduled_cycle,sources.underlying LIMIT 601""",
                (configuration.strategy_policy.strategy_version, configuration.configuration_sha256, configuration.policy_sha256,
                 configuration.policy_sha256, list(configuration.settings.underlyers), start, end, as_of, as_of, as_of,
                 OPTIONS_SWING_PROFILE.name, DEFINITION_V1_SHA256, OPTIONS_SWING_PROFILE.sha256,
                 configuration.strategy_policy_sha256, configuration.strategy_policy.strategy_version))
            rows = [dict(row) for row in cursor.fetchall()]
            if len(rows) > 600:
                raise ValueError("technical replay session exceeds matrix bound")
            cursor.execute("""SELECT COUNT(*) AS stock_publications,
                    COUNT(*) FILTER(WHERE imported_at<%s) AS imported_by_session_end
                FROM stock_alert_result_records WHERE kind='publication_evidence'
                                    AND COALESCE(payload->>'market_time',record_id)::timestamptz>=%s
                                    AND COALESCE(payload->>'market_time',record_id)::timestamptz<%s""", (end, start, end))
            publications = dict(cursor.fetchone())
        return dict(rows=rows, publications=publications)

    def completed_matrices(self, *, configuration, as_of):
        from zoneinfo import ZoneInfo

        if as_of.tzinfo is None:
            raise ValueError("completed-cycle lookup requires an aware cutoff")
        start = datetime.combine(as_of.astimezone(ZoneInfo("America/New_York")).date(),
            datetime.min.time(), ZoneInfo("America/New_York")) - timedelta(days=60)
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""
                SELECT analysis.matrix_id, analysis.underlying, ingestion.scheduled_cycle,
                       GREATEST(ingestion.completed_at, analysis.completed_at, work.completed_at) AS completed_at
                FROM option_ingestion_runs AS ingestion
                JOIN option_analysis_runs AS analysis USING(batch_id)
                JOIN option_work_items AS work
                  ON work.subject_id=analysis.matrix_id::text AND work.stage='STRATEGY'
                 AND work.business_key='strategy:' || analysis.matrix_id::text || ':' || %s
                WHERE ingestion.configuration_sha256=%s AND ingestion.policy_sha256=%s
                  AND analysis.policy_sha256=%s AND analysis.underlying=ANY(%s)
                  AND ingestion.scheduled_cycle>=%s AND ingestion.scheduled_cycle<=%s
                  AND ingestion.status='COMPLETE' AND analysis.status='COMPLETE' AND work.status='COMPLETED'
                  AND ingestion.completed_at<=%s AND analysis.completed_at<=%s AND work.completed_at<=%s
                  AND analysis.observed_time<=%s AND analysis.created_at<=%s
                ORDER BY ingestion.scheduled_cycle, analysis.underlying, completed_at, analysis.matrix_id
                LIMIT 25001
            """, (configuration.strategy_policy.strategy_version, configuration.configuration_sha256,
                configuration.policy_sha256, configuration.policy_sha256, list(configuration.settings.underlyers),
                start, as_of, as_of, as_of, as_of, as_of, as_of))
            rows = [dict(row) for row in cursor.fetchall()]
            if len(rows) > 25000:
                raise ValueError("completed-cycle lookup exceeded its bound")
            return rows

    def research_inputs(self, *, configuration, session_date, as_of, daily=False, history=False, matrix_ids=None, candidate_ids=None):
        from zoneinfo import ZoneInfo
        from options.stock_behavior_gates import STOCK_BEHAVIOR_GATE_POLICY
        from options.outcome_contracts import OptionOutcomeAvailabilityPolicy

        if as_of.tzinfo is None or session_date > as_of.astimezone(ZoneInfo("America/New_York")).date():
            raise ValueError("research review requires a nonfuture session and aware receipt cutoff")
        if (as_of.astimezone(ZoneInfo("America/New_York")).date() - session_date).days > 60:
            raise ValueError("research review is bounded to sixty days")
        start = datetime.combine(session_date, datetime.min.time(), ZoneInfo("America/New_York"))
        end = start + timedelta(days=1)
        maximum_rows = 50000 if daily or history else 5000
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""SELECT to_regclass('public.option_stock_behavior_assessments') IS NOT NULL AS stock_ready,
                to_regclass('public.option_signal_decay_outcomes') IS NOT NULL AS outcomes_ready,
                to_regclass('public.option_outcome_unavailable_evidence') IS NOT NULL AS unavailable_ready""")
            schema = dict(cursor.fetchone())
            cursor.execute("""
                WITH matrices AS (
                    SELECT analysis.matrix_id, analysis.underlying, analysis.market_time,
                           analysis.observed_time, analysis.policy_sha256 AS analysis_policy_sha256,
                           ingestion.policy_sha256 AS market_policy_sha256,
                           ingestion.configuration_sha256, ingestion.scheduled_cycle,
                           row_number() OVER (PARTITION BY analysis.underlying
                               ORDER BY analysis.market_time DESC, analysis.observed_time DESC, analysis.matrix_id) AS ordinal
                    FROM option_analysis_runs AS analysis
                    JOIN option_ingestion_runs AS ingestion USING(batch_id)
                    WHERE analysis.status='COMPLETE' AND analysis.policy_sha256=%s
                      AND ingestion.configuration_sha256=%s AND ingestion.policy_sha256=%s
                      AND analysis.market_time >= %s AND analysis.market_time < %s
                      AND analysis.observed_time<=%s AND analysis.completed_at<=%s
                      AND analysis.created_at<=%s AND analysis.underlying=ANY(%s)
                      AND (%s::uuid[] IS NULL OR analysis.matrix_id=ANY(%s::uuid[]))
                )
                SELECT candidate.candidate_id, candidate.candidate_identity, candidate.matrix_id,
                       candidate.underlying, candidate.strategy_name, candidate.strategy_version,
                       registry.display_name, candidate.structure_type, candidate.candidate_kind,
                       candidate.status, candidate.candidate_rank, candidate.policy_sha256,
                       candidate.market_data_time, candidate.observed_time, candidate.valid_until,
                       candidate.expiration_date, candidate.net_premium, candidate.capital_at_risk,
                       candidate.management_policy, candidate.management_policy_version,
                       candidate.maximum_loss, candidate.maximum_profit,
                       candidate.persona_tags, candidate.reason_codes,
                       candidate.expiration_date - (candidate.market_data_time AT TIME ZONE 'America/New_York')::date AS calendar_dte,
                       matrices.configuration_sha256, matrices.market_policy_sha256,
                       matrices.analysis_policy_sha256, matrices.scheduled_cycle,
                       COALESCE(legs.payload, '[]'::jsonb) AS legs
                FROM matrices JOIN option_strategy_candidates AS candidate USING(matrix_id)
                JOIN option_strategy_registry AS registry
                  ON registry.strategy_name=candidate.strategy_name AND registry.strategy_version=candidate.strategy_version
                LEFT JOIN LATERAL (
                    SELECT jsonb_agg(jsonb_build_object(
                        'leg_index',leg.leg_index,'contract_id',leg.contract_id,'contract_ticker',leg.contract_ticker,
                        'side',leg.side,'ratio',leg.ratio,'multiplier',leg.multiplier,'strike',leg.strike,
                        'contract_type',leg.contract_type,'expiration_date',leg.expiration_date,
                                                'model_mark',leg.model_mark,'source_market_time',leg.source_market_time,
                                                'valuation_policy_sha256',leg.valuation_policy_sha256,
                                                'spot',leg.spot,'local_iv',leg.local_iv,'local_delta',leg.local_delta,
                                                'local_gamma',leg.local_gamma,'local_theta_per_day',leg.local_theta_per_day,
                                                'local_vega_per_vol_point',leg.local_vega_per_vol_point,'local_rho_per_rate_point',leg.local_rho_per_rate_point,
                                                'mark_source',leg.mark_source,'day_volume',entry_snapshot.day_volume,'open_interest',entry_snapshot.open_interest,
                                                'quote_bid',leg.quote_bid,'quote_ask',leg.quote_ask
                    ) ORDER BY leg.leg_index) AS payload
                                        FROM option_candidate_legs AS leg
                                        LEFT JOIN option_chain_snapshots AS entry_snapshot
                                            ON entry_snapshot.snapshot_id=leg.snapshot_id AND entry_snapshot.contract_id=leg.contract_id
                                            AND entry_snapshot.first_observed_at<=candidate.observed_time
                                            AND entry_snapshot.market_data_time<=candidate.market_data_time AND NOT %s
                                        WHERE leg.candidate_id=candidate.candidate_id
                ) AS legs ON TRUE
                WHERE (%s OR matrices.ordinal=1) AND candidate.policy_sha256=%s
                                    AND candidate.observed_time<=%s AND candidate.created_at<=%s AND candidate.status='SELECTED'
                                    AND (%s::uuid[] IS NULL OR candidate.candidate_id=ANY(%s::uuid[]))
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT','MULTI_LEG')
                ORDER BY candidate.market_data_time, candidate.underlying, candidate.strategy_name,
                         candidate.candidate_rank, candidate.candidate_id LIMIT %s
            """, (configuration.policy_sha256, configuration.configuration_sha256, configuration.policy_sha256,
                start, end, as_of, as_of, as_of, list(configuration.settings.underlyers), matrix_ids, matrix_ids,
                daily or history, daily or history or matrix_ids is not None, configuration.strategy_policy_sha256,
                as_of, as_of, candidate_ids, candidate_ids, maximum_rows + 1))
            candidates = [dict(row) for row in cursor.fetchall()]
            if len(candidates) > maximum_rows:
                raise ValueError("research candidate read bound exceeded; no truncated scorecard")
            ids = [row["candidate_id"] for row in candidates]
            assessments, outcomes, unavailable = {}, [], []
            if ids and schema["stock_ready"]:
                cursor.execute("""SELECT DISTINCT ON(candidate_id) candidate_id, payload_sha256, recorded_at,
                        CASE WHEN octet_length(payload_text)<=262144 THEN payload_text END AS payload_text
                    FROM option_stock_behavior_assessments
                    WHERE candidate_id=ANY(%s::uuid[]) AND detector_policy_version=%s AND detector_policy_sha256=%s
                      AND recorded_at<=%s ORDER BY candidate_id, decision_at DESC, recorded_at DESC, assessment_id""",
                    (ids, STOCK_BEHAVIOR_GATE_POLICY.version, STOCK_BEHAVIOR_GATE_POLICY.sha256, as_of))
                payload_bytes = 0
                for record in cursor:
                    payload_bytes += len((record["payload_text"] or "").encode("utf-8"))
                    if payload_bytes > 134217728:
                        raise ValueError("research assessment payload budget exceeded")
                    assessments[str(record["candidate_id"])] = dict(record)
            if ids and daily and schema["outcomes_ready"]:
                cursor.execute("""SELECT candidate_id, measurement_type, net_return, net_pnl, estimated_cost,
                        market_time, observed_time FROM option_signal_decay_outcomes
                    WHERE candidate_id=ANY(%s::uuid[]) AND measurement_type=ANY(%s)
                      AND valuation_policy_sha256=%s AND created_at<=%s AND observed_time<=%s""",
                    (ids, ['60MIN', 'CLOSE', 'NEXT_OPEN'], configuration.valuation_policy_sha256, as_of, as_of))
                outcomes = [dict(row) for row in cursor.fetchall()]
            if ids and daily and schema["unavailable_ready"]:
                cursor.execute("""SELECT candidate_id, measurement_type FROM option_outcome_unavailable_evidence
                    WHERE candidate_id=ANY(%s::uuid[]) AND valuation_policy_sha256=%s
                      AND availability_policy_sha256=%s AND recorded_at<=%s""",
                    (ids, configuration.valuation_policy_sha256, OptionOutcomeAvailabilityPolicy().sha256, as_of))
                unavailable = [dict(row) for row in cursor.fetchall()]
        return dict(candidates=candidates, assessments=assessments, outcomes=outcomes, unavailable=unavailable, schema=schema)

    def page_leg_activity(self, candidate_ids):
        ids = sorted({UUID(str(value)) for value in candidate_ids}, key=str)
        if not 1 <= len(ids) <= 200:
            raise ValueError("activity enrichment requires 1-200 candidates")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute("""
                SELECT leg.candidate_id, leg.leg_index, snapshot.day_volume, snapshot.open_interest
                FROM option_candidate_legs AS leg
                JOIN option_strategy_candidates AS candidate USING(candidate_id)
                LEFT JOIN option_chain_snapshots AS snapshot
                  ON snapshot.snapshot_id=leg.snapshot_id AND snapshot.contract_id=leg.contract_id
                 AND snapshot.first_observed_at<=candidate.observed_time
                 AND snapshot.market_data_time<=candidate.market_data_time
                WHERE leg.candidate_id=ANY(%s::uuid[])
            """, (ids,))
            return {(str(row["candidate_id"]), row["leg_index"]):
                {key: row[key] for key in ("day_volume", "open_interest")} for row in cursor.fetchall()}

    def get_matrix_lineage(self, matrix_id: UUID) -> OptionMatrixAssessmentLineage | None:
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                """
                SELECT analysis.matrix_id, analysis.underlying,
                       analysis.market_time, analysis.observed_time,
                       ingestion.scheduled_cycle, ingestion.configuration_sha256,
                       ingestion.policy_sha256 AS market_policy_sha256,
                       analysis.policy_sha256 AS analysis_policy_sha256
                FROM option_analysis_runs AS analysis
                JOIN option_ingestion_runs AS ingestion USING (batch_id)
                WHERE analysis.matrix_id = %s
                """,
                (matrix_id,),
            )
            row = cursor.fetchone()
        return OptionMatrixAssessmentLineage(**row) if row else None

    def status(self, since: datetime, until: datetime) -> StockBehaviorAssessmentStatus:
        if since.tzinfo is None or until.tzinfo is None:
            raise ValueError("stock behavior assessment status bounds must be timezone-aware")
        if not since < until or until - since > timedelta(days=31):
            raise ValueError("stock behavior assessment status window must be in (0, 31 days]")
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                """
                SELECT COUNT(*) AS assessment_count,
                       COUNT(DISTINCT candidate_id) AS candidate_count,
                       COUNT(*) FILTER (WHERE disposition = 'ELIGIBLE_RESEARCH') AS eligible_count,
                       COUNT(*) FILTER (WHERE disposition = 'BLOCKED') AS blocked_count,
                       COUNT(*) FILTER (WHERE disposition = 'UNAVAILABLE') AS unavailable_count,
                       MAX(decision_at) AS latest_decision_at,
                       MAX(recorded_at) AS latest_recorded_at
                FROM option_stock_behavior_assessments
                WHERE recorded_at >= %s AND recorded_at < %s
                """,
                (since, until),
            )
            row = cursor.fetchone()
        return StockBehaviorAssessmentStatus(since=since, until=until, **row)

    def launch_usage(
        self, since: datetime, until: datetime, launch_manifest_sha256: str,
    ) -> StockBehaviorShadowLaunchUsage:
        with self._cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '5s'")
            cursor.execute(
                """
                SELECT COUNT(*) AS assessment_count,
                       COALESCE(SUM(octet_length(payload_text)), 0) AS payload_bytes,
                       COUNT(*) FILTER (WHERE disposition = 'UNAVAILABLE') AS unavailable_count,
                       percentile_cont(0.95) WITHIN GROUP (
                           ORDER BY EXTRACT(EPOCH FROM recorded_at - decision_at)
                       ) AS p95_decision_lag_seconds
                FROM option_stock_behavior_assessments
                WHERE decision_at >= %s AND decision_at < %s
                                    AND launch_manifest_sha256 = %s
                """,
                                (since, until, launch_manifest_sha256),
            )
            row = cursor.fetchone()
        return StockBehaviorShadowLaunchUsage(**row)

    def persist(
        self, assessments: Sequence[StockBehaviorGateAssessment],
    ) -> StockBehaviorAssessmentPersistResult:
        rows = tuple(assessments)
        if not rows:
            return StockBehaviorAssessmentPersistResult(0, 0)
        ids = tuple(stock_behavior_assessment_id(row) for row in rows)
        if len(set(ids)) != len(ids):
            raise ValueError("stock behavior assessments must be distinct")
        if any(
            row.candidate_identity_sha256 is None
            or row.matrix_id is None
            or row.disposition == "NOT_APPLICABLE"
            or row.option_strategy_version is None
            or row.option_strategy_policy_sha256 is None
            or row.option_configuration_sha256 is None
            or row.option_market_policy_sha256 is None
            or row.option_analysis_policy_sha256 is None
            or row.option_market_time is None
            or row.option_observed_at is None
            or row.stock_market_cutoff is None
            or row.launch_id is None
            or row.launch_manifest_sha256 is None
            for row in rows
        ):
            raise ValueError(
                "persisted stock behavior assessments require exact applicable candidates"
            )
        payloads = {assessment_id: row.canonical_json() for assessment_id, row in zip(ids, rows)}
        with self._cursor() as cursor:
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            cursor.execute(
                "SELECT to_regclass('option_stock_behavior_assessments') IS NOT NULL AS ready"
            )
            if not cursor.fetchone()["ready"]:
                raise RuntimeError(
                    "stock behavior assessment persistence requires migration 046"
                )
            inserted = execute_values(
                cursor,
                """
                INSERT INTO option_stock_behavior_assessments (
                    assessment_id, candidate_id, candidate_identity, matrix_id,
                    stock_snapshot_id, underlying, launch_id,
                    launch_manifest_sha256, option_strategy_version,
                    option_strategy_policy_sha256, option_configuration_sha256,
                    option_market_policy_sha256, option_analysis_policy_sha256,
                    option_market_time, option_observed_at, stock_market_cutoff,
                    detector_policy_version,
                    detector_policy_sha256, decision_at, disposition,
                    payload_text, payload_sha256
                ) VALUES %s
                ON CONFLICT (assessment_id) DO NOTHING
                RETURNING assessment_id
                """,
                [
                    (
                        assessment_id, row.candidate_id,
                        row.candidate_identity_sha256, row.matrix_id,
                        row.stock_snapshot_id, row.underlyer,
                        row.launch_id, row.launch_manifest_sha256,
                        row.option_strategy_version,
                        row.option_strategy_policy_sha256,
                        row.option_configuration_sha256,
                        row.option_market_policy_sha256,
                        row.option_analysis_policy_sha256,
                        row.option_market_time, row.option_observed_at,
                        row.stock_market_cutoff,
                        row.detector_policy_version, row.detector_policy_sha256,
                        row.decision_at, row.disposition,
                        payloads[assessment_id], row.sha256,
                    )
                    for assessment_id, row in zip(ids, rows)
                ],
                fetch=True,
            )
            cursor.execute(
                """
                SELECT assessment_id, candidate_id, candidate_identity, matrix_id,
                        stock_snapshot_id, underlying, launch_id,
                        launch_manifest_sha256, option_strategy_version,
                      option_strategy_policy_sha256, option_configuration_sha256,
                      option_market_policy_sha256, option_analysis_policy_sha256,
                      option_market_time, option_observed_at, stock_market_cutoff,
                      detector_policy_version,
                       detector_policy_sha256, decision_at, disposition,
                       payload_text, payload_sha256
                FROM option_stock_behavior_assessments
                WHERE assessment_id = ANY(%s::uuid[])
                """,
                (list(ids),),
            )
            stored = {row["assessment_id"]: row for row in cursor.fetchall()}
            if set(stored) != set(ids):
                raise ValueError("stored stock behavior assessments are incomplete")
            for assessment_id, assessment in zip(ids, rows):
                row = stored[assessment_id]
                if (
                    row["candidate_id"] != assessment.candidate_id
                    or row["candidate_identity"] != assessment.candidate_identity_sha256
                    or row["matrix_id"] != assessment.matrix_id
                    or row["stock_snapshot_id"] != assessment.stock_snapshot_id
                    or row["underlying"] != assessment.underlyer
                    or row["launch_id"] != assessment.launch_id
                    or row["launch_manifest_sha256"] != assessment.launch_manifest_sha256
                    or row["option_strategy_version"] != assessment.option_strategy_version
                    or row["option_strategy_policy_sha256"] != assessment.option_strategy_policy_sha256
                    or row["option_configuration_sha256"] != assessment.option_configuration_sha256
                    or row["option_market_policy_sha256"] != assessment.option_market_policy_sha256
                    or row["option_analysis_policy_sha256"] != assessment.option_analysis_policy_sha256
                    or row["option_market_time"] != assessment.option_market_time
                    or row["option_observed_at"] != assessment.option_observed_at
                    or row["stock_market_cutoff"] != assessment.stock_market_cutoff
                    or row["detector_policy_version"] != assessment.detector_policy_version
                    or row["detector_policy_sha256"] != assessment.detector_policy_sha256
                    or row["decision_at"] != assessment.decision_at
                    or row["disposition"] != assessment.disposition
                    or row["payload_text"] != payloads[assessment_id]
                    or row["payload_sha256"] != assessment.sha256
                ):
                    raise ValueError(
                        "stored stock behavior assessment differs from requested evidence"
                    )
        return StockBehaviorAssessmentPersistResult(
            inserted=len(inserted), existing=len(rows) - len(inserted),
        )