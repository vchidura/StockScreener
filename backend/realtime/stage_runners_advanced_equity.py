from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from .contracts import canonical_sha256
from .database_access import RealtimeDatabaseRole
from .runner_environment import RunnerEnvironment
from .stage_database_v2 import V2StageDatabase
from .stage_orchestrator import StageContext, StageOutcome, StageRunner


ADVANCED_INPUT_NOT_READY = "ADVANCED_INPUT_NOT_READY"


def advanced_input_gate_identity(
    database: V2StageDatabase,
    context: StageContext,
) -> str:
    plan, window = context.plan, context.window
    window_start = window.cutoff - timedelta(minutes=5)
    with database.transaction(read_only=True) as cursor:
        cursor.execute(
            "SELECT cohort.manifest_id,manifest.stock_count,"
            "profile.provider_tier "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
            "JOIN rt.capability_profiles AS profile USING(profile_id) "
            "WHERE cohort.cohort_id=%s AND profile.status='PASS' "
            "AND profile.expires_at>%s",
            (str(plan.cohort_id), window.cutoff),
        )
        scope = cursor.fetchone()
        if (
            scope is None
            or int(scope[1]) <= 0
            or scope[2] != "MASSIVE_ADVANCED"
        ):
            raise ValueError(
                "Advanced input gate requires an Advanced cohort"
            )
        cursor.execute(
            "SELECT member.instrument_id "
            "FROM rt.subscription_members AS member "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "WHERE member.manifest_id=%s AND member.channel='AM' "
            "AND instrument.asset_class='EQUITY' "
            "ORDER BY member.instrument_id",
            (str(scope[0]),),
        )
        members = tuple(int(row[0]) for row in cursor.fetchall())
        if len(members) != int(scope[1]) or len(set(members)) != len(members):
            raise ValueError(
                "Advanced input gate AM membership is not exact"
            )
        cursor.execute(
            "SELECT session_id,journal_generation,owner_epoch,status,"
            "subscribed_at,ended_at,event_time_watermark,"
            "received_byte_count,decoded_byte_ceiling "
            "FROM rt.stream_sessions "
            "WHERE manifest_id=%s AND feed='STOCKS' "
            "AND journal_generation IS NOT NULL "
            "AND subscribed_at<%s "
            "AND (ended_at IS NULL OR ended_at>=%s) "
            "ORDER BY owner_epoch DESC,started_at DESC LIMIT 1",
            (str(scope[0]), window.cutoff, window.cutoff),
        )
        session = cursor.fetchone()
        if (
            session is None
            or session[3] not in {"LIVE", "STOPPING", "STOPPED"}
            or session[4] >= window.cutoff
            or session[6] is None
            or session[6] < window.cutoff
            or int(session[7]) > int(session[8])
        ):
            raise ValueError(
                "Advanced input gate lacks stream finality"
            )
        cursor.execute(
            "SELECT checkpoint.journal_generation,"
            "checkpoint.owner_epoch,checkpoint.committed_offset,"
            "convert_from(checkpoint.state_payload,'UTF8')::jsonb"
            "->>'stream_session_id',"
            "(convert_from(checkpoint.state_payload,'UTF8')::jsonb"
            "->>'event_time_watermark')::timestamptz "
            "FROM rt.consumer_checkpoints AS checkpoint "
            "WHERE checkpoint.cohort_id=%s "
            "AND checkpoint.journal_generation=%s "
            "ORDER BY checkpoint.shard_id",
            (str(plan.cohort_id), str(session[1])),
        )
        checkpoints = tuple(cursor.fetchall())
        if not checkpoints or any(
            str(row[0]) != str(session[1])
            or int(row[1]) != int(session[2])
            or int(row[2]) <= 0
            or str(row[3]) != str(session[0])
            or row[4] < window.cutoff
            for row in checkpoints
        ):
            raise ValueError(
                "Advanced input gate checkpoint coverage is incomplete"
            )
        cursor.execute(
            "SELECT current.instrument_id,revision.bar_start,"
            "revision.bar_revision_id,revision.payload_sha256,"
            "revision.source_kind,revision.quality_codes,"
            "revision.journal_generation,revision.stream_session_id,"
            "revision.source_offset "
            "FROM rt.bar_current AS current "
            "JOIN rt.bar_revisions AS revision "
            "ON revision.bar_revision_id=current.bar_revision_id "
            "AND revision.observed_at=current.observed_at "
            "AND revision.cohort_id=current.cohort_id "
            "AND revision.provider_tier=current.provider_tier "
            "AND revision.instrument_id=current.instrument_id "
            "AND revision.interval=current.interval "
            "WHERE current.cohort_id=%s "
            "AND current.provider_tier='MASSIVE_ADVANCED' "
            "AND current.instrument_id=ANY(%s) "
            "AND current.interval='1m' "
            "AND current.adjustment_basis='RAW_UNADJUSTED' "
            "AND current.session_scope='RTH' "
            "AND revision.is_final "
            "AND revision.bar_start>=%s AND revision.bar_end<=%s "
            "ORDER BY current.instrument_id,revision.bar_start",
            (
                str(plan.cohort_id),
                list(members),
                window_start,
                window.cutoff,
            ),
        )
        bars = tuple(cursor.fetchall())
        expected_starts = tuple(
            window_start + timedelta(minutes=index)
            for index in range(5)
        )
        expected_keys = {
            (instrument_id, bar_start)
            for instrument_id in members
            for bar_start in expected_starts
        }
        if (
            len(bars) != len(expected_keys)
            or {(int(row[0]), row[1]) for row in bars} != expected_keys
            or any(
                not (
                    (
                        row[4] == "REALTIME_STREAM"
                        and str(row[6]) == str(session[1])
                        and str(row[7]) == str(session[0])
                        and row[8] is not None
                        and int(row[8]) > 0
                    )
                    or (
                        row[4] == "NATIVE_REST"
                        and "EXPLICIT_ADVANCED_REST_REPAIR_V1"
                        in tuple(row[5] or ())
                        and row[6] is None
                        and row[7] is None
                        and row[8] is None
                    )
                )
                for row in bars
            )
        ):
            raise ValueError(
                "Advanced input gate lacks exact stream/repair bar coverage"
            )
        cursor.execute(
            "SELECT count(*) FROM rt.stream_gaps "
            "WHERE session_id=%s AND journal_generation=%s "
            "AND gap_start<%s AND COALESCE(gap_end,%s)>%s "
            "AND recovery_status NOT IN ('REPAIRED','WAIVED')",
            (
                str(session[0]),
                str(session[1]),
                window.cutoff,
                window.cutoff,
                window_start,
            ),
        )
        if int(cursor.fetchone()[0]) != 0:
            raise ValueError(
                "Advanced input gate has an unresolved overlapping gap"
            )
    return canonical_sha256(
        {
            "contract": "advanced-equity-input-gate-v1",
            "cohort_id": plan.cohort_id,
            "window_id": window.window_id,
            "window_start": window_start,
            "cutoff": window.cutoff,
            "members": members,
            "session": session,
            "checkpoints": checkpoints,
            "bars": bars,
        }
    )


def advanced_equity_input_gate_runner(
    _environment: RunnerEnvironment,
) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        database = V2StageDatabase(
            context,
            RealtimeDatabaseRole.COMPUTE,
        )
        try:
            identity = advanced_input_gate_identity(database, context)
        except ValueError:
            return StageOutcome.blocked(ADVANCED_INPUT_NOT_READY)
        return StageOutcome.complete(evidence_ref=identity)

    return run
