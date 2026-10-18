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
            "SELECT current.instrument_id,revision.bar_start,"
            "revision.bar_end,revision.bar_revision_id,"
            "revision.payload_sha256,"
            "revision.source_kind,revision.quality_codes,"
            "revision.journal_generation,revision.stream_session_id,"
            "revision.source_offset,revision.ingestion_segment_id "
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
            "AND revision.session_date=%s "
            "AND revision.is_final "
            "AND current.bar_start>=%s AND current.bar_end<=%s "
            "ORDER BY current.instrument_id,revision.bar_start",
            (
                str(plan.cohort_id),
                list(members),
                window_start.date(),
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
                        row[5] == "REALTIME_STREAM"
                        and row[7] is not None
                        and row[8] is not None
                        and row[9] is not None
                        and int(row[9]) > 0
                        and row[10] is None
                    )
                    or (
                        row[5] == "NATIVE_REST"
                        and "EXPLICIT_ADVANCED_REST_REPAIR_V1"
                        in tuple(row[6] or ())
                        and row[7] is None
                        and row[8] is None
                        and row[9] is None
                        and row[10] is not None
                    )
                )
                for row in bars
            )
        ):
            raise ValueError(
                "Advanced input gate lacks exact stream/repair bar coverage"
            )
        stream_bars = tuple(
            row for row in bars if row[5] == "REALTIME_STREAM"
        )
        if not stream_bars:
            raise ValueError(
                "Advanced input gate lacks fenced stream lineage"
            )
        session_ids = tuple(
            sorted({str(row[8]) for row in stream_bars})
        )
        cursor.execute(
            "SELECT session_id,journal_generation,owner_epoch,status,"
            "subscribed_at,ended_at,event_time_watermark,"
            "received_byte_count,decoded_byte_ceiling,"
            "advanced_ingress_plan_id "
            "FROM rt.stream_sessions "
            "WHERE manifest_id=%s AND feed='STOCKS' "
            "AND session_id=ANY(%s::uuid[]) "
            "ORDER BY owner_epoch,started_at,session_id",
            (str(scope[0]), list(session_ids)),
        )
        sessions = tuple(cursor.fetchall())
        sessions_by_id = {str(row[0]): row for row in sessions}
        lineage = {
            (str(row[8]), str(row[7]))
            for row in stream_bars
        }
        if (
            len(sessions_by_id) != len(session_ids)
            or {
                (str(row[0]), str(row[1]))
                for row in sessions
            }
            != lineage
            or any(
                row[3]
                not in {"LIVE", "STOPPING", "STOPPED", "FAILED"}
                or row[4] is None
                or row[4] >= window.cutoff
                or row[6] is None
                or int(row[7]) > int(row[8])
                or row[9] is None
                for row in sessions
            )
        ):
            raise ValueError(
                "Advanced input gate lacks fenced stream finality"
            )
        ingress_plan_ids = {str(row[9]) for row in sessions}
        if len(ingress_plan_ids) != 1:
            raise ValueError(
                "Advanced input gate spans multiple ingress plans"
            )
        ingress_plan_id = next(iter(ingress_plan_ids))
        maxima = {}
        for row in stream_bars:
            key = (str(row[8]), str(row[7]))
            prior = maxima.get(key)
            candidate = (int(row[9]), row[2])
            maxima[key] = (
                candidate
                if prior is None
                else (max(prior[0], candidate[0]), max(prior[1], candidate[1]))
            )
        generations = tuple(
            sorted({generation for _session, generation in lineage})
        )
        cursor.execute(
            "SELECT journal_generation,stream_session_id,owner_epoch,"
            "plan_id,status,shard_ids,shard_map_sha256 "
            "FROM rt.advanced_journal_registry "
            "WHERE journal_generation=ANY(%s::uuid[]) "
            "ORDER BY owner_epoch,journal_generation",
            (list(generations),),
        )
        journals = tuple(cursor.fetchall())
        journals_by_generation = {str(row[0]): row for row in journals}
        if (
            len(journals_by_generation) != len(generations)
            or any(
                str(row[4]) not in {"WRITING", "DRAINING", "DRAINED"}
                or str(row[3]) != ingress_plan_id
                or (str(row[1]), str(row[0])) not in lineage
                or int(row[2])
                != int(sessions_by_id[str(row[1])][2])
                for row in journals
            )
        ):
            raise ValueError(
                "Advanced input gate journal lineage is unsafe"
            )
        cursor.execute(
            "SELECT checkpoint.journal_generation,"
            "checkpoint.owner_epoch,checkpoint.committed_offset,"
            "convert_from(checkpoint.state_payload,'UTF8')::jsonb"
            "->>'stream_session_id',"
            "(convert_from(checkpoint.state_payload,'UTF8')::jsonb"
            "->>'event_time_watermark')::timestamptz,"
            "checkpoint.shard_id,checkpoint.shard_map_sha256,"
            "checkpoint.checkpoint_id,checkpoint.state_hash "
            "FROM rt.consumer_checkpoints AS checkpoint "
            "WHERE checkpoint.cohort_id=%s "
            "AND checkpoint.journal_generation=ANY(%s::uuid[]) "
            "ORDER BY checkpoint.journal_generation,checkpoint.shard_id",
            (str(plan.cohort_id), list(generations)),
        )
        checkpoints = tuple(cursor.fetchall())
        checkpoints_by_generation = {
            generation: tuple(
                row for row in checkpoints if str(row[0]) == generation
            )
            for generation in generations
        }
        checkpoint_evidence = []
        for session_id, generation in lineage:
            own_session = sessions_by_id[session_id]
            journal = journals_by_generation[generation]
            own_checkpoints = checkpoints_by_generation[generation]
            maximum_offset, maximum_bar_end = maxima[
                (session_id, generation)
            ]
            if (
                tuple(int(value) for value in journal[5])
                != tuple(int(row[5]) for row in own_checkpoints)
                or not own_checkpoints
                or any(
                    int(row[1]) != int(own_session[2])
                    or int(row[2]) < maximum_offset
                    or str(row[3]) != session_id
                    or row[4] < maximum_bar_end
                    or str(row[6]) != str(journal[6])
                    for row in own_checkpoints
                )
            ):
                raise ValueError(
                    "Advanced input gate checkpoint coverage is incomplete"
                )
            checkpoint_evidence.extend(
                (
                    generation,
                    int(own_session[2]),
                    int(row[2]),
                    session_id,
                    row[4],
                    int(row[5]),
                    str(journal[6]),
                    str(row[7]),
                    str(row[8]),
                )
                for row in own_checkpoints
            )
        repair_segments = tuple(
            sorted({str(row[10]) for row in bars if row[10] is not None})
        )
        repair_evidence = ()
        if repair_segments:
            cursor.execute(
                "SELECT batch.acquisition_batch_id,request.request_id,"
                "request.state,request.cohort_id,gap.plan_id,"
                "gap.recovery_status,gap.repair_status "
                "FROM rt.acquisition_batches AS batch "
                "JOIN rt.provider_request_ledger AS request "
                "ON request.request_id=batch.request_id "
                "JOIN rt.stream_gaps AS gap "
                "ON gap.repair_reference=request.request_id "
                "WHERE batch.acquisition_batch_id=ANY(%s::uuid[]) "
                "ORDER BY batch.acquisition_batch_id",
                (list(repair_segments),),
            )
            repair_evidence = tuple(cursor.fetchall())
            if (
                {str(row[0]) for row in repair_evidence}
                != set(repair_segments)
                or any(
                    row[2] != "COMPLETE"
                    or str(row[3]) != str(plan.cohort_id)
                    or str(row[4]) != ingress_plan_id
                    or row[5] not in {"REPAIRED", "WAIVED"}
                    or row[6] not in {"VERIFIED", "REPAIRED"}
                    for row in repair_evidence
                )
            ):
                raise ValueError(
                    "Advanced input gate repair authorization is incomplete"
                )
        cursor.execute(
            "SELECT count(*) FROM rt.stream_gaps "
            "WHERE plan_id=%s "
            "AND gap_start<%s AND COALESCE(gap_end,%s)>%s "
            "AND recovery_status NOT IN ('REPAIRED','WAIVED')",
            (
                ingress_plan_id,
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
            "window_start": window_start,
            "cutoff": window.cutoff,
            "members": members,
            "ingress_plan_id": ingress_plan_id,
            "sessions": tuple(
                (
                    str(row[0]),
                    str(row[1]),
                    int(row[2]),
                    row[4],
                    str(row[9]),
                )
                for row in sessions
            ),
            "journals": tuple(
                (
                    str(row[0]),
                    str(row[1]),
                    int(row[2]),
                    str(row[3]),
                    tuple(int(value) for value in row[5]),
                    str(row[6]),
                )
                for row in journals
            ),
            "checkpoints": tuple(sorted(checkpoint_evidence)),
            "repair_evidence": repair_evidence,
            "overlapping_gap_verdict": "CLEAR",
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
