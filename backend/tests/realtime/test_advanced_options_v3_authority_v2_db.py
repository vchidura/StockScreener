from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import psycopg2
import pytest

from realtime.contracts import canonical_sha256
from realtime.advanced_options_v3_authority import (
    AdvancedOptionsWindowStageEffectAuthorizer,
    advanced_options_authority_status_cursor,
    build_window_cycle_authorization,
    consume_session_acquisition_authority_cursor,
    consume_window_cycle_authority_cursor,
    persist_session_acquisition_authorization_cursor,
    persist_window_cycle_authorization_cursor,
    TargetWindowAgePolicy,
)
from realtime.advanced_options_resident_barrier import (
    ResidentCompletion,
    ResidentCompletionBarrierState,
    ResidentCompletionMember,
    ResidentCompletionStatus,
    evaluate_resident_completion_barrier_cursor,
    record_resident_completion_cursor,
)
from realtime.database_access_v2 import (
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.orchestration_repository import (
    PostgresOrchestrationRepository,
    WindowSpec,
)
from realtime.stage_graph import STAGES_V5
from tests.realtime.test_advanced_options_v3_authority import (
    OPENING,
    _authority_bundle,
)
from tests.realtime.test_option_slot_sets_v2_db import cursor
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)


pytestmark = requires_v2_database


def _service_plan(
    cursor,
    cohort_id,
    session_date,
    enabled_stages,
    *,
    catalog_version="stages_v4",
):
    plan_id = uuid4()
    cursor.execute(
        "INSERT INTO rt.service_plans ("
        "plan_id,cohort_id,session_date,stage_catalog_version,"
        "enabled_stages,config_sha256,deadline,scope_reference"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(plan_id),
            str(cohort_id),
            session_date,
            catalog_version,
            sorted(enabled_stages),
            uuid4().hex + uuid4().hex,
            OPENING + timedelta(hours=8),
            "V3 authority disposable qualification",
        ),
    )
    return plan_id


def test_continuous_session_windows_are_deterministic_and_failure_isolated():
    with disposable_v2_database(
        "test_realtime_v2_options_v3_continuous"
    ) as pool:
        with cursor(pool) as database_cursor:
            cohort_id = insert_cohort(database_cursor, OPENING)
            plan_id = _service_plan(
                database_cursor,
                cohort_id,
                OPENING.date(),
                {"OPTIONS_INPUT_GATE"},
                catalog_version="stages_v5",
            )
            database_cursor.connection.commit()
        provider = V2ConnectionProvider(
            {OWNER_POOL: pool},
            V2AccessConfig(allow_disposable=True),
        )
        repository = PostgresOrchestrationRepository(provider)
        specs = tuple(
            WindowSpec(
                cohort_id,
                "MASSIVE_ADVANCED",
                f"V3-{index}",
                OPENING + timedelta(minutes=index * 5),
                OPENING + timedelta(minutes=index * 5),
                OPENING + timedelta(minutes=index * 5 + 2),
            )
            for index in range(1, 5)
        )
        windows = tuple(
            repository.ensure_window(plan_id, spec)
            for spec in specs
        )

        assert [row.window_id for row in windows] == [
            spec.window_id for spec in specs
        ]
        assert len({row.window_id for row in windows}) == 4
        generation = uuid4()
        with cursor(pool) as database_cursor:
            for index, window in enumerate(windows, start=1):
                offset = index * 10
                for owner_epoch, member in enumerate(
                    ResidentCompletionMember,
                    start=1,
                ):
                    evidence = {
                        "status": "COMPLETE",
                        "kind": member.value,
                        **(
                            {
                                "raw_end_offset": offset,
                                "normalized_end_offset": offset,
                                "connection_attempts": 1,
                            }
                            if member is ResidentCompletionMember.INGRESS
                            else {
                                "normalized_end_offset": offset,
                                "state_sha256": "a" * 64,
                                "shard_map_sha256": "b" * 64,
                            }
                            if member
                            is ResidentCompletionMember.MARKET_STATE
                            else {
                                "raw_end_offset": offset,
                                "checkpoint_end_offset": offset,
                                "owner_epoch": owner_epoch,
                                "evidence_ref": f"flow:{offset}",
                            }
                        ),
                        "effects": {
                            "dispatch": False,
                            "delivery": False,
                            "broker": False,
                            "trading": False,
                        },
                    }
                    record_resident_completion_cursor(
                        database_cursor,
                        ResidentCompletion(
                            target_window_id=window.window_id,
                            target_cohort_id=cohort_id,
                            member=member,
                            journal_generation=generation,
                            owner_epoch=owner_epoch,
                            ending_offset=offset,
                            evidence_sha256=canonical_sha256(evidence),
                            evidence=evidence,
                            status=ResidentCompletionStatus.COMPLETE,
                            reason_codes=(),
                            completed_at=window.cutoff,
                        ),
                    )
                verdict = evaluate_resident_completion_barrier_cursor(
                    database_cursor,
                    target_window_id=window.window_id,
                    target_cohort_id=cohort_id,
                    journal_generation=generation,
                )
                assert (
                    verdict.state
                    is ResidentCompletionBarrierState.COMPLETE
                )
            database_cursor.connection.commit()

        owner_id = uuid4()
        first = repository.claim_window(
            windows[0].window_id,
            owner_id,
            windows[0].release_at,
            60,
        )
        assert first is not None
        repository.finish_window(
            first,
            "COMPLETE",
            windows[0].release_at,
        )
        assert repository.claim_window(
            windows[0].window_id,
            owner_id,
            windows[0].release_at,
            60,
        ) is None

        deferred = repository.claim_window(
            windows[1].window_id,
            owner_id,
            windows[1].release_at,
            60,
        )
        assert deferred is not None
        repository.release_window(deferred, windows[1].release_at)
        retried = repository.claim_window(
            windows[1].window_id,
            owner_id,
            windows[1].release_at + timedelta(seconds=1),
            60,
        )
        assert retried is not None
        repository.finish_window(
            retried,
            "COMPLETE",
            windows[1].release_at + timedelta(seconds=1),
        )

        failed = repository.claim_window(
            windows[2].window_id,
            owner_id,
            windows[2].release_at,
            60,
        )
        assert failed is not None
        repository.finish_window(
            failed,
            "FAILED",
            windows[2].release_at,
        )
        next_claim = repository.claim_window(
            windows[3].window_id,
            owner_id,
            windows[3].release_at,
            60,
        )
        assert next_claim is not None
        repository.finish_window(
            next_claim,
            "COMPLETE",
            windows[3].release_at,
        )


def test_split_authorities_are_one_use_idempotent_and_cleanup_with_database():
    with disposable_v2_database(
        "test_realtime_v2_options_v3_authorities"
    ) as pool:
        with cursor(pool) as database_cursor:
            target_cohort = insert_cohort(database_cursor, OPENING)
            equity_cohort = insert_cohort(database_cursor, OPENING)
            history_cohort = insert_cohort(database_cursor, OPENING)
            equity_plan = _service_plan(
                database_cursor,
                equity_cohort,
                OPENING.date(),
                {"EQUITY_INPUT_GATE", "EQUITY_MATERIALIZE"},
            )
            target_plan = _service_plan(
                database_cursor,
                target_cohort,
                OPENING.date(),
                {"OPTIONS_INPUT_GATE", "OPTIONS_CYCLE"},
                catalog_version="stages_v5",
            )
            equity_window = uuid4()
            target_window = uuid4()
            equity_receipt = uuid4()
            equity_cutoff = OPENING + timedelta(minutes=10)
            options_cutoff = equity_cutoff + timedelta(minutes=2)
            database_cursor.execute(
                "INSERT INTO rt.window_runs ("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state"
                ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
                (
                    str(equity_window),
                    str(equity_plan),
                    str(equity_cohort),
                    f"v3-equity-{equity_window}",
                    equity_cutoff,
                    equity_cutoff,
                    equity_cutoff + timedelta(minutes=5),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.cycle_receipts ("
                "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                "reference_generation,universe_sha256,input_sha256,build_id,"
                "created_at"
                ") VALUES (%s,%s,%s,'INPUT_READY',%s,7,%s,%s,%s,%s)",
                (
                    str(equity_receipt),
                    str(equity_window),
                    str(equity_cohort),
                    equity_cutoff,
                    "8" * 64,
                    "9" * 64,
                    "advanced-equity-v3-authority-test-v1",
                    equity_cutoff,
                ),
            )
            database_cursor.execute(
                "SELECT created_at FROM rt.cycle_receipts "
                "WHERE receipt_id=%s",
                (str(equity_receipt),),
            )
            equity_receipt_created_at = database_cursor.fetchone()[0]
            database_cursor.execute(
                "INSERT INTO rt.window_runs ("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state"
                ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'PENDING')",
                (
                    str(target_window),
                    str(target_plan),
                    str(target_cohort),
                    f"v3-options-{target_window}",
                    options_cutoff,
                    options_cutoff,
                    options_cutoff + timedelta(minutes=5),
                ),
            )
            database_cursor.execute(
                "SELECT cohort.cohort_sha256,manifest.manifest_id::text,"
                "manifest.manifest_sha256,profile.profile_id::text,"
                "profile.profile_sha256 "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                "JOIN rt.capability_profiles AS profile USING(profile_id) "
                "WHERE cohort.cohort_id=%s",
                (str(equity_cohort),),
            )
            equity_identity = database_cursor.fetchone()
            database_cursor.execute(
                "SELECT manifest.manifest_sha256 "
                "FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                "WHERE cohort.cohort_id=%s",
                (str(history_cohort),),
            )
            history_manifest_sha256 = database_cursor.fetchone()[0]
            database_cursor.execute(
                "INSERT INTO rt.cohort_history_bindings ("
                "target_cohort_id,source_cohort_id,source_provider_tier,"
                "history_before,source_manifest_sha256,"
                "authorization_reference,binding_sha256"
                ") VALUES (%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s)",
                (
                    str(target_cohort),
                    str(history_cohort),
                    options_cutoff,
                    history_manifest_sha256,
                    "V3-AUTHORITY-HISTORY-TEST",
                    "5" * 64,
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.advanced_options_equity_input_bindings ("
                "target_options_window_id,target_cohort_id,source_cohort_id,"
                "source_cohort_sha256,source_manifest_id,"
                "source_manifest_sha256,source_profile_id,"
                "source_profile_sha256,equity_window_id,equity_receipt_id,"
                "equity_cutoff,reference_generation,universe_sha256,"
                "input_sha256,build_id,maximum_age_seconds,"
                "authorization_reference,binding_sha256"
                ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,7,%s,%s,%s,"
                "300,%s,%s)",
                (
                    str(target_window),
                    str(target_cohort),
                    str(equity_cohort),
                    equity_identity[0],
                    equity_identity[1],
                    equity_identity[2],
                    equity_identity[3],
                    equity_identity[4],
                    str(equity_window),
                    str(equity_receipt),
                    equity_cutoff,
                    "8" * 64,
                    "9" * 64,
                    "advanced-equity-v3-authority-test-v1",
                    "V3-AUTHORITY-EQUITY-TEST",
                    "6" * 64,
                ),
            )
            bundle = _authority_bundle(
                cohort_id=target_cohort,
                target_window_id=target_window,
                equity_receipt_id=equity_receipt,
                equity_receipt_created_at=equity_receipt_created_at,
            )

            assert persist_session_acquisition_authorization_cursor(
                database_cursor,
                bundle.session,
            )
            assert not persist_session_acquisition_authorization_cursor(
                database_cursor,
                bundle.session,
            )
            divergent_session = _authority_bundle(
                cohort_id=target_cohort,
                target_window_id=target_window,
                equity_receipt_id=equity_receipt,
                decoded_messages=100_001,
            ).session
            with pytest.raises(
                ValueError,
                match="session acquisition authorization replay diverged",
            ):
                persist_session_acquisition_authorization_cursor(
                    database_cursor,
                    divergent_session,
                )
            assert persist_window_cycle_authorization_cursor(
                database_cursor,
                bundle.cycle,
            )
            assert not persist_window_cycle_authorization_cursor(
                database_cursor,
                bundle.cycle,
            )
            divergent_cycle = build_window_cycle_authorization(
                bundle.session,
                authorization_reference=(
                    bundle.cycle.authorization_reference
                ),
                target_window_id=bundle.cycle.target_window_id,
                target_cohort_id=bundle.cycle.target_cohort_id,
                prepared_at=bundle.cycle.prepared_at,
                history_binding_sha256=(
                    bundle.cycle.history_binding_sha256
                ),
                equity_binding_sha256=(
                    bundle.cycle.equity_binding_sha256
                ),
                equity_receipt_id=bundle.cycle.equity_receipt_id,
                equity_cutoff=bundle.cycle.equity_cutoff,
                equity_receipt_created_at=(
                    bundle.cycle.equity_receipt_created_at
                ),
                options_cutoff=bundle.cycle.options_cutoff,
                age_policy=TargetWindowAgePolicy(
                    bundle.cycle.age_policy_version,
                    bundle.cycle.maximum_age_seconds,
                ),
                evidence_sha256="e" * 64,
                build_id=bundle.cycle.build_id,
            )
            with pytest.raises(
                ValueError,
                match="window cycle authorization replay diverged",
            ):
                persist_window_cycle_authorization_cursor(
                    database_cursor,
                    divergent_cycle,
                )

            with pytest.raises(
                ValueError,
                match="requires consumed session acquisition authority",
            ):
                consume_window_cycle_authority_cursor(
                    database_cursor,
                    bundle.cycle,
                    effect_kind="TARGET_WINDOW_STAGE",
                    effect_identity_sha256="c" * 64,
                    consumed_at=bundle.cycle.prepared_at,
                )

            session_receipt = consume_session_acquisition_authority_cursor(
                database_cursor,
                bundle.session,
                effect_kind="PROVIDER_REQUEST",
                effect_identity_sha256="a" * 64,
                consumed_at=bundle.session.acquisition_start,
            )
            assert consume_session_acquisition_authority_cursor(
                database_cursor,
                bundle.session,
                effect_kind="PROVIDER_REQUEST",
                effect_identity_sha256="a" * 64,
                consumed_at=bundle.session.acquisition_start,
            ) == session_receipt
            with pytest.raises(ValueError, match="already consumed"):
                consume_session_acquisition_authority_cursor(
                    database_cursor,
                    bundle.session,
                    effect_kind="PROVIDER_SOCKET",
                    effect_identity_sha256="b" * 64,
                    consumed_at=bundle.session.acquisition_start,
                )

            stage_authorizer = AdvancedOptionsWindowStageEffectAuthorizer(
                bundle.cycle
            )
            database_cursor.connection.commit()
            provider = V2ConnectionProvider(
                {OWNER_POOL: pool},
                V2AccessConfig(allow_disposable=True),
            )
            repository = PostgresOrchestrationRepository(
                provider,
                window_stage_effect_authorizer=stage_authorizer,
            )
            claim = repository.claim_window(
                target_window,
                uuid4(),
                bundle.cycle.prepared_at,
                60,
            )
            assert claim is not None
            with pytest.raises(psycopg2.DatabaseError) as waiting:
                repository.start_stage(
                    claim,
                    STAGES_V5["OPTIONS_INPUT_GATE"],
                    "stages_v5",
                    1,
                    2,
                    bundle.cycle.prepared_at,
                )
            assert waiting.value.pgcode == "RT005"
            database_cursor.execute(
                "SELECT count(*) "
                "FROM rt.advanced_options_authority_receipts "
                "WHERE window_cycle_authorization_id=%s",
                (str(bundle.cycle.authorization_id),),
            )
            assert database_cursor.fetchone() == (0,)

            completions = tuple(
                (
                    lambda evidence: ResidentCompletion(
                        target_window_id=target_window,
                        target_cohort_id=target_cohort,
                        member=member,
                        journal_generation=(
                            bundle.session.journal_generation
                        ),
                        owner_epoch=index,
                        ending_offset=10,
                        evidence_sha256=canonical_sha256(evidence),
                        evidence=evidence,
                        status=ResidentCompletionStatus.COMPLETE,
                        reason_codes=(),
                        completed_at=bundle.cycle.prepared_at,
                    )
                )({
                    "status": "COMPLETE",
                    "kind": member.value,
                    **(
                        {
                            "raw_end_offset": 10,
                            "normalized_end_offset": 10,
                            "connection_attempts": 1,
                        }
                        if member is ResidentCompletionMember.INGRESS
                        else {
                            "normalized_end_offset": 10,
                            "state_sha256": "a" * 64,
                            "shard_map_sha256": "b" * 64,
                        }
                        if member
                        is ResidentCompletionMember.MARKET_STATE
                        else {
                            "raw_end_offset": 10,
                            "checkpoint_end_offset": 10,
                            "owner_epoch": index,
                            "evidence_ref": "flow:10",
                        }
                    ),
                    "effects": {
                        "dispatch": False,
                        "delivery": False,
                        "broker": False,
                        "trading": False,
                    },
                })
                for index, member in enumerate(
                    ResidentCompletionMember,
                    start=1,
                )
            )
            for completion in completions:
                assert record_resident_completion_cursor(
                    database_cursor,
                    completion,
                )
                assert not record_resident_completion_cursor(
                    database_cursor,
                    completion,
                )
            barrier = evaluate_resident_completion_barrier_cursor(
                database_cursor,
                target_window_id=target_window,
                target_cohort_id=target_cohort,
                journal_generation=bundle.session.journal_generation,
            )
            assert (
                barrier.state
                is ResidentCompletionBarrierState.COMPLETE
            )
            assert len(barrier.members) == 3
            database_cursor.connection.commit()

            stage_run = repository.start_stage(
                claim,
                STAGES_V5["OPTIONS_INPUT_GATE"],
                "stages_v5",
                1,
                2,
                bundle.cycle.prepared_at,
            )
            assert stage_run.state == "RUNNING"
            database_cursor.execute(
                "SELECT receipt_id::text,receipt_sha256 "
                "FROM rt.advanced_options_authority_receipts "
                "WHERE window_cycle_authorization_id=%s",
                (str(bundle.cycle.authorization_id),),
            )
            consumed_cycle = database_cursor.fetchone()
            assert consumed_cycle is not None
            cycle_receipt = stage_authorizer(
                database_cursor,
                claim,
                STAGES_V5["OPTIONS_INPUT_GATE"],
                "stages_v5",
                bundle.cycle.prepared_at,
            )
            assert stage_authorizer(
                database_cursor,
                claim,
                STAGES_V5["OPTIONS_INPUT_GATE"],
                "stages_v5",
                bundle.cycle.prepared_at,
            ) == cycle_receipt
            with pytest.raises(ValueError, match="already consumed"):
                consume_window_cycle_authority_cursor(
                    database_cursor,
                    bundle.cycle,
                    effect_kind="TARGET_WINDOW_STAGE",
                    effect_identity_sha256="d" * 64,
                    consumed_at=bundle.cycle.prepared_at,
                )

            status = advanced_options_authority_status_cursor(
                database_cursor,
                bundle,
            )
            assert status["session_acquisition"]["state"] == "CONSUMED"
            assert status["window_cycle"]["state"] == "CONSUMED"
            assert status["cleanup"]["immutable_receipt_ids"] == sorted([
                str(session_receipt.receipt_id),
                str(cycle_receipt.receipt_id),
            ])
