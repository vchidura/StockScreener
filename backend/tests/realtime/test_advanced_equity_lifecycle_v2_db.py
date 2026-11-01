from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from realtime.advanced_equity_lifecycle import (
    AdvancedEquityLifecyclePlanner,
    claim_lifecycle_lease_cursor,
    ensure_desired_session_revision_cursor,
    ensure_automatic_stream_authorization_cursor,
    load_or_roll_effective_desired_session_cursor,
    release_lifecycle_lease_cursor,
)
from realtime.advanced_equity_capability_refresh import (
    AdvancedCapabilityEvidence,
    AdvancedCapabilityRefreshResident,
    MASSIVE_STOCKS_CONNECTION_RESOURCE,
    load_capability_refresh_request_cursor,
    persist_capability_refresh_outcome_cursor,
)
from realtime.advanced_ingress_plan import (
    AdvancedIngressPlanRequest,
    automatic_advanced_ingress_budgets,
)
from realtime.advanced_ingress_repository import (
    activate_advanced_runtime_profile_cursor,
    ensure_exact_advanced_runtime_profile_cursor,
    prepare_advanced_ingress_plan_cursor,
    register_advanced_runtime_profile_cursor,
    resolve_advanced_ingress_plan_cursor,
)
from realtime.advanced_ingress_runtime_profile import (
    load_advanced_ingress_runtime_catalog,
)
from realtime.advanced_realtime_desired_state import (
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredStateChange,
    MemberPreparationStatus,
    default_advanced_realtime_desired_state,
)
from realtime.advanced_realtime_desired_state_repository import (
    reconcile_desired_state,
    reconcile_desired_state_revision_cursor,
    record_desired_state_revision_cursor,
    update_desired_member_preparation,
)
from realtime.advanced_scheduler import (
    load_advanced_scheduler_progress_cursor,
    load_continuous_advanced_scheduler_scope_cursor,
)
from realtime.contracts import (
    EQUITY_VALIDATION_UNIVERSE,
    UniverseManifest,
    canonical_sha256,
)
from realtime.session_windows import advanced_session_windows
from tests.realtime.test_advanced_realtime_desired_state_v2_db import (
    _seed_source,
)
from scripts.run_realtime_v2_advanced_controller import (
    _prepare_continuous_session_cursor,
)
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc
FRIDAY = date(2026, 10, 30)
MONDAY = date(2026, 11, 2)
FRIDAY_ROLLOVER = datetime(2026, 10, 29, 21, 0, tzinfo=UTC)
MONDAY_MARKET = datetime(2026, 11, 2, 16, 30, tzinfo=UTC)
CATALOG = load_advanced_ingress_runtime_catalog(
    Path(__file__).resolve().parents[2]
    / "realtime"
    / "policies"
    / "advanced_equity_ingress_v1.json"
)
PROFILE = CATALOG.profile_for_target(9)
POLICY_SHA256 = CATALOG.scale_policy.sha256
CONFIGURATION_SHA256 = PROFILE.configuration_sha256(POLICY_SHA256)


def _digest(label):
    return canonical_sha256({"c3_fixture": label})


def _activate_runtime_profile(cursor):
    ladder = CATALOG.scale_policy.universe_ladder
    for position, target in enumerate(
        ladder[:ladder.index(PROFILE.universe_target) + 1]
    ):
        profile = CATALOG.profile_for_target(target)
        register_advanced_runtime_profile_cursor(
            cursor,
            profile,
            runtime_policy_sha256=POLICY_SHA256,
            ladder_position=position,
            previous_universe_target=(
                None if position == 0 else ladder[position - 1]
            ),
        )
        activate_advanced_runtime_profile_cursor(
            cursor,
            profile.sha256,
            stability_evidence_sha256=_digest(
                f"stability:{target}"
            ),
            capacity_evidence_sha256=_digest(
                f"capacity:{target}"
            ),
            approval_reference=(
                f"c3-disposable-runtime-profile:{target}"
            ),
            activated_at=FRIDAY_ROLLOVER,
        )


def test_non_ladder_runtime_profile_is_derived_from_reviewed_capacity():
    database = f"test_realtime_v2_c4_exact_profile_{uuid4().hex[:8]}"
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                _activate_runtime_profile(cursor)
                registration = ensure_exact_advanced_runtime_profile_cursor(
                    cursor,
                    CATALOG,
                    universe_target=10,
                    activated_at=FRIDAY_ROLLOVER,
                )
                cursor.execute(
                    "SELECT universe_target,ladder_position,"
                    "previous_universe_target,status,profile_document "
                    "FROM rt.advanced_ingress_runtime_profiles "
                    "WHERE runtime_profile_sha256=%s",
                    (registration.runtime_profile_sha256,),
                )
                row = cursor.fetchone()
            connection.commit()

            assert row[:4] == (10, 2, 9, "ACTIVE")
            assert row[4]["universe_target"] == 10
            assert (
                CATALOG.profile_for_target(10).settings
                == CATALOG.profile_for_target(25).settings
            )
        finally:
            pool.putconn(connection)


def test_market_hours_rollover_builds_idempotent_plan_and_progress():
    database = f"test_realtime_v2_c3_roll_{uuid4().hex[:8]}"
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                _activate_runtime_profile(cursor)
                source_cohort_id, _manifest_id = _seed_source(
                    cursor,
                    runtime_configuration_sha256=(
                        CONFIGURATION_SHA256
                    ),
                )
                friday_revision = record_desired_state_revision_cursor(
                    cursor,
                    default_advanced_realtime_desired_state(
                        source_cohort_id
                    ),
                    observed_at=FRIDAY_ROLLOVER,
                )
                friday_state = reconcile_desired_state_revision_cursor(
                    cursor,
                    friday_revision.revision_id,
                    materialized_at=datetime(
                        2026, 10, 30, 13, 15, tzinfo=UTC
                    ),
                )
                assert friday_state.snapshot is not None
            connection.commit()

            with connection.cursor() as cursor:
                monday = load_or_roll_effective_desired_session_cursor(
                    cursor,
                    target_session_date=MONDAY,
                    checked_at=MONDAY_MARKET,
                )
                budgets = automatic_advanced_ingress_budgets(
                    universe_target=monday.equity_member_count,
                    stream_sessions=4,
                    connection_attempts=12,
                    decoded_bytes=1024 * 1024 * 1024,
                )
                first_authorization = (
                    ensure_automatic_stream_authorization_cursor(
                        cursor,
                        desired=monday,
                        budgets=budgets,
                        runtime_policy_sha256=POLICY_SHA256,
                        promotion_ladder=(
                            CATALOG.scale_policy.universe_ladder
                        ),
                    )
                )
                replayed_authorization = (
                    ensure_automatic_stream_authorization_cursor(
                        cursor,
                        desired=monday,
                        budgets=budgets,
                        runtime_policy_sha256=POLICY_SHA256,
                        promotion_ladder=(
                            CATALOG.scale_policy.universe_ladder
                        ),
                    )
                )
                request = AdvancedIngressPlanRequest(
                    first_authorization.authorization_id,
                    MONDAY,
                    monday.equity_member_count,
                    budgets,
                    preferred_cohort_id=monday.snapshot.cohort_id,
                    cohort_selection_reference=(
                        "desired-snapshot:"
                        f"{monday.snapshot.snapshot_sha256}"
                    ),
                )
                plan = resolve_advanced_ingress_plan_cursor(
                    cursor,
                    request,
                    runtime_catalog=CATALOG,
                )
                prepared = prepare_advanced_ingress_plan_cursor(
                    cursor,
                    plan,
                    checked_at=MONDAY_MARKET,
                )
                replayed_plan = prepare_advanced_ingress_plan_cursor(
                    cursor,
                    plan,
                    checked_at=(
                        plan.authorized_end_at
                        + timedelta(seconds=1)
                    ),
                )
                (
                    _pinned_desired,
                    pinned_plan,
                    _pinned_profile,
                ) = _prepare_continuous_session_cursor(
                    cursor,
                    args=SimpleNamespace(
                        catch_up_mode="AUTO",
                        stream_sessions=8,
                        connection_attempts=20,
                        decoded_bytes=2 * 1024 * 1024 * 1024,
                    ),
                    catalog=CATALOG,
                    calendar=None,
                    session_date=MONDAY,
                    checked_at=MONDAY_MARKET + timedelta(minutes=1),
                )
                scope = load_continuous_advanced_scheduler_scope_cursor(
                    cursor,
                    session_date=MONDAY,
                )

                service_plan_id = uuid4()
                cursor.execute(
                    "INSERT INTO rt.service_plans ("
                    "plan_id,cohort_id,session_date,"
                    "stage_catalog_version,enabled_stages,config_sha256,"
                    "deadline,scope_reference) VALUES "
                    "(%s,%s,%s,'stages_v2',%s,%s,%s,%s)",
                    (
                        str(service_plan_id),
                        str(monday.snapshot.cohort_id),
                        MONDAY,
                        ["EQUITY_INGEST"],
                        _digest("service-plan"),
                        monday.snapshot.materialized_at
                        + timedelta(days=1),
                        "c3-disposable-progress",
                    ),
                )
                windows = advanced_session_windows(
                    MONDAY,
                    finality_delay_seconds=5,
                    completion_budget_seconds=300,
                    serving_lag_seconds=30,
                )
                for index, window in enumerate(windows[:21]):
                    cursor.execute(
                        "INSERT INTO rt.window_runs ("
                        "window_id,plan_id,cohort_id,provider_tier,"
                        "window_key,release_at,cutoff,deadline,state"
                        ") VALUES (%s,%s,%s,'MASSIVE_ADVANCED',"
                        "%s,%s,%s,%s,%s)",
                        (
                            str(uuid4()),
                            str(service_plan_id),
                            str(monday.snapshot.cohort_id),
                            (
                                "advanced-"
                                f"{window.cutoff:%Y%m%dT%H%M%SZ}"
                            ),
                            window.available_after,
                            window.cutoff,
                            window.deadline,
                            "COMPLETE" if index < 20 else "PENDING",
                        ),
                    )
                progress = load_advanced_scheduler_progress_cursor(
                    cursor,
                    cohort_id=monday.snapshot.cohort_id,
                    session_date=MONDAY,
                    first_cutoff=windows[0].cutoff,
                    final_cutoff=windows[-1].cutoff,
                )
            connection.commit()

            lifecycle = AdvancedEquityLifecyclePlanner(
                clock=lambda: MONDAY_MARKET
            ).plan(
                drain_seconds=30,
                finality_delay_seconds=5,
                completion_budget_seconds=300,
                last_completed_cutoff=(
                    progress.last_completed_cutoff
                ),
            )

            assert friday_revision.effective_session_date == FRIDAY
            assert monday.revision.effective_session_date == MONDAY
            assert monday.snapshot.session_date == MONDAY
            assert monday.revision.revision_id != friday_revision.revision_id
            assert first_authorization == replayed_authorization
            assert prepared == replayed_plan
            assert pinned_plan.plan_id == plan.plan_id
            assert pinned_plan.budgets == budgets
            assert scope["plan_id"] == plan.plan_id
            assert scope["cohort_id"] == monday.snapshot.cohort_id
            assert progress.last_completed_cutoff == windows[19].cutoff
            assert progress.last_discovered_cutoff == windows[20].cutoff
            assert lifecycle.first_due_cutoff == windows[20].cutoff
            assert lifecycle.catch_up_needed is True
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.realtime_desired_revisions "
                    "WHERE effective_session_date IN (%s,%s)",
                    (FRIDAY, MONDAY),
                )
                assert cursor.fetchone()[0] == 2
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.realtime_desired_session_snapshots "
                    "WHERE session_date IN (%s,%s)",
                    (FRIDAY, MONDAY),
                )
                assert cursor.fetchone()[0] == 2
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.advanced_stream_authorizations "
                    "WHERE authorization_id=%s",
                    (str(first_authorization.authorization_id),),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.advanced_stream_authorizations "
                    "WHERE approval_source="
                    "'ADVANCED_EQUITY_CONTINUOUS_V3'"
                )
                assert cursor.fetchone()[0] == (
                    CATALOG.scale_policy.universe_ladder.index(9) + 1
                )
                cursor.execute(
                    "SELECT count(*) FROM rt.advanced_ingress_plans "
                    "WHERE plan_id=%s",
                    (str(plan.plan_id),),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*),sum(connection_attempt_count),"
                    "sum(decoded_byte_count) "
                    "FROM rt.advanced_ingress_plan_usage "
                    "WHERE plan_id=%s",
                    (str(plan.plan_id),),
                )
                assert cursor.fetchone() == (1, 0, 0)
        finally:
            pool.putconn(connection)


def test_lifecycle_lease_refuses_active_and_fences_expired_owner():
    database = f"test_realtime_v2_c3_lease_{uuid4().hex[:8]}"
    first_owner = UUID(int=1)
    second_owner = UUID(int=2)
    started = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                first = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=first_owner,
                    checked_at=started,
                    lease_seconds=30,
                )
            connection.commit()
            with connection.cursor() as cursor:
                same_owner = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=first_owner,
                    checked_at=started + timedelta(seconds=1),
                    lease_seconds=30,
                )
                other_owner = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=second_owner,
                    checked_at=started + timedelta(seconds=1),
                    lease_seconds=30,
                )
            connection.commit()
            with connection.cursor() as cursor:
                takeover = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=second_owner,
                    checked_at=started + timedelta(seconds=31),
                    lease_seconds=30,
                )
            connection.commit()

            assert first is not None
            assert first.owner_epoch == 1
            assert same_owner is None
            assert other_owner is None
            assert takeover is not None
            assert takeover.owner_id == second_owner
            assert takeover.owner_epoch == 2
        finally:
            pool.putconn(connection)


def test_shared_stocks_connection_lease_is_atomic_across_roles():
    database = f"test_realtime_v2_c3_socket_{uuid4().hex[:8]}"
    ingress_owner = UUID(int=11)
    probe_owner = UUID(int=12)
    started = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                ingress = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=ingress_owner,
                    checked_at=started,
                    lease_seconds=180,
                    resource_key=MASSIVE_STOCKS_CONNECTION_RESOURCE,
                )
            connection.commit()
            with connection.cursor() as cursor:
                blocked_probe = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=probe_owner,
                    checked_at=started + timedelta(seconds=1),
                    lease_seconds=45,
                    resource_key=MASSIVE_STOCKS_CONNECTION_RESOURCE,
                )
                release_lifecycle_lease_cursor(cursor, ingress)
            connection.commit()
            with connection.cursor() as cursor:
                probe = claim_lifecycle_lease_cursor(
                    cursor,
                    owner_id=probe_owner,
                    checked_at=started + timedelta(seconds=2),
                    lease_seconds=45,
                    resource_key=MASSIVE_STOCKS_CONNECTION_RESOURCE,
                )
                release_lifecycle_lease_cursor(cursor, probe)
            connection.commit()
        finally:
            pool.putconn(connection)

        assert ingress is not None
        assert blocked_probe is None
        assert probe is not None
        barrier = Barrier(2)

        def compete(owner_id):
            own = pool.getconn()
            try:
                barrier.wait()
                with own:
                    with own.cursor() as cursor:
                        return claim_lifecycle_lease_cursor(
                            cursor,
                            owner_id=owner_id,
                            checked_at=started + timedelta(seconds=3),
                            lease_seconds=45,
                            resource_key=(
                                MASSIVE_STOCKS_CONNECTION_RESOURCE
                            ),
                        )
            finally:
                pool.putconn(own)

        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = tuple(executor.map(
                compete,
                (UUID(int=21), UUID(int=22)),
            ))

        assert sum(claim is not None for claim in claims) == 1


def test_capability_refresh_required_then_selects_fresh_reference_profile():
    database = f"test_realtime_v2_c3_cap_{uuid4().hex[:8]}"
    expired_session = date(2026, 11, 9)
    checked = datetime(2026, 11, 9, 14, 15, tzinfo=UTC)
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                _activate_runtime_profile(cursor)
                source_cohort_id, _manifest_id = _seed_source(
                    cursor,
                    runtime_configuration_sha256=(
                        CONFIGURATION_SHA256
                    ),
                )
                friday_revision = record_desired_state_revision_cursor(
                    cursor,
                    default_advanced_realtime_desired_state(
                        source_cohort_id
                    ),
                    observed_at=FRIDAY_ROLLOVER,
                )
                friday = reconcile_desired_state_revision_cursor(
                    cursor,
                    friday_revision.revision_id,
                    materialized_at=datetime(
                        2026, 10, 30, 13, 15, tzinfo=UTC
                    ),
                )
                rollover = ensure_desired_session_revision_cursor(
                    cursor,
                    target_session_date=expired_session,
                    checked_at=checked,
                )
            connection.commit()

            with pytest.raises(ValueError, match="REFRESH_REQUIRED"):
                with connection.cursor() as cursor:
                    load_or_roll_effective_desired_session_cursor(
                        cursor,
                        target_session_date=expired_session,
                        checked_at=checked,
                    )
                connection.commit()
            connection.rollback()
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT revision_id::text,effective_session_date "
                    "FROM rt.realtime_desired_current "
                    "WHERE scope_key='ADVANCED_REALTIME'"
                )
                current_revision_id, current_session = cursor.fetchone()
                assert current_revision_id == str(rollover.revision_id)
                assert current_session == expired_session
                cursor.execute(
                    "SELECT count(*) FROM rt.realtime_desired_revisions "
                    "WHERE effective_session_date=%s",
                    (expired_session,),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.realtime_desired_session_snapshots "
                    "WHERE revision_id=%s",
                    (str(rollover.revision_id),),
                )
                assert cursor.fetchone()[0] == 0

            class Source:
                def refresh(self, request):
                    return AdvancedCapabilityEvidence(
                        tickers=request.tickers,
                        feed="STOCKS",
                        channel="AM",
                        configuration_sha256=(
                            request.configuration_sha256
                        ),
                        checked_at=checked,
                        expires_at=checked + timedelta(days=7),
                        entitlement_evidence_sha256=_digest(
                            "refresh-evidence"
                        ),
                        authorization_reference=(
                            "c3-fake-capability-refresh"
                        ),
                        accepted_membership=request.tickers,
                        rejected_membership=(),
                        provider_requests=1,
                        received_control_messages=2,
                        received_bytes=256,
                        subscribe_request_bytes=128,
                    )

            class FailingSource:
                def refresh(self, _request):
                    raise RuntimeError("temporary provider outage")

            with connection.cursor() as cursor:
                request = load_capability_refresh_request_cursor(
                    cursor,
                    checked_at=checked,
                    refresh_before_seconds=1800,
                )
                assert request.requires_refresh is True
                failed = AdvancedCapabilityRefreshResident(
                    FailingSource()
                ).step(request, checked_at=checked)
                persist_capability_refresh_outcome_cursor(
                    cursor,
                    failed,
                    owner_epoch=1,
                    lease_until=checked + timedelta(seconds=60),
                )
            connection.commit()
            assert failed.status == "DEGRADED"
            assert failed.plan is None

            resident = AdvancedCapabilityRefreshResident(Source())
            with connection.cursor() as cursor:
                outcome = resident.step(request, checked_at=checked)
                persist_capability_refresh_outcome_cursor(
                    cursor,
                    outcome,
                    owner_epoch=1,
                    lease_until=checked + timedelta(seconds=60),
                )
                ready, refreshed_plan, _profile = (
                    _prepare_continuous_session_cursor(
                        cursor,
                        args=SimpleNamespace(
                            catch_up_mode="AUTO",
                            stream_sessions=4,
                            connection_attempts=12,
                            decoded_bytes=1024 * 1024 * 1024,
                        ),
                        catalog=CATALOG,
                        calendar=None,
                        session_date=expired_session,
                        checked_at=checked + timedelta(minutes=1),
                    )
                )
            connection.commit()

            assert ready.snapshot is not None
            assert refreshed_plan.session_date == expired_session
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT profile_id::text "
                    "FROM rt.subscription_manifests "
                    "WHERE manifest_id=%s",
                    (str(ready.snapshot.manifest_id),),
                )
                assert cursor.fetchone()[0] == str(
                    outcome.profile_id
                )
                cursor.execute(
                    "SELECT verdict,evidence "
                    "FROM rt.capability_checks "
                    "WHERE profile_id=%s "
                    "AND check_name='exact_subscription_refresh'",
                    (str(outcome.profile_id),),
                )
                verdict, evidence = cursor.fetchone()
                assert verdict == "PASS"
                assert tuple(evidence["accepted_membership"]) == (
                    request.tickers
                )
                assert evidence["rejected_membership"] == []
                assert evidence["provider_requests"] == 1
                assert evidence["received_control_messages"] == 2
                cursor.execute(
                    "SELECT status,source_watermark "
                    "FROM rt.readiness_current "
                    "WHERE scope_id="
                    "'ADVANCED_EQUITY_CAPABILITY_REFRESH'"
                )
                status, watermark = cursor.fetchone()
                assert status == "READY"
                assert watermark["last_success_at"] is not None
        finally:
            pool.putconn(connection)


def test_concurrent_reconcilers_share_first_committed_snapshot():
    database = f"test_realtime_v2_c3_recon_{uuid4().hex[:8]}"
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, _manifest_id = _seed_source(cursor)
                revision = record_desired_state_revision_cursor(
                    cursor,
                    default_advanced_realtime_desired_state(
                        source_cohort_id
                    ),
                    observed_at=FRIDAY_ROLLOVER,
                )
                capability_config = CATALOG.profile_for_target(
                    10
                ).configuration_sha256(POLICY_SHA256)
                target_universe = UniverseManifest(
                    version="advanced-equity-10-am-v1",
                    equity_tickers=tuple(sorted((
                        *EQUITY_VALIDATION_UNIVERSE.equity_tickers,
                        "XLK",
                    ))),
                    option_underlyings=(),
                )
                cursor.execute(
                    "INSERT INTO rt.capability_profiles ("
                    "profile_id,config_sha256,schema_sha256,"
                    "account_alias,capabilities_sha256,universe_sha256,"
                    "checked_at,expires_at,status,profile_sha256,"
                    "provider_tier,security_policy_sha256,preflight_mode,"
                    "provider_requests,action_authorized"
                    ") VALUES (%s,%s,%s,'c4-ready-race',%s,%s,%s,%s,"
                    "'PASS',%s,'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
                    (
                        str(uuid5(
                            NAMESPACE_URL,
                            "c4-readiness-race-capability",
                        )),
                        capability_config,
                        _digest("c4-race-schema"),
                        _digest("c4-race-capabilities"),
                        target_universe.sha256,
                        FRIDAY_ROLLOVER,
                        FRIDAY_ROLLOVER + timedelta(days=7),
                        _digest("c4-race-profile"),
                        _digest("c4-race-security"),
                    ),
                )
            connection.commit()
        finally:
            pool.putconn(connection)

        barrier = Barrier(2)
        times = (
            datetime(2026, 10, 30, 13, 16, tzinfo=UTC),
            datetime(2026, 10, 30, 13, 17, tzinfo=UTC),
        )

        def reconcile_at(materialized_at):
            own = pool.getconn()
            try:
                barrier.wait()
                return reconcile_desired_state(
                    own,
                    revision.revision_id,
                    materialized_at=materialized_at,
                )
            finally:
                pool.putconn(own)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(reconcile_at, times))

        assert results[0].snapshot == results[1].snapshot
        assert results[0].snapshot.materialized_at in times
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.realtime_desired_session_snapshots "
                    "WHERE revision_id=%s",
                    (str(revision.revision_id),),
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)


def test_readiness_update_serializes_with_concurrent_reconcile():
    database = f"test_realtime_v2_c3_ready_{uuid4().hex[:8]}"
    with disposable_v2_database(database) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, _manifest_id = _seed_source(cursor)
                members = list(
                    default_advanced_realtime_desired_state(
                        source_cohort_id
                    ).members
                )
                members.append(
                    DesiredMemberIntent(
                        "XLK",
                        DesiredMemberKind.SECTOR_ETF,
                    )
                )
                revision = record_desired_state_revision_cursor(
                    cursor,
                    DesiredStateChange(
                        source_cohort_id=source_cohort_id,
                        source_slot_version=1,
                        members=tuple(members),
                    ),
                    observed_at=FRIDAY_ROLLOVER,
                )
            connection.commit()
        finally:
            pool.putconn(connection)

        barrier = Barrier(2)

        def mark_ready():
            own = pool.getconn()
            try:
                barrier.wait()
                return update_desired_member_preparation(
                    own,
                    revision.revision_id,
                    ticker="XLK",
                    kind=DesiredMemberKind.SECTOR_ETF,
                    status=MemberPreparationStatus.READY,
                    checked_at=FRIDAY_ROLLOVER + timedelta(minutes=1),
                )
            finally:
                pool.putconn(own)

        def reconcile_concurrently():
            own = pool.getconn()
            try:
                barrier.wait()
                return reconcile_desired_state(
                    own,
                    revision.revision_id,
                    materialized_at=(
                        FRIDAY_ROLLOVER + timedelta(minutes=2)
                    ),
                )
            finally:
                pool.putconn(own)

        with ThreadPoolExecutor(max_workers=2) as executor:
            ready_future = executor.submit(mark_ready)
            reconcile_future = executor.submit(reconcile_concurrently)
            preparation = ready_future.result()
            concurrent = reconcile_future.result()

        assert preparation.status is MemberPreparationStatus.READY
        assert concurrent.state in {
            "WARMING",
            "ACTIVATION_PENDING",
        }
        connection = pool.getconn()
        try:
            final = reconcile_desired_state(
                connection,
                revision.revision_id,
                materialized_at=(
                    datetime(
                        2026,
                        10,
                        30,
                        13,
                        15,
                        tzinfo=UTC,
                    )
                ),
            )
            assert final.state in {"READY", "REFRESH_REQUIRED"}
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT revision_id,count(*) FROM "
                    "rt.realtime_desired_session_snapshots "
                    "GROUP BY revision_id HAVING count(*)<>1"
                )
                assert cursor.fetchall() == []
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.realtime_desired_session_snapshots"
                )
                assert cursor.fetchone()[0] <= 2
        finally:
            pool.putconn(connection)
