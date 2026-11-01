from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from psycopg2.extras import execute_batch

from realtime.advanced_options_consumers import (
    AdvancedOptionsBoundedDetectorRouter,
    AdvancedOptionsMarketStateConsumer,
    load_advanced_options_detector_routing_state_cursor,
    load_advanced_options_market_state_cursor,
    persist_advanced_options_detector_routing_state_cursor,
    persist_advanced_options_market_state_slice_cursor,
)
from realtime.advanced_options_acquisition_runtime import (
    build_acquisition_binding,
)
from realtime.advanced_options_activity_momentum_c6 import (
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ActivityMomentumCandidate,
    ActivityMomentumExpiryCalendarEvidence,
    ActivityMomentumGroup,
    ActivityMomentumLastTradingEvidence,
    activity_momentum_allocation_from_payload,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalSource,
)

from realtime.advanced_options_selection_c6 import (
    C6ConnectionCapacity,
    C6ConsumerState,
    C6ReadinessState,
    C6ShardState,
    C6UnderlyingReadiness,
    OptionSide,
    persist_c6_consumer_state_cursor,
    persist_c6_physical_subscription_state_cursor,
    persist_c6_shard_state_cursor,
    persist_c6_underlying_readiness_cursor,
)
from realtime.contracts import (
    AssetClass,
    MarketFeed,
    RawMessageEnvelope,
    canonical_sha256,
)
from realtime.advanced_options_v3_authority import (
    prospective_opening_admission_payload,
    session_acquisition_authorization_payload,
)
from realtime.database_access_v2 import APP_ROLE
from realtime.journal import RawJournalReader, RawJournalWriter
from realtime.materialization import MaterializerShardMap
from realtime.normalizer import (
    NORMALIZER_VERSION,
    InstrumentAdmission,
    normalize_raw_message,
)
from tests.realtime.test_advanced_options_session_state_v2_db import (
    OPENING,
    SESSION_DATE,
    _provider,
    _seed_session,
)
from tests.realtime.test_option_slot_sets_v2_db import cursor
from tests.realtime.v2_database import (
    disposable_v2_database,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc


def _patch_reference(monkeypatch, seed):
    from realtime import advanced_options_session_state as state_module

    monkeypatch.setattr(
        state_module.PostgresAdvancedOptionsReferenceStateRepository,
        "load_for_session",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        state_module.OpeningReferenceEvidence,
        "from_reference_state",
        classmethod(
            lambda _cls, _state: seed["reference"]
        ),
    )

def test_c6_selection_persists_rehydrates_and_exposes_partial_health(
    monkeypatch,
):
    with disposable_v2_database(
        f"test_realtime_v2_c6_selection_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        from realtime.advanced_options_session_state import (
            PostgresContinuousSessionStateRepository,
        )
        _patch_reference(monkeypatch, seed)

        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        authority = seed["authority_bundle"]
        repository.activate_session_from_documents(
            package=seed["package"],
            pre_session_authority={
                "session_date": SESSION_DATE.isoformat(),
                "target_cohort_id": str(
                    authority.session.cohort_id
                ),
                "admission": prospective_opening_admission_payload(
                    authority.admission
                ),
                "readiness_sha256": (
                    authority.readiness.readiness_sha256
                ),
                "session_acquisition": (
                    session_acquisition_authorization_payload(
                        authority.session
                    )
                ),
            },
            acquisition_binding=seed["acquisition"],
            sealed_selection=seed["sealed_selection"],
            written_at=authority.session.prepared_at,
            selection_capacity=C6ConnectionCapacity(
                account_alias="MASSIVE_ADVANCED",
                provider_cluster="OPTIONS",
                configured_cluster_connection_limit=1,
                provider_cluster_connection_limit=1,
                active_cluster_connections=0,
                proposed_physical_connections=1,
                quote_contract_ceiling=1000,
                active_quote_contract_budget=900,
                quote_slot_reserve=100,
                maximum_contracts_per_logical_shard=4,
            ),
        )

        first = repository.load_c6_selection_plan()
        second = repository.load_c6_selection_plan()
        assert first is not None
        assert second is first
        assert first.broad_underlyings == ("QQQ", "SPY")
        assert len(first.members) == 4
        assert len(first.shards) == 1
        plan = first

        shard = plan.shards[0]
        later = plan.source_as_of + timedelta(minutes=5)
        warming = C6UnderlyingReadiness(
            underlying="SPY",
            catalog=C6ReadinessState.AVAILABLE,
            open_interest=C6ReadinessState.WARMING,
            history=C6ReadinessState.AVAILABLE,
            feed_states=(
                ("OPTION_QUOTE", C6ReadinessState.AVAILABLE),
                ("OPTION_TRADE", C6ReadinessState.AVAILABLE),
            ),
            reason_codes=("OPEN_INTEREST_WARMING",),
            observed_at=later,
            source_as_of=plan.source_as_of,
        )
        provider = _provider(pool)
        with provider.transaction(APP_ROLE) as unit:
            persist_c6_underlying_readiness_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                readiness=warming,
            )
            persist_c6_shard_state_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                shard_id=shard.shard_id,
                state=C6ShardState.FAILED,
                message_count=100,
                byte_count=10_000,
                reason_codes=("PROCESSING_FAILED",),
                observed_at=later,
            )
            persist_c6_physical_subscription_state_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                subscription_id=(
                    plan.physical_subscription.subscription_id
                ),
                state=C6ShardState.FAILED,
                message_count=100,
                byte_count=10_000,
                reconnect_count=2,
                gaps=(
                    {
                        "start_offset": 91,
                        "end_offset": 100,
                        "reason": "RECONNECT_GAP",
                    },
                ),
                reason_codes=("PROVIDER_DISCONNECTED",),
                observed_at=later,
            )
            persist_c6_consumer_state_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                shard_id=shard.shard_id,
                state=C6ConsumerState(
                    consumer_name="synthetic-c6-consumer",
                    state_version="synthetic-c6-v1",
                    committed_offset=90,
                    event_watermark_ns=49_000_000,
                    history_start=None,
                    history_end=None,
                    missing_feed_states=(
                        "INDEX_OBSERVATION_UNAVAILABLE",
                    ),
                    updated_at=later,
                ),
            )

        health = repository.load_c6_selection_health()
        assert health is not None
        assert health["ready_underlying_count"] == 1
        assert health["failed_shard_count"] == 1
        assert health["shards"][0]["message_count"] == 100
        assert health["physical_failed"]
        assert health["physical_subscription"][
            "reconnect_count"
        ] == 2
        assert health["physical_subscription"]["gaps"][0][
            "reason"
        ] == "RECONNECT_GAP"


@pytest.mark.parametrize(
    "profile_version",
    (
        ACTIVITY_MOMENTUM_PROFILE_VERSION,
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ),
)
def test_activity_profile_activation_persists_exact_allocation(
    monkeypatch,
    profile_version,
):
    with disposable_v2_database(
        f"test_realtime_v2_c6_activity_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        from realtime.advanced_options_session_state import (
            PostgresContinuousSessionStateRepository,
        )

        _patch_reference(monkeypatch, seed)
        authority = seed["authority_bundle"]
        profile = load_activity_momentum_profile(
            version=profile_version
        )
        candidates = tuple(
            ActivityMomentumCandidate(
                contract_id=row.contract_id,
                contract_ticker=row.contract_ticker,
                underlying=row.underlyer,
                expiration=row.expiration_date,
                option_side=(
                    OptionSide.CALL
                    if row.contract_type == "CALL"
                    else OptionSide.PUT
                ),
                strike=row.strike,
                multiplier=Decimal("100"),
                deliverable="STANDARD",
                catalog_version_id=row.catalog_version_id,
                catalog_payload_sha256=(
                    row.catalog_payload_sha256
                ),
                observed_at=authority.admission.prepared_at,
                spot=row.spot,
                bid=Decimal("1.00"),
                ask=Decimal("1.10"),
                quote_as_of=authority.admission.prepared_at,
                completed_session_volume=row.day_volume,
                volume_as_of=authority.admission.prepared_at,
                prior_open_interest=row.open_interest,
                open_interest_as_of=authority.admission.prepared_at,
                delta=(
                    Decimal("0.50")
                    if row.contract_type == "CALL"
                    else Decimal("-0.50")
                ),
                implied_volatility=Decimal("0.25"),
                greeks_as_of=authority.admission.prepared_at,
                source_evidence_references=(
                    row.snapshot_payload_sha256,
                    row.open_interest_member_sha256,
                ),
                flow_eligible=True,
                flow_unavailable_reason=None,
            )
            for row in authority.admission.contracts
        )
        if (
            profile_version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            calendars = tuple(
                ActivityMomentumExpiryCalendarEvidence(
                    underlying=underlying,
                    target_session_date=SESSION_DATE,
                    calendar_name="XNYS",
                    week_end_session_date=(
                        SESSION_DATE + timedelta(days=4)
                    ),
                    product_convention="US_EQUITY_STANDARD_PM",
                    last_trading_evidence=(
                        ActivityMomentumLastTradingEvidence(
                            expiration=SESSION_DATE,
                            last_trading_session_date=SESSION_DATE,
                            last_trading_at=OPENING
                            + timedelta(hours=6),
                            convention="US_EQUITY_STANDARD_PM",
                            evidence_reference=canonical_sha256({
                                "last_trading": underlying,
                                "session_date": SESSION_DATE,
                            }),
                        ),
                    ),
                    evidence_references=(
                        canonical_sha256({
                            "calendar": underlying,
                            "session_date": SESSION_DATE,
                        }),
                    ),
                )
                for underlying in ("QQQ", "SPY")
            )
            resolution = resolve_activity_momentum_selection(
                profile,
                session_date=SESSION_DATE,
                candidates=candidates,
                preparation_at=authority.admission.prepared_at,
                expiry_calendars=calendars,
            )
            groups = resolution.groups
        else:
            resolution = None
            groups = tuple(
                ActivityMomentumGroup(
                    group_id=canonical_sha256({
                        "activity": row.contract_ticker,
                    }),
                    group_version="fixture-groups-v1",
                    underlying=row.underlying,
                    roles=("DIRECTIONAL_MOMENTUM",),
                    candidates=(row,),
                    requirement_identities=(
                        canonical_sha256({
                            "requirement": row.contract_ticker,
                        }),
                    ),
                    target_delta=Decimal("0.50"),
                    target_moneyness=Decimal("0.02"),
                    target_dte=row.dte_for(SESSION_DATE),
                )
                for row in candidates
            )
        allocation = allocate_activity_momentum(
            profile,
            session_date=SESSION_DATE,
            rotation_key="db-activity-fixture",
            ready_underlyings=("QQQ", "SPY"),
            groups=groups,
            active_contract_budget=4,
            resolution=resolution,
        )
        sealed_selection = deepcopy(seed["sealed_selection"])
        sealed_selection["activity_momentum_allocation"] = (
            activity_momentum_allocation_payload(allocation)
        )
        sealed_selection.pop("semantic_sha256")
        sealed_selection["semantic_sha256"] = canonical_sha256(
            sealed_selection
        )
        package = deepcopy(seed["package"])
        package["v3_pre_session_authority"][
            "source_capture_semantic_sha256"
        ] = sealed_selection["semantic_sha256"]
        package.pop("package_sha256")
        package["package_sha256"] = canonical_sha256(package)
        old_acquisition = seed["acquisition"]
        acquisition = build_acquisition_binding(
            source_sha256=sealed_selection["semantic_sha256"],
            backend_image_id=old_acquisition.backend_image_id,
            authority=old_acquisition.authority,
            contract_tickers=old_acquisition.contract_tickers,
            connection_attempt_ceiling=(
                old_acquisition.connection_attempt_ceiling
            ),
            decoded_message_ceiling=(
                old_acquisition.decoded_message_ceiling
            ),
            decoded_byte_ceiling=(
                old_acquisition.decoded_byte_ceiling
            ),
            equity_tickers=old_acquisition.equity_tickers,
        )
        repository = PostgresContinuousSessionStateRepository(
            _provider(pool),
            session_date=SESSION_DATE,
        )
        repository.activate_session_from_documents(
            package=package,
            pre_session_authority={
                "session_date": SESSION_DATE.isoformat(),
                "target_cohort_id": str(
                    authority.session.cohort_id
                ),
                "admission": prospective_opening_admission_payload(
                    authority.admission
                ),
                "readiness_sha256": (
                    authority.readiness.readiness_sha256
                ),
                "session_acquisition": (
                    session_acquisition_authorization_payload(
                        authority.session
                    )
                ),
            },
            acquisition_binding=acquisition,
            sealed_selection=sealed_selection,
            written_at=authority.session.prepared_at,
            selection_capacity=C6ConnectionCapacity(
                account_alias="MASSIVE_ADVANCED",
                provider_cluster="OPTIONS",
                configured_cluster_connection_limit=1,
                provider_cluster_connection_limit=1,
                active_cluster_connections=0,
                proposed_physical_connections=1,
                quote_contract_ceiling=1000,
                active_quote_contract_budget=4,
                quote_slot_reserve=996,
                maximum_contracts_per_logical_shard=4,
            ),
        )

        plan = repository.load_c6_selection_plan()
        health = repository.load_c6_selection_health()
        assert plan is not None
        assert plan.admission_profile_version == profile.version
        assert plan.allocation_sha256 == allocation.allocation_sha256
        assert plan.physical_subscription.owner_id == acquisition.owner_id
        assert (
            plan.physical_subscription.journal_generation
            == authority.session.journal_generation
        )
        assert health is not None
        assert health["allocation_sha256"] == allocation.allocation_sha256
        assert health["coverage"]
        assert set(plan.physical_subscription.contract_tickers) == set(
            acquisition.contract_tickers
        )
        assert activity_momentum_allocation_from_payload(
            plan.allocation_document
        ) == allocation
        if (
            profile_version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            assert allocation.short_expiry_evidence
            assert plan.allocation_document[
                "short_expiry_evidence"
            ] == activity_momentum_allocation_payload(allocation)[
                "short_expiry_evidence"
            ]

        router = AdvancedOptionsBoundedDetectorRouter(
            allocation=allocation
        )
        provider = _provider(pool)
        with provider.transaction(APP_ROLE) as unit:
            persist_advanced_options_detector_routing_state_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                state=router.state,
                updated_at=authority.session.prepared_at,
            )
        with provider.transaction(APP_ROLE) as unit:
            persist_advanced_options_detector_routing_state_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                state=router.state,
                updated_at=(
                    authority.session.prepared_at
                    + timedelta(seconds=1)
                ),
            )
        divergent_state = replace(
            router.state,
            scanned_event_count=1,
        )
        with pytest.raises(
            ValueError,
            match="routing replay diverged",
        ):
            with provider.transaction(APP_ROLE) as unit:
                persist_advanced_options_detector_routing_state_cursor(
                    unit.cursor,
                    selection_generation_id=plan.generation_id,
                    state=divergent_state,
                    updated_at=(
                        authority.session.prepared_at
                        + timedelta(seconds=2)
                    ),
                )
        event_ns = int(
            authority.session.prepared_at.timestamp()
            * 1_000_000_000
        )
        forward_state = replace(
            router.state,
            committed_event_offset=1,
            scanned_event_count=1,
            processed_detail_bytes=16,
            first_event_ns=event_ns,
            last_event_ns=event_ns,
            irrelevant_event_count=1,
        )
        with provider.transaction(APP_ROLE) as unit:
            persist_advanced_options_detector_routing_state_cursor(
                unit.cursor,
                selection_generation_id=plan.generation_id,
                state=forward_state,
                updated_at=(
                    authority.session.prepared_at
                    + timedelta(seconds=3)
                ),
            )
        with pytest.raises(
            ValueError,
            match="routing progress is stale",
        ):
            with provider.transaction(APP_ROLE) as unit:
                persist_advanced_options_detector_routing_state_cursor(
                    unit.cursor,
                    selection_generation_id=plan.generation_id,
                    state=router.state,
                    updated_at=(
                        authority.session.prepared_at
                        + timedelta(seconds=4)
                    ),
                )
        changed_allocation = replace(
            forward_state,
            allocation_sha256="f" * 64,
            committed_event_offset=2,
        )
        with pytest.raises(
            ValueError,
            match="routing allocation changed",
        ):
            with provider.transaction(APP_ROLE) as unit:
                persist_advanced_options_detector_routing_state_cursor(
                    unit.cursor,
                    selection_generation_id=plan.generation_id,
                    state=changed_allocation,
                    updated_at=(
                        authority.session.prepared_at
                        + timedelta(seconds=5)
                    ),
                )

        restarted_repository = (
            PostgresContinuousSessionStateRepository(
                _provider(pool),
                session_date=SESSION_DATE,
            )
        )
        restarted_plan = (
            restarted_repository.load_c6_selection_plan()
        )
        assert restarted_plan == plan
        assert restarted_plan is not plan
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            restarted_routing_state = (
                load_advanced_options_detector_routing_state_cursor(
                    unit.cursor,
                    selection_generation_id=plan.generation_id,
                )
            )
        restarted_router = AdvancedOptionsBoundedDetectorRouter(
            allocation=activity_momentum_allocation_from_payload(
                restarted_plan.allocation_document
            ),
            state=restarted_routing_state,
        )
        assert restarted_router.routes == router.routes
        assert restarted_router.state == forward_state
        if (
            profile_version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            restarted_allocation = (
                activity_momentum_allocation_from_payload(
                    restarted_plan.allocation_document
                )
            )
            assert (
                restarted_allocation.short_expiry_evidence
                == allocation.short_expiry_evidence
            )


def test_synthetic_consumer_attaches_to_journal_and_resumes_checkpoint(
    tmp_path,
):
    with disposable_v2_database(
        f"test_realtime_v2_c6_consumer_{uuid4().hex[:8]}"
    ) as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(database_cursor)
            database_cursor.connection.commit()
        contract = seed["authority_bundle"].admission.contracts[0]
        instrument_id = next(
            row[0]
            for row in seed["authority_bundle"].readiness.slots
            if row[2] == contract.contract_ticker
        )
        generation = uuid4()
        stream_session_id = uuid4()
        observed_at = datetime(
            SESSION_DATE.year,
            SESSION_DATE.month,
            SESSION_DATE.day,
            14,
            30,
            tzinfo=UTC,
        )
        path = tmp_path / "synthetic-consumer.sqlite"
        admissions = {
            contract.contract_ticker: InstrumentAdmission(
                instrument_id,
                contract.contract_ticker,
                contract.underlyer,
                AssetClass.OPTION,
                0,
            ),
        }

        def append_quote(writer, number, bid, ask):
            timestamp_ms = int(observed_at.timestamp() * 1000) + number
            payload = (
                '[{"ev":"Q","sym":"'
                + contract.contract_ticker
                + f'","bp":{bid},"ap":{ask},"bs":10,"as":11,'
                + f'"t":{timestamp_ms},"q":{number}}}]'
            ).encode("ascii")
            writer.append_batch((
                RawMessageEnvelope(
                    stream_session_id=stream_session_id,
                    message_number=number,
                    feed=MarketFeed.OPTIONS,
                    received_at=observed_at,
                    received_monotonic_ns=number,
                    payload=payload,
                ),
            ))
            raw = RawJournalReader(path).read_after(
                number - 1,
                limit=1,
            )[0]
            writer.commit_normalized(
                normalize_raw_message(raw, admissions)
            )

        with RawJournalWriter(
            path,
            generation=generation,
            clock=lambda: observed_at,
        ) as writer:
            append_quote(writer, 1, 1.0, 1.1)
            shard_map = MaterializerShardMap(
                "synthetic-c6-shard-map-v1",
                1,
                ((instrument_id, 0),),
            )
            registration = SimpleNamespace(
                status="DRAINED",
                raw_committed_offset=1,
                normalized_committed_offset=1,
                shard_map_sha256=shard_map.sha256,
                shard_ids=(0,),
                normalizer_version=NORMALIZER_VERSION,
            )
            source = AdvancedOptionsJournalSource.__new__(
                AdvancedOptionsJournalSource
            )
            source.registration = registration
            source.source_flow = SimpleNamespace(
                stream_session_ids=(stream_session_id,),
            )
            source.path = path
            source._reader = RawJournalReader(path)
            source.generation = generation
            consumer_name = "synthetic-c6-journal-consumer-v1"
            cohort_id = seed["options_cohort_id"]
            owner_id = uuid4()
            resource = (
                f"{consumer_name}:{cohort_id}:{generation}:0"
            )
            with cursor(pool) as database_cursor:
                database_cursor.execute(
                    "INSERT INTO rt.worker_leases "
                    "(resource_key,owner_id,owner_epoch,lease_until) "
                    "VALUES (%s,%s,1,"
                    "clock_timestamp()+interval '10 minutes')",
                    (resource, str(owner_id)),
                )
                database_cursor.execute(
                    "INSERT INTO rt.stream_sessions ("
                    "session_id,provider,feed,manifest_id,owner_id,"
                    "owner_epoch,started_at,authenticated_at,"
                    "subscribed_at,ended_at,status,journal_generation,"
                    "normalizer_version,authorization_reference,"
                    "budget_scope_sha256,connection_attempt,"
                    "connection_ceiling,decoded_byte_ceiling,"
                    "received_frame_count,received_byte_count,"
                    "admitted_market_message_count,"
                    "admitted_market_byte_count,last_raw_offset,"
                    "last_event_offset,event_time_watermark,heartbeat_at"
                    ") VALUES (%s,'provider-free','OPTIONS',%s,%s,1,"
                    "%s,%s,%s,%s,'STOPPED',%s,%s,'C6_SYNTHETIC',%s,"
                    "1,1,1048576,2,0,2,0,2,2,%s,%s)",
                    (
                        str(stream_session_id),
                        str(seed["options_manifest_id"]),
                        str(owner_id),
                        observed_at,
                        observed_at,
                        observed_at,
                        observed_at,
                        str(generation),
                        NORMALIZER_VERSION,
                        canonical_sha256({
                            "budget": "synthetic-consumer",
                        }),
                        observed_at,
                        observed_at,
                    ),
                )
                database_cursor.connection.commit()
            provider = _provider(pool)
            consumer = AdvancedOptionsMarketStateConsumer(
                source=source,
                cohort_id=cohort_id,
                shard_map=shard_map,
                owner_epoch=1,
                consumer_name=consumer_name,
                clock=lambda: observed_at,
            )
            first = consumer.plan_slice(maximum_records=10)
            with provider.transaction(APP_ROLE) as unit:
                persist_advanced_options_market_state_slice_cursor(
                    unit.cursor,
                    cohort_id=cohort_id,
                    journal_generation=generation,
                    planned=first,
                    owner_id=owner_id,
                    batch_executor=execute_batch,
                )
            consumer.commit_slice(first)
            with provider.transaction(
                APP_ROLE,
                read_only=True,
            ) as unit:
                restored = load_advanced_options_market_state_cursor(
                    unit.cursor,
                    consumer_name=consumer_name,
                    cohort_id=cohort_id,
                    journal_generation=generation,
                    shard_map=shard_map,
                )
            restarted = AdvancedOptionsMarketStateConsumer(
                source=source,
                cohort_id=cohort_id,
                shard_map=shard_map,
                owner_epoch=2,
                consumer_name=consumer_name,
                states=restored,
            )
            assert restarted.committed_offset == first.end_offset == 1

            append_quote(writer, 2, 1.1, 1.2)
            registration.raw_committed_offset = 2
            registration.normalized_committed_offset = 2
            second = restarted.plan_slice(maximum_records=10)

            assert second.start_offset == 1
            assert second.end_offset == 2
            assert second.record_count == 1
