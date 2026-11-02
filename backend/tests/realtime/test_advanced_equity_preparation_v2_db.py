from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from psycopg2.extras import Json

from equity.domain import (
    BarAvailabilityMode,
    BarSessionScope,
    BarSourceKind,
    EquityBarRevision,
)
from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
    BoundedPolygonEquityPreparationClient,
    EquityPreparationBlockedError,
    EquityPreparationChunk,
    EquityPreparationLimits,
    EquityPreparationSourceError,
    EquityPreparationStage,
    MassiveEquityPreparationSource,
    claim_equity_preparation_work_cursor,
    load_equity_preparation_status_cursor,
    reserve_equity_preparation_provider_requests_cursor,
)
from realtime.acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from realtime.advanced_equity_capability_refresh import (
    load_capability_refresh_request_cursor,
)
from realtime.advanced_realtime_desired_state import (
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredStateChange,
    default_advanced_realtime_desired_state,
)
from realtime.advanced_realtime_desired_state_repository import (
    DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY,
    apply_desired_state,
    reconcile_desired_state,
)
from realtime.advanced_ingress_runtime_profile import (
    load_advanced_ingress_runtime_catalog,
)
from realtime.contracts import UniverseManifest, canonical_sha256
from realtime.database_access_v2 import (
    APP_ROLE,
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_bar_adapter import RealtimeEquityBarFact
from realtime.preflight import ProviderTier
from realtime.provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from realtime.reference_facts import (
    ReferenceFact,
    ReferenceFactRelation,
)
from tests.realtime.test_advanced_realtime_desired_state_v2_db import (
    _digest,
    _seed_source,
    _security_id,
)
from tests.realtime.test_advanced_equity_lifecycle_v2_db import (
    CATALOG,
    CONFIGURATION_SHA256,
    _activate_runtime_profile,
)
from scripts.run_realtime_v2_advanced_controller import (
    AdvancedSessionActivationPending,
    _prepare_continuous_session_cursor,
)
from options.calendar import OptionExchangeCalendar
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc
BASELINE_CHANGE_AT = datetime(2026, 10, 29, 21, 0, tzinfo=UTC)
DESIRED_CHANGE_AT = datetime(2026, 10, 30, 21, 0, tzinfo=UTC)
WORK_AT = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
HISTORY_SESSION = date(2026, 10, 30)
EFFECTIVE_SESSION = date(2026, 11, 2)


def _provider(pool):
    return V2ConnectionProvider(
        {OWNER_POOL: pool},
        V2AccessConfig(
            security_mode="deferred",
            statement_timeout_ms=30_000,
        ),
    )


def _claim_and_reserve(
    pool,
    *,
    checked_at,
    limits,
):
    provider = _provider(pool)
    with provider.transaction(APP_ROLE) as unit:
        claim = claim_equity_preparation_work_cursor(
            unit.cursor,
            owner_id=uuid4(),
            checked_at=checked_at,
            limits=limits,
        )
    assert claim is not None
    with provider.transaction(APP_ROLE) as unit:
        reservations = (
            reserve_equity_preparation_provider_requests_cursor(
                unit.cursor,
                claim,
                checked_at=checked_at,
                limits=limits,
            )
        )
    return claim, reservations


def _seed_ten_member_capability(cursor, tickers):
    catalog = load_advanced_ingress_runtime_catalog(
        DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY
    )
    source_config = catalog.profile_for_target(
        len(tickers)
    ).configuration_sha256(catalog.scale_policy.sha256)
    universe = UniverseManifest(
        version="advanced-equity-10-am-v1",
        equity_tickers=tuple(sorted(tickers)),
        option_underlyings=(),
    )
    cursor.execute(
        "INSERT INTO rt.capability_profiles ("
        "profile_id,config_sha256,schema_sha256,account_alias,"
        "capabilities_sha256,universe_sha256,checked_at,expires_at,"
        "status,profile_sha256,provider_tier,security_policy_sha256,"
        "preflight_mode,provider_requests,action_authorized"
        ") VALUES (%s,%s,%s,'c4-fixture',%s,%s,%s,%s,'PASS',%s,"
        "'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
        (
            str(uuid5(NAMESPACE_URL, "c4-target10-capability")),
            source_config,
            _digest("c4-schema"),
            _digest("c4-capabilities"),
            universe.sha256,
            DESIRED_CHANGE_AT,
            DESIRED_CHANGE_AT + timedelta(days=7),
            _digest("c4-profile"),
            _digest("c4-security-policy"),
        ),
    )


def _minimal_targets(cursor, revision_id):
    targets = {
        "REFERENCE": {
            "history_through_session": HISTORY_SESSION.isoformat(),
        },
        "ADJUSTED_DAILY": {
            "sessions": [HISTORY_SESSION.isoformat()],
        },
        "NATIVE_INTRADAY": {
            "interval_sessions": {
                "5m": [HISTORY_SESSION.isoformat()],
            },
        },
        "DERIVED_INTERVALS": {
            "source_sessions": [HISTORY_SESSION.isoformat()],
            "target_intervals": ["1h"],
        },
        "TECHNICAL_WARMUP": {
            "required_adjusted_daily_bars": 1,
            "required_derived_hourly_bars": 1,
            "declared_products": ["TEST"],
        },
    }
    for stage, target in targets.items():
        target = {
            "effective_session_date": EFFECTIVE_SESSION.isoformat(),
            **target,
        }
        cursor.execute(
            "UPDATE rt.realtime_equity_preparation_work "
            "SET target=%s WHERE revision_id=%s AND stage=%s",
            (Json(target), str(revision_id), stage),
        )


def _fact(request, *, interval, start, end, adjusted, index):
    claim = request.claim
    revision_id = uuid5(
        NAMESPACE_URL,
        "c4-fake-bar:"
        f"{claim.revision_id}:{claim.ticker}:{interval}:"
        f"{start.isoformat()}:{index}",
    )
    revision = EquityBarRevision(
        bar_revision_id=revision_id,
        security_id=claim.security_id,
        ticker=claim.ticker,
        interval=interval,
        session_date=HISTORY_SESSION,
        bar_start=start,
        bar_end=end,
        open_price=Decimal("100"),
        high_price=Decimal("102"),
        low_price=Decimal("99"),
        close_price=Decimal("101"),
        volume=Decimal("1000"),
        vwap=Decimal("100.5"),
        transaction_count=100,
        source_kind=BarSourceKind.NATIVE_REST,
        availability_mode=(
            BarAvailabilityMode.HISTORICAL_RECONSTRUCTED
        ),
        is_final=True,
        system_observed_at=request.checked_at,
        replay_available_at=end,
        adjusted=adjusted,
        payload_sha256=canonical_sha256({
            "interval": interval,
            "start": start,
            "index": index,
        }),
        ingestion_segment_id=uuid5(
            NAMESPACE_URL,
            f"c4-fake-segment:{claim.ticker}:{interval}",
        ),
        session_scope=BarSessionScope.RTH,
        quality_codes=("EXPLICIT_ADVANCED_REST_REPAIR_V1",),
    )
    return RealtimeEquityBarFact(
        cohort_id=claim.source_cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        instrument_id=claim.instrument_id,
        revision=revision,
    )


class _FakeSource:
    def __init__(
        self,
        *,
        retry_adjusted_once=False,
        retry_native_at_offset=None,
        block_reference=False,
    ):
        self.retry_adjusted_once = retry_adjusted_once
        self.retry_native_at_offset = retry_native_at_offset
        self.block_reference = block_reference
        self.calls = []
        self.native_offsets = []

    def fetch(self, request):
        stage = request.claim.stage
        self.calls.append(stage)
        lineage = {
            "provider": "FAKE",
            "request_sha256": canonical_sha256({
                "stage": stage.value,
                "watermark": request.claim.watermark,
            }),
            "response_sha256": canonical_sha256({
                "stage": stage.value,
                "attempt": request.claim.attempts,
            }),
        }
        if stage is EquityPreparationStage.REFERENCE:
            if self.block_reference:
                raise EquityPreparationBlockedError(
                    "REFERENCE_IDENTITY_BLOCKED"
                )
            reference_facts = ()
            if request.claim.security_id is None:
                security_id = uuid5(
                    NAMESPACE_URL,
                    f"realtime-security:{request.claim.ticker}",
                )
                payload_sha256 = canonical_sha256({
                    "ticker": request.claim.ticker,
                    "source": "FAKE_REFERENCE",
                })
                reference_facts = (ReferenceFact.build(
                    ReferenceFactRelation.SECURITY,
                    "SECURITY",
                    request.claim.ticker,
                    security_id=security_id,
                    revision_id=uuid5(
                        NAMESPACE_URL,
                        "c4-fake-security-revision:"
                        f"{request.claim.ticker}:{payload_sha256}",
                    ),
                    ticker=request.claim.ticker,
                    asset_type="ETF",
                    source_origin="FAKE_REFERENCE",
                    exchange="XNYS",
                    calendar_id="XNYS",
                    effective_from=datetime(
                        2026, 10, 30, tzinfo=UTC
                    ),
                    effective_to=None,
                    observed_at=request.checked_at,
                    payload_sha256=payload_sha256,
                    company_name=f"{request.claim.ticker} Fund",
                    security_type="ETF",
                    active=True,
                ),)
            return EquityPreparationChunk(
                stage,
                {"complete": True},
                {
                    "available_intervals": {},
                    "sector_ready": True,
                    "corporate_actions_ready": True,
                },
                lineage,
                True,
                reference_facts=reference_facts,
            )
        if stage is EquityPreparationStage.ADJUSTED_DAILY:
            if self.retry_adjusted_once:
                self.retry_adjusted_once = False
                raise EquityPreparationSourceError(
                    "FAKE_REQUEST_FAILURE",
                    provider_requests=1,
                    response_bytes=64,
                )
            start = datetime(
                2026, 10, 30, 13, 30, tzinfo=UTC
            )
            end = datetime(2026, 10, 30, 20, 0, tzinfo=UTC)
            return EquityPreparationChunk(
                stage,
                {"complete": True},
                {"available_intervals": {"1d:ADJUSTED": 1}},
                lineage,
                True,
                provider_requests=1,
                response_bytes=128,
                bars=(
                    _fact(
                        request,
                        interval="1d",
                        start=start,
                        end=end,
                        adjusted=True,
                        index=0,
                    ),
                ),
            )
        if stage is EquityPreparationStage.NATIVE_INTRADAY:
            offset = int(request.claim.watermark.get("offset", 0))
            self.native_offsets.append(offset)
            if self.retry_native_at_offset == offset:
                self.retry_native_at_offset = None
                raise EquityPreparationSourceError(
                    "FAKE_NATIVE_REQUEST_FAILURE",
                    provider_requests=1,
                    response_bytes=32,
                )
            start = datetime(
                2026, 10, 30, 13, 30, tzinfo=UTC
            )
            bars = tuple(
                _fact(
                    request,
                    interval="5m",
                    start=start + timedelta(minutes=5 * index),
                    end=start + timedelta(minutes=5 * (index + 1)),
                    adjusted=False,
                    index=index,
                )
                for index in range(offset, offset + 6)
            )
            after = offset + len(bars)
            return EquityPreparationChunk(
                stage,
                {"offset": after},
                {
                    "available_intervals": {
                        "5m:RAW_UNADJUSTED": after,
                    }
                },
                lineage,
                after == 12,
                provider_requests=1,
                response_bytes=256,
                bars=bars,
            )
        raise AssertionError(stage)


def _setup_desired_change(pool, *, ticker="XLK"):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            source_cohort_id, _ = _seed_source(
                cursor,
                runtime_configuration_sha256=(
                    CONFIGURATION_SHA256
                ),
            )
            _activate_runtime_profile(cursor)
        connection.commit()
        baseline_pending = apply_desired_state(
            connection,
            default_advanced_realtime_desired_state(source_cohort_id),
            observed_at=BASELINE_CHANGE_AT,
        )
        assert baseline_pending.state == "ACTIVATION_PENDING"
        baseline = reconcile_desired_state(
            connection,
            baseline_pending.revision.revision_id,
            materialized_at=(
                OptionExchangeCalendar("XNYS").session_open(
                    baseline_pending.revision.effective_session_date
                )
                - timedelta(
                    seconds=(
                        CATALOG.profile_for_target(
                            9
                        ).settings.controller_warmup_seconds
                    )
                )
            ),
        )
        assert baseline.snapshot is not None
        members = list(
            default_advanced_realtime_desired_state(
                baseline.snapshot.cohort_id,
                source_slot_version=baseline.snapshot.slot_set_version,
            ).members
        )
        members.append(
            DesiredMemberIntent(ticker, DesiredMemberKind.SECTOR_ETF)
        )
        tickers = {
            member.ticker
            for member in members
            if member.kind in {
                DesiredMemberKind.EQUITY,
                DesiredMemberKind.SECTOR_ETF,
            }
        }
        with connection.cursor() as cursor:
            _seed_ten_member_capability(cursor, tickers)
        connection.commit()
        desired = apply_desired_state(
            connection,
            DesiredStateChange(
                source_cohort_id=baseline.snapshot.cohort_id,
                source_slot_version=baseline.snapshot.slot_set_version,
                members=tuple(members),
            ),
            observed_at=DESIRED_CHANGE_AT,
        )
        with connection.cursor() as cursor:
            _minimal_targets(cursor, desired.revision.revision_id)
        connection.commit()
        return baseline, desired
    finally:
        pool.putconn(connection)


def _controller_args():
    return SimpleNamespace(
        catch_up_mode="AUTO",
        stream_sessions=4,
        connection_attempts=12,
        decoded_bytes=1024 * 1024 * 1024,
        budgets=None,
        preferred_cohort_id=None,
        cohort_selection_reference=None,
        canary_start_at=None,
        canary_end_at=None,
    )


def _activate_session(pool, session_date, checked_at):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            result = _prepare_continuous_session_cursor(
                cursor,
                args=_controller_args(),
                catalog=CATALOG,
                calendar=OptionExchangeCalendar("XNYS"),
                session_date=session_date,
                checked_at=checked_at,
            )
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


@pytest.mark.parametrize("after_first_response", (False, True))
def test_provider_reservation_survives_crash_and_restart(
    after_first_response,
):
    with disposable_v2_database(
        f"test_realtime_v2_c4_reserve_{uuid4().hex[:8]}"
    ) as pool:
        _baseline, desired = _setup_desired_change(pool)
        limits = EquityPreparationLimits(
            maximum_response_bytes_per_chunk=300,
            maximum_requests_per_member=6,
            maximum_response_bytes_per_member=600,
            maximum_account_requests=6,
            maximum_account_response_bytes=600,
            lease_seconds=30,
        )
        first, reservations = _claim_and_reserve(
            pool,
            checked_at=WORK_AT,
            limits=limits,
        )
        assert len(reservations) == 3
        provider = _provider(pool)
        with provider.transaction(APP_ROLE) as unit:
            replayed = reserve_equity_preparation_provider_requests_cursor(
                unit.cursor,
                first,
                checked_at=WORK_AT,
                limits=limits,
            )
        assert tuple(row.request_id for row in replayed) == tuple(
            row.request_id for row in reservations
        )

        if after_first_response:
            class Response:
                status_code = 200
                content = b'{"results":{"ticker":"XLK"}}'

                @staticmethod
                def json():
                    return {"results": {"ticker": "XLK"}}

            class Session:
                @staticmethod
                def get(*_args, **_kwargs):
                    return Response()

            client = BoundedPolygonEquityPreparationClient(
                "fake-key",
                session=Session(),
            )
            client.begin_chunk(
                maximum_requests=3,
                maximum_response_bytes=300,
                reservation_ids=tuple(
                    row.request_id for row in reservations
                ),
            )
            assert client.fetch_ticker_overview("XLK")["ticker"] == "XLK"
            assert client.chunk_metrics()[0] == 1

        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT state,count(*) "
                    "FROM rt.provider_request_ledger "
                    "WHERE authorization_reference LIKE %s "
                    "GROUP BY state",
                    (
                        "equity-preparation:"
                        f"{desired.revision.revision_id}:"
                        f"{first.intent_id}:%",
                    ),
                )
                assert tuple(cursor.fetchall()) == (("RESERVED", 3),)
        finally:
            pool.putconn(connection)

        second, second_reservations = _claim_and_reserve(
            pool,
            checked_at=WORK_AT + timedelta(seconds=31),
            limits=limits,
        )
        assert second.watermark == first.watermark
        assert len(second_reservations) == 3
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT state,count(*) "
                    "FROM rt.provider_request_ledger "
                    "WHERE authorization_reference LIKE %s "
                    "GROUP BY state ORDER BY state",
                    (
                        "equity-preparation:"
                        f"{desired.revision.revision_id}:"
                        f"{first.intent_id}:%",
                    ),
                )
                assert tuple(cursor.fetchall()) == (
                    ("AMBIGUOUS", 3),
                    ("RESERVED", 3),
                )
        finally:
            pool.putconn(connection)

        provider = _provider(pool)
        with provider.transaction(APP_ROLE) as unit:
            third = claim_equity_preparation_work_cursor(
                unit.cursor,
                owner_id=uuid4(),
                checked_at=WORK_AT + timedelta(seconds=62),
                limits=limits,
            )
        with pytest.raises(
            ProviderBudgetExceeded,
            match="member provider budget",
        ):
            with provider.transaction(APP_ROLE) as unit:
                reserve_equity_preparation_provider_requests_cursor(
                    unit.cursor,
                    third,
                    checked_at=WORK_AT + timedelta(seconds=62),
                    limits=limits,
                )
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*),sum(reserved_response_bytes) "
                    "FROM rt.provider_request_ledger "
                    "WHERE authorization_reference LIKE %s",
                    (
                        "equity-preparation:"
                        f"{desired.revision.revision_id}:"
                        f"{first.intent_id}:%",
                    ),
                )
                assert cursor.fetchone() == (6, 600)
        finally:
            pool.putconn(connection)


def test_handled_http_failure_reconciles_reserved_provider_effects():
    with disposable_v2_database(
        f"test_realtime_v2_c4_http_{uuid4().hex[:8]}"
    ) as pool:
        _setup_desired_change(pool)

        class Response:
            status_code = 500
            content = b"failure"

            @staticmethod
            def json():
                return {"error": "failure"}

        class Session:
            @staticmethod
            def get(*_args, **_kwargs):
                return Response()

        resident = AdvancedEquityPreparationResident(
            _provider(pool),
            MassiveEquityPreparationSource(
                BoundedPolygonEquityPreparationClient(
                    "fake-key",
                    session=Session(),
                )
            ),
            owner_id=uuid4(),
            limits=EquityPreparationLimits(
                maximum_response_bytes_per_chunk=300,
                maximum_response_bytes_per_member=1_000,
                maximum_account_response_bytes=10_000,
            ),
            clock=lambda: WORK_AT,
        )

        result = resident.run_once()

        assert result.status == "RETRY"
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT state,outcome,response_bytes,failure_code "
                    "FROM rt.provider_request_ledger "
                    "WHERE authorization_reference LIKE "
                    "'equity-preparation:%' "
                    "ORDER BY response_bytes DESC,request_id"
                )
                rows = tuple(cursor.fetchall())
                assert len(rows) == 3
                assert rows[0][:3] == ("FAILED", "HTTP_ERROR", 7)
                assert rows[0][3].startswith(
                    "REFERENCE_SOURCE_REQUEST_FAILED"
                )
                assert all(row[0] == "FAILED" for row in rows)
                assert sum(int(row[2] or 0) for row in rows) == 7
        finally:
            pool.putconn(connection)


def test_preparation_reservation_counts_concurrent_account_work():
    with disposable_v2_database(
        f"test_realtime_v2_c4_account_{uuid4().hex[:8]}"
    ) as pool:
        _setup_desired_change(pool)
        limits = EquityPreparationLimits(
            maximum_response_bytes_per_chunk=300,
            maximum_account_requests=3,
            maximum_account_response_bytes=1_000,
        )
        provider = _provider(pool)
        scope = ProviderBudgetScope(
            account_alias="MASSIVE_ADVANCED",
            session_date=EFFECTIVE_SESSION,
            authorization_reference="concurrent-account-work",
            provider_authorization_reference="shared-account",
            expected_cost="existing",
            request_ceiling=3,
            decoded_byte_ceiling=1_000,
            account_request_ceiling=3,
            account_decoded_byte_ceiling=1_000,
            lanes=frozenset({ProviderRequestLane.EQUITY_HISTORY}),
            authorized_until=datetime(
                2026, 11, 3, tzinfo=UTC
            ),
        )
        with provider.transaction(APP_ROLE) as unit:
            reserve_coordinated_provider_request_cursor(
                unit.cursor,
                scope,
                ProviderRequestReservation(
                    request_id=uuid4(),
                    account_alias="MASSIVE_ADVANCED",
                    lane=ProviderRequestLane.EQUITY_HISTORY,
                    endpoint_class="CONCURRENT_ACCOUNT",
                    request_sha256="a" * 64,
                    reserved_at=WORK_AT,
                ),
                reserved_response_bytes=100,
                account_wide=True,
            )
            claim = claim_equity_preparation_work_cursor(
                unit.cursor,
                owner_id=uuid4(),
                checked_at=WORK_AT,
                limits=limits,
            )
        with pytest.raises(
            ProviderBudgetExceeded,
            match="account request ceiling",
        ):
            with provider.transaction(APP_ROLE) as unit:
                reserve_equity_preparation_provider_requests_cursor(
                    unit.cursor,
                    claim,
                    checked_at=WORK_AT,
                    limits=limits,
                )
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.provider_request_ledger"
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)


def test_tenth_member_resumes_bounded_work_and_promotes_next_snapshot():
    with disposable_v2_database(
        f"test_realtime_v2_c4_prepare_{uuid4().hex[:8]}"
    ) as pool:
        baseline, desired = _setup_desired_change(pool)
        assert desired.state == "ACTIVATION_PENDING"
        assert desired.snapshot is None
        assert next(
            row.status
            for row in desired.preparations
            if row.ticker == "XLK"
        ).value == "WARMING"
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                capability_request = (
                    load_capability_refresh_request_cursor(
                        cursor,
                        checked_at=WORK_AT,
                        refresh_before_seconds=1800,
                    )
                )
            catalog = load_advanced_ingress_runtime_catalog(
                DEFAULT_ADVANCED_INGRESS_RUNTIME_POLICY
            )
            assert capability_request.configuration_sha256 == (
                catalog.profile_for_target(10).configuration_sha256(
                    catalog.scale_policy.sha256
                )
            )
        finally:
            pool.putconn(connection)
        source = _FakeSource(
            retry_adjusted_once=True,
            retry_native_at_offset=6,
        )
        clock = [WORK_AT]

        def run_once():
            resident = AdvancedEquityPreparationResident(
                _provider(pool),
                source,
                owner_id=uuid4(),
                limits=EquityPreparationLimits(
                    retry_initial_seconds=15,
                    retry_maximum_seconds=60,
                ),
                clock=lambda: clock[0],
            )
            return resident.run_once()

        assert run_once().stage == "REFERENCE"
        retry = run_once()
        assert retry.status == "RETRY"
        assert retry.stage == "ADJUSTED_DAILY"
        clock[0] = retry.next_retry_at
        resumed = run_once()
        assert resumed.status == "COMPLETE"
        assert resumed.attempts == 2
        assert run_once().status == "PROGRESSED"
        native_retry = run_once()
        assert native_retry.status == "RETRY"
        assert native_retry.next_retry_at - clock[0] == timedelta(
            seconds=15
        )
        clock[0] = native_retry.next_retry_at
        assert run_once().status == "COMPLETE"
        assert source.native_offsets == [0, 6, 6]
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.bar_current AS current "
                    "JOIN rt.bar_revisions AS revision "
                    "ON revision.bar_revision_id=current.bar_revision_id "
                    "AND revision.observed_at=current.observed_at "
                    "WHERE revision.ticker='XLK' "
                    "AND current.interval='5m'"
                )
                assert cursor.fetchone()[0] == 12
        finally:
            pool.putconn(connection)
        derived = run_once()
        assert derived.stage == "DERIVED_INTERVALS"
        assert derived.status == "COMPLETE", derived.reason
        promoted = run_once()
        assert promoted.stage == "TECHNICAL_WARMUP"
        assert promoted.reconciliation_state == "ACTIVATION_PENDING"
        assert run_once().status == "IDLE"
        calendar = OptionExchangeCalendar("XNYS")
        activation_at = calendar.session_open(
            desired.revision.effective_session_date
        ) - timedelta(
            seconds=(
                CATALOG.profile_for_target(
                    10
                ).settings.controller_warmup_seconds
            )
        )
        with pytest.raises(AdvancedSessionActivationPending):
            _activate_session(
                pool,
                desired.revision.effective_session_date,
                activation_at - timedelta(seconds=1),
            )
        ready, plan, _profile = _activate_session(
            pool,
            desired.revision.effective_session_date,
            activation_at,
        )
        assert ready.equity_member_count == 10
        assert plan.universe_target == 10

        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current.interval,count(*) "
                    "FROM rt.bar_current AS current "
                    "JOIN rt.bar_revisions AS revision "
                    "ON revision.bar_revision_id=current.bar_revision_id "
                    "AND revision.observed_at=current.observed_at "
                    "WHERE revision.ticker='XLK' "
                    "GROUP BY current.interval ORDER BY current.interval"
                )
                assert tuple(cursor.fetchall()) == (
                    ("1d", 1),
                    ("1h", 1),
                    ("5m", 12),
                )
                cursor.execute(
                    "SELECT count(*) FROM "
                    "rt.realtime_desired_session_snapshots"
                )
                assert cursor.fetchone()[0] == 2
                cursor.execute(
                    "SELECT count(*) FROM rt.advanced_ingress_plans "
                    "WHERE session_date=%s",
                    (desired.revision.effective_session_date,),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*) FROM rt.advanced_ingress_plan_usage "
                    "WHERE plan_id=%s",
                    (str(plan.plan_id),),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT manifest.stock_count "
                    "FROM rt.realtime_desired_current AS current "
                    "JOIN rt.realtime_desired_session_snapshots AS snapshot "
                    "ON snapshot.revision_id=current.revision_id "
                    "JOIN rt.subscription_manifests AS manifest "
                    "ON manifest.manifest_id=snapshot.manifest_id "
                    "WHERE current.scope_key='ADVANCED_REALTIME'"
                )
                assert cursor.fetchone()[0] == 10
                status = load_equity_preparation_status_cursor(
                    cursor,
                    active_on=HISTORY_SESSION,
                    ticker="XLK",
                )
                assert status["members"][0]["readiness"] == "READY"
                assert status["members"][0]["available_intervals"] == {
                    "1d:ADJUSTED": 1,
                    "1h:RAW_UNADJUSTED": 1,
                    "5m:RAW_UNADJUSTED": 12,
                }
                availability = {
                    (
                        row["interval"],
                        row["adjustment_basis"],
                    ): row
                    for row in status["members"][0][
                        "stored_availability"
                    ]
                }
                assert availability[("5m", "RAW_UNADJUSTED")][
                    "count"
                ] == 12
                assert availability[("1h", "RAW_UNADJUSTED")][
                    "latest"
                ]["close"] == "101.00000000"
                assert availability[("1h", "RAW_UNADJUSTED")][
                    "source_lineage"
                ]["source_kind"] == "DERIVED"
                assert status["members"][0]["publication_state"] == (
                    "PREPARATION_ONLY_READY_AWAITING_C7"
                )
                assert status["c7_handoff"] == {
                    "contract": "advanced-equity-c7-handoff-v1",
                    "status": "REQUIRED",
                    "members": ["XLK"],
                    "owner": "C7_CANONICAL_RELEASE_MERGE",
                }
                assert status["active_snapshot"]["cohort_id"] == str(
                    baseline.snapshot.cohort_id
                )
                assert status["intended_snapshot"] is not None
                cursor.execute(
                    "SELECT attempts,failure_streak,watermark,status "
                    "FROM rt.realtime_equity_preparation_work "
                    "WHERE revision_id=%s AND ticker='XLK' "
                    "AND stage='NATIVE_INTRADAY'",
                    (status["revision_id"],),
                )
                (
                    attempts,
                    failure_streak,
                    watermark,
                    work_status,
                ) = cursor.fetchone()
                assert attempts == 3
                assert failure_streak == 0
                assert watermark == {"offset": 12}
                assert work_status == "COMPLETE"
                cursor.execute(
                    "SELECT attempts,failure_streak,"
                    "provider_request_count,response_bytes "
                    "FROM rt.realtime_equity_preparation_work "
                    "WHERE revision_id=%s AND ticker='XLK' "
                    "AND stage='ADJUSTED_DAILY'",
                    (status["revision_id"],),
                )
                assert cursor.fetchone() == (2, 0, 2, 192)
                cursor.execute(
                    "SELECT state,count(*) "
                    "FROM rt.provider_request_ledger "
                    "WHERE authorization_reference LIKE %s "
                    "GROUP BY state ORDER BY state",
                    (
                        "equity-preparation:"
                        f"{desired.revision.revision_id}:%",
                    ),
                )
                assert tuple(cursor.fetchall()) == (
                    ("COMPLETE", 3),
                    ("FAILED", 5),
                )
                cursor.execute(
                    "SELECT count(*) FROM rt.provider_request_ledger "
                    "WHERE state IN ('RESERVED','AMBIGUOUS') "
                    "AND authorization_reference LIKE %s",
                    (
                        "equity-preparation:"
                        f"{desired.revision.revision_id}:%",
                    ),
                )
                assert cursor.fetchone()[0] == 0
        finally:
            pool.putconn(connection)


def test_absent_ticker_intent_resolves_without_manual_reference_bootstrap():
    with disposable_v2_database(
        f"test_realtime_v2_c4_unresolved_{uuid4().hex[:8]}"
    ) as pool:
        _baseline, desired = _setup_desired_change(
            pool,
            ticker="XLU",
        )
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM ref.security_revisions "
                    "WHERE ticker='XLU'"
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT security_id,instrument_id "
                    "FROM rt.realtime_equity_preparation_work "
                    "WHERE revision_id=%s AND ticker='XLU' "
                    "AND stage='REFERENCE'",
                    (str(desired.revision.revision_id),),
                )
                assert cursor.fetchone() == (None, None)
        finally:
            pool.putconn(connection)

        source = _FakeSource()

        def run_once():
            return AdvancedEquityPreparationResident(
                _provider(pool),
                source,
                owner_id=uuid4(),
                clock=lambda: WORK_AT,
            ).run_once()

        reference = run_once()
        assert reference.stage == "REFERENCE"
        assert reference.status == "COMPLETE"
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT binding.security_id::text,binding.instrument_id "
                    "FROM rt.realtime_desired_member_intents AS intent "
                    "JOIN rt.realtime_desired_member_bindings AS binding "
                    "ON binding.intent_id=intent.intent_id "
                    "WHERE intent.revision_id=%s AND intent.ticker='XLU'",
                    (str(desired.revision.revision_id),),
                )
                binding = cursor.fetchone()
                assert binding is not None
                cursor.execute(
                    "SELECT count(*) FROM ref.security_revisions "
                    "WHERE ticker='XLU' AND security_id=%s",
                    (binding[0],),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*) FROM rt.instruments "
                    "WHERE instrument_id=%s AND security_id=%s",
                    (binding[1], binding[0]),
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)

        assert run_once().stage == "ADJUSTED_DAILY"
        assert run_once().status == "PROGRESSED"
        assert run_once().status == "COMPLETE"
        assert run_once().stage == "DERIVED_INTERVALS"
        promotion = run_once()
        assert promotion.stage == "TECHNICAL_WARMUP"
        assert promotion.reconciliation_state == "ACTIVATION_PENDING"
        activation_at = OptionExchangeCalendar("XNYS").session_open(
            desired.revision.effective_session_date
        ) - timedelta(
            seconds=(
                CATALOG.profile_for_target(
                    10
                ).settings.controller_warmup_seconds
            )
        )
        _activate_session(
            pool,
            desired.revision.effective_session_date,
            activation_at,
        )
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT manifest.stock_count "
                    "FROM rt.realtime_desired_current AS current "
                    "JOIN rt.realtime_desired_session_snapshots AS snapshot "
                    "ON snapshot.revision_id=current.revision_id "
                    "JOIN rt.subscription_manifests AS manifest "
                    "ON manifest.manifest_id=snapshot.manifest_id "
                    "WHERE current.scope_key='ADVANCED_REALTIME'"
                )
                assert cursor.fetchone()[0] == 10
        finally:
            pool.putconn(connection)


def test_member_ready_after_activation_rolls_to_following_session():
    with disposable_v2_database(
        f"test_realtime_v2_c4_late_ready_{uuid4().hex[:8]}"
    ) as pool:
        _baseline, desired = _setup_desired_change(pool)
        calendar = OptionExchangeCalendar("XNYS")
        activation_at = calendar.session_open(
            desired.revision.effective_session_date
        ) - timedelta(
            seconds=(
                CATALOG.profile_for_target(
                    10
                ).settings.controller_warmup_seconds
            )
        )
        established, baseline_plan, _profile = _activate_session(
            pool,
            desired.revision.effective_session_date,
            activation_at,
        )
        assert established.equity_member_count == 9
        assert baseline_plan.universe_target == 9

        source = _FakeSource()
        late = activation_at + timedelta(minutes=1)

        def run_once():
            return AdvancedEquityPreparationResident(
                _provider(pool),
                source,
                owner_id=uuid4(),
                clock=lambda: late,
            ).run_once()

        assert run_once().stage == "REFERENCE"
        assert run_once().stage == "ADJUSTED_DAILY"
        assert run_once().status == "PROGRESSED"
        assert run_once().status == "COMPLETE"
        assert run_once().stage == "DERIVED_INTERVALS"
        promotion = run_once()
        assert promotion.reconciliation_state == "ACTIVATION_PENDING"
        following_session = calendar.next_session_date(
            desired.revision.effective_session_date
        )

        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT effective_session_date "
                    "FROM rt.realtime_desired_current "
                    "WHERE scope_key='ADVANCED_REALTIME'"
                )
                assert cursor.fetchone()[0] == following_session
                cursor.execute(
                    "SELECT count(*) FROM rt.advanced_ingress_plans "
                    "WHERE session_date=%s",
                    (desired.revision.effective_session_date,),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*) FROM rt.advanced_ingress_plan_usage "
                    "WHERE plan_id=%s",
                    (str(baseline_plan.plan_id),),
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)

        following_activation = calendar.session_open(
            following_session
        ) - timedelta(
            seconds=(
                CATALOG.profile_for_target(
                    10
                ).settings.controller_warmup_seconds
            )
        )
        ready, following_plan, _profile = _activate_session(
            pool,
            following_session,
            following_activation,
        )
        assert ready.equity_member_count == 10
        assert following_plan.universe_target == 10
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT session_date,count(*) "
                    "FROM rt.advanced_ingress_plans "
                    "WHERE session_date IN (%s,%s) "
                    "GROUP BY session_date ORDER BY session_date",
                    (
                        desired.revision.effective_session_date,
                        following_session,
                    ),
                )
                assert tuple(cursor.fetchall()) == (
                    (desired.revision.effective_session_date, 1),
                    (following_session, 1),
                )
                cursor.execute(
                    "SELECT count(*) FROM rt.advanced_ingress_plan_usage "
                    "WHERE plan_id IN (%s,%s)",
                    (
                        str(baseline_plan.plan_id),
                        str(following_plan.plan_id),
                    ),
                )
                assert cursor.fetchone()[0] == 2
        finally:
            pool.putconn(connection)


def test_exhausted_member_provider_budget_is_permanently_blocked():
    with disposable_v2_database(
        f"test_realtime_v2_c4_budget_{uuid4().hex[:8]}"
    ) as pool:
        _baseline, desired = _setup_desired_change(pool)
        resident = AdvancedEquityPreparationResident(
            _provider(pool),
            _FakeSource(),
            owner_id=uuid4(),
            limits=EquityPreparationLimits(
                maximum_requests_per_member=4,
            ),
            clock=lambda: WORK_AT,
        )

        assert resident.run_once().stage == "REFERENCE"
        assert resident.run_once().stage == "ADJUSTED_DAILY"
        blocked = resident.run_once()

        assert blocked.status == "BLOCKED"
        assert blocked.reason == "MEMBER_PROVIDER_BUDGET_EXHAUSTED"
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT work.status,work.blocked_reason,"
                    "work.watermark,readiness.status,readiness.reasons "
                    "FROM rt.realtime_equity_preparation_work AS work "
                    "JOIN rt.realtime_desired_intent_readiness AS readiness "
                    "ON readiness.intent_id=work.intent_id "
                    "WHERE work.revision_id=%s AND work.ticker='XLK' "
                    "AND work.stage='NATIVE_INTRADAY'",
                    (str(desired.revision.revision_id),),
                )
                assert cursor.fetchone() == (
                    "BLOCKED",
                    "MEMBER_PROVIDER_BUDGET_EXHAUSTED",
                    {},
                    "FAILED",
                    ["MEMBER_PROVIDER_BUDGET_EXHAUSTED"],
                )
        finally:
            pool.putconn(connection)


def test_blocked_new_member_keeps_established_snapshot_available():
    with disposable_v2_database(
        f"test_realtime_v2_c4_blocked_{uuid4().hex[:8]}"
    ) as pool:
        baseline, desired = _setup_desired_change(pool)
        resident = AdvancedEquityPreparationResident(
            _provider(pool),
            _FakeSource(block_reference=True),
            owner_id=uuid4(),
            clock=lambda: WORK_AT,
        )

        result = resident.run_once()

        assert result.status == "BLOCKED"
        assert result.reconciliation_state == "ACTIVATION_PENDING"
        activation_at = OptionExchangeCalendar("XNYS").session_open(
            desired.revision.effective_session_date
        ) - timedelta(
            seconds=(
                CATALOG.profile_for_target(
                    10
                ).settings.controller_warmup_seconds
            )
        )
        _activate_session(
            pool,
            desired.revision.effective_session_date,
            activation_at,
        )
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                status = load_equity_preparation_status_cursor(
                    cursor,
                    active_on=HISTORY_SESSION,
                    ticker="XLK",
                )
                assert status["members"][0]["readiness"] == "FAILED"
                assert status["active_snapshot"]["cohort_id"] == str(
                    baseline.snapshot.cohort_id
                )
                cursor.execute(
                    "SELECT manifest.stock_count "
                    "FROM rt.realtime_desired_session_snapshots AS snapshot "
                    "JOIN rt.subscription_manifests AS manifest "
                    "ON manifest.manifest_id=snapshot.manifest_id "
                    "WHERE snapshot.revision_id=%s",
                    (str(desired.revision.revision_id),),
                )
                assert cursor.fetchone()[0] == 9
        finally:
            pool.putconn(connection)
