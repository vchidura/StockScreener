"""G102 universe sizing, Options (WP-C, Nov 15), on a disposable database: the real C8 preparation executor,
provider-free, for an underlying outside the October validation nine, and for a configured underlying whose Equity
member is not READY (UNAVAILABLE for the day while the others activate)."""
from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from realtime.advanced_options_acquisition_runtime import acquisition_binding_from_payload
from realtime.advanced_options_automatic_preparation import PostgresAutomaticOptionsPreparationExecutor
from realtime.advanced_options_control_plane import advanced_options_market_universe
from realtime.advanced_options_lifecycle import (
    reconcile_advanced_options_lifecycle_cursor,
    run_advanced_options_preparation_cycle,
)
from realtime.advanced_options_session_state import PostgresContinuousSessionStateRepository
from realtime.advanced_options_v3_deployment import validate_advanced_options_v3_runtime_identity
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE

from tests.realtime.test_advanced_options_session_state_v2_db import (
    _ProviderFreeSelectionSource,
    _add_desired_option_underlying,
    _provider,
    _provider_free_selection_capture,
    _seed_c8_equity_history,
    _seed_complete_c8_reference,
    _seed_session,
)
from tests.realtime.test_option_session_manifest import SESSION_DATE
from tests.realtime.test_option_slot_sets_v2_db import cursor
from tests.realtime.v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
ALL = ("GOOGL", "QQQ", "SPY")


def _equity_not_ready(database_cursor, ticker):
    database_cursor.execute(
        "UPDATE rt.realtime_desired_intent_readiness SET status='WARMING' "
        "WHERE intent_id IN (SELECT intent_id FROM rt.realtime_desired_member_intents "
        "WHERE ticker=%s AND member_kind='EQUITY')",
        (ticker,),
    )
    database_cursor.execute(
        "UPDATE rt.realtime_desired_member_readiness SET status='WARMING' "
        "WHERE member_kind='EQUITY' AND security_id IN (SELECT security_id "
        "FROM rt.realtime_desired_members WHERE ticker=%s AND member_kind='EQUITY')",
        (ticker,),
    )


def _prepare_until_active(pool, seed, *, captured, prepared):
    """Runs the real executor (provider-free) until the session activates; seeds the reference once every
    prepared underlying's catalog stage completed (as the generalized-membership test does)."""
    with cursor(pool) as database_cursor:
        checked_at = seed["authority_bundle"].session.prepared_at
        reconcile_advanced_options_lifecycle_cursor(
            database_cursor, target_session_date=SESSION_DATE, checked_at=checked_at,
        )
        database_cursor.connection.commit()
    provider = _provider(pool)
    source_state = {"interrupted": True, "reserved_before_io": False}
    captures = {ticker: _provider_free_selection_capture(seed, ticker) for ticker in captured}
    executor = PostgresAutomaticOptionsPreparationExecutor(
        provider,
        api_key="provider-free",
        backend_image_id="sha256:" + "e" * 64,
        maximum_provider_requests_per_underlying=1,
        maximum_decoded_bytes_per_underlying=4096,
        clock=lambda: checked_at,
        selection_source_factory=lambda claim: _ProviderFreeSelectionSource(
            provider, captures[claim.ticker], source_state,
        ),
    )
    owner_id = uuid4()
    reference_seeded = False
    lifecycle = None
    for attempt in range(30):
        cycle_at = checked_at + timedelta(minutes=attempt * 10)
        report = run_advanced_options_preparation_cycle(
            provider, executor, owner_id=owner_id, checked_at=cycle_at, clock=lambda: checked_at,
        )
        with cursor(pool) as database_cursor:
            if not reference_seeded:
                database_cursor.execute(
                    "SELECT count(*) FROM rt.advanced_options_preparation_work "
                    "WHERE stage='CATALOG_QUOTE' AND status='COMPLETE'"
                )
                if int(database_cursor.fetchone()[0]) == len(prepared):
                    snapshots = {
                        ticker: raw
                        for capture in captures.values()
                        for ticker, raw in capture.selected_snapshots.items()
                    }
                    _seed_complete_c8_reference(
                        database_cursor, seed, underlyings=prepared, snapshots=snapshots,
                    )
                    reference_seeded = True
            lifecycle = reconcile_advanced_options_lifecycle_cursor(
                database_cursor, target_session_date=SESSION_DATE, checked_at=cycle_at,
            )
            database_cursor.connection.commit()
        if lifecycle is not None and lifecycle.status == "ACTIVE":
            return lifecycle, provider
    raise AssertionError(f"C8 executor did not activate: report={report!r}, lifecycle={lifecycle!r}")


def test_one_underlying_outside_the_october_nine_prepares_and_binds():
    assert "GOOGL" not in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    with disposable_v2_database(f"test_realtime_v2_wpc_googl_{uuid4().hex[:8]}") as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor, options_ready=False, canonical_equity=True, extra_equity_tickers=("GOOGL",),
            )
            _add_desired_option_underlying(database_cursor, "GOOGL")
            _seed_c8_equity_history(database_cursor, seed, underlyings=ALL)
            database_cursor.connection.commit()
        lifecycle, provider = _prepare_until_active(pool, seed, captured=ALL, prepared=ALL)
        assert lifecycle.option_underlyings == ALL

        state = PostgresContinuousSessionStateRepository(provider, session_date=SESSION_DATE)
        package = state.load_package()
        bundle = state.load_bundle_document()
        acquisition = acquisition_binding_from_payload(bundle["acquisition_binding"])
        assert acquisition.equity_tickers == ALL
        assert package["cohort"]["underlyings"] == list(ALL)
        # every P8 role and the scheduler run this identity check on load
        validate_advanced_options_v3_runtime_identity(package, bundle)
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT profile.universe_sha256 FROM rt.cohorts AS cohort "
                "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                "JOIN rt.capability_profiles AS profile USING(profile_id) "
                "WHERE cohort.cohort_id=%s",
                (package["cohort"]["cohort_id"],),
            )
            stored_universe = str(database_cursor.fetchone()[0])
            database_cursor.execute(
                "SELECT DISTINCT underlying,flow_eligible FROM rt.advanced_options_selection_members"
            )
            members = set(database_cursor.fetchall())
        assert stored_universe == advanced_options_market_universe(ALL).sha256
        assert {underlying for underlying, _flow in members} == set(ALL)
        assert ("GOOGL", True) in members  # O-3: flow runs for a name the flow policy does not list


def test_an_underlying_whose_equity_is_not_ready_is_unavailable_and_the_rest_activate():
    with disposable_v2_database(f"test_realtime_v2_wpc_unready_{uuid4().hex[:8]}") as pool:
        with cursor(pool) as database_cursor:
            seed = _seed_session(
                database_cursor, options_ready=False, canonical_equity=True, extra_equity_tickers=("GOOGL",),
            )
            _add_desired_option_underlying(database_cursor, "GOOGL")
            _equity_not_ready(database_cursor, "GOOGL")
            _seed_c8_equity_history(database_cursor, seed, underlyings=("QQQ", "SPY"))
            database_cursor.connection.commit()
        lifecycle, provider = _prepare_until_active(
            pool, seed, captured=("QQQ", "SPY"), prepared=("QQQ", "SPY"),
        )
        assert lifecycle.option_underlyings == ("QQQ", "SPY")
        state = PostgresContinuousSessionStateRepository(provider, session_date=SESSION_DATE)
        package = state.load_package()
        assert package["cohort"]["underlyings"] == ["QQQ", "SPY"]
        validate_advanced_options_v3_runtime_identity(package, state.load_bundle_document())
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SELECT option_underlyings FROM rt.advanced_options_preparation_sessions"
            )
            assert list(database_cursor.fetchone()[0]) == ["QQQ", "SPY"]
