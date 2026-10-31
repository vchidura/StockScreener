from datetime import date, datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from realtime.advanced_realtime_desired_state import (
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredStateChange,
    MemberPreparationStatus,
    default_advanced_realtime_desired_state,
)
from realtime.advanced_realtime_desired_state_repository import (
    apply_desired_state,
    load_current_desired_state_revision_cursor,
    record_desired_state_revision_cursor,
    reconcile_desired_state,
    update_desired_member_preparation,
)
from realtime.contracts import (
    AssetClass,
    EQUITY_VALIDATION_UNIVERSE,
    MarketChannel,
    SubscriptionManifest,
    SubscriptionMember,
    UniverseManifest,
    canonical_sha256,
)
from realtime.option_manifest_cohort_repository import (
    option_universe_manifest_id,
    persist_option_universe_manifest_cursor,
    persist_shadow_cohort_cursor,
)
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from realtime.option_slot_set_repository import persist_and_activate_slot_set_cursor
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc
SOURCE_SESSION = date(2026, 10, 30)
SOURCE_EFFECTIVE = datetime(2026, 10, 30, 13, 30, tzinfo=UTC)
AFTER_CLOSE = datetime(2026, 10, 30, 21, 0, tzinfo=UTC)
MARKET_HOURS = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)


def _digest(label: str) -> str:
    return canonical_sha256({"fixture": label})


def _security_id(ticker: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"c2-security:{ticker}")


def _seed_source(cursor, *, runtime_configuration_sha256=None):
    profile_id = uuid5(NAMESPACE_URL, "c2-profile")
    profile_sha256 = _digest("profile")
    profile_configuration_sha256 = (
        runtime_configuration_sha256
        or _digest("profile-config")
    )
    cohort_configuration_sha256 = (
        runtime_configuration_sha256
        or _digest("source-configuration")
    )
    cursor.execute(
        "INSERT INTO rt.capability_profiles ("
        "profile_id,config_sha256,schema_sha256,account_alias,"
        "capabilities_sha256,universe_sha256,checked_at,expires_at,"
        "status,profile_sha256,provider_tier,security_policy_sha256,"
        "preflight_mode,provider_requests,action_authorized) VALUES ("
        "%s,%s,%s,'c2-fixture',%s,%s,%s,%s,'PASS',%s,"
        "'MASSIVE_ADVANCED',%s,'PASSIVE',false,false)",
        (
            str(profile_id),
            profile_configuration_sha256,
            _digest("schema"),
            _digest("capabilities"),
            EQUITY_VALIDATION_UNIVERSE.sha256,
            SOURCE_EFFECTIVE - timedelta(days=1),
            SOURCE_EFFECTIVE + timedelta(days=7),
            profile_sha256,
            _digest("security-policy"),
        ),
    )
    tickers = (*EQUITY_VALIDATION_UNIVERSE.equity_tickers, "XLK")
    instrument_ids = {}
    for ticker in tickers:
        security_id = _security_id(ticker)
        is_etf = ticker in {"SPY", "QQQ", "IWM", "XLK"}
        cursor.execute(
            "INSERT INTO ref.securities ("
            "security_id,asset_type,source_origin,created_at) "
            "VALUES (%s,%s,'c2-fixture',%s)",
            (
                str(security_id),
                "ETF" if is_etf else "STOCK",
                SOURCE_EFFECTIVE - timedelta(days=10),
            ),
        )
        cursor.execute(
            "INSERT INTO ref.security_revisions ("
            "revision_id,security_id,ticker,exchange,calendar_id,"
            "effective_from,effective_to,observed_at,payload_sha256,"
            "security_type,active"
            ") VALUES (%s,%s,%s,'XNYS','XNYS',%s,%s,%s,%s,%s,true)",
            (
                str(uuid4()),
                str(security_id),
                ticker,
                SOURCE_EFFECTIVE - timedelta(days=9),
                (
                    SOURCE_EFFECTIVE - timedelta(days=8)
                    if ticker == "SPY"
                    else None
                ),
                SOURCE_EFFECTIVE - timedelta(days=9),
                _digest(f"security:{ticker}"),
                "ETF" if is_etf else "CS",
            ),
        )
        cursor.execute(
            "INSERT INTO rt.instruments (asset_class,security_id) "
            "VALUES ('EQUITY',%s) RETURNING instrument_id",
            (str(security_id),),
        )
        instrument_ids[ticker] = int(cursor.fetchone()[0])
        if ticker == "SPY":
            cursor.execute(
                "INSERT INTO ref.security_revisions ("
                "revision_id,security_id,ticker,exchange,calendar_id,"
                "effective_from,observed_at,payload_sha256,"
                "security_type,active) VALUES ("
                "%s,%s,%s,'XNYS','XNYS',%s,%s,%s,'ETF',true)",
                (
                    str(uuid4()),
                    str(security_id),
                    ticker,
                    SOURCE_EFFECTIVE - timedelta(days=8),
                    SOURCE_EFFECTIVE - timedelta(days=8),
                    _digest("security:SPY:observed-update"),
                ),
            )

    contracts = []
    for offset, (ticker, call_put) in enumerate(
        (("QQQ", "C"), ("QQQ", "P"), ("SPY", "C"), ("SPY", "P")),
        start=1,
    ):
        contract_id = 9_100_000 + offset
        version_id = 9_200_000 + offset
        contract_ticker = (
            f"O:{ticker}261120{call_put}"
            f"{(500_000 + offset * 1_000):08d}"
        )
        cursor.execute(
            "INSERT INTO ref.option_contracts ("
            "contract_id,underlying_security_id,provider,contract_ticker,"
            "first_observed_at,source_origin) VALUES ("
            "%s,%s,'massive',%s,%s,'c2-fixture')",
            (
                contract_id,
                str(_security_id(ticker)),
                contract_ticker,
                SOURCE_EFFECTIVE - timedelta(days=2),
            ),
        )
        cursor.execute(
            "INSERT INTO ref.option_contract_versions ("
            "version_id,contract_id,call_put,expiration,strike,"
            "exercise_style,multiplier,deliverables,eligibility,"
            "effective_from,observed_at,payload_sha256) VALUES ("
            "%s,%s,%s,%s,%s,'AMERICAN',100,'[]'::jsonb,"
            "'VALIDATED_ACTIVE',%s,%s,%s)",
            (
                version_id,
                contract_id,
                call_put,
                date(2026, 11, 20),
                500 + offset,
                SOURCE_EFFECTIVE - timedelta(days=2),
                SOURCE_EFFECTIVE - timedelta(days=2),
                _digest(f"contract:{ticker}:{call_put}"),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.instruments (asset_class,contract_id) "
            "VALUES ('OPTION',%s) RETURNING instrument_id",
            (contract_id,),
        )
        option_instrument_id = int(cursor.fetchone()[0])
        contracts.append(
            (
                ticker,
                contract_ticker,
                option_instrument_id,
                version_id,
            )
        )

    universe = UniverseManifest(
        version="c2-source-target9-exact-four-v1",
        equity_tickers=EQUITY_VALIDATION_UNIVERSE.equity_tickers,
        option_underlyings=("QQQ", "SPY"),
    )
    equity_members = tuple(
        SubscriptionMember(
            AssetClass.EQUITY,
            ticker,
            ticker,
            MarketChannel.MINUTE_AGGREGATE,
            priority=1,
        )
        for ticker in universe.equity_tickers
    )
    option_members = tuple(
        SubscriptionMember(
            AssetClass.OPTION,
            contract_ticker,
            ticker,
            channel,
            priority=1,
            catalog_version_id=version_id,
        )
        for ticker, contract_ticker, _instrument_id, version_id in contracts
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    policy_sha256 = _digest("exact-four-selection-policy")
    manifest = SubscriptionManifest(
        version="c2-source-target9-exact-four-v1",
        session_date=SOURCE_SESSION,
        universe=universe,
        capability_profile_sha256=profile_sha256,
        allocation_policy_sha256=policy_sha256,
        members=equity_members + option_members,
    )
    manifest_id = option_universe_manifest_id(manifest)
    persist_option_universe_manifest_cursor(
        cursor,
        manifest,
        manifest_id=manifest_id,
        profile_id=profile_id,
        effective_at=SOURCE_EFFECTIVE,
        equity_instrument_ids={
            ticker: instrument_ids[ticker]
            for ticker in universe.equity_tickers
        },
    )
    cohort_id, _cohort_sha256 = persist_shadow_cohort_cursor(
        cursor,
        manifest_id=manifest_id,
        configuration_sha256=cohort_configuration_sha256,
        model_bundle_sha256=_digest("source-model"),
        valuation_sha256=_digest("source-valuation"),
        effective_at=SOURCE_EFFECTIVE,
    )
    slot_set = SlotSetVersion(
        cohort_id=cohort_id,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=SOURCE_EFFECTIVE,
        policy_sha256=policy_sha256,
        members=tuple(
            SlotSetMember(
                instrument_id=instrument_id,
                channel=channel,
                purpose=SlotSetPurpose.PRIMARY,
                priority=1,
                catalog_version_id=version_id,
            )
            for _ticker, _contract_ticker, instrument_id, version_id in contracts
            for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
        ),
    )
    persist_and_activate_slot_set_cursor(
        cursor,
        slot_set,
        activated_at=SOURCE_EFFECTIVE,
    )
    return cohort_id, manifest_id


def test_after_close_default_revision_materializes_and_replays_exact_snapshot():
    with disposable_v2_database(
        f"test_realtime_v2_c2_default_{uuid4().hex[:8]}"
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, source_manifest_id = _seed_source(cursor)
            connection.commit()
            change = default_advanced_realtime_desired_state(source_cohort_id)

            result = apply_desired_state(
                connection,
                change,
                observed_at=AFTER_CLOSE,
            )
            replay = apply_desired_state(
                connection,
                change,
                observed_at=AFTER_CLOSE + timedelta(hours=2),
            )

            assert result.state == "READY"
            assert result.revision.effective_session_date == date(2026, 11, 2)
            assert result.snapshot is not None
            assert replay.snapshot == result.snapshot
            assert all(
                row.status is MemberPreparationStatus.READY
                for row in result.preparations
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT stock_count,option_underlying_count,member_count "
                    "FROM rt.subscription_manifests WHERE manifest_id=%s",
                    (str(result.snapshot.manifest_id),),
                )
                assert cursor.fetchone() == (9, 2, 9)
                cursor.execute(
                    "SELECT primary_contract_count,reserve_contract_count "
                    "FROM rt.slot_set_versions "
                    "WHERE cohort_id=%s AND version=1",
                    (str(result.snapshot.cohort_id),),
                )
                assert cursor.fetchone() == (4, 0)
                cursor.execute(
                    "SELECT count(*) FROM rt.slot_set_members "
                    "WHERE cohort_id=%s AND version=1",
                    (str(result.snapshot.cohort_id),),
                )
                assert cursor.fetchone()[0] == 8
                cursor.execute(
                    "SELECT count(*) FROM rt.subscription_members "
                    "WHERE manifest_id=%s",
                    (str(source_manifest_id),),
                )
                assert cursor.fetchone()[0] == 9
        finally:
            pool.putconn(connection)


def test_market_hours_change_defers_and_failed_new_member_preserves_active_snapshot():
    with disposable_v2_database(
        f"test_realtime_v2_c2_defer_{uuid4().hex[:8]}"
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, source_manifest_id = _seed_source(cursor)
            connection.commit()
            members = list(
                default_advanced_realtime_desired_state(
                    source_cohort_id
                ).members
            )
            members.append(
                DesiredMemberIntent("XLK", DesiredMemberKind.SECTOR_ETF)
            )
            change = DesiredStateChange(
                source_cohort_id=source_cohort_id,
                source_slot_version=1,
                members=tuple(members),
            )

            warming = apply_desired_state(
                connection,
                change,
                observed_at=MARKET_HOURS,
            )

            assert warming.state == "WARMING"
            assert warming.snapshot is None
            assert warming.revision.effective_session_date == date(2026, 11, 3)
            assert next(
                row.status
                for row in warming.preparations
                if row.ticker == "XLK"
            ) is MemberPreparationStatus.WARMING
            update_desired_member_preparation(
                connection,
                warming.revision.revision_id,
                ticker="XLK",
                kind=DesiredMemberKind.SECTOR_ETF,
                status=MemberPreparationStatus.FAILED,
                reasons=("HISTORY_NOT_READY",),
                checked_at=MARKET_HOURS + timedelta(minutes=1),
            )
            ready = reconcile_desired_state(
                connection,
                warming.revision.revision_id,
                materialized_at=MARKET_HOURS + timedelta(minutes=2),
            )

            assert ready.state == "READY"
            assert ready.snapshot is not None
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.subscription_members "
                    "WHERE manifest_id=%s",
                    (str(source_manifest_id),),
                )
                assert cursor.fetchone()[0] == 9
                cursor.execute(
                    "SELECT version FROM rt.slot_set_current "
                    "WHERE cohort_id=%s",
                    (str(source_cohort_id),),
                )
                assert cursor.fetchone()[0] == 1
                cursor.execute(
                    "SELECT count(*) FROM rt.subscription_members AS member "
                    "JOIN rt.instruments AS instrument "
                    "ON instrument.instrument_id=member.instrument_id "
                    "WHERE member.manifest_id=%s "
                    "AND instrument.security_id=%s",
                    (
                        str(ready.snapshot.manifest_id),
                        str(_security_id("XLK")),
                    ),
                )
                assert cursor.fetchone()[0] == 0
        finally:
            pool.putconn(connection)


def test_unknown_stored_identity_is_rejected_without_snapshot_or_source_change():
    with disposable_v2_database(
        f"test_realtime_v2_c2_identity_{uuid4().hex[:8]}"
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, source_manifest_id = _seed_source(cursor)
            connection.commit()
            change = DesiredStateChange(
                source_cohort_id=source_cohort_id,
                source_slot_version=1,
                members=(
                    *default_advanced_realtime_desired_state(
                        source_cohort_id
                    ).members,
                    DesiredMemberIntent(
                        "ZZZZ",
                        DesiredMemberKind.EQUITY,
                    ),
                ),
            )

            with pytest.raises(ValueError, match="active stored identity"):
                apply_desired_state(
                    connection,
                    change,
                    observed_at=AFTER_CLOSE,
                )

            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.realtime_desired_revisions"
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT count(*) FROM rt.subscription_members "
                    "WHERE manifest_id=%s",
                    (str(source_manifest_id),),
                )
                assert cursor.fetchone()[0] == 9
        finally:
            pool.putconn(connection)


def test_same_session_revisions_select_one_current_without_erasing_evidence():
    with disposable_v2_database(
        f"test_realtime_v2_c2_current_{uuid4().hex[:8]}"
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, _source_manifest_id = _seed_source(cursor)
            connection.commit()
            first_change = default_advanced_realtime_desired_state(
                source_cohort_id
            )
            second_change = DesiredStateChange(
                source_cohort_id=source_cohort_id,
                source_slot_version=1,
                members=(
                    *first_change.members,
                    DesiredMemberIntent(
                        "XLK",
                        DesiredMemberKind.SECTOR_ETF,
                    ),
                ),
            )
            with connection.cursor() as cursor:
                first = record_desired_state_revision_cursor(
                    cursor,
                    first_change,
                    observed_at=AFTER_CLOSE,
                )
                second = record_desired_state_revision_cursor(
                    cursor,
                    second_change,
                    observed_at=AFTER_CLOSE + timedelta(minutes=1),
                )
                replay = record_desired_state_revision_cursor(
                    cursor,
                    first_change,
                    observed_at=AFTER_CLOSE + timedelta(hours=2),
                )
                current = load_current_desired_state_revision_cursor(cursor)
            connection.commit()

            assert replay.revision_id == first.revision_id
            assert first.effective_session_date == second.effective_session_date
            assert current is not None
            assert current.revision_id == second.revision_id
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.realtime_desired_revisions"
                )
                assert cursor.fetchone()[0] == 2
                cursor.execute(
                    "SELECT count(*) FROM rt.realtime_desired_current "
                    "WHERE scope_key='ADVANCED_REALTIME'"
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)


@pytest.mark.parametrize(
    ("ticker", "kind"),
    (
        ("AAPL", DesiredMemberKind.EQUITY),
        ("QQQ", DesiredMemberKind.OPTION_UNDERLYING),
    ),
)
def test_failed_existing_source_member_blocks_reduced_snapshot(ticker, kind):
    with disposable_v2_database(
        f"test_realtime_v2_c2_block_{ticker.lower()}_{uuid4().hex[:8]}"
    ) as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                source_cohort_id, source_manifest_id = _seed_source(cursor)
                revision = record_desired_state_revision_cursor(
                    cursor,
                    default_advanced_realtime_desired_state(
                        source_cohort_id
                    ),
                    observed_at=AFTER_CLOSE,
                )
            connection.commit()
            update_desired_member_preparation(
                connection,
                revision.revision_id,
                ticker=ticker,
                kind=kind,
                status=MemberPreparationStatus.FAILED,
                reasons=("PREPARATION_FAILED",),
                checked_at=AFTER_CLOSE + timedelta(minutes=1),
            )

            blocked = reconcile_desired_state(
                connection,
                revision.revision_id,
                materialized_at=AFTER_CLOSE + timedelta(minutes=2),
            )

            assert blocked.state == "BLOCKED"
            assert blocked.snapshot is None
            assert blocked.reason_codes == (
                f"BASELINE_MEMBER_FAILED:{ticker}:{kind.value}",
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM rt.realtime_desired_session_snapshots "
                    "WHERE revision_id=%s",
                    (str(revision.revision_id),),
                )
                assert cursor.fetchone()[0] == 0
                cursor.execute(
                    "SELECT count(*) FROM rt.subscription_members "
                    "WHERE manifest_id=%s",
                    (str(source_manifest_id),),
                )
                assert cursor.fetchone()[0] == 9
                cursor.execute(
                    "SELECT version FROM rt.slot_set_current "
                    "WHERE cohort_id=%s",
                    (str(source_cohort_id),),
                )
                assert cursor.fetchone()[0] == 1
        finally:
            pool.putconn(connection)
