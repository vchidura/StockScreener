from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from psycopg2.extras import Json, RealDictCursor

from options.calendar import OptionExchangeCalendar
from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
    EquityPreparationFetchRequest,
    EquityPreparationLimits,
    EquityPreparationStage,
)
from realtime.advanced_equity_bootstrap import (
    VerifiedEquityBootstrapHistory,
    plan_equity_bootstrap,
)
from realtime.advanced_options_reference_producer import _load_bars_and_metrics
from realtime.equity_bar_repository import (
    load_bar_groups_cursor,
    persist_realtime_equity_bars_cursor,
)
from realtime.equity_history import (
    equity_history_coordinates,
    load_equity_history_cursor,
)
from realtime.preflight import ProviderTier
from realtime.database_access_v2 import APP_ROLE
from realtime.advanced_options_os1_os2_shadow import _load_bars_cursor
from realtime.option_manifest_cohort_repository import persist_shadow_cohort_cursor
from realtime.reference_continuity import source_manifest_identity_cursor
from realtime.screening_repository import load_realtime_screening_input
from equity.domain import BarSourceKind
from tests.realtime.test_advanced_equity_preparation import _claim
from tests.realtime.test_advanced_equity_preparation_v2_db import (
    WORK_AT,
    _FakeSource,
    _fact,
    _provider,
    _setup_desired_change,
)
from tests.realtime.test_advanced_realtime_desired_state_v2_db import (
    _seed_source,
    _security_id,
)
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database


pytestmark = requires_v2_database
UTC = timezone.utc
CALENDAR = OptionExchangeCalendar("XNYS")


def _insert_as_of_fact(cursor, fact, checked):
    row = fact.revision
    cursor.execute(
        "INSERT INTO rt.bar_catalog (bar_revision_id,cohort_id,provider_tier,"
        "instrument_id,interval,bar_start,bar_end,observed_at,session_date) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            str(row.bar_revision_id), str(fact.cohort_id), fact.provider_tier.value,
            fact.instrument_id, row.interval, row.bar_start, row.bar_end,
            row.system_observed_at, row.session_date,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.bar_revisions (bar_revision_id,cohort_id,provider_tier,"
        "security_id,instrument_id,ticker,interval,session_date,session_scope,"
        "adjustment_basis,bar_start,bar_end,open_price,high_price,low_price,"
        "close_price,volume,source_kind,availability_mode,is_final,observed_at,"
        "replay_available_at,ingestion_segment_id,payload_sha256,quality_codes,"
        "source_revision_ids,created_at) VALUES "
        "(%s,%s,%s,%s,%s,%s,%s,%s,'RTH',%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s,'HISTORICAL_RECONSTRUCTED',true,%s,%s,%s,%s,%s,%s::uuid[],%s)",
        (
            str(row.bar_revision_id), str(fact.cohort_id), fact.provider_tier.value,
            str(row.security_id), fact.instrument_id, row.ticker, row.interval,
            row.session_date, "ADJUSTED" if row.adjusted else "RAW_UNADJUSTED",
            row.bar_start, row.bar_end, row.open_price, row.high_price, row.low_price,
            row.close_price, row.volume, row.source_kind.value,
            row.system_observed_at, row.replay_available_at,
            str(row.ingestion_segment_id), row.payload_sha256, list(row.quality_codes),
            [str(value) for value in row.source_bar_revision_ids], checked,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.bar_current (cohort_id,provider_tier,instrument_id,"
        "interval,bar_start,session_scope,adjustment_basis,source_kind,"
        "bar_revision_id,bar_end,observed_at,updated_at) VALUES "
        "(%s,%s,%s,%s,%s,'RTH',%s,%s,%s,%s,%s,%s)",
        (
            str(fact.cohort_id), fact.provider_tier.value, fact.instrument_id,
            row.interval, row.bar_start, "ADJUSTED" if row.adjusted else "RAW_UNADJUSTED",
            row.source_kind.value,
            str(row.bar_revision_id), row.bar_end, row.system_observed_at, checked,
        ),
    )


def test_one_time_bootstrap_completes_durable_work_and_normal_restart_spends_nothing():
    class ReferenceOnlySource(_FakeSource):
        def fetch(self, request):
            assert request.claim.stage is EquityPreparationStage.REFERENCE
            return replace(
                super().fetch(request), provider_requests=3, response_bytes=333,
            )

    with disposable_v2_database(f"test_realtime_v2_bootstrap_restart_{uuid4().hex[:8]}") as pool:
        _baseline, desired = _setup_desired_change(pool)
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT security_id,instrument_id FROM rt.realtime_equity_preparation_work "
                    "WHERE revision_id=%s AND ticker='XLK' LIMIT 1",
                    (str(desired.revision.revision_id),),
                )
                security_id, instrument_id = cursor.fetchone()
                session = date(2026, 10, 30)
                targets = {
                    "ADJUSTED_DAILY": {
                        "interval_sessions": {"1d": [str(session)]},
                        "expected_counts": {"1d:ADJUSTED": 1},
                    },
                    "NATIVE_INTRADAY": {
                        "interval_sessions": {"5m": [str(session)]},
                        "expected_counts": {"5m:RAW_UNADJUSTED": 78},
                    },
                    "DERIVED_INTERVALS": {
                        "source_sessions": [str(session)],
                        "target_intervals": ["1h"],
                        "expected_counts": {"1h:RAW_UNADJUSTED": 7},
                    },
                }
                for stage, target in targets.items():
                    cursor.execute(
                        "UPDATE rt.realtime_equity_preparation_work SET target=%s "
                        "WHERE revision_id=%s AND ticker='XLK' AND stage=%s",
                        (Json(target), str(desired.revision.revision_id), stage),
                    )
                claim = _claim(
                    ticker="XLK", security_id=security_id, instrument_id=instrument_id,
                    source_cohort_id=desired.revision.source_cohort_id,
                    revision_id=desired.revision.revision_id,
                )
                request = EquityPreparationFetchRequest(claim, WORK_AT, EquityPreparationLimits())
                for interval, adjusted in (("1d", True), ("5m", False)):
                    for index, (start, end) in enumerate(
                        equity_history_coordinates(CALENDAR, interval, session)
                    ):
                        _insert_as_of_fact(
                            cursor,
                            _fact(
                                request, interval=interval, start=start, end=end,
                                adjusted=adjusted, index=index,
                            ),
                            WORK_AT,
                        )
            connection.commit()
        finally:
            pool.putconn(connection)
        provider = _provider(pool)
        source = ReferenceOnlySource()
        plan = plan_equity_bootstrap(
            provider, revision_id=desired.revision.revision_id, checked_at=WORK_AT,
        )
        assert plan["provider_requests"] == plan["database_writes"] == 0
        assert plan["fits_remaining_request_budgets"]
        assert plan["fits_remaining_response_budgets"]
        assert plan["members"]["XLK"]["estimated_stored_chunks"] == 2
        assert plan["members"]["XLK"]["estimated_provider_requests"] == 3

        def bootstrap_resident():
            return AdvancedEquityPreparationResident(
                provider, source, owner_id=uuid4(), clock=lambda: WORK_AT,
                bootstrap_history=VerifiedEquityBootstrapHistory(provider),
                bootstrap_revision_id=desired.revision.revision_id,
            )

        first = bootstrap_resident()
        for expected in ("REFERENCE", "ADJUSTED_DAILY", "NATIVE_INTRADAY"):
            result = first.run_once()
            assert (result.stage, result.status) == (expected, "COMPLETE"), result.reason
        restarted = bootstrap_resident()
        assert restarted.run_once().status == "COMPLETE"
        assert restarted.run_once().status == "COMPLETE"
        normal = AdvancedEquityPreparationResident(
            provider, source, owner_id=uuid4(), clock=lambda: WORK_AT,
        )
        assert normal.run_once().status == "IDLE"
        assert source.calls == [EquityPreparationStage.REFERENCE]
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            unit.cursor.execute(
                "SELECT count(*),sum(provider_requests),sum(response_bytes) "
                "FROM rt.realtime_equity_preparation_chunks WHERE revision_id=%s",
                (str(desired.revision.revision_id),),
            )
            assert unit.cursor.fetchone() == (5, 3, 333)
            unit.cursor.execute(
                "SELECT count(*) FROM rt.realtime_equity_preparation_work "
                "WHERE revision_id=%s AND status='COMPLETE'",
                (str(desired.revision.revision_id),),
            )
            assert unit.cursor.fetchone() == (5,)


def test_bound_history_adoption_and_real_hv_reader_preserve_original_sources():
    with disposable_v2_database(f"test_realtime_v2_h1_{uuid4().hex[:8]}") as pool:
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                target, _manifest = _seed_source(cursor)
                source = insert_cohort(cursor, datetime(2026, 10, 1, tzinfo=UTC))
                cursor.execute(
                    "SELECT manifest.manifest_sha256 FROM rt.cohorts AS cohort "
                    "JOIN rt.subscription_manifests AS manifest USING(manifest_id) "
                    "WHERE cohort.cohort_id=%s",
                    (str(source),),
                )
                source_sha = cursor.fetchone()[0]
                cutoff = CALENDAR.session_open(date(2026, 10, 27))
                cursor.execute(
                    "INSERT INTO rt.cohort_history_bindings ("
                    "target_cohort_id,source_cohort_id,source_provider_tier,"
                    "history_before,source_manifest_sha256,"
                    "authorization_reference,binding_sha256) "
                    "VALUES (%s,%s,'MASSIVE_DEVELOPER',%s,%s,'H1-TEST',%s)",
                    (str(target), str(source), cutoff, source_sha, "b" * 64),
                )
                cursor.execute(
                    "SELECT instrument_id FROM rt.instruments WHERE security_id=%s",
                    (str(_security_id("QQQ")),),
                )
                instrument_id = cursor.fetchone()[0]
                claim = _claim(
                    ticker="QQQ", source_cohort_id=target,
                    security_id=_security_id("QQQ"), instrument_id=instrument_id,
                    stage=EquityPreparationStage.ADJUSTED_DAILY,
                )
                sessions = CALENDAR.trailing_sessions_before(date(2026, 11, 2), 31)
                expected_ids = []
                raw_ids = []
                for session in sessions:
                    opened, closed = equity_history_coordinates(CALENDAR, "1d", session)[0]
                    cursor.execute("SELECT rt.ensure_partitions('rt.bar_revisions',%s)", (session,))
                    request = EquityPreparationFetchRequest(claim, closed, EquityPreparationLimits())
                    fact = _fact(
                        request, interval="1d", start=opened, end=closed,
                        adjusted=True, index=1,
                    )
                    fact = replace(fact, revision=replace(fact.revision, session_date=session))
                    if closed < cutoff:
                        fact = replace(
                            fact, cohort_id=source, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                            revision=replace(fact.revision, quality_codes=()),
                        )
                    persist_realtime_equity_bars_cursor(cursor, (fact,))
                    expected_ids.append(fact.revision.bar_revision_id)
                    if session in sessions[-15:]:
                        raw = _fact(
                            request, interval="1d", start=opened, end=closed,
                            adjusted=False, index=0,
                        )
                        raw = replace(
                            raw, cohort_id=fact.cohort_id, provider_tier=fact.provider_tier,
                            revision=replace(
                                raw.revision, session_date=session,
                                quality_codes=fact.revision.quality_codes,
                            ),
                        )
                        persist_realtime_equity_bars_cursor(cursor, (raw,))
                        raw_ids.append(raw.revision.bar_revision_id)
                period = date(2026, 10, 23)
                start, end = equity_history_coordinates(CALENDAR, "1wk", period)[0]
                weekly = _fact(
                    request, interval="1wk", start=start, end=end,
                    adjusted=True, index=2,
                )
                weekly = replace(
                    weekly, cohort_id=source, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                    revision=replace(weekly.revision, session_date=period, quality_codes=()),
                )
                persist_realtime_equity_bars_cursor(cursor, (weekly,))
                derived = replace(
                    weekly,
                    revision=replace(
                        weekly.revision, bar_revision_id=uuid4(),
                        source_kind=BarSourceKind.DERIVED,
                        source_bar_revision_ids=tuple(expected_ids[:5]),
                    ),
                )
                persist_realtime_equity_bars_cursor(cursor, (derived,))
                head, _head_sha = persist_shadow_cohort_cursor(
                    cursor, manifest_id=_manifest, configuration_sha256="d" * 64,
                    model_bundle_sha256="e" * 64, valuation_sha256="f" * 64,
                    effective_at=CALENDAR.session_open(date(2026, 11, 2)),
                )
                cursor.execute(
                    "SELECT manifest_sha256 FROM rt.subscription_manifests WHERE manifest_id=%s",
                    (str(_manifest),),
                )
                target_sha = cursor.fetchone()[0]
                cursor.execute(
                    "INSERT INTO rt.cohort_history_bindings ("
                    "target_cohort_id,source_cohort_id,source_provider_tier,"
                    "history_before,source_manifest_sha256,"
                    "authorization_reference,binding_sha256) "
                    "VALUES (%s,%s,'MASSIVE_ADVANCED',%s,%s,'BOOTSTRAP-TEST',%s)",
                    (
                        str(head), str(target), CALENDAR.session_open(date(2026, 11, 2)),
                        target_sha, "c" * 64,
                    ),
                )
            connection.commit()
            with connection.cursor() as cursor:
                cursor.execute("SELECT clock_timestamp()")
                checked = cursor.fetchone()[0]
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                history = load_equity_history_cursor(
                    cursor, cohort_id=head, security_id=_security_id("QQQ"),
                    ticker="QQQ", interval="1d", adjusted=True,
                    sessions=sessions, checked_at=checked,
                )
                assert [row["bar_revision_id"] for row in history.rows] == expected_ids
                assert {row["provider_tier"] for row in history.rows} == {
                    "MASSIVE_DEVELOPER", "MASSIVE_ADVANCED",
                }
                retained = load_equity_history_cursor(
                    cursor, cohort_id=target, security_id=_security_id("QQQ"),
                    ticker="QQQ", interval="1wk", adjusted=True,
                    sessions=(period,), checked_at=checked, source_kind="NATIVE_REST",
                )
                assert retained.facts == (weekly,)
                evidence = []
                volatility, _metrics, missing = _load_bars_and_metrics(
                    cursor,
                    SimpleNamespace(
                        control_plane=SimpleNamespace(
                            session_date=date(2026, 11, 2),
                            universe=SimpleNamespace(option_underlyings=("QQQ",)),
                        ),
                        history_source_cohort_id=head,
                        known_at=checked, valid_until=checked + timedelta(hours=8),
                    ),
                    {"QQQ": (500, _security_id("QQQ"))},
                    evidence,
                )
                assert not missing
                assert len(volatility) == 2
                assert tuple(bar.bar_revision_id for bar in volatility[-1].bars) == tuple(expected_ids)
                market_cutoff = CALENDAR.session_open(date(2026, 11, 2)) + timedelta(minutes=5)
                observed = max(checked, market_cutoff + timedelta(seconds=1))
                os_history = _load_bars_cursor(
                    cursor,
                    SimpleNamespace(
                        cohort_id=head, security_id=_security_id("QQQ"), ticker="QQQ",
                        bar_end=market_cutoff, observed_at=observed,
                    ),
                )
                assert [bar.bar_revision_id for bar, _created in os_history] == raw_ids
                inputs = load_realtime_screening_input(
                    cursor, cohort_id=head, session=date(2026, 11, 2),
                    cutoff=observed, source_generation=1, include_portal_history=True,
                    known_by=observed,
                )
                assert len(inputs.history_scopes) == 2
                assert {
                    row["bar_revision_id"] for row in inputs.bars
                    if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"
                } == set(expected_ids)
                # G102 A4: the chart reads the cohort-free head; no history scopes are needed to reach the
                # adopted history.
                chart_rows = load_bar_groups_cursor(
                    cursor, instrument_id=instrument_id, security_id=_security_id("QQQ"),
                    groups={"1d:ADJUSTED": (
                        CALENDAR.session_open(sessions[0]), CALENDAR.session_close(sessions[-1]),
                    )},
                )
                assert [row["bar_revision_id"] for row in chart_rows["1d:ADJUSTED"]] == expected_ids
                chain = [scope.payload() for scope in inputs.history_scopes]
                identity, _continuity = source_manifest_identity_cursor(
                    cursor, {"cohort_id": str(head), "history_scope_chain": chain},
                    manifest_id=_manifest, cohort_manifest_sha256=target_sha,
                    history_binding_sha256="c" * 64, cutoff=observed,
                )
                assert identity == inputs.source_manifest_sha256
            history_bootstrap = VerifiedEquityBootstrapHistory(_provider(pool))
            claim = replace(claim, target={
                "interval_sessions": {"1d": [str(session) for session in sessions]},
                "history_through_session": str(sessions[-1]),
            })
            adopted, missing_sessions = history_bootstrap(claim, checked)
            assert missing_sessions == ()
            assert len(adopted.source_lineage["adopted_history"]["bars"]) == 31
            assert adopted.provider_requests == adopted.response_bytes == 0
            assert adopted.bars == ()
            assert adopted.complete
            assert adopted.coverage_after["available_intervals"] == {"1d:ADJUSTED": 31}
            with connection.cursor() as cursor:
                action_at = CALENDAR.session_close(sessions[-1]) + timedelta(minutes=5)
                cursor.execute(
                    "INSERT INTO ref.corporate_action_revisions ("
                    "action_id,security_id,ticker,action_type,effective_date,"
                    "split_from,split_to,source,source_key,observed_at,received_at,"
                    "payload_sha256,raw_payload) VALUES "
                    "(%s,%s,'QQQ','SPLIT',%s,1,2,'H1-TEST','new-split',%s,%s,%s,'{}')",
                    (
                        str(uuid4()), str(_security_id("QQQ")),
                        date(2026, 10, 29), action_at, action_at, "d" * 64,
                    ),
                )
            connection.commit()
            with connection.cursor() as cursor:
                cursor.execute("SELECT clock_timestamp()")
                after_action = cursor.fetchone()[0]
            stale, missing_sessions = history_bootstrap(claim, after_action)
            assert stale is None
            assert missing_sessions == sessions[:20]
        finally:
            connection.rollback()
            pool.putconn(connection)
