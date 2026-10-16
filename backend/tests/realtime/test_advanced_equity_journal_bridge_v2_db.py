"""Disposable-DB atomicity coverage for the Advanced equity journal bridge."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from realtime.advanced_equity_journal_bridge import (
    AdvancedEquityJournalBridgeConfig,
    process_advanced_equity_journal_once,
)
from realtime.contracts import AssetClass, MarketChannel
from realtime.database_access import RealtimeDatabaseRole
from realtime.database_access_v2 import (
    OWNER_POOL,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.journal import DecoderProgress, NormalizedJournalRecord
from realtime.equity_interval_finalizer import (
    EQUITY_INTERVAL_FINALIZE_JOB,
    EquityWorkerConfig,
    enqueue_five_minute_finalizer_jobs_cursor,
    run_five_minute_finalizer_once,
)
from realtime.stage_database_v2 import V2WorkerDatabase
from realtime.stage_runners_equity import input_ready_identity
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database
DATABASE = "test_realtime_v2_advanced_bridge"
UTC = timezone.utc
NOW = datetime(2026, 10, 14, 14, 30, tzinfo=UTC)
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SESSION = UUID("33333333-3333-3333-3333-333333333333")


def _digest():
    return uuid4().hex + uuid4().hex


@pytest.fixture
def database():
    with disposable_v2_database(DATABASE) as pool:
        provider = V2ConnectionProvider(
            {OWNER_POOL: pool},
            V2AccessConfig(allow_disposable=True),
        )
        provider.verify()
        yield V2WorkerDatabase(provider, RealtimeDatabaseRole.COMPUTE)


def _seed(cursor):
    profile, manifest, cohort = uuid4(), uuid4(), uuid4()
    security, revision, owner = uuid4(), uuid4(), uuid4()
    born = NOW - timedelta(days=30)
    cursor.execute(
        "INSERT INTO rt.capability_profiles ("
        "profile_id, config_sha256, schema_sha256, account_alias, "
        "capabilities_sha256, universe_sha256, checked_at, expires_at, status, "
        "created_at, profile_sha256, provider_tier, security_policy_sha256, preflight_mode, "
        "provider_requests, action_authorized) VALUES ("
        "%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,%s,'MASSIVE_ADVANCED',%s,"
        "'PASSIVE',false,false)",
        (
            str(profile),
            _digest(),
            _digest(),
            _digest(),
            _digest(),
            born,
            NOW + timedelta(days=30),
            born,
            _digest(),
            _digest(),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.subscription_manifests ("
        "manifest_id, manifest_sha256, universe_sha256, session_date, "
        "effective_at, profile_id, manifest_kind, stock_count, "
        "option_underlying_count, member_count, allocation_policy_sha256, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,'MARKET_OPTIONS',1,1,1,%s,%s)",
        (
            str(manifest),
            _digest(),
            _digest(),
            NOW.date(),
            born,
            str(profile),
            _digest(),
            born,
        ),
    )
    cursor.execute(
        "INSERT INTO rt.cohorts ("
        "cohort_id, cohort_sha256, manifest_id, configuration_sha256, "
        "model_bundle_sha256, valuation_sha256, effective_at, mode, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,'SHADOW',%s)",
        (
            str(cohort),
            _digest(),
            str(manifest),
            _digest(),
            _digest(),
            _digest(),
            born,
            born,
        ),
    )
    cursor.execute(
        "INSERT INTO ref.securities "
        "(security_id, asset_type, source_origin, created_at) "
        "VALUES (%s,'STOCK','fixture',%s)",
        (str(security), born),
    )
    cursor.execute(
        "INSERT INTO ref.security_revisions ("
        "revision_id, security_id, ticker, exchange, calendar_id, "
        "effective_from, observed_at, payload_sha256, company_name, "
        "security_type, active) VALUES ("
        "%s,%s,'SPY','ARCX','XNYS',%s,%s,%s,'SPY','ETF',true)",
        (str(revision), str(security), born, born, _digest()),
    )
    cursor.execute(
        "INSERT INTO rt.instruments (asset_class, security_id) "
        "VALUES ('EQUITY',%s) RETURNING instrument_id",
        (str(security),),
    )
    instrument = int(cursor.fetchone()[0])
    cursor.execute(
        "INSERT INTO rt.subscription_members ("
        "manifest_id, instrument_id, channel, priority, purpose, option_underlying) "
        "VALUES (%s,%s,'AM',1,'PRIMARY',true)",
        (str(manifest), instrument),
    )
    cursor.execute(
        "INSERT INTO rt.stream_sessions ("
        "session_id, provider, feed, manifest_id, owner_id, owner_epoch, "
        "started_at, subscribed_at, status) "
        "VALUES (%s,'massive','STOCKS',%s,%s,7,%s,%s,'LIVE')",
        (
            str(SESSION),
            str(manifest),
            str(owner),
            NOW - timedelta(minutes=5),
            NOW - timedelta(minutes=4),
        ),
    )
    return cohort, security, instrument


def _record(offset, instrument):
    end = NOW + timedelta(minutes=offset)
    start = end - timedelta(minutes=1)
    return NormalizedJournalRecord(
        event_offset=offset,
        raw_offset=offset,
        event_index=0,
        normalizer_version="massive_market_event_v1",
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        instrument_id=instrument,
        provider_symbol="SPY",
        underlying="SPY",
        asset_class=AssetClass.EQUITY,
        shard_id=0,
        event_kind=MarketChannel.MINUTE_AGGREGATE,
        source_session=date(2026, 10, 14),
        source_sequence=offset,
        source_timestamp=int(end.timestamp() * 1000),
        source_timestamp_unit="ms",
        event_at=end,
        price=None,
        size=Decimal("1000"),
        bid=None,
        ask=None,
        bid_size=None,
        ask_size=None,
        exchange=None,
        conditions=(),
        quality="GOOD",
        reason_codes=(),
        details_json=(
            '{"close":"101","end_timestamp":'
            + str(int(end.timestamp() * 1000))
            + ',"high":"102","low":"99","open":"100","start_timestamp":'
            + str(int(start.timestamp() * 1000))
            + ',"vwap":"100.5"}'
        ),
        payload_sha256=f"{offset:064x}",
        received_at=end + timedelta(milliseconds=1),
        committed_at=end + timedelta(milliseconds=2),
    )


def _reader(record):
    source = MagicMock()
    source.decoder_progress.return_value = DecoderProgress(
        generation=GENERATION,
        normalizer_version="massive_market_event_v1",
        committed_raw_offset=record.raw_offset,
        committed_event_offset=record.event_offset,
        updated_at=record.committed_at + timedelta(milliseconds=1),
    )
    source.read_normalized_after.return_value = (record,)
    return source


def _reader_many(records):
    source = MagicMock()
    last = records[-1]
    source.decoder_progress.return_value = DecoderProgress(
        generation=GENERATION,
        normalizer_version="massive_market_event_v1",
        committed_raw_offset=last.raw_offset,
        committed_event_offset=last.event_offset,
        updated_at=last.committed_at + timedelta(milliseconds=1),
    )
    source.read_normalized_after.return_value = tuple(records)
    return source


def test_bars_and_all_shard_checkpoints_commit_and_a_gap_rolls_back_next_batch(database):
    with database.transaction() as cursor:
        cohort, security, instrument = _seed(cursor)
    config = AdvancedEquityJournalBridgeConfig(
        cohort_id=cohort,
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        owner_epoch=7,
        normalizer_version="massive_market_event_v1",
        shard_ids=(0, 1),
        batch_limit=10,
    )
    first = _record(1, instrument)

    status = process_advanced_equity_journal_once(
        database,
        _reader(first),
        config,
        checked_at=first.committed_at + timedelta(seconds=1),
    )

    assert (status.persisted_bars, status.shards_advanced) == (1, 2)
    with database.transaction(read_only=True) as cursor:
        cursor.execute(
            "SELECT instrument_id, interval, source_kind, source_offset "
            "FROM rt.bar_revisions WHERE cohort_id=%s",
            (str(cohort),),
        )
        assert cursor.fetchall() == [(instrument, "1m", "REALTIME_STREAM", 1)]
        cursor.execute(
            "SELECT shard_id, committed_offset, owner_epoch "
            "FROM rt.consumer_checkpoints WHERE cohort_id=%s "
            "ORDER BY shard_id",
            (str(cohort),),
        )
        assert cursor.fetchall() == [(0, 1, 7), (1, 1, 7)]

    with database.transaction() as cursor:
        cursor.execute(
            "INSERT INTO rt.stream_gaps ("
            "gap_id, session_id, instrument_id, channel, gap_start, "
            "journal_generation, shard_id, last_durable_offset, reason, "
            "recovery_status) VALUES (%s,%s,%s,'AM',%s,%s,0,1,"
            "'SOURCE_OFFSET_HOLE','UNRESOLVED')",
            (
                str(uuid4()),
                str(SESSION),
                instrument,
                first.event_at,
                str(GENERATION),
            ),
        )
    second = _record(2, instrument)
    with pytest.raises(ValueError, match="unresolved Advanced equity AM gap"):
        process_advanced_equity_journal_once(
            database,
            _reader(second),
            config,
            checked_at=second.committed_at + timedelta(seconds=1),
        )

    with database.transaction(read_only=True) as cursor:
        cursor.execute(
            "SELECT count(*) FROM rt.bar_revisions WHERE cohort_id=%s",
            (str(cohort),),
        )
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            "SELECT shard_id, committed_offset FROM rt.consumer_checkpoints "
            "WHERE cohort_id=%s ORDER BY shard_id",
            (str(cohort),),
        )
        assert cursor.fetchall() == [(0, 1), (1, 1)]


def test_advanced_bridge_finalizer_and_input_ready_complete_one_exact_window(database):
    with database.transaction() as cursor:
        cohort, _security, instrument = _seed(cursor)
        cursor.execute(
            "SELECT cohort.manifest_id, manifest.universe_sha256 "
            "FROM rt.cohorts AS cohort JOIN rt.subscription_manifests AS manifest USING (manifest_id) "
            "WHERE cohort.cohort_id=%s",
            (str(cohort),),
        )
        _manifest_id, universe_sha256 = cursor.fetchone()
        plan_id, window_id = uuid4(), uuid4()
        cutoff = NOW + timedelta(minutes=5)
        release_at = datetime.now(UTC)
        deadline = release_at + timedelta(minutes=5)
        cursor.execute(
            "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, "
            "enabled_stages, config_sha256, deadline, scope_reference) "
            "VALUES (%s,%s,%s,'stages_v2',%s,%s,%s,'advanced bridge integration')",
            (
                str(plan_id),
                str(cohort),
                cutoff.date(),
                ["EQUITY_MATERIALIZE"],
                _digest(),
                deadline + timedelta(hours=1),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, "
            "release_at, cutoff, deadline) VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s)",
            (
                str(window_id),
                str(plan_id),
                str(cohort),
                f"advanced:{window_id}",
                release_at,
                cutoff,
                deadline,
            ),
        )
        cursor.execute(
            "INSERT INTO rt.seed_manifests (seed_id, seed_sha256, universe_sha256, session_date, status, "
            "member_count, created_at, effective_at, valid_until, source_generations) "
            "VALUES (%s,%s,%s,%s,'READY',1,%s,%s,%s,'{}'::jsonb)",
            (
                str(uuid4()),
                _digest(),
                universe_sha256,
                cutoff.date(),
                NOW - timedelta(days=1),
                NOW - timedelta(days=1),
                deadline + timedelta(days=1),
            ),
        )

    records = tuple(_record(offset, instrument) for offset in range(1, 6))
    config = AdvancedEquityJournalBridgeConfig(
        cohort_id=cohort,
        journal_generation=GENERATION,
        stream_session_id=SESSION,
        owner_epoch=7,
        normalizer_version="massive_market_event_v1",
        shard_ids=(0,),
        batch_limit=10,
    )
    bridge_status = process_advanced_equity_journal_once(
        database,
        _reader_many(records),
        config,
        checked_at=records[-1].committed_at + timedelta(seconds=1),
    )
    assert (bridge_status.persisted_bars, bridge_status.committed_offset) == (5, 5)

    barrier_id = uuid4()
    with database.transaction() as cursor:
        enqueue_five_minute_finalizer_jobs_cursor(
            cursor,
            cohort_id=cohort,
            window_id=window_id,
            instrument_ids=(instrument,),
            checked_at=records[-1].committed_at + timedelta(seconds=1),
            policy_sha256="a" * 64,
            build_id="advanced-integration",
            barrier_id=barrier_id,
        )
    finalizer_status = run_five_minute_finalizer_once(
        database,
        EquityWorkerConfig(
            cohort_id=cohort,
            job_kind=EQUITY_INTERVAL_FINALIZE_JOB,
            lease_owner=uuid4(),
            claim_limit=1,
        ),
        checked_at=records[-1].committed_at + timedelta(seconds=1),
    )
    assert (finalizer_status.completed, finalizer_status.output_count) == (1, 1)

    universe, input_sha256 = input_ready_identity(
        database,
        cohort_id=cohort,
        cutoff=cutoff,
        known_by=deadline,
        reference_generation=1,
    )
    assert universe == universe_sha256
    assert len(input_sha256) == 64
