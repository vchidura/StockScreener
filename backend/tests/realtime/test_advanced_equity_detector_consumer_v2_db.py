from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from realtime.advanced_equity_detector_consumer import (
    AdvancedEquityDetectorConsumerConfig,
    AdvancedEquityDetectorDecision,
    claim_advanced_equity_detector_lease_cursor,
    plan_advanced_equity_detector_slice_cursor,
    persist_advanced_equity_detector_slice_cursor,
)
from realtime.advanced_equity_detector_shadow import (
    detector_shadow_input_sha256,
)
from realtime.detector_adapters import StableDetectorAdapterResult
from realtime.preflight import ProviderTier
from v2_database import (
    disposable_v2_database,
    insert_cohort,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
NOW = datetime(2026, 10, 20, 13, 36, tzinfo=UTC)


def _decision(record, inputs):
    payload = {
        "ticker": record.ticker,
        "disposition": "DETECTED",
    }
    return AdvancedEquityDetectorDecision(
        adapter=StableDetectorAdapterResult(
            adapter_id="advanced-equity-breakout-shadow",
            cohort_id=record.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED,
            input_sha256=detector_shadow_input_sha256(inputs),
            status="EXECUTED",
            output=payload,
        ),
        trigger_identity=f"{record.ticker}:{record.interval}:{record.bar_end.isoformat()}",
        valid_until=None,
        disposition="DETECTED",
        direction=1,
        reason_codes=(),
        payload_schema="advanced_equity_breakout_shadow_signal_v1",
        payload=payload,
        state_key=f"{record.ticker}:ADVANCED_EQUITY_BREAKOUT",
        state_revision=record.source_offset,
        state_payload={"last_offset": record.source_offset},
    )


def test_detector_shadow_persists_evidence_only_and_restarts_from_checkpoint():
    database = f"test_realtime_v2_detector_shadow_{uuid4().hex[:8]}"
    with disposable_v2_database(database) as pool:
        owner_id = uuid4()
        generation = uuid4()
        stream_session_id = uuid4()
        source_revision_id = uuid4()
        derived_revision_id = uuid4()
        security_id = uuid4()
        connection = pool.getconn()
        try:
            with connection.cursor() as cursor:
                cohort_id = insert_cohort(cursor, NOW)
                cursor.execute(
                    "SELECT manifest_id FROM rt.cohorts WHERE cohort_id=%s",
                    (str(cohort_id),),
                )
                manifest_id = UUID(str(cursor.fetchone()[0]))
                cursor.execute(
                    "INSERT INTO ref.securities "
                    "(security_id,asset_type,source_origin,created_at) "
                    "VALUES (%s,'STOCK','detector-shadow-fixture',%s)",
                    (str(security_id), NOW - timedelta(days=1)),
                )
                cursor.execute(
                    "INSERT INTO rt.instruments (asset_class,security_id) "
                    "VALUES ('EQUITY',%s) RETURNING instrument_id",
                    (str(security_id),),
                )
                instrument_id = int(cursor.fetchone()[0])
                cursor.execute(
                    "INSERT INTO rt.stream_sessions ("
                    "session_id,provider,feed,manifest_id,owner_id,owner_epoch,"
                    "started_at,ended_at,status) VALUES ("
                    "%s,'massive','STOCKS',%s,%s,1,%s,%s,'STOPPED')",
                    (
                        str(stream_session_id),
                        str(manifest_id),
                        str(owner_id),
                        NOW - timedelta(minutes=2),
                        NOW,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_catalog ("
                    "bar_revision_id,cohort_id,provider_tier,instrument_id,"
                    "interval,bar_start,bar_end,observed_at,session_date"
                    ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,'1m',%s,%s,%s,%s)",
                    (
                        str(source_revision_id),
                        str(cohort_id),
                        instrument_id,
                        NOW - timedelta(minutes=1),
                        NOW,
                        NOW,
                        NOW.date(),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_revisions ("
                    "bar_revision_id,cohort_id,provider_tier,security_id,"
                    "instrument_id,ticker,interval,session_date,session_scope,"
                    "adjustment_basis,bar_start,bar_end,open_price,high_price,"
                    "low_price,close_price,volume,vwap,transaction_count,"
                    "source_kind,availability_mode,is_final,observed_at,"
                    "journal_generation,stream_session_id,source_offset,"
                    "payload_sha256) VALUES ("
                    "%s,%s,'MASSIVE_ADVANCED',%s,%s,'SPY','1m',%s,'RTH',"
                    "'RAW_UNADJUSTED',%s,%s,600,601,599,600.5,1000,600.4,100,"
                    "'REALTIME_STREAM','LIVE_OBSERVED',true,%s,%s,%s,7,%s)",
                    (
                        str(source_revision_id),
                        str(cohort_id),
                        str(security_id),
                        instrument_id,
                        NOW.date(),
                        NOW - timedelta(minutes=1),
                        NOW,
                        NOW,
                        str(generation),
                        str(stream_session_id),
                        "a" * 64,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_catalog ("
                    "bar_revision_id,cohort_id,provider_tier,instrument_id,"
                    "interval,bar_start,bar_end,observed_at,session_date"
                    ") VALUES (%s,%s,'MASSIVE_ADVANCED',%s,'5m',%s,%s,%s,%s)",
                    (
                        str(derived_revision_id),
                        str(cohort_id),
                        instrument_id,
                        NOW - timedelta(minutes=5),
                        NOW,
                        NOW,
                        NOW.date(),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_revisions ("
                    "bar_revision_id,cohort_id,provider_tier,security_id,"
                    "instrument_id,ticker,interval,session_date,session_scope,"
                    "adjustment_basis,bar_start,bar_end,open_price,high_price,"
                    "low_price,close_price,volume,vwap,transaction_count,"
                    "source_kind,availability_mode,is_final,observed_at,"
                    "ingestion_segment_id,source_revision_ids,payload_sha256"
                    ") VALUES ("
                    "%s,%s,'MASSIVE_ADVANCED',%s,%s,'SPY','5m',%s,'RTH',"
                    "'RAW_UNADJUSTED',%s,%s,600,601,599,600.5,5000,600.4,500,"
                    "'DERIVED','LIVE_OBSERVED',true,%s,%s,%s::uuid[],%s)",
                    (
                        str(derived_revision_id),
                        str(cohort_id),
                        str(security_id),
                        instrument_id,
                        NOW.date(),
                        NOW - timedelta(minutes=5),
                        NOW,
                        NOW,
                        str(uuid4()),
                        [str(source_revision_id)],
                        "e" * 64,
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.bar_current ("
                    "cohort_id,provider_tier,instrument_id,interval,bar_start,"
                    "session_scope,adjustment_basis,source_kind,bar_revision_id,"
                    "bar_end,observed_at) VALUES ("
                    "%s,'MASSIVE_ADVANCED',%s,'5m',%s,'RTH',"
                    "'RAW_UNADJUSTED','DERIVED',%s,%s,%s)",
                    (
                        str(cohort_id),
                        instrument_id,
                        NOW - timedelta(minutes=5),
                        str(derived_revision_id),
                        NOW,
                        NOW,
                    ),
                )
                plan_id = uuid4()
                cursor.execute(
                    "INSERT INTO rt.service_plans ("
                    "plan_id,cohort_id,session_date,stage_catalog_version,"
                    "enabled_stages,config_sha256,deadline,scope_reference"
                    ") VALUES (%s,%s,%s,'stages_v3',%s,%s,%s,%s)",
                    (
                        str(plan_id),
                        str(cohort_id),
                        NOW.date(),
                        [
                            "EQUITY_INPUT_GATE",
                            "EQUITY_MATERIALIZE",
                            "EQUITY_PROJECTION",
                        ],
                        uuid4().hex + uuid4().hex,
                        NOW + timedelta(hours=8),
                        "detector shadow disposable fixture",
                    ),
                )
                window_id = uuid4()
                cursor.execute(
                    "INSERT INTO rt.window_runs ("
                    "window_id,plan_id,cohort_id,provider_tier,window_key,"
                    "release_at,cutoff,deadline,state) VALUES ("
                    "%s,%s,%s,'MASSIVE_ADVANCED','detector-shadow-window',"
                    "%s,%s,%s,'COMPLETE')",
                    (
                        str(window_id),
                        str(plan_id),
                        str(cohort_id),
                        NOW,
                        NOW,
                        NOW + timedelta(minutes=5),
                    ),
                )
                cursor.execute(
                    "INSERT INTO rt.cycle_receipts ("
                    "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                    "reference_generation,universe_sha256,input_sha256,"
                    "build_id) VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s)",
                    (
                        str(uuid4()),
                        str(window_id),
                        str(cohort_id),
                        NOW,
                        "f" * 64,
                        "1" * 64,
                        "advanced-equity-window-v1:" + "2" * 64,
                    ),
                )
                epoch = claim_advanced_equity_detector_lease_cursor(
                    cursor,
                    resource_key="advanced-equity-detector-shadow:0",
                    owner_id=owner_id,
                    checked_at=NOW,
                    lease_seconds=600,
                )
            connection.commit()

            config = AdvancedEquityDetectorConsumerConfig(
                cohort_id=cohort_id,
                journal_generation=generation,
                instrument_id=instrument_id,
                shard_id=0,
                shard_map_sha256="d" * 64,
                owner_id=owner_id,
                owner_epoch=epoch,
                lease_resource_key="advanced-equity-detector-shadow:0",
                detector_id="ADVANCED_EQUITY_BREAKOUT",
                detector_version="advanced-equity-breakout-shadow-v1",
                code_sha256="b" * 64,
                policy_sha256="c" * 64,
                requirements={"interval": "5m"},
                output_kind="SIGNAL",
                readiness_epoch=1,
                interval="5m",
            )
            with connection.cursor() as cursor:
                planned = plan_advanced_equity_detector_slice_cursor(
                    cursor,
                    config,
                    evaluator=_decision,
                    clock=lambda: NOW,
                )
                assert planned.committed_offset == 7
                persist_advanced_equity_detector_slice_cursor(
                    cursor,
                    planned,
                )
            connection.commit()

            with connection.cursor() as cursor:
                for relation, expected in (
                    ("detector_versions", 1),
                    ("detector_events", 1),
                    ("detector_event_inputs", 2),
                    ("detector_state", 1),
                    ("consumer_checkpoints", 1),
                    ("alert_current", 0),
                    ("alert_outbox", 0),
                    ("portal_cohort_current", 0),
                ):
                    cursor.execute(f"SELECT count(*) FROM rt.{relation}")
                    assert cursor.fetchone()[0] == expected
                replay = plan_advanced_equity_detector_slice_cursor(
                    cursor,
                    config,
                    evaluator=_decision,
                    clock=lambda: NOW,
                )
            assert replay.start_offset == replay.committed_offset == 7
            assert replay.records == replay.batches == ()
        finally:
            pool.putconn(connection)
