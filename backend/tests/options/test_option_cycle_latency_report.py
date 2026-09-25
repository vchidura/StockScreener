from datetime import date, datetime, timedelta, timezone

from scripts.report_option_cycle_latency import _plan_nodes, build_report, parse_cycles, summarize_persisted_phases


LINES = [
    "2026-09-23 10:00:00,000 | INFO | materializing option slot=2026-09-23T16:45:00+00:00",
    "2026-09-23 10:04:00,000 | WARNING | option trade ingestion budget exceeded underlyer=AAPL budget_seconds=120 completed_contracts=10 watchlist=15",
    "2026-09-23 10:06:00,000 | INFO | option slot complete slot=2026-09-23T16:45:00+00:00 statuses=COMPLETE:12,DEGRADED:1 stock_behavior_shadow=RECORDED:13 package_assessments=RECORDED:13",
    "2026-09-23 10:06:00,100 | INFO | option detector evaluation slot=2026-09-23T16:45:00+00:00 result={'status': 'RECORDED'}",
    "2026-09-23 10:08:00,000 | INFO | option outcomes candidates=200 due=1000 available=120 persisted=120 pending=0",
    "2026-09-23 10:08:05,000 | INFO | option continuous validation={'state': 'RUNNING'}",
    "2026-09-23 10:15:00,000 | INFO | materializing option slot=2026-09-23T17:00:00+00:00",
    "2026-09-23 10:35:00,000 | INFO | option slot complete slot=2026-09-23T17:00:00+00:00 statuses=COMPLETE:13 stock_behavior_shadow=RECORDED:13 package_assessments=RECORDED:13",
]


def test_parse_cycles_preserves_materialization_and_post_cycle_tails():
    cycles = parse_cycles(LINES)

    assert len(cycles) == 2
    assert cycles[0]["materialization_seconds"] == 360
    assert cycles[0]["detector_log_tail_seconds"] == 0.1
    assert cycles[0]["outcomes_tail_seconds"] == 120
    assert cycles[0]["validation_tail_seconds"] == 5
    assert cycles[0]["trade_budget_exceeded_underlyers"] == ["AAPL"]
    assert cycles[1]["materialization_seconds"] == 1200


def test_build_report_summarizes_caps_without_claiming_requests_occurred():
    report = build_report(LINES, session_date=date(2026, 9, 23), settings={
        "trade_ingestion_enabled": True,
        "trade_watchlist_per_underlyer": 15,
        "trade_ingestion_budget_seconds": 120,
    }, underlyers=13)

    assert report["complete_cycle_count"] == 2
    assert report["materialization_seconds"] == {
        "minimum": 360.0, "median": 360.0, "p95": 1200.0, "maximum": 1200.0,
    }
    assert report["cycles_exceeding_15_minutes"] == 1
    assert report["status_counts"] == {"COMPLETE": 25, "DEGRADED": 1}
    assert report["trade_ingestion"]["maximum_serial_contract_requests_per_cycle"] == 195
    assert report["trade_ingestion"]["maximum_serial_budget_seconds_per_cycle"] == 1560
    assert report["trade_ingestion"]["observed_budget_exceeded_cycles"] == 1
    assert report["source_writes"] == 0


def test_persisted_phase_summary_uses_sequential_gaps_and_recorded_boundaries():
    start = datetime(2026, 9, 23, 17, 45, tzinfo=timezone.utc)
    rows = [
        dict(scheduled_cycle=start, underlying="AAPL", status="COMPLETE",
            ingestion_started_at=start, ingestion_completed_at=start + timedelta(seconds=10),
            latency_ms=10_000, analysis_started_at=start + timedelta(seconds=40),
            analysis_completed_at=start + timedelta(seconds=45), work_completed_at=start + timedelta(seconds=50),
            page_count=2, received_row_count=500, retained_row_count=200,
            chain_response_bytes=1000),
        dict(scheduled_cycle=start, underlying="MSFT", status="COMPLETE",
            ingestion_started_at=start + timedelta(seconds=60), ingestion_completed_at=start + timedelta(seconds=80),
            latency_ms=20_000, analysis_started_at=start + timedelta(seconds=120),
            analysis_completed_at=start + timedelta(seconds=126), work_completed_at=start + timedelta(seconds=135),
            page_count=3, received_row_count=600, retained_row_count=250,
            chain_response_bytes=2000),
    ]

    result = summarize_persisted_phases(rows)

    assert result["cycles"] == [dict(slot=start.isoformat(), underlyers=2,
        persisted_underlyer_span_seconds=135.0, received_contracts=1100,
        retained_contracts=450, chain_pages=5, chain_response_bytes=3000)]
    assert result["phase_seconds_per_underlyer"]["pre_chain_seconds"]["samples"] == 1
    assert result["phase_seconds_per_underlyer"]["pre_chain_seconds"]["median"] == 10.0
    assert result["phase_seconds_per_underlyer"]["post_chain_to_analysis_seconds"]["median"] == 30.0
    assert result["slowest_underlyers"][0]["underlying"] == "MSFT"


def test_plan_nodes_retains_scan_and_index_identity_only():
    plan = [{"Plan": {"Node Type": "Sort", "Plan Rows": 10, "Total Cost": 4.5,
        "Plans": [{"Node Type": "Index Scan", "Relation Name": "events",
            "Index Name": "events_underlying_time", "Plan Rows": 10, "Total Cost": 3.2,
            "Filter": "secret detail"}]}}]

    assert _plan_nodes(plan) == [
        {"Node Type": "Sort", "Plan Rows": 10, "Total Cost": 4.5},
        {"Node Type": "Index Scan", "Relation Name": "events",
            "Index Name": "events_underlying_time", "Plan Rows": 10, "Total Cost": 3.2},
    ]
