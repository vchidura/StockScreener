import json

from scripts import plan_realtime_equity_portal_history as script


def test_portal_history_plan_script_is_inert_exact_and_redacted(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "plan_realtime_equity_portal_history.py",
            "--session-date",
            "2026-10-02",
        ],
    )

    assert script.main() == 0

    report = json.loads(capsys.readouterr().out)
    counts = {
        (row["interval"], row["basis"]): row["count"]
        for row in report["expected_counts_per_ticker"]
    }
    assert report["mode"] == "PLAN"
    assert report["native_interval"] == "5m"
    assert report["adjusted_daily_sessions"] == 283
    assert report["adjusted_start"] < report["adjusted_end"] == "2026-10-02"
    assert report["raw_intraday_sessions"] == 90
    assert report["native_one_minute_sessions"] == 5
    assert report["native_adjusted_monthly_periods"] == 60
    assert report["native_adjusted_weekly_periods"] == 261
    assert report["expected_provider_requests"] == 45
    assert counts[("1m", "RAW_UNADJUSTED")] == 1_950
    assert counts[("1mo", "ADJUSTED")] == 60
    assert counts[("5m", "RAW_UNADJUSTED")] > 6_900
    assert counts[("15m", "RAW_UNADJUSTED")] * 3 == counts[("5m", "RAW_UNADJUSTED")]
    assert counts[("30m", "RAW_UNADJUSTED")] * 6 == counts[("5m", "RAW_UNADJUSTED")]
    assert report["expected_total_facts"] > 100_000
    assert report["maximum_rows_per_provider_response"] == 50_000
    assert report["maximum_persist_batch"] == 1_000
    assert report["provider_requests_performed"] == 0
    assert report["database_connections"] == 0
    assert report["database_writes"] == 0
    assert report["values_exposed"] is False
