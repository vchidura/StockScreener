from scripts.benchmark_realtime_journal import run_benchmark


def test_synthetic_journal_benchmark_reports_durability_and_all_caps(tmp_path):
    report = run_benchmark(
        tmp_path / "raw.sqlite",
        message_count=20,
        payload_bytes=256,
        batch_size=5,
    )

    assert report["status"] == "MEASURED_SYNTHETIC_LOCAL_ONLY"
    assert report["message_count"] == 20
    assert report["batch_count"] == 4
    assert report["journal"] == {
        "mode": "wal",
        "synchronous": 2,
        "last_offset": 20,
    }
    assert report["physical_journal_bytes"] > report["total_payload_bytes"]
    assert report["durable_batch_latency_ms"]["p95"] > 0
    assert set(report["planning_cap_projections_gb"]) == {"50", "100", "250"}
    assert all(
        row["projected_messages_at_measured_density"] > 20
        for row in report["planning_cap_projections_gb"].values()
    )
    assert "not provider completeness" in report["limitations"]