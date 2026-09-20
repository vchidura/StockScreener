import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from options.analytics.package_evaluation import (
    EvaluationWindow,
    OptionPackageEvaluationManifest,
    evaluation_split,
    first_candidate_cohorts,
    stock_variant_states,
    summarize_evaluation,
)


class _ReportCursor:
    def __init__(self, package_rows):
        self.package_rows = package_rows
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, _parameters=None):
        if "transaction_timestamp()" in query:
            self.rows = [{"cutoff": datetime(2026, 9, 18, 20, tzinfo=timezone.utc)}]
        elif "FROM option_package_assessments" in query:
            self.rows = self.package_rows
        else:
            self.rows = []

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows


class _ReportConnection:
    def __init__(self, package_rows):
        self.closed = False
        self.cursor_instance = _ReportCursor(package_rows)
        self.session_settings = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def rollback(self):
        pass

    def set_session(self, **settings):
        self.session_settings.append(settings)

    def cursor(self, **_kwargs):
        return self.cursor_instance


def _report_with_fake_database(monkeypatch, package_rows, **manifest_updates):
    from scripts import prepare_option_package_evaluation_manifest as prepare
    from scripts import report_option_package_evaluation as report

    frozen = manifest(
        implementation_sha256=prepare._implementation_sha256(),
        **manifest_updates,
    )
    connection = _ReportConnection(package_rows)
    monkeypatch.setattr(report, "_load_manifest", lambda _path: frozen)
    monkeypatch.setattr(report, "get_db_connection", lambda: connection)
    return report, connection


def manifest(**updates):
    values = dict(
        study_id="option_package_evaluation_v1",
        frozen_at="2026-09-18T19:55:00Z",
        underlyers=("AAPL", "SPY"),
        directional_strategies=(
            "DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD",
        ),
        variants=(
            "EXISTING_CANDIDATE_BASELINE",
            "MINIMAL_STOCK_GATES",
            "ALIGNED_RELATIVE_STRENGTH_SIGN",
        ),
        primary_measurement="NEXT_OPEN",
        sensitivity_measurements=("60MIN", "CLOSE"),
        train=EvaluationWindow(
            start_session=date(2026, 9, 18), end_session=date(2026, 10, 2),
        ),
        validation=EvaluationWindow(
            start_session=date(2026, 10, 6), end_session=date(2026, 10, 16),
        ),
        test=EvaluationWindow(
            start_session=date(2026, 10, 20), end_session=date(2026, 10, 30),
        ),
        embargo_sessions=1, maximum_package_rows=50000,
        minimum_outcome_coverage=0.8, minimum_independent_clusters=20,
        package_assessment_policy_sha256="a" * 64,
        stock_behavior_launch_sha256="b" * 64,
        detector_policy_sha256="c" * 64,
        strategy_policy_sha256="d" * 64,
        valuation_policy_sha256="e" * 64,
        outcome_availability_policy_sha256="f" * 64,
        implementation_sha256="0" * 64,
        outcome_basis="INDICATIVE_OPTION_MARKS_NET_COMMISSION",
        artifact_destination="backups/options-package-evaluation/v1/report.json",
    )
    values.update(updates)
    return OptionPackageEvaluationManifest(**values)


def test_manifest_enforces_chronological_splits_and_embargo():
    frozen = manifest()

    assert evaluation_split(date(2026, 9, 18), frozen) == "TRAIN"
    assert evaluation_split(date(2026, 10, 5), frozen) == "EMBARGO"
    assert evaluation_split(date(2026, 10, 6), frozen) == "VALIDATION"
    assert evaluation_split(date(2026, 10, 19), frozen) == "EMBARGO"
    assert evaluation_split(date(2026, 10, 20), frozen) == "TEST"
    with pytest.raises(ValidationError, match="embargo"):
        manifest(validation=EvaluationWindow(
            start_session=date(2026, 10, 5), end_session=date(2026, 10, 16),
        ))


def test_first_candidate_cohort_ignores_future_outcomes():
    rows = [
        dict(entry_session=date(2026, 9, 18), underlying="AAPL",
             strategy_name="DIRECTIONAL_LONG_PREMIUM", structure_type="LONG_CALL",
             candidate_rank=2, candidate_id="later", net_return=100.0),
        dict(entry_session=date(2026, 9, 18), underlying="AAPL",
             strategy_name="DIRECTIONAL_LONG_PREMIUM", structure_type="LONG_CALL",
             candidate_rank=1, candidate_id="first", net_return=-100.0),
    ]

    assert first_candidate_cohorts(rows)[0]["candidate_id"] == "first"
    rows[0]["net_return"], rows[1]["net_return"] = -999.0, 999.0
    assert first_candidate_cohorts(rows)[0]["candidate_id"] == "first"


@pytest.mark.parametrize(("thesis", "value", "aligned"), [
    ("BULLISH", 0.2, True), ("BULLISH", -0.2, False),
    ("BEARISH", -0.2, True), ("BEARISH", 0.2, False),
])
def test_stock_variants_require_minimal_gates_then_aligned_rs_sign(
    thesis, value, aligned,
):
    assessment = {
        "disposition": "ELIGIBLE_RESEARCH", "directional_thesis": thesis,
        "gates": [{
            "gate_id": "RELATIVE_STRENGTH_EVIDENCE", "actual_float": value,
        }],
    }

    states = stock_variant_states(assessment)

    assert states["EXISTING_CANDIDATE_BASELINE"]["included"] is True
    assert states["MINIMAL_STOCK_GATES"]["included"] is True
    assert states["ALIGNED_RELATIVE_STRENGTH_SIGN"]["included"] is aligned


def test_missing_or_blocked_stock_assessment_never_removes_baseline():
    missing = stock_variant_states(None)
    blocked = stock_variant_states({"disposition": "BLOCKED"})

    assert missing["EXISTING_CANDIDATE_BASELINE"]["included"] is True
    assert missing["MINIMAL_STOCK_GATES"]["included"] is False
    assert blocked["MINIMAL_STOCK_GATES"]["reason"] == "STOCK_DISPOSITION_BLOCKED"


def test_summary_groups_dependence_and_remains_inconclusive_when_underpowered():
    frozen = manifest(minimum_outcome_coverage=1.0, minimum_independent_clusters=3)
    baseline = stock_variant_states({
        "disposition": "ELIGIBLE_RESEARCH", "directional_thesis": "BULLISH",
        "gates": [{
            "gate_id": "RELATIVE_STRENGTH_EVIDENCE", "actual_float": -0.1,
        }],
    })
    rows = [
        dict(entry_session=date(2026, 9, 18), underlying="AAPL",
             measurement_type="NEXT_OPEN", net_return=0.1,
             variant_states=baseline),
        dict(entry_session=date(2026, 9, 18), underlying="AAPL",
             measurement_type="NEXT_OPEN", net_return=0.2,
             variant_states=baseline),
    ]

    report = summarize_evaluation(rows, frozen)
    baseline_summary = next(
        row for row in report["summaries"]
        if row["measurement_type"] == "NEXT_OPEN"
        and row["variant"] == "EXISTING_CANDIDATE_BASELINE"
    )
    rs_summary = next(
        row for row in report["summaries"]
        if row["measurement_type"] == "NEXT_OPEN"
        and row["variant"] == "ALIGNED_RELATIVE_STRENGTH_SIGN"
    )
    assert baseline_summary["independent_session_underlying_clusters"] == 1
    assert baseline_summary["cluster_mean_net_return"] == pytest.approx(0.15)
    assert baseline_summary["verdict"] == "INCONCLUSIVE"
    assert rs_summary["cash_abstentions"] == 2
    assert rs_summary["cluster_mean_net_return"] == 0.0
    assert report["probability"] is None
    assert report["execution_permission"] is False


def test_report_digest_serializes_database_dates_deterministically():
    from scripts.report_option_package_evaluation import _digest

    assert _digest({"session": date(2026, 9, 18)}) == _digest(
        {"session": date(2026, 9, 18)}
    )


def test_empty_report_is_read_only_uncalibrated_and_resets_connection(monkeypatch):
    report, connection = _report_with_fake_database(monkeypatch, [])

    result = report.run_report(Path("unused.json"))

    assert result["source_counts"] == {
        "package_rows": 0,
        "cohorts": 0,
        "stock_assessments": 0,
        "measured_outcomes": 0,
        "unavailable_outcomes": 0,
    }
    assert result["transaction_read_only"] is True
    assert result["probability"] is None
    assert result["execution_permission"] is False
    assert result["rows"] == []
    assert connection.session_settings == [
        {"readonly": True, "isolation_level": "REPEATABLE READ"},
        {"readonly": False, "isolation_level": "READ COMMITTED"},
    ]


def test_report_refuses_package_rows_above_frozen_bound(monkeypatch):
    report, connection = _report_with_fake_database(
        monkeypatch, [{}, {}], maximum_package_rows=1,
    )

    with pytest.raises(ValueError, match="package row bound exceeded"):
        report.run_report(Path("unused.json"))

    assert connection.session_settings[-1]["readonly"] is False


def test_train_candidate_does_not_populate_future_splits(monkeypatch):
    package = {
        "candidate_id": UUID("00000000-0000-0000-0000-000000000001"),
        "candidate_identity": "1" * 64,
        "underlying": "AAPL",
        "strategy_name": "DIRECTIONAL_LONG_PREMIUM",
        "structure_type": "LONG_CALL",
        "candidate_rank": 1,
        "market_data_time": datetime(2026, 9, 18, 15, tzinfo=timezone.utc),
        "observed_time": datetime(2026, 9, 18, 15, 1, tzinfo=timezone.utc),
        "package_terms_sha256": "2" * 64,
        "package_assessment_sha256": "3" * 64,
        "package_payload_text": "{}",
        "entry_session": date(2026, 9, 18),
    }
    report, _connection = _report_with_fake_database(monkeypatch, [package])

    result = report.run_report(Path("unused.json"))

    assert result["concentration_before_returns"]["split_packages"] == {"TRAIN": 1}
    for split in ("VALIDATION", "TEST"):
        assert all(
            row["candidate_cohorts"] == 0
            for row in result["split_reports"][split]["summaries"]
        )
        assert all(
            row["verdict"] == "INCONCLUSIVE"
            for row in result["split_reports"][split]["summaries"]
        )


def test_report_refuses_implementation_hash_mismatch(tmp_path):
    from scripts import report_option_package_evaluation as report

    path = tmp_path / "manifest.json"
    path.write_text(manifest(implementation_sha256="1" * 64).model_dump_json())

    with pytest.raises(ValueError, match="implementation hash differs"):
        report.run_report(path)


def test_report_main_refuses_existing_artifact(monkeypatch, tmp_path):
    from scripts import report_option_package_evaluation as report

    frozen = manifest(artifact_destination="backups/options-package-evaluation/existing.json")
    output = tmp_path / frozen.artifact_destination
    output.parent.mkdir(parents=True)
    output.write_text("sentinel", encoding="utf-8")
    result = {
        "study_id": frozen.study_id,
        "manifest_sha256": frozen.sha256,
        "report_sha256": "4" * 64,
        "source_counts": {},
        "concentration_before_returns": {},
        "split_reports": {
            split: {"summaries": []} for split in ("TRAIN", "VALIDATION", "TEST")
        },
    }
    monkeypatch.setattr(
        report, "_arguments",
        lambda: SimpleNamespace(manifest=Path("manifest.json"), no_write=False),
    )
    monkeypatch.setattr(report, "run_report", lambda _path: result)
    monkeypatch.setattr(report, "_load_manifest", lambda _path: frozen)
    monkeypatch.setattr(report, "BACKEND_DIR", tmp_path)

    with pytest.raises(FileExistsError, match="already exists"):
        report.main()

    assert output.read_text(encoding="utf-8") == "sentinel"


def _write_verifier_fixture(monkeypatch, tmp_path, **report_updates):
    from scripts import verify_option_package_evaluation_report as verifier

    frozen = manifest()
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(frozen.model_dump_json(), encoding="utf-8")
    report = {
        "schema_version": "option_package_evaluation_report_v1",
        "study_id": frozen.study_id,
        "manifest": frozen.model_dump(mode="json"),
        "manifest_sha256": frozen.sha256,
        "transaction_read_only": True,
        "calibration_status": "NOT_ATTEMPTED",
        "probability": None,
        "research_only": True,
        "execution_permission": False,
        "source_counts": {"cohorts": 0},
    }
    report.update(report_updates)
    report["report_sha256"] = verifier._sha256_json(report)
    report_path = tmp_path / frozen.artifact_destination
    report_path.parent.mkdir(parents=True)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    monkeypatch.setattr(verifier, "BACKEND_DIR", tmp_path)
    return verifier, manifest_path, report_path


def test_independent_report_verifier_resolves_frozen_hashes(monkeypatch, tmp_path):
    verifier, manifest_path, report_path = _write_verifier_fixture(
        monkeypatch, tmp_path,
    )

    result = verifier.verify_report(manifest_path, report_path)

    assert result["status"] == "VERIFIED"
    assert result["probability"] is None
    assert result["execution_permission"] is False


def test_independent_report_verifier_rejects_content_tampering(monkeypatch, tmp_path):
    verifier, manifest_path, report_path = _write_verifier_fixture(
        monkeypatch, tmp_path,
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["source_counts"]["cohorts"] = 1
    report_path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(ValueError, match="report hash"):
        verifier.verify_report(manifest_path, report_path)


def test_independent_report_verifier_rejects_rehashed_execution_permission(
    monkeypatch, tmp_path,
):
    verifier, manifest_path, report_path = _write_verifier_fixture(
        monkeypatch, tmp_path, execution_permission=True,
    )

    with pytest.raises(ValueError, match="execution_permission"):
        verifier.verify_report(manifest_path, report_path)