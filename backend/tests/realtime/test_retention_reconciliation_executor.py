import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from realtime.retention_reconciliation_executor import (
    EXPECTED_PACKAGE_SHA256,
    STATUS_AMBIGUOUS,
    STATUS_CONSUMED,
    STATUS_LOCKED,
    STATUS_POSTFLIGHT_PASS,
    STATUS_READY,
    DatabaseObservation,
    ReconciliationExecutionError,
    ReconciliationExecutor,
    _assert_database_ready,
    load_exact_package,
)
from scripts.audit_realtime_v2_retention_archive import (
    build_retention_compatibility_plan,
)
from scripts.run_realtime_v2_retention_reconciliation import (
    main as reconciliation_main,
)


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[3]
PACKAGE_PATH = (
    ROOT
    / "backend/.realtime-canary/"
    "options-owp5-reconciliation-authorization-20261022-01.json"
)
POSTFLIGHT_PATH = (
    ROOT
    / "backend/.realtime-canary/"
    "options-owp5-reconciliation-postflight-contract-20261022-01.json"
)
REPORT_PATH = (
    ROOT
    / "backend/.realtime-canary/"
    "retention-archive-reconciliation-20261019-v3.json"
)
REPORT_SHA256 = (
    "387d7a09c3073a294133162449876bda49a193c6e83f7699857bb1d135097cad"
)
PLAN_SHA256 = (
    "7605568c00244513ff25ed7a360e2d6e1031088748298b6225a34fc9fb75ec20"
)
ACTIVE = datetime(2026, 10, 22, 16, 35, tzinfo=UTC)


def _package_and_plan():
    package = load_exact_package(PACKAGE_PATH, POSTFLIGHT_PATH)
    plan = build_retention_compatibility_plan(
        REPORT_PATH,
        expected_report_sha256=REPORT_SHA256,
        expected_build_id="retention-production-v1",
        expected_policy_id="retention-policy-v1",
    )
    assert package["package_sha256"] == EXPECTED_PACKAGE_SHA256
    assert plan["frozen_plan_sha256"] == PLAN_SHA256
    return package, plan


def _legacy_empty_gzip(expected_sha256):
    value = bytearray(gzip.compress(b"", mtime=0))
    if hashlib.sha256(value).hexdigest() == expected_sha256:
        return bytes(value)
    for index in range(len(value)):
        original = value[index]
        for candidate in range(256):
            value[index] = candidate
            if hashlib.sha256(value).hexdigest() == expected_sha256:
                return bytes(value)
        value[index] = original
    raise AssertionError("legacy empty gzip bytes are not one byte from current output")


def _seed_original_files(package, archive_root):
    mtimes = {}
    for revision in package["authorization_scope"]["revisions"]:
        preserved = revision["preserved"]
        data_path = archive_root / preserved["data_file"]
        manifest_path = archive_root / preserved["manifest_file"]
        data_path.parent.mkdir(parents=True, exist_ok=True)
        data_path.write_bytes(
            _legacy_empty_gzip(preserved["data_sha256"])
        )
        manifest_path.write_text(
            json.dumps({
                "archive_kind": revision["kind"],
                "build_id": revision["append_only_revision"]["build_id"],
                "clocks": {
                    "market": None,
                    "observed": None,
                    "durable": None,
                    "replay": None,
                },
                "content_sha256": revision[
                    "append_only_revision"
                ]["content_sha256"],
                "file": data_path.name,
                "horizon_close_date": None,
                "policy_id": revision[
                    "append_only_revision"
                ]["policy_id"],
                "relation_counts": {
                    revision["relations"][0]: 0,
                },
                "row_count": 0,
                "schema_version": revision[
                    "append_only_revision"
                ]["archive_schema_version"],
                "session_date": package[
                    "authorization_scope"
                ]["scope"]["session_date"],
                "sha256": preserved["data_sha256"],
                "uncompressed_bytes": 0,
            }, indent=2, sort_keys=True) + "\n",
            encoding="ascii",
        )
        mtimes[str(data_path)] = data_path.stat().st_mtime_ns
        mtimes[str(manifest_path)] = manifest_path.stat().st_mtime_ns
    return mtimes


def _services():
    return {
        "retention": "STOPPED",
        "archive": "STOPPED",
        "starts": 0,
        "restarts": 0,
    }


class FakeRepository:
    def __init__(self, package):
        scope = package["authorization_scope"]["scope"]
        self.source_counts = {
            relation: 0 for relation in scope["read_relations"]
        }
        self.partitions = tuple(scope["read_partitions"])
        self.rows = []
        self.append_calls = 0
        self.lock_conflict = False

    def observe(self, _package, _ledger_rows):
        return DatabaseObservation(
            dict(self.source_counts),
            self.partitions,
            tuple(dict(row) for row in self.rows),
        )

    def append(self, package, ledger_rows, *, promote, fault):
        self.append_calls += 1
        if self.lock_conflict:
            raise ReconciliationExecutionError(
                "DATABASE_LOCK_CONFLICT",
                "fake",
            )
        observation = self.observe(package, ledger_rows)
        _assert_database_ready(package, observation)
        pending = [{
            "archive_id": str(row.archive_id),
            "session_date": row.session_date,
            "kind": row.kind,
            "uri": row.uri,
            "sha256": row.sha256,
            "row_count": row.row_count,
            "schema_version": row.schema_version,
            "build_id": row.build_id,
            "verified_at": row.verified_at,
        } for row in ledger_rows]
        fault("AFTER_DB_INSERTS")
        promote()
        fault("BEFORE_COMMIT")
        self.rows = pending


def _executor(tmp_path):
    package, plan = _package_and_plan()
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    original_mtimes = _seed_original_files(package, archive_root)
    repository = FakeRepository(package)
    lock_path = tmp_path / "execution.lock"
    executor = ReconciliationExecutor(
        repository,
        repository_root=ROOT,
        archive_root=archive_root,
        report_path=REPORT_PATH,
        lock_path=lock_path,
        service_probe=_services,
    )
    return (
        package,
        plan,
        repository,
        executor,
        archive_root,
        lock_path,
        original_mtimes,
    )


def test_plan_preflight_and_status_are_zero_write(tmp_path):
    (
        package,
        plan,
        repository,
        executor,
        archive_root,
        lock_path,
        original_mtimes,
    ) = _executor(tmp_path)

    plan_result = executor.plan(package)
    status = executor.status(package, checked_at=ACTIVE)
    preflight = executor.preflight(
        package,
        plan,
        checked_at=ACTIVE - timedelta(days=1),
    )

    assert plan_result["mode"] == "PLAN"
    assert status.state == STATUS_READY
    assert preflight["status"] == (
        "PASS_RECONCILIATION_EXECUTION_PREFLIGHT"
    )
    assert repository.append_calls == 0
    assert not lock_path.exists()
    assert {
        str(path): path.stat().st_mtime_ns
        for path in archive_root.rglob("*")
        if path.is_file()
    } == original_mtimes


def test_cli_plan_is_provider_free_and_exact_package_only(tmp_path, capsys):
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    service_state = tmp_path / "services.json"
    service_state.write_text(json.dumps(_services()), encoding="utf-8")

    assert reconciliation_main([
        "--package", str(PACKAGE_PATH),
        "--postflight-contract", str(POSTFLIGHT_PATH),
        "--report", str(REPORT_PATH),
        "--archive-root", str(archive_root),
        "--lock-path", str(tmp_path / "execution.lock"),
        "--service-state-file", str(service_state),
        "--plan",
    ]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "PLAN"
    assert payload["database_writes"] == 0
    assert payload["archive_writes"] == 0
    assert not payload["retention_started"]
    assert not (tmp_path / "execution.lock").exists()

    changed = tmp_path / "changed-package.json"
    document = json.loads(PACKAGE_PATH.read_text(encoding="utf-8"))
    document["authority"]["archive_delete_authorized"] = True
    changed.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="package file identity changed"):
        reconciliation_main([
            "--package", str(changed),
            "--postflight-contract", str(POSTFLIGHT_PATH),
            "--report", str(REPORT_PATH),
            "--archive-root", str(archive_root),
            "--lock-path", str(tmp_path / "execution.lock"),
            "--service-state-file", str(service_state),
            "--plan",
        ])


def test_exact_execution_commits_once_runs_postflight_and_refuses_replay(
    tmp_path,
):
    (
        package,
        plan,
        repository,
        executor,
        archive_root,
        lock_path,
        original_mtimes,
    ) = _executor(tmp_path)

    result = executor.execute(package, plan, checked_at=ACTIVE)

    assert result["status"] == STATUS_POSTFLIGHT_PASS
    assert result["postflight"]["status"] == STATUS_POSTFLIGHT_PASS
    assert not result["postflight"]["retention_resume_authorized"]
    assert len(result["created_files"]) == 4
    assert len(repository.rows) == 2
    assert not lock_path.exists()
    assert all(
        (archive_root / relative).is_file()
        for relative in result["created_files"]
    )
    for path, mtime in original_mtimes.items():
        assert Path(path).stat().st_mtime_ns == mtime
    assert executor.status(package, checked_at=ACTIVE).state == STATUS_CONSUMED
    with pytest.raises(
        ReconciliationExecutionError,
        match="EXECUTION_NOT_READY",
    ):
        executor.execute(package, plan, checked_at=ACTIVE)


@pytest.mark.parametrize(
    "stage, expected_state, lock_remains, committed",
    (
        ("AFTER_LOCK", STATUS_READY, False, False),
        ("AFTER_STAGE", STATUS_READY, False, False),
        ("AFTER_DB_INSERTS", STATUS_READY, False, False),
        ("AFTER_FIRST_PROMOTE", STATUS_AMBIGUOUS, True, False),
        ("BEFORE_COMMIT", STATUS_AMBIGUOUS, True, False),
        ("AFTER_COMMIT", STATUS_CONSUMED, True, True),
        ("BEFORE_POSTFLIGHT", STATUS_CONSUMED, True, True),
    ),
)
def test_crash_boundaries_are_failure_shaped_without_replay(
    tmp_path,
    stage,
    expected_state,
    lock_remains,
    committed,
):
    (
        package,
        plan,
        repository,
        executor,
        _archive_root,
        lock_path,
        _original_mtimes,
    ) = _executor(tmp_path)

    def fault(current):
        if current == stage:
            raise RuntimeError(f"crash at {stage}")

    with pytest.raises(
        ReconciliationExecutionError,
        match="EXECUTION_INTERRUPTED",
    ):
        executor.execute(
            package,
            plan,
            checked_at=ACTIVE,
            fault=fault,
        )

    status = executor.status(package, checked_at=ACTIVE)
    assert status.state == expected_state
    assert lock_path.exists() is lock_remains
    assert bool(repository.rows) is committed
    if expected_state != STATUS_READY:
        with pytest.raises(ReconciliationExecutionError):
            executor.execute(package, plan, checked_at=ACTIVE)


def test_lock_conflict_source_drift_and_early_late_windows_fail_closed(
    tmp_path,
):
    (
        package,
        plan,
        repository,
        executor,
        _archive_root,
        lock_path,
        _original_mtimes,
    ) = _executor(tmp_path)
    lock_path.write_text("occupied\n", encoding="ascii")
    assert executor.status(package, checked_at=ACTIVE).state == STATUS_LOCKED
    with pytest.raises(ReconciliationExecutionError, match="EXECUTION_NOT_READY"):
        executor.execute(package, plan, checked_at=ACTIVE)
    lock_path.unlink()

    repository.source_counts["rt.option_flow_events"] = 1
    with pytest.raises(ReconciliationExecutionError, match="SOURCE_COUNT_DRIFT"):
        executor.preflight(package, plan, checked_at=ACTIVE)
    repository.source_counts["rt.option_flow_events"] = 0

    with pytest.raises(
        ReconciliationExecutionError,
        match="EARLY_EXECUTION_WINDOW",
    ):
        executor.execute(
            package,
            plan,
            checked_at=datetime(2026, 10, 22, 16, 29, tzinfo=UTC),
        )
    with pytest.raises(
        ReconciliationExecutionError,
        match="MISSED_EXECUTION_WINDOW",
    ):
        executor.execute(
            package,
            plan,
            checked_at=datetime(2026, 10, 22, 16, 40, tzinfo=UTC),
        )


def test_database_lock_conflict_rolls_back_staging_and_releases_host_lock(
    tmp_path,
):
    (
        package,
        plan,
        repository,
        executor,
        archive_root,
        lock_path,
        _original_mtimes,
    ) = _executor(tmp_path)
    repository.lock_conflict = True

    with pytest.raises(
        ReconciliationExecutionError,
        match="DATABASE_LOCK_CONFLICT",
    ):
        executor.execute(package, plan, checked_at=ACTIVE)

    assert executor.status(package, checked_at=ACTIVE).state == STATUS_READY
    assert not lock_path.exists()
    assert not tuple(archive_root.rglob(f"*.{package['execution_nonce']}.partial"))
