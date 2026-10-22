from __future__ import annotations

import json
import hashlib
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

import realtime.advanced_options_p8_canary_executor as executor_module
from realtime.advanced_options_control_plane import (
    build_advanced_options_history_binding,
)
from realtime.advanced_options_p8_canary_authorization import (
    build_p8_canary_authorization,
    canonical_sha256,
)
from realtime.advanced_options_p8_canary_executor import (
    P8CanaryExecutionBinding,
    P8CanaryExecutionError,
    P8CanaryExecutor,
)
from realtime.advanced_options_service import (
    OPTIONS_NON_SERVING_STAGE_ORDER,
    AdvancedOptionsProcessKind,
)
from realtime.advanced_options_underlying_snapshot import (
    UnderlyingSnapshotBinding,
    UnderlyingSnapshotPrice,
    underlying_snapshot_binding_payload,
)
from realtime.contracts import MarketChannel, UniverseManifest
from realtime.option_slot_set import (
    SlotSetKind,
    SlotSetMember,
    SlotSetPurpose,
    SlotSetVersion,
)
from tests.realtime.test_advanced_options_control_plane import (
    _enrollment_plan,
)
from tests.realtime.test_advanced_options_p8_canary_authorization import (
    _observation,
    _page_release_observation,
    _selection,
)


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[3]
SOURCE_FILES = (
    "backend/realtime/advanced_options_p8_canary_authorization.py",
)
COMMIT = "d38c4480f954b4f02270541913adfce666bae99d"
PREPARED = datetime(2026, 10, 16, 11, 0, tzinfo=UTC)
START = datetime(2026, 10, 16, 13, 30, tzinfo=UTC)
STOP = START + timedelta(minutes=10)
CHECKED = START + timedelta(seconds=30)
REPORT_ENV = "STOCKS_P8_EXECUTOR_RETAIN_REPORT"
REPORT_ROOT = Path(__file__).parents[2] / ".realtime-canary"
PIPELINE_QUALIFICATION_SEMANTIC_SHA256 = (
    "898f9ad2a9c2ff5d4d0f0089fbd488113fed51d9b4ac32a532e30f2bb63d4c2f"
)
PIPELINE_QUALIFICATION_FILE_SHA256 = (
    "9511b8033749b1d948969daa688f3a0b9502b2154485448f40303e9d34cee798"
)


def _qualification(tmp_path):
    semantic = {
        "contract": "p8-executor-test-qualification-v1",
        "verdict": "PASS",
        "provider": {"calls": 0},
    }
    digest = canonical_sha256(semantic)
    path = tmp_path / "qualification.json"
    path.write_text(
        json.dumps(
            {**semantic, "report_sha256": digest},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path, digest, hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def _package(tmp_path, *, stored_reads_prepared=False):
    report, semantic, file_sha = _qualification(tmp_path)
    package = build_p8_canary_authorization(
        report,
        repository_root=ROOT,
        source_commit=COMMIT,
        authorization_reference=f"P8-EXECUTOR-{uuid4()}",
        package_nonce=uuid4(),
        prepared_at=PREPARED,
        window_start=START,
        window_end=STOP,
        expected_qualification_semantic_sha256=semantic,
        expected_qualification_file_sha256=file_sha,
        source_files=SOURCE_FILES,
        stored_reads_prepared=stored_reads_prepared,
    )
    return package, report, semantic, file_sha


def _binding(package):
    base = _enrollment_plan()
    spy = base.activity_selection.contracts[0]
    qqq = replace(
        spy,
        instrument_id=202,
        contract_id=2_002,
        catalog_version_id=2_202,
        contract_ticker="O:QQQ261120P00400000",
        underlyer="QQQ",
        call_put="P",
        strike=spy.strike - 100,
        catalog_payload_sha256="8" * 64,
        source_sha256="9" * 64,
    )
    selection = replace(
        base.activity_selection,
        contracts=(qqq, spy),
        source_clocks_sha256="",
        selection_sha256="",
    )
    cohort_id = UUID(str(package["cohort"]["cohort_id"]))
    members = tuple(
        SlotSetMember(
            instrument_id=row.instrument_id,
            channel=channel,
            purpose=SlotSetPurpose.PRIMARY,
            priority=100,
            catalog_version_id=row.catalog_version_id,
        )
        for row in selection.contracts
        for channel in (MarketChannel.TRADE, MarketChannel.QUOTE)
    )
    slot_set = SlotSetVersion(
        cohort_id=cohort_id,
        version=1,
        kind=SlotSetKind.PRE_SESSION,
        effective_at=base.control_plane.slot_set.effective_at,
        policy_sha256=base.control_plane.slot_set.policy_sha256,
        members=members,
    )
    control = replace(
        base.control_plane,
        plan_id=UUID(str(package["orchestration"]["plan_id"])),
        cohort_id=cohort_id,
        universe=UniverseManifest(
            "p8-executor-test",
            ("AAPL", "QQQ", "SPY"),
            ("QQQ", "SPY"),
        ),
        slot_set=slot_set,
        slot_set_sha256="6" * 64,
    )
    enrollment = replace(
        base,
        control_plane=control,
        activity_selection=selection,
        enrollment_id=UUID("4d4edcd1-1643-5b65-91ee-2a5078f17761"),
        enrollment_sha256="1" * 64,
    )
    history = build_advanced_options_history_binding(
        target_cohort_id=cohort_id,
        source_cohort_id=UUID(
            str(package["history_binding"]["source_cohort_id"])
        ),
        history_before=datetime.fromisoformat(
            package["history_binding"]["history_before_utc"]
        ),
        source_manifest_sha256="7" * 64,
        authorization_reference=package["authorization_reference"],
    )
    live_selection = _executor_selection(package)
    underlying_prices = UnderlyingSnapshotBinding(
        selection_sha256=live_selection["selection_sha256"],
        source_snapshot_sha256=live_selection[
            "source_snapshot_sha256"
        ],
        reviewed_at=CHECKED - timedelta(seconds=5),
        valid_until=STOP,
        prices=tuple(
            UnderlyingSnapshotPrice(
                ticker=row["underlyer"],
                session_date=START.date(),
                price=(
                    Decimal("715")
                    if row["underlyer"] == "QQQ"
                    else Decimal("770")
                ),
                market_time=CHECKED - timedelta(seconds=5),
                timeframe="REAL-TIME",
                selected_contract_ticker=row["contract_ticker"],
                selected_contract_source_sha256=(
                    "8" * 64
                    if row["underlyer"] == "QQQ"
                    else "9" * 64
                ),
                underlying_payload_sha256=(
                    "a" * 64
                    if row["underlyer"] == "QQQ"
                    else "b" * 64
                ),
            )
            for row in live_selection["selected"]
        ),
    )
    return P8CanaryExecutionBinding(
        enrollment,
        history,
        uuid4(),
        underlying_prices,
    )


def _enrollment_payload(plan):
    return {
        "enrollment_id": str(plan.enrollment_id),
        "enrollment_sha256": plan.enrollment_sha256,
    }


def _executor_selection(package):
    selection = _selection(package)
    selection["source_snapshot_at_utc"] = (
        START - timedelta(minutes=2)
    ).isoformat()
    selection["selected_at_utc"] = (
        START - timedelta(minutes=1)
    ).isoformat()
    selection["valid_until_utc"] = STOP.isoformat()
    selection["selected"] = [
        {
            "underlyer": "QQQ",
            "contract_ticker": "O:QQQ261120P00400000",
        },
        {
            "underlyer": "SPY",
            "contract_ticker": "O:SPY261120C00500000",
        },
    ]
    unsigned = dict(selection)
    unsigned.pop("selection_sha256")
    selection["selection_sha256"] = canonical_sha256(unsigned)
    return selection


def _live_observation(
    package,
    binding,
    *,
    stored_reads_authorized=False,
):
    observation = _observation(package)
    observation["checked_at_utc"] = CHECKED.isoformat()
    observation["owner_authorization"]["granted_at_utc"] = (
        CHECKED - timedelta(seconds=5)
    ).isoformat()
    observation["owner_authorization"]["stored_reads"] = (
        stored_reads_authorized
    )
    observation["underlying_price_binding"] = (
        underlying_snapshot_binding_payload(
            binding.underlying_price_binding
        )
    )
    control = binding.enrollment.control_plane
    observation["advanced_enrollment"] = {
        "enrollment_id": str(binding.enrollment.enrollment_id),
        "enrollment_sha256": binding.enrollment.enrollment_sha256,
        "control_plane_plan_id": str(control.plan_id),
        "control_plane_plan_sha256": control.plan_sha256,
        "cohort_id": str(control.cohort_id),
        "cohort_sha256": control.cohort_sha256,
        "profile_id": str(control.profile_id),
        "profile_sha256": control.profile_sha256,
        "manifest_id": str(control.manifest_id),
        "manifest_sha256": control.manifest_sha256,
        "slot_set_sha256": control.slot_set_sha256,
        "selected_contracts": [
            row.contract_ticker
            for row in binding.enrollment.activity_selection.contracts
        ],
    }
    observation["selection"] = _executor_selection(package)
    return observation


class FakeLifecyclePort:
    def __init__(
        self,
        package,
        binding,
        *,
        stored_reads_authorized=False,
    ):
        self.package = package
        self.binding = binding
        self.observation = _live_observation(
            package,
            binding,
            stored_reads_authorized=stored_reads_authorized,
        )
        self.enrollments = 0
        self.starts = 0
        self.stops = 0
        self.stage_runs = 0
        self.publications = 0
        self.rollbacks = 0
        self.cleanups = 0
        self.stored_read_enables = 0
        self.stored_read_disables = 0
        self.stored_reads_enabled = False

    def capture_binding(self, package, *, checked_at):
        assert package is self.package
        assert checked_at == CHECKED
        return self.observation, self.binding

    def enroll(self, binding):
        assert binding is self.binding
        self.enrollments += 1

    def start_residents(self, binding, *, checked_at):
        assert binding is self.binding
        assert checked_at == CHECKED
        self.starts += 1
        return tuple(kind.value for kind in AdvancedOptionsProcessKind)

    def run_stages(self, binding):
        assert binding is self.binding
        self.stage_runs += 1
        return {
            "status": "FINISHED",
            "window_state": "COMPLETE",
            "stage_order": list(OPTIONS_NON_SERVING_STAGE_ORDER),
        }

    def publish(self, binding, stage_result, *, checked_at):
        assert binding is self.binding
        assert stage_result["window_state"] == "COMPLETE"
        assert checked_at == CHECKED
        self.publications += 1
        observation = _page_release_observation(self.package)
        observation["checked_at_utc"] = CHECKED.isoformat()
        observation["published_at_utc"] = CHECKED.isoformat()
        observation["valid_until_utc"] = (
            STOP + timedelta(minutes=30)
        ).isoformat()
        observation["stock_publications"]["screening"][
            "generated_at_utc"
        ] = CHECKED.isoformat()
        observation["stock_publications"]["alerts"][
            "generated_at_utc"
        ] = CHECKED.isoformat()
        return observation

    def enable_stored_reads(self, binding, publication):
        assert binding is self.binding
        self.stored_read_enables += 1
        self.stored_reads_enabled = True
        return {
            "cohort_id": str(
                binding.enrollment.control_plane.cohort_id
            ),
            "release_id": publication["release_id"],
            "verified": True,
        }

    def rollback(self, binding, publication):
        assert binding is self.binding
        self.rollbacks += 1
        if self.stored_reads_enabled:
            self.stored_read_disables += 1
            self.stored_reads_enabled = False

    def stop_residents(self, binding):
        assert binding is self.binding
        self.stops += 1

    def cleanup(self, binding):
        assert binding is self.binding
        self.cleanups += 1
        return {
            "temporary_files": [],
            "processes": [],
            "locks": [],
        }

    def postflight(self):
        return {
            "resident_starts": self.starts,
            "resident_stops": self.stops,
            "stage_runs": self.stage_runs,
            "publication_runs": self.publications,
            "dispatches": 0,
            "deliveries": 0,
            "broker_actions": 0,
            "trades": 0,
        }


def _executor(package, report, semantic, file_sha, port):
    return P8CanaryExecutor(
        port,
        repository_root=ROOT,
        qualification_report_path=report,
        current_source_commit=COMMIT,
        expected_package_sha256=package["package_sha256"],
        expected_qualification_semantic_sha256=semantic,
        expected_qualification_file_sha256=file_sha,
        source_files=SOURCE_FILES,
    )


def _receipt_path(package):
    return ROOT / package["cleanup"]["consumption_receipt_path"]


def _lock_path(package):
    return ROOT / package["cleanup"]["lock_path"]


def _dispose(package):
    _lock_path(package).unlink(missing_ok=True)
    _receipt_path(package).unlink(missing_ok=True)


def test_one_use_success_replay_stop_and_cleanup(tmp_path, monkeypatch):
    package, report, semantic, file_sha = _package(tmp_path)
    binding = _binding(package)
    port = FakeLifecyclePort(package, binding)
    monkeypatch.setattr(
        executor_module,
        "advanced_options_enrollment_plan_payload",
        _enrollment_payload,
    )
    executor = _executor(
        package,
        report,
        semantic,
        file_sha,
        port,
    )
    try:
        result = executor.execute(package, checked_at=CHECKED)

        assert result["status"] == "PASS_P8_CANARY_EXECUTION"
        assert result["postflight"]["resident_stops"] == 1
        assert port.enrollments == port.starts == port.stops == 1
        assert port.stage_runs == port.publications == 1
        assert port.rollbacks == 0
        assert port.cleanups == 1
        assert not _lock_path(package).exists()
        receipt = json.loads(
            _receipt_path(package).read_text(encoding="utf-8")
        )
        assert receipt["state"] == "CONSUMED_CLEAN"
        with pytest.raises(
            P8CanaryExecutionError,
            match="EXECUTION_NOT_READY",
        ):
            executor.execute(package, checked_at=CHECKED)
    finally:
        _dispose(package)


def test_refuses_early_without_lock_receipt_or_effects(
    tmp_path,
    monkeypatch,
):
    package, report, semantic, file_sha = _package(tmp_path)
    binding = _binding(package)
    port = FakeLifecyclePort(package, binding)
    monkeypatch.setattr(
        executor_module,
        "advanced_options_enrollment_plan_payload",
        _enrollment_payload,
    )
    executor = _executor(
        package,
        report,
        semantic,
        file_sha,
        port,
    )
    try:
        with pytest.raises(
            P8CanaryExecutionError,
            match="EARLY_EXECUTION_WINDOW",
        ):
            executor.execute(
                package,
                checked_at=START - timedelta(seconds=1),
            )
        assert not _lock_path(package).exists()
        assert not _receipt_path(package).exists()
        assert port.enrollments == port.starts == port.publications == 0
    finally:
        _dispose(package)


def test_release_failure_rolls_back_stops_and_consumes_package(
    tmp_path,
    monkeypatch,
):
    package, report, semantic, file_sha = _package(tmp_path)
    binding = _binding(package)
    port = FakeLifecyclePort(package, binding)
    monkeypatch.setattr(
        executor_module,
        "advanced_options_enrollment_plan_payload",
        _enrollment_payload,
    )
    executor = _executor(
        package,
        report,
        semantic,
        file_sha,
        port,
    )
    try:
        with pytest.raises(
            P8CanaryExecutionError,
            match="EXECUTION_FAILED",
        ):
            executor.execute(
                package,
                checked_at=CHECKED,
                fault=lambda phase: (
                    (_ for _ in ()).throw(RuntimeError("forced rollback"))
                    if phase == "AFTER_PUBLICATION"
                    else None
                ),
            )
        assert port.rollbacks == port.stops == port.cleanups == 1
        assert not _lock_path(package).exists()
        receipt = json.loads(
            _receipt_path(package).read_text(encoding="utf-8")
        )
        assert receipt["state"] == "CONSUMED_ROLLED_BACK"
    finally:
        _dispose(package)


@pytest.mark.parametrize(
    ("phase", "expected"),
    (
        (
            "AFTER_LIVE_BINDING",
            (0, 0, 0, 0),
        ),
        (
            "AFTER_ENROLLMENT",
            (1, 0, 0, 0),
        ),
        (
            "AFTER_RESIDENT_START",
            (1, 1, 0, 0),
        ),
        (
            "AFTER_STAGES",
            (1, 1, 1, 0),
        ),
        (
            "AFTER_PUBLICATION",
            (1, 1, 1, 1),
        ),
    ),
)
def test_each_executor_failure_boundary_rolls_back_and_cleans(
    tmp_path,
    monkeypatch,
    phase,
    expected,
):
    package, report, semantic, file_sha = _package(tmp_path)
    binding = _binding(package)
    port = FakeLifecyclePort(package, binding)
    monkeypatch.setattr(
        executor_module,
        "advanced_options_enrollment_plan_payload",
        _enrollment_payload,
    )
    executor = _executor(
        package,
        report,
        semantic,
        file_sha,
        port,
    )
    try:
        with pytest.raises(
            P8CanaryExecutionError,
            match="EXECUTION_FAILED",
        ):
            executor.execute(
                package,
                checked_at=CHECKED,
                fault=lambda current: (
                    (_ for _ in ()).throw(RuntimeError(phase))
                    if current == phase
                    else None
                ),
            )
        assert (
            port.enrollments,
            port.starts,
            port.stage_runs,
            port.publications,
        ) == expected
        assert port.rollbacks == port.cleanups == 1
        assert port.stops == int(port.starts > 0)
        assert not _lock_path(package).exists()
        receipt = json.loads(
            _receipt_path(package).read_text(encoding="utf-8")
        )
        assert receipt["state"] == "CONSUMED_ROLLED_BACK"
    finally:
        _dispose(package)


def test_failure_after_stored_read_activation_disables_exact_pin(
    tmp_path,
    monkeypatch,
):
    package, report, semantic, file_sha = _package(
        tmp_path,
        stored_reads_prepared=True,
    )
    binding = _binding(package)
    port = FakeLifecyclePort(
        package,
        binding,
        stored_reads_authorized=True,
    )
    monkeypatch.setattr(
        executor_module,
        "advanced_options_enrollment_plan_payload",
        _enrollment_payload,
    )
    executor = _executor(
        package,
        report,
        semantic,
        file_sha,
        port,
    )
    try:
        with pytest.raises(
            P8CanaryExecutionError,
            match="EXECUTION_FAILED",
        ):
            executor.execute(
                package,
                checked_at=CHECKED,
                stored_reads_authorized=True,
                fault=lambda phase: (
                    (_ for _ in ()).throw(RuntimeError(phase))
                    if phase == "AFTER_STORED_READS"
                    else None
                ),
            )
        assert port.stored_read_enables == 1
        assert port.stored_read_disables == 1
        assert port.stored_reads_enabled is False
        assert port.rollbacks == port.stops == port.cleanups == 1
        receipt = json.loads(
            _receipt_path(package).read_text(encoding="utf-8")
        )
        assert receipt["state"] == "CONSUMED_ROLLED_BACK"
    finally:
        _dispose(package)


def test_owned_disposable_executor_qualification(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        executor_module,
        "advanced_options_enrollment_plan_payload",
        _enrollment_payload,
    )
    cases = {}
    for name in ("success", "refusal", "rollback"):
        root = tmp_path / name
        root.mkdir()
        package, report, semantic, file_sha = _package(root)
        binding = _binding(package)
        port = FakeLifecyclePort(package, binding)
        cases[name] = (
            package,
            port,
            _executor(
                package,
                report,
                semantic,
                file_sha,
                port,
            ),
        )
    retained = None
    try:
        success_package, success_port, success_executor = cases["success"]
        result = success_executor.execute(
            success_package,
            checked_at=CHECKED,
        )
        receipt = json.loads(
            _receipt_path(success_package).read_text(encoding="utf-8")
        )
        with pytest.raises(P8CanaryExecutionError) as replay_error:
            success_executor.execute(
                success_package,
                checked_at=CHECKED,
            )

        refusal_package, refusal_port, refusal_executor = cases["refusal"]
        with pytest.raises(P8CanaryExecutionError) as refusal_error:
            refusal_executor.execute(
                refusal_package,
                checked_at=START - timedelta(seconds=1),
            )

        rollback_package, rollback_port, rollback_executor = cases[
            "rollback"
        ]
        with pytest.raises(P8CanaryExecutionError) as rollback_error:
            rollback_executor.execute(
                rollback_package,
                checked_at=CHECKED,
                fault=lambda phase: (
                    (_ for _ in ()).throw(
                        RuntimeError("qualification rollback")
                    )
                    if phase == "AFTER_PUBLICATION"
                    else None
                ),
            )
        rollback_receipt = json.loads(
            _receipt_path(rollback_package).read_text(encoding="utf-8")
        )
        for package, _, _ in cases.values():
            _dispose(package)
        leftovers = [
            str(path)
            for package, _, _ in cases.values()
            for path in (_lock_path(package), _receipt_path(package))
            if path.exists()
        ]
        qualification = {
            "contract": (
                "advanced-options-p8-executor-provider-free-"
                "qualification-v1"
            ),
            "verdict": "PASS",
            "pipeline_qualification": {
                "semantic_sha256": (
                    PIPELINE_QUALIFICATION_SEMANTIC_SHA256
                ),
                "file_sha256": PIPELINE_QUALIFICATION_FILE_SHA256,
            },
            "lifecycle": {
                "success": result["status"],
                "refusal": refusal_error.value.code,
                "replay": replay_error.value.code,
                "rollback_error": rollback_error.value.code,
                "rollback_receipt": rollback_receipt["state"],
                "receipt_state": receipt["state"],
                "mandatory_stops": (
                    success_port.stops + rollback_port.stops
                ),
                "rollback_actions": rollback_port.rollbacks,
                "refusal_effects": {
                    "enrollments": refusal_port.enrollments,
                    "resident_starts": refusal_port.starts,
                    "publications": refusal_port.publications,
                },
            },
            "effects": {
                "provider_requests": 0,
                "dispatches": 0,
                "deliveries": 0,
                "broker_actions": 0,
                "trades": 0,
            },
            "cleanup": {
                "databases": [],
                "roles": [],
                "files": leftovers,
                "processes": [],
                "locks": [],
            },
        }
        assert qualification["lifecycle"] == {
            "success": "PASS_P8_CANARY_EXECUTION",
            "refusal": "EARLY_EXECUTION_WINDOW",
            "replay": "EXECUTION_NOT_READY",
            "rollback_error": "EXECUTION_FAILED",
            "rollback_receipt": "CONSUMED_ROLLED_BACK",
            "receipt_state": "CONSUMED_CLEAN",
            "mandatory_stops": 2,
            "rollback_actions": 1,
            "refusal_effects": {
                "enrollments": 0,
                "resident_starts": 0,
                "publications": 0,
            },
        }
        assert leftovers == []
        if os.environ.get(REPORT_ENV) == "1":
            digest = canonical_sha256(qualification)
            retained = REPORT_ROOT / (
                "advanced-options-p8-executor-provider-free-"
                f"qualification-{digest[:16]}.json"
            )
            with retained.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as handle:
                json.dump(
                    {**qualification, "report_sha256": digest},
                    handle,
                    indent=2,
                    sort_keys=True,
                )
                handle.write("\n")
        assert qualification["verdict"] == "PASS"
    finally:
        for package, _, _ in cases.values():
            _dispose(package)
    if retained is not None:
        print(f"P8_EXECUTOR_QUALIFICATION {retained}")
