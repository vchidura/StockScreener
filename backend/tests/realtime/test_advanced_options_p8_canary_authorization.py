import copy
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

import realtime.advanced_options_p8_canary_authorization as authorization
from realtime.advanced_options_p8_canary_authorization import (
    LIVE_OBSERVATION_SCHEMA,
    PAGE_RELEASE_OBSERVATION_SCHEMA,
    PREPARED_AUTHORITIES,
    SELECTION_EVIDENCE_SCHEMA,
    SOURCE_BINDING_PATHS,
    build_p8_canary_authorization,
    canonical_sha256,
    p8_canary_plan,
    p8_canary_status,
    provider_free_p8_canary_preflight,
    validate_p8_canary_authorization,
    validate_p8_live_observation,
    validate_p8_page_release_observation,
)
from realtime.advanced_options_underlying_snapshot import (
    UnderlyingSnapshotBinding,
    UnderlyingSnapshotPrice,
    underlying_snapshot_binding_payload,
)
from scripts.audit_advanced_options_p8_canary_authorization import (
    main as audit_package,
)
from scripts.prepare_advanced_options_p8_canary_authorization import (
    main as prepare_package,
)


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[3]
SOURCE_FILES = (
    "backend/realtime/advanced_options_p8_canary_authorization.py",
)
COMMIT = "d38c4480f954b4f02270541913adfce666bae99d"
REFERENCE = "ADVANCED-OPTIONS-P8-CANARY-20261022-TEST"
NONCE = UUID("2b1d9a74-36c8-4c4d-a2ab-6e790af83122")
PREPARED = datetime(2026, 10, 21, 8, 0, tzinfo=UTC)
START = datetime(2026, 10, 22, 17, 0, tzinfo=UTC)
STOP = datetime(2026, 10, 22, 17, 10, tzinfo=UTC)
CHECKED = datetime(2026, 10, 22, 16, 59, tzinfo=UTC)


def test_source_binding_covers_repository_owned_p8_docker_runtime():
    expected = {
        "backend/realtime/advanced_options_p8_docker.py",
        "backend/realtime/advanced_options_p8_production.py",
        "backend/realtime/advanced_options_p8_roles.py",
        "backend/realtime/advanced_options_p8_runtime.py",
        "backend/realtime/advanced_options_p8_selection_source.py",
        "backend/realtime/advanced_options_p8_window.py",
        "backend/realtime/advanced_options_read_pin.py",
        "backend/scripts/run_realtime_v2_container.py",
        "backend/realtime/sql_v2/032_advanced_options_read_pins.sql",
        "docker-compose.realtime-v2.yml",
    }

    assert expected <= set(SOURCE_BINDING_PATHS)
    assert len(SOURCE_BINDING_PATHS) == len(set(SOURCE_BINDING_PATHS))
    assert all((ROOT / path).is_file() for path in expected)


@pytest.fixture
def qualification(tmp_path):
    semantic_report = {
        "contract": "advanced-options-p8-provider-free-qualification-report-v1",
        "verdict": "PASS",
        "provider": {"calls": 0},
        "cleanup": {
            "databases": [],
            "roles": [],
            "files": [],
            "processes": [],
            "locks": [],
        },
    }
    semantic_sha256 = canonical_sha256(semantic_report)
    report = {
        **semantic_report,
        "report_sha256": semantic_sha256,
    }
    path = tmp_path / "qualification.json"
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return (
        path,
        semantic_sha256,
        authorization.hashlib.sha256(path.read_bytes()).hexdigest(),
    )


@pytest.fixture
def package(qualification):
    report, semantic_sha256, file_sha256 = qualification
    return build_p8_canary_authorization(
        report,
        repository_root=ROOT,
        source_commit=COMMIT,
        authorization_reference=REFERENCE,
        package_nonce=NONCE,
        prepared_at=PREPARED,
        window_start=START,
        window_end=STOP,
        expected_qualification_semantic_sha256=semantic_sha256,
        expected_qualification_file_sha256=file_sha256,
        source_files=SOURCE_FILES,
    )


def _selection(package):
    value = {
        "schema": SELECTION_EVIDENCE_SCHEMA,
        "source_snapshot_at_utc": (
            START - timedelta(minutes=2)
        ).isoformat(),
        "selected_at_utc": (
            START - timedelta(seconds=90)
        ).isoformat(),
        "valid_until_utc": STOP.isoformat(),
        "policy_sha256": package[
            "provider_selection"
        ]["policy"]["sha256"],
        "source_snapshot_sha256": "a" * 64,
        "request_count": 2,
        "decoded_bytes": 2_000_000,
        "pages": 2,
        "pagination_requests": 0,
        "selected": [
            {
                "underlyer": "QQQ",
                "contract_ticker": "O:QQQ261022P00715000",
            },
            {
                "underlyer": "SPY",
                "contract_ticker": "O:SPY261022C00770000",
            },
        ],
    }
    value["selection_sha256"] = canonical_sha256(value)
    return value


def _observation(package):
    selection = _selection(package)
    underlying_prices = UnderlyingSnapshotBinding(
        selection_sha256=selection["selection_sha256"],
        source_snapshot_sha256=selection[
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
            for row in selection["selected"]
        ),
    )
    return {
        "schema": LIVE_OBSERVATION_SCHEMA,
        "checked_at_utc": CHECKED.isoformat(),
        "current_source_commit": package["source"]["commit"],
        "source_files": package["source"]["files"],
        "policy_hashes": package["policy_hashes"],
        "orchestration": package["orchestration"],
        "selection": selection,
        "advanced_enrollment": {
            "enrollment_id": "4d4edcd1-1643-5b65-91ee-2a5078f17761",
            "enrollment_sha256": "1" * 64,
            "control_plane_plan_id": package["orchestration"]["plan_id"],
            "control_plane_plan_sha256": "2" * 64,
            "cohort_id": package["cohort"]["cohort_id"],
            "cohort_sha256": "3" * 64,
            "profile_id": "5247fc68-4236-591d-90dd-d7d805b65a66",
            "profile_sha256": "4" * 64,
            "manifest_id": "a56fc2b1-57a7-56ce-938d-7bcc5dff8045",
            "manifest_sha256": "5" * 64,
            "slot_set_sha256": "6" * 64,
            "selected_contracts": [
                "O:QQQ261022P00715000",
                "O:SPY261022C00770000",
            ],
        },
        "history_source": {
            "cohort_id": package["history_binding"]["source_cohort_id"],
            "provider_tier": "MASSIVE_DEVELOPER",
            "manifest_sha256": "7" * 64,
            "immutable": True,
            "detector_ready": False,
            "repaired_sessions": package[
                "history_binding"
            ]["required_sessions"],
        },
        "underlying_price_binding": (
            underlying_snapshot_binding_payload(
                underlying_prices
            )
        ),
        "equity_windows": [],
        "running_peer_owners": [],
        "retention_dependencies": [],
        "existing_options_releases": [],
        "existing_options_pointers": [],
        "stored_read_flags": {
            "backend": False,
            "frontend": False,
        },
        "owner_authorization": {
            "authorization_reference": package[
                "authorization_reference"
            ],
            "package_sha256": package["package_sha256"],
            "granted_at_utc": (
                CHECKED - timedelta(seconds=30)
            ).isoformat(),
            "granted_authorities": list(PREPARED_AUTHORITIES),
            "stored_reads": False,
            "dispatch_delivery": False,
            "broker_trading": False,
        },
    }


def _resign_selection(observation):
    selection = observation["selection"]
    unsigned = dict(selection)
    unsigned.pop("selection_sha256")
    selection["selection_sha256"] = canonical_sha256(unsigned)
    underlying = observation.get("underlying_price_binding")
    if underlying is not None:
        underlying["selection_sha256"] = selection["selection_sha256"]
        underlying["source_snapshot_sha256"] = selection[
            "source_snapshot_sha256"
        ]
        underlying["valid_until"] = min(
            datetime.fromisoformat(underlying["valid_until"]),
            datetime.fromisoformat(selection["valid_until_utc"]),
        ).isoformat()
        unsigned_binding = dict(underlying)
        unsigned_binding.pop("binding_sha256")
        underlying["binding_sha256"] = canonical_sha256(
            unsigned_binding
        )


def test_live_observation_refuses_missing_or_stale_underlying_binding(package):
    missing = _observation(package)
    missing.pop("underlying_price_binding")
    with pytest.raises(ValueError, match="fields changed"):
        validate_p8_live_observation(
            package,
            missing,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )
    stale = _observation(package)
    underlying = stale["underlying_price_binding"]
    underlying["valid_until"] = (
        CHECKED - timedelta(seconds=1)
    ).isoformat()
    unsigned = dict(underlying)
    unsigned.pop("binding_sha256")
    underlying["binding_sha256"] = canonical_sha256(unsigned)
    with pytest.raises(ValueError, match="stale or future"):
        validate_p8_live_observation(
            package,
            stale,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )


def _page_release_observation(package):
    return {
        "schema": PAGE_RELEASE_OBSERVATION_SCHEMA,
        "checked_at_utc": STOP.isoformat(),
        "cohort_id": package["cohort"]["cohort_id"],
        "scope_sha256": "e" * 64,
        "release_id": "6c297a25-821b-51ee-b1f0-16ebf3125410",
        "published_at_utc": (
            STOP - timedelta(seconds=20)
        ).isoformat(),
        "valid_until_utc": (
            STOP + timedelta(minutes=30)
        ).isoformat(),
        "artifacts": [
            {
                "artifact_kind": "FLOW",
                "snapshot_type": "OPTIONS_FLOW_PAGE_V2",
                "contract": "advanced-options-flow-page-v2",
                "snapshot_id": (
                    "2a25ef4c-3bcf-57de-a02b-9492e360b7c0"
                ),
                "cohort_id": package["cohort"]["cohort_id"],
                "release_id": (
                    "6c297a25-821b-51ee-b1f0-16ebf3125410"
                ),
                "scope_sha256": "e" * 64,
                "pages": ["OPTIONS_ACTIVITY", "OPTIONS_FLOW"],
            },
            {
                "artifact_kind": "SCREENER",
                "snapshot_type": "OPTIONS_SCREENER_PAGE_V2",
                "contract": "advanced-options-screener-page-v2",
                "snapshot_id": (
                    "13772969-5430-5c08-91b2-38cf794e7417"
                ),
                "cohort_id": package["cohort"]["cohort_id"],
                "release_id": (
                    "6c297a25-821b-51ee-b1f0-16ebf3125410"
                ),
                "scope_sha256": "e" * 64,
                "pages": ["OPTIONS_SCREENER"],
            },
            {
                "artifact_kind": "ALERTS",
                "snapshot_type": "OPTIONS_ALERTS_PAGE_V2",
                "contract": "advanced-options-alerts-page-v2",
                "snapshot_id": (
                    "f8eb7df7-9e81-5529-a3f0-87fa16dde614"
                ),
                "cohort_id": package["cohort"]["cohort_id"],
                "release_id": (
                    "6c297a25-821b-51ee-b1f0-16ebf3125410"
                ),
                "scope_sha256": "e" * 64,
                "pages": ["OPTIONS_ALERTS"],
            },
        ],
        "stock_publications": {
            "screening": {
                "page": "STOCK_SCREENER",
                "contract": "SCREENING_GENERATION",
                "generation_id": (
                    "1e32bc9d-7319-592a-9ebc-92ea6d082ca9"
                ),
                "cohort_id": package["cohort"]["cohort_id"],
                "provider_tier": "MASSIVE_ADVANCED",
                "status": "COMPLETE",
                "generated_at_utc": (
                    STOP - timedelta(seconds=40)
                ).isoformat(),
            },
            "alerts": {
                "page": "STOCK_ALERTS",
                "contract": "STOCK_ALERTS_V1/PLATFORM",
                "release_id": (
                    "6c297a25-821b-51ee-b1f0-16ebf3125410"
                ),
                "snapshot_id": (
                    "5040328b-3c30-5ee8-bdcc-222109ec6323"
                ),
                "screening_generation_id": (
                    "1e32bc9d-7319-592a-9ebc-92ea6d082ca9"
                ),
                "cohort_id": package["cohort"]["cohort_id"],
                "provider_tier": "MASSIVE_ADVANCED",
                "generated_at_utc": (
                    STOP - timedelta(seconds=30)
                ).isoformat(),
            },
        },
        "portal_projection_diagnostics": [
            "OPTIONS_FLOW",
            "OPTIONS_SCREENER",
            "OPTIONS_ALERTS",
            "OPTIONS_MARKS",
        ],
        "request_time_effects": {
            "provider_fetches": 0,
            "detector_runs": 0,
            "publication_retries": 0,
            "dispatches": 0,
            "deliveries": 0,
            "broker_actions": 0,
            "trades": 0,
        },
    }


def test_package_plan_preflight_status_and_live_binding_pass(
    package,
    qualification,
):
    report, semantic_sha256, file_sha256 = qualification
    assert validate_p8_canary_authorization(
        package,
        qualification_report_path=report,
        repository_root=ROOT,
        current_source_commit=COMMIT,
        expected_package_sha256=package["package_sha256"],
        expected_qualification_semantic_sha256=semantic_sha256,
        expected_qualification_file_sha256=file_sha256,
        source_files=SOURCE_FILES,
    ) == package
    plan = p8_canary_plan(package)
    assert plan["status"] == "PASS_INERT_EXACT_P8_CANARY_PLAN"
    assert plan["prepared_authorities"] == list(PREPARED_AUTHORITIES)
    assert not plan["automatic_execution_available"]
    assert plan["guarded_one_use_execution_available"]
    status = p8_canary_status(
        package,
        now=PREPARED + timedelta(hours=1),
        prior_run_records=[],
    )
    assert status["state"] == "PREPARED_UNARMED_UNCONSUMED"
    preflight = provider_free_p8_canary_preflight(
        package,
        repository_root=ROOT,
        now=PREPARED + timedelta(hours=1),
        prior_run_records=[],
        environ={},
    )
    assert preflight["status"] == (
        "PASS_PROVIDER_FREE_P8_CANARY_PREFLIGHT"
    )
    live = validate_p8_live_observation(
        package,
        _observation(package),
        repository_root=ROOT,
        now=CHECKED,
        prior_run_records=[],
        environ={},
    )
    assert live["status"] == "PASS_READ_ONLY_LIVE_P8_CANARY_PREFLIGHT"
    assert live["realized_binding"]["manifest"]["member_count"] == 4
    assert len(live["realized_binding"]["slot"]["members"]) == 4
    assert live["realized_history_binding"]["source_cohort_id"] == (
        package["history_binding"]["source_cohort_id"]
    )
    assert not live["stored_reads_authorized"]
    assert not live["dispatch_delivery_authorized"]
    assert not live["broker_trading_authorized"]


def test_live_binding_accepts_fresh_active_window_selection(package):
    checked_at = START + timedelta(seconds=30)
    observation = _observation(package)
    observation["checked_at_utc"] = checked_at.isoformat()
    observation["selection"].update({
        "source_snapshot_at_utc": (
            checked_at - timedelta(seconds=30)
        ).isoformat(),
        "selected_at_utc": checked_at.isoformat(),
    })
    _resign_selection(observation)
    underlying = observation["underlying_price_binding"]
    underlying["selection_sha256"] = observation[
        "selection"
    ]["selection_sha256"]
    underlying["reviewed_at"] = checked_at.isoformat()
    unsigned = dict(underlying)
    unsigned.pop("binding_sha256")
    underlying["binding_sha256"] = canonical_sha256(unsigned)
    observation["owner_authorization"]["granted_at_utc"] = (
        checked_at.isoformat()
    )

    live = validate_p8_live_observation(
        package,
        observation,
        repository_root=ROOT,
        now=checked_at,
        prior_run_records=[],
        environ={},
    )

    assert live["status"] == "PASS_READ_ONLY_LIVE_P8_CANARY_PREFLIGHT"


def test_package_rejects_source_commit_and_authority_drift(
    package,
    qualification,
):
    report, semantic_sha256, file_sha256 = qualification
    with pytest.raises(ValueError, match="drifted"):
        validate_p8_canary_authorization(
            package,
            qualification_report_path=report,
            repository_root=ROOT,
            current_source_commit="f" * 40,
            expected_package_sha256=package["package_sha256"],
            expected_qualification_semantic_sha256=semantic_sha256,
            expected_qualification_file_sha256=file_sha256,
            source_files=SOURCE_FILES,
        )
    changed = copy.deepcopy(package)
    changed["authorities"]["dispatch_delivery"]["delivery"] = True
    changed["package_sha256"] = canonical_sha256({
        key: value for key, value in changed.items()
        if key != "package_sha256"
    })
    with pytest.raises(ValueError, match="drifted"):
        validate_p8_canary_authorization(
            changed,
            qualification_report_path=report,
            repository_root=ROOT,
            current_source_commit=COMMIT,
            expected_package_sha256=changed["package_sha256"],
            expected_qualification_semantic_sha256=semantic_sha256,
            expected_qualification_file_sha256=file_sha256,
            source_files=SOURCE_FILES,
        )


def test_live_preflight_accepts_separately_prepared_exact_cohort_reads(
    qualification,
):
    report, semantic_sha256, file_sha256 = qualification
    package = build_p8_canary_authorization(
        report,
        repository_root=ROOT,
        source_commit=COMMIT,
        authorization_reference=REFERENCE + "-READS",
        package_nonce=UUID("8d8ed441-bf4c-4a53-b181-61215ceaf3e6"),
        prepared_at=PREPARED,
        window_start=START,
        window_end=STOP,
        expected_qualification_semantic_sha256=semantic_sha256,
        expected_qualification_file_sha256=file_sha256,
        source_files=SOURCE_FILES,
        stored_reads_prepared=True,
    )
    observation = _observation(package)
    observation["owner_authorization"]["stored_reads"] = True
    assert p8_canary_plan(package)["stored_reads_prepared"] is True

    live = validate_p8_live_observation(
        package,
        observation,
        repository_root=ROOT,
        now=CHECKED,
        prior_run_records=[],
        environ={},
        stored_reads_authorized=True,
    )

    assert live["stored_reads_authorized"] is True


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda value: value["selection"].update({
                "source_snapshot_at_utc": (
                    START - timedelta(minutes=6)
                ).isoformat(),
            }),
            "stale",
        ),
        (
            lambda value: value["selection"].update({
                "request_count": 3,
            }),
            "budget",
        ),
        (
            lambda value: value["selection"]["selected"].append({
                "underlyer": "IWM",
                "contract_ticker": "O:IWM261022C00300000",
            }),
            "exactly two",
        ),
    ],
)
def test_live_preflight_rejects_selection_staleness_budget_and_expansion(
    package,
    mutator,
    message,
):
    observation = _observation(package)
    mutator(observation)
    _resign_selection(observation)
    with pytest.raises(ValueError, match=message):
        validate_p8_live_observation(
            package,
            observation,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )


def test_live_preflight_requires_selection_through_arm_not_entire_window(
    package,
):
    observation = _observation(package)
    observation["selection"]["valid_until_utc"] = (
        START + timedelta(minutes=1)
    ).isoformat()
    _resign_selection(observation)

    live = validate_p8_live_observation(
        package,
        observation,
        repository_root=ROOT,
        now=CHECKED,
        prior_run_records=[],
        environ={},
    )

    assert live["live_observation_validated"] is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "running_peer_owners",
            [{"owner": "advanced-options-ingress-v1", "pid": 10}],
            "peer owner",
        ),
        (
            "retention_dependencies",
            [{"package": "OPTIONS-O-WP5"}],
            "retention",
        ),
        (
            "existing_options_releases",
            ["prior-release"],
            "release or pointer",
        ),
        (
            "existing_options_pointers",
            ["prior-pointer"],
            "release or pointer",
        ),
    ],
)
def test_live_preflight_rejects_peer_retention_and_ambiguous_release_state(
    package,
    field,
    value,
    message,
):
    observation = _observation(package)
    observation[field] = value
    with pytest.raises(ValueError, match=message):
        validate_p8_live_observation(
            package,
            observation,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )


def test_live_preflight_rejects_equity_overlap(package):
    observation = _observation(package)
    observation["equity_windows"] = [{
        "start_utc": (START - timedelta(minutes=1)).isoformat(),
        "stop_utc": (START + timedelta(minutes=1)).isoformat(),
    }]
    with pytest.raises(ValueError, match="overlaps an Equity"):
        validate_p8_live_observation(
            package,
            observation,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )


@pytest.mark.parametrize("field", ["source_files", "policy_hashes", "orchestration"])
def test_live_preflight_rejects_source_config_and_stage_drift(package, field):
    observation = _observation(package)
    observation[field] = copy.deepcopy(observation[field])
    if field == "source_files":
        observation[field][0]["sha256"] = "f" * 64
    elif field == "policy_hashes":
        observation[field]["flow_policy_sha256"] = "f" * 64
    else:
        observation[field]["enabled_stage_ids"].append("EQUITY_INPUT_GATE")
    with pytest.raises(ValueError, match="source or configuration"):
        validate_p8_live_observation(
            package,
            observation,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )


@pytest.mark.parametrize("field", ["stored_reads", "dispatch_delivery", "broker_trading"])
def test_live_preflight_rejects_expanded_owner_authority(package, field):
    observation = _observation(package)
    observation["owner_authorization"][field] = True
    with pytest.raises(ValueError, match="expanded"):
        validate_p8_live_observation(
            package,
            observation,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=[],
            environ={},
        )


def test_status_rejects_reused_expired_and_ambiguous_package(package):
    consumed = p8_canary_status(
        package,
        now=CHECKED,
        prior_run_records=[{
            "package_nonce": package["package_nonce"],
            "state": "CONSUMED_CLEAN",
        }],
    )
    assert consumed["state"] == "CONSUMED"
    expired = p8_canary_status(
        package,
        now=STOP,
        prior_run_records=[],
    )
    assert expired["state"] == "EXPIRED_UNCONSUMED"
    ambiguous = p8_canary_status(
        package,
        now=CHECKED,
        prior_run_records=None,
    )
    assert ambiguous["state"] == "AMBIGUOUS_PRIOR_RUN"
    with pytest.raises(ValueError, match="AMBIGUOUS_PRIOR_RUN"):
        provider_free_p8_canary_preflight(
            package,
            repository_root=ROOT,
            now=CHECKED,
            prior_run_records=None,
            environ={},
        )


def test_page_release_gate_preserves_all_existing_page_contracts(package):
    result = validate_p8_page_release_observation(
        package,
        _page_release_observation(package),
    )
    assert result["status"] == "PASS_P8_COMBINED_PAGE_RELEASE_GATE"
    assert result["artifact_kinds"] == ["FLOW", "SCREENER", "ALERTS"]
    assert result["pages"] == [
        "STOCK_SCREENER",
        "STOCK_ALERTS",
        "OPTIONS_ACTIVITY",
        "OPTIONS_FLOW",
        "OPTIONS_SCREENER",
        "OPTIONS_ALERTS",
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "stock_publications",
            {
                "screening": {
                    "page": "STOCK_SCREENER",
                    "contract": "SCREENING_GENERATION",
                    "generation_id": (
                        "1e32bc9d-7319-592a-9ebc-92ea6d082ca9"
                    ),
                    "cohort_id": "wrong-cohort",
                    "provider_tier": "MASSIVE_ADVANCED",
                    "status": "COMPLETE",
                    "generated_at_utc": (
                        STOP - timedelta(seconds=40)
                    ).isoformat(),
                },
                "alerts": {
                    "page": "STOCK_ALERTS",
                    "contract": "STOCK_ALERTS_V1/PLATFORM",
                    "release_id": (
                        "6c297a25-821b-51ee-b1f0-16ebf3125410"
                    ),
                    "snapshot_id": (
                        "5040328b-3c30-5ee8-bdcc-222109ec6323"
                    ),
                    "screening_generation_id": (
                        "1e32bc9d-7319-592a-9ebc-92ea6d082ca9"
                    ),
                    "cohort_id": "wrong-cohort",
                    "provider_tier": "MASSIVE_ADVANCED",
                    "generated_at_utc": (
                        STOP - timedelta(seconds=30)
                    ).isoformat(),
                },
            },
            "stock screener",
        ),
        (
            "artifacts",
            [{
                "artifact_kind": "ALERTS",
                "pages": ["OPTIONS_ALERTS"],
            }],
            "membership",
        ),
        (
            "portal_projection_diagnostics",
            ["OPTIONS_ALERTS"],
            "diagnostic",
        ),
        (
            "request_time_effects",
            {
                "provider_fetches": 1,
                "detector_runs": 0,
                "publication_retries": 0,
                "dispatches": 0,
                "deliveries": 0,
                "broker_actions": 0,
                "trades": 0,
            },
            "forbidden",
        ),
    ],
)
def test_page_release_gate_rejects_missing_scope_and_request_time_work(
    package,
    field,
    value,
    message,
):
    observation = _page_release_observation(package)
    observation[field] = value
    with pytest.raises(ValueError, match=message):
        validate_p8_page_release_observation(package, observation)


def test_prepare_and_audit_cli_use_owned_files_only(
    tmp_path,
    qualification,
    capsys,
):
    report, semantic_sha256, file_sha256 = qualification
    package_path = tmp_path / "package.json"
    assert prepare_package([
        "--qualification-report",
        str(report),
        "--expected-qualification-semantic-sha256",
        semantic_sha256,
        "--expected-qualification-file-sha256",
        file_sha256,
        "--source-commit",
        COMMIT,
        "--authorization-reference",
        REFERENCE,
        "--package-nonce",
        str(NONCE),
        "--prepared-at",
        PREPARED.isoformat(),
        "--window-start",
        START.isoformat(),
        "--window-end",
        STOP.isoformat(),
        "--output",
        str(package_path),
    ]) == 0
    prepared = json.loads(capsys.readouterr().out)
    prior = tmp_path / "prior.json"
    prior.write_text("[]\n", encoding="utf-8")
    plan = tmp_path / "plan.json"
    assert audit_package([
        "--package",
        str(package_path),
        "--qualification-report",
        str(report),
        "--expected-qualification-semantic-sha256",
        semantic_sha256,
        "--expected-qualification-file-sha256",
        file_sha256,
        "--expected-package-sha256",
        prepared["package_sha256"],
        "--source-commit",
        COMMIT,
        "--prior-run-records",
        str(prior),
        "--checked-at",
        (PREPARED + timedelta(hours=1)).isoformat(),
        "--plan",
        "--output",
        str(plan),
    ]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "PASS_INERT_EXACT_P8_CANARY_PLAN"
    assert plan.is_file()
    assert set(tmp_path.iterdir()) == {
        report,
        package_path,
        prior,
        plan,
    }


def test_no_automatic_execution_function_exists():
    assert not any(
        name.startswith(("execute_", "run_"))
        for name in vars(authorization)
    )
