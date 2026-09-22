from datetime import timedelta
import hashlib
from pathlib import Path

import pytest

from options.stock_setup_binding import (
    SETUP_BINDING_CODE_PATHS, SETUP_RESEARCH_UNDERLYERS, SetupResearchManifest,
    load_setup_research_manifest,
)
from options.stock_setup_gates import DirectHourlyAcceptancePolicy
from test_equity_behavior_setup import NOW, direct_policy, publication_inputs


BACKEND = Path(__file__).resolve().parents[2]


def research_manifest(**updates):
    _, _, _, source = publication_inputs()
    source = direct_policy(source)
    values = dict(
        study_id="setup-binding-fixture-v1", frozen_at=NOW - timedelta(days=1),
        starts_at=NOW - timedelta(minutes=30), ends_at=NOW + timedelta(days=1),
        underlyers=tuple(sorted(SETUP_RESEARCH_UNDERLYERS)),
        source_ledger="backups/equity-shadow/stock-ideas-forward-v2/forward.sqlite",
        source_policy=source, detector_policy=DirectHourlyAcceptancePolicy(setup_source_policy_sha256=source.sha256),
        option_strategy_policy_sha256="b" * 64, valuation_policy_sha256="e" * 64,
        option_configuration_sha256="c" * 64, option_market_policy_sha256="d" * 64,
        option_analysis_policy_sha256="d" * 64,
        implementation_sha256s=tuple((name, hashlib.sha256((BACKEND / name).read_bytes()).hexdigest())
                                     for name in SETUP_BINDING_CODE_PATHS),
        maximum_candidates_per_read=20, maximum_holding_seconds=7200,
    )
    values.update(updates)
    return SetupResearchManifest(**values)


@pytest.mark.parametrize("mutation", ["cohort", "duplicate", "window", "retroactive", "path", "windows_path", "source", "code", "activation"])
def test_setup_manifest_rejects_implicit_or_changed_scope(mutation):
    payload = research_manifest().model_dump()
    if mutation == "cohort": payload["underlyers"] = ("AAPL",)
    if mutation == "duplicate": payload["underlyers"] = ("AAPL",) * 13
    if mutation == "window": payload["ends_at"] = NOW + timedelta(days=8)
    if mutation == "retroactive": payload["frozen_at"] = NOW
    if mutation == "path": payload["source_ledger"] = "backups/equity-shadow/../../secret.sqlite"
    if mutation == "windows_path": payload["source_ledger"] = "C:\\outside.sqlite"
    if mutation == "source": payload["detector_policy"]["setup_source_policy_sha256"] = "0" * 64
    if mutation == "code": payload["implementation_sha256s"] = ()
    if mutation == "activation": payload["collection_enabled"] = True
    with pytest.raises(ValueError):
        SetupResearchManifest.model_validate(payload)


def test_setup_manifest_loader_resolves_external_hash_and_code(tmp_path):
    manifest = research_manifest()
    path = tmp_path / "manifest.json"
    path.write_text(manifest.canonical_json(), encoding="ascii")
    loaded = load_setup_research_manifest(path, expected_sha256=manifest.sha256, backend_dir=BACKEND)
    assert loaded == manifest and not loaded.collection_enabled
    with pytest.raises(ValueError, match="independently pinned"):
        load_setup_research_manifest(path, expected_sha256="0" * 64, backend_dir=BACKEND)
    changed = manifest.model_dump()
    changed["implementation_sha256s"] = tuple((name, "0" * 64) for name in SETUP_BINDING_CODE_PATHS)
    changed = SetupResearchManifest.model_validate(changed)
    path.write_text(changed.canonical_json(), encoding="ascii")
    with pytest.raises(ValueError, match="implementation"):
        load_setup_research_manifest(path, expected_sha256=changed.sha256, backend_dir=BACKEND)


def binding_inputs(structure="LONG_CALL"):
    from dataclasses import replace
    from datetime import datetime
    from uuid import UUID, uuid4
    from equity.behavior import BehaviorComponent, BehaviorMetric, METRICS, StockBehaviorSnapshot
    from equity.behavior_setup import bind_direct_stock_setup
    from options.domain import AssetType, ContractType, OptionContractReference
    from options.outcomes import configured_valuation_policy
    from options.outcome_contracts import assess_option_package
    from options.repositories.stock_behavior_assessments import OptionMatrixAssessmentLineage
    from options.strategies.domain import OptionSide, StructureType
    from options.strategies.engine import _leg
    from research.stock_idea_engine import candidate_record, digest
    from test_equity_behavior import NOW as STOCK_NOW, snapshot_payload
    from test_equity_context_resolution import security
    from test_equity_outcomes import bar
    from test_outcome_contracts import package_candidate
    from test_strategy_engine import snapshot

    direction = -1 if structure.startswith("PUT") or structure == "LONG_PUT" else 1
    setup_candidate, instance, publication, source_policy = publication_inputs()
    setup_candidate = replace(setup_candidate, direction=direction,
        stop=98. if direction == 1 else 102., target=110. if direction == 1 else 90.)
    publication["candidates"] = {setup_candidate.episode_id: candidate_record(setup_candidate)}
    publication["dispositions"][0].update(episode_id=setup_candidate.episode_id, direction=direction)
    source_policy = direct_policy(source_policy)
    setup = bind_direct_stock_setup(instance=instance, publication=publication,
        record_id=publication["window_key"], payload_sha256=digest(publication), received_at=NOW,
        episode_id=setup_candidate.episode_id, security_id=UUID(setup_candidate.security_id), ticker="AAPL", policy=source_policy)
    reference = replace(security(), security_id=setup.source.security_id)
    payload = snapshot_payload()
    shift = NOW - STOCK_NOW
    for field in ("market_time", "computed_at", "available_at", "valid_until"):
        payload[field] = datetime.fromisoformat(payload[field]) + shift
    payload.update(ticker="AAPL", security_id=str(reference.security_id), security_revision_id=str(reference.security_revision_id))
    for component in payload["components"]:
        component["metrics"][0]["value"] *= direction
        for origin in component["sources"]:
            origin["security_id"] = str(reference.security_id)
            for field in ("market_time", "observed_at", "recorded_at", "received_at", "valid_until"):
                origin[field] = datetime.fromisoformat(origin[field]) + shift
    payload["components"].append(BehaviorComponent(factor="PARTICIPATION", interval="1d", status="READY",
        sources=(payload["components"][0]["sources"][0],), metrics=(BehaviorMetric(
            definition=next(row for row in METRICS if row.metric_id == "median_dollar_volume20"), interval="1d",
            status="READY", value=30000000., sample_count=21),)).model_dump(mode="json"))
    stock = StockBehaviorSnapshot.model_validate(payload)
    valuation = configured_valuation_policy()
    market_time = NOW - timedelta(minutes=1)
    batch_id = uuid4()
    snapshots = []
    references = []
    bars = []
    legs = []
    count = 2 if "VERTICAL" in structure else 1
    for index in range(count):
        strike = 100 + direction * index * 5
        contract_type = ContractType.CALL if direction == 1 else ContractType.PUT
        observed = market_time + timedelta(seconds=15)
        item = replace(snapshot(index + 1, str(strike), "5" if index == 0 else "2", .25, contract_type),
            underlyer="AAPL", batch_id=batch_id, market_data_time=market_time, mark_market_data_time=market_time,
            spot_market_data_time=market_time, first_observed_at=observed,
            valuation_policy_sha256=valuation.policy_sha256, valuation_policy_version=valuation.policy_version)
        snapshots.append(item)
        references.append(OptionContractReference(contract_ticker=item.contract_ticker, underlyer="AAPL", asset_type=AssetType.STOCK,
            provider="polygon", provider_version=None, provider_contract_type=item.contract_type.value,
            expiration_date=item.expiration_date, strike=item.strike, provider_exercise_style="AMERICAN", shares_per_contract=100,
            primary_exchange=None, correction=None, additional_underlyings_json="[]", adjustment_metadata_json="{}",
            changes_deliverables=False, valid_from=NOW - timedelta(days=1), valid_to=None,
            first_observed_at=NOW - timedelta(days=1), revised_observed_at=None, refreshed_at=NOW - timedelta(days=1), payload_sha256="a" * 64))
        bars.append(replace(bar(market_time - timedelta(minutes=1), 100, 101, 99, 100),
            security_id=reference.security_id, interval="1m", bar_end=market_time, system_observed_at=observed))
        legs.append(_leg(item, index, OptionSide.BUY if index == 0 else OptionSide.SELL))
    option = replace(package_candidate(tuple(legs), StructureType(structure)), legs=tuple(legs),
        underlyer="AAPL", strategy_name="DIRECTIONAL_DEBIT_SPREAD" if count == 2 else "DIRECTIONAL_LONG_PREMIUM",
        observed_time=market_time + timedelta(seconds=30), primary_evidence={"directional_thesis": "BULLISH" if direction == 1 else "BEARISH"})
    package = assess_option_package(option, valuation_policy_sha256=valuation.policy_sha256)
    manifest = research_manifest(source_policy=source_policy,
        detector_policy=DirectHourlyAcceptancePolicy(setup_source_policy_sha256=source_policy.sha256), valuation_policy_sha256=valuation.policy_sha256)
    lineage = OptionMatrixAssessmentLineage(option.matrix_id, "AAPL", option.market_data_time,
        option.observed_time, option.market_data_time, "c" * 64, "d" * 64, "d" * 64)
    return dict(manifest=manifest, expected_manifest_sha256=manifest.sha256, candidate=option,
        package_assessment=package, stock=stock, setup=setup, security=reference, lineage=lineage,
        snapshots=tuple(snapshots), references=tuple(references), raw_bars=tuple(bars),
        raw_bar_created_ats=tuple(market_time + timedelta(seconds=20) for _ in bars),
        source_received_at=NOW, decision_at=NOW, planned_entry_at=NOW + timedelta(seconds=1),
        entry_deadline=NOW + timedelta(minutes=1), planned_exit_at=NOW + timedelta(hours=1), valuation_policy=valuation)


@pytest.mark.parametrize("structure", ["LONG_CALL", "LONG_PUT", "CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL"])
def test_setup_binding_freezes_exact_package_and_same_session_horizon(structure):
    from options.stock_setup_binding import SetupPackageBinding, bind_setup_option_package

    inputs = binding_inputs(structure)
    result = bind_setup_option_package(**inputs)
    assert result.package_assessment == inputs["package_assessment"]
    assert result.stock_assessment.disposition == "ELIGIBLE_RESEARCH"
    assert result.entry_geometry_status == "PASS"
    assert result.assessment_mode == "LATER_RESEARCH_ASSESSMENT"
    assert result.execution_permission is False
    assert SetupPackageBinding.model_validate_json(result.canonical_json()) == result


@pytest.mark.parametrize("metadata", [{"cfi": "OCASPS"}, {"cfi": "OCASPS", "correction": 1}])
def test_setup_binding_preserves_standard_reference_metadata(metadata):
    import json
    from dataclasses import asdict, replace
    from options.detector_collection import retained_contract_reference
    from options.stock_setup_binding import bind_setup_option_package

    inputs = binding_inputs()
    original = replace(inputs["references"][0], adjustment_metadata_json=json.dumps(metadata),
        correction=str(metadata["correction"]) if "correction" in metadata else None)
    row = asdict(original)
    row.update(underlying=row.pop("underlyer"), eligibility_status="VALIDATED_ACTIVE",
        additional_underlyings=[], adjustment_metadata=metadata)
    reference = retained_contract_reference(row)
    assert json.loads(reference.adjustment_metadata_json) == metadata
    assert reference.payload_sha256 == original.payload_sha256
    inputs["references"] = (reference,)
    assert bind_setup_option_package(**inputs).entry_geometry_status == "PASS"
    for changes in ({"shares_per_contract": 10}, {"provider_exercise_style": "EUROPEAN"},
            {"additional_underlyings": [{"ticker": "OTHER"}]}, {"changes_deliverables": True},
            {"eligibility_status": "REJECTED_UNSUPPORTED"}, {"adjustment_metadata": {"cash_deliverable": 10}}):
        with pytest.raises(ValueError):
            retained_contract_reference({**row, **changes})


@pytest.mark.parametrize("mutation", ["manifest", "candidate", "matrix", "identity", "deliverable", "unknown_adjustment", "additional_deliverable", "spot", "snapshot",
    "late_reference", "adjusted", "late_bar", "entry_expiry", "overnight", "exit_expiry", "faked_original", "tamper", "stale_mark"])
def test_setup_binding_rejects_incoherent_package_or_horizon(mutation):
    from dataclasses import replace
    from decimal import Decimal
    from uuid import uuid4
    from options.stock_setup_binding import SetupPackageBinding, bind_setup_option_package

    inputs = binding_inputs()
    if mutation == "manifest": inputs["expected_manifest_sha256"] = "0" * 64
    if mutation == "candidate": inputs["candidate"] = replace(inputs["candidate"], identity_sha256="0" * 64)
    if mutation == "matrix": inputs["lineage"] = replace(inputs["lineage"], matrix_id=uuid4())
    if mutation == "identity": inputs["security"] = replace(inputs["security"], security_id=uuid4())
    if mutation == "deliverable": inputs["references"] = (replace(inputs["references"][0], changes_deliverables=True),)
    if mutation == "unknown_adjustment": inputs["references"] = (replace(inputs["references"][0], adjustment_metadata_json='{"cash_deliverable":10}'),)
    if mutation == "additional_deliverable": inputs["references"] = (replace(inputs["references"][0], additional_underlyings_json='[{"ticker":"OTHER"}]'),)
    if mutation == "spot": inputs["raw_bars"] = (replace(inputs["raw_bars"][0], close_price=Decimal("99")),)
    if mutation == "snapshot": inputs["snapshots"] = (replace(inputs["snapshots"][0], model_mark=Decimal("6")),)
    if mutation == "late_reference": inputs["references"] = (replace(inputs["references"][0], revised_observed_at=NOW + timedelta(seconds=1)),)
    if mutation == "adjusted": inputs["raw_bars"] = (replace(inputs["raw_bars"][0], adjusted=True),)
    if mutation == "late_bar": inputs["raw_bar_created_ats"] = (NOW + timedelta(seconds=1),)
    if mutation == "entry_expiry": inputs["entry_deadline"] = inputs["setup"].source.valid_until
    if mutation == "overnight": inputs["planned_exit_at"] += timedelta(days=1)
    if mutation == "exit_expiry": inputs["planned_exit_at"] = inputs["snapshots"][0].expiration_cutoff
    if mutation == "stale_mark": inputs["decision_at"] += timedelta(hours=1)
    if mutation in ("faked_original", "tamper"):
        result = bind_setup_option_package(**inputs).model_dump(mode="json")
        if mutation == "faked_original": result["assessment_mode"] = "ORIGINAL_CANDIDATE_TIME"
        else: result["stock_assessment"]["checks"][0]["verdict"] = "FAIL"
        with pytest.raises(ValueError):
            SetupPackageBinding.model_validate(result)
    else:
        with pytest.raises(ValueError):
            bind_setup_option_package(**inputs)


@pytest.mark.parametrize("mutation", ["batch", "order", "reference_type"])
def test_setup_binding_rejects_cross_leg_source_drift(mutation):
    from dataclasses import replace
    from uuid import uuid4
    from options.stock_setup_binding import bind_setup_option_package

    inputs = binding_inputs("CALL_DEBIT_VERTICAL")
    if mutation == "batch": inputs["snapshots"] = (inputs["snapshots"][0], replace(inputs["snapshots"][1], batch_id=uuid4()))
    if mutation == "order": inputs["snapshots"] = tuple(reversed(inputs["snapshots"]))
    if mutation == "reference_type": inputs["references"] = (replace(inputs["references"][0], provider_contract_type="PUT"), inputs["references"][1])
    with pytest.raises(ValueError):
        bind_setup_option_package(**inputs)


def test_in_memory_binding_validation_never_recalculates_stock_gates(monkeypatch):
    from options.stock_setup_binding import SetupPackageBinding, bind_setup_option_package

    binding = bind_setup_option_package(**binding_inputs())
    def reject_evaluation(*_args, **_kwargs):
        pytest.fail("binding validation cannot execute stock assessment")
    monkeypatch.setattr("options.stock_setup_binding.evaluate_hourly_acceptance", reject_evaluation)
    monkeypatch.setattr("options.stock_setup_binding.entry_gate", reject_evaluation)
    assert SetupPackageBinding.model_validate_json(binding.canonical_json()) == binding