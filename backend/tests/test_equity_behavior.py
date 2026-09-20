from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

from equity.behavior import BehaviorComponent, BehaviorMetric, BehaviorSource, METRICS, OPTIONS_SWING_PROFILE, StockBehaviorSnapshot


NOW = datetime(2026, 9, 17, 15, 16, tzinfo=timezone.utc)
SECURITY = uuid4()


def component(interval="1d", state="UP"):
    return BehaviorComponent(factor="TREND", interval=interval, status="READY", state=state,
        classification_policy_sha256="a" * 64,
        metrics=(BehaviorMetric(definition=METRICS[0], interval=interval, status="READY", value=0.12, sample_count=200),
             BehaviorMetric(definition=METRICS[1], interval=interval, status="READY", value=25.0, sample_count=200)),
        sources=(BehaviorSource(evidence_id=uuid4(), security_id=SECURITY, interval=interval,
            payload_sha256="b" * 64, policy_sha256="c" * 64, market_time=NOW - timedelta(minutes=16),
            observed_at=NOW - timedelta(minutes=1), recorded_at=NOW, received_at=NOW,
            valid_until=NOW + timedelta(hours=1), price_basis="RAW_ACTION_GATED", availability_mode="PROSPECTIVE_RECEIPT"),))


def snapshot_payload():
    return StockBehaviorSnapshot(security_id=SECURITY, security_revision_id=uuid4(), ticker="SPY",
        policy_sha256=OPTIONS_SWING_PROFILE.sha256, market_time=NOW - timedelta(minutes=16), computed_at=NOW,
        available_at=NOW, valid_until=NOW + timedelta(minutes=30), availability_mode="PROSPECTIVE_RECEIPT",
        components=tuple(component(interval) for interval in ("1d", "1h", "30m")),
        required_components=("TREND.1d", "TREND.1h", "TREND.30m")).model_dump(mode="json")


def test_behavior_roundtrip_is_immutable_versioned_and_not_permission():
    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    assert StockBehaviorSnapshot.model_validate_json(snapshot.canonical_json()) == snapshot
    assert len(snapshot.sha256) == 64 and snapshot.snapshot_id == StockBehaviorSnapshot.model_validate_json(snapshot.canonical_json()).snapshot_id
    assessment = snapshot.assess_at(NOW)
    assert assessment.data_status == "READY" and assessment.alignment_state == "ALIGNED_UP"
    assert snapshot.execution_permission is False
    with pytest.raises(ValidationError):
        snapshot.ticker = "AAPL"


def test_version_dispatch_preserves_v1_when_default_catalog_changes(monkeypatch):
    from equity import behavior

    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    original = snapshot.canonical_json()
    monkeypatch.setattr(behavior, "METRICS", ())
    monkeypatch.setattr(behavior, "DEFINITION_SHA256", "0" * 64)
    restored = behavior.load_stock_behavior_snapshot(original)
    assert restored.canonical_json() == original
    assert restored.snapshot_id == snapshot.snapshot_id
    assert behavior.resolve_behavior_profile(
        snapshot.profile, snapshot.definition_sha256, snapshot.policy_sha256,
    ) is OPTIONS_SWING_PROFILE
    with pytest.raises(ValueError, match="schema/definition"):
        behavior.load_stock_behavior_snapshot(original.replace("stock_behavior_v1", "stock_behavior_v2"))
    with pytest.raises(ValueError, match="registered"):
        behavior.resolve_behavior_profile(snapshot.profile, "0" * 64, snapshot.policy_sha256)


def test_snapshot_identity_ignores_only_retry_receipt_and_assembly_clocks():
    first = StockBehaviorSnapshot.model_validate(snapshot_payload())
    payload = first.model_dump(mode="python")
    retry_time = NOW + timedelta(seconds=1)
    payload.update(computed_at=retry_time, available_at=retry_time)
    for component in payload["components"]:
        for source in component["sources"]:
            source["received_at"] = retry_time
    repeated = StockBehaviorSnapshot.model_validate(payload)

    assert repeated.snapshot_id == first.snapshot_id
    assert repeated.identity_sha256 == first.identity_sha256
    assert repeated.sha256 != first.sha256

    changed = payload.copy()
    changed["components"] = [dict(component) for component in payload["components"]]
    changed["components"][0] = dict(changed["components"][0])
    changed["components"][0]["metrics"] = [dict(metric) for metric in changed["components"][0]["metrics"]]
    changed["components"][0]["metrics"][0]["value"] = 0.13
    changed = StockBehaviorSnapshot.model_validate(changed)
    assert changed.snapshot_id != first.snapshot_id


@pytest.mark.parametrize("change", ["future", "identity", "expired", "reconstructed", "definition", "unit", "short", "nan", "duplicate", "missing_requirement", "extend"])
def test_behavior_rejects_incompatible_or_noncausal_inputs(change):
    payload = snapshot_payload()
    source = payload["components"][0]["sources"][0]
    metric = payload["components"][0]["metrics"][0]
    if change == "future": source["received_at"] = (NOW + timedelta(seconds=1)).isoformat()
    if change == "identity": source["security_id"] = str(uuid4())
    if change == "expired": source["valid_until"] = NOW.isoformat()
    if change == "reconstructed": source["availability_mode"] = "RECONSTRUCTED"
    if change == "definition": metric["definition"]["formula_id"] = "unversioned_atr"
    if change == "unit": metric["definition"]["unit"] = "FRACTION"
    if change == "short": metric["sample_count"] = 20
    if change == "nan": metric["value"] = float("nan")
    if change == "duplicate": payload["components"].append(payload["components"][0])
    if change == "missing_requirement": payload["required_components"].append("VOLATILITY.1d")
    if change == "extend": payload["valid_until"] = (NOW + timedelta(hours=2)).isoformat()
    with pytest.raises(ValidationError):
        StockBehaviorSnapshot.model_validate(payload)


def test_mixed_flat_and_missing_are_distinct_and_do_not_change_data_readiness():
    payload = snapshot_payload()
    payload["components"][1]["state"] = "DOWN"
    mixed = StockBehaviorSnapshot.model_validate(payload)
    assert mixed.assess_at(NOW).alignment_state == "MIXED" and mixed.assess_at(NOW).data_status == "READY"
    for item in payload["components"]:
        item["state"] = "FLAT"
    assert StockBehaviorSnapshot.model_validate(payload).assess_at(NOW).alignment_state == "ALL_FLAT"
    payload["components"][1].update(status="UNAVAILABLE", state=None, metrics=[], sources=[], reason_codes=["MISSING_SOURCE"])
    partial = StockBehaviorSnapshot.model_validate(payload)
    assert partial.assess_at(NOW).alignment_state == "PARTIAL" and partial.assess_at(NOW).data_status == "PARTIAL"


def test_optional_missing_component_does_not_expire_required_profile():
    payload = snapshot_payload()
    payload["components"].append(dict(factor="PARTICIPATION", interval="30m", status="UNAVAILABLE", reason_codes=["SAME_TIME_VOLUME_NOT_ASSEMBLED"]))
    assert StockBehaviorSnapshot.model_validate(payload).assess_at(NOW).data_status == "READY"


@pytest.mark.parametrize("change", ["schema", "extra", "naive", "future_market", "publication", "adjustment", "provider_adjustment", "wrong_factor_state"])
def test_behavior_rejects_unsupported_contracts_and_ambiguous_sources(change):
    payload = snapshot_payload()
    source = payload["components"][0]["sources"][0]
    if change == "schema": payload["schema_version"] = "stock_behavior_v2"
    if change == "extra": payload["probability"] = .99
    if change == "naive": payload["available_at"] = NOW.replace(tzinfo=None).isoformat()
    if change == "future_market": source.update(market_time=NOW.isoformat())
    if change == "publication": source["published_at"] = (NOW + timedelta(seconds=1)).isoformat()
    if change == "adjustment": source["price_basis"] = "REVIEWED_SPLIT_ADJUSTED"
    if change == "provider_adjustment": source["price_basis"] = "PROVIDER_SPLIT_ADJUSTED"
    if change == "wrong_factor_state": payload["components"][0]["state"] = "INSIDE_RANGE"
    with pytest.raises(ValidationError):
        StockBehaviorSnapshot.model_validate(payload)


@pytest.mark.parametrize("mismatch", [False, True])
def test_relative_strength_requires_paired_distinct_benchmark_evidence(mismatch):
    payload = snapshot_payload()
    benchmark = uuid4()
    source = dict(payload["components"][0]["sources"][0])
    benchmark_source = {**source, "security_id": str(benchmark), "evidence_id": str(uuid4())}
    if mismatch:
        benchmark_source["market_time"] = (NOW - timedelta(days=1)).isoformat()
    payload["components"].append(dict(factor="RELATIVE_STRENGTH", interval="1d", status="READY",
        metrics=[BehaviorMetric(definition=METRICS[-1], interval="1d", status="READY", value=.01,
                                sample_count=21, benchmark_security_id=benchmark).model_dump(mode="json")],
        sources=[source, benchmark_source]))
    if mismatch:
        with pytest.raises(ValidationError, match="paired market"):
            StockBehaviorSnapshot.model_validate(payload)
    else:
        assert StockBehaviorSnapshot.model_validate(payload).assess_at(NOW).data_status == "READY"


def test_behavior_contract_exposes_machine_readable_schema_without_legacy_domain_changes():
    from equity.domain import EquityContextSnapshot
    schema = StockBehaviorSnapshot.model_json_schema()
    assert schema["additionalProperties"] is False
    assert schema["properties"]["schema_version"]["const"] == "stock_behavior_v1"
    assert "behavior_payload" not in EquityContextSnapshot.__dataclass_fields__


def test_zero_is_valid_but_missing_is_not_zero():
    metric = BehaviorMetric(definition=METRICS[0], interval="1d", status="READY", value=0.0, sample_count=200)
    assert metric.value == 0
    with pytest.raises(ValidationError):
        BehaviorMetric(definition=METRICS[0], interval="1d", status="UNAVAILABLE", value=0.0, sample_count=200, reason_codes=("MISSING",))


def stored_behavior(snapshot):
    return dict(equity_context_snapshot_id=snapshot.snapshot_id, security_id=snapshot.security_id,
        security_revision_id=snapshot.security_revision_id, ticker=snapshot.ticker, strategy_horizon=snapshot.profile,
        context_policy_sha256=snapshot.policy_sha256, market_time=snapshot.market_time, created_at=snapshot.available_at,
        behavior_schema_version=snapshot.schema_version, behavior_definition_sha256=snapshot.definition_sha256,
        behavior_computed_at=snapshot.computed_at, observed_at=snapshot.available_at,
        valid_until=snapshot.valid_until, behavior_payload_text=snapshot.canonical_json(), behavior_payload_sha256=snapshot.sha256)


@pytest.mark.parametrize("mutation", [None, "hash", "column", "late_created", "wrong_security", "expired"])
def test_exact_behavior_reader_validates_payload_identity_and_receipt(mutation):
    from equity.domain import DecisionWatermark
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    row = stored_behavior(snapshot)
    if mutation == "hash": row["behavior_payload_sha256"] = "0" * 64
    if mutation == "column": row["observed_at"] = NOW - timedelta(seconds=1)
    if mutation == "late_created": row["created_at"] = NOW + timedelta(seconds=1)
    if mutation == "wrong_security": row["security_id"] = uuid4()
    decision = NOW + timedelta(hours=1) if mutation == "expired" else NOW
    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.side_effect = [{"ready": True}, row]
    arguments = dict(profile=snapshot.profile, definition_sha256=snapshot.definition_sha256, policy_sha256=snapshot.policy_sha256)
    with patch(
        "equity.behavior_sources.snapshot_uses_current_source_policies",
        return_value=True,
    ):
        if mutation:
            with pytest.raises(ValueError):
                repository.get_behavior_as_of(SECURITY, DecisionWatermark(snapshot.market_time, decision), **arguments)
        else:
            assert repository.get_behavior_as_of(SECURITY, DecisionWatermark(snapshot.market_time, decision), **arguments) == snapshot
    sql, parameters = cursor.execute.call_args.args
    assert "created_at <= %s" in sql and "valid_until > %s" in sql
    assert "strategy_horizon = %s" in sql and "behavior_profile" not in sql
    assert "behavior_definition_sha256 = %s" in sql and "PROSPECTIVE_RECEIPT" in sql
    assert parameters[:4] == (SECURITY, snapshot.profile, snapshot.definition_sha256, snapshot.policy_sha256)


def test_behavior_reader_without_migration_does_not_query_new_columns():
    from equity.behavior import DEFINITION_SHA256
    from equity.domain import DecisionWatermark
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.return_value = {"ready": False}
    assert repository.get_behavior_as_of(SECURITY, DecisionWatermark(NOW, NOW), profile="OPTIONS_SWING_V1",
        definition_sha256=DEFINITION_SHA256, policy_sha256=OPTIONS_SWING_PROFILE.sha256) is None
    assert cursor.execute.call_count == 3


def test_behavior_reader_rejects_superseded_source_policy():
    from equity.domain import DecisionWatermark
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.side_effect = [{"ready": True}, stored_behavior(snapshot)]
    with patch(
        "equity.behavior_sources.snapshot_uses_current_source_policies",
        return_value=False,
    ):
        assert repository.get_behavior_as_of(
            snapshot.security_id,
            DecisionWatermark(snapshot.market_time, snapshot.available_at),
            profile=snapshot.profile,
            definition_sha256=snapshot.definition_sha256,
            policy_sha256=snapshot.policy_sha256,
        ) is None


@pytest.mark.parametrize("migrated", [False, True])
def test_context_history_preserves_explicit_legacy_response_columns(migrated):
    from unittest.mock import patch
    from equity.api import equity_context_history
    from equity.repositories import LEGACY_CONTEXT_COLUMNS

    with patch("database.get_db_cursor") as connection:
        cursor = connection.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"ready": migrated}
        cursor.fetchall.return_value = []
        assert equity_context_history("SPY", "INTRADAY_30M", 1) == {"ticker": "SPY", "count": 0, "results": []}
    sql, parameters = cursor.execute.call_args.args
    assert "SELECT *" not in sql and "to_jsonb" not in sql
    assert ("context_kind = 'LEGACY'" in sql) is migrated
    assert all(column in sql.split("FROM")[0] for column in LEGACY_CONTEXT_COLUMNS)
    assert parameters == ["SPY", "INTRADAY_30M", 1]


@pytest.mark.parametrize("mutation", ["requirements", "policy", "metrics"])
def test_profile_cannot_self_declare_weaker_ready_requirements(mutation):
    payload = snapshot_payload()
    if mutation == "requirements":
        payload["required_components"] = ["TREND.1d"]
    if mutation == "policy":
        payload["policy_sha256"] = "d" * 64
    if mutation == "metrics":
        payload["components"][0]["metrics"].pop()
    with pytest.raises(ValidationError, match="profile"):
        StockBehaviorSnapshot.model_validate(payload)


def test_decision_assessment_masks_expired_optional_metrics_without_mutating_evidence():
    payload = snapshot_payload()
    optional_expiry = NOW + timedelta(minutes=5)
    source = {**payload["components"][0]["sources"][0], "valid_until": optional_expiry.isoformat()}
    payload["components"].append(dict(factor="PARTICIPATION", interval="1d", status="READY", state="ELEVATED",
        classification_policy_sha256="a" * 64, sources=[source], metrics=[
            BehaviorMetric(definition=METRICS[8], interval="1d", status="READY", value=2.0, sample_count=21).model_dump(mode="json")]))
    snapshot = StockBehaviorSnapshot.model_validate(payload)
    original_hash = snapshot.sha256
    before = snapshot.assess_at(optional_expiry - timedelta(microseconds=1))
    after = snapshot.assess_at(optional_expiry)
    assert before.components[-1].state == "ELEVATED" and before.components[-1].metrics
    assert after.components[-1].status == "STALE" and after.components[-1].state is None
    assert after.components[-1].metrics == () and after.components[-1].reason_codes
    assert after.data_status == "READY" and after.alignment_state == "ALIGNED_UP"
    assert snapshot.sha256 == original_hash and snapshot.components[-1].status == "READY"
    for decision in (NOW - timedelta(microseconds=1), snapshot.valid_until):
        assessment = snapshot.assess_at(decision)
        assert assessment.data_status == "UNAVAILABLE" and assessment.alignment_state == "PARTIAL"
        assert all(component.state is None and not component.metrics for component in assessment.components)
    with pytest.raises(ValueError, match="timezone-aware"):
        snapshot.assess_at(NOW.replace(tzinfo=None))


def stored_source_rows(snapshot):
    rows = []
    seen = set()
    for component in snapshot.components:
        for source in component.sources:
            if source.evidence_id in seen:
                continue
            seen.add(source.evidence_id)
            rows.append({
                "evidence_id": source.evidence_id,
                "evidence_key": f"behavior:{source.evidence_id}",
                "lifecycle_key": f"behavior:{snapshot.ticker}:{source.interval}",
                "evidence_type": "FEATURE_SNAPSHOT", "evidence_role": "REGIME",
                "security_id": source.security_id, "ticker": snapshot.ticker,
                "interval": source.interval, "market_time": source.market_time,
                "direction": None, "lifecycle_status": "SNAPSHOT", "strength": None,
                "observed_at": source.observed_at, "valid_until": source.valid_until,
                "source_name": "STOCK_BEHAVIOR_RAW_SOURCE",
                "source_version": "behavior_feature_source_v1",
                "payload_schema_version": "stock_behavior_raw_source_v1",
                "analysis_run_id": None, "latest_bar_revision_id": None,
                "security_revision_id": snapshot.security_revision_id,
                "fundamental_report_ids": (), "source_revision_ids": (),
                "quality_state": "COMPLETE", "quality_codes": (),
                "qualification_revision_id": None, "payload": {},
                "payload_sha256": source.payload_sha256,
                "created_at": source.recorded_at,
            })
    return rows


def derived_source_evidence(snapshot):
    from equity.domain import EquityEvidence, EvidenceRole, EvidenceType, LifecycleStatus, QualityState

    return tuple(EquityEvidence(
        evidence_id=row["evidence_id"], evidence_key=f"behavior:{row['evidence_id']}",
        lifecycle_key=f"behavior:{snapshot.ticker}:{row['interval']}",
        evidence_type=EvidenceType.FEATURE_SNAPSHOT, evidence_role=EvidenceRole.REGIME,
        security_id=row["security_id"], ticker=row["ticker"], interval=row["interval"],
        direction=None, lifecycle_status=LifecycleStatus.SNAPSHOT, strength=None,
        market_time=row["market_time"], observed_at=row["observed_at"],
        valid_until=row["valid_until"], source_name=row["source_name"],
        source_version=row["source_version"], payload_schema_version=row["payload_schema_version"],
        analysis_run_id=None, latest_bar_revision_id=None,
        security_revision_id=row["security_revision_id"], fundamental_report_ids=(),
        source_revision_ids=(), quality_state=QualityState.COMPLETE, quality_codes=(),
        qualification_revision_id=None, payload_json="{}", payload_sha256=row["payload_sha256"],
    ) for row in stored_source_rows(snapshot))


def test_behavior_writer_is_migration_gated_before_snapshot_factory():
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.return_value = {"ready": False}
    factory = MagicMock()
    with pytest.raises(RuntimeError, match="migration 044"):
        repository.persist_behavior(factory)
    factory.assert_not_called()
    assert "behavior_payload_text" in cursor.execute.call_args.args[0]


def test_behavior_writer_verifies_sources_and_inserts_snapshot_with_exact_links():
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    evidence = derived_source_evidence(snapshot)
    rows = stored_source_rows(snapshot)
    repository, connection, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.side_effect = [
        {"ready": True}, None,
        {"equity_context_snapshot_id": snapshot.snapshot_id},
    ]
    cursor.fetchall.side_effect = [rows, rows]
    with patch("equity.repositories.execute_values") as execute_values:
        stored, inserted = repository.persist_behavior(
            lambda clocks: snapshot, derived_evidence=evidence,
        )

    assert stored == snapshot and inserted is True and connection.commit.call_count == 1
    assert execute_values.call_count == 2
    link_rows = execute_values.call_args_list[1].args[2]
    assert [row[1] for row in link_rows] == sorted(
        {source.evidence_id for component in snapshot.components for source in component.sources},
        key=str,
    )
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("context_kind" in statement and "STOCK_BEHAVIOR" in statement for statement in statements)
    assert any("lock_timeout" in statement for statement in statements)


def test_behavior_writer_returns_original_payload_for_retry_only_clock_changes():
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    original = StockBehaviorSnapshot.model_validate(snapshot_payload())
    payload = original.model_dump(mode="python")
    retry_time = NOW + timedelta(seconds=1)
    payload.update(computed_at=retry_time, available_at=retry_time)
    for component in payload["components"]:
        for source in component["sources"]:
            source["received_at"] = retry_time
    retry = StockBehaviorSnapshot.model_validate(payload)
    evidence = derived_source_evidence(original)
    source_rows = stored_source_rows(original)
    links = [{"evidence_id": row["evidence_id"]} for row in sorted(source_rows, key=lambda row: str(row["evidence_id"]))]
    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.side_effect = [{"ready": True}, {"behavior_payload_text": original.canonical_json()}]
    cursor.fetchall.side_effect = [source_rows, source_rows, links]

    with patch("equity.repositories.execute_values"):
        stored, inserted = repository.persist_behavior(
            lambda clocks: retry, derived_evidence=evidence,
        )

    assert stored == original and inserted is False
    assert retry.snapshot_id == original.snapshot_id and retry.sha256 != original.sha256


def test_behavior_writer_uses_database_recording_clock_for_derived_evidence():
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    base = StockBehaviorSnapshot.model_validate(snapshot_payload())
    evidence = derived_source_evidence(base)
    recorded_at = NOW + timedelta(milliseconds=1)
    source_rows = [row | {"created_at": recorded_at} for row in stored_source_rows(base)]
    expected_clocks = {row.evidence_id: recorded_at for row in evidence}

    def factory(clocks):
        assert clocks == expected_clocks
        payload = base.model_dump(mode="python")
        for component in payload["components"]:
            component["sources"][0]["recorded_at"] = recorded_at
            component["sources"][0]["received_at"] = recorded_at
        payload["computed_at"] = recorded_at
        payload["available_at"] = recorded_at
        return StockBehaviorSnapshot.model_validate(payload)

    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.side_effect = [
        {"ready": True}, None,
        {"equity_context_snapshot_id": base.snapshot_id},
    ]
    cursor.fetchall.side_effect = [source_rows, source_rows]
    with patch("equity.repositories.execute_values") as execute_values:
        stored, inserted = repository.persist_behavior(factory, derived_evidence=evidence)
    assert inserted is True and stored.components[0].sources[0].recorded_at == recorded_at
    assert execute_values.call_count == 2


def test_behavior_writer_rejects_source_payload_mismatch():
    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    evidence = derived_source_evidence(snapshot)
    rows = stored_source_rows(snapshot)
    rows[0] = rows[0] | {"payload_sha256": "0" * 64}
    repository, _, cursor = _repository(EquityEvidenceRepository)
    cursor.fetchone.return_value = {"ready": True}
    cursor.fetchall.side_effect = [stored_source_rows(snapshot), rows]
    with pytest.raises(ValueError, match="stored evidence"):
        with patch("equity.repositories.execute_values"):
            repository.persist_behavior(lambda clocks: snapshot, derived_evidence=evidence)


def test_behavior_writer_rejects_legacy_source_evidence():
    from dataclasses import replace

    from equity.repositories import EquityEvidenceRepository
    from test_equity_materialization_repositories import _repository

    snapshot = StockBehaviorSnapshot.model_validate(snapshot_payload())
    evidence = list(derived_source_evidence(snapshot))
    evidence[0] = replace(
        evidence[0], source_name="EQUITY_FEATURES", source_version="equity_features_v1",
        payload_schema_version="1.0",
    )
    repository, _, _ = _repository(EquityEvidenceRepository)
    with pytest.raises(ValueError, match="dedicated immutable evidence"):
        repository.persist_behavior(lambda clocks: snapshot, derived_evidence=evidence)