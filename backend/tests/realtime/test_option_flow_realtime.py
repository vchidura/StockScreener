import ast
import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi.testclient import TestClient
import pytest

from options.discovery import CONTRACT_FILTERS, EligibleChainQuery
from options.analytics.eligible_chain_projection import query_stored_eligible_chain
from options.analytics.flow_projection import assemble_option_flow
from options.calendar import OptionExchangeCalendar
from options.data.polygon_chain_discovery import (
    PolygonOptionChainDiscoveryAdapter, PolygonOptionChainDiscoveryLimits, build_polygon_option_chain_discovery_plan,
)
from options.data.polygon_http import PolygonHttpResponse
from options.data.normalizer import parse_polygon_snapshot
from options.repositories.daily_facts import DailyOpenInterestRecord
from realtime.contracts import canonical_sha256
from realtime.forward_ingestion import ForwardOpenInterestEvidence, OfflineForwardPublicationSlot, admit_developer_matrix, normalize_developer_matrix
from realtime.option_discovery_repository import RealtimeOptionDiscoveryEvidence
from realtime.option_flow_projection import DeveloperFlowChainEvidence, FlowOpenInterestPair, seal_offline_option_flow_publication
from realtime.option_screener_projection import seal_offline_eligible_chain_publication
from realtime.preflight import ProviderTier
from test_forward_ingestion import cycle
from options.strategies.registry import build_discovery_catalog
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.portal_compatibility_repository import (
    RealtimeOptionFlowRepository,
    RealtimeOptionScreenerRepository,
    RealtimeOptionsAlertsPageRepository,
)


NOW = datetime(2026, 10, 7, 20, 15, tzinfo=timezone.utc)
COHORT_ID = UUID("d1baee6f-4ab2-543c-8a86-ce10f8952e69")


class OptionFlowStore:
    def __init__(self, option_underlying_count=0, active=True):
        self.option_underlying_count = option_underlying_count
        self.active = active
        self.statements = []
        self.result = []

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params=()):
        self.statements.append(sql)
        self.result = []
        if sql.startswith("SET"):
            return
        if sql.startswith("SELECT release.* FROM rt.developer_forward_releases"):
            return
        if sql.startswith("SELECT generation.cohort_id"):
            if self.active:
                self.result = [{
                    "cohort_id": COHORT_ID,
                    "provider_tier": "MASSIVE_DEVELOPER",
                    "effective_at": NOW,
                }]
            return
        if sql.startswith("SELECT manifest.option_underlying_count"):
            self.result = [{"option_underlying_count": self.option_underlying_count}]
            return
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self.result[0] if self.result else None


def option_flow_app(*, option_underlying_count=0, active=True):
    store = OptionFlowStore(option_underlying_count, active)
    flow_reader = RealtimeOptionFlowRepository(store.cursor)
    screener_reader = RealtimeOptionScreenerRepository(store.cursor)
    alert_reader = RealtimeOptionsAlertsPageRepository(store.cursor)
    app = create_realtime_portal_app(
        RealtimePortalRepositories(
            None, None, None,
            option_flow=flow_reader,
            option_screener=screener_reader,
            option_alert_views=alert_reader,
        ),
        clock=lambda: NOW,
    )
    return app, store


@pytest.mark.parametrize(("option_underlying_count", "reason"), [
    (0, "EQUITY_ONLY_COHORT"),
    (8, "NO_COMPLETE_MATRIX"),
])
def test_option_flow_returns_explicit_unavailable_envelope(option_underlying_count, reason):
    app, store = option_flow_app(option_underlying_count=option_underlying_count)
    with TestClient(app) as client:
        response = client.get("/api/options/flow", params={
            "cohort_id": str(COHORT_ID),
            "underlyer": "aapl",
            "session_date": "2026-10-06",
        })
    assert response.status_code == 200, response.text
    assert response.json() == {
        "available": False,
        "reason": reason,
        "data_tier": "15-MINUTE DELAYED RESEARCH DATA",
        "generated_at": NOW.isoformat(),
        "as_of": None,
        "observed_at": None,
        "policy_sha256": None,
        "model_version": None,
        "data": {
            "underlyers": [],
            "selected": "AAPL",
            "session_date": "2026-10-06",
        },
    }
    assert all("public." not in sql and "canonical" not in sql for sql in store.statements)
    assert all(sql.startswith(("SET", "SELECT")) for sql in store.statements)


def test_option_flow_requires_explicit_valid_cohort_and_underlying():
    app, store = option_flow_app()
    with TestClient(app) as client:
        assert client.get("/api/options/flow").status_code == 422
        response = client.get("/api/options/flow", params={
            "cohort_id": str(COHORT_ID), "underlyer": "not valid",
        })
        assert response.status_code == 422
    assert store.statements == []


def test_option_flow_does_not_substitute_an_unavailable_cohort():
    app, store = option_flow_app(active=False)
    with TestClient(app) as client:
        response = client.get("/api/options/flow", params={
            "cohort_id": str(COHORT_ID),
        })
    assert response.status_code == 200
    assert response.json()["reason"] == "ACTIVE_COHORT_UNAVAILABLE"
    assert response.json()["data"] == {
        "underlyers": [], "selected": "SPY", "session_date": None,
    }
    assert not any("subscription_manifests" in sql for sql in store.statements)


def test_option_screener_catalog_matches_the_pure_original_contract_without_io():
    app, store = option_flow_app()
    with TestClient(app) as client:
        response = client.get("/api/options/discovery-catalog", params={
            "cohort_id": str(COHORT_ID),
        })
        assert client.get("/api/options/discovery-catalog").status_code == 422
    assert response.status_code == 200
    assert response.json() == {
        **build_discovery_catalog(), "contract_filters": CONTRACT_FILTERS,
    }
    assert store.statements == []


@pytest.mark.parametrize(("option_underlying_count", "reason"), [
    (0, "EQUITY_ONLY_COHORT"),
    (8, "NO_ACTIVE_POLICY_ELIGIBLE_CHAIN"),
])
def test_option_screener_returns_explicit_unavailable_chain(option_underlying_count, reason):
    app, store = option_flow_app(option_underlying_count=option_underlying_count)
    request = {
        "session_date": "2026-10-06",
        "underlyer": "AAPL",
        "contract_type": "CALL",
        "filters": [{"field": "calendar_dte", "minimum": 7, "maximum": 45}],
        "sort": "day_volume",
        "descending": True,
        "limit": 100,
        "offset": 0,
    }
    with TestClient(app) as client:
        response = client.post(
            "/api/options/eligible-chain/query",
            params={"cohort_id": str(COHORT_ID)},
            json=request,
        )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["available"] is False
    assert result["reason"] == reason
    assert result["data"] == {
        "rows": [], "total": 0, "coverage": [],
        "requested_underlyers": ["AAPL"],
        "missing_underlyers": ["AAPL"],
        "coverage_status": "MISSING",
        "selection_basis": "LATEST_COMPLETE_MATRIX_PER_UNDERLYING",
        "serving_mode": "CURRENT_POLICY",
        "session_date": "2026-10-06", "limit": 100, "offset": 0,
        "filters": [{"field": "calendar_dte", "minimum": 7.0, "maximum": 45.0}],
        "quote_liquidity": "NOT_AVAILABLE",
        "execution_mode": "READ_ONLY_RESEARCH",
    }
    assert all(sql.startswith(("SET", "SELECT")) for sql in store.statements)


def test_option_screener_rejects_missing_cohort_and_invalid_body_without_io():
    app, store = option_flow_app()
    valid = {
        "filters": [], "sort": "day_volume", "descending": True,
        "limit": 100, "offset": 0,
    }
    with TestClient(app) as client:
        assert client.post("/api/options/eligible-chain/query", json=valid).status_code == 422
        invalid = client.post(
            "/api/options/eligible-chain/query",
            params={"cohort_id": str(COHORT_ID)},
            json={**valid, "sort": "unsupported"},
        )
        assert invalid.status_code == 422
    assert store.statements == []


def test_original_options_alerts_contracts_return_one_equity_only_verdict():
    app, store = option_flow_app()
    candidate = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    cohort = {"cohort_id": str(COHORT_ID)}
    with TestClient(app) as client:
        responses = [
            client.get("/api/options/alerts/datasets", params=cohort),
            client.get("/api/options/alerts/schedule", params=cohort),
            client.get("/api/options/alerts/evaluations", params={
                **cohort, "dataset_id": "frozen-v1", "detector": "O1",
            }),
            client.get("/api/options/alerts/detector-runs", params={
                **cohort, "dataset_id": "frozen-v1", "scope": "LATEST",
            }),
            client.get(f"/api/options/candidates/{candidate}", params=cohort),
            client.get("/api/options/quotes/current", params=[
                ("cohort_id", str(COHORT_ID)), ("contract_id", "11"), ("contract_id", "12"),
            ]),
            client.post(
                f"/api/options/alerts/preview/{candidate}", params=cohort, json={},
            ),
        ]
    assert all(response.status_code == 200 for response in responses)
    assert {response.json()["reason"] for response in responses} == {"EQUITY_ONLY_COHORT"}
    assert all(response.json()["available"] is False for response in responses)
    assert responses[0].json()["data"] == {
        "storage_ready": False, "datasets": [], "dataset_sessions": {},
        "sessions": [], "session_datasets": {}, "current_session_date": None,
        "current_dataset_id": None, "default_dataset_id": None,
        "configured_dataset_id": None, "configured_effective_from": None,
    }
    assert responses[5].json()["data"] == {
        "quotes": [], "requested_contract_ids": [11, 12],
        "missing_contract_ids": [11, 12],
        "source": "PERSISTED_ADVANCED_QUOTE_CURRENT",
        "provider_fetch_performed": False, "execution_permission": False,
    }
    assert all("public." not in sql and "canonical" not in sql for sql in store.statements)
    assert all(sql.startswith(("SET", "SELECT")) for sql in store.statements)


def test_options_alerts_require_cohort_and_preserve_request_validation():
    app, store = option_flow_app()
    candidate = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    with TestClient(app) as client:
        assert client.get("/api/options/alerts/datasets").status_code == 422
        assert client.get("/api/options/alerts/detector-runs").status_code == 422
        assert client.get("/api/options/quotes/current").status_code == 422
        invalid = client.post(
            f"/api/options/alerts/preview/{candidate}",
            params={"cohort_id": str(COHORT_ID)},
            json={"entry_deadline": "2026-10-08T15:00:00Z", "exit_deadline": "2026-10-08T14:00:00Z"},
        )
        assert invalid.status_code == 422
    assert store.statements == []


def original_flow_assembly_reference(summaries, *, requested="SPY", session_date=None, oi_changes=(),
                                    expirations=(), strikes=(), top_contracts=()):
    source = Path(__file__).resolve().parents[2] / "options" / "api.py"
    handler = next(node for node in ast.parse(source.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name == "option_flow")
    summary_loop = next(node for node in ast.walk(handler)
        if isinstance(node, ast.For) and isinstance(node.target, ast.Name) and node.target.id == "summary")
    returned = next(node for node in handler.body if isinstance(node, ast.Return))
    data = next(item.value for item in returned.value.keywords if item.arg == "data")
    namespace = dict(summaries=deepcopy(summaries), requested=requested, session_date=session_date,
        configuration=SimpleNamespace(policy_sha256="a" * 64, configuration_sha256="b" * 64),
        oi_by_underlying={}, expirations=deepcopy(list(expirations)), strikes=deepcopy(list(strikes)),
        top_contracts=deepcopy(list(top_contracts)), OptionExchangeCalendar=OptionExchangeCalendar,
        cursor=SimpleNamespace(fetchall=lambda: deepcopy(list(oi_changes))))
    oi_loop = next(node for node in ast.walk(handler) if isinstance(node, ast.For)
        and isinstance(node.target, ast.Name) and node.target.id == "row"
        and any(isinstance(part, ast.Assign) and isinstance(part.targets[0], ast.Subscript)
            and isinstance(part.targets[0].value, ast.Name) and part.targets[0].value.id == "oi_by_underlying"
            for part in node.body))
    exec(compile(ast.Module(body=[oi_loop], type_ignores=[]), str(source), "exec"), namespace)
    exec(compile(ast.Module(body=[summary_loop], type_ignores=[]), str(source), "exec"), namespace)
    if namespace["summaries"] and requested not in {row["underlying"] for row in namespace["summaries"]}:
        requested = namespace["requested"] = namespace["summaries"][0]["underlying"]
    namespace["selected_summary"] = next((row for row in namespace["summaries"] if row["underlying"] == requested), None)
    namespace["newest_market"] = max((row["market_data_time"] for row in namespace["summaries"]), default=None)
    return eval(compile(ast.Expression(body=data), str(source), "eval"), namespace)


@pytest.mark.parametrize("empty", [False, True])
def test_pure_flow_assembly_matches_original_contract_without_canonical_import(empty):
    summaries = [] if empty else [dict(underlying="SPY", batch_id=str(COHORT_ID), matrix_id=str(COHORT_ID),
        asset_type="ETF", scheduled_cycle=NOW, market_data_time=NOW, first_observed_at=NOW,
        received_row_count=2, retained_row_count=2, model_version="LOCAL_BSM_V1", policy_sha256="a" * 64,
        configuration_sha256="b" * 64, current_policy=True, spot=Decimal("100"), contract_count=2,
        expiration_count=1, strike_count=1, call_contract_count=1, put_contract_count=1,
        call_volume=200, put_volume=100, call_open_interest=100, put_open_interest=200,
        call_premium_activity=Decimal("50000"), put_premium_activity=Decimal("25000"), premium_activity_contract_count=2)]
    before = deepcopy(summaries)
    projection = assemble_option_flow(summaries=tuple(summaries), policy_sha256="a" * 64, configuration_sha256="b" * 64)
    assert projection.data == original_flow_assembly_reference(summaries)
    assert projection.available is not empty
    assert projection.reason == ("NO_COMPLETE_MATRIX" if empty else None)
    assert summaries == before
    projection.data["underlyers"].clear()
    assert summaries == before


def flow_chain_evidence(config, scope, matrix, *, inputs=None):
    members = inputs or matrix.ingress.inputs
    symbol = matrix.analysis.underlying.underlyer
    plan = build_polygon_option_chain_discovery_plan(symbol, scope.cutoff, Decimal("100"),
        maximum_dte=config.policy.contract_filter.maximum_dte, acquisition_maximum_dte=config.settings.acquisition_maximum_dte,
        strike_corridor_fraction=config.policy.contract_filter.strike_corridor_fraction)
    payload = dict(status="DELAYED", results=[json.loads(item.raw.raw_payload_json) for item in members])
    adapter = PolygonOptionChainDiscoveryAdapter(lambda *args: PolygonHttpResponse(200, json.dumps(payload).encode("utf-8")),
        base_url="https://api.polygon.io", limits=PolygonOptionChainDiscoveryLimits(40, 10000, 10485760, 67108864),
        clock=lambda: scope.available_after)
    result = adapter.discover(plan, batch_id=uuid5(NAMESPACE_URL, f"flow-discovery:{scope.sha256}:{symbol}"),
        policy_sha256=config.policy_sha256, started_at=scope.available_after)
    discovery = RealtimeOptionDiscoveryEvidence(scope.selection.scope, uuid5(NAMESPACE_URL, f"flow-security:{symbol}"), result)
    return DeveloperFlowChainEvidence(scope.sha256, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, discovery, tuple(members))


def flow_matrices(cycle):
    config, scope, sources = cycle
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config) for source in sources)
    evidence = tuple(flow_chain_evidence(config, scope, matrix) for matrix in matrices)
    return config, scope, matrices, evidence


def read_flow(publication):
    return publication.read(cohort_id=publication.cohort_id, provider_tier=publication.provider_tier,
        scope_sha256=publication.scope_sha256)


def test_full_policy_chain_produces_original_immutable_activity_flow_response(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    publication = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after, chain_evidence=evidence)
    payload = read_flow(publication)
    assert payload["available"] is True and payload["reason"] is None
    assert publication.coverage()["status"] == "COMPLETE"
    assert len(payload["data"]["underlyers"]) == 8
    assert payload["data"]["selected"] == "SPY"
    assert payload["data"]["selected_summary"]["total_volume"] == 400
    assert payload["data"]["selected_summary"]["open_interest_change"] is None
    assert len(payload["data"]["expirations"]) == len(payload["data"]["strikes"]) == 1
    assert len(payload["data"]["top_contracts"]) == 2
    assert payload["data"]["top_contracts"][0]["volume_open_interest_ratio"] == 2
    assert payload["data"]["directional_flow_available"] is False
    assert payload["data"]["quote_liquidity"] == "NOT_AVAILABLE"
    assert "schema_version" not in payload and "flow" not in payload and "screener" not in payload
    assert seal_offline_option_flow_publication(scope, tuple(reversed(matrices)), config,
        selected_at=scope.available_after, chain_evidence=tuple(reversed(evidence))) == publication
    payload["data"]["underlyers"].clear()
    assert len(read_flow(publication)["data"]["underlyers"]) == 8


def test_selected_manifests_without_full_chain_evidence_are_not_original_flow_success(cycle):
    config, scope, matrices, _ = flow_matrices(cycle)
    publication = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after)
    assert read_flow(publication)["available"] is False
    assert read_flow(publication)["reason"] == "POLICY_ELIGIBLE_CHAIN_COVERAGE_UNAVAILABLE"
    assert read_flow(publication)["data"]["underlyers"] == []
    assert publication.coverage()["status"] == "MISSING"


def oi_pairs(scope, matrices):
    calendar = OptionExchangeCalendar()
    observed_session = scope.selection.session_date
    current_settlement = calendar.previous_session(observed_session)
    prior_settlement = calendar.previous_session(current_settlement)
    prior_observed = calendar.session_open(current_settlement) + timedelta(minutes=15)
    pairs = []
    for matrix in matrices:
        for snapshot in matrix.snapshots:
            identity = uuid5(NAMESPACE_URL, f"flow-oi:{snapshot.contract_ticker}")
            current = ForwardOpenInterestEvidence(uuid5(identity, "current"),
                DailyOpenInterestRecord(snapshot.contract_id, current_settlement, snapshot.underlyer,
                    snapshot.open_interest, scope.available_after, observed_session, snapshot.batch_id),
                "PROVIDER_CHAIN_SNAPSHOT", scope.available_after)
            prior = ForwardOpenInterestEvidence(uuid5(identity, "prior"),
                DailyOpenInterestRecord(snapshot.contract_id, prior_settlement, snapshot.underlyer,
                    snapshot.open_interest - 10, prior_observed, current_settlement, uuid5(identity, "prior-batch")),
                "PROVIDER_CHAIN_SNAPSHOT", prior_observed)
            pairs.append(FlowOpenInterestPair(current, prior))
    return tuple(pairs)


def test_original_flow_oi_profiles_and_summary_assembly_parity(cycle):
    from options.analytics.flow_projection import option_flow_snapshot_rows

    config, scope, matrices, evidence = flow_matrices(cycle)
    publication = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after,
        chain_evidence=evidence, open_interest_pairs=oi_pairs(scope, matrices))
    payload = read_flow(publication)
    profiles = option_flow_snapshot_rows(next(matrix for matrix in matrices if matrix.analysis.underlying.underlyer == "SPY").snapshots)
    summaries, changes = [], []
    for row in payload["data"]["underlyers"]:
        summary = dict(row)
        summary.update(market_data_time=scope.cutoff, first_observed_at=scope.available_after,
            call_premium_activity=Decimal(summary["call_premium_activity"]), put_premium_activity=Decimal(summary["put_premium_activity"]))
        summaries.append(summary)
        changes.append(dict(row["open_interest_change"]))
    expected = original_flow_assembly_reference(summaries, session_date=scope.selection.session_date,
        oi_changes=changes, expirations=profiles["expirations"], strikes=profiles["strikes"], top_contracts=profiles["top_contracts"])
    expected["active_policy_sha256"] = config.policy_sha256
    expected["active_configuration_sha256"] = config.configuration_sha256
    expected = json.loads(json.dumps(expected, default=lambda value: value.isoformat() if isinstance(value, (date, datetime)) else str(value)))
    assert payload["data"] == expected
    change = payload["data"]["selected_summary"]["open_interest_change"]
    assert change["call_open_interest_change"] == change["put_open_interest_change"] == 10
    assert change["total_open_interest_change"] == 20
    assert change["matched_coverage_fraction"] == 1
    assert change["matched_contract_count"] == change["current_contract_count"] == 2
    assert payload["data"]["selected_summary"]["total_premium_activity"] == "100000.0"
    partial_pairs = oi_pairs(scope, matrices)[:1]
    partial = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after,
        chain_evidence=evidence, open_interest_pairs=partial_pairs)
    first = read_flow(partial)["data"]["underlyers"][0]
    assert first["open_interest_change"]["matched_coverage_fraction"] == 0.5
    assert read_flow(partial)["data"]["selected_summary"]["open_interest_change"] is None


def test_flow_partial_missing_coverage_and_original_selected_fallback(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    missing = "SPY"
    partial_matrices = tuple(matrix for matrix in matrices if matrix.analysis.underlying.underlyer != missing)
    partial_evidence = tuple(row for row in evidence if row.discovery.result.batch.underlyer != missing)
    declaration = ((missing, ("MATRIX_UNAVAILABLE",)),)
    partial = seal_offline_option_flow_publication(scope, partial_matrices, config, selected_at=scope.available_after,
        chain_evidence=partial_evidence, unavailable_underlyers=declaration)
    assert read_flow(partial)["available"] is True
    assert partial.coverage()["status"] == "PARTIAL"
    assert partial.coverage()["unavailable_underlyers"] == [[missing, ["MATRIX_UNAVAILABLE"]]]
    assert read_flow(partial)["data"]["selected"] == matrices[0].analysis.underlying.underlyer
    empty = seal_offline_option_flow_publication(scope, (), config, selected_at=scope.available_after,
        unavailable_underlyers=tuple((symbol, ("MATRIX_UNAVAILABLE",)) for symbol in scope.underlyings))
    assert read_flow(empty)["available"] is False
    assert read_flow(empty)["reason"] == "NO_COMPLETE_MATRIX"
    assert empty.coverage()["status"] == "MISSING"
    with pytest.raises(ValueError, match="missing-underlying"):
        seal_offline_option_flow_publication(scope, partial_matrices, config, selected_at=scope.available_after,
            chain_evidence=partial_evidence)


def test_extra_eligible_contract_cannot_masquerade_as_selected_matrix(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    matrix = matrices[0]
    item = matrix.ingress.inputs[0]
    ticker = f"O:{item.catalog.underlyer}261016C00101000"
    raw = json.loads(item.raw.raw_payload_json)
    raw["details"]["ticker"] = ticker
    extra = replace(item, catalog=replace(item.catalog, contract_id=9999, contract_ticker=ticker, strike=Decimal("101")),
        raw=parse_polygon_snapshot(raw, scope.available_after))
    changed = flow_chain_evidence(config, scope, matrix, inputs=(*matrix.ingress.inputs, extra))
    with pytest.raises(ValueError, match="selected-contract subset"):
        seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after,
            chain_evidence=(changed, *evidence[1:]))


@pytest.mark.parametrize("defect", ["cohort", "scope", "tier", "canonical", "reference", "action", "raw_rows",
    "snapshot", "missing_oi", "stale", "chain_inputs", "policy", "window", "late", "oi_date", "oi_revision", "oi_receipt"])
def test_flow_evidence_drift_and_missing_inputs_fail_closed(cycle, defect):
    config, scope, matrices, evidence = flow_matrices(cycle)
    changed = evidence[0]
    selected_at = scope.available_after
    pairs = ()
    if defect == "cohort":
        scope = replace(scope, cohort_id=uuid5(NAMESPACE_URL, "another-flow-cohort"))
    elif defect == "scope":
        changed = replace(changed, scope_sha256="f" * 64)
    elif defect == "tier":
        changed = replace(changed, provider_tier=ProviderTier.MASSIVE_ADVANCED)
    elif defect == "canonical":
        changed = replace(changed, database_name="stocks")
    elif defect == "reference":
        item = changed.inputs[0]
        changed = replace(changed, inputs=(replace(item, catalog=replace(item.catalog, payload_sha256="f" * 64)), *changed.inputs[1:]))
    elif defect == "action":
        scope = replace(scope, action_sha256="f" * 64)
    elif defect == "raw_rows":
        discovery = deepcopy(changed.discovery)
        discovery.result.rows[0]["day"]["volume"] = 900
        changed = replace(changed, discovery=discovery)
    elif defect in ("snapshot", "missing_oi", "stale"):
        items = list(changed.inputs)
        payload = json.loads(items[0].raw.raw_payload_json)
        if defect == "snapshot":
            payload["day"]["volume"] = 900
        elif defect == "missing_oi":
            payload.pop("open_interest")
        else:
            payload["last_trade"]["sip_timestamp"] = int((scope.cutoff - timedelta(hours=1)).timestamp()) * 1_000_000_000
        items[0] = replace(items[0], raw=parse_polygon_snapshot(payload, scope.available_after))
        changed = flow_chain_evidence(config, scope, matrices[0], inputs=tuple(items))
    elif defect == "chain_inputs":
        changed = replace(changed, inputs=changed.inputs[:1])
    elif defect == "policy":
        batch = replace(changed.discovery.result.batch, policy_sha256="f" * 64)
        changed = replace(changed, discovery=replace(changed.discovery, result=replace(changed.discovery.result, batch=batch)))
    elif defect == "window":
        result = changed.discovery.result
        changed = replace(changed, discovery=replace(changed.discovery,
            result=replace(result, included_ranges=tuple(replace(row, boundary="2026-10-16")
                if row.field == "expiration_date" and row.comparison == "lte" else row for row in result.included_ranges))))
    elif defect == "late":
        selected_at = scope.deadline + timedelta(seconds=1)
    else:
        pairs = list(oi_pairs(scope, matrices))
        pair = pairs[0]
        if defect == "oi_date":
            prior = replace(pair.prior, record=replace(pair.prior.record, settlement_session=scope.selection.session_date))
        elif defect == "oi_revision":
            prior = replace(pair.prior, revision_count=1)
        else:
            prior = replace(pair.prior, recorded_at=scope.available_after + timedelta(seconds=1))
        pairs[0] = replace(pair, prior=prior)
    with pytest.raises(ValueError):
        seal_offline_option_flow_publication(scope, matrices, config, selected_at=selected_at,
            chain_evidence=(changed, *evidence[1:]), open_interest_pairs=tuple(pairs))


def test_original_flow_retry_fences_and_stored_reads_do_no_work(cycle, monkeypatch):
    import realtime.option_flow_projection as flow

    config, scope, matrices, evidence = flow_matrices(cycle)
    publication = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after, chain_evidence=evidence)
    slot = OfflineForwardPublicationSlot(scope, 2)
    commit = dict(owner_epoch=2, current_scope_sha256=scope.sha256, checked_at=scope.available_after)
    assert slot.commit(publication, **commit) == slot.commit(publication, **commit)
    changed = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after,
        chain_evidence=evidence, open_interest_pairs=oi_pairs(scope, matrices))
    with pytest.raises(ValueError, match="immutable publication"):
        slot.commit(changed, **commit)
    with pytest.raises(ValueError, match="fence"):
        slot.commit(publication, **{**commit, "owner_epoch": 1})
    with pytest.raises(ValueError, match="fence"):
        slot.commit(publication, **{**commit, "current_scope_sha256": "f" * 64})
    with pytest.raises(FrozenInstanceError):
        publication.payload_json = "{}"
    before = read_flow(publication)

    def forbidden(*args, **kwargs):
        raise AssertionError("GET-side Flow production is prohibited")

    monkeypatch.setattr(flow, "_validate_chain", forbidden)
    monkeypatch.setattr(flow, "assemble_option_flow", forbidden)
    monkeypatch.setattr(flow, "option_flow_snapshot_rows", forbidden)
    monkeypatch.setattr(flow.DeveloperOptionNormalizer, "normalize", forbidden)
    assert read_flow(publication) == before
    publication.coverage()["available_underlyers"].clear()
    assert len(publication.coverage()["available_underlyers"]) == 8
    with pytest.raises(ValueError, match="fallback prohibited"):
        publication.read(cohort_id=scope.cohort_id, provider_tier=ProviderTier.MASSIVE_ADVANCED, scope_sha256=scope.sha256)


def test_actual_zero_volume_is_available_activity_not_missing_evidence(cycle):
    from test_forward_ingestion import context_cycle

    config, scope, matrices, _ = context_cycle(cycle, volume=0)
    evidence = tuple(flow_chain_evidence(config, scope, matrix) for matrix in matrices)
    publication = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after,
        chain_evidence=evidence)
    payload = read_flow(publication)
    assert payload["available"] is True
    assert len(payload["data"]["underlyers"]) == 8
    assert all(row["contract_count"] == 2 and row["total_volume"] == 0
        and Decimal(row["total_premium_activity"]) == 0 for row in payload["data"]["underlyers"])
    assert all(row["volume_open_interest_ratio"] == 0 for row in payload["data"]["top_contracts"])


def test_adjacent_flow_cutoffs_share_dataset_without_replacing_pinned_body(cycle):
    from test_forward_ingestion import context_cycle

    config, scope, matrices, evidence = flow_matrices(cycle)
    first = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after, chain_evidence=evidence)
    before = first.payload_json
    config, later_scope, later_matrices, _ = context_cycle(cycle, shift=timedelta(minutes=15))
    later_evidence = tuple(flow_chain_evidence(config, later_scope, matrix) for matrix in later_matrices)
    second = seal_offline_option_flow_publication(later_scope, later_matrices, config,
        selected_at=later_scope.available_after, chain_evidence=later_evidence)
    assert first.dataset_id == second.dataset_id
    assert first.scope_sha256 != second.scope_sha256 and first.input_sha256 != second.input_sha256
    assert read_flow(first)["as_of"] == scope.cutoff.isoformat()
    assert read_flow(second)["as_of"] == later_scope.cutoff.isoformat()
    assert first.payload_json == before
    with pytest.raises(ValueError, match="fallback prohibited"):
        first.read(cohort_id=scope.cohort_id, provider_tier=first.provider_tier, scope_sha256=later_scope.sha256)


def original_chain_assembly_reference(result, request, requested):
    source = Path(__file__).resolve().parents[2] / "options" / "api.py"
    handler = next(node for node in ast.parse(source.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name == "option_eligible_chain")
    returned = next(node for node in handler.body if isinstance(node, ast.Return))
    data = next(item.value for item in returned.value.keywords if item.arg == "data")
    coverage = result["coverage"]
    namespace = dict(result=deepcopy(result), request=request, requested=requested, coverage=coverage,
        missing=[symbol for symbol in requested if symbol not in {row["underlying"] for row in coverage}])
    expected = eval(compile(ast.Expression(body=data), str(source), "eval"), namespace)
    return json.loads(json.dumps(expected, default=lambda value: value.isoformat() if isinstance(value, (date, datetime)) else str(value)))


@pytest.mark.parametrize("minimum", [0, 900])
def test_stored_eligible_query_original_assembly_and_genuine_zero_matches(minimum):
    request = EligibleChainQuery(underlyer="SPY", filters=({"field": "day_volume", "minimum": minimum},))
    rows = (dict(underlying="SPY", expiration_date="2026-10-16", strike=100, contract_type="CALL", contract_id=1,
        day_volume=200, model_mark=2.5),)
    coverage = (dict(underlying="SPY", market_time=NOW.isoformat(), observed_time=NOW.isoformat(),
        batch_id=str(COHORT_ID), matrix_id=str(COHORT_ID), received=1, retained=1, eligible=1),)
    projection = query_stored_eligible_chain(request, rows=rows, coverage=coverage, underlyers=("SPY",))
    result = {key: projection["data"][key] for key in ("rows", "total", "coverage")}
    assert projection["data"] == original_chain_assembly_reference(result, request, ["SPY"])
    assert projection["available"] is True
    assert projection["data"]["total"] == (1 if minimum == 0 else 0)
    assert projection["data"]["coverage_status"] == "COVERED"
    assert len(projection["data"]["coverage"]) == 1


def query_chain(publication, request=None):
    return publication.query(request or EligibleChainQuery(), cohort_id=publication.cohort_id,
        provider_tier=publication.provider_tier, scope_sha256=publication.scope_sha256)


def test_original_screener_immutable_bundle_and_snapshot_contract(cycle):
    from options.repositories.snapshots import _snapshot

    config, scope, matrices, evidence = flow_matrices(cycle)
    publication = seal_offline_eligible_chain_publication(scope, matrices, config,
        selected_at=scope.available_after, chain_evidence=evidence)
    result = query_chain(publication)
    assert result["available"] is True and result["reason"] is None
    assert result["data"]["total"] == len(result["data"]["rows"]) == 16
    assert len(result["data"]["coverage"]) == 8
    assert result["data"]["coverage_status"] == "COVERED"
    assert result["model_version"] is None
    request = EligibleChainQuery()
    original = {key: result["data"][key] for key in ("rows", "total", "coverage")}
    assert result["data"] == original_chain_assembly_reference(original, request, list(scope.underlyings))
    row = result["data"]["rows"][0]
    assert "underlyer" not in row and "underlying" in row
    assert isinstance(row["strike"], (int, float)) and isinstance(row["model_mark"], (int, float))
    assert row["mark"] == row["model_mark"] and row["absolute_delta"] == abs(row["local_delta"])
    assert row["volume_open_interest_ratio"] == 2
    assert row["policy_version"] == config.policy.policy_version
    assert row["policy_sha256"] == config.policy_sha256
    assert row["asset_type"] == "ETF"
    typed_row = dict(row, snapshot_id=UUID(row["snapshot_id"]), batch_id=UUID(row["batch_id"]),
        expiration_date=date.fromisoformat(row["expiration_date"]))
    for field in ("expiration_cutoff", "spot_market_data_time", "mark_market_data_time", "market_data_time", "first_observed_at"):
        typed_row[field] = datetime.fromisoformat(row[field])
    for field in ("strike", "spot", "display_mark", "model_mark", "intrinsic_value", "extrinsic_value", "single_contract_breakeven"):
        typed_row[field] = Decimal(str(row[field])) if row[field] is not None else None
    assert _snapshot(typed_row).contract_id == row["contract_id"]
    assert seal_offline_eligible_chain_publication(scope, tuple(reversed(matrices)), config,
        selected_at=scope.available_after, chain_evidence=tuple(reversed(evidence))) == publication


def test_shared_bundle_gate_preserves_prior_flow_input_hash_identity(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    pairs = oi_pairs(scope, matrices)
    publication = seal_offline_option_flow_publication(scope, matrices, config, selected_at=scope.available_after,
        chain_evidence=evidence, open_interest_pairs=pairs)
    expected = canonical_sha256(dict(scope=scope.sha256,
        matrices=tuple(row.content_sha256 for row in sorted(matrices, key=lambda row: row.analysis.underlying.underlyer)),
        chains=tuple(sorted(canonical_sha256(dict(scope=row.scope_sha256, database_name=row.database_name,
            provider_tier=row.provider_tier, discovery_sha256=row.discovery.evidence_sha256,
            inputs=tuple(sorted(row.inputs, key=lambda item: item.catalog.contract_id)))) for row in evidence)),
        oi_pairs=tuple(sorted(canonical_sha256(row) for row in pairs))))
    assert publication.input_sha256 == expected


@pytest.mark.parametrize("descending", [False, True])
def test_original_screener_nulls_last_tie_breaks_and_pagination(descending):
    rows = tuple(dict(underlying=symbol, expiration_date="2026-10-16", strike=100, contract_type=side,
        contract_id=identity, volume_open_interest_ratio=ratio) for symbol, side, identity, ratio in (
            ("SPY", "PUT", 4, None), ("SPY", "CALL", 3, 2), ("QQQ", "PUT", 2, 2), ("QQQ", "CALL", 1, 4)))
    coverage = tuple(dict(underlying=symbol, market_time=NOW.isoformat(), observed_time=NOW.isoformat()) for symbol in ("QQQ", "SPY"))
    request = EligibleChainQuery(sort="volume_open_interest_ratio", descending=descending, limit=2, offset=1)
    result = query_stored_eligible_chain(request, rows=rows, coverage=coverage, underlyers=("QQQ", "SPY"))
    assert result["data"]["total"] == 4
    assert [row["contract_id"] for row in result["data"]["rows"]] == ([3, 1] if not descending else [2, 3])
    all_rows = query_stored_eligible_chain(EligibleChainQuery(sort=request.sort, descending=descending),
        rows=rows, coverage=coverage, underlyers=("QQQ", "SPY"))["data"]["rows"]
    assert all_rows[-1]["contract_id"] == 4
    beyond = query_stored_eligible_chain(EligibleChainQuery(offset=100, sort=request.sort),
        rows=rows, coverage=coverage, underlyers=("QQQ", "SPY"))
    assert beyond["available"] is True and beyond["data"]["rows"] == [] and beyond["data"]["total"] == 4


@pytest.mark.parametrize("field", tuple(CONTRACT_FILTERS))
def test_original_screener_ranges_are_inclusive_and_nulls_never_match(field):
    from options.discovery import contract_filter_sql

    value = 10 if field in ("calendar_dte", "day_volume", "open_interest") else 0.5
    request = EligibleChainQuery(filters=({"field": field, "minimum": value, "maximum": value},), sort=field)
    sql, params = contract_filter_sql(request)
    assert sql == f"{field} >= %s AND {field} <= %s" and params == (value, value)
    rows = tuple(dict(underlying="SPY", expiration_date="2026-10-16", strike=100, contract_type="CALL",
        contract_id=identity, **{field: measurement}) for identity, measurement in ((1, value), (2, None), (3, value + 1)))
    coverage = (dict(underlying="SPY", market_time=NOW.isoformat(), observed_time=NOW.isoformat()),)
    result = query_stored_eligible_chain(request, rows=rows, coverage=coverage, underlyers=("SPY",))
    assert result["data"]["total"] == 1
    assert result["data"]["rows"][0]["contract_id"] == 1


def test_original_screener_complete_partial_missing_and_session_pins(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    publication = seal_offline_eligible_chain_publication(scope, matrices, config,
        selected_at=scope.available_after, chain_evidence=evidence)
    selected = query_chain(publication, EligibleChainQuery(underlyer="SPY", contract_type="CALL",
        session_date=scope.selection.session_date))
    assert selected["data"]["total"] == len(selected["data"]["rows"]) == 1
    assert selected["data"]["requested_underlyers"] == ["SPY"]
    zero = query_chain(publication, EligibleChainQuery(filters=({"field": "day_volume", "minimum": 999},)))
    assert zero["available"] is True and zero["data"]["rows"] == [] and zero["data"]["total"] == 0
    assert zero["data"]["coverage_status"] == "COVERED"
    wrong_session = query_chain(publication, EligibleChainQuery(session_date=scope.selection.session_date - timedelta(days=1)))
    assert wrong_session["available"] is False and wrong_session["reason"] == "NO_ACTIVE_POLICY_ELIGIBLE_CHAIN"
    assert wrong_session["data"]["coverage_status"] == "MISSING"
    unknown = query_chain(publication, EligibleChainQuery(underlyer="ZZZ"))
    assert unknown["available"] is False and unknown["reason"] == "UNDERLYING_NOT_CONFIGURED"
    assert unknown["data"] == dict(rows=[], total=0, coverage=[])
    missing_symbol = matrices[-1].analysis.underlying.underlyer
    declaration = ((missing_symbol, ("MATRIX_UNAVAILABLE",)),)
    partial = seal_offline_eligible_chain_publication(scope, matrices[:-1], config,
        selected_at=scope.available_after, chain_evidence=evidence[:-1], unavailable_underlyers=declaration)
    assert query_chain(partial)["available"] is True
    assert query_chain(partial)["data"]["coverage_status"] == "PARTIAL"
    assert query_chain(partial)["data"]["missing_underlyers"] == [missing_symbol]
    assert query_chain(partial, EligibleChainQuery(underlyer=missing_symbol))["available"] is False
    blocked = seal_offline_eligible_chain_publication(scope, matrices, config, selected_at=scope.available_after)
    assert query_chain(blocked)["available"] is False
    assert query_chain(blocked)["reason"] == "POLICY_ELIGIBLE_CHAIN_COVERAGE_UNAVAILABLE"
    missing = seal_offline_eligible_chain_publication(scope, (), config, selected_at=scope.available_after,
        unavailable_underlyers=tuple((symbol, ("MATRIX_UNAVAILABLE",)) for symbol in scope.underlyings))
    assert query_chain(missing)["available"] is False
    assert query_chain(missing)["data"]["coverage_status"] == "MISSING"
    with pytest.raises(ValueError, match="missing-underlying"):
        seal_offline_eligible_chain_publication(scope, matrices[:-1], config, selected_at=scope.available_after,
            chain_evidence=evidence[:-1], unavailable_underlyers=((missing_symbol, ()),))


@pytest.mark.parametrize("defect", ["tier", "cohort", "reference", "action", "snapshot", "chain_rows", "late", "subset"])
def test_original_screener_uses_full_chain_and_scope_fences(cycle, defect):
    config, scope, matrices, evidence = flow_matrices(cycle)
    changed = evidence[0]
    selected_at = scope.available_after
    if defect == "tier":
        changed = replace(changed, provider_tier=ProviderTier.MASSIVE_ADVANCED)
    elif defect == "cohort":
        scope = replace(scope, cohort_id=uuid5(NAMESPACE_URL, "other-screener-cohort"))
    elif defect == "reference":
        item = changed.inputs[0]
        changed = replace(changed, inputs=(replace(item, catalog=replace(item.catalog, payload_sha256="f" * 64)), *changed.inputs[1:]))
    elif defect == "action":
        scope = replace(scope, action_sha256="f" * 64)
    elif defect == "snapshot":
        matrices = (replace(matrices[0], content_sha256="f" * 64), *matrices[1:])
    elif defect == "chain_rows":
        changed = replace(changed, inputs=changed.inputs[:1])
    elif defect == "late":
        selected_at = scope.deadline + timedelta(seconds=1)
    else:
        item = changed.inputs[0]
        ticker = f"O:{item.catalog.underlyer}261016C00101000"
        raw = json.loads(item.raw.raw_payload_json)
        raw["details"]["ticker"] = ticker
        extra = replace(item, catalog=replace(item.catalog, contract_id=9999, contract_ticker=ticker, strike=Decimal("101")),
            raw=parse_polygon_snapshot(raw, scope.available_after))
        changed = flow_chain_evidence(config, scope, matrices[0], inputs=(*matrices[0].ingress.inputs, extra))
    with pytest.raises(ValueError):
        seal_offline_eligible_chain_publication(scope, matrices, config, selected_at=selected_at,
            chain_evidence=(changed, *evidence[1:]))


def test_original_screener_stored_queries_do_no_producer_work_and_keep_retries_immutable(cycle, monkeypatch):
    import realtime.option_screener_projection as screener
    import realtime.option_flow_projection as flow

    config, scope, matrices, evidence = flow_matrices(cycle)
    publication = seal_offline_eligible_chain_publication(scope, matrices, config,
        selected_at=scope.available_after, chain_evidence=evidence)
    before = publication.payload_json
    slot = OfflineForwardPublicationSlot(scope, 2)
    commit = dict(owner_epoch=2, current_scope_sha256=scope.sha256, checked_at=scope.available_after)
    assert slot.commit(publication, **commit) == slot.commit(publication, **commit)
    changed = seal_offline_eligible_chain_publication(scope, matrices, config, selected_at=scope.available_after)
    with pytest.raises(ValueError, match="immutable publication"):
        slot.commit(changed, **commit)

    def forbidden(*args, **kwargs):
        raise AssertionError("stored Screener queries cannot fetch, normalize, analyze or publish")

    monkeypatch.setattr(screener, "validate_developer_policy_chain_bundle", forbidden)
    monkeypatch.setattr(screener, "eligible_chain_snapshot_row", forbidden)
    monkeypatch.setattr(flow.DeveloperOptionNormalizer, "normalize", forbidden)
    all_rows = query_chain(publication)
    filtered = query_chain(publication, EligibleChainQuery(underlyer="SPY", contract_type="PUT"))
    assert all_rows["data"]["total"] == 16 and filtered["data"]["total"] == 1
    filtered["data"]["rows"][0]["quality_flags"].clear()
    assert publication.payload_json == before
    with pytest.raises(ValueError, match="fallback prohibited"):
        publication.query(EligibleChainQuery(), cohort_id=scope.cohort_id,
            provider_tier=ProviderTier.MASSIVE_ADVANCED, scope_sha256=scope.sha256)


def test_stored_screener_preserves_exact_numeric_boundaries_and_order():
    rows = tuple(dict(snapshot_id=str(identity), underlying="SPY", expiration_date="2026-10-16", strike=100,
        contract_type="CALL", contract_id=identity, model_mark=0.1, day_volume=200) for identity in (1, 2))
    coverage = (dict(underlying="SPY", market_time=NOW.isoformat(), observed_time=NOW.isoformat()),)
    exact = {"1": {"model_mark": "0.10000000000000000000001"}, "2": {"model_mark": "0.1"}}
    result = query_stored_eligible_chain(EligibleChainQuery(filters=({"field": "model_mark", "maximum": 0.1},)),
        rows=rows, coverage=coverage, underlyers=("SPY",), filter_values=exact)
    assert result["data"]["total"] == 1 and result["data"]["rows"][0]["contract_id"] == 2
    ordered = query_stored_eligible_chain(EligibleChainQuery(sort="model_mark", descending=False),
        rows=rows, coverage=coverage, underlyers=("SPY",), filter_values=exact)
    assert [row["contract_id"] for row in ordered["data"]["rows"]] == [2, 1]
    assert all(isinstance(row["model_mark"], float) for row in ordered["data"]["rows"])


@pytest.mark.parametrize("defect", ["iv", "delta", "gamma", "theta", "vega", "rho", "mark", "market_clock", "observed_clock", "revision_clock", "expiry"])
def test_native_screener_eligibility_never_substitutes_missing_greeks_or_clocks(cycle, defect):
    from options.analytics.eligible_chain_projection import eligible_chain_snapshot_row

    config, scope, matrices, _ = flow_matrices(cycle)
    snapshot = matrices[0].snapshots[0]
    market_time, observed_time = scope.cutoff, scope.available_after
    if defect == "iv":
        snapshot = replace(snapshot, iv_converged=False, local_iv=None, iv_failure_reason="SYNTHETIC_UNAVAILABLE")
    elif defect in ("delta", "gamma", "theta", "vega", "rho"):
        field = {"delta": "local_delta", "gamma": "local_gamma", "theta": "local_theta_per_day",
            "vega": "local_vega_per_vol_point", "rho": "local_rho_per_rate_point"}[defect]
        snapshot = replace(snapshot, **{field: None})
    elif defect == "mark":
        snapshot = replace(snapshot, model_mark=None)
    elif defect == "market_clock":
        market_time -= timedelta(seconds=1)
    elif defect == "observed_clock":
        observed_time -= timedelta(seconds=1)
    elif defect == "revision_clock":
        snapshot = replace(snapshot, revised_observed_at=observed_time + timedelta(seconds=1))
    else:
        market_time = snapshot.expiration_cutoff
    assert eligible_chain_snapshot_row(snapshot, matrix_id=matrices[0].matrix_id, market_time=market_time,
        observed_time=observed_time, asset_type="ETF", policy_version=config.policy.policy_version,
        policy_sha256=config.policy_sha256) is None


def test_original_screener_adjacent_cutoffs_keep_dataset_and_exact_publication_pins(cycle):
    from test_forward_ingestion import context_cycle

    config, scope, matrices, evidence = flow_matrices(cycle)
    first = seal_offline_eligible_chain_publication(scope, matrices, config,
        selected_at=scope.available_after, chain_evidence=evidence)
    before = first.payload_json
    config, later_scope, later_matrices, _ = context_cycle(cycle, shift=timedelta(minutes=15))
    later_evidence = tuple(flow_chain_evidence(config, later_scope, matrix) for matrix in later_matrices)
    second = seal_offline_eligible_chain_publication(later_scope, later_matrices, config,
        selected_at=later_scope.available_after, chain_evidence=later_evidence)
    assert first.dataset_id == second.dataset_id
    assert first.scope_sha256 != second.scope_sha256 and first.input_sha256 != second.input_sha256
    assert query_chain(first)["as_of"] == scope.cutoff.isoformat()
    assert query_chain(second)["as_of"] == later_scope.cutoff.isoformat()
    assert first.payload_json == before
    with pytest.raises(ValueError, match="fallback prohibited"):
        first.query(EligibleChainQuery(), cohort_id=scope.cohort_id, provider_tier=first.provider_tier,
            scope_sha256=second.scope_sha256)


@pytest.mark.parametrize("query_data", [dict(sort="unsupported"), dict(contract_type="BOTH"),
    dict(filters=[dict(field="day_volume", minimum=0.5)]), dict(limit=0), dict(offset=-1)])
def test_stored_screener_revalidates_native_query_contract_before_work(query_data):
    with pytest.raises(ValueError):
        query_stored_eligible_chain(EligibleChainQuery.model_construct(**query_data), rows=(), coverage=(), underlyers=("SPY",))


def test_original_detector_evaluation_owner_accepts_native_forward_observations_without_io(cycle):
    from options.analytics.alert_selection import load_detector_run, load_evaluation_evidence
    from options.analytics.behavior_review import build_detector_evaluation_review
    from test_forward_ingestion import context_cycle, publish_context, read

    config, scope, matrices, source = context_cycle(cycle)
    _, publication = publish_context(config, scope, matrices, source)
    payload = read(publication)
    run = load_detector_run(json.dumps(payload["alerts"]["run"]))
    records = tuple(load_evaluation_evidence(json.dumps(row)) for row in payload["alerts"]["records"])
    repository = SimpleNamespace(review_inputs=lambda **kwargs: dict(ready=True, datasets=(publication.dataset_id,),
        dataset_id=publication.dataset_id, sessions=(scope.selection.session_date,), session_date=scope.selection.session_date,
        runs=(run,), records=records))
    result = build_detector_evaluation_review(as_of=scope.available_after, dataset_id=publication.dataset_id,
        repository=repository, detector="O1")
    assert result["version"] == "option_detector_evaluation_review_v1"
    assert result["total"] == 16 and len(result["completed_runs"]) == 1
    assert all(row["selection_status"] == "OBSERVATION" and row["candidate_id"] is None for row in result["rows"])
    assert result["models"][0]["selected"] == 0 and result["models"][0]["observations"] == 16
    assert result["execution_permission"] is False


def original_alert_fixture(cycle, *, volume=400, source_change=None):
    from realtime.option_alert_projection import ForwardAlertCycleSource
    from test_forward_ingestion import context_cycle, publish_context

    config, scope, matrices, source = context_cycle(cycle, volume=volume)
    if source_change is not None:
        source = source_change(source)
    context, publication = publish_context(config, scope, matrices, source)
    receipt = ForwardAlertCycleSource(scope, publication, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context)
    return config, scope, receipt


def alert_identity(publication):
    return dict(cohort_id=publication.cohort_id, provider_tier=publication.provider_tier, scope_sha256=publication.scope_sha256)


def test_original_alert_datasets_and_evaluations_are_native_observations_not_alert_hits(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery, seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle)
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    identity = alert_identity(publication)
    datasets = publication.datasets(**identity)
    assert datasets["available"] is True
    assert datasets["data"]["current_dataset_id"] == publication.dataset_id
    assert datasets["data"]["configured_dataset_id"] is None
    assert datasets["data"]["sessions"] == [scope.selection.session_date.isoformat()]
    review = publication.evaluations(DetectorEvaluationQuery(detector="O1"), **identity)
    assert review["available"] is True and review["reason"] is None
    assert review["data"]["version"] == "option_detector_evaluation_review_v1"
    assert review["data"]["total"] == len(review["data"]["rows"]) == 16
    assert all(row["candidate_id"] is None and row["selection_status"] == "OBSERVATION" for row in review["data"]["rows"])
    assert "alerts" not in review and "schema_version" not in review
    latest = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id), **identity)
    assert latest["available"] is False and latest["reason"] == "PACKAGE_CONTEXT_UNAVAILABLE"
    assert latest["data"]["version"] == "option_detector_alert_review_v2"
    assert latest["data"]["detected_observations"] == 16
    assert latest["data"]["total"] == latest["data"]["new_alerts"] == 0
    assert latest["data"]["rows"] == [] and latest["data"]["execution_permission"] is False


def test_original_alert_missing_context_and_unsupported_detectors_never_report_zero_success(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle, source_change=lambda source: replace(source, open_interest=()))
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    for detector in (None, "O1", "O2", "O3", "S1", "S2"):
        result = publication.evaluations(DetectorEvaluationQuery(detector=detector), **alert_identity(publication))
        assert result["available"] is False and result["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"
    complete_config, complete_scope, complete = original_alert_fixture(cycle, volume=200)
    zero = seal_offline_option_alert_publication(complete_scope, complete_config,
        selected_at=complete_scope.available_after, cycles=(complete,))
    result = zero.evaluations(DetectorEvaluationQuery(detector="O1"), **alert_identity(zero))
    assert result["available"] is True and result["data"]["total"] == 0


def native_alert_evaluation_reference(receipts, selected_at, query):
    from options.analytics.alert_selection import load_detector_run, load_evaluation_evidence
    from options.analytics.behavior_review import build_detector_evaluation_review
    from test_forward_ingestion import read

    runs, records = [], []
    for receipt in receipts:
        payload = read(receipt.publication)
        runs.append(load_detector_run(json.dumps(payload["alerts"]["run"])))
        records.extend(load_evaluation_evidence(json.dumps(row)) for row in payload["alerts"]["records"])
    session = query.session_date or receipts[-1].scope.selection.session_date
    scoped_runs = tuple(run for run in runs if run.scheduled_cycle.astimezone(timezone.utc).date() == session)
    run_ids = {run.run_id for run in scoped_runs}
    repository = SimpleNamespace(review_inputs=lambda **kwargs: dict(ready=bool(runs), datasets=(runs[0].dataset_id,),
        dataset_id=runs[0].dataset_id, sessions=tuple(sorted({run.scheduled_cycle.date() for run in runs})),
        session_date=session, runs=scoped_runs, records=tuple(record for record in records if record.run_id in run_ids)))
    arguments = query.model_dump()
    arguments["dataset_id"] = arguments["dataset_id"] or runs[0].dataset_id
    return json.loads(json.dumps(build_detector_evaluation_review(as_of=selected_at, repository=repository, **arguments)))


@pytest.mark.parametrize("sort_by", ("triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"))
def test_original_alert_stored_evaluation_sort_filter_and_page_match_native_owner(cycle, sort_by):
    from realtime.option_alert_projection import DetectorEvaluationQuery, seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle)
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    query = DetectorEvaluationQuery(detector="O1", selection_status="OBSERVATION", sort_by=sort_by,
        sort_order="desc", limit=1, offset=1, underlyer="spy")
    actual = publication.evaluations(query, **alert_identity(publication))
    expected = native_alert_evaluation_reference((receipt,), scope.available_after, query)
    assert actual["available"] is True
    assert actual["data"] == expected
    assert actual["data"]["total"] == 2 and len(actual["data"]["rows"]) == 1


def test_original_alert_partial_context_has_target_local_availability_not_empty_clearance(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle, source_change=lambda source:
        replace(source, open_interest=tuple(row for row in source.open_interest if row.record.underlying != "SPY")))
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    identity = alert_identity(publication)
    assert publication.evaluations(DetectorEvaluationQuery(detector="O1"), **identity)["available"] is False
    missing = publication.evaluations(DetectorEvaluationQuery(detector="O1", underlyer="SPY"), **identity)
    complete = publication.evaluations(DetectorEvaluationQuery(detector="O1", underlyer="AAPL"), **identity)
    assert missing["available"] is False and missing["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"
    assert complete["available"] is True and complete["data"]["total"] == 2
    unknown = publication.evaluations(DetectorEvaluationQuery(detector="O1", underlyer="ZZZ"), **identity)
    assert unknown["available"] is False and unknown["reason"] == "UNDERLYING_NOT_IN_MANIFEST"


def test_original_alert_history_continuity_and_immutable_scope_pins(cycle):
    from realtime.option_alert_projection import (
        DetectorEvaluationQuery, DetectorRunQuery, ForwardAlertCycleSource, seal_offline_option_alert_publication,
    )
    from test_forward_ingestion import context_cycle, publish_context

    config, scope, first_receipt = original_alert_fixture(cycle)
    first = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(first_receipt,))
    before = first.payload_json
    config, later_scope, later_matrices, later_source = context_cycle(cycle, shift=timedelta(minutes=15))
    later_context, later_forward = publish_context(config, later_scope, later_matrices, later_source)
    later_receipt = ForwardAlertCycleSource(later_scope, later_forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, later_context)
    second = seal_offline_option_alert_publication(later_scope, config, selected_at=later_scope.available_after,
        cycles=(later_receipt, first_receipt))
    assert first.dataset_id == second.dataset_id and first.scope_sha256 != second.scope_sha256
    review = second.evaluations(DetectorEvaluationQuery(detector="O1"), **alert_identity(second))
    assert review["available"] is True and review["data"]["total"] == 32
    assert len(review["data"]["completed_runs"]) == 2
    assert review["data"] == native_alert_evaluation_reference((first_receipt, later_receipt), later_scope.available_after,
        DetectorEvaluationQuery(detector="O1"))
    latest = second.detector_runs(DetectorRunQuery(dataset_id=second.dataset_id, detector="O1"), **alert_identity(second))
    history = second.detector_runs(DetectorRunQuery(dataset_id=second.dataset_id, scope="HISTORY",
        session_date=later_scope.selection.session_date, session_rollup=True, detector="O1"), **alert_identity(second))
    assert latest["available"] is history["available"] is False
    assert latest["data"]["detected_observations"] == history["data"]["detected_observations"] == 16
    assert len(latest["data"]["runs"]) == len(history["data"]["runs"]) == 1
    assert latest["data"]["runs"][0]["run_id"] != history["data"]["runs"][0]["run_id"]
    assert first.payload_json == before
    with pytest.raises(ValueError, match="fallback prohibited"):
        first.datasets(cohort_id=scope.cohort_id, provider_tier=first.provider_tier, scope_sha256=later_scope.sha256)


@pytest.mark.parametrize("defect", ("cohort", "tier", "canonical", "reference", "action", "context", "record", "record_scope",
    "run", "run_counts", "source_clock", "source_permission", "late", "duplicate"))
def test_original_alert_exact_source_receipts_and_native_records_reject_drift(cycle, defect):
    from realtime.option_alert_projection import seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle)
    selected_at = scope.available_after
    cycles = (receipt,)
    if defect == "cohort":
        receipt = replace(receipt, scope=replace(scope, cohort_id=uuid5(NAMESPACE_URL, "other-alert-cohort")))
    elif defect == "tier":
        receipt = replace(receipt, provider_tier=ProviderTier.MASSIVE_ADVANCED)
    elif defect == "canonical":
        receipt = replace(receipt, database_name="stocks")
    elif defect == "reference":
        receipt = replace(receipt, scope=replace(scope, reference_sha256="f" * 64))
    elif defect == "action":
        receipt = replace(receipt, scope=replace(scope, action_sha256="f" * 64))
    elif defect == "context":
        receipt = replace(receipt, detector_context=replace(receipt.detector_context, content_sha256="f" * 64))
    elif defect == "late":
        selected_at = scope.deadline + timedelta(seconds=1)
    elif defect == "duplicate":
        cycles = (receipt, receipt)
    else:
        payload = json.loads(receipt.publication.payload_json)
        if defect == "record":
            payload["alerts"]["records"].pop()
        elif defect == "record_scope":
            payload["alerts"]["records"][0]["dataset_id"] = "another-dataset"
        elif defect == "run":
            payload["alerts"]["run"]["market_policy_sha256"] = "f" * 64
        elif defect == "run_counts":
            payload["alerts"]["run"]["selection_counts"] = [["NOT_SELECTED", 0], ["OBSERVATION", 0], ["REPEAT", 0], ["SELECTED", 16]]
        elif defect == "source_permission":
            payload["original_page_contracts"] = True
        else:
            payload["published_at"] = (scope.available_after + timedelta(seconds=1)).isoformat()
        receipt = replace(receipt, publication=replace(receipt.publication, payload_json=json.dumps(payload)))
    if defect != "duplicate":
        cycles = (receipt,)
    with pytest.raises(ValueError):
        seal_offline_option_alert_publication(scope, config, selected_at=selected_at, cycles=cycles)


def test_original_alert_unavailable_gates_and_empty_source_are_not_zero_match_success(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery, seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle)
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    identity = alert_identity(publication)
    reasons = dict(schedule="DETECTOR_LAUNCH_NOT_CONFIGURED", candidate="CANDIDATE_UNAVAILABLE",
        quotes="ADVANCED_QUOTES_UNAVAILABLE", preview="ASSESSMENT_UNAVAILABLE")
    for contract, reason in reasons.items():
        result = publication.unavailable(contract, requested_contract_ids=(1, 2), **identity)
        assert result["available"] is False and result["reason"] == reason
    quotes = publication.unavailable("quotes", requested_contract_ids=(1, 2), **identity)
    assert quotes["data"]["quotes"] == [] and quotes["data"]["missing_contract_ids"] == [1, 2]
    assert quotes["data"]["provider_fetch_performed"] is False and quotes["data"]["execution_permission"] is False
    assert quotes["as_of"] is None and quotes["observed_at"] is None
    for status in ("SELECTED", "NOT_SELECTED", "REPEAT"):
        result = publication.evaluations(DetectorEvaluationQuery(detector="O1", selection_status=status), **identity)
        assert result["available"] is False and result["reason"] == "PACKAGE_CONTEXT_UNAVAILABLE"
    wrong_dataset = publication.evaluations(DetectorEvaluationQuery(detector="O1", dataset_id="other-dataset"), **identity)
    assert wrong_dataset["available"] is False and wrong_dataset["reason"] == "DETECTOR_DATASET_UNAVAILABLE"
    empty = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after)
    assert empty.datasets(**alert_identity(empty))["available"] is False
    assert empty.evaluations(DetectorEvaluationQuery(detector="O1"), **alert_identity(empty))["available"] is False
    assert empty.detector_runs(DetectorRunQuery(dataset_id=empty.dataset_id), **alert_identity(empty))["available"] is False


def test_original_alert_readers_keep_immutable_retries_and_call_no_native_builders(cycle, monkeypatch):
    import realtime.option_alert_projection as alerts

    config, scope, receipt = original_alert_fixture(cycle)
    publication = alerts.seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    assert alerts.seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,)) == publication
    slot = OfflineForwardPublicationSlot(scope, 1)
    commit = dict(owner_epoch=1, current_scope_sha256=scope.sha256, checked_at=scope.available_after)
    assert slot.commit(publication, **commit) == slot.commit(publication, **commit)
    empty = alerts.seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after)
    with pytest.raises(ValueError, match="immutable publication"):
        slot.commit(empty, **commit)
    before = publication.payload_json

    def forbidden(*args, **kwargs):
        raise AssertionError("GET-side native builder, detector or source loader work is prohibited")

    monkeypatch.setattr(alerts, "build_detector_evaluation_review", forbidden)
    monkeypatch.setattr(alerts, "build_detector_alert_review", forbidden)
    monkeypatch.setattr(alerts, "load_detector_run", forbidden)
    monkeypatch.setattr(alerts, "load_evaluation_evidence", forbidden)
    monkeypatch.setattr(alerts, "_decode_cycle", forbidden)
    result = publication.evaluations(alerts.DetectorEvaluationQuery(detector="O1", limit=1), **alert_identity(publication))
    assert result["available"] is True and result["data"]["total"] == 16 and len(result["data"]["rows"]) == 1
    result["data"]["models"].clear()
    assert len(publication.evaluations(alerts.DetectorEvaluationQuery(detector="O1"), **alert_identity(publication))["data"]["models"]) == 5
    assert publication.datasets(**alert_identity(publication))["available"] is True
    assert publication.payload_json == before


def test_original_alert_native_run_review_matches_reused_owner_and_partial_matrix_coverage(cycle):
    from options.analytics.alert_selection import load_detector_run, load_evaluation_evidence
    from options.analytics.behavior_review import build_detector_alert_review
    from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery, ForwardAlertCycleSource, seal_offline_option_alert_publication
    from test_forward_ingestion import context_cycle, context_source, publish_context, read

    config, scope, matrices, _ = context_cycle(cycle)
    missing_symbol = matrices[-1].analysis.underlying.underlyer
    matrices = matrices[:-1]
    source = context_source(config, scope, matrices)
    context, forward = publish_context(config, scope, matrices, source,
        unavailable_underlyers=((missing_symbol, ("MATRIX_UNAVAILABLE",)),))
    receipt = ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context)
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    payload = read(forward)
    run = load_detector_run(json.dumps(payload["alerts"]["run"]))
    records = tuple(load_evaluation_evidence(json.dumps(row)) for row in payload["alerts"]["records"])
    reader = SimpleNamespace(completed_runs=lambda **kwargs: (run,), completed_run=lambda **kwargs: (run, records))
    expected = build_detector_alert_review(dataset_id=publication.dataset_id, as_of=scope.available_after,
        session_date=scope.selection.session_date, detector="O1", repository=reader)
    actual = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id, detector="O1"), **alert_identity(publication))
    assert actual["data"] == json.loads(json.dumps(expected))
    assert actual["data"]["status"] == "PARTIAL"
    assert actual["data"]["run"]["covered_underlyings"] == 7
    assert actual["data"]["run"]["unavailable_underlyers"] == {missing_symbol: ["MATRIX_UNAVAILABLE"]}
    assert publication.evaluations(DetectorEvaluationQuery(detector="O1"), **alert_identity(publication))["available"] is False
    assert publication.evaluations(DetectorEvaluationQuery(detector="O1", underlyer="AAPL"), **alert_identity(publication))["available"] is True


def test_original_alert_stale_calendar_and_history_context_remain_unavailable(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, seal_offline_option_alert_publication

    for change in (
        lambda source: replace(source, exchange_session=None),
        lambda source: replace(source, activity_history=None),
        lambda source: replace(source, bars=tuple(row for row in source.bars if row[0].bar_end != cycle[1].cutoff)),
    ):
        config, scope, receipt = original_alert_fixture(cycle, source_change=change)
        publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
        result = publication.evaluations(DetectorEvaluationQuery(detector="O1"), **alert_identity(publication))
        assert result["available"] is False and result["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"


def test_original_alert_unknown_date_dataset_and_invalid_reads_do_not_fallback(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery, seal_offline_option_alert_publication

    config, scope, receipt = original_alert_fixture(cycle)
    publication = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(receipt,))
    before = publication.payload_json
    wrong_date = publication.evaluations(DetectorEvaluationQuery(detector="O1",
        session_date=scope.selection.session_date - timedelta(days=1)), **alert_identity(publication))
    wrong_dataset = publication.detector_runs(DetectorRunQuery(dataset_id="other-dataset"), **alert_identity(publication))
    assert wrong_date["available"] is wrong_dataset["available"] is False
    beyond = publication.evaluations(DetectorEvaluationQuery(detector="O1", offset=100), **alert_identity(publication))
    assert beyond["available"] is True and beyond["data"]["rows"] == [] and beyond["data"]["total"] == 16
    for changes in (dict(cohort_id=uuid5(NAMESPACE_URL, "another-alert-reader-cohort")),
                    dict(provider_tier=ProviderTier.MASSIVE_ADVANCED), dict(scope_sha256="f" * 64)):
        with pytest.raises(ValueError, match="fallback prohibited"):
            publication.datasets(**{**alert_identity(publication), **changes})
    for bad_ids in ((True,), (0,), (-1,)):
        with pytest.raises(ValueError):
            publication.unavailable("quotes", requested_contract_ids=bad_ids, **alert_identity(publication))
    assert publication.payload_json == before


@pytest.mark.parametrize("query_data", (dict(limit=0), dict(limit=201), dict(offset=-1), dict(detector="O4"),
    dict(selection_status="UNKNOWN"), dict(sort_by="unsupported"), dict(sort_order="up"), dict(dataset_id="x" * 81)))
def test_original_alert_query_validation_remains_original(query_data):
    from realtime.option_alert_projection import DetectorEvaluationQuery

    with pytest.raises(ValueError):
        DetectorEvaluationQuery(**query_data)


@pytest.mark.parametrize("query_data", (dict(dataset_id="fixture", scope="CURRENT"),
    dict(dataset_id="fixture", session_rollup=True), dict(dataset_id="fixture", scope="HISTORY", session_rollup=True),
    dict(dataset_id="fixture", selection_status="SELECTED")))
def test_original_alert_run_validation_preserves_history_scope_rules(query_data):
    from realtime.option_alert_projection import DetectorRunQuery

    with pytest.raises(ValueError):
        DetectorRunQuery(**query_data)


def indicative_display_fixture(cycle, *, shift=timedelta(0), package_change=None):
    from realtime.option_alert_projection import ForwardAlertCycleSource
    from realtime.option_package_context import load_forward_o1_package_context
    from test_forward_ingestion import o1_package_cycle, o1_package_source, seal_offline_forward_publication, EmptyPriorEvaluations

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True, shift=shift)
    forward = seal_offline_forward_publication(scope, matrices, config, selected_at=scope.available_after,
        detector_context=context, prior_evaluations=EmptyPriorEvaluations())
    receipt = load_forward_o1_package_context(scope, matrices[0], config, context,
        o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)
    if package_change is not None:
        payload = package_change(json.loads(receipt.payload_json))
        receipt = replace(receipt, payload_json=json.dumps(payload))
    source = ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER,
        context, (receipt,))
    return config, scope, source


def test_indicative_display_uses_original_alert_rows_and_preserves_delayed_data_clocks(cycle):
    from realtime.option_alert_projection import DetectorRunQuery, seal_offline_indicative_alert_publication

    config, scope, source = indicative_display_fixture(cycle)
    publication = seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    result = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id), **alert_identity(publication))
    assert result["available"] is True and result["reason"] is None
    assert result["data_tier"] == "15-MINUTE DELAYED RESEARCH DATA"
    assert result["as_of"] == scope.cutoff.isoformat()
    assert result["observed_at"] == result["generated_at"] == scope.available_after.isoformat()
    assert result["data"]["version"] == "option_detector_alert_review_v2"
    assert result["data"]["new_alerts"] == result["data"]["total"] == 1
    assert result["data"]["status"] == "PARTIAL"
    row = result["data"]["rows"][0]
    assert row["event_horizon_status"] == "UNAVAILABLE"
    assert row["original_package"]["status"] == "AVAILABLE"
    assert len(row["original_package"]["legs"]) == 1
    assert row["original_package"]["market_data_time"] == scope.cutoff.isoformat()
    assert row["original_package"]["observed_time"] == scope.available_after.isoformat()
    assert row["current_mark"]["status"] == "UNAVAILABLE"
    assert row["management_status"] == "UNAVAILABLE"
    assert row["fill"] is None and result["data"]["execution_permission"] is False
    assert "qualified_indicative" not in result["data"]


@pytest.mark.parametrize("detector", ("S1", "S2"))
def test_technical_display_filters_native_stock_first_packages_and_disables_o2(cycle, detector):
    from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery, ForwardAlertCycleSource, seal_offline_indicative_alert_publication
    from realtime.option_package_context import load_forward_technical_package_context
    from test_forward_ingestion import stock_first_package_cycle, o1_package_source, seal_offline_forward_publication, EmptyPriorEvaluations

    config, scope, matrices, context = stock_first_package_cycle(cycle, detector)
    forward = seal_offline_forward_publication(scope, matrices, config, selected_at=scope.available_after,
        detector_context=context, prior_evaluations=EmptyPriorEvaluations())
    receipt = load_forward_technical_package_context(scope, matrices[0], config, context,
        o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)
    source = ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context, (receipt,))
    publication = seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    result = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id, detector=detector), **alert_identity(publication))
    assert result["available"] is True
    assert result["data"]["rows"]
    assert all(row["detector_id"] == detector for row in result["data"]["rows"])
    assert result["data"]["context_availability"][detector] == "AVAILABLE_INDICATIVE"
    assert result["data"]["context_availability"]["O2"] == "UNAVAILABLE"
    disabled = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id, detector="O2"), **alert_identity(publication))
    assert disabled["available"] is False and disabled["reason"] == "DETECTOR_DISABLED_BY_PROVIDER_TIER"


def test_technical_display_o3_reuses_native_credit_evidence_and_shared_budget(cycle):
    from realtime.option_alert_projection import DetectorEvaluationQuery, DetectorRunQuery, ForwardAlertCycleSource, seal_offline_indicative_alert_publication
    from realtime.option_package_context import load_forward_technical_package_context
    from test_forward_ingestion import credit_package_cycle, o1_package_source, seal_offline_forward_publication, EmptyPriorEvaluations

    config, scope, matrices, context = credit_package_cycle(cycle)
    forward = seal_offline_forward_publication(scope, matrices, config, selected_at=scope.available_after,
        detector_context=context, prior_evaluations=EmptyPriorEvaluations())
    receipt = load_forward_technical_package_context(scope, matrices[0], config, context,
        o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)
    source = ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context, (receipt,))
    publication = seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    result = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id, detector="O3"), **alert_identity(publication))
    assert result["available"] is True
    assert result["data"]["total"] == 1
    row = result["data"]["rows"][0]
    assert row["detector_id"] == "O3"
    assert row["original_package"]["basis"] == "ORIGINAL_CANDIDATE_SNAPSHOTS"
    assert row["fill"] is None and row["management_status"] == "NOT_APPLICABLE"
    all_models = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id), **alert_identity(publication))
    assert all_models["data"]["new_alerts"] <= 20
    assert all_models["data"]["runs"][0]["coverage_status"] == "COMPLETE"
    assert all_models["data"]["detected_observations"] > 0
    indicators = publication.evaluations(DetectorEvaluationQuery(detector="O1", selection_status="OBSERVATION"), **alert_identity(publication))
    assert indicators["available"] is True and indicators["data"]["total"] > 0
    assert all(row["detector_id"] == "O1" and row["selection_status"] == "OBSERVATION" for row in indicators["data"]["rows"])


def test_original_alert_stored_candidate_detail_has_exact_identity_and_no_calculation(cycle, monkeypatch):
    import realtime.option_alert_projection as alerts
    from realtime.option_package_context import load_forward_technical_package_context
    from test_forward_ingestion import stock_first_package_cycle, o1_package_source, seal_offline_forward_publication, EmptyPriorEvaluations

    config, scope, matrices, context = stock_first_package_cycle(cycle, "S1")
    forward = seal_offline_forward_publication(scope, matrices, config, selected_at=scope.available_after,
        detector_context=context, prior_evaluations=EmptyPriorEvaluations())
    receipt = load_forward_technical_package_context(scope, matrices[0], config, context,
        o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)
    source = alerts.ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context, (receipt,))
    publication = alerts.seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    stored = json.loads(publication.payload_json)
    identity = next(iter(stored["candidate_details"]))

    def forbidden(*args, **kwargs):
        raise AssertionError("candidate GET cannot recalculate")

    monkeypatch.setattr(alerts, "assess_option_package", forbidden)
    result = publication.candidate(UUID(identity), **alert_identity(publication))
    assert result["available"] is True
    assert result["data"]["candidate"]["candidate_id"] == identity
    assert result["data"]["legs"] == stored["original_packages"][identity]["legs"]
    assert result["data"]["execution_mode"] == "READ_ONLY_RESEARCH"
    assert result["data"]["source_snapshots"]
    schedule = publication.schedule(checked_at=scope.available_after, **alert_identity(publication))
    assert schedule["available"] is False and schedule["reason"] == "DETECTOR_LAUNCH_NOT_CONFIGURED"
    assert schedule["data"]["completed_run_count"] == 1
    assert schedule["data"]["worker_health"] == "NOT_CHECKED"
    assert "window_start" not in schedule["data"]
    result["data"]["legs"].clear()
    assert publication.candidate(UUID(identity), **alert_identity(publication))["data"]["legs"]
    assert publication.candidate(UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"), **alert_identity(publication))["available"] is False


def test_original_alert_preview_reuses_native_blockers_and_never_reopens_deadlines(cycle):
    import realtime.option_alert_projection as alerts
    from realtime.option_package_context import load_forward_technical_package_context
    from test_forward_ingestion import stock_first_package_cycle, o1_package_source, seal_offline_forward_publication, EmptyPriorEvaluations

    config, scope, matrices, context = stock_first_package_cycle(cycle, "S1")
    forward = seal_offline_forward_publication(scope, matrices, config, selected_at=scope.available_after,
        detector_context=context, prior_evaluations=EmptyPriorEvaluations())
    receipt = load_forward_technical_package_context(scope, matrices[0], config, context,
        o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)
    source = alerts.ForwardAlertCycleSource(scope, forward, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context, (receipt,))
    publication = alerts.seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    candidate = UUID(next(iter(json.loads(publication.payload_json)["candidate_details"])))
    original = publication.payload_json
    result = publication.preview(candidate, assessed_at=scope.available_after, request={}, **alert_identity(publication))
    assert result["available"] is True and result["data"]["status"] == "BLOCKED"
    assert {blocker["field"] for blocker in result["data"]["blockers"] if blocker["code"] == "PLAN_TERM_REQUIRED"} == {
        "entry_deadline", "exit_deadline", "entry_limit"}
    assert result["data"]["persisted"] is result["data"]["publication_permission"] is result["data"]["execution_permission"] is False
    assert result["data"]["paper_position_created"] is False and result["data"]["plan_preview"] is None
    expired = publication.preview(candidate, assessed_at=scope.available_after + timedelta(hours=1), request={}, **alert_identity(publication))
    assert expired["generated_at"] == scope.available_after + timedelta(hours=1)
    assert expired["as_of"] == result["as_of"]
    assert any(row["code"] == "ORIGINAL_ENTRY_WINDOW_ELAPSED" for row in expired["data"]["blockers"])
    assert publication.payload_json == original
    with pytest.raises(ValueError, match="precede"):
        publication.preview(candidate, assessed_at=scope.available_after - timedelta(seconds=1), request={}, **alert_identity(publication))


@pytest.mark.parametrize("read_error", (False, True))
def test_original_alert_marks_overlay_uses_native_validation_and_exact_scope(cycle, monkeypatch, read_error):
    from unittest.mock import MagicMock
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository
    from realtime.option_portal_repository import RealtimeOptionPortalMarkRepository
    import options.outcomes as outcomes

    config, scope, source = indicative_display_fixture(cycle)
    candidate = source.package_contexts[0].read(scope_sha256=scope.sha256,
        matrix_id=source.package_contexts[0].matrix_id)["qualified_indicative"][0]["candidate"]
    publication = SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER, payload_json=json.dumps(dict(
        valuation_policy=config.valuation_policy.model_dump(mode="json"), valuation_policy_sha256=config.valuation_policy_sha256,
        candidate_details={candidate["candidate_id"]: dict(candidate=dict(candidate, candidate_identity=candidate["identity_sha256"]))})))
    captured = []

    def read(repository, ids, **kwargs):
        captured.append((ids, kwargs))
        if read_error:
            from psycopg2 import OperationalError
            raise OperationalError("offline retained mark read unavailable")
        return dict(ready=True, rows={candidate["candidate_id"]: {"bad": "unbound"}})

    monkeypatch.setattr(RealtimeOptionPortalMarkRepository, "retained_plan_marks", read)
    envelope = dict(available=True, data=dict(rows=[dict(candidate_id=candidate["candidate_id"],
        matrix_id=candidate["matrix_id"], original_package=dict(legs=candidate["legs"],
            market_data_time=candidate["market_data_time"], net_premium=candidate["net_premium"], capital_at_risk=candidate["capital_at_risk"]))]))
    repository = RealtimeOptionsAlertsPageRepository(lambda: None)
    result = repository._retained_marks(MagicMock(), publication, envelope, cutoff=scope.available_after, cohort_id=scope.cohort_id)
    assert captured[0][1]["cohort_id"] == scope.cohort_id
    assert captured[0][1]["provider_tier"] is ProviderTier.MASSIVE_DEVELOPER
    assert result["data"]["rows"][0]["current_mark"]["status"] == "UNAVAILABLE"
    assert result["available"] is True
    assert result["data"]["rows"][0]["current_mark"]["reason"] == (
        "CURRENT_MARK_READ_UNAVAILABLE" if read_error else "CURRENT_MARK_BINDING_INVALID")
    assert result["data"]["rows"][0]["current_mark"]["execution_permission"] is False


def test_original_alert_marks_overlay_preserves_existing_developer_valuation(cycle, monkeypatch):
    from unittest.mock import MagicMock
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository
    from realtime.option_portal_repository import RealtimeOptionPortalMarkRepository
    from realtime.option_portal_adapter import materialize_developer_option_package_mark
    from test_option_portal_adapter import _developer_policy, _package, _developer_snapshot, _plan, NOW as mark_time

    policy = _developer_policy()
    package = _package(policy.policy_sha256, ProviderTier.MASSIVE_DEVELOPER)
    snapshots = {leg.instrument_id: _developer_snapshot(leg, policy,
        batch_id=UUID("12121212-1212-1212-1212-121212121212"),
        mark="6.10" if leg.leg_index == 0 else "2.50", milliseconds=100 + leg.leg_index * 50) for leg in package.legs}
    checked = mark_time + timedelta(milliseconds=200)
    retained = materialize_developer_option_package_mark(package, snapshots, checked_at=checked, policy=policy).mark.retained_row()
    plan = _plan(package)
    publication = SimpleNamespace(provider_tier=ProviderTier.MASSIVE_DEVELOPER, payload_json=json.dumps(dict(
        valuation_policy=policy.model_dump(mode="json"), valuation_policy_sha256=policy.policy_sha256,
        candidate_details={str(package.candidate_id): dict(candidate=dict(candidate_identity=package.candidate_identity_sha256))})))
    monkeypatch.setattr(RealtimeOptionPortalMarkRepository, "retained_plan_marks", lambda *args, **kwargs:
        dict(ready=True, rows={str(package.candidate_id): retained}))
    envelope = dict(available=True, data=dict(rows=[dict(candidate_id=str(package.candidate_id), matrix_id=str(package.matrix_id),
        original_package=dict(market_data_time=plan["source_market_time"], net_premium=package.entry_net_premium,
            capital_at_risk=package.capital_at_risk, legs=[dict(leg, model_mark=leg["entry_model_mark"]) for leg in plan["legs"]]))]))
    repository = RealtimeOptionsAlertsPageRepository(lambda: None)
    result = repository._retained_marks(MagicMock(), publication, envelope, cutoff=checked, cohort_id=package.cohort_id)
    mark = result["data"]["rows"][0]["current_mark"]
    assert mark["status"] == "FRESH" and mark["reason"] is None
    assert mark["gross_pnl"] == Decimal("60.00")
    assert mark["execution_permission"] is False and mark["slippage"] == "UNAVAILABLE"


def test_indicative_display_original_review_parity_and_observation_identity_isolation(cycle):
    import realtime.option_alert_projection as alerts
    from options.analytics.alert_selection import load_detector_run, load_evaluation_evidence
    from options.analytics.behavior_review import build_detector_alert_review

    config, scope, source = indicative_display_fixture(cycle)
    observation = alerts.seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    before = observation.payload_json
    publication = alerts.seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    stored = publication.read(**alert_identity(publication))
    native = stored["native_cycles"][0]
    run = load_detector_run(native["run_payload"])
    records = tuple(load_evaluation_evidence(value) for value in native["record_payloads"])
    reader = SimpleNamespace(completed_runs=lambda **kwargs: (run,), completed_run=lambda **kwargs: (run, records),
        repeat_counts=lambda **kwargs: {record.evaluation_id: dict(repeats=0, last_seen_at=None, confirmation_timeframes=[]) for record in records},
        original_packages=lambda *args, **kwargs: stored["original_packages"])
    marks = SimpleNamespace(retained_plan_marks=lambda *args, **kwargs: dict(ready=False, rows={}, management_status_ready=False))
    expected = build_detector_alert_review(dataset_id=publication.dataset_id, as_of=scope.available_after,
        session_date=scope.selection.session_date, detector="O1", source_repository=reader, repository=reader,
        mark_repository=marks, valuation_policy=config.valuation_policy)
    actual = publication.detector_runs(alerts.DetectorRunQuery(dataset_id=publication.dataset_id, detector="O1"), **alert_identity(publication))
    actual["data"].pop("context_availability")
    assert actual["data"] == json.loads(json.dumps(expected, default=lambda value: value.isoformat() if isinstance(value, (date, datetime)) else str(value)))
    assert observation.dataset_id != publication.dataset_id
    assert observation.payload_json == before
    old = observation.detector_runs(alerts.DetectorRunQuery(dataset_id=observation.dataset_id), **alert_identity(observation))
    assert old["available"] is False
    evaluation = publication.evaluations(alerts.DetectorEvaluationQuery(selection_status="SELECTED"), **alert_identity(publication))
    assert evaluation["available"] is True and evaluation["data"]["total"] == 1
    assert evaluation["data"]["rows"][0]["event_horizon_status"] == "UNAVAILABLE"
    assert evaluation["data"]["context_availability"]["S1"] == "UNAVAILABLE"


def test_indicative_display_requires_native_packages_but_not_complete_execution_context(cycle):
    from realtime.option_alert_projection import DetectorRunQuery, seal_offline_indicative_alert_publication

    config, scope, source = indicative_display_fixture(cycle)
    absent = seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after,
        cycles=(replace(source, package_contexts=()),))
    assert absent.detector_runs(DetectorRunQuery(dataset_id=absent.dataset_id), **alert_identity(absent))["available"] is False
    publication = seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    for detector in ("O2", "O3", "S1", "S2"):
        result = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id, detector=detector), **alert_identity(publication))
        assert result["available"] is False and result["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"
    unknown = publication.detector_runs(DetectorRunQuery(dataset_id=publication.dataset_id, underlyer="SPY"), **alert_identity(publication))
    assert unknown["available"] is False
    quotes = publication.unavailable("quotes", requested_contract_ids=(1, 2), **alert_identity(publication))
    assert quotes["available"] is False and quotes["data"]["provider_fetch_performed"] is False


@pytest.mark.parametrize("defect", ("plan", "candidate_price", "leg_price", "snapshot", "event_blocked", "tier", "matrix", "late"))
def test_indicative_display_keeps_native_input_scope_and_deadline_fences(cycle, defect):
    from realtime.option_alert_projection import seal_offline_indicative_alert_publication

    config, scope, source = indicative_display_fixture(cycle)
    selected_at = scope.available_after
    if defect == "tier":
        source = replace(source, provider_tier=ProviderTier.MASSIVE_ADVANCED)
    elif defect == "late":
        selected_at += timedelta(seconds=31)
    else:
        receipt = source.package_contexts[0]
        payload = json.loads(receipt.payload_json)
        row = payload["qualified_indicative"][0]
        if defect == "plan":
            row["plan_sha256"] = "f" * 64
        elif defect == "candidate_price":
            row["candidate"]["net_premium"] = "-900"
        elif defect == "leg_price":
            row["candidate"]["legs"][0]["model_mark"] = "9"
        elif defect == "snapshot":
            row["source_snapshots"][0]["open_interest"] = 999
        elif defect == "matrix":
            payload["matrix_id"] = str(COHORT_ID)
        else:
            plan = json.loads(row["plan_payload_json"])
            plan["event_horizon"]["checks"][0]["status"] = "BLOCKED"
            row["plan_payload_json"] = json.dumps(plan)
        source = replace(source, package_contexts=(replace(receipt, payload_json=json.dumps(payload)),))
    with pytest.raises(ValueError):
        seal_offline_indicative_alert_publication(scope, config, selected_at=selected_at, cycles=(source,))


def test_indicative_display_history_uses_sealed_selections_and_native_repeat_counts(cycle):
    from realtime.option_alert_projection import DetectorRunQuery, seal_offline_indicative_alert_publication

    config, scope, source = indicative_display_fixture(cycle)
    first = seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    before = first.payload_json
    later_config, later_scope, later_source = indicative_display_fixture(cycle, shift=timedelta(minutes=15))
    second = seal_offline_indicative_alert_publication(later_scope, later_config, selected_at=later_scope.available_after,
        cycles=(later_source,), prior_publications=(first,))
    assert first.dataset_id == second.dataset_id and first.scope_sha256 != second.scope_sha256
    latest = second.detector_runs(DetectorRunQuery(dataset_id=second.dataset_id), **alert_identity(second))
    history = second.detector_runs(DetectorRunQuery(dataset_id=second.dataset_id, scope="HISTORY",
        session_date=later_scope.selection.session_date), **alert_identity(second))
    assert latest["available"] is history["available"] is True
    assert latest["data"]["repeat_hits"] == 1 and latest["data"]["new_alerts"] == 0
    assert history["data"]["total"] == 1 and history["data"]["rows"][0]["hit_count"] == 2
    assert history["data"]["rows"][0]["last_seen_at"] == later_scope.available_after.isoformat()
    assert history["data"]["rows"][0]["original_package"]["market_data_time"] == scope.cutoff.isoformat()
    assert first.payload_json == before
    with pytest.raises(ValueError, match="deadline|cycle"):
        seal_offline_indicative_alert_publication(later_scope, later_config, selected_at=later_scope.available_after,
            cycles=(source, later_source))


def test_indicative_display_stored_reads_do_no_selector_or_review_work(cycle, monkeypatch):
    import realtime.option_alert_projection as alerts

    config, scope, source = indicative_display_fixture(cycle)
    publication = alerts.seal_offline_indicative_alert_publication(scope, config, selected_at=scope.available_after, cycles=(source,))
    before = publication.payload_json

    def forbidden(*args, **kwargs):
        raise AssertionError("indicative GET cannot compute or select")

    monkeypatch.setattr(alerts, "select_dual_origin_packages", forbidden)
    monkeypatch.setattr(alerts, "build_detector_alert_review", forbidden)
    monkeypatch.setattr(alerts, "build_detector_evaluation_review", forbidden)
    monkeypatch.setattr(alerts, "_decode_indicative_packages", forbidden)
    page = publication.detector_runs(alerts.DetectorRunQuery(dataset_id=publication.dataset_id, limit=1), **alert_identity(publication))
    assert page["available"] is True and page["data"]["total"] == 1
    page["data"]["rows"][0]["original_package"]["legs"].clear()
    assert len(publication.detector_runs(alerts.DetectorRunQuery(dataset_id=publication.dataset_id),
        **alert_identity(publication))["data"]["rows"][0]["original_package"]["legs"]) == 1
    assert publication.payload_json == before