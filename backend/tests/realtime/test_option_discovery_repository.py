import json
import inspect
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from options.config import load_option_runtime_configuration
from options.data.polygon_chain_discovery import (
    PolygonOptionChainDiscoveryAdapter,
    PolygonOptionChainDiscoveryLimits,
    PolygonOptionChainDiscoveryPlan,
)
from options.data.polygon_http import PolygonHttpResponse
from realtime.option_discovery_repository import (
    RealtimeOptionDiscoveryEvidence,
    persist_realtime_option_discovery,
)
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
import realtime.option_discovery_repository as option_discovery_repository
import realtime.polygon_option_discovery as polygon_option_discovery
from realtime.polygon_option_discovery import (
    PolygonDeveloperOptionDiscoveryBinding,
    PolygonDeveloperOptionDiscoverySource,
)
from realtime.preflight import ProviderTier


UTC = timezone.utc
MARKET_TIME = datetime(2026, 10, 5, 14, 30, tzinfo=UTC)
OBSERVED_AT = MARKET_TIME + timedelta(minutes=15)
SECURITY_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
BACKEND_DIR = Path(__file__).resolve().parents[2]
PROFILE_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _scope(provider_tier=ProviderTier.MASSIVE_DEVELOPER):
    from realtime.contracts import CANARY_UNIVERSE

    return RealtimeOptionDiscoveryScope(
        provider_tier,
        PROFILE_ID,
        "d" * 64,
        CANARY_UNIVERSE,
    )


def _response(payload):
    return PolygonHttpResponse(200, json.dumps(payload).encode("utf-8"))


class QueueHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.responses.pop(0)


def _configuration():
    return load_option_runtime_configuration(
        {"POLYGON_API_KEY": "fake-only"},
        BACKEND_DIR,
    )


def _source(http):
    return PolygonDeveloperOptionDiscoverySource(
        http,
        _configuration(),
        _scope(),
        clock=lambda: OBSERVED_AT,
    )


def _result():
    next_url = "https://api.massive.com/v3/snapshot/options/AAPL?cursor=second"
    return _source(QueueHttp([
        _response({
            "status": "DELAYED",
            "results": [{"details": {"ticker": "O:1"}}],
            "next_url": next_url,
        }),
        _response({
            "status": "DELAYED",
            "results": [{"details": {"ticker": "O:2"}}],
        }),
    ])).discover("AAPL", MARKET_TIME, Decimal("100"))


def _stored_batch(evidence):
    batch = evidence.result.batch
    return (
        str(SECURITY_ID),
        str(evidence.scope.profile_id),
        evidence.scope.profile_sha256,
        evidence.scope.universe.sha256,
        evidence.scope.provider_tier.value,
        evidence.evidence_sha256,
        len(batch.pages),
        batch.row_count,
        evidence.response_bytes,
    )


def _profile_row(evidence):
    return (
        evidence.scope.profile_sha256,
        evidence.scope.provider_tier.value,
        evidence.scope.universe.sha256,
    )


def _advanced_result():
    plan = PolygonOptionChainDiscoveryPlan.build(
        "AVGO",
        MARKET_TIME,
        date(2026, 11, 19),
        Decimal("280"),
        Decimal("420"),
    )
    adapter = PolygonOptionChainDiscoveryAdapter(
        QueueHttp([_response({"status": "OK", "results": []})]).get,
        base_url="https://api.massive.com",
        limits=PolygonOptionChainDiscoveryLimits(40, 10_000, 10_485_760, 67_108_864),
        clock=lambda: OBSERVED_AT,
    )
    return adapter.discover(
        plan,
        batch_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        policy_sha256="e" * 64,
        started_at=MARKET_TIME,
    )


def _advanced_scope():
    from realtime.contracts import CANARY_UNIVERSE, UniverseManifest

    universe = UniverseManifest(
        "advanced-expanded-test-v1",
        CANARY_UNIVERSE.equity_tickers + ("AVGO",),
        CANARY_UNIVERSE.option_underlyings + ("AVGO",),
    )
    return RealtimeOptionDiscoveryScope(
        ProviderTier.MASSIVE_ADVANCED,
        UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        "f" * 64,
        universe,
    )


def test_evidence_identity_is_deterministic_complete_and_explicit():
    evidence = RealtimeOptionDiscoveryEvidence(_scope(), SECURITY_ID, _result())
    repeated = RealtimeOptionDiscoveryEvidence(
        _scope(), SECURITY_ID, evidence.result
    )

    assert evidence.evidence_sha256 == repeated.evidence_sha256
    assert evidence.expiration_from.isoformat() == "2026-10-05"
    assert evidence.expiration_through.isoformat() == "2026-12-04"
    assert evidence.strike_min == Decimal("85.00")
    assert evidence.strike_max == Decimal("115.00")
    assert evidence.maximum_dte == 60
    assert evidence.spot_price == Decimal("100.00")
    assert evidence.strike_corridor_fraction == Decimal("0.15")
    assert len(evidence.coverage) == 8
    assert sum(row.disposition == "INCLUDED" for row in evidence.coverage) == 4
    assert sum(row.disposition == "EXCLUDED" for row in evidence.coverage) == 4
    assert {row.reason_code for row in evidence.coverage if row.reason_code} == {
        "OUTSIDE_DISCOVERY_WINDOW",
        "OUTSIDE_STRIKE_CORRIDOR",
    }


def test_advanced_evidence_accepts_reviewed_expanded_policy_without_relabelling():
    evidence = RealtimeOptionDiscoveryEvidence(
        _advanced_scope(),
        SECURITY_ID,
        _advanced_result(),
    )

    assert evidence.scope.provider_tier is ProviderTier.MASSIVE_ADVANCED
    assert evidence.result.batch.underlyer == "AVGO"
    assert evidence.maximum_dte == 45
    assert evidence.spot_price == Decimal("350")
    assert evidence.strike_corridor_fraction == Decimal("0.2")


def test_evidence_rejects_payload_hash_or_coverage_loss():
    result = _result()
    damaged_page = replace(result.batch.pages[0], payload_sha256="0" * 64)
    damaged_batch = replace(
        result.batch,
        pages=(damaged_page,) + result.batch.pages[1:],
    )

    with pytest.raises(ValueError, match="payload hash mismatch"):
        RealtimeOptionDiscoveryEvidence(
            _scope(),
            SECURITY_ID,
            replace(result, batch=damaged_batch),
        )
    with pytest.raises(ValueError, match="exact included/excluded coverage"):
        RealtimeOptionDiscoveryEvidence(
            _scope(),
            SECURITY_ID,
            replace(result, included_ranges=result.included_ranges[:-1]),
        )


def test_repository_persists_complete_raw_pages_and_coverage_atomically():
    evidence = RealtimeOptionDiscoveryEvidence(_scope(), SECURITY_ID, _result())
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        _profile_row(evidence),
        ("AAPL",),
        _stored_batch(evidence),
    ]
    cursor.fetchall.side_effect = [
        [
            (
                page.page_number, page.row_count, page.response_bytes,
                page.payload_sha256, page.received_at, page.terminal,
                page.request_filter_sha256, page.request_cursor_sha256,
                page.request_id, page.next_cursor_sha256,
                dict(page.request_metadata),
            )
            for page in evidence.result.batch.pages
        ],
        [
            (
                row.dimension, row.disposition, row.comparison,
                row.boundary, row.reason_code,
            )
            for row in evidence.coverage
        ],
    ]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_realtime_option_discovery(connection, evidence)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert sum("INSERT INTO rt.option_discovery_batches" in row for row in statements) == 1
    assert sum("INSERT INTO rt.option_discovery_pages" in row for row in statements) == 2
    assert sum("INSERT INTO rt.option_discovery_coverage" in row for row in statements) == 8
    page_calls = [
        call for call in cursor.execute.call_args_list
        if "INSERT INTO rt.option_discovery_pages" in call.args[0]
    ]
    assert [call.args[1][3] for call in page_calls] == [
        page.response_bytes for page in evidence.result.batch.pages
    ]
    batch_call = next(
        call for call in cursor.execute.call_args_list
        if "INSERT INTO rt.option_discovery_batches" in call.args[0]
    )
    assert batch_call.args[0].count("%s") == len(batch_call.args[1])
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_repository_rolls_back_identity_conflict_without_partial_acceptance():
    evidence = RealtimeOptionDiscoveryEvidence(_scope(), SECURITY_ID, _result())
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        _profile_row(evidence),
        ("AAPL",),
        (
            str(SECURITY_ID), str(evidence.scope.profile_id),
            evidence.scope.profile_sha256, evidence.scope.universe.sha256,
            evidence.scope.provider_tier.value, "0" * 64, 2, 2,
            evidence.response_bytes,
        ),
    ]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(ValueError, match="batch identity conflict"):
        persist_realtime_option_discovery(connection, evidence)

    assert connection.commit.call_count == 0
    assert connection.rollback.call_count == 1
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("option_discovery_pages" in row for row in statements)


def test_repository_rejects_profile_relabelling_before_discovery_insert():
    evidence = RealtimeOptionDiscoveryEvidence(
        _advanced_scope(),
        SECURITY_ID,
        _advanced_result(),
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        evidence.scope.profile_sha256,
        ProviderTier.MASSIVE_DEVELOPER.value,
        evidence.scope.universe.sha256,
    )
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    with pytest.raises(ValueError, match="capability profile identity mismatch"):
        persist_realtime_option_discovery(connection, evidence)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("INSERT INTO rt.option_discovery" in row for row in statements)
    assert connection.rollback.call_count == 1


def test_repository_persists_advanced_scope_without_developer_relabelling():
    evidence = RealtimeOptionDiscoveryEvidence(
        _advanced_scope(),
        SECURITY_ID,
        _advanced_result(),
    )
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        _profile_row(evidence),
        ("AVGO",),
        _stored_batch(evidence),
    ]
    cursor.fetchall.side_effect = [
        [
            (
                page.page_number, page.row_count, page.response_bytes,
                page.payload_sha256, page.received_at, page.terminal,
                page.request_filter_sha256, page.request_cursor_sha256,
                page.request_id, page.next_cursor_sha256,
                dict(page.request_metadata),
            )
            for page in evidence.result.batch.pages
        ],
        [
            (
                row.dimension, row.disposition, row.comparison,
                row.boundary, row.reason_code,
            )
            for row in evidence.coverage
        ],
    ]
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor

    persist_realtime_option_discovery(connection, evidence)

    batch_call = next(
        call for call in cursor.execute.call_args_list
        if "INSERT INTO rt.option_discovery_batches" in call.args[0]
    )
    parameters = batch_call.args[1]
    assert parameters[2] == str(evidence.scope.profile_id)
    assert parameters[4] == evidence.scope.universe.sha256
    assert parameters[5] == ProviderTier.MASSIVE_ADVANCED.value
    assert connection.commit.call_count == 1


def test_binding_persists_only_after_complete_shared_discovery():
    http = QueueHttp([_response({"status": "DELAYED", "results": []})])
    persisted = []
    binding = PolygonDeveloperOptionDiscoveryBinding(
        _source(http),
        lambda scope, security_id, result: persisted.append(
            (scope, security_id, result)
        ),
    )

    result = binding.run(SECURITY_ID, "AAPL", MARKET_TIME, Decimal("100"))

    assert result.batch.complete is True
    assert persisted == [(_scope(), SECURITY_ID, result)]
    assert len(http.calls) == 1


def test_binding_never_persists_partial_pagination_failure():
    http = QueueHttp([_response({
        "status": "DELAYED",
        "results": [],
        "next_url": "https://example.com/v3/snapshot/options/AAPL?cursor=stolen",
    })])
    persisted = []
    binding = PolygonDeveloperOptionDiscoveryBinding(
        _source(http),
        lambda scope, security_id, result: persisted.append(
            (scope, security_id, result)
        ),
    )

    with pytest.raises(Exception, match="next page failed validation"):
        binding.run(SECURITY_ID, "AAPL", MARKET_TIME, Decimal("100"))

    assert persisted == []


def test_discovery_binding_has_no_canonical_fallback_or_connection_construction():
    source = "\n".join((
        inspect.getsource(polygon_option_discovery),
        inspect.getsource(option_discovery_repository),
    ))

    for forbidden in (
        "options.repositories",
        "from database",
        "import database",
        "backend.database",
        "from main",
        "psycopg2.connect",
        "create_pool",
        "canonical_fallback",
        "UPDATE rt.option_discovery",
        "DELETE FROM rt.option_discovery",
    ):
        assert forbidden not in source


def test_shared_discovery_contract_cannot_narrow_to_developer_or_canary():
    repository_source = inspect.getsource(option_discovery_repository)
    discovery_source = inspect.getsource(polygon_option_discovery)

    for forbidden in (
        "CANARY_UNIVERSE",
        "ProviderTier.MASSIVE_DEVELOPER.value",
        "maximum_dte != 60",
        'Decimal("0.15")',
    ):
        assert forbidden not in repository_source
    assert "class PolygonDeveloperOptionDiscoverySource" in discovery_source
    assert "class PolygonAdvancedOptionDiscoverySource" in discovery_source
    assert discovery_source.count("PolygonOptionChainDiscoveryAdapter(") == 1