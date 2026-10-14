"""V2 options evidence identity (lineage `advanced_realtime_v2`) and the explicit v1/V2 gates of the Flow, Alerts
and package-context modules. Offline: no database is opened; the physical names below are provenance strings."""
import json
from dataclasses import dataclass, field, fields, replace
from datetime import timedelta
from types import SimpleNamespace

import pytest

import realtime.options_evidence_identity as identity_module
from realtime.contracts import canonical_sha256
from realtime.forward_ingestion import ForwardDetectorContextSource, _context_sha256
from realtime.option_alert_projection import (
    ForwardAlertCycleSource, ForwardAlertCycleSourceV2, seal_offline_indicative_alert_publication,
    seal_offline_option_alert_publication,
)
from realtime.option_flow_projection import (
    DeveloperFlowChainEvidence, DeveloperFlowChainEvidenceV2, build_option_flow_bundle,
    developer_policy_chain_input_sha256, seal_offline_option_flow_publication, seal_offline_option_flow_publications,
    validate_developer_policy_chain_bundle,
)
from realtime.option_package_context import (
    ForwardO1PackageSource, ForwardO1PackageSourceV2, ForwardTradeWindow, ForwardTradeWindowV2,
    ForwardTradeWindowWithPages, load_forward_o1_package_context, load_forward_technical_package_context,
    retained_trade_context,
)
from realtime.options_evidence_identity import (
    ADVANCED_REALTIME_V2_LINEAGE, V1_FORWARD_DATABASE_NAME, V2_OPTIONS_EVIDENCE_IDENTITY, OptionsEvidenceIdentity,
    OptionsEvidenceLineage, carried_identity, identity_binding, is_v2_options_evidence_identity,
    options_evidence_admitted, require_options_evidence, require_options_lineage,
)
from realtime.preflight import ProviderTier
from test_forward_ingestion import (  # noqa: F401 - cycle is a pytest fixture
    context_cycle, cycle, o1_package_cycle, o1_package_source, publish_context, seal,
)
from test_option_flow_realtime import flow_matrices, oi_pairs, read_flow

V1 = OptionsEvidenceLineage.V1_FORWARD
V2 = OptionsEvidenceLineage.ADVANCED_REALTIME_V2
V2_DATABASE = "test_realtime_v2_oid_evidence"
BAD_LINEAGES = ("", " ", "advanced_realtime_v2 ", " advanced_realtime_v2", "Advanced_Realtime_V2",
                "advanced-realtime-v2", "advanced_realtime_v1", "advanced_realtime", "stocks_realtime_v1", "v1_forward")


class _SpoofIdentity(OptionsEvidenceIdentity):
    __slots__ = ()


# --- the identity concept -------------------------------------------------------------------------------------

def test_the_v2_lineage_is_a_logical_name_distinct_from_the_v1_database_literal():
    assert ADVANCED_REALTIME_V2_LINEAGE == "advanced_realtime_v2" and V1_FORWARD_DATABASE_NAME == "stocks_realtime_v1"
    assert OptionsEvidenceLineage("advanced_realtime_v2") is V2 and V2.value == ADVANCED_REALTIME_V2_LINEAGE
    assert V1.value != V1_FORWARD_DATABASE_NAME and V2.value != V1.value
    assert V2_OPTIONS_EVIDENCE_IDENTITY == OptionsEvidenceIdentity("advanced_realtime_v2")
    assert is_v2_options_evidence_identity(V2_OPTIONS_EVIDENCE_IDENTITY)


@pytest.mark.parametrize("value", ["advanced_realtime_v2", "v1_forward", "stocks_realtime_v1", "", None, 2])
def test_a_lineage_is_an_enum_member_never_a_free_form_string(value):
    with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
        require_options_lineage(value)
    with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
        options_evidence_admitted(value, database_name=V2_DATABASE, identity=V2_OPTIONS_EVIDENCE_IDENTITY)


@pytest.mark.parametrize(("lineage", "database", "identity", "admitted"), [
    (V1, "stocks_realtime_v1", None, True),
    (V1, "stocks_realtime_v1", V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V1, V2_DATABASE, V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V1, V2_DATABASE, None, False),
    (V1, "stocks_realtime_v2", None, False),
    (V1, "advanced_realtime_v2", None, False),
    (V1, "", None, False),
    (V1, " stocks_realtime_v1", None, False),
    (V1, "stocks_realtime_v1 ", None, False),
    (V1, "STOCKS_REALTIME_V1", None, False),
    (V1, None, None, False),
    (V2, V2_DATABASE, V2_OPTIONS_EVIDENCE_IDENTITY, True),
    (V2, "stocks_realtime_v2", V2_OPTIONS_EVIDENCE_IDENTITY, True),
    (V2, "stocks_realtime_v1", V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, V2_DATABASE, None, False),
    (V2, "stocks_realtime_v1", None, False),
    (V2, "", V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, " ", V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, " " + V2_DATABASE, V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, V2_DATABASE + " ", V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, None, V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, 7, V2_OPTIONS_EVIDENCE_IDENTITY, False),
    (V2, V2_DATABASE, _SpoofIdentity("advanced_realtime_v2"), False),
    (V2, V2_DATABASE, "advanced_realtime_v2", False),
])
def test_each_lineage_admits_only_its_own_evidence(lineage, database, identity, admitted):
    assert options_evidence_admitted(lineage, database_name=database, identity=identity) is admitted
    if admitted:
        require_options_evidence(lineage, database_name=database, identity=identity, what="Flow")
    else:
        with pytest.raises(ValueError, match="Flow evidence identity.*fallback prohibited"):
            require_options_evidence(lineage, database_name=database, identity=identity, what="Flow")


@pytest.mark.parametrize("lineage_text", BAD_LINEAGES)
@pytest.mark.parametrize("database", [V2_DATABASE, "stocks_realtime_v1"])
def test_a_blank_or_mistyped_lineage_is_admitted_by_neither_gate(lineage_text, database):
    identity = OptionsEvidenceIdentity(lineage_text)
    assert not is_v2_options_evidence_identity(identity)
    assert not options_evidence_admitted(V2, database_name=database, identity=identity)
    assert not options_evidence_admitted(V1, database_name=database, identity=identity)
    for value in (None, 7, b"advanced_realtime_v2"):
        wrong_type = OptionsEvidenceIdentity(value)
        assert not options_evidence_admitted(V2, database_name=V2_DATABASE, identity=wrong_type)
        assert not options_evidence_admitted(V1, database_name=V1_FORWARD_DATABASE_NAME, identity=wrong_type)


def test_identity_binding_leaves_v1_hashes_untouched_and_separates_v2():
    assert identity_binding(SimpleNamespace(database_name="stocks_realtime_v1")) == {}
    assert identity_binding(SimpleNamespace(identity=V2_OPTIONS_EVIDENCE_IDENTITY)) == {
        "evidence_identity": V2_OPTIONS_EVIDENCE_IDENTITY}
    assert carried_identity(SimpleNamespace()) is None


def test_the_identity_module_is_a_leaf_with_no_v1_lineage_imports():
    source = identity_module.__file__
    with open(source, encoding="utf-8") as handle:
        imports = [line for line in handle.read().splitlines() if line.startswith(("import ", "from "))]
    assert imports == ["from __future__ import annotations", "from dataclasses import dataclass",
                       "from enum import Enum"]


def test_v1_carriers_keep_their_exact_field_sets_and_v2_carriers_add_only_the_identity():
    names = lambda cls: [item.name for item in fields(cls)]  # noqa: E731
    assert names(DeveloperFlowChainEvidence) == ["scope_sha256", "database_name", "provider_tier", "discovery", "inputs"]
    assert names(ForwardAlertCycleSource) == ["scope", "publication", "database_name", "provider_tier",
                                              "detector_context", "package_contexts"]
    assert names(ForwardO1PackageSource)[:5] == ["scope_sha256", "matrix_sha256", "detector_context_sha256",
                                                  "database_name", "provider_tier"]
    assert "identity" not in names(ForwardO1PackageSource) and "_LINEAGE" not in names(ForwardTradeWindow)
    assert names(ForwardTradeWindow)[-3:] == ["requested_contract_ids", "events", "coverage"]
    for base, derived in ((DeveloperFlowChainEvidence, DeveloperFlowChainEvidenceV2),
                          (ForwardAlertCycleSource, ForwardAlertCycleSourceV2),
                          (ForwardO1PackageSource, ForwardO1PackageSourceV2),
                          (ForwardTradeWindow, ForwardTradeWindowV2)):
        assert names(derived) == [*names(base), "identity"]
        assert not any(item.name == "identity" for item in fields(base))
    assert not issubclass(ForwardTradeWindowWithPages, ForwardTradeWindowV2)


# --- Flow ----------------------------------------------------------------------------------------------------

def _v2_chain(row, *, database_name=V2_DATABASE, identity=V2_OPTIONS_EVIDENCE_IDENTITY):
    return DeveloperFlowChainEvidenceV2(row.scope_sha256, database_name, row.provider_tier, row.discovery, row.inputs,
                                        identity=identity)


def _v1_chain(row, database_name):
    return DeveloperFlowChainEvidence(row.scope_sha256, database_name, row.provider_tier, row.discovery, row.inputs)


def _flow_options(scope, matrices):
    return dict(selected_at=scope.available_after, open_interest_pairs=oi_pairs(scope, matrices))


def test_the_v2_flow_gate_seals_the_same_page_contract_under_its_own_input_identity(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    options = _flow_options(scope, matrices)
    v1 = seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=evidence, **options)
    v2_evidence = tuple(_v2_chain(row) for row in evidence)
    v2 = seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=v2_evidence, lineage=V2, **options)
    assert set(v2) == set(v1) == set(scope.underlyings)
    for symbol in scope.underlyings:
        assert v2[symbol].payload_json == v1[symbol].payload_json
        assert v2[symbol].coverage_json == v1[symbol].coverage_json and v2[symbol].dataset_id == v1[symbol].dataset_id
        assert v2[symbol].input_sha256 != v1[symbol].input_sha256
    assert read_flow(v2["SPY"])["available"] is True and v2["SPY"].coverage()["status"] == "COMPLETE"
    single = seal_offline_option_flow_publication(scope, matrices, config, chain_evidence=v2_evidence, lineage=V2,
                                                  underlyer="SPY", **options)
    assert single == v2["SPY"]
    bundle = build_option_flow_bundle(scope, matrices, config, chain_evidence=v2_evidence, lineage=V2, **options)
    assert len(bundle.summaries) == len(scope.underlyings)
    accepted, blocked = validate_developer_policy_chain_bundle(scope, matrices, config, selected_at=scope.available_after,
                                                               chain_evidence=v2_evidence, lineage=V2)
    assert len(accepted) == len(scope.underlyings) and blocked == ()


def test_the_v1_flow_input_hash_is_byte_identical_to_the_formula_before_the_identity_change(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    legacy = canonical_sha256(dict(scope=scope.sha256,
        matrices=tuple(row.content_sha256 for row in sorted(matrices, key=lambda row: row.analysis.underlying.underlyer)),
        chains=tuple(sorted(canonical_sha256(dict(scope=row.scope_sha256, database_name=row.database_name,
            provider_tier=row.provider_tier, discovery_sha256=row.discovery.evidence_sha256,
            inputs=tuple(sorted(row.inputs, key=lambda item: item.catalog.contract_id)))) for row in evidence))))
    assert developer_policy_chain_input_sha256(scope, matrices, evidence) == legacy
    assert developer_policy_chain_input_sha256(scope, matrices, tuple(_v2_chain(row) for row in evidence)) != legacy


def test_each_flow_gate_rejects_the_other_lineages_evidence(cycle):
    config, scope, matrices, evidence = flow_matrices(cycle)
    options = _flow_options(scope, matrices)
    v2_evidence = tuple(_v2_chain(row) for row in evidence)
    with pytest.raises(TypeError, match="explicit full policy-chain evidence"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=v2_evidence, **options)
    with pytest.raises(TypeError, match="explicit full policy-chain evidence"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=v2_evidence, lineage=V1, **options)
    with pytest.raises(TypeError, match="explicit full policy-chain evidence"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=evidence, lineage=V2, **options)
    mixed = (v2_evidence[0], *evidence[1:])
    for lineage in (V1, V2):
        with pytest.raises(TypeError, match="explicit full policy-chain evidence"):
            seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=mixed, lineage=lineage, **options)


@pytest.mark.parametrize("lineage", ["advanced_realtime_v2", "v1_forward", "stocks_realtime_v1", "", None])
def test_the_flow_entry_points_take_an_enum_lineage_not_a_string(cycle, lineage):
    config, scope, matrices, evidence = flow_matrices(cycle)
    options = _flow_options(scope, matrices)
    for call in (seal_offline_option_flow_publications, seal_offline_option_flow_publication, build_option_flow_bundle):
        with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
            call(scope, matrices, config, chain_evidence=evidence, lineage=lineage, **options)
    with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
        validate_developer_policy_chain_bundle(scope, matrices, config, selected_at=scope.available_after,
                                               chain_evidence=evidence, lineage=lineage)


@pytest.mark.parametrize("lineage_text", BAD_LINEAGES)
def test_a_blank_or_mistyped_flow_lineage_is_rejected_by_both_gates(cycle, lineage_text):
    config, scope, matrices, evidence = flow_matrices(cycle)
    options = _flow_options(scope, matrices)
    bad = tuple(_v2_chain(row, identity=OptionsEvidenceIdentity(lineage_text)) for row in evidence)
    with pytest.raises(ValueError, match="fallback prohibited"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=bad, lineage=V2, **options)
    with pytest.raises(TypeError, match="explicit full policy-chain evidence"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=bad, lineage=V1, **options)


@pytest.mark.parametrize("database", ["", " ", "stocks_realtime_v1"])
def test_v2_flow_evidence_needs_a_physical_name_that_is_not_the_v1_database(cycle, database):
    config, scope, matrices, evidence = flow_matrices(cycle)
    bad = tuple(_v2_chain(row, database_name=database) for row in evidence)
    with pytest.raises(ValueError, match="fallback prohibited"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=bad, lineage=V2,
                                              **_flow_options(scope, matrices))


@pytest.mark.parametrize("database", ["", "stocks_realtime_v2", "advanced_realtime_v2", "test_realtime_v2_oid_evidence",
                                      "STOCKS_REALTIME_V1", "stocks_realtime_v1 "])
def test_the_v1_flow_gate_still_accepts_only_the_exact_v1_literal(cycle, database):
    config, scope, matrices, evidence = flow_matrices(cycle)
    options = _flow_options(scope, matrices)
    bad = tuple(_v1_chain(row, database) for row in evidence)
    with pytest.raises(ValueError, match="fallback prohibited"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=bad, **options)
    with pytest.raises(TypeError, match="explicit full policy-chain evidence"):
        seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=bad, lineage=V2, **options)
    exact = tuple(_v1_chain(row, "stocks_realtime_v1") for row in evidence)
    assert set(seal_offline_option_flow_publications(scope, matrices, config, chain_evidence=exact, **options)) \
        == set(scope.underlyings)


# --- Alerts --------------------------------------------------------------------------------------------------

def _alert_sources(cycle):
    config, scope, sources = cycle
    publication = seal(config, scope, sources)
    v1 = ForwardAlertCycleSource(scope, publication, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER)
    v2 = ForwardAlertCycleSourceV2(scope, publication, V2_DATABASE, ProviderTier.MASSIVE_DEVELOPER,
                                   identity=V2_OPTIONS_EVIDENCE_IDENTITY)
    return config, scope, publication, v1, v2


def test_the_v2_alert_gate_decodes_a_v2_cycle_receipt_into_the_same_review(cycle):
    config, scope, _, v1, v2 = _alert_sources(cycle)
    expected = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(v1,))
    actual = seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(v2,),
                                                   lineage=V2)
    assert actual.payload_json == expected.payload_json and actual.dataset_id == expected.dataset_id
    assert actual.input_sha256 != expected.input_sha256


def test_each_alert_gate_rejects_the_other_lineages_receipt(cycle):
    config, scope, _, v1, v2 = _alert_sources(cycle)
    common = dict(selected_at=scope.available_after)
    for call in (seal_offline_option_alert_publication, seal_offline_indicative_alert_publication):
        with pytest.raises(TypeError, match="immutable cycle receipts"):
            call(scope, config, cycles=(v2,), **common)
        with pytest.raises(TypeError, match="immutable cycle receipts"):
            call(scope, config, cycles=(v1,), lineage=V2, **common)
        with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
            call(scope, config, cycles=(v1,), lineage="stocks_realtime_v1", **common)


@pytest.mark.parametrize("lineage_text", BAD_LINEAGES)
def test_a_blank_or_mistyped_alert_lineage_is_rejected_by_both_gates(cycle, lineage_text):
    config, scope, publication, _, _ = _alert_sources(cycle)
    bad = ForwardAlertCycleSourceV2(scope, publication, V2_DATABASE, ProviderTier.MASSIVE_DEVELOPER,
                                    identity=OptionsEvidenceIdentity(lineage_text))
    with pytest.raises(ValueError, match="fallback prohibited"):
        seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(bad,), lineage=V2)
    with pytest.raises(TypeError, match="immutable cycle receipts"):
        seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(bad,))


@pytest.mark.parametrize("database", ["", " ", "stocks_realtime_v1"])
def test_a_v2_alert_receipt_needs_a_physical_name_that_is_not_the_v1_database(cycle, database):
    config, scope, publication, _, _ = _alert_sources(cycle)
    bad = ForwardAlertCycleSourceV2(scope, publication, database, ProviderTier.MASSIVE_DEVELOPER,
                                    identity=V2_OPTIONS_EVIDENCE_IDENTITY)
    with pytest.raises(ValueError, match="fallback prohibited"):
        seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(bad,), lineage=V2)


@pytest.mark.parametrize("database", ["", "stocks_realtime_v2", "advanced_realtime_v2"])
def test_the_v1_alert_gate_still_accepts_only_the_exact_v1_literal(cycle, database):
    config, scope, publication, _, _ = _alert_sources(cycle)
    bad = ForwardAlertCycleSource(scope, publication, database, ProviderTier.MASSIVE_DEVELOPER)
    with pytest.raises(ValueError, match="fallback prohibited"):
        seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(bad,))


@dataclass(frozen=True, slots=True)
class _V2ContextSource(ForwardDetectorContextSource):
    """Stands in for a V2 detector-context source type, which does not exist yet (its base class gates on the v1
    database literal in forward_ingestion, which is un-ported lineage)."""

    identity: OptionsEvidenceIdentity = field(kw_only=True)

    def __post_init__(self):
        pass


def _v2_context(context, *, database_name=V2_DATABASE, identity=V2_OPTIONS_EVIDENCE_IDENTITY):
    values = {item.name: getattr(context.source, item.name) for item in fields(ForwardDetectorContextSource)}
    source = _V2ContextSource(**{**values, "database_name": database_name}, identity=identity)
    return replace(context, source=source,
                   content_sha256=_context_sha256(source, context.selected_at, context.inputs, context.dependencies))


def _bound_publication(publication, context):
    """The cycle publication with its detector-context receipt pointing at `context`. No sealing path produces a
    V2 context receipt yet, so the tests patch the one hash a V2 receipt would carry."""
    payload = json.loads(publication.payload_json)
    payload["detector_context"]["content_sha256"] = context.content_sha256
    return replace(publication, payload_json=json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _review(publication):
    payload = json.loads(publication.payload_json)
    payload.pop("source_sha256s")
    return payload


def test_a_v1_detector_context_is_never_admitted_into_a_v2_alert_cycle(cycle):
    config, scope, matrices, source = context_cycle(cycle)
    context, publication = publish_context(config, scope, matrices, source)
    v1 = ForwardAlertCycleSource(scope, publication, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context)
    assert seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(v1,)).payload_json
    v2_with_v1_context = ForwardAlertCycleSourceV2(scope, publication, V2_DATABASE, ProviderTier.MASSIVE_DEVELOPER,
                                                   context, identity=V2_OPTIONS_EVIDENCE_IDENTITY)
    with pytest.raises(ValueError, match="detector-context receipt drift"):
        seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after,
                                              cycles=(v2_with_v1_context,), lineage=V2)
    v2_context = _v2_context(context, database_name="stocks_realtime_v1")
    v1_with_v2_context = ForwardAlertCycleSource(scope, _bound_publication(publication, v2_context),
        "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, v2_context)
    with pytest.raises(ValueError, match="detector-context receipt drift"):
        seal_offline_option_alert_publication(scope, config, selected_at=scope.available_after, cycles=(v1_with_v2_context,))


def test_the_v2_alert_gate_requires_the_detector_context_to_carry_the_v2_lineage(cycle):
    config, scope, matrices, source = context_cycle(cycle)
    context, publication = publish_context(config, scope, matrices, source)
    at = dict(selected_at=scope.available_after)
    v1 = ForwardAlertCycleSource(scope, publication, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, context)
    expected = _review(seal_offline_option_alert_publication(scope, config, cycles=(v1,), **at))

    def receipt(identity):
        v2_context = _v2_context(context, identity=identity)
        return ForwardAlertCycleSourceV2(scope, _bound_publication(publication, v2_context), V2_DATABASE,
            ProviderTier.MASSIVE_DEVELOPER, v2_context, identity=V2_OPTIONS_EVIDENCE_IDENTITY)

    accepted = seal_offline_option_alert_publication(
        scope, config, cycles=(receipt(V2_OPTIONS_EVIDENCE_IDENTITY),), lineage=V2, **at)
    assert _review(accepted) == expected
    for lineage_text in BAD_LINEAGES:
        with pytest.raises(ValueError, match="detector-context receipt drift"):
            seal_offline_option_alert_publication(
                scope, config, cycles=(receipt(OptionsEvidenceIdentity(lineage_text)),), lineage=V2, **at)


# --- Package context -----------------------------------------------------------------------------------------

def _trade_window(cls, scope, matrix, **extra):
    from options.data.polygon_developer import PolygonDeveloperEngine

    contract = next(row for row in scope.catalog if row.underlyer == matrix.analysis.underlying.underlyer)
    event = PolygonDeveloperEngine._trade_event(contract, matrix.ingress.batch_id,
        dict(sip_timestamp=int((scope.cutoff - timedelta(seconds=1)).timestamp()) * 1_000_000_000,
             sequence_number=1, price=2.5, size=10, exchange=1, conditions=[219]), scope.available_after)
    database = V2_DATABASE if cls is ForwardTradeWindowV2 else "stocks_realtime_v1"
    return cls(scope.sha256, matrix.matrix_id, extra.pop("database_name", database), ProviderTier.MASSIVE_DEVELOPER,
               scope.cutoff - timedelta(minutes=15), scope.cutoff, scope.available_after, scope.available_after,
               (contract.contract_id,), (event,), "COMPLETE", **extra)


def test_a_v2_trade_window_is_gated_at_construction_and_by_the_requested_lineage(cycle):
    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    v1 = _trade_window(ForwardTradeWindow, scope, matrix)
    v2 = _trade_window(ForwardTradeWindowV2, scope, matrix, identity=V2_OPTIONS_EVIDENCE_IDENTITY)
    expected = retained_trade_context(scope, matrix, v1, selected_at=scope.available_after)
    assert expected["status"] == "AVAILABLE" and expected["volume"] == 10
    assert retained_trade_context(scope, matrix, v2, selected_at=scope.available_after, lineage=V2) == expected
    assert retained_trade_context(scope, matrix, None, selected_at=scope.available_after, lineage=V2)["status"] == "UNAVAILABLE"
    with pytest.raises(ValueError, match="retained trade window scope, matrix or receipt drift"):
        retained_trade_context(scope, matrix, v2, selected_at=scope.available_after)
    with pytest.raises(ValueError, match="retained trade window scope, matrix or receipt drift"):
        retained_trade_context(scope, matrix, v1, selected_at=scope.available_after, lineage=V2)
    with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
        retained_trade_context(scope, matrix, v2, selected_at=scope.available_after, lineage="advanced_realtime_v2")
    for database in ("stocks_realtime_v1", "", " "):
        with pytest.raises(ValueError, match="retained trade window target"):
            _trade_window(ForwardTradeWindowV2, scope, matrix, database_name=database,
                          identity=V2_OPTIONS_EVIDENCE_IDENTITY)
    for lineage_text in BAD_LINEAGES:
        with pytest.raises(ValueError, match="retained trade window target"):
            _trade_window(ForwardTradeWindowV2, scope, matrix, identity=OptionsEvidenceIdentity(lineage_text))
    for database in ("", "stocks_realtime_v2", "advanced_realtime_v2", V2_DATABASE):
        with pytest.raises(ValueError, match="retained trade window target"):
            _trade_window(ForwardTradeWindow, scope, matrix, database_name=database)


def _v2_package_source(source, context, *, database_name=V2_DATABASE, identity=V2_OPTIONS_EVIDENCE_IDENTITY):
    values = {item.name: getattr(source, item.name) for item in fields(ForwardO1PackageSource)}
    return ForwardO1PackageSourceV2(**{**values, "database_name": database_name,
                                       "detector_context_sha256": context.content_sha256}, identity=identity)


def _without_context_hash(receipt, scope, matrix):
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    payload.pop("detector_context_sha256", None)
    return payload


def test_the_v2_package_gate_needs_a_v2_source_and_a_v2_detector_context(cycle):
    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    v1_source = o1_package_source(scope, matrix, context)
    v2_context = _v2_context(context)
    v2_source = _v2_package_source(v1_source, v2_context)
    args = (scope, matrix, config)
    at = dict(selected_at=scope.available_after)
    expected = load_forward_o1_package_context(*args, context, v1_source, **at)
    actual = load_forward_o1_package_context(*args, v2_context, v2_source, lineage=V2, **at)
    assert _without_context_hash(actual, scope, matrix) == _without_context_hash(expected, scope, matrix)
    assert actual.input_sha256 != expected.input_sha256
    technical_v1 = load_forward_technical_package_context(*args, context, v1_source, **at)
    technical_v2 = load_forward_technical_package_context(*args, v2_context, v2_source, lineage=V2, **at)
    assert _without_context_hash(technical_v2, scope, matrix) == _without_context_hash(technical_v1, scope, matrix)
    with_window = load_forward_technical_package_context(
        *args, v2_context, v2_source, lineage=V2, trade_window=_trade_window(
            ForwardTradeWindowV2, scope, matrix, identity=V2_OPTIONS_EVIDENCE_IDENTITY), **at)
    assert with_window.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)["trade_context"]["status"] == "AVAILABLE"
    assert with_window.input_sha256 != technical_v2.input_sha256

    with pytest.raises(TypeError, match="explicit target-local receipts"):
        load_forward_o1_package_context(*args, v2_context, v2_source, **at)
    with pytest.raises(TypeError, match="explicit target-local receipts"):
        load_forward_o1_package_context(*args, context, v1_source, lineage=V2, **at)
    with pytest.raises(ValueError, match="scope/tier/configuration/context/deadline drift"):
        load_forward_o1_package_context(*args, context, _v2_package_source(v1_source, context), lineage=V2, **at)
    with pytest.raises(ValueError, match="scope/tier/configuration/context/deadline drift"):
        load_forward_o1_package_context(*args, v2_context, v1_source, **at)
    with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
        load_forward_o1_package_context(*args, v2_context, v2_source, lineage="advanced_realtime_v2", **at)
    with pytest.raises(TypeError, match="explicit OptionsEvidenceLineage"):
        load_forward_technical_package_context(*args, v2_context, v2_source, lineage="advanced_realtime_v2", **at)


@pytest.mark.parametrize("lineage_text", BAD_LINEAGES)
def test_a_blank_or_mistyped_package_lineage_is_rejected_by_both_gates(cycle, lineage_text):
    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    v1_source = o1_package_source(scope, matrix, context)
    bad_identity = OptionsEvidenceIdentity(lineage_text)
    bad_source = _v2_package_source(v1_source, _v2_context(context), identity=bad_identity)
    bad_context = _v2_context(context, identity=bad_identity)
    at = dict(selected_at=scope.available_after)
    with pytest.raises(ValueError, match="scope/tier/configuration/context/deadline drift"):
        load_forward_o1_package_context(scope, matrix, config, _v2_context(context), bad_source, lineage=V2, **at)
    with pytest.raises(ValueError, match="scope/tier/configuration/context/deadline drift"):
        load_forward_o1_package_context(scope, matrix, config, bad_context,
                                        _v2_package_source(v1_source, bad_context), lineage=V2, **at)
    with pytest.raises(TypeError, match="explicit target-local receipts"):
        load_forward_o1_package_context(scope, matrix, config, _v2_context(context), bad_source, **at)


@pytest.mark.parametrize("database", ["", " ", "stocks_realtime_v1"])
def test_a_v2_package_source_needs_a_physical_name_that_is_not_the_v1_database(cycle, database):
    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    v2_context = _v2_context(context)
    source = _v2_package_source(o1_package_source(scope, matrix, context), v2_context, database_name=database)
    with pytest.raises(ValueError, match="scope/tier/configuration/context/deadline drift"):
        load_forward_o1_package_context(scope, matrix, config, v2_context, source, lineage=V2,
                                        selected_at=scope.available_after)


@pytest.mark.parametrize("database", ["", "stocks_realtime_v2", "advanced_realtime_v2"])
def test_the_v1_package_gate_still_accepts_only_the_exact_v1_literal(cycle, database):
    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    source = replace(o1_package_source(scope, matrix, context), database_name=database)
    with pytest.raises(ValueError, match="scope/tier/configuration/context/deadline drift"):
        load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
