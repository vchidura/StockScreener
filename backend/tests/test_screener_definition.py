import json
from dataclasses import FrozenInstanceError

import pytest

from options.discovery import CONTRACT_FILTERS, EligibleChainQuery, contract_filter_sql
from research.screening import FIELDS, GAP_CATALOG, HOURLY_CATALOG, PATTERNS, VERSION, Predicate, Query, query_generation
from research.screener_definition import (
    MAX_DEFINITION_BYTES, StockScreenDefinition, build_screener_catalog, compile_screener_definition,
    parse_screener_definition, screener_definition_schema,
)


def stock_catalog(**overrides):
    payload = dict(version=VERSION, fields=FIELDS, patterns=PATTERNS)
    payload.update(overrides)
    return build_screener_catalog("stock", payload)


def option_catalog():
    return build_screener_catalog("option_contract", {"contract_filters": CONTRACT_FILTERS})


def test_manual_and_json_stock_definitions_compile_to_the_existing_query():
    catalog = stock_catalog()
    predicate = Predicate(filters=[dict(field="vs_ema20", min=0.), dict(field="relative_volume_20", min=1.5)])
    manual = StockScreenDefinition(domain="stock", catalog_sha256=catalog.sha256, predicate=predicate)
    drafted = parse_screener_definition(manual.model_dump_json())
    expected = Query(predicate=predicate)
    compiled = compile_screener_definition(drafted, catalog)
    assert compiled == compile_screener_definition(manual, catalog)
    assert compiled.native_query() == expected


def test_option_definition_reuses_native_ranges_and_parameterized_filter_compiler():
    catalog = option_catalog()
    filters = [dict(field="calendar_dte", minimum=21., maximum=45.),
        dict(field="absolute_delta", minimum=.2, maximum=.4)]
    drafted = parse_screener_definition(json.dumps(dict(domain="option_contract",
        catalog_sha256=catalog.sha256, contract_type="CALL", filters=filters)))
    compiled = compile_screener_definition(drafted, catalog)
    manual = EligibleChainQuery(contract_type="CALL", filters=tuple(filters))
    assert compiled.native_query() == manual
    assert contract_filter_sql(compiled.native_query()) == contract_filter_sql(manual)


def test_stock_results_explanations_and_unknowns_are_unchanged():
    catalog = stock_catalog()
    predicate = Predicate(filters=[dict(field="vs_ema20", min=0.), dict(field="relative_volume_20", min=1.5)])
    definition = StockScreenDefinition(domain="stock", catalog_sha256=catalog.sha256, predicate=predicate)
    compiled = compile_screener_definition(definition, catalog)
    rows = [dict(security_id=str(index), ticker=f"TEST{index}", eligible=True,
        values=dict(vs_ema20=distance, relative_volume_20=volume), missing={}, patterns={})
        for index, (distance, volume) in enumerate(((0., 1.5), (-.1, 2.), (.1, None)))]
    generation = dict(version=VERSION, generation="a" * 64, rows=rows)
    actual = query_generation(generation, compiled.native_query())
    assert actual == query_generation(generation, Query(predicate=predicate))
    assert (actual["matched_count"], actual["nonmatch_count"], actual["unknown_count"]) == (1, 1, 1)
    assert actual["rows"][0]["explanations"][0]["state"] == "MATCH"


@pytest.mark.parametrize("domain", ["stock", "option_contract"])
def test_definition_identity_is_independent_of_filter_order(domain):
    catalog = stock_catalog() if domain == "stock" else option_catalog()
    filters = ([dict(field="price", min=10.), dict(field="volume", min=100.)] if domain == "stock"
        else [dict(field="calendar_dte", minimum=21.), dict(field="absolute_delta", minimum=.2)])
    documents = []
    for ordered in (filters, list(reversed(filters))):
        document = dict(domain=domain, catalog_sha256=catalog.sha256)
        document.update(dict(predicate=dict(filters=ordered)) if domain == "stock" else dict(filters=ordered))
        documents.append(compile_screener_definition(parse_screener_definition(document), catalog))
    assert documents[0].definition_sha256 == documents[1].definition_sha256


def test_catalog_identity_ignores_runtime_rotation_but_preserves_price_basis():
    catalog = stock_catalog(publications=[{"generation": "old"}], status="READY")
    rotated = stock_catalog(publications=[{"generation": "new"}], status="AWAITING_FIRST_PUBLICATION")
    assert catalog == rotated
    definition = StockScreenDefinition(domain="stock", catalog_sha256=catalog.sha256)
    changed_fields = {**FIELDS, "price": {**FIELDS["price"], "price_basis": "ANOTHER_BASIS"}}
    changed = stock_catalog(fields=changed_fields)
    with pytest.raises(ValueError, match="catalog changed"):
        compile_screener_definition(definition, changed)


@pytest.mark.parametrize("domain", ["stock", "option_contract"])
def test_absent_catalog_fields_are_rejected_even_for_disjoint_field_sets(domain):
    if domain == "stock":
        catalog = stock_catalog(fields={"price": FIELDS["price"]})
        definition = dict(domain=domain, catalog_sha256=catalog.sha256, predicate=dict(filters=[dict(field="volume", min=1.)]))
    else:
        catalog = build_screener_catalog(domain, {"contract_filters": {"spot": CONTRACT_FILTERS["spot"]}})
        definition = dict(domain=domain, catalog_sha256=catalog.sha256, filters=[dict(field="calendar_dte", minimum=21.)])
    with pytest.raises(ValueError, match="fields are unavailable"):
        compile_screener_definition(parse_screener_definition(definition), catalog)


@pytest.mark.parametrize("context", ["patterns", "gap", "hourly"])
def test_stock_context_cannot_be_added_without_advertised_contract(context):
    catalog = stock_catalog(patterns={})
    rules = {
        "patterns": [dict(id="hammer", direction=1)],
        "gap": dict(filters=[dict(field="direction", values=["UP"])]),
        "hourly": dict(filters=[dict(field="vs_ema20", min=0.)]),
    }
    definition = parse_screener_definition(dict(domain="stock", catalog_sha256=catalog.sha256,
        predicate={context: rules[context]}))
    with pytest.raises(ValueError, match="unavailable"):
        compile_screener_definition(definition, catalog)


def test_supported_stock_context_preserves_same_episode_and_unknown_semantics():
    catalog = stock_catalog(gaps=GAP_CATALOG, hourly=HOURLY_CATALOG)
    predicate = Predicate(patterns=[dict(id="hammer", direction=1)], pattern_mode="NONE",
        gap=dict(filters=[dict(field="direction", values=["UP"]), dict(field="formation_age", max=5.)]),
        hourly=dict(filters=[dict(field="vs_ema20", min=0.)]))
    definition = StockScreenDefinition(domain="stock", catalog_sha256=catalog.sha256, predicate=predicate)
    query = compile_screener_definition(definition, catalog).native_query()
    assert query == Query(predicate=predicate)
    generation = dict(version=VERSION, generation="a" * 64,
        gap_contract=GAP_CATALOG, hourly_contract=HOURLY_CATALOG,
        rows=[dict(security_id="1", ticker="TEST", eligible=True, values={}, missing={}, patterns={})])
    assert query_generation(generation, query)["unknown_count"] == 1


@pytest.mark.parametrize("extra", [
    {"sql": "SELECT * FROM public.option_chain_snapshots"},
    {"cohort_id": "another-cohort"}, {"provider_tier": "MASSIVE_ADVANCED"},
    {"generation": "a" * 64}, {"session_date": "2026-10-08"},
    {"any": []}, {"execution_permission": True},
])
def test_definition_cannot_supply_code_execution_scope_or_unsupported_logic(extra):
    with pytest.raises(ValueError):
        parse_screener_definition(dict(domain="option_contract", catalog_sha256=option_catalog().sha256, **extra))


@pytest.mark.parametrize("condition", [
    dict(field="absolute_delta; DROP TABLE rt.cohorts", minimum=.2),
    dict(field="absolute_delta", minimum="0.2"),
    dict(field="absolute_delta", minimum=True),
    dict(field="absolute_delta", minimum=20.),
    dict(field="calendar_dte", minimum=21.5),
    dict(field="calendar_dte", minimum=45., maximum=21.),
    dict(field="local_iv", minimum=float("nan")),
    dict(field="day_volume", operator="gt", minimum=100.),
    dict(field="local_iv", minimum=20., unit="percent"),
])
def test_invalid_option_rules_are_not_coerced_or_approximated(condition):
    with pytest.raises(ValueError):
        parse_screener_definition(dict(domain="option_contract", catalog_sha256=option_catalog().sha256, filters=[condition]))


def test_definition_size_version_duplicate_and_cross_domain_guards():
    catalog = option_catalog()
    for changes in (
        dict(schema_version="screener_definition_v999"),
        dict(filters=[dict(field="day_volume", minimum=1.)] * 2),
        dict(underlyer="AAPL' OR TRUE"),
    ):
        with pytest.raises(ValueError):
            parse_screener_definition(dict(domain="option_contract", catalog_sha256=catalog.sha256, **changes))
    with pytest.raises(ValueError, match="byte budget"):
        parse_screener_definition(" " * (MAX_DEFINITION_BYTES + 1))
    definition = StockScreenDefinition(domain="stock", catalog_sha256=catalog.sha256)
    with pytest.raises(ValueError, match="catalog changed"):
        compile_screener_definition(definition, catalog)


def test_schema_and_compiled_artifact_are_detached_from_mutable_requests():
    catalog = stock_catalog()
    definition = StockScreenDefinition(domain="stock", catalog_sha256=catalog.sha256,
        predicate=Predicate(filters=[dict(field="change", min=.1)]))
    compiled = compile_screener_definition(definition, catalog)
    definition.predicate.filters[0].min = .9
    compiled.native_query().predicate.filters[0].min = .8
    assert compiled.native_query().predicate.filters[0].min == .1
    assert catalog.document()["fields"]["change"]["unit"] == "fraction"
    with pytest.raises(FrozenInstanceError):
        compiled.request_json = "{}"
    schema = screener_definition_schema()
    assert schema["discriminator"]["propertyName"] == "domain"
    assert set(schema["discriminator"]["mapping"]) == {"stock", "option_contract"}