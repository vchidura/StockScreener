import json
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pytest

BACKEND = Path(__file__).resolve().parents[2]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from options.analytics.analysis_engine import OptionAnalysisEngine
from options.analytics.alert_selection import load_detector_run
from options.analytics.marks import UnderlyingMinuteBar
from options.config import load_option_runtime_configuration
from options.data.normalizer import (
    DeveloperNormalizationInput, DeveloperOptionNormalizer, parse_polygon_snapshot,
)
from options.detector_collection import DetectorCycleInputs
from equity.domain import (
    BarAvailabilityMode, BarSourceKind, EquityBarRevision, EquityCorporateActionCoverage, SecurityReferenceRevision,
)
from options.calendar import OptionExchangeCalendar
from options.repositories.daily_facts import DailyOpenInterestRecord
from options.domain import (
    AssetType, CatalogEligibility, ContractType, DecisionContext, ExerciseStyle,
    OptionContractCatalogEntry,
)
from realtime.contracts import CANARY_UNIVERSE, canonical_sha256
from realtime.forward_ingestion import (
    CompletedDeveloperMatrixSource, DeveloperMatrixPage, ForwardMatrixScope,
    ForwardActivityHistoryEvidence, ForwardDetectorContextSource, ForwardExchangeSessionEvidence,
    ForwardOpenInterestEvidence, load_forward_detector_context,
    OfflineForwardPublicationSlot, admit_developer_matrix, forward_detector_dataset_id, normalize_developer_matrix,
    seal_offline_forward_publication,
)
from realtime.option_discovery_scope import RealtimeOptionDiscoveryScope
from realtime.option_session_manifest import (
    BoundedOptionManifestPolicy, OptionManifestCandidate, build_bounded_option_session_manifest,
)
from realtime.preflight import DetectorFamily, ProviderTier


MARKET = datetime(2026, 10, 7, 15, 0, tzinfo=timezone.utc)
RECEIVED = MARKET + timedelta(minutes=15)
BATCH = uuid5(NAMESPACE_URL, "offline-forward-matrix")


def configuration(**overrides):
    symbols = CANARY_UNIVERSE.option_underlyings
    etfs = tuple(symbol for symbol in symbols if symbol in {"SPY", "QQQ", "IWM"})
    stocks = tuple(symbol for symbol in symbols if symbol not in etfs)
    return load_option_runtime_configuration({
        "POLYGON_API_KEY": "offline-only",
        "OPTION_FIXED_STOCK_UNDERLYERS": ",".join(stocks),
        "OPTION_FIXED_ETF_UNDERLYERS": ",".join(etfs),
        "OPTION_STOCK_UNIVERSE_SIZE": str(len(stocks)),
        "OPTION_ETF_UNIVERSE_SIZE": str(len(etfs)),
        "OPTION_RISK_FREE_RATE_SOURCE": "us_treasury_par_yield_v1",
        "OPTION_DIVIDEND_INPUT_SOURCE": "polygon_corporate_actions_v1",
        **overrides,
    }, BACKEND)


def inputs(symbol="SPY", first_id=1):
    result = []
    for offset, side in enumerate((ContractType.CALL, ContractType.PUT)):
        ticker = f"O:{symbol}261016{side.value[0]}00100000"
        catalog = OptionContractCatalogEntry(
            contract_id=first_id + offset, contract_ticker=ticker, underlyer=symbol,
            asset_type=AssetType.ETF, provider="polygon", provider_version="1",
            contract_type=side, expiration_date=date(2026, 10, 16),
            strike=Decimal("100"), exercise_style=ExerciseStyle.AMERICAN,
            shares_per_contract=100, primary_exchange="X",
            eligibility_status=CatalogEligibility.VALIDATED_ACTIVE, exclusion_reasons=(),
            valid_from=MARKET - timedelta(days=1), valid_to=None,
            first_observed_at=MARKET - timedelta(days=1), revised_observed_at=None,
            payload_sha256="a" * 64,
        )
        raw = parse_polygon_snapshot({
            "details": {"ticker": ticker}, "underlying_asset": {"price": 100},
            "day": {"close": 2.5, "vwap": 2.4, "volume": 200},
            "last_trade": {"sip_timestamp": int(MARKET.timestamp()) * 1_000_000_000},
            "open_interest": 100,
        }, RECEIVED)
        result.append(DeveloperNormalizationInput(
            raw=raw, catalog=catalog,
            underlying_bars=(UnderlyingMinuteBar(Decimal("100"), MARKET),),
            expiration_cutoff=datetime(2026, 10, 16, 20, 0, tzinfo=timezone.utc),
            risk_free_rate=0.04, dividend_yield=0.0,
        ))
    return tuple(result)


def test_shared_normalizer_and_analysis_are_provider_free():
    config = configuration()
    normalized = DeveloperOptionNormalizer(config.policy, config.valuation_policy).normalize(BATCH, inputs())
    assert len(normalized.matrix_snapshots) == 2
    analysis = OptionAnalysisEngine(config.policy).analyze(
        BATCH, normalized.matrix_snapshots, DecisionContext(MARKET, RECEIVED),
        received_count=2, catalog_matched_count=2, unknown_reference_count=0,
        reference_drift_failed=False, batch_complete=True,
    )
    assert analysis.chain_health.status == "COMPLETE"
    assert len(analysis.contracts) == 2
    assert analysis.underlying.total_day_volume == 400
    assert all(row.bid is None and row.ask is None for row in normalized.snapshots)
    assert "database" not in sys.modules and "main" not in sys.modules


@pytest.fixture
def cycle():
    config = configuration()
    members = tuple(item for index, symbol in enumerate(CANARY_UNIVERSE.option_underlyings)
        for item in inputs(symbol, index * 2 + 1))
    normalizer = DeveloperOptionNormalizer(config.policy, config.valuation_policy)
    candidates = tuple(OptionManifestCandidate(item.catalog.contract_id, item.catalog,
        normalizer.normalize(BATCH, (item,)).snapshots[0], "d" * 64) for item in members)
    discovery_scope = RealtimeOptionDiscoveryScope(ProviderTier.MASSIVE_DEVELOPER,
        uuid5(NAMESPACE_URL, "offline-forward-profile"), "e" * 64, CANARY_UNIVERSE)
    policy = BoundedOptionManifestPolicy(
        version="offline-forward-two-contract-probe-v1", target_dtes=(9,),
        maximum_target_dte_deviation=0, strikes_per_side_per_expiry=1,
        minimum_dte=1, maximum_dte=60, strike_corridor_fraction=Decimal("0.15"),
        minimum_day_volume=20, minimum_open_interest=100,
        maximum_contracts_per_underlying=2, maximum_option_quote_contracts=1000,
        reserved_option_quote_contracts=200,
        required_detectors=(DetectorFamily.S1, DetectorFamily.S2, DetectorFamily.O1, DetectorFamily.O3),
        unavailable_detectors=(DetectorFamily.O2,),
    )
    plan = build_bounded_option_session_manifest(discovery_scope, policy, candidates, session_date=MARKET.date())
    scope = ForwardMatrixScope(uuid5(NAMESPACE_URL, "offline-forward-cohort"), plan,
        tuple(item.catalog for item in members), config.configuration_sha256, "b" * 64, "c" * 64,
        MARKET, RECEIVED, RECEIVED + timedelta(seconds=60))
    sources = tuple(CompletedDeveloperMatrixSource(scope.cohort_id, plan.selection_sha256,
        symbol, MARKET, RECEIVED, RECEIVED,
        (DeveloperMatrixPage(None, None, canonical_sha256(symbol),
            tuple(item for item in members if item.catalog.underlyer == symbol)),))
        for symbol in scope.underlyings)
    return config, scope, sources


class EmptyPriorEvaluations:
    def prior_selected(self, **kwargs):
        return ()


def seal(config, scope, sources, **kwargs):
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config)
        for source in sources)
    return seal_offline_forward_publication(scope, matrices, config,
        selected_at=kwargs.pop("selected_at", RECEIVED),
        prior_evaluations=kwargs.pop("prior_evaluations", EmptyPriorEvaluations()), **kwargs)


def read(publication):
    return publication.read(cohort_id=publication.cohort_id, provider_tier=publication.provider_tier,
        scope_sha256=publication.scope_sha256)


def test_manifest_ingress_complete_matrices_and_immutable_projections(cycle):
    config, scope, sources = cycle
    publication = seal(config, scope, sources)
    payload = read(publication)
    assert len(sources) == 8
    assert len(payload["flow"]) == len(payload["screener"]) == 8
    assert sum(len(row["snapshots"]) for row in payload["screener"]) == 16
    assert payload["coverage_status"] == "COMPLETE"
    assert payload["alerts"]["run"]["source_matrices"]
    assert payload["alerts"]["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"
    assert payload["alerts"]["available"] is False
    assert payload["execution_permission"] is False
    assert seal(config, scope, tuple(reversed(sources))) == publication
    payload["flow"].clear()
    assert len(read(publication)["flow"]) == 8
    with pytest.raises(FrozenInstanceError):
        publication.payload_json = "{}"


def test_partial_ticker_coverage_is_frozen_not_filled_from_other_cycles(cycle):
    config, scope, sources = cycle
    missing = ((sources[-1].underlyer, ("PAGE_BUDGET_EXHAUSTED",)),)
    partial = seal(config, scope, sources[:-1], unavailable_underlyers=missing)
    before = partial.payload_json
    assert read(partial)["coverage_status"] == "PARTIAL"
    assert read(partial)["alerts"]["run"]["unavailable_underlyers"] == [[missing[0][0], list(missing[0][1])]]
    seal(config, scope, sources)
    assert partial.payload_json == before
    with pytest.raises(ValueError, match="missing-underlying"):
        seal(config, scope, sources[:-1])


@pytest.mark.parametrize("defect", ["missing", "pagination", "cycle", "tier", "future", "late", "reference"])
def test_invalid_source_never_becomes_ingress(cycle, defect):
    config, scope, sources = cycle
    source = sources[0]
    page = source.pages[0]
    if defect == "missing":
        source = replace(source, pages=(replace(page, inputs=page.inputs[:1]),))
    elif defect == "pagination":
        source = replace(source, pages=(replace(page, next_cursor="unfetched"),))
    elif defect == "cycle":
        source = replace(source, cutoff=MARKET - timedelta(minutes=15))
    elif defect == "tier":
        scope = replace(scope, selection=replace(scope.selection,
            scope=replace(scope.selection.scope, provider_tier=ProviderTier.MASSIVE_ADVANCED)))
    elif defect == "future":
        bad = replace(page.inputs[0], raw=replace(page.inputs[0].raw, option_mark_time=MARKET + timedelta(seconds=1)))
        source = replace(source, pages=(replace(page, inputs=(bad, page.inputs[1])),))
    elif defect == "late":
        source = replace(source, completed_at=scope.deadline + timedelta(seconds=1))
    else:
        bad = replace(page.inputs[0], catalog=replace(page.inputs[0].catalog, payload_sha256="f" * 64))
        source = replace(source, pages=(replace(page, inputs=(bad, page.inputs[1])),))
    with pytest.raises(ValueError):
        admit_developer_matrix(scope, source)


def test_stream_event_is_not_a_developer_snapshot(cycle):
    _, scope, _ = cycle
    with pytest.raises(TypeError, match="never stream events"):
        admit_developer_matrix(scope, {"ev": "Q", "bp": 2, "ap": 3})


def test_duplicate_rows_and_conflicting_revisions(cycle):
    config, scope, sources = cycle
    source = sources[0]
    page = source.pages[0]
    duplicate = replace(source, pages=(replace(page, inputs=page.inputs + page.inputs[:1]),))
    assert len(admit_developer_matrix(scope, duplicate).inputs) == 2
    changed = replace(page.inputs[0], risk_free_rate=0.05)
    with pytest.raises(ValueError, match="conflicting"):
        admit_developer_matrix(scope, replace(source, pages=(replace(page, inputs=page.inputs + (changed,)),)))
    matrix = normalize_developer_matrix(admit_developer_matrix(scope, source), config)
    broken = replace(matrix, snapshots=matrix.snapshots[:1])
    with pytest.raises(ValueError, match="content changed"):
        seal_offline_forward_publication(scope, (broken,), config, selected_at=RECEIVED,
            prior_evaluations=EmptyPriorEvaluations())


def test_stored_reads_do_no_producer_work_or_scope_fallback(cycle, monkeypatch):
    import realtime.forward_ingestion as forward

    config, scope, sources = cycle
    publication = seal(config, scope, sources)

    def forbidden(*args, **kwargs):
        raise AssertionError("GET-side production is forbidden")

    monkeypatch.setattr(forward, "normalize_developer_matrix", forbidden)
    monkeypatch.setattr(forward, "admit_developer_matrix", forbidden)
    monkeypatch.setattr(forward, "load_forward_detector_context", forbidden)
    monkeypatch.setattr(forward, "DetectorCycleCollector", forbidden)
    monkeypatch.setattr(DeveloperOptionNormalizer, "normalize", forbidden)
    assert read(publication)["coverage_status"] == "COMPLETE"
    for identity in (
        dict(cohort_id=uuid5(NAMESPACE_URL, "other"), provider_tier=publication.provider_tier,
            scope_sha256=scope.sha256),
        dict(cohort_id=scope.cohort_id, provider_tier=ProviderTier.MASSIVE_ADVANCED,
            scope_sha256=scope.sha256),
        dict(cohort_id=scope.cohort_id, provider_tier=publication.provider_tier,
            scope_sha256=replace(scope, action_sha256="f" * 64).sha256),
    ):
        with pytest.raises(ValueError, match="fallback prohibited"):
            publication.read(**identity)


def test_publication_fences_idempotency_and_late_coverage(cycle):
    config, scope, sources = cycle
    missing = ((sources[-1].underlyer, ("PROVIDER_ERROR",)),)
    publication = seal(config, scope, sources[:-1], unavailable_underlyers=missing)
    slot = OfflineForwardPublicationSlot(scope, owner_epoch=2)
    commit = dict(owner_epoch=2, current_scope_sha256=scope.sha256, checked_at=RECEIVED)
    assert slot.commit(publication, **commit) == publication
    assert slot.commit(publication, **commit) == publication
    with pytest.raises(ValueError, match="immutable publication"):
        slot.commit(seal(config, scope, sources), **commit)
    for change in (dict(owner_epoch=1), dict(current_scope_sha256="f" * 64),
                   dict(checked_at=scope.deadline + timedelta(seconds=1))):
        with pytest.raises(ValueError, match="fence"):
            slot.commit(publication, **{**commit, **change})


def test_raw_payload_and_stale_matrix_are_not_repaired(cycle):
    config, scope, sources = cycle
    source = sources[0]
    page = source.pages[0]
    changed = replace(page.inputs[0], raw=replace(page.inputs[0].raw, day_close=Decimal("9")))
    with pytest.raises(ValueError, match="raw payload changed"):
        admit_developer_matrix(scope, replace(source, pages=(replace(page, inputs=(changed, page.inputs[1])),)))
    old_time = MARKET - timedelta(hours=1)
    stale = []
    for item in page.inputs:
        payload = json.loads(item.raw.raw_payload_json)
        payload["last_trade"]["sip_timestamp"] = int(old_time.timestamp()) * 1_000_000_000
        stale.append(replace(item, raw=parse_polygon_snapshot(payload, RECEIVED),
            underlying_bars=(UnderlyingMinuteBar(Decimal("100"), old_time),)))
    ingress = admit_developer_matrix(scope, replace(source, pages=(replace(page, inputs=tuple(stale)),)))
    with pytest.raises(ValueError, match="exact complete matrix"):
        normalize_developer_matrix(ingress, config)


def test_developer_cannot_acquire_o2_capability_through_detector_inputs(cycle):
    config, scope, sources = cycle
    with pytest.raises(ValueError, match="manifest disables O2"):
        seal(config, scope, sources, detector_inputs=DetectorCycleInputs(matrices=(), surface_inputs=({},)))


def test_empty_detector_carrier_is_not_available_context(cycle):
    config, scope, sources = cycle
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config)
        for source in sources)
    carrier = DetectorCycleInputs(matrices=tuple(dict(underlying=matrix.analysis.underlying.underlyer,
        matrix_id=matrix.matrix_id, market_time=max(row.market_data_time for row in matrix.snapshots),
        observed_time=matrix.ingress.source.completed_at) for matrix in matrices))
    publication = seal_offline_forward_publication(scope, matrices, config, selected_at=RECEIVED,
        prior_evaluations=EmptyPriorEvaluations(), detector_inputs=carrier)
    assert read(publication)["alerts"]["available"] is False
    assert read(publication)["alerts"]["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"


def context_source(config, scope, matrices):
    from importlib.metadata import version

    calendar = OptionExchangeCalendar()
    session = scope.selection.session_date
    observed = RECEIVED - timedelta(hours=1)
    securities, oi, bars, coverage = [], [], [], []
    for matrix in matrices:
        symbol = matrix.analysis.underlying.underlyer
        identity = uuid5(NAMESPACE_URL, f"forward-security:{symbol}")
        securities.append(SecurityReferenceRevision(
            security_revision_id=uuid5(identity, "revision"), security_id=identity, ticker=symbol, active=True,
            company_name=None, security_type=None, cik=None, composite_figi=None, share_class_figi=None,
            primary_exchange="X", sic_code=None, sic_description=None, sector=None, industry=None, list_date=None,
            delisted_date=None, weighted_shares=None, free_float=None, free_float_percent=None, market_cap=None,
            source="polygon", effective_from=MARKET - timedelta(days=100), observed_at=observed,
            payload_sha256=canonical_sha256(symbol), raw_payload_json="{}"))
        for snapshot in matrix.snapshots:
            oi.append(ForwardOpenInterestEvidence(uuid5(identity, f"oi:{snapshot.contract_id}"),
                DailyOpenInterestRecord(snapshot.contract_id, calendar.previous_session(session), symbol,
                    snapshot.open_interest, RECEIVED, session, snapshot.batch_id),
                "PROVIDER_CHAIN_SNAPSHOT", RECEIVED))
        trend_slots = []
        for day in (*calendar.trailing_sessions_before(session, 20), session):
            start, close = calendar.session_open(day), calendar.session_close(day)
            end = start + timedelta(minutes=30)
            while end <= min(close, scope.cutoff):
                trend_slots.append((day, end - timedelta(minutes=30), end))
                end += timedelta(minutes=30)
        daily_slots = [(day, calendar.session_open(day), calendar.session_close(day))
            for day in calendar.trailing_sessions_before(session, 21)]
        for interval, slots in (("30m", trend_slots[-202:]), ("1d", daily_slots)):
            for index, (day, start, end) in enumerate(slots):
                price = Decimal("100") + Decimal("0.05") * index
                bar = EquityBarRevision(uuid5(identity, f"{interval}:{end.isoformat()}"), identity, symbol, interval,
                    day, start, end, price, price + Decimal("0.2"), price - Decimal("0.2"), price,
                    Decimal("1000000"), None, None, BarSourceKind.NATIVE_REST, BarAvailabilityMode.LIVE_OBSERVED,
                    True, end + timedelta(seconds=20), None, False, canonical_sha256((symbol, interval, end, price)))
                bars.append((bar, end + timedelta(seconds=30)))
        coverage.append((EquityCorporateActionCoverage(uuid5(identity, "split-coverage"), "SPLIT", symbol,
            trend_slots[-202][0], session, "polygon", f"split:{symbol}", observed,
            BarAvailabilityMode.LIVE_OBSERVED, None, canonical_sha256((symbol, "splits")),
            identity, 0, canonical_sha256(())), observed))
    exchange = ForwardExchangeSessionEvidence(calendar.calendar_name, version("exchange_calendars"), session,
        calendar.previous_session(session), calendar.session_open(session), calendar.session_close(session), observed, observed)
    history = ForwardActivityHistoryEvidence(forward_detector_dataset_id(scope, config), scope.cutoff, (), RECEIVED, RECEIVED)
    return ForwardDetectorContextSource(scope.sha256, tuple(row.content_sha256 for row in matrices),
        database_name="stocks_realtime_v1", provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        securities=tuple(securities), open_interest=tuple(oi), bars=tuple(bars), split_coverage=tuple(coverage),
        exchange_session=exchange, activity_history=history)


def test_target_local_context_complete_zero_activity_is_not_original_page_success(cycle):
    config, scope, sources = cycle
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config) for source in sources)
    context = load_forward_detector_context(scope, matrices, config, context_source(config, scope, matrices), selected_at=RECEIVED)
    assert context.status == "COMPLETE"
    assert context.inputs.o1_observations == ()
    publication = seal_offline_forward_publication(scope, matrices, config, selected_at=RECEIVED,
        prior_evaluations=EmptyPriorEvaluations(), detector_context=context)
    payload = read(publication)
    assert payload["detector_context"]["status"] == "COMPLETE"
    assert payload["alerts"]["available"] is False
    assert payload["alerts"]["reason"] == "PACKAGE_CONTEXT_UNAVAILABLE"
    assert payload["original_page_contracts"] is False


def context_cycle(cycle, *, shift=timedelta(0), volume=400):
    config, scope, sources = cycle
    scope = replace(scope, cutoff=scope.cutoff + shift,
        available_after=scope.available_after + shift, deadline=scope.deadline + shift)
    changed = []
    for source in sources:
        items = []
        for item in source.pages[0].inputs:
            payload = json.loads(item.raw.raw_payload_json)
            payload["day"]["volume"] = volume
            payload["last_trade"]["sip_timestamp"] = int(scope.cutoff.timestamp()) * 1_000_000_000
            items.append(replace(item, raw=parse_polygon_snapshot(payload, scope.available_after),
                underlying_bars=(UnderlyingMinuteBar(Decimal("100"), scope.cutoff),)))
        changed.append(replace(source, cutoff=scope.cutoff, started_at=scope.available_after,
            completed_at=scope.available_after,
            pages=(DeveloperMatrixPage(None, None, canonical_sha256(tuple(items)), tuple(items)),)))
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config) for source in changed)
    return config, scope, matrices, context_source(config, scope, matrices)


def publish_context(config, scope, matrices, source, *, prior=None, selected_at=None, unavailable_underlyers=()):
    selected_at = selected_at or scope.available_after
    context = load_forward_detector_context(scope, matrices, config, source, selected_at=selected_at)
    publication = seal_offline_forward_publication(scope, matrices, config, selected_at=selected_at,
        prior_evaluations=prior or EmptyPriorEvaluations(), detector_context=context,
        unavailable_underlyers=unavailable_underlyers)
    return context, publication


def test_native_o1_context_positive_observations_and_immutable_receipt(cycle, monkeypatch):
    import realtime.forward_ingestion as forward
    from options.analytics.alert_selection import load_evaluation_evidence

    config, scope, matrices, source = context_cycle(cycle)
    context, publication = publish_context(config, scope, matrices, source)
    payload = read(publication)
    assert context.status == "COMPLETE"
    assert len(context.inputs.o1_observations) == len(payload["alerts"]["records"]) == 16
    records = tuple(load_evaluation_evidence(json.dumps(row)) for row in payload["alerts"]["records"])
    assert {row.detector_id for row in records} == {"O1"}
    assert {row.observation.baseline_disposition for row in records} == {"CONFIRMED", "CONTRADICTED"}
    assert all(row.observation.volume_oi_ratio == 4 for row in records)
    assert all(row.observation.package_status == "NOT_ASSESSED" for row in records)
    assert payload["alerts"]["available"] is False
    reversed_source = replace(source, securities=tuple(reversed(source.securities)),
        open_interest=tuple(reversed(source.open_interest)), bars=tuple(reversed(source.bars)),
        split_coverage=tuple(reversed(source.split_coverage)))
    assert publish_context(config, scope, tuple(reversed(matrices)), reversed_source) == (context, publication)
    with pytest.raises(TypeError):
        context.inputs.matrices[0]["matrix_id"] = BATCH
    with pytest.raises(ValueError, match="receipt changed"):
        seal_offline_forward_publication(scope, matrices, config, selected_at=RECEIVED,
            prior_evaluations=EmptyPriorEvaluations(), detector_context=replace(context, dependencies=()))
    slot = OfflineForwardPublicationSlot(scope, 1)
    commit = dict(owner_epoch=1, current_scope_sha256=scope.sha256, checked_at=RECEIVED)
    assert slot.commit(publication, **commit) == slot.commit(publication, **commit)
    _, changed = publish_context(config, scope, matrices, replace(source, activity_history=None))
    with pytest.raises(ValueError, match="immutable publication"):
        slot.commit(changed, **commit)

    def forbidden(*args, **kwargs):
        raise AssertionError("stored reads must not load context or compute")

    monkeypatch.setattr(forward, "load_forward_detector_context", forbidden)
    monkeypatch.setattr(forward, "build_canonical_trend_evidence", forbidden)
    monkeypatch.setattr(forward, "DetectorCycleCollector", forbidden)
    assert read(publication) == payload


@pytest.mark.parametrize(("defect", "reason"), [
    ("oi", "DATED_OPEN_INTEREST_UNAVAILABLE"),
    ("oi_date", "DATED_OPEN_INTEREST_UNAVAILABLE"),
    ("history", "INSUFFICIENT_30M_HISTORY"),
    ("equity", "STOCK_SECURITY_IDENTITY_UNAVAILABLE"),
    ("daily", "INSUFFICIENT_DAILY_HISTORY"),
    ("stale_window", "EXACT_CURRENT_30M_UNAVAILABLE"),
    ("split", "SPLIT_COVERAGE_UNAVAILABLE_OR_CROSSED"),
    ("stale_split", "SPLIT_COVERAGE_UNAVAILABLE_OR_CROSSED"),
    ("calendar", "EXCHANGE_CALENDAR_UNAVAILABLE"),
    ("activity_history", "ACTIVITY_HISTORY_UNAVAILABLE"),
])
def test_missing_or_stale_context_is_unavailable_not_zero_matches(cycle, defect, reason):
    config, scope, matrices, source = context_cycle(cycle)
    if defect == "oi":
        source = replace(source, open_interest=())
    elif defect == "oi_date":
        source = replace(source, open_interest=tuple(replace(row,
            record=replace(row.record, settlement_session=scope.selection.session_date)) for row in source.open_interest))
    elif defect in ("history", "daily"):
        interval = "30m" if defect == "history" else "1d"
        source = replace(source, bars=tuple(row for row in source.bars if row[0].interval != interval))
    elif defect == "stale_window":
        source = replace(source, bars=tuple(row for row in source.bars if row[0].bar_end != scope.cutoff))
    elif defect == "equity":
        source = replace(source, securities=(), bars=(), split_coverage=())
    elif defect == "split":
        source = replace(source, split_coverage=())
    elif defect == "stale_split":
        source = replace(source, split_coverage=tuple((replace(row,
            first_observed_at=RECEIVED - timedelta(hours=25)), recorded) for row, recorded in source.split_coverage))
    elif defect == "calendar":
        source = replace(source, exchange_session=None)
    else:
        source = replace(source, activity_history=None)
    context, publication = publish_context(config, scope, matrices, source)
    payload = read(publication)
    assert context.status == payload["detector_context"]["status"] == "UNAVAILABLE"
    assert all(reason in reasons for _, reasons in context.dependencies)
    assert dict(context.inputs.rejections)[reason] == 8
    assert payload["alerts"]["available"] is False
    assert payload["original_page_contracts"] is False
    assert not payload["alerts"]["records"] or defect not in {"oi", "oi_date", "equity", "calendar"}
    if defect == "activity_history":
        assert all(next(row for row in observation.measurements
            if row.metric_id == "prior_same_direction_activity").status == "UNAVAILABLE"
            for observation in context.inputs.o1_observations)


def test_partial_context_and_missing_matrices_have_distinct_frozen_coverage(cycle):
    config, scope, matrices, source = context_cycle(cycle)
    missing_symbol = matrices[-1].analysis.underlying.underlyer
    partial_source = replace(source, open_interest=tuple(row for row in source.open_interest
        if row.record.underlying != missing_symbol))
    context, publication = publish_context(config, scope, matrices, partial_source)
    assert context.status == "PARTIAL"
    assert read(publication)["coverage_status"] == "COMPLETE"
    assert len(context.inputs.o1_observations) == 14
    fewer = matrices[:-1]
    smaller_source = context_source(config, scope, fewer)
    smaller_context, smaller_publication = publish_context(config, scope, fewer, smaller_source,
        unavailable_underlyers=((missing_symbol, ("MATRIX_UNAVAILABLE",)),))
    assert smaller_context.status == "COMPLETE"
    assert read(smaller_publication)["coverage_status"] == "PARTIAL"
    assert read(smaller_publication)["unavailable_underlyers"] == [[missing_symbol, ["MATRIX_UNAVAILABLE"]]]


@pytest.mark.parametrize("defect", ["scope", "action_scope", "matrix", "canonical", "tier", "calendar", "reference", "bar_tier",
    "bar_basis", "bar_receipt", "bar_revision", "oi_identity", "oi_source", "oi_revision", "history_dataset", "late"])
def test_context_scope_drift_never_substitutes_other_evidence(cycle, defect):
    config, scope, matrices, source = context_cycle(cycle)
    selected_at = RECEIVED
    with pytest.raises(ValueError):
        if defect == "scope":
            scope = replace(scope, reference_sha256="f" * 64)
        elif defect == "action_scope":
            scope = replace(scope, action_sha256="f" * 64)
        elif defect == "matrix":
            source = replace(source, matrix_sha256s=("f" * 64,))
        elif defect == "canonical":
            source = replace(source, database_name="stocks")
        elif defect == "tier":
            source = replace(source, provider_tier=ProviderTier.MASSIVE_ADVANCED)
        elif defect == "calendar":
            source = replace(source, exchange_session=replace(source.exchange_session, calendar_version="another-version"))
        elif defect == "reference":
            source = replace(source, securities=(replace(source.securities[0], observed_at=RECEIVED), *source.securities[1:]))
        elif defect in {"bar_tier", "bar_basis", "bar_receipt"}:
            bar, recorded = source.bars[0]
            if defect == "bar_tier":
                bar = replace(bar, source_kind=BarSourceKind.REALTIME_STREAM)
            elif defect == "bar_basis":
                bar = replace(bar, adjusted=True)
            else:
                recorded = RECEIVED + timedelta(seconds=1)
            source = replace(source, bars=((bar, recorded), *source.bars[1:]))
        elif defect == "bar_revision":
            source = replace(source, bars=(*source.bars, source.bars[0]))
        elif defect in {"oi_identity", "oi_source", "oi_revision"}:
            row = source.open_interest[0]
            if defect == "oi_identity":
                row = replace(row, record=replace(row.record, batch_id=BATCH))
            elif defect == "oi_source":
                row = replace(row, source="INTRADAY_STREAM")
            else:
                row = replace(row, revision_count=1)
            source = replace(source, open_interest=(row, *source.open_interest[1:]))
        elif defect == "history_dataset":
            source = replace(source, activity_history=replace(source.activity_history, dataset_id="other-dataset"))
        else:
            selected_at = scope.deadline + timedelta(seconds=1)
        load_forward_detector_context(scope, matrices, config, source, selected_at=selected_at)


def test_oi_available_after_snapshot_does_not_clear_dated_evidence_gate(cycle):
    config, scope, matrices, source = context_cycle(cycle)
    selected_at = RECEIVED + timedelta(seconds=1)
    source = replace(source, open_interest=tuple(replace(row,
        record=replace(row.record, observed_at=selected_at), recorded_at=selected_at) for row in source.open_interest))
    context, publication = publish_context(config, scope, matrices, source, selected_at=selected_at)
    assert context.status == "UNAVAILABLE"
    assert all("OPEN_INTEREST_AFTER_SNAPSHOT" in reasons for _, reasons in context.dependencies)
    assert read(publication)["alerts"]["reason"] == "DETECTOR_CONTEXT_UNAVAILABLE"


def test_dataset_identity_pins_native_context_policy_but_not_source_receipts(cycle, monkeypatch):
    import realtime.forward_ingestion as forward

    config, scope, _, _ = context_cycle(cycle)
    identity = forward_detector_dataset_id(scope, config)
    assert forward_detector_dataset_id(replace(scope, reference_sha256="f" * 64), config) == identity
    monkeypatch.setattr(forward, "forward_context_policy_sha256", lambda: "0" * 64)
    assert forward_detector_dataset_id(scope, config) != identity


def test_native_o1_recurrence_crosses_cutoffs_without_rewriting_prior_publication(cycle):
    class PriorObservations(EmptyPriorEvaluations):
        def __init__(self):
            self.calls = []
            self.recurrences = set()

        def prior_o1_observed(self, **kwargs):
            self.calls.append(kwargs)
            return self.recurrences

    prior = PriorObservations()
    config, scope, matrices, source = context_cycle(cycle)
    context, first = publish_context(config, scope, matrices, source, prior=prior)
    before = first.payload_json
    prior.recurrences = {row.recurrence_sha256 for row in context.inputs.o1_observations}
    config, later_scope, later_matrices, later_source = context_cycle(cycle, shift=timedelta(minutes=15))
    later_source = replace(later_source, activity_history=replace(later_source.activity_history,
        directions=tuple((symbol, direction) for symbol in scope.underlyings for direction in (-1, 1))))
    later_context, second = publish_context(config, later_scope, later_matrices, later_source, prior=prior)
    assert first.dataset_id == second.dataset_id == prior.calls[0]["dataset_id"] == prior.calls[1]["dataset_id"]
    assert first.scope_sha256 != second.scope_sha256
    assert load_detector_run(json.dumps(read(first)["alerts"]["run"])).run_id != load_detector_run(
        json.dumps(read(second)["alerts"]["run"])).run_id
    assert len(later_context.inputs.o1_observations) == 16
    assert read(second)["alerts"]["records"] == []
    assert first.payload_json == before
    assert "O1_REPEAT_OBSERVATION" in json.dumps(read(second)["alerts"]["run"])


def test_adjacent_cutoffs_share_dataset_and_prior_lookup_but_not_run(cycle):
    config, scope, sources = cycle
    calls = []

    class RecordingPriorEvaluations(EmptyPriorEvaluations):
        def prior_selected(self, **kwargs):
            calls.append(kwargs)
            return ()

    prior = RecordingPriorEvaluations()
    first = seal(config, scope, sources, prior_evaluations=prior)
    shift = timedelta(minutes=15)
    later_scope = replace(scope, cutoff=scope.cutoff + shift,
        available_after=scope.available_after + shift, deadline=scope.deadline + shift,
        reference_sha256="e" * 64, action_sha256="f" * 64)
    later_sources = []
    for source in sources:
        pages = []
        for page in source.pages:
            later_inputs = []
            for item in page.inputs:
                payload = json.loads(item.raw.raw_payload_json)
                payload["last_trade"]["sip_timestamp"] = int((MARKET + shift).timestamp()) * 1_000_000_000
                later_inputs.append(replace(item, raw=parse_polygon_snapshot(payload, RECEIVED + shift),
                    underlying_bars=(UnderlyingMinuteBar(Decimal("100"), MARKET + shift),)))
            pages.append(replace(page, response_sha256=canonical_sha256(tuple(later_inputs)), inputs=tuple(later_inputs)))
        later_sources.append(replace(source, cutoff=source.cutoff + shift,
            started_at=source.started_at + shift, completed_at=source.completed_at + shift, pages=tuple(pages)))
    second = seal(config, later_scope, tuple(later_sources), selected_at=RECEIVED + shift, prior_evaluations=prior)
    assert first.dataset_id == second.dataset_id
    assert first.scope_sha256 != second.scope_sha256
    first_run = load_detector_run(json.dumps(read(first)["alerts"]["run"]))
    second_run = load_detector_run(json.dumps(read(second)["alerts"]["run"]))
    assert first_run.run_id != second_run.run_id
    assert first_run.scope_sha256 == second_run.scope_sha256
    assert calls[0]["dataset_id"] == calls[1]["dataset_id"] == first.dataset_id
    assert calls[0]["scheduled_cycle"] < calls[1]["scheduled_cycle"]


def test_dataset_identity_keeps_cohort_tier_manifest_and_policy_isolation(cycle):
    config, scope, _ = cycle
    identity = forward_detector_dataset_id(scope, config)
    changed_scopes = (
        replace(scope, cohort_id=uuid5(NAMESPACE_URL, "another-forward-cohort")),
        replace(scope, selection=replace(scope.selection, selection_sha256="1" * 64)),
        replace(scope, selection=replace(scope.selection,
            scope=replace(scope.selection.scope, provider_tier=ProviderTier.MASSIVE_ADVANCED))),
    )
    assert all(forward_detector_dataset_id(changed, config) != identity for changed in changed_scopes)
    changed_policy = replace(config, strategy_policy_sha256="2" * 64)
    assert forward_detector_dataset_id(scope, changed_policy) != identity
    with pytest.raises(ValueError, match="configuration drift"):
        forward_detector_dataset_id(replace(scope, configuration_sha256="3" * 64), config)


def o1_package_cycle(cycle, *, forward_policy=False, credit_policy=False, shift=timedelta(0)):
    config, scope, sources = cycle
    if forward_policy or credit_policy:
        config = configuration(OPTION_STRATEGY_POLICY_FILE=("options/policies/strategy_o3_credit_v2.json" if credit_policy
            else "options/policies/strategy_technical_forward_v2.json"),
            OPTION_VALUATION_POLICY_FILE="options/policies/valuation_raw_spot_v2.json")
    open_interest = max(500 if credit_policy else 300, config.strategy_policy.long_premium.minimum_open_interest)
    volume = max(open_interest * 4, config.strategy_policy.long_premium.minimum_day_volume)
    changed = []
    for source in sources:
        members = []
        for item in source.pages[0].inputs:
            payload = json.loads(item.raw.raw_payload_json)
            payload["open_interest"] = max(open_interest, payload["open_interest"]) if credit_policy else open_interest
            payload["day"]["volume"] = max(payload["open_interest"] * 4, volume)
            members.append(replace(item, raw=parse_polygon_snapshot(payload, scope.available_after)))
        changed.append(replace(source, pages=(DeveloperMatrixPage(None, None, canonical_sha256(tuple(members)), tuple(members)),)))
    if forward_policy or credit_policy:
        normalizer = DeveloperOptionNormalizer(config.policy, config.valuation_policy)
        members = tuple(item for source in changed for item in source.pages[0].inputs)
        candidates = tuple(OptionManifestCandidate(item.catalog.contract_id, item.catalog,
            normalizer.normalize(BATCH, (item,)).snapshots[0], "d" * 64) for item in members)
        selection = build_bounded_option_session_manifest(scope.selection.scope, scope.selection.policy, candidates,
            session_date=scope.selection.session_date)
        scope = replace(scope, cohort_id=uuid5(NAMESPACE_URL, "offline-o1-package-forward-v2-cohort"),
            selection=selection, configuration_sha256=config.configuration_sha256)
        changed = [replace(source, cohort_id=scope.cohort_id, selection_sha256=selection.selection_sha256) for source in changed]
    if shift:
        scope = replace(scope, cutoff=scope.cutoff + shift, available_after=scope.available_after + shift,
            deadline=scope.deadline + shift)
        later = []
        for source in changed:
            members = []
            for item in source.pages[0].inputs:
                payload = json.loads(item.raw.raw_payload_json)
                payload["last_trade"]["sip_timestamp"] = int(scope.cutoff.timestamp()) * 1_000_000_000
                members.append(replace(item, raw=parse_polygon_snapshot(payload, scope.available_after),
                    underlying_bars=(UnderlyingMinuteBar(Decimal("100"), scope.cutoff),)))
            later.append(replace(source, cutoff=scope.cutoff, started_at=scope.available_after, completed_at=scope.available_after,
                pages=(DeveloperMatrixPage(None, None, canonical_sha256(tuple(members)), tuple(members)),)))
        changed = later
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config) for source in changed)
    source = context_source(config, scope, matrices)
    highs, lows = set(), set()
    for symbol in scope.underlyings:
        history = [bar for bar, _ in source.bars if bar.ticker == symbol and bar.interval == "30m"]
        highs.add(history[-6].bar_revision_id)
        lows.add(history[-12].bar_revision_id)
    bars = []
    for bar, recorded in source.bars:
        if bar.interval == "30m":
            offset = Decimal("10.05")
            bar = replace(bar, open_price=bar.open_price - offset, high_price=bar.high_price - offset,
                low_price=bar.low_price - offset, close_price=bar.close_price - offset,
                payload_sha256=canonical_sha256((bar.payload_sha256, "package-coherent-spot", offset)))
            if bar.bar_revision_id in highs:
                bar = replace(bar, high_price=Decimal("104"),
                    payload_sha256=canonical_sha256((bar.payload_sha256, "completed-resistance")))
            if bar.bar_revision_id in lows:
                bar = replace(bar, low_price=Decimal("98.7"),
                    payload_sha256=canonical_sha256((bar.payload_sha256, "completed-support")))
        bars.append((bar, recorded))
    source = replace(source, bars=tuple(bars))
    context = load_forward_detector_context(scope, matrices, config, source, selected_at=scope.available_after)
    return config, scope, matrices, context


def credit_package_cycle(cycle):
    config, scope, sources = cycle
    changed = []
    for source in sources:
        members = []
        for item in source.pages[0].inputs:
            terms = tuple(zip((Decimal(100 + index) for index in range(10)),
                (2.5, 2.2, 1.9, 1.6, 1.3, 1.0, 0.8, 0.6, 0.45, 0.3))) if item.catalog.contract_type is ContractType.CALL else (
                tuple(zip((Decimal(90 + index) for index in range(10)),
                    (0.08, 0.12, 0.18, 0.25, 0.35, 0.5, 0.7, 1.0, 1.35, 1.8))))
            for index, (strike, price) in enumerate(terms):
                ticker = f"O:{item.catalog.underlyer}{item.catalog.expiration_date:%y%m%d}{item.catalog.contract_type.value[0]}{int(strike * 1000):08d}"
                catalog = replace(item.catalog, contract_id=item.catalog.contract_id + index * 1000,
                    strike=strike, contract_ticker=ticker, payload_sha256=canonical_sha256((ticker, strike)))
                payload = json.loads(item.raw.raw_payload_json)
                payload["details"]["ticker"] = ticker
                wall = strike == (Decimal("105") if item.catalog.contract_type is ContractType.CALL else Decimal("97"))
                payload["open_interest"] = 6000 if wall else 500 + index * 50
                payload["day"]["volume"] = payload["open_interest"] * 4
                payload["day"].update(close=price, vwap=price)
                members.append(replace(item, catalog=catalog, raw=parse_polygon_snapshot(payload, scope.available_after)))
        changed.append(replace(source, pages=(DeveloperMatrixPage(None, None, canonical_sha256(tuple(members)), tuple(members)),)))
    policy = replace(scope.selection.policy, strikes_per_side_per_expiry=10, maximum_contracts_per_underlying=20)
    normalizer = DeveloperOptionNormalizer(config.policy, config.valuation_policy)
    members = tuple(item for source in changed for item in source.pages[0].inputs)
    candidates = tuple(OptionManifestCandidate(item.catalog.contract_id, item.catalog,
        normalizer.normalize(BATCH, (item,)).snapshots[0], "d" * 64) for item in members)
    selection = build_bounded_option_session_manifest(scope.selection.scope, policy, candidates,
        session_date=scope.selection.session_date)
    scope = replace(scope, selection=selection, catalog=tuple(item.catalog for item in members))
    return o1_package_cycle((config, scope, tuple(changed)), credit_policy=True)


def test_native_o1_package_strategy_probe_uses_original_engine_without_clearance(cycle):
    from options.strategies.domain import StrategyContextSnapshot, StrategyContextStatus
    from options.strategies.engine import OptionStrategyEngine

    config, scope, matrices, _ = o1_package_cycle(cycle)
    matrix = matrices[0]
    context = StrategyContextSnapshot(uuid5(NAMESPACE_URL, f"offline-package-context:{matrix.matrix_id}"), matrix.matrix_id,
        matrix.analysis.underlying.underlyer, scope.cutoff, scope.available_after, StrategyContextStatus.DEGRADED,
        None, None, 0, None, None, 0, None, "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE",
        ("PACKAGE_CONTEXT_UNAVAILABLE",), (), config.policy.policy_version, config.policy_sha256)
    result = OptionStrategyEngine(config.strategy_policy, config.strategy_policy_sha256).scan(matrix.matrix_id,
        matrix.snapshots, matrix.analysis.chain_health, matrix.analysis.expirations, context)
    packages = tuple(candidate for candidate in result.candidates
        if candidate.strategy_name == "DIRECTIONAL_LONG_PREMIUM" and candidate.legs)
    assert packages, dict(reasons=[candidate.reason_codes for candidate in result.candidates
        if candidate.strategy_name == "DIRECTIONAL_LONG_PREMIUM"],
        policy={field: getattr(config.strategy_policy.long_premium, field) for field in (
            "minimum_dte", "maximum_dte", "minimum_open_interest", "minimum_day_volume")},
        snapshots=[(row.calendar_dte, row.open_interest, row.day_volume, row.local_delta) for row in matrix.snapshots])
    assert all(candidate.policy_sha256 == config.strategy_policy_sha256 and candidate.matrix_id == matrix.matrix_id for candidate in packages)
    assert all(candidate.legs[0].snapshot_id in {row.snapshot_id for row in matrix.snapshots} for candidate in packages)
    assert context.earnings_blackout_state == context.fed_blackout_state == "UNAVAILABLE"
    assert "database" not in sys.modules


def test_native_o1_package_context_sources_match_frozen_observation_hashes(cycle):
    config, scope, matrices, context = o1_package_cycle(cycle)
    assert context.status == "COMPLETE"
    assert len(context.activity_sources) == len(context.inputs.o1_observations) == 16
    assert len(context.stock_sources) == 8
    activities = {row.sha256 for row in context.activity_sources}
    stocks = {stock.sha256 for _, stock in context.stock_sources if stock is not None}
    assert all(row.activity_source_sha256 in activities and row.stock_source_sha256 in stocks
        for row in context.inputs.o1_observations)


def o1_package_source(scope, matrix, context):
    from options.detector_collection import direct_structural_policy_sha256
    from options.domain import OptionContractReference
    from options.event_calendar import parse_event_calendar_document
    from realtime.option_package_context import ForwardO1PackageSource, ForwardPackageCalendar, ForwardPackageReference

    symbol = matrix.analysis.underlying.underlyer
    security = next(row for row in context.source.securities if row.ticker == symbol)
    references = tuple(ForwardPackageReference(uuid5(NAMESPACE_URL, f"package-reference:{row.contract_id}"), row.contract_id,
        OptionContractReference(row.contract_ticker, row.underlyer, row.asset_type, row.provider, row.provider_version,
            row.contract_type.value.lower(), row.expiration_date, row.strike, row.exercise_style.value.lower(),
            row.shares_per_contract, row.primary_exchange, None, "[]", "{}", False, row.valid_from, row.valid_to,
            row.first_observed_at, row.revised_observed_at, row.first_observed_at, row.payload_sha256))
        for row in scope.catalog if row.underlyer == symbol)
    snapshot = matrix.snapshots[0]
    raw_bar = EquityBarRevision(uuid5(NAMESPACE_URL, f"package-spot:{symbol}:{snapshot.spot_market_data_time.isoformat()}"), security.security_id, symbol, "1m",
        scope.selection.session_date, snapshot.spot_market_data_time - timedelta(minutes=1), snapshot.spot_market_data_time,
        snapshot.spot, snapshot.spot, snapshot.spot, snapshot.spot, Decimal("1000"), None, None,
        BarSourceKind.NATIVE_REST, BarAvailabilityMode.LIVE_OBSERVED, True, scope.available_after,
        None, False, canonical_sha256((symbol, snapshot.spot, snapshot.spot_market_data_time)))
    observed = scope.available_after - timedelta(minutes=1)
    document = dict(schema_version=1, source="offline-calendar-v1", observed_at=observed.isoformat(), coverage=[
        dict(event_type=event_type, affected_underlying=underlying, source_key=f"{event_type}:{symbol}",
            window_start=scope.cutoff.isoformat(), window_end=(scope.available_after + timedelta(hours=2)).isoformat())
        for event_type, underlying in (("EARNINGS", symbol), ("FED_RATE_DECISION", None))], events=[])
    calendar = ForwardPackageCalendar(parse_event_calendar_document(document, received_at=observed), observed)
    return ForwardO1PackageSource(scope.sha256, matrix.content_sha256, context.content_sha256,
        "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, references, ((raw_bar, scope.available_after),),
        tuple((bar, recorded) for bar, recorded in context.source.bars if bar.ticker == symbol and bar.interval == "30m"),
        direct_structural_policy_sha256(), (calendar,), scope.available_after + timedelta(seconds=1),
        scope.available_after + timedelta(seconds=30), scope.available_after + timedelta(hours=1))


def test_native_o1_package_qualification_retains_exact_indicative_plan_not_calendar_clearance(cycle):
    from realtime.option_package_context import load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    source = o1_package_source(scope, matrix, context)
    publication = load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    payload = publication.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["native_candidate_count"] >= 1
    assert len(payload["qualified_indicative"]) == 1, dict(reasons=payload["reasons"], rejected=payload["rejected"])
    assert payload["status"] == "PARTIAL" and payload["available"] is False
    assert payload["reasons"] == ["COMPLETE_OPTIONS_EVENT_COVERAGE_UNAVAILABLE"]
    package = payload["qualified_indicative"][0]
    plan = json.loads(package["plan_payload_json"])
    assert package["package"]["status"] == "QUALIFIED_INDICATIVE"
    assert plan["management_policy"]["technical_exit"]
    assert plan["event_horizon"]["complete_options_event_coverage"] is False
    assert {row["status"] for row in plan["event_horizon"]["checks"]} == {"CLEAR"}
    assert plan["execution_permission"] is payload["publication_permission"] is False


def test_native_o1_package_default_policy_deadline_remains_blocked(cycle):
    from realtime.option_package_context import load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle)
    matrix = matrices[0]
    source = o1_package_source(scope, matrix, context)
    publication = load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    payload = publication.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["available"] is False and payload["qualified_indicative"] == []
    assert any("deadline mismatch" in reason for _, reason in payload["rejected"])
    forward_config, forward_scope, _, _ = o1_package_cycle(cycle, forward_policy=True)
    assert forward_scope.cohort_id != scope.cohort_id
    assert forward_config.configuration_sha256 != config.configuration_sha256
    assert forward_detector_dataset_id(forward_scope, forward_config) != forward_detector_dataset_id(scope, config)


def test_technical_package_receipt_preserves_o1_and_reports_other_model_dependencies(cycle):
    from realtime.option_package_context import TECHNICAL_PACKAGE_CONTEXT_CONTRACT, load_forward_technical_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    receipt = load_forward_technical_package_context(scope, matrix, config, context,
        o1_package_source(scope, matrix, context), selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["contract"] == TECHNICAL_PACKAGE_CONTEXT_CONTRACT
    assert payload["enabled_detectors"] == ["O1", "O3", "S1", "S2"]
    assert payload["detector_dependencies"]["O1"] == []
    assert payload["detector_dependencies"]["O3"] == ["O3_CREDIT_POLICY_UNAVAILABLE"]
    assert any(row["package"]["detector_id"] == "O1" for row in payload["qualified_indicative"])
    assert payload["execution_permission"] is payload["publication_permission"] is False


def test_technical_package_credit_receipt_reuses_native_credit_policy(cycle):
    from dataclasses import asdict
    from realtime.option_package_context import load_forward_technical_package_context

    config, scope, matrices, context = credit_package_cycle(cycle)
    matrix = matrices[0]
    assert len(matrix.snapshots) == 20
    assert json.loads(matrix.analysis.expirations[0].wall_clusters_json)
    assert not next(row.profile for row in matrix.analysis.gamma_profiles if row.scope.value == "TOTAL").quality_reasons
    receipt = load_forward_technical_package_context(scope, matrix, config, context,
        o1_package_source(scope, matrix, context), selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["o3_observations"], dict(dependencies=payload["detector_dependencies"], rejected=payload["rejected"])
    assert payload["detector_dependencies"]["O3"] == []
    assert len(payload["o3_candidates"]) == len(payload["o3_observations"])
    assert all(row["policy_version"] == "o3_options_first_credit_v2" and row["execution_permission"] is False
        for row in payload["o3_observations"])
    assert payload["gamma_context"]["role"] == "ADVISORY"
    assert payload["gamma_context"]["policy_sha256"] == config.gamma_policy_sha256
    assert payload["gamma_context"]["profiles"] == json.loads(json.dumps(tuple(asdict(row)
        for row in matrix.analysis.gamma_profiles), default=str))
    assert payload["trade_context"]["status"] == "UNAVAILABLE"
    assert payload["trade_context"]["volume"] is payload["trade_context"]["transacted_premium"] is None
    assert payload["trade_context"]["chain_volume_used_as_tape"] is False
    assert payload["trade_context"]["role"] == "ADVISORY" and payload["o3_observations"]


def test_technical_package_credit_does_not_inherit_debit_holding_prerequisites(cycle):
    from realtime.option_package_context import load_forward_technical_package_context

    config, scope, matrices, context = credit_package_cycle(cycle)
    matrix = matrices[0]
    source = replace(o1_package_source(scope, matrix, context), raw_spot_bars=(), calendars=(),
        planned_entry_at=None, entry_deadline=None, exit_deadline=None)
    receipt = load_forward_technical_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["o3_observations"] and payload["detector_dependencies"]["O3"] == []
    assert "EXACT_RAW_SPOT_UNAVAILABLE" in payload["detector_dependencies"]["O1"]
    assert "PACKAGE_HOLDING_CLOCKS_UNAVAILABLE" in payload["detector_dependencies"]["S1"]
    assert payload["qualified_indicative"] == []


@pytest.mark.parametrize("coverage", ("COMPLETE", "PARTIAL"))
def test_technical_package_retained_trade_context_reuses_parser_classification_and_notional(cycle, coverage):
    from options.data.polygon_developer import PolygonDeveloperEngine
    from realtime.option_package_context import ForwardTradeWindow, retained_trade_context

    config, scope, matrices, context = credit_package_cycle(cycle)
    matrix = matrices[0]
    contract = next(row for row in scope.catalog if row.underlyer == matrix.analysis.underlying.underlyer)
    events = tuple(PolygonDeveloperEngine._trade_event(contract, matrix.ingress.batch_id,
        dict(sip_timestamp=int((scope.cutoff - timedelta(seconds=index + 1)).timestamp()) * 1_000_000_000,
            sequence_number=index + 1, price=2.5, size=10, exchange=1, conditions=[condition]), scope.available_after)
        for index, condition in enumerate((219, 201, 999)))
    receipt = ForwardTradeWindow(scope.sha256, matrix.matrix_id, "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER,
        scope.cutoff - timedelta(minutes=15), scope.cutoff, scope.available_after, scope.available_after,
        (contract.contract_id,), (*events, events[0]), coverage)
    result = retained_trade_context(scope, matrix, receipt, selected_at=scope.available_after)
    assert result["status"] == "PARTIAL" and result["reason"] == "TRADE_CLASSIFICATION_PARTIAL"
    assert result["volume"] == 10 and result["transacted_premium"] == events[0].notional == Decimal("2500.0")
    assert result["unique_event_count"] == 3 and result["classification_counts"] == {"INCLUDED": 1, "CANCELED": 1, "UNKNOWN": 1}
    assert result["aggressor_side"] is None and result["role"] == "ADVISORY"
    empty = retained_trade_context(scope, matrix, replace(receipt, coverage="COMPLETE", events=()), selected_at=scope.available_after)
    assert empty["status"] == "AVAILABLE" and empty["volume"] == 0 and empty["transacted_premium"] == 0
    assert retained_trade_context(scope, matrix, None, selected_at=scope.available_after)["volume"] is None
    from realtime.forward_storage import encode_forward_artifact, decode_forward_artifact
    from realtime.forward_runtime import PackageInputFactsWithTrades
    source = o1_package_source(scope, matrix, context)
    facts = PackageInputFactsWithTrades(matrix.analysis.underlying.underlyer, source.references,
        source.raw_spot_bars, source.structural_bars, source.structural_policy_sha256, source.calendars,
        source.planned_entry_at, source.entry_deadline, source.exit_deadline, receipt)
    encoded, digest = encode_forward_artifact(facts)
    assert decode_forward_artifact(encoded, digest) == facts
    with pytest.raises(ValueError, match="classification"):
        retained_trade_context(scope, matrix, replace(receipt, events=(replace(events[0], semantics_version="unsupported"),)),
            selected_at=scope.available_after)
    changed_catalog = tuple(replace(row, valid_from=scope.cutoff, first_observed_at=scope.cutoff)
        if row.contract_id == contract.contract_id else row
        for row in scope.catalog)
    with pytest.raises(ValueError, match="identity"):
        retained_trade_context(replace(scope, catalog=changed_catalog), matrix,
            replace(receipt, scope_sha256=replace(scope, catalog=changed_catalog).sha256), selected_at=scope.available_after)
    with pytest.raises(ValueError, match="receipt drift"):
        retained_trade_context(scope, matrix, replace(receipt, recorded_at=scope.available_after + timedelta(seconds=1)),
            selected_at=scope.available_after)


def test_technical_package_analysis_passes_exact_catalogue_asset_type(cycle, monkeypatch):
    config, scope, sources = cycle
    captured = []
    original = OptionAnalysisEngine.analyze

    def analyze(engine, *args, **kwargs):
        captured.append(kwargs.get("asset_type"))
        return original(engine, *args, **kwargs)

    monkeypatch.setattr(OptionAnalysisEngine, "analyze", analyze)
    for source in sources:
        normalize_developer_matrix(admit_developer_matrix(scope, source), config)
    assert captured == [source.pages[0].inputs[0].catalog.asset_type for source in sources]


def test_technical_packages_reject_developer_o2_before_native_work(cycle, monkeypatch):
    import realtime.option_package_context as package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    policy = replace(scope.selection.policy, required_detectors=(*scope.selection.policy.required_detectors, DetectorFamily.O2),
        unavailable_detectors=())
    changed = replace(scope, selection=replace(scope.selection, policy=policy))
    monkeypatch.setattr(package_context, "load_forward_o1_package_context", lambda *args, **kwargs: pytest.fail("native work started"))
    with pytest.raises(ValueError, match="including O2"):
        package_context.load_forward_technical_package_context(changed, matrices[0], config, context,
            o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)


def stock_first_package_cycle(cycle, detector):
    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    symbol = matrices[0].analysis.underlying.underlyer
    current = tuple(bar for bar, _ in context.source.bars
        if bar.ticker == symbol and bar.interval == "30m" and bar.session_date == scope.selection.session_date)
    prices = (Decimal("99.6"), Decimal("100")) if detector == "S1" else (
        Decimal("100.2"), Decimal("99.8"), Decimal("100"))
    changed = {bar.bar_revision_id: price for bar, price in zip(current[-len(prices):], prices)}
    bars = []
    for bar, recorded in context.source.bars:
        if bar.ticker == symbol and (bar.interval == "1d" or bar.bar_revision_id in changed):
            price = Decimal("99.5") if bar.interval == "1d" else changed[bar.bar_revision_id]
            bar = replace(bar, open_price=price, close_price=price, high_price=price + Decimal("0.2"),
                low_price=price - Decimal("0.2"), payload_sha256=canonical_sha256((bar.bar_revision_id, detector, price)))
        bars.append((bar, recorded))
    earliest = min(bar.session_date for bar, _ in bars if bar.ticker == symbol and bar.interval == "1d")
    coverage = tuple((replace(receipt, window_start=earliest), recorded) if receipt.ticker == symbol else (receipt, recorded)
        for receipt, recorded in context.source.split_coverage)
    source = replace(context.source, bars=tuple(bars), split_coverage=coverage)
    context = load_forward_detector_context(scope, matrices, config, source, selected_at=scope.available_after)
    return config, scope, matrices, context


@pytest.mark.parametrize("detector", ("S1", "S2"))
def test_technical_packages_qualify_native_stock_first_models_without_execution(cycle, detector):
    from realtime.option_package_context import load_forward_technical_package_context

    config, scope, matrices, context = stock_first_package_cycle(cycle, detector)
    matrix = matrices[0]
    receipt = load_forward_technical_package_context(scope, matrix, config, context,
        o1_package_source(scope, matrix, context), selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    packages = [row for row in payload["qualified_indicative"] if row["package"]["detector_id"] == detector]
    assert packages, dict(dependencies=payload["detector_dependencies"], rejected=payload["rejected"])
    assert payload["detector_dependencies"][detector] == []
    assert all(row["package"]["execution_permission"] is False for row in packages)
    assert all(json.loads(row["plan_payload_json"])["execution_permission"] is False for row in packages)


def test_technical_stock_first_missing_split_coverage_is_not_empty_success(cycle):
    from realtime.option_package_context import load_forward_technical_package_context

    config, scope, matrices, context = stock_first_package_cycle(cycle, "S1")
    source = replace(context.source, split_coverage=())
    context = load_forward_detector_context(scope, matrices, config, source, selected_at=scope.available_after)
    matrix = matrices[0]
    receipt = load_forward_technical_package_context(scope, matrix, config, context,
        o1_package_source(scope, matrix, context), selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert "CANONICAL_STOCK_SPLIT_COVERAGE_UNAVAILABLE" in payload["detector_dependencies"]["S1"]
    assert not any(row["package"]["detector_id"] == "S1" for row in payload["qualified_indicative"])


@pytest.mark.parametrize(("defect", "reason"), (
    ("references", "EXACT_PACKAGE_REFERENCES_UNAVAILABLE"), ("spot", "EXACT_RAW_SPOT_UNAVAILABLE"),
    ("structure", "STRUCTURAL_LEVELS_UNAVAILABLE"), ("calendar", "HOLDING_EVENT_CALENDAR_UNAVAILABLE"),
    ("clocks", "PACKAGE_HOLDING_CLOCKS_UNAVAILABLE"),
))
def test_native_o1_package_missing_dependencies_are_unavailable_not_zero_matches(cycle, defect, reason):
    from realtime.option_package_context import load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    source = o1_package_source(scope, matrix, context)
    changes = {"references": dict(references=()), "spot": dict(raw_spot_bars=()),
        "structure": dict(structural_bars=()), "calendar": dict(calendars=()), "clocks": dict(exit_deadline=None)}
    source = replace(source, **changes[defect])
    receipt = load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["available"] is False and payload["status"] == "UNAVAILABLE"
    assert reason in payload["reasons"] and payload["qualified_indicative"] == []
    assert payload["native_candidate_count"] >= 1


@pytest.mark.parametrize("defect", ("tier", "canonical", "cohort", "matrix", "context", "reference", "reference_asset",
    "spot_price", "spot_tier", "spot_interval", "spot_receipt", "structure", "structural_policy", "calendar_receipt", "late"))
def test_native_o1_package_scope_reference_raw_basis_and_policy_drift_fail_closed(cycle, defect):
    from realtime.option_package_context import load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    source = o1_package_source(scope, matrix, context)
    selected_at = scope.available_after
    if defect == "tier":
        source = replace(source, provider_tier=ProviderTier.MASSIVE_ADVANCED)
    elif defect == "canonical":
        source = replace(source, database_name="stocks")
    elif defect == "cohort":
        scope = replace(scope, cohort_id=uuid5(NAMESPACE_URL, "another-package-scope"))
    elif defect == "matrix":
        source = replace(source, matrix_sha256="f" * 64)
    elif defect == "context":
        context = replace(context, activity_sources=(context.activity_sources[0].model_copy(update={"day_volume": 999}),))
    elif defect in ("reference", "reference_asset"):
        row = source.references[0]
        reference = replace(row.reference, payload_sha256="f" * 64) if defect == "reference" else replace(row.reference, asset_type=AssetType.STOCK)
        source = replace(source, references=(replace(row, reference=reference), *source.references[1:]))
    elif defect in ("spot_price", "spot_tier", "spot_interval", "spot_receipt"):
        bar, recorded = source.raw_spot_bars[0]
        if defect == "spot_price":
            bar = replace(bar, open_price=Decimal("99"), close_price=Decimal("99"), high_price=Decimal("99"), low_price=Decimal("99"))
        elif defect == "spot_tier":
            bar = replace(bar, source_kind=BarSourceKind.REALTIME_STREAM)
        elif defect == "spot_interval":
            bar = replace(bar, interval="5m")
        else:
            recorded = selected_at + timedelta(seconds=1)
        source = replace(source, raw_spot_bars=((bar, recorded),))
    elif defect == "structure":
        bar, recorded = source.structural_bars[0]
        source = replace(source, structural_bars=((replace(bar, payload_sha256="f" * 64), recorded), *source.structural_bars[1:]))
    elif defect == "structural_policy":
        source = replace(source, structural_policy_sha256="f" * 64)
    elif defect == "calendar_receipt":
        source = replace(source, calendars=(replace(source.calendars[0], recorded_at=selected_at + timedelta(seconds=1)),))
    else:
        selected_at = scope.deadline + timedelta(seconds=1)
    if defect == "spot_price":
        receipt = load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=selected_at)
        payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
        assert payload["qualified_indicative"] == [] and payload["available"] is False
    else:
        with pytest.raises(ValueError):
            load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=selected_at)


def test_native_o1_package_calendar_block_and_stale_coverage_never_clear(cycle):
    from options.event_calendar import parse_event_calendar_document
    from realtime.option_package_context import ForwardPackageCalendar, load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    source = o1_package_source(scope, matrix, context)
    observed = scope.available_after - timedelta(minutes=1)
    event = dict(event_type="EARNINGS", affected_underlying=matrix.analysis.underlying.underlyer,
        scheduled_time=(scope.available_after + timedelta(minutes=30)).isoformat(), source_key="known-earnings",
        confidence="CONFIRMED", status="SCHEDULED")
    batch = source.calendars[0].batch
    document = dict(schema_version=1, source=batch.source, observed_at=observed.isoformat(), coverage=[], events=[event])
    parsed = parse_event_calendar_document(document, received_at=observed)
    source = replace(source, calendars=(ForwardPackageCalendar(replace(batch, events=parsed.events), observed),))
    blocked = load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    payload = blocked.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["available"] is False and payload["qualified_indicative"] == []
    assert "SUPPORTED_HOLDING_EVENT_EVIDENCE_UNAVAILABLE_OR_BLOCKED" in payload["reasons"]
    stale = replace(batch, observed_at=observed - timedelta(hours=25), coverage=tuple(replace(row,
        first_observed_at=observed - timedelta(hours=25), source_observed_at=observed - timedelta(hours=25)) for row in batch.coverage))
    source = replace(source, calendars=(ForwardPackageCalendar(stale, stale.observed_at),))
    receipt = load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    assert receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)["qualified_indicative"] == []


def test_native_o1_package_immutable_retries_and_reads_do_no_engine_work(cycle, monkeypatch):
    import realtime.option_package_context as package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    source = o1_package_source(scope, matrix, context)
    receipt = package_context.load_forward_o1_package_context(scope, matrix, config, context, source, selected_at=scope.available_after)
    reversed_source = replace(source, references=tuple(reversed(source.references)),
        structural_bars=tuple(reversed(source.structural_bars)))
    assert package_context.load_forward_o1_package_context(scope, matrix, config, context, reversed_source,
        selected_at=scope.available_after) == receipt
    before = receipt.payload_json

    def forbidden(*args, **kwargs):
        raise AssertionError("stored package receipt cannot calculate or qualify on read")

    monkeypatch.setattr(package_context, "materialize_equity_evidence", forbidden)
    monkeypatch.setattr(package_context, "qualify_dual_origin_package", forbidden)
    monkeypatch.setattr(package_context.OptionStrategyEngine, "scan", forbidden)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    payload["qualified_indicative"].clear()
    assert len(receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)["qualified_indicative"]) == 1
    assert receipt.payload_json == before
    with pytest.raises(ValueError, match="fallback prohibited"):
        receipt.read(scope_sha256="f" * 64, matrix_id=matrix.matrix_id)


def test_native_o1_package_adjacent_cutoffs_preserve_dataset_and_original_inputs(cycle):
    from realtime.option_package_context import load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    first = load_forward_o1_package_context(scope, matrices[0], config, context,
        o1_package_source(scope, matrices[0], context), selected_at=scope.available_after)
    before = first.payload_json
    later_config, later_scope, later_matrices, later_context = o1_package_cycle(cycle,
        forward_policy=True, shift=timedelta(minutes=15))
    second = load_forward_o1_package_context(later_scope, later_matrices[0], later_config, later_context,
        o1_package_source(later_scope, later_matrices[0], later_context), selected_at=later_scope.available_after)
    assert scope.selection.selection_sha256 == later_scope.selection.selection_sha256
    assert forward_detector_dataset_id(scope, config) == forward_detector_dataset_id(later_scope, later_config)
    assert first.scope_sha256 != second.scope_sha256 and first.matrix_id != second.matrix_id
    assert first.input_sha256 != second.input_sha256 and first.payload_json == before
    first_payload = first.read(scope_sha256=scope.sha256, matrix_id=first.matrix_id)
    second_payload = second.read(scope_sha256=later_scope.sha256, matrix_id=second.matrix_id)
    assert len(first_payload["qualified_indicative"]) == len(second_payload["qualified_indicative"]) == 1
    assert first_payload["qualified_indicative"][0]["plan_sha256"] != second_payload["qualified_indicative"][0]["plan_sha256"]


def test_native_o1_package_indicative_display_does_not_claim_complete_event_or_execution_context(cycle):
    from realtime.option_package_context import load_forward_o1_package_context

    config, scope, matrices, context = o1_package_cycle(cycle, forward_policy=True)
    matrix = matrices[0]
    receipt = load_forward_o1_package_context(scope, matrix, config, context,
        o1_package_source(scope, matrix, context), selected_at=scope.available_after)
    payload = receipt.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    assert payload["display_available"] is True
    assert payload["complete_context_available"] is payload["available"] is payload["execution_permission"] is False
    row = payload["qualified_indicative"][0]
    assert row["package"]["event_horizon_status"] == "UNAVAILABLE"
    assert row["source_snapshots"][0]["snapshot_id"] == row["candidate"]["legs"][0]["snapshot_id"]
    assert row["source_snapshots"][0]["market_data_time"] == scope.cutoff.isoformat()
    assert row["source_snapshots"][0]["first_observed_at"] == scope.available_after.isoformat()
    assert scope.available_after - scope.cutoff == timedelta(minutes=15)


def test_forward_runtime_input_codec_round_trips_exact_completed_matrix_sources(cycle):
    from realtime.forward_storage import decode_forward_inputs, encode_forward_inputs

    config, scope, sources = cycle
    matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, source), config) for source in sources)
    detector_source = context_source(config, scope, matrices)
    payload, digest = encode_forward_inputs(scope, sources, detector_source=detector_source)
    decoded = decode_forward_inputs(payload, digest)
    assert decoded["scope"] == scope and decoded["sources"] == sources
    assert decoded["detector_source"] == detector_source
    assert decoded["sources"][0].pages[0].inputs[0].catalog.strike == Decimal("100")
    assert encode_forward_inputs(decoded["scope"], decoded["sources"], detector_source=decoded["detector_source"]) == (payload, digest)
    with pytest.raises(ValueError, match="hash/version"):
        decode_forward_inputs(payload, "f" * 64)


def test_forward_runtime_schema_and_security_are_additive_role_scoped_only():
    from realtime.migrations import load_realtime_migrations
    from realtime.security import API_READ_TABLES, INSERT_GRANTS, UPDATE_GRANTS

    migrations = {row.version: row for row in load_realtime_migrations()}
    migration = migrations["018_realtime_developer_forward"]
    assert "019_realtime_option_pre_session_staging" in migrations
    for relation in ("inputs", "work", "artifacts", "releases", "current"):
        assert f"CREATE TABLE rt.developer_forward_{relation}" in migration.sql
    assert "rt.developer_forward_inputs" in INSERT_GRANTS["ingress"]
    assert "rt.developer_forward_inputs" not in API_READ_TABLES
    assert "rt.developer_forward_current" not in UPDATE_GRANTS["compute"]
    assert "rt.developer_forward_current" in UPDATE_GRANTS["publication"]
    assert "provider_tier = 'MASSIVE_DEVELOPER'" in migration.sql
    assert "DROP" not in migration.sql and "ALTER TABLE" not in migration.sql


def test_forward_runtime_full_policy_scope_does_not_expand_frozen_selection(cycle):
    from realtime.forward_ingestion import PolicyChainMatrixScope
    from realtime.forward_storage import encode_forward_inputs, decode_forward_inputs

    config, scope, sources = cycle
    item = sources[0].pages[0].inputs[0]
    ticker = f"O:{item.catalog.underlyer}261016C00101000"
    payload = json.loads(item.raw.raw_payload_json)
    payload["details"]["ticker"] = ticker
    extra = replace(item, catalog=replace(item.catalog, contract_id=9999, contract_ticker=ticker, strike=Decimal("101")),
        raw=parse_polygon_snapshot(payload, scope.available_after))
    full = PolicyChainMatrixScope(scope.cohort_id, scope.selection, (*scope.catalog, extra.catalog), scope.configuration_sha256,
        scope.reference_sha256, scope.action_sha256, scope.cutoff, scope.available_after, scope.deadline)
    first = replace(sources[0], pages=(replace(sources[0].pages[0], inputs=(*sources[0].pages[0].inputs, extra)),))
    matrix = normalize_developer_matrix(admit_developer_matrix(full, first), config)
    assert len(matrix.snapshots) == 3
    assert full.selection == scope.selection and len(full.selection.dispositions) == len(scope.selection.dispositions)
    assert full.sha256 != scope.sha256 and forward_detector_dataset_id(full, config) != forward_detector_dataset_id(scope, config)
    text, digest = encode_forward_inputs(full, (first, *sources[1:]))
    assert decode_forward_inputs(text, digest)["scope"] == full


def test_forward_runtime_materialization_artifacts_use_exact_domain_codec(cycle):
    from realtime.forward_storage import encode_forward_artifact, decode_forward_artifact

    config, scope, sources = cycle
    matrix = normalize_developer_matrix(admit_developer_matrix(scope, sources[0]), config)
    text, digest = encode_forward_artifact(matrix)
    assert decode_forward_artifact(text, digest) == matrix


def test_forward_runtime_scheduler_enforces_release_pacing_bytes_and_deadlines():
    from types import SimpleNamespace
    from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler

    now = [RECEIVED]
    waits = []
    def wait(seconds):
        waits.append(seconds)
        now[0] += timedelta(seconds=seconds)
    scheduler = DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=2, maximum_response_bytes=20), clock=lambda: now[0], wait=wait)
    arguments = dict(available_after=RECEIVED, deadline=RECEIVED + timedelta(seconds=10))
    operation = lambda: SimpleNamespace(response_bytes=b"fixture")
    scheduler.request(operation, **arguments)
    scheduler.request(operation, **arguments)
    assert waits == [0.5] and scheduler.requests == 2 and scheduler.response_bytes == 14
    with pytest.raises(ValueError, match="budget"):
        scheduler.request(operation, **arguments)


def test_forward_runtime_reader_absence_is_stored_only_without_producer_fallback():
    from realtime.forward_reader import load_forward_release_cursor
    from unittest.mock import MagicMock

    cursor = MagicMock()
    cursor.fetchone.return_value = None
    assert load_forward_release_cursor(cursor, cohort_id=uuid5(NAMESPACE_URL, "future-forward-cohort"),
        available_by=RECEIVED, artifact_kind="ALERTS") is None
    assert cursor.execute.call_count == 1
    assert "SELECT release.*" in cursor.execute.call_args.args[0]
    assert "canonical" not in cursor.execute.call_args.args[0]


def test_forward_runtime_native_discovery_is_paced_and_captures_complete_chain(cycle):
    from types import SimpleNamespace
    from options.data.polygon_http import PolygonHttpResponse
    from options.model_inputs import ExpirationValuationCandidate
    from realtime.forward_ingestion import PolicyChainMatrixScope
    from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler, build_paced_developer_discovery, collect_developer_forward_cycle

    config, scope, sources = cycle
    full = PolicyChainMatrixScope(scope.cohort_id, scope.selection, scope.catalog, scope.configuration_sha256,
        scope.reference_sha256, scope.action_sha256, scope.cutoff, scope.available_after, scope.deadline)
    now = [scope.available_after]
    waits = []
    def wait(seconds):
        waits.append(seconds)
        now[0] += timedelta(seconds=seconds)
    by_symbol = {row.underlyer: row for row in sources}
    calls = []
    def get(url, params):
        symbol = url.split("/")[-1]
        calls.append(symbol)
        payload = dict(status="DELAYED", results=[json.loads(item.raw.raw_payload_json) for item in by_symbol[symbol].pages[0].inputs])
        return PolygonHttpResponse(200, json.dumps(payload).encode("utf-8"))
    scheduler = DeveloperForwardScheduler(DeveloperForwardBudget(), clock=lambda: now[0], wait=wait)
    source = build_paced_developer_discovery(SimpleNamespace(get=get), scheduler, full, config)
    def facts_loader(symbol, scope):
        members = by_symbol[symbol].pages[0].inputs
        expirations = {item.catalog.expiration_date for item in members}
        candidates = {expiration: ExpirationValuationCandidate(scope.selection.session_date,
            expiration, Decimal("100"), 0.04, 0.0, scope.selection.session_date - timedelta(days=1),
            (uuid5(NAMESPACE_URL, f"runtime-rate:{expiration}"),), ("c" * 64,), (),
            scope.selection.session_date, expiration, f"fixture:dividend:{symbol}", "d" * 64,
            scope.available_after) for expiration in expirations}
        return dict(scope_sha256=scope.sha256, database_name="stocks_realtime_v1", provider_tier=ProviderTier.MASSIVE_DEVELOPER,
            security_id=uuid5(NAMESPACE_URL, f"runtime-discovery:{symbol}"), spot_price=Decimal("100"),
            underlying_bars=members[0].underlying_bars, valuation_candidates=candidates,
            expiration_cutoffs={item.catalog.contract_id: item.expiration_cutoff for item in members})
    captured, chains = collect_developer_forward_cycle(full, config, scheduler=scheduler, discovery_source=source, facts_loader=facts_loader)
    assert len(captured) == len(chains) == len(calls) == 8
    assert scheduler.requests == 8 and len(waits) == 7
    assert all(len(row.pages[0].inputs) == 2 for row in captured)
    assert full.selection == scope.selection


def test_forward_runtime_transport_retry_counts_failure_and_honors_retry_after():
    from types import SimpleNamespace
    from realtime.forward_runtime import DeveloperForwardBudget, DeveloperForwardScheduler

    now = [RECEIVED]
    waits = []
    def wait(seconds):
        waits.append(seconds)
        now[0] += timedelta(seconds=seconds)
    scheduler = DeveloperForwardScheduler(DeveloperForwardBudget(maximum_requests=3), clock=lambda: now[0], wait=wait)
    responses = iter((SimpleNamespace(status_code=429, response_bytes=b"rate", headers={"Retry-After": "2"}),
        SimpleNamespace(status_code=200, response_bytes=b"okay", headers={})))
    result = scheduler.request_with_retry(lambda: next(responses), available_after=RECEIVED, deadline=RECEIVED + timedelta(seconds=10))
    assert result.status_code == 200 and waits == [2.0]
    assert scheduler.requests == 2 and scheduler.response_bytes == 8


def test_forward_runtime_codec_rejects_credentials_and_unapproved_configuration_types(cycle):
    from realtime.forward_storage import encode_forward_artifact

    config, _, _ = cycle
    with pytest.raises(ValueError, match="credentials"):
        encode_forward_artifact(dict(api_key="synthetic-not-a-secret"))
    with pytest.raises((ValueError, TypeError)):
        encode_forward_artifact(config)