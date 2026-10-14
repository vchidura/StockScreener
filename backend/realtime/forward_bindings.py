from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from importlib.metadata import version
from math import isfinite
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision, EquityCorporateActionCoverage

from options.analytics.marks import UnderlyingMinuteBar
from options.calendar import OptionExchangeCalendar
from options.detector_collection import direct_structural_policy_sha256
from options.domain import OptionContractReference
from options.event_calendar import (
    OptionEventCalendarBatch, OptionEventCalendarCoverage, OptionMarketEvent,
    OptionMarketEventConfidence, OptionMarketEventStatus, OptionMarketEventType,
)
from options.intraday_participation import CANONICAL_TREND_SOURCE_POLICY
from options.model_inputs import (
    DefaultedExpirationValuationInput, DividendCashFlow, RiskFreeRateObservation,
    interpolate_rate, prepare_expiration_valuation_candidate,
)
from options.repositories.daily_facts import DailyOpenInterestRecord, OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT

from .database_access import RealtimeDatabaseRole
from .forward_ingestion import (
    ForwardActivityHistoryEvidence, ForwardDetectorContextSource, ForwardExchangeSessionEvidence,
    ForwardOpenInterestEvidence, PolicyChainMatrixScope, admit_developer_matrix,
    forward_detector_dataset_id, load_forward_detector_context, normalize_developer_matrix,
)
from .forward_runtime import PackageInputFacts, _require_role
from .forward_storage import _database_clock, decode_forward_artifact
from .option_package_context import ForwardPackageCalendar, ForwardPackageReference
from .preflight import ProviderTier
from .ticker_projection import _security


def _load_treasury_curve(cursor, *, valuation_date, selected_at, maximum_age_seconds):
    cursor.execute("WITH eligible AS (SELECT * FROM ref.risk_free_rate_observations "
        "WHERE source='us_treasury_par_yield_v1' AND rate_date<=%s AND received_at<=%s), "
        "latest AS (SELECT MAX(rate_date) AS rate_date FROM eligible) "
        "SELECT DISTINCT ON (tenor_days) eligible.* FROM eligible JOIN latest USING(rate_date) "
        "ORDER BY tenor_days,received_at DESC,observation_id LIMIT 65",
        (valuation_date, selected_at))
    rate_rows = cursor.fetchall()
    if (not rate_rows or len(rate_rows) > 64
            or len({row["tenor_days"] for row in rate_rows}) != len(rate_rows)):
        raise ValueError("stored Treasury curve is missing or ambiguous")
    rate_date = rate_rows[0]["rate_date"]
    if any(row["rate_date"] != rate_date for row in rate_rows):
        raise ValueError("stored Treasury curve mixes observation dates")
    curve = tuple(RiskFreeRateObservation(UUID(str(row["observation_id"])), row["rate_date"],
        int(row["tenor_days"]), float(row["annual_rate"]), row["source"], row["source_key"],
        row["source_observed_at"], row["received_at"], "LIVE_OBSERVED", None,
        row["payload_sha256"]) for row in rate_rows)
    for row in curve:
        if (row.source != "us_treasury_par_yield_v1"
                or row.source_key != f"{rate_date.isoformat()}:{row.tenor_days}d"
                or row.source_observed_at is None
                or not row.source_observed_at <= row.first_observed_at <= selected_at
                or not 0 <= (selected_at - row.first_observed_at).total_seconds() <= maximum_age_seconds):
            raise ValueError("stored Treasury curve provenance is invalid")
    return curve, rate_date


def _load_defaulted_expiration_valuation_inputs(cursor, *, valuation_date, expirations,
                                                selected_at, maximum_age_seconds,
                                                default_dividend_yield):
    dates = tuple(sorted(set(expirations)))
    curve, _ = _load_treasury_curve(cursor, valuation_date=valuation_date,
        selected_at=selected_at, maximum_age_seconds=maximum_age_seconds)
    return {expiration: DefaultedExpirationValuationInput(
        interpolate_rate(curve, max((expiration - valuation_date).days, 1)),
        float(default_dividend_yield)) for expiration in dates}


def _load_expiration_valuation_candidates(cursor, *, security_id, symbol, spot,
                                          valuation_date, expirations, selected_at,
                                          maximum_age_seconds):
    dates = tuple(sorted(set(expirations)))
    if (not isinstance(security_id, UUID) or not isinstance(symbol, str) or symbol != symbol.upper()
            or not isinstance(spot, Decimal) or not spot.is_finite() or spot <= 0
            or not isinstance(valuation_date, date) or not dates
            or any(not isinstance(expiration, date) or expiration < valuation_date for expiration in dates)
            or selected_at.utcoffset() is None or type(maximum_age_seconds) is not int
            or maximum_age_seconds <= 0):
        raise ValueError("stored valuation candidates require exact finite inputs")
    curve, rate_date = _load_treasury_curve(cursor, valuation_date=valuation_date,
        selected_at=selected_at, maximum_age_seconds=maximum_age_seconds)
    maximum_expiration = max(dates)
    cursor.execute("SELECT * FROM ref.corporate_action_coverage WHERE security_id=%s "
        "AND action_type='DIVIDEND' AND window_start<=%s AND window_end>=%s "
        "AND observed_at<=%s AND received_at<=%s ORDER BY received_at DESC,coverage_id LIMIT 2",
        (str(security_id), valuation_date, maximum_expiration, selected_at, selected_at))
    coverage_rows = cursor.fetchall()
    if len(coverage_rows) != 1:
        raise ValueError("dividend/distribution coverage is missing or ambiguous")
    coverage = coverage_rows[0]
    if (UUID(str(coverage["security_id"])) != security_id
            or coverage["action_type"] != "DIVIDEND"
            or coverage["window_start"] > valuation_date
            or coverage["window_end"] < maximum_expiration
            or coverage["source"] != "massive"
            or not coverage["observed_at"] <= coverage["received_at"] <= selected_at):
        raise ValueError("dividend/distribution coverage identity or availability drift")
    cursor.execute("SELECT * FROM ref.corporate_action_revisions AS action WHERE security_id=%s "
        "AND ticker=%s AND action_type='DIVIDEND' AND ex_date BETWEEN %s AND %s "
        "AND cash_amount IS NOT NULL AND observed_at<=%s AND received_at<=%s "
        "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS newer "
        "WHERE newer.supersedes_action_id=action.action_id AND newer.received_at<=%s) "
        "ORDER BY ex_date,source_key,action_id LIMIT 257",
        (str(security_id), symbol, valuation_date, maximum_expiration,
            selected_at, selected_at, selected_at))
    dividend_rows = cursor.fetchall()
    if (len(dividend_rows) > 256
            or int(coverage["response_action_count"]) != len(dividend_rows)):
        raise ValueError("dividend/distribution coverage action count drift")
    flows = []
    for row in dividend_rows:
        if (UUID(str(row["security_id"])) != security_id or row["ticker"] != symbol
                or row["action_type"] != "DIVIDEND" or row["source"] != coverage["source"]
                or not valuation_date <= row["ex_date"] <= maximum_expiration
                or not row["observed_at"] <= row["received_at"] <= selected_at):
            raise ValueError("dividend/distribution action identity or availability drift")
        flows.append(DividendCashFlow(row["ex_date"], Decimal(row["cash_amount"])))
    return {expiration: prepare_expiration_valuation_candidate(curve=curve,
        expected_rate_date=rate_date, valuation_date=valuation_date,
        expiration_date=expiration, spot=spot, selected_at=selected_at,
        maximum_age_seconds=maximum_age_seconds,
        cash_flows=tuple(flow for flow in flows if flow.ex_date <= expiration),
        dividend_coverage_start=coverage["window_start"],
        dividend_coverage_end=coverage["window_end"], dividend_coverage_complete=True,
        dividend_received_at=coverage["received_at"],
        dividend_source_key=coverage["source_key"],
        dividend_payload_sha256=coverage["response_sha256"]) for expiration in dates}


def load_developer_collection_facts_cursor(cursor, scope, symbol, *, selected_at,
                                         valuation_maximum_age_seconds,
                                         dividend_input_source="polygon_corporate_actions_v1",
                                         default_dividend_yield=Decimal("0")):
    if (not isinstance(scope, PolicyChainMatrixScope)
            or scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or symbol not in scope.underlyings
            or selected_at.utcoffset() is None
            or not scope.available_after <= selected_at < scope.deadline):
        raise ValueError("Developer collection facts require exact scope/tier/deadline")
    cursor.execute("SELECT * FROM rt.bar_revisions WHERE cohort_id=%s AND provider_tier=%s "
        "AND ticker=%s AND interval='1m' AND adjustment_basis='RAW_UNADJUSTED' "
        "AND session_scope='RTH' AND source_kind='NATIVE_REST' AND is_final "
        "AND bar_end>%s AND bar_end<=%s AND observed_at<=%s AND created_at<=%s "
        "ORDER BY bar_end LIMIT 61",
        (str(scope.cohort_id), ProviderTier.MASSIVE_DEVELOPER.value, symbol,
            scope.cutoff - timedelta(hours=1), scope.cutoff, selected_at, selected_at))
    rows = cursor.fetchall()
    if not rows or len(rows) > 60:
        raise ValueError("Developer exact raw spot history unavailable or over budget")
    securities, windows = set(), set()
    for row in rows:
        if (str(row["cohort_id"]) != str(scope.cohort_id)
                or row["provider_tier"] != ProviderTier.MASSIVE_DEVELOPER.value
                or row["ticker"] != symbol or row["interval"] != "1m"
                or row["adjustment_basis"] != "RAW_UNADJUSTED" or row["session_scope"] != "RTH"
                or row["source_kind"] != "NATIVE_REST" or not row["is_final"] or row["quality_codes"]
                or row["availability_mode"] != "LIVE_OBSERVED"
                or row["bar_end"] - row["bar_start"] != timedelta(minutes=1)
                or not scope.cutoff - timedelta(hours=1) < row["bar_end"] <= scope.cutoff
                or not row["bar_end"] <= row["observed_at"] <= row["created_at"] <= selected_at
                or any(row.get(name) is not None and row[name] > selected_at
                    for name in ("replay_available_at", "provider_published_at"))):
            raise ValueError("Developer raw spot target/tier/scope/availability drift")
        securities.add(UUID(str(row["security_id"])))
        windows.add(row["bar_end"])
    if len(securities) != 1 or len(windows) != len(rows) or max(windows) != scope.cutoff:
        raise ValueError("Developer exact raw spot is stale or ambiguous")
    cursor.execute("SELECT DISTINCT ON (security_id,ticker) revision.*,revision.revision_id AS security_revision_id "
        "FROM ref.security_revisions AS revision "
        "WHERE security_id=%s AND ticker=%s AND effective_from<=%s "
        "AND (effective_to IS NULL OR effective_to>%s) AND observed_at<=%s "
        "ORDER BY security_id,ticker,observed_at DESC,revision_id DESC LIMIT 2",
        (str(next(iter(securities))), symbol, scope.cutoff, scope.cutoff, scope.cutoff))
    dated = cursor.fetchall()
    if len(dated) != 1:
        raise ValueError("Developer exact dated security reference unavailable or ambiguous")
    security = _security(dated[0])
    if (security.security_id not in securities or security.ticker != symbol or not security.active
            or security.observed_at > scope.cutoff or security.effective_from > scope.cutoff
            or dated[0]["effective_to"] is not None and dated[0]["effective_to"] <= scope.cutoff):
        raise ValueError("Developer dated security identity/availability drift")
    bars = tuple(UnderlyingMinuteBar(row["close_price"], row["bar_end"])
        for row in sorted(rows, key=lambda row: row["bar_end"]))
    catalog = {row.contract_id: row for row in scope.catalog if row.underlyer == symbol}
    expirations = tuple(row.expiration_date for row in catalog.values())
    if dividend_input_source == "polygon_corporate_actions_v1":
        valuation_candidates = _load_expiration_valuation_candidates(cursor,
            security_id=security.security_id, symbol=symbol, spot=bars[-1].close,
            valuation_date=scope.selection.session_date, expirations=expirations,
            selected_at=selected_at, maximum_age_seconds=valuation_maximum_age_seconds)
    elif dividend_input_source is None:
        valuation_candidates = _load_defaulted_expiration_valuation_inputs(cursor,
            valuation_date=scope.selection.session_date, expirations=expirations,
            selected_at=selected_at, maximum_age_seconds=valuation_maximum_age_seconds,
            default_dividend_yield=default_dividend_yield)
    else:
        raise ValueError("Developer dividend input source is unsupported")
    version_ids = {row.contract_id: row.catalog_version_id for row in scope.selection.dispositions
        if row.contract_id in catalog}
    if set(version_ids) != set(catalog):
        raise ValueError("Developer enrolled catalog version identities are incomplete")
    cursor.execute("SELECT version.*,contract.contract_ticker,contract.provider,contract.underlying_security_id, "
        "contract.first_observed_at FROM ref.option_contract_versions AS version "
        "JOIN ref.option_contracts AS contract USING(contract_id) WHERE version.contract_id=ANY(%s) "
        "AND version.version_id=ANY(%s) "
        "AND version.effective_from<=%s AND (version.effective_to IS NULL OR version.effective_to>%s) "
        "AND version.observed_at<=%s ORDER BY version.contract_id LIMIT %s",
        (list(catalog), list(version_ids.values()), scope.cutoff, scope.cutoff, scope.cutoff, len(catalog) + 1))
    references = cursor.fetchall()
    if len(references) != len(catalog) or {row["contract_id"] for row in references} != set(catalog):
        raise ValueError("Developer exact target-local contract references unavailable or ambiguous")
    for row in references:
        entry = catalog[row["contract_id"]]
        actual = (row["contract_ticker"], row["provider"], row["call_put"], row["expiration"],
            row["strike"], row["exercise_style"], row["multiplier"], row["eligibility"],
            row["effective_from"], row["effective_to"], row["observed_at"], row["payload_sha256"])
        expected = (entry.contract_ticker, entry.provider, entry.contract_type.value[0], entry.expiration_date,
            entry.strike, entry.exercise_style.value, entry.shares_per_contract, entry.eligibility_status.value,
            entry.valid_from, entry.valid_to,
            entry.revised_observed_at or entry.first_observed_at, entry.payload_sha256)
        if (actual != expected or row["version_id"] != version_ids[entry.contract_id]
            or row["first_observed_at"] > entry.first_observed_at
            or UUID(str(row["underlying_security_id"])) not in securities
            or row["deliverables"] != []):
            raise ValueError("Developer dated contract reference identity drift")
    calendar = OptionExchangeCalendar()
    return dict(scope_sha256=scope.sha256, database_name="stocks_realtime_v1",
        provider_tier=ProviderTier.MASSIVE_DEVELOPER, security_id=next(iter(securities)),
        spot_price=bars[-1].close, underlying_bars=bars,
        expiration_cutoffs={identity: calendar.expiration_cutoff(entry.expiration_date)
            for identity, entry in catalog.items()}, valuation_candidates=valuation_candidates,
        reference_versions=tuple(references), raw_spot_rows=tuple(rows))


class TargetDeveloperFactsLoader:
    def __init__(self, database, scope, configuration):
        _require_role(database, RealtimeDatabaseRole.COMPUTE)
        if (configuration.configuration_sha256 != scope.configuration_sha256
                or configuration.settings.risk_free_rate_source != "us_treasury_par_yield_v1"
            or configuration.settings.dividend_input_source not in (None, "polygon_corporate_actions_v1")):
            raise ValueError("Developer facts binding requires exact configuration and reviewed valuation inputs")
        self.database, self.scope, self.configuration = database, scope, configuration

    def __call__(self, symbol, scope):
        if scope != self.scope or symbol not in self.scope.underlyings:
            raise ValueError("Developer facts loader scope drift")
        with self.database.transaction(dict_cursor=True, read_only=True, isolation_level="REPEATABLE READ") as cursor:
            return load_developer_collection_facts_cursor(cursor, scope, symbol, selected_at=_database_clock(cursor),
                valuation_maximum_age_seconds=self.configuration.settings.dividend_input_max_age_seconds,
                dividend_input_source=self.configuration.settings.dividend_input_source,
                default_dividend_yield=self.configuration.settings.default_dividend_yield)


def _bar_receipt(row, scope, security, selected_at):
    if (str(row["cohort_id"]) != str(scope.cohort_id)
            or row["provider_tier"] != ProviderTier.MASSIVE_DEVELOPER.value
            or row["ticker"] != security.ticker or UUID(str(row["security_id"])) != security.security_id
            or row["adjustment_basis"] != "RAW_UNADJUSTED" or row["session_scope"] != "RTH"
            or row["availability_mode"] != "LIVE_OBSERVED" or not row["is_final"]
            or tuple(row["quality_codes"]) not in ((), ("DERIVED_FROM_CANONICAL_30M",))
            or row["quality_codes"] and row["interval"] not in ("1h", "1d")
            or row["source_kind"] not in ("NATIVE_REST", "DERIVED")
            or row["bar_end"] > scope.cutoff
            or not row["observed_at"] <= row["created_at"] <= selected_at
            or any(row.get(name) is not None and row[name] > selected_at
                for name in ("replay_available_at", "provider_published_at"))):
        raise ValueError("Developer context bar target/tier/identity/availability drift")
    bar = EquityBarRevision(
        bar_revision_id=UUID(str(row["bar_revision_id"])), security_id=security.security_id,
        ticker=row["ticker"], interval=row["interval"], session_date=row["session_date"],
        bar_start=row["bar_start"], bar_end=row["bar_end"], open_price=row["open_price"],
        high_price=row["high_price"], low_price=row["low_price"], close_price=row["close_price"],
        volume=row["volume"], vwap=row["vwap"], transaction_count=row["transaction_count"],
        source_kind=BarSourceKind(row["source_kind"]), availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        is_final=row["is_final"], system_observed_at=row["observed_at"], replay_available_at=row["replay_available_at"],
        adjusted=False, payload_sha256=row["payload_sha256"], quality_codes=tuple(row["quality_codes"]),
        source_bar_revision_ids=tuple(UUID(str(identity)) for identity in row["source_revision_ids"]),
        provider_published_at=row.get("provider_published_at"), session_scope=BarSessionScope.RTH)
    return bar, row["created_at"]


def _load_context_bars(cursor, scope, security, selected_at):
    receipts = []
    policy = CANONICAL_TREND_SOURCE_POLICY
    for interval, count in (("30m", policy.trend_history_bars), ("1d", policy.liquidity_bars), ("1h", 3), ("15m", 3), ("5m", 3)):
        cursor.execute("SELECT DISTINCT ON (bar_start) * FROM rt.bar_revisions "
            "WHERE cohort_id=%s AND provider_tier=%s AND security_id=%s AND ticker=%s AND interval=%s "
            "AND adjustment_basis='RAW_UNADJUSTED' AND session_scope='RTH' AND is_final "
            "AND availability_mode='LIVE_OBSERVED' AND source_kind IN ('NATIVE_REST','DERIVED') "
            "AND observed_at<=%s AND created_at<=%s AND bar_end<=%s "
            "ORDER BY bar_start DESC,observed_at DESC,created_at DESC,bar_revision_id LIMIT %s",
            (str(scope.cohort_id), ProviderTier.MASSIVE_DEVELOPER.value, str(security.security_id), security.ticker,
                interval, selected_at, selected_at, scope.cutoff, count))
        rows = cursor.fetchall()
        if len(rows) > count or any(row["interval"] != interval for row in rows):
            raise ValueError("Developer context history exceeds its interval budget")
        receipts.extend(_bar_receipt(row, scope, security, selected_at) for row in rows)
    return tuple(receipts)


def _load_open_interest(cursor, scope, matrices, selected_at, calendar):
    snapshots = {row.contract_id: row for matrix in matrices for row in matrix.snapshots}
    session = scope.selection.session_date
    prior = calendar.previous_session(session)
    cursor.execute("SELECT * FROM ref.oi_baselines AS baseline WHERE contract_id=ANY(%s) "
        "AND settlement_session=%s AND observed_session=%s AND observed_at<=%s AND received_at<=%s "
        "ORDER BY contract_id,observed_at DESC LIMIT %s",
        (list(snapshots), prior, session, selected_at, selected_at, len(snapshots) + 1))
    rows = cursor.fetchall()
    if len(rows) > len(snapshots) or len({row["contract_id"] for row in rows}) != len(rows):
        raise ValueError("Developer OI baselines are ambiguous or revised")
    evidence = []
    for row in rows:
        snapshot = snapshots.get(row["contract_id"])
        if (snapshot is None or row["settlement_session"] != prior or row["observed_session"] != session
                or row["source"] != "massive" or row["date_basis"] != "PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION"
                or row["supersedes_baseline_id"] is not None
                or row["open_interest"] != snapshot.open_interest or type(row["open_interest"]) is not int
                or not row["observed_at"] <= row["received_at"] <= selected_at):
            raise ValueError("Developer dated OI source/value/session/availability drift")
        evidence.append(ForwardOpenInterestEvidence(UUID(str(row["baseline_id"])),
            DailyOpenInterestRecord(snapshot.contract_id, prior, snapshot.underlyer, row["open_interest"],
                row["observed_at"], session, snapshot.batch_id), OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT, row["received_at"]))
    return tuple(evidence)


def _load_activity_history(cursor, scope, configuration, selected_at):
    cursor.execute("SELECT artifact.payload_text,artifact.payload_sha256,input.cutoff,input.configuration_sha256, "
        "input.scope_sha256,artifact.computed_at,release.published_at "
        "FROM rt.developer_forward_releases AS release JOIN rt.developer_forward_inputs AS input USING(input_id) "
        "JOIN rt.developer_forward_artifacts AS artifact USING(input_id) WHERE release.cohort_id=%s "
        "AND release.provider_tier='MASSIVE_DEVELOPER' AND input.selection_sha256=%s "
        "AND input.configuration_sha256=%s AND input.reference_sha256=%s AND input.action_sha256=%s "
        "AND input.cutoff<%s AND release.published_at<=%s AND artifact.artifact_kind='CONTEXT' "
        "ORDER BY input.cutoff DESC LIMIT 1",
        (str(scope.cohort_id), scope.selection.selection_sha256, scope.configuration_sha256,
            scope.reference_sha256, scope.action_sha256, scope.cutoff, selected_at))
    row = cursor.fetchone()
    if row is None:
        return None
    context = decode_forward_artifact(row["payload_text"], row["payload_sha256"])
    from .forward_ingestion import ForwardDetectorContext, _context_sha256

    if (type(context) is not ForwardDetectorContext or context.source.database_name != "stocks_realtime_v1"
            or context.source.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or context.source.scope_sha256 != row["scope_sha256"]
            or context.content_sha256 != _context_sha256(context.source, context.selected_at, context.inputs, context.dependencies)
            or not row["cutoff"] < scope.cutoff or row["configuration_sha256"] != scope.configuration_sha256
            or not context.selected_at <= row["computed_at"] <= row["published_at"] <= selected_at
            or any(item["market_time"] > row["cutoff"] for item in context.inputs.matrices)
            or any(item.scheduled_cycle != row["cutoff"] or item.underlyer not in scope.underlyings
                for item in context.inputs.o1_observations)):
        raise ValueError("Developer retained activity history identity/availability drift")
    directions = tuple(sorted({(item.underlyer, item.direction) for item in context.inputs.o1_observations}))
    return ForwardActivityHistoryEvidence(forward_detector_dataset_id(scope, configuration), scope.cutoff,
        directions, context.selected_at, row["published_at"])


def _load_split_coverage(cursor, scope, security, bars, selected_at):
    trend = tuple(bar for bar, _ in bars if bar.interval == "30m")
    if not trend:
        return ()
    start = min(bar.session_date for bar in trend)
    cursor.execute("SELECT coverage.* FROM ref.corporate_action_coverage AS coverage "
        "WHERE security_id=%s AND action_type='SPLIT' AND window_start<=%s AND window_end>=%s "
        "AND observed_at>=%s AND observed_at<=%s AND received_at<=%s "
        "AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS action "
        "WHERE action.security_id=coverage.security_id AND action.action_type='SPLIT' "
        "AND action.effective_date BETWEEN %s AND %s AND action.received_at<=%s) "
        "ORDER BY received_at DESC,coverage_id LIMIT 1",
        (str(security.security_id), start, scope.selection.session_date, selected_at - timedelta(hours=24),
            selected_at, selected_at, start, scope.selection.session_date, selected_at))
    row = cursor.fetchone()
    if row is None:
        return ()
    if (UUID(str(row["security_id"])) != security.security_id or row["action_type"] != "SPLIT"
            or row["window_start"] > start or row["window_end"] < scope.selection.session_date
            or not selected_at - timedelta(hours=24) <= row["observed_at"] <= row["received_at"] <= selected_at):
        raise ValueError("Developer split coverage identity/freshness drift")
    receipt = EquityCorporateActionCoverage(UUID(str(row["coverage_id"])), "SPLIT", security.ticker,
        row["window_start"], row["window_end"], row["source"], row["source_key"], row["observed_at"],
        BarAvailabilityMode.LIVE_OBSERVED, None, row["payload_sha256"], security.security_id,
        row["response_action_count"], row["response_sha256"])
    return ((receipt, row["received_at"]),)


def _load_package_calendars(cursor, security, *, selected_at, planned_entry_at, exit_deadline):
    cursor.execute("SELECT * FROM ref.market_event_revisions AS event WHERE "
        "(security_id IS NULL OR security_id=%s) AND event_date BETWEEN %s AND %s "
        "AND observed_at<=%s AND received_at<=%s AND valid_until>%s "
        "AND NOT EXISTS (SELECT 1 FROM ref.market_event_revisions AS newer "
        "WHERE newer.supersedes_revision_id=event.revision_id AND newer.received_at<=%s) "
        "ORDER BY source,event_key LIMIT 257",
        (str(security.security_id), planned_entry_at.date(), exit_deadline.date(), selected_at,
            selected_at, selected_at, selected_at))
    events = cursor.fetchall()
    cursor.execute("SELECT DISTINCT ON (source,event_type,security_id,window_start,window_end) * "
        "FROM ref.market_event_coverage WHERE (security_id IS NULL OR security_id=%s) "
        "AND window_start<=%s AND window_end>=%s AND checked_at<=%s AND valid_until>%s "
        "ORDER BY source,event_type,security_id,window_start,window_end,checked_at DESC LIMIT 257",
        (str(security.security_id), planned_entry_at.date(), exit_deadline.date(), selected_at, selected_at))
    coverage = cursor.fetchall()
    if len(events) > 256 or len(coverage) > 256:
        raise ValueError("Developer holding calendar source budget exceeded")
    batches = {}
    for row in events + coverage:
        if row["security_id"] is not None and UUID(str(row["security_id"])) != security.security_id:
            raise ValueError("Developer holding calendar security drift")
        if row["valid_until"] <= selected_at:
            raise ValueError("Developer holding calendar expired")
        batches.setdefault(row["source"], [[], [], []])
    for row in events:
        if not row["observed_at"] <= row["received_at"] <= selected_at:
            raise ValueError("Developer holding event availability drift")
        event = OptionMarketEvent(UUID(str(row["revision_id"])), OptionMarketEventType(row["event_type"]),
            security.ticker if row["security_id"] is not None else None,
            datetime.combine(row["event_date"], time(), ZoneInfo(row["event_timezone"])),
            row["source"], row["event_key"], row["announced_at"], row["received_at"], row["observed_at"],
            OptionMarketEventConfidence.UNKNOWN, OptionMarketEventStatus(row["status"]), row["payload_sha256"])
        batches[row["source"]][1].append(event)
        batches[row["source"]][2].append(row["received_at"])
    for row in coverage:
        if row["checked_at"] > selected_at:
            raise ValueError("Developer holding coverage availability drift")
        if row["verdict"] not in ("COVERED", "KNOWN_NO_EVENT"):
            continue
        receipt = OptionEventCalendarCoverage(UUID(str(row["coverage_id"])), OptionMarketEventType(row["event_type"]),
            security.ticker if row["security_id"] is not None else None,
            datetime.combine(row["window_start"], time(), ZoneInfo("America/New_York")),
            datetime.combine(row["window_end"] + timedelta(days=1), time(), ZoneInfo("America/New_York")),
            row["source"], str(row["coverage_id"]), row["checked_at"], row["checked_at"], row["evidence_sha256"])
        batches[row["source"]][0].append(receipt)
        batches[row["source"]][2].append(row["checked_at"])
    if len(batches) > 16:
        raise ValueError("Developer holding calendar source budget exceeded")
    return tuple(ForwardPackageCalendar(OptionEventCalendarBatch(source, max(clocks), tuple(covered), tuple(scheduled)),
        max(clocks)) for source, (covered, scheduled, clocks) in sorted(batches.items()) if clocks)


class TargetDeveloperSourceBindings:
    def __init__(self, facts_loader, configuration, *, planned_entry_at, entry_deadline, exit_deadline):
        if not isinstance(facts_loader, TargetDeveloperFactsLoader):
            raise TypeError("Developer sources require concrete target-local facts binding")
        if (configuration.configuration_sha256 != facts_loader.scope.configuration_sha256
                or any(clock.utcoffset() is None for clock in (planned_entry_at, entry_deadline, exit_deadline))
                or not facts_loader.scope.deadline < planned_entry_at <= entry_deadline < exit_deadline):
            raise ValueError("Developer source configuration or reviewed holding clocks drift")
        self.facts_loader, self.configuration = facts_loader, configuration
        self.planned_entry_at, self.entry_deadline, self.exit_deadline = planned_entry_at, entry_deadline, exit_deadline
        self._loaded = None

    def detector_source(self, cursor, scope, sources):
        if scope != self.facts_loader.scope:
            raise ValueError("Developer context binding scope drift")
        selected_at = _database_clock(cursor)
        matrices = tuple(normalize_developer_matrix(admit_developer_matrix(scope, row), self.configuration) for row in sources)
        if {row.underlyer for row in sources} != set(scope.underlyings) or len(sources) != len(scope.underlyings):
            raise ValueError("Developer source binding requires exact complete matrix membership")
        cursor.execute("SELECT DISTINCT ON (ticker) revision.*,revision.revision_id AS security_revision_id "
            "FROM ref.security_revisions AS revision "
            "WHERE ticker=ANY(%s) AND effective_from<=%s AND (effective_to IS NULL OR effective_to>%s) "
            "AND observed_at<=%s ORDER BY ticker,observed_at DESC,revision_id DESC LIMIT %s",
            (list(scope.underlyings), scope.cutoff, scope.cutoff, scope.cutoff, len(scope.underlyings) + 1))
        rows = cursor.fetchall()
        if len(rows) > len(scope.underlyings) or len({row["ticker"] for row in rows}) != len(rows):
            raise ValueError("Developer security references are ambiguous")
        securities = tuple(_security(row) for row in rows)
        bars, coverage, packages = [], [], []
        for security in securities:
            if (security.ticker not in scope.underlyings or not security.active or security.observed_at > scope.cutoff
                    or security.effective_from > scope.cutoff):
                raise ValueError("Developer security reference scope/availability drift")
            facts = load_developer_collection_facts_cursor(cursor, scope, security.ticker, selected_at=selected_at,
                valuation_maximum_age_seconds=self.configuration.settings.dividend_input_max_age_seconds,
                dividend_input_source=self.configuration.settings.dividend_input_source,
                default_dividend_yield=self.configuration.settings.default_dividend_yield)
            if facts["security_id"] != security.security_id:
                raise ValueError("Developer security reference/bar identity drift")
            stock_bars = _load_context_bars(cursor, scope, security, selected_at)
            bars.extend(stock_bars)
            coverage.extend(_load_split_coverage(cursor, scope, security, stock_bars, selected_at))
            catalog = {row.contract_id: row for row in scope.catalog if row.underlyer == security.ticker}
            references = []
            for row in facts["reference_versions"]:
                entry = catalog[row["contract_id"]]
                reference = OptionContractReference(entry.contract_ticker, entry.underlyer, entry.asset_type, entry.provider,
                    entry.provider_version, entry.contract_type.value, entry.expiration_date, entry.strike,
                    entry.exercise_style.value, entry.shares_per_contract, entry.primary_exchange, None, "[]", "{}", False,
                    entry.valid_from, entry.valid_to, entry.first_observed_at, entry.revised_observed_at,
                    row["observed_at"], entry.payload_sha256)
                references.append(ForwardPackageReference(uuid5(NAMESPACE_URL,
                    f"realtime-option-version:{row['version_id']}:{row['payload_sha256']}"), entry.contract_id, reference))
            packages.append(PackageInputFacts(security.ticker, tuple(references),
                tuple(_bar_receipt(row, scope, security, selected_at) for row in facts["raw_spot_rows"]),
                tuple(row for row in stock_bars if row[0].interval == "30m"), direct_structural_policy_sha256(),
                _load_package_calendars(cursor, security, selected_at=selected_at,
                    planned_entry_at=self.planned_entry_at, exit_deadline=self.exit_deadline),
                self.planned_entry_at, self.entry_deadline, self.exit_deadline))
        calendar = OptionExchangeCalendar()
        session = scope.selection.session_date
        source = ForwardDetectorContextSource(scope.sha256, tuple(row.content_sha256 for row in matrices),
            "stocks_realtime_v1", ProviderTier.MASSIVE_DEVELOPER, securities,
            _load_open_interest(cursor, scope, matrices, selected_at, calendar), tuple(bars), tuple(coverage),
            ForwardExchangeSessionEvidence(calendar.calendar_name, version("exchange_calendars"), session,
                calendar.previous_session(session), calendar.session_open(session), calendar.session_close(session),
                selected_at, selected_at), _load_activity_history(cursor, scope, self.configuration, selected_at))
        load_forward_detector_context(scope, matrices, self.configuration, source, selected_at=selected_at)
        self._loaded = (cursor, scope, sources, tuple(packages))
        return source

    def package_sources(self, cursor, scope, sources):
        if self._loaded is None or self._loaded[:3] != (cursor, scope, sources):
            raise ValueError("Developer packages require the same frozen context read transaction")
        packages = self._loaded[3]
        self._loaded = None
        return packages