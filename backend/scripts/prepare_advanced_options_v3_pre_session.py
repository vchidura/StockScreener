#!/usr/bin/env python3
"""Plan or execute configured V3 pre-session snapshot admission."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.advanced_options_activity_momentum_c6 import (  # noqa: E402
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ActivityMomentumCandidate,
    ActivityMomentumExpiryCalendarEvidence,
    ActivityMomentumLastTradingEvidence,
    activity_momentum_allocation_payload,
    allocate_activity_momentum,
    load_activity_momentum_profile,
    resolve_activity_momentum_selection,
)
from realtime.advanced_options_flow_policy import (  # noqa: E402
    EXPECTED_UNDERLYINGS,
    load_advanced_options_flow_policy,
)
from realtime.advanced_options_p7_selection import (  # noqa: E402
    P7TransportSelectionPolicy,
)
from realtime.advanced_options_p8_production import (  # noqa: E402
    _PostgresSelectionSpots,
)
from realtime.advanced_options_p8_selection_source import (  # noqa: E402
    MassiveAdvancedOptionsSelectionSource,
    P8_SELECTION_MAXIMUM_DECODED_BYTES,
    SEALED_CAPTURE_GENERALIZED_C6_PROFILE,
)
from realtime.advanced_options_selection_c6 import OptionSide  # noqa: E402
from realtime.contracts import UniverseManifest, canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.equity_recent_repair import (  # noqa: E402
    AuthorizedNativeRepairTransport,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_adapters import _option_fact  # noqa: E402
from realtime.reference_facts import (  # noqa: E402
    ReferenceFact,
    ReferenceFactRelation,
)
from realtime.reference_repository import (  # noqa: E402
    persist_reference_facts_cursor,
)
from realtime.reference_seed import ReferenceKind  # noqa: E402
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from scripts.realtime_script_support import (  # noqa: E402
    PacedPolygonTransport,
    write_report,
)


UTC = timezone.utc
UNDERLYINGS = ("QQQ", "SPY")
_OPTION_TICKER = re.compile(
    r"^O:([A-Z][A-Z0-9.-]{0,14})\d{6}([CP])\d{8}$"
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--source-cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--reference-valid-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument(
        "--maximum-response-decoded-bytes",
        type=int,
        required=True,
    )
    parser.add_argument("--maximum-decoded-bytes", type=int, required=True)
    parser.add_argument(
        "--maximum-moneyness-fraction",
        type=Decimal,
        required=True,
    )
    parser.add_argument(
        "--active-quote-contract-budget",
        type=int,
    )
    parser.add_argument(
        "--per-underlying-contract-limit",
        type=int,
    )
    parser.add_argument(
        "--established-measured-capacity",
        type=int,
    )
    parser.add_argument(
        "--effective-provider-project-ceiling",
        type=int,
    )
    parser.add_argument(
        "--admission-profile-version",
        choices=(
            LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            ACTIVITY_MOMENTUM_PROFILE_VERSION,
            SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
        ),
        default=ACTIVITY_MOMENTUM_PROFILE_VERSION,
    )
    parser.add_argument(
        "--short-delta-override",
        type=Decimal,
    )
    parser.add_argument(
        "--disable-same-day",
        action="store_true",
    )
    parser.add_argument(
        "--last-trading-convention",
        action="append",
        metavar="UNDERLYING=CONVENTION",
    )
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--underlying", action="append")
    parser.add_argument("--enabled-strategy", action="append")
    parser.add_argument(
        "--required-contract-ticker",
        action="append",
    )
    parser.add_argument("--capture-path", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--capture-only", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("V3 pre-session admission targets stocks_realtime_v2")
    if (
        args.authorized_until.tzinfo is None
        or args.reference_valid_until.tzinfo is None
        or not 0
        < args.maximum_response_decoded_bytes
        <= args.maximum_decoded_bytes
        <= P8_SELECTION_MAXIMUM_DECODED_BYTES
        or not Decimal("0")
        < args.maximum_moneyness_fraction
        <= Decimal("1")
        or (
            args.active_quote_contract_budget is not None
            and args.active_quote_contract_budget <= 0
        )
        or (
            args.per_underlying_contract_limit is not None
            and args.per_underlying_contract_limit <= 0
        )
        or (
            args.established_measured_capacity is not None
            and args.established_measured_capacity <= 0
        )
        or (
            args.effective_provider_project_ceiling is not None
            and args.effective_provider_project_ceiling <= 0
        )
        or (
            args.short_delta_override is not None
            and not Decimal("0")
            < args.short_delta_override
            <= Decimal("1")
        )
        or not args.authorization_reference.strip()
    ):
        parser.error("V3 pre-session scope or budget is invalid")
    args.authorized_until = args.authorized_until.astimezone(UTC)
    args.reference_valid_until = args.reference_valid_until.astimezone(UTC)
    args.capture_path = args.capture_path.resolve()
    args.report_path = args.report_path.resolve()
    if args.capture_path.exists() or args.report_path.exists():
        parser.error("capture and report paths must be new")
    args.underlyings = tuple(sorted({
        value.strip().upper()
        for value in (args.underlying or UNDERLYINGS)
        if value.strip()
    }))
    if (
        not args.underlyings
        or len(args.underlyings)
        != len(args.underlying or UNDERLYINGS)
        or args.maximum_provider_requests < len(args.underlyings)
    ):
        parser.error(
            "provider request budget must cover configured underlyings"
        )
    args.required_contract_tickers = tuple(sorted({
        value.strip().upper()
        for value in (args.required_contract_ticker or ())
        if value.strip()
    }))
    args.enabled_strategies = tuple(sorted({
        value.strip().upper()
        for value in (args.enabled_strategy or ())
        if value.strip()
    }))
    if len(args.enabled_strategies) != len(
        args.enabled_strategy or ()
    ):
        parser.error("enabled strategies must be unique and nonblank")
    if args.required_contract_ticker and (
        len(args.required_contract_tickers)
        != len(args.required_contract_ticker)
    ):
        parser.error(
            "required contract scope must contain unique tickers"
        )
    if args.required_contract_tickers:
        memberships = {
            match.group(1)
            for ticker in args.required_contract_tickers
            if (match := _OPTION_TICKER.fullmatch(ticker)) is not None
        }
        if not memberships or not memberships.issubset(
            args.underlyings
        ):
            parser.error(
                "required contracts must belong to configured underlyings"
            )
    conventions = {}
    for value in args.last_trading_convention or ():
        underlying, separator, convention = value.partition("=")
        underlying = underlying.strip().upper()
        convention = convention.strip().upper()
        if (
            separator != "="
            or not underlying
            or not convention
            or underlying in conventions
        ):
            parser.error(
                "last-trading conventions must be unique "
                "UNDERLYING=CONVENTION values"
            )
        conventions[underlying] = convention
    if set(conventions) - set(args.underlyings):
        parser.error(
            "last-trading conventions must match configured underlyings"
        )
    args.last_trading_conventions = conventions
    if (
        args.admission_profile_version
        != SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        and (
            args.short_delta_override is not None
            or args.disable_same_day
            or conventions
        )
    ):
        parser.error(
            "weekly-expiry options require the v3 admission profile"
        )
    return args


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
        ),
    )
    provider.verify()
    return provider, pool


def _persist_facts(provider, facts, *, capture_only):
    if capture_only:
        return 0
    with provider.transaction(APP_ROLE) as unit:
        persist_reference_facts_cursor(unit.cursor, facts)
    return len(facts)


def _facts(
    selected_rows,
    selected_snapshots,
    request,
    settlement_session,
    *,
    underlyings=UNDERLYINGS,
):
    facts = []
    selected = []
    for row in selected_rows:
        raw = dict(selected_snapshots[row.contract_ticker])
        details = dict(raw["details"])
        details["underlying_ticker"] = row.underlyer
        catalog = _option_fact(row.underlyer, details, request)
        contract_id = dict(catalog.values)["contract_id"]
        raw_open_interest = raw.get("open_interest")
        open_interest = (
            int(raw_open_interest)
            if isinstance(raw_open_interest, int)
            and raw_open_interest >= 0
            else None
        )
        digest = canonical_sha256(raw)
        facts.append(catalog)
        if open_interest is not None:
            facts.append(ReferenceFact.build(
                ReferenceFactRelation.OI_BASELINE,
                ReferenceKind.OI_COVERAGE.value,
                row.underlyer,
                baseline_id=uuid5(
                    NAMESPACE_URL,
                    "advanced-options-v3-oi:"
                    f"{row.contract_ticker}:{settlement_session}:"
                    f"{request.created_at.isoformat()}:{digest}",
                ),
                contract_id=contract_id,
                settlement_session=settlement_session,
                open_interest=open_interest,
                source="massive",
                date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
                observed_session=request.session_date,
                observed_at=request.created_at,
                received_at=request.created_at,
                payload_sha256=digest,
                supersedes_baseline_id=None,
            ))
        selected.append({
            "underlyer": row.underlyer,
            "contract_ticker": row.contract_ticker,
            "expiration_date": row.expiration_date.isoformat(),
            "option_type": row.option_type,
            "open_interest": open_interest,
        })
    if (
        not selected
        or not {
            row["underlyer"] for row in selected
        }.issubset(underlyings)
        or any(
            not request.session_date
            <= date.fromisoformat(row["expiration_date"])
            <= request.session_date + timedelta(days=60)
            for row in selected
        )
    ):
        raise ValueError("V3 selected contract membership changed")
    return tuple(facts), tuple(selected)


def _select(candidates, *, underlyings=UNDERLYINGS):
    selected = []
    for underlyer in underlyings:
        for option_type in ("C", "P"):
            ranked = sorted(
                (
                    row
                    for row in candidates
                    if row.underlyer == underlyer
                    and row.option_type == option_type
                ),
                key=lambda row: (
                    row.moneyness_fraction,
                    -row.open_interest,
                    -row.day_volume,
                    row.relative_spread,
                    row.contract_ticker,
                ),
            )
            if not ranked:
                raise ValueError(
                    f"{underlyer} {option_type} pre-session scope is empty"
                )
            selected.append(ranked[0])
    return tuple(selected)


def _source_datetime(
    value,
    *,
    fallback: datetime,
) -> datetime:
    if type(value) is not int or value <= 0:
        return fallback
    return datetime.fromtimestamp(
        value / 1_000_000_000,
        tz=UTC,
    )


def _session_on_or_before(
    calendar: OptionExchangeCalendar,
    calendar_date: date,
) -> date:
    session = calendar.session_on_or_after(calendar_date)
    return (
        session
        if session == calendar_date
        else calendar.previous_session_date(session)
    )


def _expiry_calendar_evidence(
    calendar: OptionExchangeCalendar,
    *,
    session_date: date,
    activity_candidates,
    conventions,
):
    friday = session_date + timedelta(
        days=4 - session_date.weekday()
    )
    week_end_session = _session_on_or_before(
        calendar,
        friday,
    )
    values = []
    for underlying in sorted({
        row.underlying for row in activity_candidates
    }):
        convention = conventions.get(underlying, "UNSUPPORTED")
        expiries = tuple(sorted({
            row.expiration
            for row in activity_candidates
            if row.underlying == underlying
            and 0 <= row.dte_for(session_date) <= 5
        }))
        last_trading = []
        for expiration in expiries:
            last_trading_session = _session_on_or_before(
                calendar,
                expiration,
            )
            last_trading_at = calendar.session_close(
                last_trading_session
            )
            evidence_reference = canonical_sha256({
                "calendar": calendar.calendar_name,
                "underlying": underlying,
                "expiration": expiration,
                "last_trading_session": last_trading_session,
                "last_trading_at": last_trading_at,
                "convention": convention,
            })
            last_trading.append(
                ActivityMomentumLastTradingEvidence(
                    expiration=expiration,
                    last_trading_session_date=(
                        last_trading_session
                    ),
                    last_trading_at=last_trading_at,
                    convention=convention,
                    evidence_reference=evidence_reference,
                )
            )
        calendar_reference = canonical_sha256({
            "calendar": calendar.calendar_name,
            "target_session_date": session_date,
            "week_end_session_date": week_end_session,
            "underlying": underlying,
            "product_convention": convention,
            "last_trading_evidence": tuple(
                row.evidence_reference for row in last_trading
            ),
        })
        values.append(ActivityMomentumExpiryCalendarEvidence(
            underlying=underlying,
            target_session_date=session_date,
            calendar_name=calendar.calendar_name,
            week_end_session_date=week_end_session,
            product_convention=convention,
            last_trading_evidence=tuple(last_trading),
            evidence_references=(calendar_reference,),
        ))
    return tuple(values)


# R16 (Nov 10, live): the provider reports delta 1.0000000000047 for deep in-the-money calls (float noise), which the
# candidate's [-1, 1] check rejected, and one rejected candidate failed the whole CAPACITY_SHARDS allocation. Within
# this tolerance the value is the bound; a delta or IV further out of range is not used (the Greeks are then absent).
_GREEK_BOUND_TOLERANCE = Decimal("0.000001")


def _provider_delta(value):
    try:
        delta = None if value is None else Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return None
    if delta is None or not delta.is_finite():
        return None
    if abs(delta) <= 1:
        return delta
    if abs(delta) - 1 <= _GREEK_BOUND_TOLERANCE:
        return Decimal(1).copy_sign(delta)
    return None


def _provider_implied_volatility(value):
    try:
        volatility = None if value is None else Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError):
        return None
    if volatility is None or not volatility.is_finite() or volatility < 0:
        return None
    return volatility


def _activity_candidate(
    row,
    raw_snapshot,
    *,
    session_date,
    observed_at,
    flow_policy,
):
    raw = dict(raw_snapshot)
    details = dict(raw.get("details") or {})
    quote_row = dict(raw.get("last_quote") or {})
    day_row = dict(raw.get("day") or raw.get("session") or {})
    greeks = dict(raw.get("greeks") or {})
    digest = canonical_sha256(raw)
    dte = (row.expiration_date - session_date).days
    try:
        flow_policy.tier_for(row.underlyer)
        flow_eligible = flow_policy.lane_for(dte) is not None
    except ValueError:
        flow_eligible = False
    option_side = (
        OptionSide.CALL
        if row.option_type == "C"
        else OptionSide.PUT
    )
    multiplier = Decimal(str(
        details.get("shares_per_contract") or 100
    ))
    identity = canonical_sha256({
        "contract_ticker": row.contract_ticker,
        "catalog_payload_sha256": digest,
    })
    open_interest = raw.get("open_interest")
    volume = day_row.get("volume")
    delta = _provider_delta(greeks.get("delta"))
    implied_volatility = _provider_implied_volatility(
        raw.get("implied_volatility")
    )
    return ActivityMomentumCandidate(
        contract_id=int(identity[:15], 16) + 1,
        contract_ticker=row.contract_ticker,
        underlying=row.underlyer,
        expiration=row.expiration_date,
        option_side=option_side,
        strike=row.strike,
        multiplier=multiplier,
        deliverable="STANDARD",
        catalog_version_id=int(identity[15:30], 16) + 1,
        catalog_payload_sha256=digest,
        observed_at=observed_at,
        spot=row.spot,
        bid=row.bid,
        ask=row.ask,
        quote_as_of=_source_datetime(
            quote_row.get("last_updated"),
            fallback=row.market_data_time,
        ),
        completed_session_volume=(
            int(volume) if isinstance(volume, int) else None
        ),
        volume_as_of=(
            _source_datetime(
                day_row.get("last_updated"),
                fallback=row.market_data_time,
            )
            if isinstance(volume, int)
            else None
        ),
        prior_open_interest=(
            int(open_interest)
            if isinstance(open_interest, int)
            and open_interest >= 0
            else None
        ),
        open_interest_as_of=(
            observed_at
            if isinstance(open_interest, int)
            and open_interest >= 0
            else None
        ),
        delta=delta,
        implied_volatility=implied_volatility,
        greeks_as_of=(
            row.market_data_time
            if delta is not None or implied_volatility is not None
            else None
        ),
        source_evidence_references=(digest,),
        flow_eligible=flow_eligible,
        flow_unavailable_reason=(
            None
            if flow_eligible
            else "FLOW_V1_DTE_OR_UNDERLYING_UNAVAILABLE"
        ),
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    checked_at = datetime.now(UTC)
    profile = load_activity_momentum_profile(
        version=args.admission_profile_version
    )
    exchange_calendar = OptionExchangeCalendar()
    configured_contract_budget = (
        profile.active_quote_contract_budget
        if args.active_quote_contract_budget is None
        else args.active_quote_contract_budget
    )
    per_underlying_limit = (
        profile.default_contracts_per_underlying
        if args.per_underlying_contract_limit is None
        else args.per_underlying_contract_limit
    )
    measured_capacity = (
        profile.established_measured_capacity
        if args.established_measured_capacity is None
        else args.established_measured_capacity
    )
    provider_project_ceiling = (
        profile.effective_provider_project_ceiling
        if args.effective_provider_project_ceiling is None
        else args.effective_provider_project_ceiling
    )
    if (
        configured_contract_budget
        > profile.active_quote_contract_budget
        or per_underlying_limit
        > profile.maximum_contracts_per_underlying
        or provider_project_ceiling
        > profile.effective_provider_project_ceiling
    ):
        raise ValueError(
            "activity/momentum configured capacity exceeds profile"
        )
    planned_contract_budget = min(
        configured_contract_budget,
        per_underlying_limit * len(args.underlyings),
        provider_project_ceiling,
        *(
            ()
            if measured_capacity is None
            else (measured_capacity,)
        ),
    )
    if not set(args.enabled_strategies).issubset(
        profile.supported_group_strategies
    ):
        raise ValueError("unsupported activity/momentum strategy")
    preparation_window_start = (
        exchange_calendar.session_open(args.session_date)
        - timedelta(hours=18)
    )
    if (
        not preparation_window_start
        <= checked_at
        <= exchange_calendar.session_close(args.session_date)
        or not checked_at <= args.authorized_until <= checked_at + timedelta(hours=1)
        or args.reference_valid_until <= args.authorized_until
        or args.reference_valid_until.date() != args.session_date
    ):
        raise ValueError("V3 pre-session clocks are invalid")
    plan = {
        "contract": "advanced-options-v3-pre-session-snapshot-v1",
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "source_cohort_id": str(args.source_cohort_id),
        "session_date": args.session_date.isoformat(),
        "underlyings": args.underlyings,
        "channels": ("T", "Q"),
        "admission_profile_version": profile.version,
        "admission_profile_sha256": profile.profile_sha256,
        "same_day_enabled": (
            profile.same_day_enabled
            and not args.disable_same_day
        ),
        "short_delta_override": (
            None
            if args.short_delta_override is None
            else str(args.short_delta_override)
        ),
        "last_trading_conventions": dict(sorted(
            args.last_trading_conventions.items()
        )),
        "configured_total_contract_budget": (
            configured_contract_budget
        ),
        "planned_active_quote_contract_budget": (
            planned_contract_budget
        ),
        "configured_contracts_per_underlying": (
            per_underlying_limit
        ),
        "effective_per_underlying_limit": (
            profile.maximum_contracts_per_underlying
        ),
        "expansion_enabled": (
            per_underlying_limit
            > profile.default_contracts_per_underlying
        ),
        "established_measured_capacity": measured_capacity,
        "effective_provider_project_ceiling": (
            provider_project_ceiling
        ),
        "quote_contract_ceiling": profile.quote_contract_ceiling,
        "quote_slot_reserve": (
            profile.quote_contract_ceiling
            - planned_contract_budget
        ),
        "maximum_moneyness_fraction": str(
            args.maximum_moneyness_fraction
        ),
        "enabled_strategies": args.enabled_strategies,
        "capture_only": args.capture_only,
        "maximum_provider_requests": args.maximum_provider_requests,
        "maximum_response_decoded_bytes": (
            args.maximum_response_decoded_bytes
        ),
        "maximum_decoded_bytes": args.maximum_decoded_bytes,
        "authorization_reference": args.authorization_reference,
    }
    if not args.execute:
        report = {
            **plan,
            "status": "PLANNED",
            "provider_requests": 0,
            "database_writes": 0,
            "semantic_sha256": canonical_sha256(plan),
        }
        write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("V3 pre-session admission requires POLYGON_API_KEY")
    request = ReferenceSeedRequest(
        UniverseManifest(
            "advanced-options-v3-pre-session-v1",
            args.underlyings,
            args.underlyings,
        ),
        args.session_date,
        checked_at,
        checked_at,
        args.reference_valid_until,
    )
    settlement = exchange_calendar.previous_session_date(
        args.session_date
    )
    provider, pool = _provider()
    try:
        transport = PacedPolygonTransport(
            AuthorizedNativeRepairTransport(
                RequestsPolygonHttpTransport(
                    api_key,
                    timeout_seconds=30,
                    maximum_response_bytes=(
                        args.maximum_response_decoded_bytes
                    ),
                ),
                args.authorized_until,
                lambda: datetime.now(UTC),
            ),
            minimum_interval_seconds=0.5,
        )
        spots = _PostgresSelectionSpots(
            provider,
            args.source_cohort_id,
        )
        source = MassiveAdvancedOptionsSelectionSource(
            transport,
            spots,
            policy=P7TransportSelectionPolicy(
                version="c6-activity-momentum-discovery-v1",
                underlyings=args.underlyings,
                minimum_dte=0,
                maximum_dte=profile.discovery_maximum_dte(
                    enabled_strategies=args.enabled_strategies,
                    required_contract_tickers=(
                        args.required_contract_tickers
                    ),
                ),
                maximum_moneyness_fraction=(
                    args.maximum_moneyness_fraction
                ),
            ),
            clock=lambda: datetime.now(UTC),
            maximum_response_decoded_bytes=(
                args.maximum_response_decoded_bytes
            ),
            maximum_total_decoded_bytes=(
                args.maximum_decoded_bytes
            ),
            maximum_provider_requests=(
                args.maximum_provider_requests
            ),
            deadline=args.authorized_until,
        )
        spot_values = spots.load_spots(
            underlyings=args.underlyings,
            available_before=checked_at,
        )
        candidates = []
        decoded_bytes = 0
        snapshot_times = []
        responses = []
        snapshots = {}
        received_at_by_underlyer = {}
        provider_requests = 0
        for underlyer in args.underlyings:
            (
                rows,
                response_bytes,
                snapshot_at,
                payload,
                raw_snapshots,
            ) = source._request(
                underlyer,
                spot=spot_values[underlyer],
                session_date=args.session_date,
                maximum_requests=(
                    args.maximum_provider_requests - provider_requests
                ),
                maximum_total_decoded_bytes=(
                    args.maximum_decoded_bytes - decoded_bytes
                ),
            )
            page_count = int(payload["page_count"])
            provider_requests += page_count
            candidates.extend(rows)
            decoded_bytes += response_bytes
            snapshot_times.append(snapshot_at)
            snapshots.update(raw_snapshots)
            responses.append({
                "underlyer": underlyer,
                "request_id": payload.get("request_id"),
                "response_sha256": canonical_sha256(payload),
                "row_count": len(payload["results"]),
                "page_count": page_count,
                "pagination_requests": int(
                    payload["pagination_requests"]
                ),
                "received_at_utc": payload["received_at_utc"],
            })
            received_at_by_underlyer[underlyer] = (
                datetime.fromisoformat(
                    str(payload["received_at_utc"])
                ).astimezone(UTC)
            )
        candidates_by_ticker = {
            row.contract_ticker: row for row in candidates
        }
        if not set(args.required_contract_tickers).issubset(
            candidates_by_ticker
        ):
            raise ValueError(
                "required admitted contract is absent from fresh snapshots"
            )
        flow_policy = load_advanced_options_flow_policy()
        activity_candidates = tuple(
            _activity_candidate(
                row,
                snapshots[row.contract_ticker],
                session_date=args.session_date,
                observed_at=received_at_by_underlyer[
                    row.underlyer
                ],
                flow_policy=flow_policy,
            )
            for row in candidates
        )
        expiry_calendars = (
            _expiry_calendar_evidence(
                exchange_calendar,
                session_date=args.session_date,
                activity_candidates=activity_candidates,
                conventions=args.last_trading_conventions,
            )
            if profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            else ()
        )
        resolution = resolve_activity_momentum_selection(
            profile,
            session_date=args.session_date,
            candidates=activity_candidates,
            enabled_strategies=args.enabled_strategies,
            required_contract_tickers=(
                args.required_contract_tickers
            ),
            preparation_at=(
                checked_at
                if profile.version
                == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
                else None
            ),
            expiry_calendars=expiry_calendars,
            same_day_enabled=(
                profile.same_day_enabled
                and not args.disable_same_day
                if profile.version
                == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
                else None
            ),
            short_delta_override=args.short_delta_override,
        )
        allocation = allocate_activity_momentum(
            profile,
            session_date=args.session_date,
            rotation_key=args.authorization_reference,
            ready_underlyings=args.underlyings,
            groups=resolution.groups,
            active_contract_budget=configured_contract_budget,
            per_underlying_contract_limit=per_underlying_limit,
            measured_capacity_limit=measured_capacity,
            effective_provider_project_ceiling=(
                provider_project_ceiling
            ),
            resolution=resolution,
        )
        if not set(args.required_contract_tickers).issubset(
            allocation.selected_contract_tickers
        ):
            raise ValueError(
                "required strategy group was not admitted"
            )
        selected_rows = tuple(
            candidates_by_ticker[ticker]
            for ticker in allocation.selected_contract_tickers
        )
        selected_snapshots = {
            row.contract_ticker: snapshots[row.contract_ticker]
            for row in selected_rows
        }
        if (
            provider_requests > args.maximum_provider_requests
            or decoded_bytes > args.maximum_decoded_bytes
        ):
            raise RuntimeError("V3 pre-session provider budget changed")
        completed_at = max(
            datetime.now(UTC),
            *received_at_by_underlyer.values(),
        )
        if completed_at > args.authorized_until:
            raise TimeoutError(
                "V3 pre-session completion exceeded authorization"
            )
        selection_sha256 = canonical_sha256({
            "schema": "advanced-options-v3-pre-session-selection-v1",
            "reviewed_at": checked_at,
            "responses": responses,
            "selected": tuple(
                row.contract_ticker for row in selected_rows
            ),
        })
        capture_document = {
            "contract": "advanced-options-v3-pre-session-capture-v1",
            "compatibility_profile": (
                SEALED_CAPTURE_GENERALIZED_C6_PROFILE
            ),
            "configured_underlyings": list(args.underlyings),
            "captured_at_utc": completed_at.isoformat(),
            "session_date": args.session_date.isoformat(),
            "source_cohort_id": str(args.source_cohort_id),
            "provider_requests": provider_requests,
            "decoded_bytes": decoded_bytes,
            "responses": responses,
            "selection_sha256": selection_sha256,
            "selected_snapshots": selected_snapshots,
            "activity_momentum_allocation": (
                activity_momentum_allocation_payload(allocation)
            ),
        }
        capture_document["semantic_sha256"] = canonical_sha256(
            capture_document
        )
        write_report(args.capture_path, capture_document)
        if selected_rows:
            facts, selected = _facts(
                selected_rows,
                selected_snapshots,
                request,
                settlement,
                underlyings=args.underlyings,
            )
            database_writes = _persist_facts(
                provider,
                facts,
                capture_only=args.capture_only,
            )
        else:
            facts = ()
            selected = []
            database_writes = 0
    finally:
        pool.closeall()
    report = {
        **plan,
        "status": (
            "NO_ELIGIBLE_CONTRACTS"
            if not selected_rows
            else "CAPTURED_NOT_APPLIED"
            if args.capture_only
            else "COMPLETE"
        ),
        "provider_requests": provider_requests,
        "provider_response_bytes": decoded_bytes,
        "source_snapshot_at_utc": min(snapshot_times).isoformat(),
        "selection_sha256": selection_sha256,
        "effective_active_quote_contract_budget": (
            allocation.active_contract_budget
        ),
        "subscription_attempted": bool(selected_rows),
        "selected": selected,
        "settlement_session": settlement.isoformat(),
        "database_writes": database_writes,
        "fact_sha256": canonical_sha256(facts),
        "credentials_exposed": False,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
