from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import pytest

from equity.model_detection.contracts import Disposition
from equity.model_detection.policies import ContextOptionalBreakoutPolicy
from realtime.canonical_stock_alert_product import (
    canonical_stock_alert_contexts,
    canonical_stock_alert_membership,
    produce_canonical_stock_alert_batch,
)
from realtime.canonical_stock_alert_repository import _membership_cursor
from realtime.contracts import canonical_sha256
from realtime.equity_history import EquityHistoryScope
from realtime.preflight import ProviderTier
from realtime.reference_continuity import source_manifest_identity
from realtime.screening_projection import RealtimeScreeningInput
from realtime.stock_alert_projection import (
    produce_realtime_stock_alerts,
)
from research.stock_alerts import alert_page


UTC = timezone.utc
SESSION = date(2026, 10, 28)
CUTOFF = datetime(2026, 10, 28, 14, 0, tzinfo=UTC)
COHORT = uuid5(NAMESPACE_URL, "canonical-stock-alert-cohort")
WINDOW = uuid5(NAMESPACE_URL, "canonical-stock-alert-window")
RECEIPT = uuid5(NAMESPACE_URL, "canonical-stock-alert-receipt")


def _member(ticker, instrument_id):
    return {
        "security_id": uuid5(NAMESPACE_URL, f"security:{ticker}"),
        "instrument_id": instrument_id,
        "ticker": ticker,
        "exchange": "XNYS",
        "company_name": ticker,
        "security_revision_id": uuid5(
            NAMESPACE_URL,
            f"reference:{ticker}",
        ),
        "payload_sha256": canonical_sha256({
            "reference": ticker,
        }),
        "observed_at": CUTOFF - timedelta(days=1),
    }


def _bar(member, index, *, closes, volumes):
    start = CUTOFF - timedelta(minutes=5 * (3 - index))
    close = Decimal(str(closes[index]))
    return {
        "bar_revision_id": uuid5(
            NAMESPACE_URL,
            f"bar:{member['ticker']}:{index}",
        ),
        "cohort_id": COHORT,
        "security_id": member["security_id"],
        "ticker": member["ticker"],
        "interval": "5m",
        "adjustment_basis": "RAW_UNADJUSTED",
        "session_date": SESSION,
        "bar_start": start,
        "bar_end": start + timedelta(minutes=5),
        "open": close - Decimal("0.5"),
        "high": close + Decimal("0.5"),
        "low": close - Decimal("1"),
        "close": close,
        "volume": Decimal(str(volumes[index])),
        "observed_at": start + timedelta(minutes=5, seconds=1),
        "created_at": start + timedelta(minutes=5, seconds=1),
        "provider_published_at": start + timedelta(minutes=5),
        "replay_available_at": start + timedelta(minutes=5),
        "is_final": True,
        "payload_sha256": canonical_sha256({
            "ticker": member["ticker"],
            "index": index,
            "close": close,
            "volume": volumes[index],
        }),
    }


def _inputs():
    members = (_member("AAPL", 1), _member("SPY", 2))
    bars = tuple(
        _bar(
            member,
            index,
            closes=(
                (100, 101, 103)
                if member["ticker"] == "AAPL"
                else (500, 501, 502)
            ),
            volumes=(100, 100, 250),
        )
        for member in members
        for index in range(3)
    )
    return RealtimeScreeningInput(
        cohort_id=COHORT,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        source_manifest_sha256="a" * 64,
        source_generation=1,
        session=SESSION,
        cutoff=CUTOFF,
        known_by=CUTOFF + timedelta(seconds=2),
        members=members,
        bars=bars,
        actions=(),
        coverage=(),
    )


def _ready_context_inputs(*, event_risk=False):
    base = _inputs()
    product_members = (
        {
            **base.members[0],
            "security_type": "CS",
            "sector": "IT - Semiconductors & Hardware",
            "sic_code": "3571",
        },
        {
            **base.members[1],
            "security_type": "ETF",
            "sector": "ETF",
        },
    )
    context_members = (
        {
            **_member("SMH", 3),
            "security_type": "ETF",
            "sector": "ETF",
        },
        {
            **_member("QQQ", 4),
            "security_type": "ETF",
            "sector": "ETF",
        },
    )
    all_members = (*product_members, *context_members)
    bars = tuple(
        _bar(
            member,
            index,
            closes=(
                (100, 101, 103)
                if member["ticker"] == "AAPL"
                else (100, 101, 102)
            ),
            volumes=(100, 100, 250),
        )
        for member in all_members
        for index in range(3)
    )
    inputs = replace(base, members=product_members, bars=bars)
    checked_at = CUTOFF - timedelta(minutes=5)
    valid_until = CUTOFF + timedelta(days=7)
    coverage = (
        {
            "coverage_id": uuid5(
                NAMESPACE_URL,
                "canonical-event-coverage:aapl",
            ),
            "security_id": product_members[0]["security_id"],
            "event_type": "EARNINGS",
            "window_start": (
                CUTOFF - timedelta(days=1)
            ).date(),
            "window_end": (
                CUTOFF + timedelta(hours=72)
            ).date(),
            "source": "finnhub",
            "verdict": "KNOWN_NO_EVENT",
            "checked_at": checked_at,
            "valid_until": valid_until,
            "evidence_sha256": canonical_sha256(
                ("earnings-coverage", "AAPL")
            ),
        },
        {
            "coverage_id": uuid5(
                NAMESPACE_URL,
                "canonical-event-coverage:fed",
            ),
            "security_id": None,
            "event_type": "FED_RATE_DECISION",
            "window_start": (
                CUTOFF - timedelta(days=1)
            ).date(),
            "window_end": (
                CUTOFF + timedelta(hours=72)
            ).date(),
            "source": "federal_reserve",
            "verdict": "KNOWN_NO_EVENT",
            "checked_at": checked_at,
            "valid_until": valid_until,
            "evidence_sha256": canonical_sha256("fed-coverage"),
        },
    )
    events = ()
    if event_risk:
        events = ({
            "revision_id": uuid5(
                NAMESPACE_URL,
                "canonical-event:aapl-earnings",
            ),
            "security_id": product_members[0]["security_id"],
            "event_key": "aapl-earnings",
            "event_type": "EARNINGS",
            "event_date": (CUTOFF + timedelta(days=1)).date(),
            "announced_at": None,
            "event_timezone": "America/New_York",
            "time_precision": "DATE_ONLY",
            "status": "SCHEDULED",
            "source": "finnhub",
            "observed_at": checked_at,
            "received_at": checked_at,
            "valid_until": valid_until,
            "payload_sha256": canonical_sha256(
                ("earnings-event", "AAPL")
            ),
        },)
    return inputs, context_members, events, coverage


def _retained_projection(batch):
    retained = {
        "contract": "canonical-stock-alert-retained-projection-v1",
        "loaded_at": batch.completed_at.isoformat(),
        "batch": {
            "batch_id": str(uuid5(
                NAMESPACE_URL,
                f"batch:{batch.batch_sha256}",
            )),
            "batch_sha256": batch.batch_sha256,
            "cohort_id": str(batch.cohort_id),
            "window_id": str(batch.window_id),
            "input_receipt_id": str(batch.input_receipt_id),
            "session_date": batch.session_date.isoformat(),
            "cutoff": batch.cutoff.isoformat(),
            "receipt_input_sha256": batch.receipt_input_sha256,
            "detector_input_sha256": batch.detector_input_sha256,
            "detector_sha256": batch.version.detector_sha256,
            "desired_revision_id": (
                None
                if batch.membership.desired_revision_id is None
                else str(batch.membership.desired_revision_id)
            ),
            "desired_snapshot_sha256": (
                batch.membership.desired_snapshot_sha256
            ),
            "membership_sha256": batch.membership.membership_sha256,
            "membership": batch.membership.document,
            "late_catch_up": batch.is_late,
            "deliverable": not batch.is_late,
            "completed_at": batch.completed_at.isoformat(),
        },
        "evaluations": [
            {
                "evaluation_id": str(uuid5(
                    NAMESPACE_URL,
                    f"evaluation:{batch.batch_sha256}:{row.ticker}",
                )),
                "event_id": str(row.event.event_id),
                "security_id": str(row.security_id),
                "instrument_id": row.instrument_id,
                "ticker": row.ticker,
                "disposition": row.decision.disposition.value,
                "reason_codes": list(row.decision.reason_codes),
                "input_sha256": row.input_sha256,
                "evaluation_sha256": canonical_sha256(
                    row.event.payload
                ),
                "payload": row.event.payload,
                "alert_id": (
                    None
                    if row.alert is None
                    else str(row.alert.alert_id)
                ),
                "alert": (
                    None
                    if row.alert is None
                    else dict(row.alert.projection)
                ),
                "observed_at": row.event.decision_at.isoformat(),
                "desired_revision_id": (
                    None
                    if batch.membership.desired_revision_id is None
                    else str(batch.membership.desired_revision_id)
                ),
                "desired_snapshot_sha256": (
                    batch.membership.desired_snapshot_sha256
                ),
                "membership_sha256": (
                    batch.membership.membership_sha256
                ),
                "observation_cutoff": batch.cutoff.isoformat(),
                "late_catch_up": batch.is_late,
                "deliverable": not batch.is_late,
            }
            for row in batch.evaluations
        ],
    }
    retained["projection_sha256"] = canonical_sha256(retained)
    return retained


REQUIRED_CONTEXT = ContextOptionalBreakoutPolicy(optional_context=())


def test_e_context_provides_market_only_and_retains_spy_lineage():
    inputs = _inputs()
    contexts = canonical_stock_alert_contexts(inputs)

    for context in contexts.values():
        assert context.market_trend is not None
        assert context.sector_trend is None
        assert context.event_risk is None
        assert context.reason_codes == (
            "MISSING_SECTOR_CONTEXT",
            "MISSING_EVENT_CONTEXT",
        )
        assert len(context.contributors) == 3
    spy_ids = {
        str(row["bar_revision_id"])
        for row in inputs.bars
        if row["ticker"] == "SPY"
    }
    assert {
        revision_id
        for revision_id, _payload_sha256
        in contexts[str(inputs.members[0]["security_id"])].contributors
    } == spy_ids


def test_stream_bars_without_publication_clocks_provide_market_context():
    """Live stream bars carry no provider or replay publication clocks."""
    inputs = _inputs()
    for absent in (
        lambda row: {
            **row,
            "provider_published_at": None,
            "replay_available_at": None,
        },
        lambda row: {
            name: value
            for name, value in row.items()
            if name not in {"provider_published_at", "replay_available_at"}
        },
    ):
        contexts = canonical_stock_alert_contexts(replace(
            inputs,
            bars=tuple(absent(row) for row in inputs.bars),
        ))

        for context in contexts.values():
            assert context.market_trend is not None
            assert context.available_at == CUTOFF
            assert context.received_at == CUTOFF + timedelta(seconds=1)


def test_bars_without_any_optional_clock_fall_back_to_bar_end():
    inputs = _inputs()
    optional = {
        "provider_published_at",
        "replay_available_at",
        "observed_at",
        "created_at",
    }

    contexts = canonical_stock_alert_contexts(replace(
        inputs,
        bars=tuple(
            {
                name: value
                for name, value in row.items()
                if name not in optional
            }
            for row in inputs.bars
        ),
    ))

    for context in contexts.values():
        assert context.available_at == CUTOFF
        assert context.received_at == CUTOFF


def test_missing_sector_and_event_context_retains_unavailable_alert_rows():
    inputs = _inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        policy=REQUIRED_CONTEXT,
    )

    assert len(batch.evaluations) == 2
    assert all(
        row.decision.disposition is Disposition.UNAVAILABLE
        for row in batch.evaluations
    )
    assert all(
        row.alert is not None
        and row.alert.lifecycle == "UNAVAILABLE"
        and row.alert.projection["non_actionable"] is True
        and row.alert.projection["trigger_price"] is None
        for row in batch.evaluations
    )


def test_no_sector_or_event_context_still_opens_an_alert_and_says_so():
    inputs, _context_members, _events, _coverage = _ready_context_inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
    )

    aapl = next(row for row in batch.evaluations if row.ticker == "AAPL")
    assert aapl.decision.disposition is Disposition.MATCH
    assert aapl.alert is not None
    assert aapl.alert.lifecycle == "OPEN"
    assert aapl.alert.projection["non_actionable"] is False
    assert aapl.alert.projection["execution_permission"] is False
    assert aapl.alert.projection["trigger_price"] is not None
    gates = {row["name"]: row["status"] for row in aapl.alert.projection["gates"]}
    assert gates["sector"] == "NOT_APPLICABLE"
    assert gates["event"] == "NOT_APPLICABLE"
    assert gates["market"] == "PASS"


def test_retained_sector_and_event_context_produce_match():
    inputs, context_members, events, coverage = _ready_context_inputs()
    contexts = canonical_stock_alert_contexts(
        inputs,
        context_members=context_members,
        market_events=events,
        market_event_coverage=coverage,
    )
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        contexts=contexts,
    )

    aapl = next(row for row in batch.evaluations if row.ticker == "AAPL")
    assert aapl.decision.disposition is Disposition.MATCH
    assert aapl.alert is not None
    assert aapl.alert.lifecycle == "OPEN"
    assert aapl.alert.projection["non_actionable"] is False
    context = contexts[str(aapl.security_id)]
    assert context.event_risk is False
    assert context.sector_trend is not None
    assert {
        row["role"] for row in context.evidence
    } >= {
        "MARKET_BAR",
        "SECURITY_REFERENCE",
        "SECTOR_MAPPING",
        "SECTOR_BENCHMARK_REFERENCE",
        "SECTOR_BAR",
        "MARKET_EVENT_COVERAGE",
    }


def test_real_nine_member_shape_uses_context_only_sector_proxies():
    tickers = (
        "AAPL",
        "AMZN",
        "IWM",
        "MSFT",
        "NFLX",
        "NVDA",
        "QQQ",
        "SPY",
        "TSLA",
    )
    product_members = tuple(
        {
            **_member(ticker, index + 1),
            "security_type": "ETF" if ticker in {"IWM", "QQQ", "SPY"} else "CS",
            "sector": (
                "ETF"
                if ticker in {"IWM", "QQQ", "SPY"}
                else "IT - Software & Services"
                if ticker in {"MSFT", "NFLX"}
                else "IT - Semiconductors & Hardware"
            ),
        }
        for index, ticker in enumerate(tickers)
    )
    context_members = (
        {
            **_member("IGV", 101),
            "security_type": "ETF",
            "sector": "ETF",
        },
        {
            **_member("SMH", 102),
            "security_type": "ETF",
            "sector": "ETF",
        },
    )
    all_members = (*product_members, *context_members)
    base = _inputs()
    inputs = replace(
        base,
        members=product_members,
        bars=tuple(
            _bar(
                member,
                index,
                closes=(100, 101, 103),
                volumes=(100, 100, 250),
            )
            for member in all_members
            for index in range(3)
        ),
    )
    membership = canonical_stock_alert_membership(inputs)
    contexts = canonical_stock_alert_contexts(
        inputs,
        membership=membership,
        context_members=context_members,
    )
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        membership=membership,
        contexts=contexts,
    )

    assert {row["ticker"] for row in membership.effective_members} == set(
        tickers
    )
    assert {row.ticker for row in batch.evaluations} == set(tickers)
    assert len(batch.evaluations) == 9
    assert set(contexts) == {
        str(row["security_id"]) for row in product_members
    }
    assert not {"IGV", "SMH"} & {
        row["ticker"] for row in membership.effective_members
    }
    aapl = contexts[str(product_members[0]["security_id"])]
    sector_bars = tuple(
        row for row in aapl.evidence if row["role"] == "SECTOR_BAR"
    )
    expected = tuple(
        row
        for row in inputs.bars
        if row["ticker"] == "SMH"
    )
    assert {
        (row["source_id"], row["payload_sha256"])
        for row in sector_bars
    } == {
        (str(row["bar_revision_id"]), row["payload_sha256"])
        for row in expected
    }
    assert aapl.available_at == max(
        row["provider_published_at"] for row in expected
    )
    assert aapl.received_at == max(
        row["observed_at"] for row in expected
    )


def test_relevant_retained_event_vetoes_without_creating_alert():
    inputs, context_members, events, coverage = _ready_context_inputs(
        event_risk=True
    )
    coverage = tuple(
        row for row in coverage if row["event_type"] != "EARNINGS"
    )
    contexts = canonical_stock_alert_contexts(
        inputs,
        context_members=context_members,
        market_events=events,
        market_event_coverage=coverage,
    )
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        contexts=contexts,
    )

    aapl = next(row for row in batch.evaluations if row.ticker == "AAPL")
    assert contexts[str(aapl.security_id)].event_risk is True
    assert aapl.decision.disposition is Disposition.NO_MATCH
    assert aapl.decision.reason_codes == ("GATE_EVENT_FAILED",)
    assert aapl.alert is None


def test_missing_sector_mapping_is_member_local():
    inputs, context_members, events, coverage = _ready_context_inputs()
    members = tuple(
        (
            {
                **member,
                "sector": None,
            }
            if member["ticker"] == "AAPL"
            else member
        )
        for member in inputs.members
    )
    inputs = replace(inputs, members=members)
    contexts = canonical_stock_alert_contexts(
        inputs,
        context_members=context_members,
        market_events=events,
        market_event_coverage=coverage,
    )
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        contexts=contexts,
        policy=REQUIRED_CONTEXT,
    )

    dispositions = {
        row.ticker: row.decision.disposition
        for row in batch.evaluations
    }
    assert dispositions["AAPL"] is Disposition.UNAVAILABLE
    assert all(
        disposition is not Disposition.UNAVAILABLE
        for ticker, disposition in dispositions.items()
        if ticker != "AAPL"
    )


def test_missing_event_coverage_is_member_local_and_never_clear():
    inputs, context_members, events, coverage = _ready_context_inputs()
    coverage = tuple(
        row for row in coverage if row["event_type"] != "EARNINGS"
    )
    contexts = canonical_stock_alert_contexts(
        inputs,
        context_members=context_members,
        market_events=events,
        market_event_coverage=coverage,
    )

    aapl = contexts[str(inputs.members[0]["security_id"])]
    spy = contexts[str(inputs.members[1]["security_id"])]
    assert aapl.event_risk is None
    assert "MISSING_EVENT_CONTEXT" in aapl.reason_codes
    assert spy.event_risk is False


@pytest.mark.parametrize("clock_state", ["late", "stale"])
def test_late_or_stale_event_evidence_is_not_decision_visible(clock_state):
    inputs, context_members, events, coverage = _ready_context_inputs()
    if clock_state == "late":
        coverage = tuple(
            {
                **row,
                "checked_at": CUTOFF + timedelta(seconds=1),
            }
            if row["event_type"] == "EARNINGS"
            else row
            for row in coverage
        )
    else:
        coverage = tuple(
            {
                **row,
                "valid_until": CUTOFF,
            }
            if row["event_type"] == "EARNINGS"
            else row
            for row in coverage
        )

    contexts = canonical_stock_alert_contexts(
        inputs,
        context_members=context_members,
        market_events=events,
        market_event_coverage=coverage,
    )

    assert contexts[
        str(inputs.members[0]["security_id"])
    ].event_risk is None


def test_context_checksum_drift_fails_closed():
    inputs, context_members, events, coverage = _ready_context_inputs()
    context = canonical_stock_alert_contexts(
        inputs,
        context_members=context_members,
        market_events=events,
        market_event_coverage=coverage,
    )[str(inputs.members[0]["security_id"])]

    with pytest.raises(
        ValueError,
        match="context identity changed",
    ):
        replace(context, source_sha256="0" * 64)


def test_canonical_portal_snapshot_exposes_unavailable_reasons():
    inputs = _inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        policy=REQUIRED_CONTEXT,
    )
    retained = _retained_projection(batch)

    snapshot = produce_realtime_stock_alerts(
        inputs,
        retained_projection=retained,
    )

    assert snapshot.payload["canonical_product"] is True
    assert len(snapshot.payload["shadow"]["alerts"]) == 2
    assert all(
        row["status"] == "UNAVAILABLE"
        and row["lane"] == "OBSERVATION"
        and row["non_actionable"] is True
        for row in snapshot.payload["shadow"]["alerts"]
    )
    assert {
        reason
        for row in snapshot.payload["observations"]
        for reason in row["reason_codes"]
    } == {"MISSING_SECTOR_CONTEXT", "MISSING_EVENT_CONTEXT"}
    page = alert_page(snapshot.payload["shadow"])
    assert page["total"] == 2
    assert all(
        row["status"] == "UNAVAILABLE"
        and row["non_actionable"] is True
        for row in page["rows"]
    )


def test_canonical_portal_snapshot_carries_chained_history_identity():
    scopes = (
        EquityHistoryScope(
            uuid5(NAMESPACE_URL, "canonical-history-1"),
            ProviderTier.MASSIVE_DEVELOPER,
            CUTOFF - timedelta(days=60),
            "c" * 64,
        ),
        EquityHistoryScope(
            uuid5(NAMESPACE_URL, "canonical-history-2"),
            ProviderTier.MASSIVE_DEVELOPER,
            CUTOFF - timedelta(days=365),
            "d" * 64,
        ),
    )
    chain = [scope.payload() for scope in scopes]
    inputs = replace(
        _inputs(),
        source_manifest_sha256=source_manifest_identity(
            cohort_manifest_sha256="e" * 64,
            history_binding_sha256=canonical_sha256(chain),
        ),
        history_source_cohort_id=scopes[0].cohort_id,
        history_source_provider_tier=scopes[0].provider_tier,
        history_before=scopes[0].history_before,
        history_binding_sha256=scopes[0].binding_sha256,
        history_scopes=scopes,
    )
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
    )

    snapshot = produce_realtime_stock_alerts(
        inputs,
        retained_projection=_retained_projection(batch),
    )

    assert snapshot.source_manifest["history_scope_chain"] == chain


def test_variable_membership_and_one_warming_member_are_partial_not_blocked():
    base = _inputs()
    members = (
        *base.members,
        _member("MSFT", 3),
        _member("QQQ", 4),
        _member("XLF", 5),
    )
    bars = tuple(
        _bar(
            member,
            index,
            closes=(100, 101, 102),
            volumes=(100, 110, 120),
        )
        for member in members
        for index in range(3)
    )
    inputs = replace(base, members=members, bars=bars)
    intended = tuple({
        "security_id": row["security_id"],
        "instrument_id": row["instrument_id"],
        "ticker": row["ticker"],
        "member_kind": (
            "SECTOR_ETF" if row["ticker"] == "XLF" else "EQUITY"
        ),
        "enabled": True,
        "status": "READY",
        "reason_codes": (),
    } for row in members) + ({
        "security_id": None,
        "instrument_id": None,
        "ticker": "NVDA",
        "member_kind": "EQUITY",
        "enabled": True,
        "status": "WARMING",
        "reason_codes": ("HISTORY_WARMING",),
    },)
    membership = canonical_stock_alert_membership(
        inputs,
        desired_revision_id=uuid5(
            NAMESPACE_URL,
            "desired-revision:variable",
        ),
        desired_snapshot_sha256="c" * 64,
        intended_members=intended,
    )
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        membership=membership,
    )
    snapshot = produce_realtime_stock_alerts(
        inputs,
        retained_projection=_retained_projection(batch),
    )

    expected_members = tuple(
        row for row in members if row["ticker"] != "XLF"
    )
    assert len(batch.evaluations) == len(expected_members) == 4
    assert {
        row.ticker for row in batch.evaluations
    } == {row["ticker"] for row in expected_members}
    assert batch.membership.partial is True
    assert snapshot.payload["membership_status"] == "PARTIAL"
    assert snapshot.payload["shadow"]["membership_status"] == "PARTIAL"
    assert snapshot.payload["shadow"]["status"] == "DEGRADED"
    assert snapshot.payload["shadow"]["publications"][0]["expected"] == 4
    assert snapshot.payload["shadow"]["publications"][0]["status"] == "PARTIAL"
    assert any(
        row["ticker"] == "NVDA" and row["status"] == "WARMING"
        for row in snapshot.payload["membership"]["intended_members"]
    )


def test_canonical_snapshot_retains_prior_intraday_publications():
    inputs = _inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
    )
    retained = _retained_projection(batch)
    prior = {
        "batch": deepcopy(retained["batch"]),
        "evaluations": deepcopy(retained["evaluations"]),
    }
    prior["batch"]["batch_id"] = str(uuid5(
        NAMESPACE_URL,
        "prior-canonical-alert-batch",
    ))
    prior["batch"]["cutoff"] = (
        batch.cutoff - timedelta(minutes=5)
    ).isoformat()
    prior["batch"]["completed_at"] = (
        batch.completed_at - timedelta(minutes=5)
    ).isoformat()
    retained["history"] = [prior]
    retained["projection_sha256"] = canonical_sha256({
        key: value
        for key, value in retained.items()
        if key != "projection_sha256"
    })

    snapshot = produce_realtime_stock_alerts(
        inputs,
        retained_projection=retained,
    )

    publications = snapshot.payload["shadow"]["publications"]
    assert [row["run_id"] for row in publications] == [
        prior["batch"]["batch_id"],
        retained["batch"]["batch_id"],
    ]
    assert len(snapshot.payload["shadow"]["alerts"]) >= len(
        retained["evaluations"]
    )


def test_late_catch_up_keeps_original_cutoff_and_is_not_deliverable():
    inputs = _inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
        deadline=CUTOFF + timedelta(seconds=1),
    )
    snapshot = produce_realtime_stock_alerts(
        inputs,
        retained_projection=_retained_projection(batch),
    )

    assert batch.is_late is True
    assert all(
        row.event.payload["observation_cutoff"] == CUTOFF.isoformat()
        and row.event.payload["deliverable"] is False
        for row in batch.evaluations
    )
    assert snapshot.generated_at == CUTOFF
    assert snapshot.payload["late_catch_up"] is True
    assert snapshot.payload["deliverable"] is False
    assert snapshot.payload["shadow"]["status"] == "STALE"
    assert snapshot.payload["shadow"]["deliverable"] is False
    assert all(
        row["deliverable"] is False
        and row["delivery_permission"] is False
        for row in snapshot.payload["shadow"]["alerts"]
    )


def test_repository_membership_uses_snapshot_and_keeps_warming_intent():
    inputs = _inputs()
    revision_id = uuid5(NAMESPACE_URL, "desired-revision:repository")
    rows = [
        ((str(revision_id), "d" * 64),),
        tuple(
            (
                row["ticker"],
                "EQUITY",
                True,
                "READY",
                [],
                str(row["security_id"]),
                row["instrument_id"],
            )
            for row in inputs.members
        ) + ((
            "NVDA",
            "EQUITY",
            True,
            "WARMING",
            ["HISTORY_WARMING"],
            None,
            None,
        ),),
    ]

    class Cursor:
        def __init__(self):
            self.index = -1

        def execute(self, _statement, _parameters):
            self.index += 1

        def fetchall(self):
            return rows[self.index]

    membership = _membership_cursor(Cursor(), inputs)

    assert membership.desired_revision_id == revision_id
    assert membership.desired_snapshot_sha256 == "d" * 64
    assert membership.partial is True
    assert [row["ticker"] for row in membership.effective_members] == [
        "AAPL",
        "SPY",
    ]
    warming = next(
        row
        for row in membership.intended_members
        if row["ticker"] == "NVDA"
    )
    assert warming["status"] == "WARMING"
