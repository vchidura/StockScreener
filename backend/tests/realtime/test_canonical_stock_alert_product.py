from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from equity.model_detection.contracts import Disposition
from realtime.canonical_stock_alert_product import (
    CanonicalStockAlertContext,
    canonical_stock_alert_contexts,
    produce_canonical_stock_alert_batch,
)
from realtime.contracts import canonical_sha256
from realtime.preflight import ProviderTier
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


def test_missing_sector_and_event_context_retains_unavailable_alert_rows():
    inputs = _inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
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


def test_match_is_emitted_only_when_all_context_gates_are_available():
    inputs = _inputs()
    base = canonical_stock_alert_contexts(inputs)
    contexts = {
        identity: CanonicalStockAlertContext(
            market_trend=context.market_trend,
            sector_trend=Decimal("0.01"),
            event_risk=False,
            source_record_id=context.source_record_id,
            source_sha256=context.source_sha256,
            available_at=context.available_at,
            received_at=context.received_at,
            contributors=context.contributors,
        )
        for identity, context in base.items()
    }
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


def test_canonical_portal_snapshot_exposes_unavailable_reasons():
    inputs = _inputs()
    batch = produce_canonical_stock_alert_batch(
        inputs,
        window_id=WINDOW,
        input_receipt_id=RECEIPT,
        receipt_input_sha256="b" * 64,
    )
    retained = {
        "contract": "canonical-stock-alert-retained-projection-v1",
        "loaded_at": (CUTOFF + timedelta(seconds=2)).isoformat(),
        "batch": {
            "batch_id": str(uuid5(NAMESPACE_URL, "batch")),
            "batch_sha256": batch.batch_sha256,
            "cohort_id": str(COHORT),
            "window_id": str(WINDOW),
            "input_receipt_id": str(RECEIPT),
            "cutoff": CUTOFF.isoformat(),
            "receipt_input_sha256": "b" * 64,
            "detector_input_sha256": batch.detector_input_sha256,
            "detector_sha256": batch.version.detector_sha256,
            "completed_at": batch.completed_at.isoformat(),
        },
        "evaluations": [
            {
                "evaluation_id": str(uuid5(
                    NAMESPACE_URL,
                    f"evaluation:{row.ticker}",
                )),
                "event_id": str(row.event.event_id),
                "security_id": str(row.security_id),
                "instrument_id": row.instrument_id,
                "ticker": row.ticker,
                "disposition": row.decision.disposition.value,
                "reason_codes": list(row.decision.reason_codes),
                "input_sha256": row.input_sha256,
                "evaluation_sha256": canonical_sha256(row.event.payload),
                "payload": row.event.payload,
                "alert_id": str(row.alert.alert_id),
                "alert": dict(row.alert.projection),
                "observed_at": row.event.decision_at.isoformat(),
            }
            for row in batch.evaluations
        ],
    }
    retained["projection_sha256"] = canonical_sha256(retained)

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
