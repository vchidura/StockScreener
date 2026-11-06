from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_cycle import (
    NOW,
    _complete,
    _gate_and_receipt,
    _reference_repository,
)
from realtime.advanced_options_cycle import run_advanced_options_cycle
from realtime.advanced_options_projection import (
    AdvancedOptionsAlertsPagePublication,
    AdvancedOptionsCanaryPublicationAuthorization,
    AdvancedOptionsPublicationAuthorization,
    OPTIONS_PAGE_CONTRACTS,
    OPTIONS_PAGE_SNAPSHOT_TYPES,
    OPTIONS_PROJECTION_TYPES,
    OPTIONS_SNAPSHOT_TYPES,
    ADVANCED_OPTIONS_PAGE_FLOW_EVENT_DETAIL_LIMIT,
    authorize_advanced_options_canary_publication,
    authorize_advanced_options_publication,
    build_advanced_options_projection_batch,
    build_advanced_options_publisher_handoff,
    load_advanced_options_page_release_cursor,
    load_advanced_options_page_source_cursor,
    persist_advanced_options_projection_cursor,
    read_advanced_options_projection_cursor,
)
from realtime.contracts import canonical_sha256
from realtime.option_alert_projection import DetectorEvaluationQuery
from realtime.orchestration_repository import OPTIONS_PORTAL_READY
from realtime.preflight import DetectorFamily


def _cycle():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    executors = {
        detector: (lambda _context: _complete())
        for detector in control.manifest_policy.required_detectors
        if detector is not DetectorFamily.O2
    }
    return control, run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors=executors,
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p4-build",
    )


def test_page_release_loader_uses_the_selected_release_identity():
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = ()
    release_id = uuid4()

    assert load_advanced_options_page_release_cursor(
        cursor,
        cohort_id=UUID(int=2),
        available_by=NOW,
        artifact_kind="FLOW",
        release_id=release_id,
        release_cutoff=NOW,
    ) is None

    statement, parameters = cursor.execute.call_args.args
    assert "portal_release_current" not in statement
    assert "WHERE release.release_id=%s" in statement
    assert parameters[1:4] == (
        str(release_id),
        NOW,
        str(UUID(int=2)),
    )


def test_page_source_uses_complete_aggregates_and_bounded_detail():
    control = _control_plane()
    underlyer = control.universe.option_underlyings[0]
    contract_ticker = (
        f"O:{underlyer}{NOW:%y%m%d}C00500000"
    )
    aggregate = {
        "event_id": uuid4(),
        "event_time": NOW,
        "first_event_time": NOW - timedelta(minutes=5),
        "contract_ticker": contract_ticker,
        "contract_id": 1,
        "underlyer": underlyer,
        "price": Decimal("1.25"),
        "size": 6001,
        "multiplier": 100,
        "premium": Decimal("750125"),
        "prior_open_interest": None,
        "event_count": 6001,
    }
    detail = {
        key: value
        for key, value in aggregate.items()
        if key not in {"first_event_time", "event_count"}
    }
    cursor = MagicMock()
    admitted = tuple(
        (
            member.instrument_id,
            index + 100,
            f"O:{underlyer}{NOW:%y%m%d}C{index + 1:08d}",
            underlyer,
            "C",
            NOW.date() + timedelta(days=30),
            Decimal(index + 1),
            "ELIGIBLE",
            NOW,
        )
        for index, member in enumerate(control.slot_set.members)
    )
    cursor.fetchall.side_effect = (
        (aggregate,),
        (detail,),
        admitted,
    )
    cycle = SimpleNamespace(
        input_gate=SimpleNamespace(
            closure=SimpleNamespace(
                cycle_input=SimpleNamespace(
                    cohort_id=control.cohort_id,
                )
            )
        ),
        evaluations=(),
    )

    source = load_advanced_options_page_source_cursor(
        cursor,
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
    )

    assert source.flow_events[0]["event_count"] == 6001
    assert source.flow_event_detail == (detail,)
    assert source.admitted_contracts[0]["instrument_id"] == (
        control.slot_set.members[0].instrument_id
    )
    aggregate_query = cursor.execute.call_args_list[0].args[0]
    detail_query, detail_parameters = (
        cursor.execute.call_args_list[1].args
    )
    assert "GROUP BY event.contract_ticker" in aggregate_query
    assert "LIMIT" not in aggregate_query
    assert "event.event_time>%s" not in aggregate_query
    assert detail_query.endswith(
        "ORDER BY event.event_time DESC,event.event_id DESC LIMIT %s"
    )
    assert detail_parameters[-1] == (
        ADVANCED_OPTIONS_PAGE_FLOW_EVENT_DETAIL_LIMIT
    )


def test_page_source_recomputes_full_session_for_late_inserts_and_corrections():
    control = _control_plane()
    underlyer = control.universe.option_underlyings[0]
    contract_ticker = f"O:{underlyer}{NOW:%y%m%d}C00500000"
    base = {
        "event_id": uuid4(),
        "event_time": NOW - timedelta(minutes=2),
        "first_event_time": NOW - timedelta(minutes=5),
        "contract_ticker": contract_ticker,
        "contract_id": 1,
        "underlyer": underlyer,
        "price": Decimal("1.25"),
        "size": 20,
        "multiplier": 100,
        "premium": Decimal("2500"),
        "prior_open_interest": None,
        "event_count": 2,
    }
    corrected = {
        **base,
        "event_id": uuid4(),
        "size": 15,
        "premium": Decimal("1875"),
        "event_count": 3,
    }
    detail = {
        key: value
        for key, value in corrected.items()
        if key not in {"first_event_time", "event_count"}
    }
    admitted = tuple(
        {"instrument_id": member.instrument_id}
        for member in control.slot_set.members
    )
    cycle = SimpleNamespace(
        input_gate=SimpleNamespace(
            closure=SimpleNamespace(
                cycle_input=SimpleNamespace(
                    cohort_id=control.cohort_id,
                )
            )
        ),
        evaluations=(),
    )

    def load(aggregate):
        cursor = MagicMock()
        cursor.fetchall.side_effect = (
            (aggregate,),
            (detail,),
            admitted,
        )
        source = load_advanced_options_page_source_cursor(
            cursor,
            control_plane=control,
            cycle=cycle,
            generated_at=NOW,
        )
        return source, cursor

    original, _ = load(base)
    recomputed, cursor = load(corrected)

    assert original.flow_events[0]["size"] == 20
    assert recomputed.flow_events[0]["size"] == 15
    assert recomputed.flow_events[0]["event_count"] == 3
    aggregate_query = cursor.execute.call_args_list[0].args[0]
    assert "event.event_time>%s" not in aggregate_query
    assert "portal_release_current" not in aggregate_query


def test_projection_batch_is_deterministic_complete_and_nonpublishing():
    control, cycle = _cycle()
    first = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    repeated = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )

    assert first == repeated
    assert tuple(row.snapshot_type for row in first.snapshots) == (
        OPTIONS_SNAPSHOT_TYPES
    )
    pages = {
        row.payload["artifact_kind"]: row
        for row in first.snapshots
        if row.snapshot_type in OPTIONS_PAGE_SNAPSHOT_TYPES.values()
    }
    assert set(pages) == set(OPTIONS_PAGE_SNAPSHOT_TYPES)
    assert {
        kind: row.payload["contract"] for kind, row in pages.items()
    } == OPTIONS_PAGE_CONTRACTS
    assert len({
        row.payload["scope_sha256"] for row in pages.values()
    }) == 1
    assert first.portal_ready_receipt.receipt_kind == OPTIONS_PORTAL_READY
    assert not first.portal_ready_receipt.advance_release
    assert not first.publication_authorized
    alerts = next(
        row for row in first.snapshots
        if row.snapshot_type == "OPTIONS_ALERTS"
    )
    assert alerts.payload["production_authorized"] is False
    assert alerts.payload["delivery_authorized"] is False


def test_alert_page_reports_only_actual_cycle_lanes_and_acceptance():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    snapshot = next(
        row
        for row in batch.snapshots
        if row.snapshot_type == OPTIONS_PAGE_SNAPSHOT_TYPES["ALERTS"]
    )
    publication = AdvancedOptionsAlertsPagePublication(snapshot.payload)
    identity = {
        "cohort_id": control.cohort_id,
        "provider_tier": control.binding.provider_tier,
        "scope_sha256": snapshot.payload["scope_sha256"],
    }

    result = publication.evaluations(
        DetectorEvaluationQuery(),
        **identity,
    )
    models = {
        row["detector_id"]: row
        for row in result["data"]["models"]
    }

    assert set(models) == {"O1", "O2", "O3", "OS1", "OS2"}
    assert models["O1"]["accepted"]
    assert models["O1"]["lane_state"] == "COMPLETE_ZERO"
    assert models["O3"]["accepted"]
    assert models["O3"]["lane_state"] == "COMPLETE_ZERO"
    assert models["OS1"]["accepted"]
    assert models["OS1"]["lane_state"] == "COMPLETE_ZERO"
    assert models["OS2"]["accepted"]
    assert models["OS2"]["lane_state"] == "COMPLETE_ZERO"
    assert not models["O2"]["accepted"]
    assert models["O2"]["lane_state"] == "UNAVAILABLE"
    assert models["O2"]["reason_codes"] == ["O2_MODEL_NOT_ACCEPTED"]
def test_projection_persistence_uses_snapshot_owner_and_no_release_owner():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    cursor = MagicMock()
    stored = []

    persist_advanced_options_projection_cursor(
        cursor,
        batch,
        snapshot_repository=lambda active_cursor, snapshot: stored.append(
            (active_cursor, snapshot)
        ),
    )

    assert [row[1] for row in stored] == list(batch.snapshots)
    assert all(row[0] is cursor for row in stored)
    assert cursor.execute.call_count == 0


def test_publisher_handoff_is_canonical_and_inert():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    handoff = build_advanced_options_publisher_handoff(
        control_plane=control,
        batch=batch,
    )

    assert handoff.owner_name == "portal-publisher"
    assert handoff.snapshots == batch.snapshots
    assert not handoff.dispatch_authorized
    assert not handoff.release_authorized
    assert not handoff.pointer_authorized


def test_owned_disposable_canary_authorizes_only_release_and_pointer():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    inert = build_advanced_options_publisher_handoff(
        control_plane=control,
        batch=batch,
    )
    authorization = AdvancedOptionsCanaryPublicationAuthorization(
        database_name="test_realtime_v2_p8_exact",
        cohort_id=control.cohort_id,
        batch_sha256=batch.batch_sha256,
        receipt_id=UUID(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ),
    )

    canary = authorize_advanced_options_canary_publication(
        inert,
        authorization,
    )

    assert canary.owner_name == "portal-publisher"
    assert canary.release_authorized
    assert canary.pointer_authorized
    assert not canary.dispatch_authorized
    with pytest.raises(ValueError, match="authority is invalid"):
        AdvancedOptionsCanaryPublicationAuthorization(
            database_name="stocks_realtime_v1",
            cohort_id=control.cohort_id,
            batch_sha256=batch.batch_sha256,
            receipt_id=UUID(
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
            ),
        )


def test_v5_publication_authority_is_exact_and_keeps_forbidden_effects_false():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="v5-build",
    )
    inert = build_advanced_options_publisher_handoff(
        control_plane=control,
        batch=batch,
    )
    authority = AdvancedOptionsPublicationAuthorization(
        uuid4(),
        control.cohort_id,
        control.binding.provider_tier,
        batch.batch_sha256,
        uuid4(),
        "OWNED_DISPOSABLE_V5_PUBLICATION",
    )

    authorized = authorize_advanced_options_publication(
        inert,
        authority,
    )

    assert authorized.release_authorized
    assert authorized.pointer_authorized
    assert not authorized.dispatch_authorized
    assert authorized.receipt.receipt_id == authority.receipt_id
    with pytest.raises(ValueError, match="authority scope changed"):
        authorize_advanced_options_publication(
            inert,
            AdvancedOptionsPublicationAuthorization(
                uuid4(),
                control.cohort_id,
                control.binding.provider_tier,
                "f" * 64,
                uuid4(),
                "OWNED_DISPOSABLE_V5_PUBLICATION",
            ),
        )


def test_stored_reader_performs_no_provider_detector_or_publication_work():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    snapshot = batch.snapshots[0]
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        snapshot.snapshot_id,
        snapshot.generated_at,
        snapshot.payload_sha256,
        snapshot.payload,
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        snapshot.generated_at,
        "e" * 64,
    )

    result = read_advanced_options_projection_cursor(
        cursor,
        cohort_id=control.cohort_id,
        snapshot_type=snapshot.snapshot_type,
        available_by=NOW,
    )

    assert result["available"]
    assert result["data"] == snapshot.payload
    assert result["provider_fetch_performed"] is False
    assert result["detector_work_performed"] is False
    assert result["publication_performed"] is False
    assert result["release_id"] == (
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    )
    assert cursor.execute.call_count == 1
    assert "portal_release_current" in cursor.execute.call_args.args[0]


def test_stored_reader_rejects_hash_drift_and_missing_is_explicit():
    control, cycle = _cycle()
    batch = build_advanced_options_projection_batch(
        control_plane=control,
        cycle=cycle,
        generated_at=NOW,
        build_id="p4-build",
    )
    snapshot = batch.snapshots[0]
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        snapshot.snapshot_id,
        snapshot.generated_at,
        "f" * 64,
        snapshot.payload,
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        snapshot.generated_at,
        "e" * 64,
    )
    with pytest.raises(ValueError, match="hash mismatch"):
        read_advanced_options_projection_cursor(
            cursor,
            cohort_id=control.cohort_id,
            snapshot_type=snapshot.snapshot_type,
            available_by=NOW,
        )
    cursor.fetchone.return_value = None
    missing = read_advanced_options_projection_cursor(
        cursor,
        cohort_id=control.cohort_id,
        snapshot_type=snapshot.snapshot_type,
        available_by=NOW,
    )
    assert missing["available"] is False
    assert missing["provider_fetch_performed"] is False


def test_projection_rejects_cycle_clock_drift():
    control, cycle = _cycle()
    with pytest.raises(ValueError, match="cycle scope changed"):
        build_advanced_options_projection_batch(
            control_plane=control,
            cycle=cycle,
            generated_at=NOW.replace(minute=31),
            build_id="p4-build",
        )


def _stored_contract_facts():
    from realtime.advanced_options_projection import _contract_facts

    volumes = [
        {"checkpoint": {
            "contract_ticker": "O:SPY261105C00764000", "cumulative_volume": 74878,
            "baseline_kind": "SNAPSHOT_RECONCILED", "last_event_time": "2026-11-05T16:24:47Z",
        }},
        {"checkpoint": {
            "contract_ticker": "O:SPY261105C00765000", "cumulative_volume": 91960,
            "baseline_kind": "SNAPSHOT_RECONCILED", "last_event_time": "2026-11-05T16:24:44Z",
        }},
    ]
    ticks = [{"evidence": {"ticker": "SPY", "price": "744.82"}}]
    interest = [
        {"contract_ticker": "O:SPY261105C00764000", "availability": "AVAILABLE", "open_interest": 1308},
        {"contract_ticker": "O:SPY261105C00765000", "availability": "UNAVAILABLE", "open_interest": None},
    ]
    admitted = (
        {"contract_ticker": "O:SPY261105C00764000", "contract_id": 1},
        {"contract_ticker": "O:SPY261105C00765000", "contract_id": 2},
    )
    return _contract_facts(volumes, ticks, interest, admitted)


def test_flow_page_shows_each_contracts_true_volume_not_its_filtered_events():
    """G71: a contract with 74,878 traded showed 1,000, and one with 91,960 was absent."""
    from datetime import datetime, timezone

    from realtime.advanced_options_projection import _apply_contract_facts

    cutoff = datetime(2026, 11, 5, 16, 25, tzinfo=timezone.utc)
    filtered = {
        "contract_id": 1, "contract_ticker": "O:SPY261105C00764000", "contract_type": "CALL",
        "expiration_date": "2026-11-05", "calendar_dte": 0, "strike": "764", "spot": "0",
        "day_volume": 1000, "open_interest": 900, "model_mark": "0.04", "display_mark": "0.04",
        "local_iv": None, "local_delta": None, "volume_open_interest_ratio": 1.11,
        "premium_activity": "4000", "moneyness_fraction": "0", "market_data_time": cutoff.isoformat(),
    }
    rows = {"SPY": [filtered], "QQQ": []}

    _apply_contract_facts(rows, _stored_contract_facts(), cutoff)

    by_ticker = {row["contract_ticker"]: row for row in rows["SPY"]}
    traded = by_ticker["O:SPY261105C00764000"]
    assert traded["day_volume"] == 74878
    assert traded["open_interest"] == 1308
    assert traded["volume_open_interest_ratio"] == 74878 / 1308
    assert traded["premium_activity"] == "4000", "premium stays what flow events support"
    assert traded["display_mark"] == "0.04"
    assert traded["spot"] == "744.82"
    assert float(traded["moneyness_fraction"]) == pytest.approx(764 / 744.82 - 1)
    quiet = by_ticker["O:SPY261105C00765000"]
    assert quiet["day_volume"] == 91960
    assert quiet["contract_type"] == "CALL"
    assert quiet["strike"] == "765"
    assert quiet["premium_activity"] is None, "no flow event means unknown, never zero"
    assert quiet["display_mark"] is None
    assert quiet["open_interest"] is None
    assert quiet["volume_open_interest_ratio"] is None
    assert rows["QQQ"] == []


def test_contract_facts_refuse_a_contract_outside_the_window():
    from realtime.advanced_options_projection import _contract_facts

    volumes = [{"checkpoint": {
        "contract_ticker": "O:SPY261105C00999000", "cumulative_volume": 1, "baseline_kind": "SESSION_COMPLETE",
    }}]
    with pytest.raises(ValueError, match="changed cycle scope"):
        _contract_facts(volumes, [], [], ())


def test_screener_rows_carry_stored_facts_not_placeholders():
    """G71: the Screener showed $0.00 and Unavailable for all 16 contracts with quotes stored."""
    from datetime import date as _date, datetime as _datetime, timezone as _timezone
    from uuid import UUID as _UUID

    from realtime.advanced_options_projection import _admitted_chain_row, _contract_facts

    cutoff = _datetime(2026, 11, 5, 16, 25, tzinfo=_timezone.utc)
    contract = {
        "instrument_id": 7, "contract_id": 1, "contract_ticker": "O:SPY261120P00766000", "underlyer": "SPY",
        "call_put": "P", "expiration": _date(2026, 11, 20), "strike": Decimal("766.00000000"),
        "eligibility": "ELIGIBLE", "observed_at": _datetime(2026, 11, 3, 2, 47, tzinfo=_timezone.utc),
    }
    facts = _contract_facts(
        [{"checkpoint": {"contract_ticker": contract["contract_ticker"], "cumulative_volume": 2976, "baseline_kind": "SNAPSHOT_RECONCILED"}}],
        [{"evidence": {"ticker": "SPY", "price": "744.82"}}],
        [{"contract_ticker": contract["contract_ticker"], "availability": "AVAILABLE", "open_interest": 5872}],
        (contract,),
        ((7, Decimal("23.92"), Decimal("24.48"), cutoff),),
    )
    row = _admitted_chain_row(contract, dataset_id=_UUID(int=1), cutoff=cutoff, fact=facts[0])

    assert row["strike"] == "766"
    assert row["spot"] == "744.82"
    assert row["mark"] == row["model_mark"] == row["display_mark"] == "24.20"
    assert (row["bid"], row["ask"]) == ("23.92", "24.48")
    assert row["mark_source"] == "QUOTE_MIDPOINT"
    assert row["day_volume"] == 2976
    assert row["open_interest"] == 5872
    assert row["volume_open_interest_ratio"] == 2976 / 5872
    assert float(row["otm_fraction"]) == pytest.approx((744.82 - 766) / 744.82)
    assert row["market_data_time"] == cutoff.isoformat()

    no_quote = _contract_facts(
        [{"checkpoint": {"contract_ticker": contract["contract_ticker"], "cumulative_volume": 1, "baseline_kind": "SESSION_COMPLETE"}}],
        [], [], (contract,),
    )
    unknown = _admitted_chain_row(contract, dataset_id=_UUID(int=1), cutoff=cutoff, fact=no_quote[0])
    assert unknown["mark"] is None, "an unknown price is never shown as zero"
    assert unknown["mark_source"] == "UNAVAILABLE"

    old_session = _admitted_chain_row(contract, dataset_id=_UUID(int=1), cutoff=cutoff)
    assert old_session["mark_source"] == "ADMITTED_INVENTORY_UNEVALUATED", "no facts leaves the row as it was"


def test_a_contract_reads_the_same_mark_and_volume_status_on_every_page():
    """G71: SPY 766 PUT read $22.29 on Flow, an old print, and $25.32 on the Screener."""
    from datetime import datetime, timezone

    from realtime.advanced_options_projection import _apply_contract_facts

    cutoff = datetime(2026, 11, 5, 16, 45, tzinfo=timezone.utc)
    row = {
        "contract_id": 1, "contract_ticker": "O:SPY261120P00766000", "strike": "766", "open_interest": None,
        "day_volume": 2879, "model_mark": "22.29", "display_mark": "22.29", "premium_activity": "7900000",
        "spot": "0", "moneyness_fraction": "0", "volume_open_interest_ratio": None,
    }
    fact = {
        "contract_ticker": "O:SPY261120P00766000", "contract_id": 1, "underlyer": "SPY", "day_volume": 2997,
        "baseline_kind": "INCOMPLETE", "open_interest": 5872, "spot": "743.93", "mark": "25.32",
    }
    rows = {"SPY": [row]}

    _apply_contract_facts(rows, (fact,), cutoff)

    assert row["display_mark"] == row["model_mark"] == "25.32"
    assert row["premium_activity"] == "7900000"
    assert row["volume_baseline"] == "INCOMPLETE"


def _greeks_case(call_put="P", strike=766, expiry="261120", bid="23.92", ask="24.48", **extra):
    from datetime import date, datetime, timezone

    from realtime.advanced_options_projection import _contract_facts

    cutoff = datetime(2026, 11, 5, 16, 45, tzinfo=timezone.utc)
    ticker = f"O:SPY{expiry}{call_put}{strike * 1000:08d}"
    expiration = date(2000 + int(expiry[:2]), int(expiry[2:4]), int(expiry[4:]))
    contract = {"instrument_id": 7, "contract_id": 1, "contract_ticker": ticker}
    return cutoff, _contract_facts(
        [{"checkpoint": {"contract_ticker": ticker, "cumulative_volume": 10, "baseline_kind": "SESSION_COMPLETE"}}],
        [{"evidence": {"ticker": "SPY", "price": "700"}}],
        [],
        (contract,),
        ((7, Decimal(bid), Decimal(ask), cutoff),),
        spot_closes=(("SPY", Decimal("743.93000000")),),
        valuation_inputs=extra.get("valuation", [{
            "ticker": "SPY", "expiration_date": expiration.isoformat(), "risk_free_rate": "0.0401",
            "dividend_yield": "0.0", "model_version": "black-scholes-merton-v1",
        }]),
        cutoff=cutoff,
    )


def test_greeks_come_from_the_stored_quote_and_reproduce_it():
    """A price from the solved volatility must give back the quote it was solved from."""
    from options.analytics.greeks import price_and_greeks
    from options.domain import ContractType
    from realtime.advanced_options_projection import AdvancedOptionsPageSource

    _cutoff, facts = _greeks_case()
    fact = facts[0]

    assert fact["spot"] == "743.93", "the stock's close at the cutoff, not the older tick"
    assert fact["greeks_reason"] is None
    assert 0 < fact["local_iv"] < 5
    assert -1 < fact["local_delta"] < 0, "a put's delta is negative"
    price, delta, gamma, theta, vega, rho = price_and_greeks(
        ContractType.PUT, spot=743.93, strike=766.0, maturity=fact["time_to_expiration_years"], rate=0.0401, dividend=0.0, volatility=fact["local_iv"],
    )
    assert float(price) == pytest.approx(24.20, abs=0.001)
    assert fact["local_delta"] == pytest.approx(float(delta))
    assert fact["local_gamma"] == pytest.approx(float(gamma))
    assert fact["local_theta_per_day"] == pytest.approx(float(theta))
    assert fact["local_vega_per_vol_point"] == pytest.approx(float(vega))
    assert fact["local_rho_per_rate_point"] == pytest.approx(float(rho))
    assert fact["time_to_expiration_years"] == pytest.approx(15 / 365)
    assert isinstance(AdvancedOptionsPageSource(contract_facts=facts).sha256, str)


def test_a_contract_expiring_today_counts_the_time_to_the_close():
    _cutoff, facts = _greeks_case(call_put="C", strike=744, expiry="261105", bid="4.10", ask="4.30")
    # 11:45 AM ET to 4:00 PM ET is 4.25 hours.
    assert facts[0]["time_to_expiration_years"] == pytest.approx(4.25 / (365 * 24))
    assert facts[0]["local_delta"] > 0


def test_greeks_say_why_when_they_cannot_be_solved():
    _cutoff, facts = _greeks_case(valuation=[])
    assert facts[0]["greeks_reason"] == "VALUATION_INPUT_UNAVAILABLE"
    assert facts[0].get("local_iv") is None
    # Below intrinsic value no volatility fits, and the solver must not invent one.
    _cutoff, facts = _greeks_case(bid="1.00", ask="1.10")
    assert facts[0]["local_iv"] is None
    assert facts[0]["greeks_reason"] == "BELOW_NO_ARBITRAGE_BOUND"


def test_the_screener_detail_panel_gets_every_derivable_field():
    from datetime import date, datetime, timezone
    from uuid import UUID as _UUID

    from realtime.advanced_options_projection import _admitted_chain_row, _contract_facts

    cutoff = datetime(2026, 11, 5, 17, 10, tzinfo=timezone.utc)
    bar_end = datetime(2026, 11, 5, 17, 10, tzinfo=timezone.utc)
    contract = {
        "instrument_id": 7, "contract_id": 1, "contract_ticker": "O:SPY261120P00766000", "underlyer": "SPY",
        "call_put": "P", "expiration": date(2026, 11, 20), "strike": Decimal("766.00000000"),
        "eligibility": "VALIDATED_ACTIVE", "observed_at": cutoff,
    }
    fact = _contract_facts(
        [{"checkpoint": {"contract_ticker": contract["contract_ticker"], "cumulative_volume": 5, "baseline_kind": "SESSION_COMPLETE"}}],
        [], [], (contract,),
        ((7, Decimal("30.00"), Decimal("31.00"), cutoff),),
        spot_closes=(("SPY", Decimal("740.00"), bar_end),),
        valuation_inputs=[{"ticker": "SPY", "expiration_date": "2026-11-20", "risk_free_rate": "0.04", "dividend_yield": "0.0", "model_version": "m"}],
        cutoff=cutoff,
        contract_terms=((1, "AMERICAN", Decimal("100.00000000")),),
    )[0]
    row = _admitted_chain_row(contract, dataset_id=_UUID(int=1), cutoff=cutoff, fact=fact)

    assert row["exercise_style"] == "AMERICAN"
    assert row["shares_per_contract"] == 100
    assert row["midpoint"] == "30.50"
    assert Decimal(row["intrinsic_value"]) == 26, "a put's intrinsic value is strike less spot"
    assert Decimal(row["extrinsic_value"]) == Decimal("4.50")
    assert Decimal(row["single_contract_breakeven"]) == Decimal("735.50")
    assert row["spot_market_data_time"] == bar_end.isoformat()
    assert row["iv_converged"] is True
    assert row["iv_iteration_count"] >= 1
    assert row["iv_failure_reason"] is None if "iv_failure_reason" in row else True


class _SnapshotCursor:
    """Answers only what _provider_greeks_cursor and load_raw_payload_cursor ask."""

    def __init__(self, payloads):
        self.payloads = payloads
        self.rows = []
        self.parameters = []

    def execute(self, query, parameters=None):
        import hashlib

        self.parameters.append((query, parameters))
        if "provider_request_ledger" in query:
            self.rows = [(payload_id,) for payload_id in self.payloads]
        elif "rt.raw_payloads" in query:
            self.current = self.payloads[parameters[0]]
            self.rows = [(
                "arrived",
                hashlib.sha256(self.current).hexdigest(),
                len(self.current),
                1,
            )]
        elif "rt.raw_payload_chunks" in query:
            self.rows = [(0, self.current)]
        else:
            raise AssertionError(query)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


def _snapshot(*rows):
    import json as _json

    return _json.dumps({"results": list(rows)}).encode("utf-8")


def test_provider_iv_and_gamma_come_from_the_stored_snapshot():
    from datetime import date, datetime, timezone

    from realtime.advanced_options_projection import _provider_greeks_cursor

    quoted = "O:SPY261120P00766000"
    bare = "O:SPY261110P00764000"
    cursor = _SnapshotCursor({
        "11111111-1111-1111-1111-111111111111": _snapshot(
            {"details": {"ticker": quoted}, "implied_volatility": 0.21, "greeks": {"gamma": 0.013}},
            {"details": {"ticker": bare}, "greeks": {}},
            {"details": {"ticker": "O:SPY261120C00999000"}, "implied_volatility": 9.0},
        ),
    })

    found = _provider_greeks_cursor(
        cursor,
        session_date=date(2026, 11, 5),
        cutoff=datetime(2026, 11, 5, 17, 10, tzinfo=timezone.utc),
        tickers=(quoted, bare),
    )

    assert found == {quoted: {"provider_iv": 0.21, "provider_gamma": 0.013}, bare: {}}
    query, parameters = cursor.parameters[0]
    assert "requested_to<=%s" in query, "a snapshot after the cutoff is not used"
    assert parameters[1] == datetime(2026, 11, 5, 17, 10, tzinfo=timezone.utc)


def test_the_newest_readable_snapshot_wins():
    from datetime import date, datetime, timezone

    from realtime.advanced_options_projection import _provider_greeks_cursor

    ticker = "O:SPY261120P00766000"
    cursor = _SnapshotCursor({
        "11111111-1111-1111-1111-111111111111": b"not json",
        "22222222-2222-2222-2222-222222222222": _snapshot(
            {"details": {"ticker": ticker}, "implied_volatility": 0.30},
        ),
        "33333333-3333-3333-3333-333333333333": _snapshot(
            {"details": {"ticker": ticker}, "implied_volatility": 0.99},
        ),
    })

    found = _provider_greeks_cursor(
        cursor,
        session_date=date(2026, 11, 5),
        cutoff=datetime(2026, 11, 5, 17, 10, tzinfo=timezone.utc),
        tickers=(ticker,),
    )

    assert found == {ticker: {"provider_iv": 0.30}}


def test_provider_values_reach_the_contract_fact_and_never_replace_ours():
    from datetime import date, datetime, timezone

    from realtime.advanced_options_projection import _contract_facts

    cutoff = datetime(2026, 11, 5, 17, 10, tzinfo=timezone.utc)
    ticker = "O:SPY261120P00766000"
    contract = {
        "instrument_id": 7, "contract_id": 1, "contract_ticker": ticker, "underlyer": "SPY",
        "call_put": "P", "expiration": date(2026, 11, 20), "strike": Decimal("766.00000000"),
        "eligibility": "VALIDATED_ACTIVE", "observed_at": cutoff,
    }
    fact = _contract_facts(
        [{"checkpoint": {"contract_ticker": ticker, "cumulative_volume": 5, "baseline_kind": "SESSION_COMPLETE"}}],
        [], [], (contract,),
        ((7, Decimal("30.00"), Decimal("31.00"), cutoff),),
        spot_closes=(("SPY", Decimal("740.00"), cutoff),),
        valuation_inputs=[{"ticker": "SPY", "expiration_date": "2026-11-20", "risk_free_rate": "0.04", "dividend_yield": "0.01", "model_version": "m"}],
        cutoff=cutoff,
        provider_greeks={ticker: {"provider_iv": 0.5, "provider_gamma": 0.01}},
    )[0]

    assert fact["provider_iv"] == 0.5
    assert fact["provider_gamma"] == 0.01
    assert fact["local_iv"] != 0.5, "the value we solve stays ours"
