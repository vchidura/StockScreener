from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from option_flow_support import (
    CALL,
    CANCEL,
    COHORT,
    CROSS,
    FLOOR,
    JOURNAL_GENERATION,
    MULTI_LEG,
    NON_VOLUME,
    OTHER,
    PUT,
    REGULAR,
    SWEEP,
    T0,
    UNKNOWN_CODE,
    UNREVIEWED,
    check_event_rules,
    engine,
    quote,
    run,
    trade,
)
from realtime.option_flow.engine import GapMarker, SubmitResult
from realtime.option_flow.labels import PriorOpenInterest
from realtime.option_flow.volume import BaselineKind

SESSION = date(2026, 10, 12)
PRIOR = date(2026, 10, 9)
HEURISTIC_MARKER = "HEURISTIC_UNCALIBRATED_V1"


def _complete(**overrides):
    from realtime.option_flow.volume import VolumeConfig

    return dict(volume=VolumeConfig(initial_baseline=BaselineKind.SESSION_COMPLETE), **overrides)


def test_small_prints_count_toward_volume_but_are_not_persisted():
    eng = engine(**_complete())
    messages = [quote(seq=1, time_ms=T0 - 100)]
    messages += [trade(seq=seq, time_ms=T0 + seq, price=1.0, size=10) for seq in range(1, 6)]
    messages.append(trade(seq=6, time_ms=T0 + 6, price=5.0, size=100))
    out = run(eng, messages)
    assert [event.source_seq for event in out.events] == [6]
    event = out.events[0]
    assert (event.cumulative_volume_before, event.cumulative_volume_after) == (50, 150)
    assert eng.stats.below_filter == 5 and eng.stats.persisted_events == 1 and eng.stats.counted_prints == 6
    check_event_rules(event)


def test_persisted_event_copies_the_quote_by_value_and_places_the_print():
    eng = engine()
    out = run(eng, [quote(seq=1, time_ms=T0 - 100, bid=4.9, ask=5.0), quote(seq=2, time_ms=T0 + 50, bid=1, ask=2),
                    trade(seq=7, time_ms=T0, price=5.0, size=100)])
    (event,) = out.events
    assert event.side_class == "AT_OR_ABOVE_ASK" and event.side_confidence == "HIGH"
    assert (event.quote_bid, event.quote_ask, event.quote_seq) == (Decimal("4.9"), Decimal("5.0"), 1)
    assert event.quote_time < event.event_time
    assert event.premium == Decimal("50000.00") and event.price == Decimal("5.0")
    assert event.exchange_id == 302 and event.conditions == (REGULAR,)
    assert event.condition_semantics_version == "massive_options_conditions_v2"
    assert event.journal_generation == JOURNAL_GENERATION and event.cohort_id == COHORT
    assert event.underlyer == "SPY" and event.session_date == SESSION and event.source_offset == 3
    check_event_rules(event)


def test_the_filter_uses_premium_or_an_optional_block_size():
    eng = engine(minimum_size=500)
    out = run(eng, [trade(seq=1, price=0.5, size=600), trade(seq=2, price=0.5, size=400),
                    trade(seq=3, price=4.99, size=100), trade(seq=4, price=5.0, size=100)])
    assert [event.source_seq for event in out.events] == [1, 4]


def test_filter_boundary_is_exact_even_when_floating_point_would_round():
    eng = engine()
    threshold = Decimal("50000")
    exact_cases = []
    for cents in range(1, 3000):
        price = Decimal(cents) / 100
        size = threshold / (price * 100)
        if size == size.to_integral_value():
            exact_cases.append((float(price), int(size)))
    assert len(exact_cases) > 20
    for price, size in exact_cases:
        assert eng._passes_filter(price, size), (price, size)
        assert not eng._passes_filter(price, size - 1), (price, size)


def test_quotes_for_all_contracts_are_cached_before_any_filtering():
    eng = engine()
    out = run(eng, [quote(symbol=OTHER, seq=1, time_ms=T0 - 100), trade(symbol=OTHER, seq=1, price=5.0, size=100)])
    assert out.events[0].side_class == "AT_OR_ABOVE_ASK"


def test_unknown_condition_fails_closed_and_blocks_the_factual_flag():
    prior = {CALL: PriorOpenInterest(100, PRIOR)}
    eng = engine(prior, **_complete())
    out = run(eng, [quote(seq=1, time_ms=T0 - 100),
                    trade(seq=1, time_ms=T0, price=5.0, size=100, conditions=(UNKNOWN_CODE,)),
                    trade(seq=2, time_ms=T0 + 1, price=5.0, size=100)])
    unknown, later = out.events
    assert unknown.trade_treatment == "UNKNOWN" and unknown.side_class == "NOT_CLASSIFIED"
    assert unknown.side_confidence == "NONE" and unknown.cumulative_volume_before == unknown.cumulative_volume_after == 0
    assert f"UNKNOWN_TRADE_CONDITION_{UNKNOWN_CODE}" in unknown.reasons
    assert later.cumulative_volume_after == 100 and later.volume_baseline_kind == "INCOMPLETE"
    assert not later.volume_exceeds_prior_oi and later.opening_inference == "NOT_ASSESSED"
    assert eng.stats.unknown_condition_prints == 1 and eng.stats.unknown_condition_codes == {UNKNOWN_CODE: 1}
    for event in out.events:
        check_event_rules(event)


def test_a_reviewed_provider_code_without_a_review_is_also_unknown():
    eng = engine()
    out = run(eng, [trade(seq=1, conditions=(UNREVIEWED,))])
    assert out.events[0].trade_treatment == "UNKNOWN"


def test_unknown_prints_can_be_left_out_of_persistence():
    eng = engine(persist_unknown_condition_prints=False)
    out = run(eng, [trade(seq=1, conditions=(UNKNOWN_CODE,))])
    assert out.events == [] and eng.stats.unknown_condition_prints == 1


def test_non_volume_print_is_neither_counted_nor_persisted():
    eng = engine(**_complete())
    out = run(eng, [trade(seq=1, conditions=(NON_VOLUME,)), trade(seq=2, price=5.0, size=100)])
    assert [event.cumulative_volume_before for event in out.events] == [0]
    assert eng.stats.non_volume_prints == 1


def test_multi_leg_print_is_excluded_from_side_but_its_volume_counts():
    eng = engine(**_complete())
    out = run(eng, [quote(seq=1, time_ms=T0 - 100), trade(seq=1, conditions=(MULTI_LEG,), price=5.0, size=100)])
    (event,) = out.events
    assert event.side_class == "EXCLUDED_COMPLEX" and event.side_confidence == "NONE" and event.is_multi_leg
    assert event.quote_bid is None and event.cumulative_volume_after == 100
    assert event.spread_position is None
    check_event_rules(event)


def test_negotiated_and_sweep_prints_keep_a_class_with_the_right_confidence():
    eng = engine()
    out = run(eng, [quote(seq=1, time_ms=T0 - 100),
                    trade(seq=1, conditions=(CROSS,)), trade(seq=2, conditions=(FLOOR,)),
                    trade(seq=3, conditions=(SWEEP,))])
    cross, floor, sweep = out.events
    assert (cross.side_confidence, floor.side_confidence, sweep.side_confidence) == ("LOW", "LOW", "HIGH")
    assert cross.is_cross and floor.is_floor and sweep.is_sweep


def test_same_millisecond_and_stale_quotes_never_force_a_side():
    eng = engine()
    out = run(eng, [quote(seq=1, time_ms=T0), trade(seq=1, time_ms=T0, price=5.0, size=100),
                    trade(seq=2, time_ms=T0 + 20_000, price=5.0, size=100)])
    same_ms, stale = out.events
    assert same_ms.side_class == "AT_OR_ABOVE_ASK" and same_ms.side_confidence == "LOW"
    assert stale.side_class == "QUOTE_STALE" and stale.side_confidence == "NONE"
    for event in out.events:
        check_event_rules(event)


@pytest.mark.parametrize("bid, ask, expected", [(5.1, 5.0, "QUOTE_CROSSED_OR_LOCKED"),
                                                (5.0, 5.0, "QUOTE_CROSSED_OR_LOCKED")])
def test_crossed_and_locked_quotes(bid, ask, expected):
    out = run(engine(), [quote(seq=1, time_ms=T0 - 100, bid=bid, ask=ask), trade(seq=1, price=5.0, size=100)])
    assert out.events[0].side_class == expected and out.events[0].side_confidence == "NONE"
    check_event_rules(out.events[0])


def test_contract_without_any_quote_is_no_quote_and_an_unlisted_contract_is_aggression_unavailable():
    advanced = run(engine(), [trade(seq=1)])
    assert advanced.events[0].side_class == "NO_QUOTE" and advanced.events[0].side_confidence == "NONE"
    developer = engine(provider_tier="MASSIVE_DEVELOPER")
    out = run(developer, [quote(seq=1, time_ms=T0 - 100), trade(seq=1)])
    (event,) = out.events
    assert event.side_class == "AGGRESSION_UNAVAILABLE_NO_QUOTE" and event.side_confidence == "NONE"
    assert event.quote_bid is None and event.provider_tier == "MASSIVE_DEVELOPER"
    assert "CONTRACT_NOT_ON_QUOTE_LIST" in event.reasons
    check_event_rules(event)


def test_quote_list_membership_and_promotion_do_not_classify_the_triggering_print():
    eng = engine(quote_universe=frozenset({OTHER}))
    first = run(eng, [quote(seq=1, time_ms=T0 - 100), trade(seq=1)], final=False)
    assert first.events[0].side_class == "AGGRESSION_UNAVAILABLE_NO_QUOTE"
    eng.submit_quote_universe({OTHER, CALL})
    later = run(eng, [quote(seq=2, time_ms=T0 + 400), trade(seq=2, time_ms=T0 + 500)], first_offset=10)
    assert later.events[0].side_class == "AT_OR_ABOVE_ASK"


def test_spread_position_is_persisted_only_for_an_inside_spread_print():
    messages = [quote(seq=1, time_ms=T0 - 100, bid=4.9, ask=5.0),
                trade(seq=1, price=4.95, size=102), trade(seq=2, price=5.0, size=100),
                trade(seq=3, price=4.9, size=103)]
    inside, ask, bid = run(engine(), messages).events
    assert inside.side_class == "INSIDE_SPREAD" and inside.spread_position == Decimal("0.5000")
    assert ask.side_class == "AT_OR_ABOVE_ASK" and ask.spread_position is None
    assert bid.side_class == "AT_OR_BELOW_BID" and bid.spread_position is None
    for event in (inside, ask, bid):
        check_event_rules(event)


def test_opening_score_and_reasons_exist_only_for_an_assessed_inference():
    prior = {CALL: PriorOpenInterest(100, PRIOR)}
    events = run(engine(prior, **_complete()), [quote(seq=1, time_ms=T0 - 100),
                                                trade(seq=1, price=5.0, size=100, time_ms=T0),
                                                trade(seq=2, price=5.0, size=300, time_ms=T0 + 1)]).events
    unassessed, assessed = events
    assert unassessed.opening_inference == "NOT_ASSESSED"
    assert unassessed.opening_score is None and unassessed.opening_reasons == ()
    assert assessed.opening_inference == "LIKELY_OPENING"
    assert Decimal("0.6") <= assessed.opening_score <= 1 and assessed.opening_score == assessed.opening_score.quantize(Decimal("0.0001"))
    assert assessed.opening_reasons[0] == "VOLUME_EXCEEDS_PRIOR_OI" and "PRICE_AT_OR_THROUGH_QUOTE" in assessed.opening_reasons
    assert assessed.opening_reasons[-1] == "HEURISTIC_UNCALIBRATED_V1"
    for event in events:
        check_event_rules(event)


def test_every_assessed_row_is_marked_as_an_uncalibrated_heuristic_and_unassessed_rows_are_not():
    from realtime.option_flow import labels

    prior = {CALL: PriorOpenInterest(190, PRIOR), PUT: PriorOpenInterest(100, PRIOR)}
    out = run(engine(prior, **_complete()), [
        quote(seq=1, time_ms=T0 - 100), trade(seq=1, price=5.0, size=100), trade(seq=2, price=5.0, size=100),
        trade(symbol=PUT, seq=1, price=5.0, size=300)])
    assessed = [event for event in out.events if event.opening_inference != "NOT_ASSESSED"]
    assert {event.opening_inference for event in assessed} == {"INDETERMINATE", "LIKELY_OPENING"}
    assert assessed and all(event.opening_reasons[-1] == labels.HEURISTIC_REASON == "HEURISTIC_UNCALIBRATED_V1"
                            for event in assessed)
    assert all(HEURISTIC_MARKER not in event.opening_reasons for event in out.events
               if event.opening_inference == "NOT_ASSESSED")
    assert "not a probability" in labels.infer_opening.__doc__


def test_cancel_and_unknown_rows_carry_no_score_reasons_or_spread_position():
    prior = {CALL: PriorOpenInterest(10, PRIOR)}
    out = run(engine(prior, **_complete()), [trade(seq=1, price=5.0, size=100),
                                             trade(seq=2, price=5.0, size=100, conditions=(CANCEL,)),
                                             trade(seq=3, conditions=(UNKNOWN_CODE,), price=5.0, size=100)])
    assert {event.trade_treatment for event in out.events} == {"INCLUDED", "CANCELED", "UNKNOWN"}
    for event in out.events:
        if event.trade_treatment != "INCLUDED":
            assert event.opening_inference == "NOT_ASSESSED" and event.opening_score is None
            assert event.opening_reasons == () and event.spread_position is None
        check_event_rules(event)


def test_duplicate_message_is_idempotent():
    eng = engine()
    out = run(eng, [trade(seq=1), trade(seq=1), trade(seq=1)])
    assert len(out.events) == 1 and eng.stats.duplicates == 2


def test_out_of_order_print_is_counted_and_marked_late():
    eng = engine(**_complete())
    out = run(eng, [trade(seq=5, time_ms=T0 + 5), trade(seq=3, time_ms=T0 + 3)])
    late = out.events[1]
    assert late.is_late and late.cumulative_volume_before == 100 and late.cumulative_volume_after == 200
    assert eng.stats.late_prints == 1


def test_late_condition_code_marks_the_event_late():
    out = run(engine(), [trade(seq=1, conditions=(9008,))])
    assert out.events[0].is_late


def test_cancel_adjusts_volume_and_emits_a_superseding_revision():
    eng = engine(**_complete())
    out = run(eng, [trade(seq=1, time_ms=T0, price=5.0, size=100),
                    trade(seq=2, time_ms=T0 + 1, price=1.0, size=10),
                    trade(seq=3, time_ms=T0 + 5, price=5.0, size=100, conditions=(CANCEL,))])
    original, revision = out.events
    assert revision.supersedes_event_id == original.event_id and revision.event_id != original.event_id
    assert revision.revision == 2 and original.revision == 1
    assert (revision.source_seq, revision.event_time) == (original.source_seq, original.event_time)
    assert revision.source_offset == 3 and original.source_offset == 1
    assert revision.trade_treatment == "CANCELED" and revision.side_class == "NOT_CLASSIFIED"
    assert revision.cumulative_volume_before is None and revision.cumulative_volume_after is None
    assert not revision.volume_exceeds_prior_oi
    check_event_rules(revision)
    checkpoint = next(row for row in out.checkpoints if row.contract_ticker == CALL)
    assert checkpoint.cumulative_volume == 10
    assert eng.stats.cancel_revisions == 1


def test_cancel_revision_shares_the_print_identity_whether_or_not_the_provider_echoes_it():
    echoed = run(engine(**_complete()), [trade(seq=7, time_ms=T0 + 7, price=5.0, size=100),
                                         trade(seq=7, time_ms=T0 + 7, price=5.0, size=100, conditions=(CANCEL,))])
    original, revision = echoed.events
    assert (revision.contract_ticker, revision.source_seq, revision.event_time) == (
        original.contract_ticker, original.source_seq, original.event_time)
    assert (original.revision, revision.revision) == (1, 2) and revision.supersedes_event_id == original.event_id
    own = run(engine(**_complete()), [trade(seq=7, time_ms=T0 + 7, price=5.0, size=100),
                                      trade(seq=9, time_ms=T0 + 50, price=5.0, size=100, conditions=(CANCEL,))])
    assert (own.events[1].source_seq, own.events[1].event_time) == (7, own.events[0].event_time)
    assert own.events[1].event_id == revision.event_id


def test_cancel_revision_ids_are_deterministic_and_replays_emit_identical_rows():
    stream = [trade(seq=1, price=5.0, size=100), trade(seq=2, price=5.0, size=100, conditions=(CANCEL,))]
    first, second = run(engine(**_complete()), stream), run(engine(**_complete()), stream)
    assert first.events == second.events and first.events[1].revision == 2


def test_cancel_of_a_print_that_was_not_persisted_adjusts_volume_silently():
    eng = engine(**_complete())
    out = run(eng, [trade(seq=1, price=1.0, size=10), trade(seq=2, price=1.0, size=10, conditions=(CANCEL,))])
    assert out.events == []
    assert next(row for row in out.checkpoints).cumulative_volume == 0


def test_unmatched_cancel_on_a_complete_baseline_makes_it_incomplete():
    eng = engine(**_complete())
    out = run(eng, [trade(seq=1, price=1.0, size=10, conditions=(CANCEL,))])
    assert eng.stats.cancels_unmatched == 1
    assert next(row for row in out.checkpoints).baseline_kind == "INCOMPLETE"


def test_cancel_redelivery_is_idempotent():
    eng = engine(**_complete())
    run(eng, [trade(seq=1, price=5.0, size=100), trade(seq=2, price=5.0, size=100, conditions=(CANCEL,)),
              trade(seq=2, price=5.0, size=100, conditions=(CANCEL,))])
    assert eng.stats.duplicates == 1 and eng.stats.cancel_revisions == 1


def test_volume_exceeds_prior_oi_is_a_fact_that_needs_prior_oi_and_a_complete_baseline():
    prior = {CALL: PriorOpenInterest(100, PRIOR)}
    below = run(engine(prior, **_complete()), [trade(seq=1, price=5.0, size=100)]).events[0]
    assert below.prior_open_interest == 100 and below.open_interest_as_of == PRIOR
    assert not below.volume_exceeds_prior_oi and below.opening_inference == "NOT_ASSESSED"
    above = run(engine(prior, **_complete()), [trade(seq=1, price=5.0, size=100),
                                              trade(seq=2, price=5.0, size=100)]).events[1]
    assert above.volume_exceeds_prior_oi and above.cumulative_volume_after == 200
    no_oi = run(engine(**_complete()), [trade(seq=1, price=5.0, size=1000)]).events[0]
    assert not no_oi.volume_exceeds_prior_oi and no_oi.prior_open_interest is None
    warming = run(engine(prior), [trade(seq=1, price=5.0, size=1000)]).events[0]
    assert warming.volume_baseline_kind == "WARMING" and not warming.volume_exceeds_prior_oi
    assert warming.opening_inference == "NOT_ASSESSED"
    for event in (below, above, no_oi, warming):
        check_event_rules(event)


def test_an_open_interest_that_is_not_prior_to_the_session_is_ignored():
    same_day = {CALL: PriorOpenInterest(10, SESSION)}
    event = run(engine(same_day, **_complete()), [trade(seq=1, price=5.0, size=100)]).events[0]
    assert event.prior_open_interest is None and not event.volume_exceeds_prior_oi
    assert "OPEN_INTEREST_NOT_PRIOR_TO_SESSION" in event.reasons


def test_likely_opening_is_a_separate_scored_inference_on_top_of_the_fact():
    prior = {CALL: PriorOpenInterest(100, PRIOR)}
    strong = run(engine(prior, **_complete()), [quote(seq=1, time_ms=T0 - 100),
                                               trade(seq=1, price=5.0, size=300)]).events[0]
    assert strong.volume_exceeds_prior_oi and strong.opening_inference == "LIKELY_OPENING"
    assert strong.opening_score is not None and strong.opening_score >= 0.6
    assert "VOLUME_EXCEEDS_PRIOR_OI" in strong.opening_reasons
    weak = run(engine({CALL: PriorOpenInterest(190, PRIOR)}, **_complete()),
               [trade(seq=1, price=5.0, size=100), trade(seq=2, price=5.0, size=100)]).events[1]
    assert weak.volume_exceeds_prior_oi and weak.opening_inference == "INDETERMINATE"
    assert weak.opening_score < 0.6
    for event in (strong, weak):
        check_event_rules(event)


def test_gap_marks_accumulators_incomplete_and_withdraws_the_factual_flag():
    prior = {CALL: PriorOpenInterest(100, PRIOR)}
    eng = engine(prior, **_complete())
    eng.feed(trade(seq=1, price=1.0, size=10), 1)
    eng.submit_gap(GapMarker("STREAM_DISCONNECT", T0 + 100, T0 + 900, channel="T"))
    out = eng.feed(trade(seq=9, time_ms=T0 + 1_000, price=5.0, size=500), 2)
    assert out.gaps[0].reason == "STREAM_DISCONNECT" and out.gaps[0].channel == "T" and out.gaps[0].dropped_messages == 0
    event = out.events[0]
    assert event.volume_baseline_kind == "INCOMPLETE" and event.cumulative_volume_after == 510 > 100
    assert not event.volume_exceeds_prior_oi and event.opening_inference == "NOT_ASSESSED"
    check_event_rules(event)


def test_quote_gap_invalidates_cached_quotes():
    eng = engine()
    eng.feed(quote(seq=1, time_ms=T0 - 100), 1)
    eng.submit_gap(GapMarker("QUOTE_FEED_RESET", T0 - 50, T0 - 10, channel="Q", contracts=(CALL,)))
    out = eng.feed(trade(seq=1, price=5.0, size=100), 2)
    assert out.events[0].side_class == "NO_QUOTE" and out.gaps[0].contracts == (CALL,)


def test_warm_up_then_snapshot_reconcile_in_stream_order():
    prior = {CALL: PriorOpenInterest(100, PRIOR)}
    eng = engine(prior)
    for seq in range(1, 4):
        eng.feed(trade(seq=seq, time_ms=T0 + seq, price=1.0, size=10), seq)
    eng.submit_reconcile(CALL, 150, 2, T0 + 10)
    out = eng.feed(trade(seq=4, time_ms=T0 + 20, price=5.0, size=100), 4)
    (result,) = out.reconciles
    assert result.accepted and result.cumulative_volume == 160 and result.baseline_kind == "SNAPSHOT_RECONCILED"
    event = out.events[0]
    assert (event.cumulative_volume_before, event.cumulative_volume_after) == (160, 260)
    assert event.volume_baseline_kind == "SNAPSHOT_RECONCILED" and event.volume_exceeds_prior_oi
    checkpoint = next(row for row in eng.flush().checkpoints if row.contract_ticker == CALL)
    assert (checkpoint.snapshot_volume, checkpoint.snapshot_watermark_seq) == (150, 2)


def test_malformed_messages_are_rejected_with_a_reason_and_accounted():
    eng = engine(**_complete())
    eng.feed(trade(seq=1, price=1.0, size=10), 1)
    broken = trade(seq=2)
    del broken["s"]
    bad_symbol = trade(symbol="spy")
    unsupported = {"ev": "status", "sym": CALL}
    for message in (broken, bad_symbol, unsupported):
        eng.feed(message, 10)
    assert eng.stats.rejected == 3 and eng.stats.rejected_by_reason == {
        "MISSING_FIELD_s": 1, "INVALID_FIELD_sym": 1, "UNSUPPORTED_EVENT_TYPE": 1}
    assert eng.stats.submitted == eng.stats.accounted() == 4
    # An unreadable print on a known contract may have been volume.
    assert next(row for row in eng.flush().checkpoints).baseline_kind == "INCOMPLETE"


def test_backpressure_records_a_gap_never_blocks_and_never_drops_silently():
    eng = engine(shard_count=1, queue_capacity=4, **_complete())
    results = [eng.submit(trade(seq=seq, time_ms=T0 + seq, price=1.0, size=10), seq) for seq in range(1, 11)]
    assert results.count(SubmitResult.ACCEPTED) == 4 and results.count(SubmitResult.GAP_RECORDED) == 6
    assert eng.queued() == 4 and eng.stats.queue_dropped == 6
    out = eng.process()
    (gap,) = out.gaps
    assert gap.reason == "BACKPRESSURE_BUFFER_FULL" and gap.dropped_messages == 6 and gap.contracts == (CALL,)
    assert (gap.first_offset, gap.last_offset) == (5, 10) and gap.channel == "T"
    assert eng.stats.submitted == eng.stats.accounted() == 10
    row = next(item for item in eng.flush().checkpoints if item.contract_ticker == CALL)
    assert row.baseline_kind == "INCOMPLETE" and row.cumulative_volume == 40


def test_gap_barrier_is_applied_between_the_messages_it_separates():
    eng = engine(shard_count=1, queue_capacity=3, **_complete())
    assert eng.submit(trade(seq=1, time_ms=T0 + 1, price=1.0, size=10), 1) is SubmitResult.ACCEPTED
    assert eng.submit(trade(seq=2, time_ms=T0 + 2, price=1.0, size=10), 2) is SubmitResult.ACCEPTED
    assert eng.submit(trade(seq=3, time_ms=T0 + 3, price=1.0, size=10), 3) is SubmitResult.ACCEPTED
    assert eng.submit(trade(seq=4, time_ms=T0 + 4, price=1.0, size=10), 4) is SubmitResult.GAP_RECORDED
    out = eng.process(max_messages_per_shard=2)
    assert out.gaps == []
    assert eng.submit(trade(seq=5, time_ms=T0 + 5, price=5.0, size=100), 5) is SubmitResult.ACCEPTED
    out = eng.process()
    assert len(out.gaps) == 1
    event = out.events[0]
    assert event.source_seq == 5 and event.volume_baseline_kind == "INCOMPLETE"
    assert event.cumulative_volume_after == 130


def test_gap_with_no_later_traffic_is_still_applied_when_the_queue_drains():
    eng = engine(shard_count=1, queue_capacity=2, **_complete())
    eng.submit(trade(seq=1, price=1.0, size=10), 1)
    eng.submit(trade(seq=2, price=1.0, size=10), 2)
    eng.submit(trade(seq=3, price=1.0, size=10), 3)
    out = eng.process()
    assert len(out.gaps) == 1 and out.gaps[0].dropped_messages == 1


def test_unroutable_symbol_is_rejected_at_submit_without_raising():
    eng = engine()
    assert eng.submit({"ev": "T", "sym": None}, 1) is SubmitResult.REJECTED
    assert eng.submit({"ev": "T", "sym": "AAPL"}, 2) is SubmitResult.REJECTED
    assert eng.stats.rejected == 2 and eng.stats.accounted() == eng.stats.submitted


def test_sharding_is_stable_by_underlyer_and_keeps_a_contract_on_one_shard():
    eng = engine(shard_count=8)
    assert eng.shard_of(CALL) == eng.shard_of(PUT)
    assert eng.shard_of(CALL) == engine(shard_count=8).shard_of(CALL)
    assert {engine(shard_count=8).shard_of(symbol) for symbol in (CALL, OTHER)} <= set(range(8))


def _stream():
    messages = [quote(seq=1, time_ms=T0 - 100), quote(symbol=PUT, seq=1, time_ms=T0 - 100, bid=2.0, ask=2.1)]
    for index in range(1, 40):
        messages.append(trade(seq=index, time_ms=T0 + index * 300, price=5.0 if index % 3 else 1.0,
                              size=100 if index % 2 else 10, exchange=300 + index % 4))
        messages.append(trade(symbol=PUT, seq=index, time_ms=T0 + index * 300 + 1, price=2.1, size=400,
                              exchange=300 + index % 3))
    messages.append(trade(seq=20, time_ms=T0 + 20 * 300, price=5.0, size=100, exchange=303))
    messages.append(trade(seq=99, time_ms=T0 + 8_000, price=5.0, size=100, conditions=(CANCEL,), exchange=301))
    return messages


def test_replay_of_the_same_input_yields_identical_rows_and_state():
    prior = {CALL: PriorOpenInterest(300, PRIOR), PUT: PriorOpenInterest(1_000, PRIOR)}
    first_engine, second_engine = engine(prior, **_complete()), engine(prior, **_complete())
    first, second = run(first_engine, _stream()), run(second_engine, _stream())
    assert first.events and first.clusters and first.checkpoints
    assert first.events == second.events
    assert first.clusters == second.clusters
    assert first.checkpoints == second.checkpoints
    assert first_engine.state_digest() == second_engine.state_digest()
    assert first_engine.stats == second_engine.stats
    assert [event.event_id for event in first.events] == [event.event_id for event in second.events]


def test_replay_does_not_depend_on_how_the_input_is_batched():
    prior = {CALL: PriorOpenInterest(300, PRIOR)}
    fed = engine(prior, **_complete())
    batched = engine(prior, **_complete())
    one_by_one = run(fed, _stream())
    for offset, message in enumerate(_stream(), start=1):
        batched.submit(message, offset)
    all_at_once = batched.process()
    all_at_once.extend(batched.flush(final=True))
    assert one_by_one.events == all_at_once.events
    assert one_by_one.clusters == all_at_once.clusters
    assert fed.state_digest() == batched.state_digest()


def test_every_emitted_event_satisfies_the_database_rules():
    prior = {CALL: PriorOpenInterest(300, PRIOR), PUT: PriorOpenInterest(1_000, PRIOR)}
    out = run(engine(prior, **_complete()), _stream())
    assert len(out.events) > 10
    for event in out.events:
        check_event_rules(event)


def test_journal_offsets_are_reported_per_shard():
    eng = engine(shard_count=1)
    out = run(eng, [trade(seq=1), trade(seq=2)], first_offset=41)
    assert out.offsets == {0: 42} and [event.source_offset for event in out.events] == [41, 42]


def test_engine_policy_hash_is_a_new_detector_version_distinct_from_sweep_like_cluster():
    from realtime.option_flow.engine import DETECTOR_VERSION

    eng = engine()
    assert DETECTOR_VERSION == "option_flow_stream_v1" and "SWEEP_LIKE_CLUSTER" not in DETECTOR_VERSION
    assert len(eng.policy_sha256) == 64
    assert engine().policy_sha256 == eng.policy_sha256
    assert engine(minimum_size=10).policy_sha256 != eng.policy_sha256
    from option_flow_support import policy

    assert engine(policy=policy(minimum_sweep_prints=4)).policy_sha256 != eng.policy_sha256
