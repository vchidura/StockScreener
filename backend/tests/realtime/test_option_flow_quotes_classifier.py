from __future__ import annotations

from decimal import Decimal

import pytest

from option_flow_support import CALL, CROSS, FLOOR, MULTI_LEG, OTHER, REGULAR, T0, quote, semantics
from realtime.option_flow.classifier import (
    AggressionPolicy,
    SideClass,
    SideConfidence,
    classify_aggression,
)
from realtime.option_flow.messages import MessageRejected, parse_quote
from realtime.option_flow.quote_cache import QuoteCache, QuoteCacheConfig, QuoteUpdate

POLICY = AggressionPolicy(fresh_quote_ms=1_000, stale_quote_ms=5_000)


def _cache(**overrides) -> QuoteCache:
    return QuoteCache(QuoteCacheConfig(**overrides) if overrides else None)


def _put(cache: QuoteCache, **fields) -> QuoteUpdate:
    return cache.update(parse_quote(quote(**fields)))


def _classify(price, lookup, conditions=(REGULAR,), listed=True, policy=POLICY):
    return classify_aggression(price, lookup, verdict=semantics().classify(conditions),
                               quote_listed=listed, policy=policy)


def test_lookup_uses_the_last_quote_at_or_before_the_trade_time():
    cache = _cache()
    _put(cache, seq=1, time_ms=T0 - 500, bid=4.0, ask=4.2)
    _put(cache, seq=2, time_ms=T0 - 100, bid=4.9, ask=5.0)
    _put(cache, seq=3, time_ms=T0 + 50, bid=9.0, ask=9.5)
    found = cache.lookup(CALL, T0)
    assert (found.quote.bid, found.quote.ask, found.age_ms, found.same_ms) == (4.9, 5.0, 100, False)
    assert cache.lookup(CALL, T0 - 200).quote.bid == 4.0


def test_quote_after_the_trade_is_never_used_and_empty_cache_is_labelled():
    cache = _cache()
    assert cache.lookup(CALL, T0).reason == "NO_QUOTE_CACHED"
    _put(cache, seq=1, time_ms=T0 + 5)
    later = cache.lookup(CALL, T0)
    assert later.quote is None and later.reason == "ONLY_QUOTES_AFTER_TRADE"


def test_same_millisecond_quote_is_found_but_flagged():
    cache = _cache()
    _put(cache, seq=1, time_ms=T0)
    found = cache.lookup(CALL, T0)
    assert found.quote is not None and found.same_ms and found.age_ms == 0


def test_out_of_order_duplicate_and_too_old_quotes():
    cache = _cache(max_quotes_per_contract=3)
    assert _put(cache, seq=3, time_ms=T0 - 100) is QuoteUpdate.ACCEPTED
    assert _put(cache, seq=1, time_ms=T0 - 300) is QuoteUpdate.OUT_OF_ORDER
    assert _put(cache, seq=3, time_ms=T0 - 100) is QuoteUpdate.DUPLICATE
    assert _put(cache, seq=4, time_ms=T0 - 50) is QuoteUpdate.ACCEPTED
    assert _put(cache, seq=0, time_ms=T0 - 900) is QuoteUpdate.TOO_OLD
    assert (cache.out_of_order, cache.duplicates, cache.too_old) == (1, 1, 1)
    # A trade that arrives after a newer quote still finds the quote current at its own time.
    assert cache.lookup(CALL, T0 - 250).quote.time_ms == T0 - 300


def test_memory_is_bounded_per_contract_and_overall_and_evictions_are_counted():
    cache = _cache(max_quotes_per_contract=2, max_contracts=2)
    for seq in range(5):
        _put(cache, seq=seq, time_ms=T0 + seq)
    assert len(cache._by_contract[CALL]) == 2
    _put(cache, symbol=OTHER, seq=1)
    _put(cache, symbol="O:IWM261218C00200000", seq=1)
    assert len(cache) == 2 and cache.evicted_contracts == 1
    assert not cache.has_quotes(CALL)


def test_a_quote_gap_invalidates_cached_quotes():
    cache = _cache()
    _put(cache, seq=1)
    _put(cache, symbol=OTHER, seq=1)
    cache.invalidate(CALL)
    assert cache.lookup(CALL, T0).reason == "NO_QUOTE_CACHED" and cache.has_quotes(OTHER)
    cache.invalidate()
    assert len(cache) == 0 and cache.invalidations == 2


def test_the_event_gets_a_copy_not_a_reference_to_the_cache():
    cache = _cache()
    _put(cache, seq=1, time_ms=T0 - 10)
    copy = cache.lookup(CALL, T0).quote
    _put(cache, seq=2, time_ms=T0 - 5, bid=1.0, ask=2.0)
    assert (copy.bid, copy.ask, copy.seq) == (4.9, 5.0, 1)


def test_malformed_quote_is_rejected_with_a_reason():
    raw = quote()
    del raw["ap"]
    with pytest.raises(MessageRejected) as raised:
        parse_quote(raw)
    assert raised.value.reason == "MISSING_FIELD_ap"
    raw = quote()
    raw["bp"] = -1.0
    with pytest.raises(MessageRejected):
        parse_quote(raw)


def _lookup(cache: QuoteCache, **fields):
    _put(cache, **fields)
    return cache.lookup(CALL, T0)


@pytest.mark.parametrize("price, expected", [
    (5.00, SideClass.AT_OR_ABOVE_ASK),
    (5.05, SideClass.AT_OR_ABOVE_ASK),
    (4.90, SideClass.AT_OR_BELOW_BID),
    (4.80, SideClass.AT_OR_BELOW_BID),
    (4.95, SideClass.INSIDE_SPREAD),
])
def test_price_is_placed_against_the_quote(price, expected):
    result = _classify(price, _lookup(_cache(), seq=1, time_ms=T0 - 200))
    assert result.side_class is expected
    assert result.confidence is SideConfidence.HIGH
    assert result.quote is not None


def test_inside_spread_reports_the_position():
    result = _classify(4.95, _lookup(_cache(), seq=1, time_ms=T0 - 200))
    assert result.position == Decimal("0.5000")
    assert _classify(5.0, _lookup(_cache(), seq=1, time_ms=T0 - 200)).position is None


def test_one_sided_quote_with_no_bid_still_places_a_print_inside_the_spread():
    lookup = _lookup(_cache(), seq=1, time_ms=T0 - 200, bid=0.0, ask=0.05)
    result = _classify(0.03, lookup)
    assert result.side_class is SideClass.INSIDE_SPREAD and result.position == Decimal("0.6000")


def test_quote_without_an_offer_is_not_a_quote():
    result = _classify(1.0, _lookup(_cache(), seq=1, time_ms=T0 - 200, bid=0.0, ask=0.0))
    assert result.side_class is SideClass.NO_QUOTE and result.reasons == ("QUOTE_HAS_NO_OFFER",)


def test_confidence_drops_with_quote_age_and_a_stale_quote_forces_no_side():
    medium = _classify(5.0, _lookup(_cache(), seq=1, time_ms=T0 - 3_000))
    assert medium.side_class is SideClass.AT_OR_ABOVE_ASK and medium.confidence is SideConfidence.MEDIUM
    stale = _classify(5.0, _lookup(_cache(), seq=1, time_ms=T0 - 9_000))
    assert stale.side_class is SideClass.QUOTE_STALE
    assert stale.confidence is SideConfidence.UNKNOWN and not stale.side_class.has_side
    assert stale.quote is not None


def test_same_millisecond_quote_is_low_confidence_but_still_a_class():
    result = _classify(5.0, _lookup(_cache(), seq=1, time_ms=T0))
    assert result.side_class is SideClass.AT_OR_ABOVE_ASK
    assert result.confidence is SideConfidence.LOW_CONFIDENCE
    assert "QUOTE_IN_SAME_MILLISECOND" in result.reasons


@pytest.mark.parametrize("bid, ask, reason", [(5.1, 5.0, "QUOTE_CROSSED"), (5.0, 5.0, "QUOTE_LOCKED")])
def test_crossed_and_locked_quotes_never_force_a_side(bid, ask, reason):
    result = _classify(5.0, _lookup(_cache(), seq=1, time_ms=T0 - 200, bid=bid, ask=ask))
    assert result.side_class is SideClass.QUOTE_CROSSED_OR_LOCKED
    assert result.confidence is SideConfidence.UNKNOWN and result.reasons == (reason,)


def test_no_quote_and_an_unlisted_contract_are_different_reasons_with_one_database_value():
    empty = _classify(5.0, _cache().lookup(CALL, T0))
    unlisted = _classify(5.0, None, listed=False)
    assert empty.side_class is SideClass.NO_QUOTE
    assert unlisted.side_class is SideClass.AGGRESSION_UNAVAILABLE_NO_QUOTE
    assert unlisted.confidence is SideConfidence.UNKNOWN and unlisted.reasons == ("CONTRACT_NOT_ON_QUOTE_LIST",)
    assert empty.side_class.value != unlisted.side_class.value


def test_multi_leg_prints_are_excluded_before_any_quote_is_considered():
    good_quote = _lookup(_cache(), seq=1, time_ms=T0 - 200)
    for conditions in ((MULTI_LEG,), (MULTI_LEG, CROSS)):
        result = _classify(5.0, good_quote, conditions=conditions)
        assert result.side_class is SideClass.EXCLUDED_COMPLEX and result.quote is None
    assert _classify(5.0, None, conditions=(MULTI_LEG,), listed=False).side_class is SideClass.EXCLUDED_COMPLEX


def test_negotiated_prints_are_capped_at_low_confidence():
    good_quote = _lookup(_cache(), seq=1, time_ms=T0 - 200)
    for conditions in ((CROSS,), (FLOOR,)):
        result = _classify(5.0, good_quote, conditions=conditions)
        assert result.side_class is SideClass.AT_OR_ABOVE_ASK
        assert result.confidence is SideConfidence.LOW_CONFIDENCE


def test_unclassifiable_condition_is_not_classified():
    assert _classify(5.0, None, conditions=(9999,)).side_class is SideClass.NOT_CLASSIFIED


def test_policy_rejects_inverted_age_limits():
    with pytest.raises(ValueError):
        AggressionPolicy(fresh_quote_ms=10, stale_quote_ms=5)
