from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from option_flow_support import (
    CALL,
    CANCEL,
    MULTI_LEG,
    OTHER,
    T0,
    advanced_contract_scope,
    advanced_policy,
    check_cluster_rules,
    engine,
    policy,
    quote,
    run,
    trade,
)
from realtime.option_flow.clusters import ClusterConfig, cluster_bucket


def _cluster_engine(**overrides):
    values = dict(policy=policy(sweep_window_seconds=5), cluster_windows_seconds=(5,), shard_count=1)
    values.update(overrides)
    return engine(**values)


def _big(seq, time_ms, exchange, symbol=CALL, price=5.0, size=100, conditions=(9001,)):
    return trade(symbol=symbol, seq=seq, time_ms=time_ms, price=price, size=size, exchange=exchange,
                 conditions=conditions)


def _burst(first_seq=1, start=T0, exchanges=(302, 303, 304), symbol=CALL):
    messages = [quote(symbol=symbol, seq=first_seq, time_ms=start - 100)]
    for index, exchange in enumerate(exchanges):
        messages.append(_big(first_seq + index, start + index * 100, exchange, symbol))
    return messages


def _by_cluster(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.cluster_id].append(row)
    return grouped


def test_qualifying_burst_becomes_provisional_then_final_with_a_superseding_revision():
    eng = _cluster_engine()
    out = run(eng, _burst() + [trade(symbol=OTHER, seq=1, time_ms=T0 + 30_000, price=1.0, size=1)], final=False)
    provisional = [row for row in out.clusters if row.status == "PROVISIONAL"]
    final = [row for row in out.clusters if row.status == "FINAL"]
    assert provisional and final
    groups = _by_cluster(out.clusters)
    for rows in groups.values():
        assert [row.revision for row in rows] == [1, 2]
        assert [row.status for row in rows] == ["PROVISIONAL", "FINAL"]
        assert rows[0].cluster_id == rows[1].cluster_id
    side = [row for row in final if row.side_class == "AT_OR_ABOVE_ASK"]
    assert side, "a side-aware cluster must exist"
    row = side[0]
    assert (row.print_count, row.total_size, row.distinct_exchanges) == (3, 300, 3)
    assert row.total_premium == 3 * 50_000 and row.contract_ticker == CALL and row.underlyer == "SPY"
    assert row.window_seconds == 5 and row.provider_tier == "MASSIVE_ADVANCED"
    assert row.policy_sha256 == eng.policy_sha256
    for item in out.clusters:
        check_cluster_rules(item)


def test_a_cluster_is_not_final_before_the_watermark_passes_its_window():
    eng = _cluster_engine()
    out = run(eng, _burst() + [trade(symbol=OTHER, seq=1, time_ms=T0 + 3_000, price=1.0, size=1)], final=False)
    assert out.clusters and all(row.status == "PROVISIONAL" for row in out.clusters)


def test_final_flush_finalizes_open_clusters_at_end_of_session():
    eng = _cluster_engine()
    out = run(eng, _burst())
    assert {row.status for row in out.clusters} == {"PROVISIONAL", "FINAL"}


def test_thresholds_come_from_the_policy_prints_venues_and_notional():
    few = run(_cluster_engine(), _burst(exchanges=(302, 303)))
    one_venue = run(_cluster_engine(), _burst(exchanges=(302, 302, 302)))
    small = run(_cluster_engine(), [quote(seq=1, time_ms=T0 - 100)] + [
        _big(seq, T0 + seq, 300 + seq, price=4.0, size=100) for seq in range(1, 6)])
    assert few.clusters == [] and one_venue.clusters == [] and small.clusters == []
    assert len(one_venue.events) == 3
    looser = run(_cluster_engine(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=2)),
                 _burst(exchanges=(302, 303)))
    assert looser.clusters
    assert ClusterConfig.from_policy(policy(sweep_window_seconds=5)).minimum_prints == 3


def test_advanced_clusters_are_profile_scoped_with_exact_threshold_evidence():
    flow_policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    messages = [quote(seq=1, time_ms=T0 - 100)]
    messages += [
        trade(
            seq=index + 1,
            time_ms=T0 + index * 80,
            price=2.0,
            size=50,
            exchange=302 + index % 3,
        )
        for index in range(50)
    ]

    out = run(engine(
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
        shard_count=1,
        cluster_windows_seconds=(5,),
    ), messages)

    assert len(out.events) == 50
    assert out.events[-1].cumulative_volume_after == 2500
    advanced_rows = [row for row in out.clusters if row.profile]
    assert {row.profile for row in advanced_rows} == {
        "ACCUMULATION",
        "SWEEP",
    }
    assert {
        (row.profile, row.window_seconds)
        for row in advanced_rows
    } == {
        ("ACCUMULATION", 180),
        ("SWEEP", 5),
    }
    assert all(
        row.flow_universe_tier == "ETF"
        and row.flow_dte_lane == "CORE_SWING"
        and row.retention_threshold.profile == row.profile
        and row.advanced_flow_policy_sha256 == flow_policy.sha256
        and row.contract_scope_sha256 == scope.sha256
        and row.runtime_policy_sha256 == row.policy_sha256
        for row in advanced_rows
    )
    event_ids = {event.event_id for event in out.events}
    assert all(
        len(row.member_event_ids) == row.print_count
        and set(row.member_event_ids) <= event_ids
        for row in advanced_rows
    )
    assert max(row.cumulative_contracts for row in advanced_rows) == 2500
    assert all(
        row.cumulative_contracts == 2500
        for row in advanced_rows
        if row.profile == "ACCUMULATION"
    )
    by_profile = defaultdict(set)
    for row in advanced_rows:
        by_profile[row.profile].add(row.cluster_id)
    assert by_profile["ACCUMULATION"].isdisjoint(by_profile["SWEEP"])


def test_advanced_cluster_cumulative_gate_uses_factual_small_print_volume():
    flow_policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    eng = engine(
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
        shard_count=1,
        cluster_windows_seconds=(5,),
    )
    retained = [
        trade(
            seq=index + 1,
            time_ms=T0 + index * 100,
            price=10.0,
            size=25,
            exchange=302 + index % 3,
        )
        for index in range(20)
    ]
    small = [
        trade(
            seq=100 + index,
            time_ms=T0 + 3_000 + index,
            price=0.01,
            size=10,
            exchange=310,
        )
        for index in range(150)
    ]
    out = run(
        eng,
        [quote(seq=1, time_ms=T0 - 100), *retained, *small],
        final=False,
    )

    assert len(out.events) == 20
    assert out.events[-1].cumulative_volume_after == 500
    accumulation = [
        row for row in out.clusters
        if row.profile == "ACCUMULATION"
    ]
    assert accumulation
    assert all(row.cumulative_contracts == 2000 for row in accumulation)
    assert all(row.qualifies for row in accumulation)

    canceled = eng.feed(
        trade(
            seq=500,
            time_ms=T0 + 4_000,
            price=0.01,
            size=10,
            exchange=310,
            conditions=(CANCEL,),
        ),
        500,
    )
    assert canceled.events == []
    assert [
        row
        for row in canceled.clusters
        if row.profile == "ACCUMULATION"
        and row.cumulative_contracts == 1990
        and row.qualifies is False
    ]


def test_advanced_cluster_cancel_revises_each_profile_without_new_volume():
    flow_policy = advanced_policy()
    scope = advanced_contract_scope((CALL,))
    eng = engine(
        advanced_flow_policy=flow_policy,
        contract_scope=scope,
        shard_count=1,
        cluster_windows_seconds=(5,),
    )
    messages = [quote(seq=1, time_ms=T0 - 100)]
    messages += [
        trade(
            seq=index + 1,
            time_ms=T0 + index * 80,
            price=2.0,
            size=50,
            exchange=302 + index % 3,
        )
        for index in range(50)
    ]
    initial = run(eng, messages, final=False)
    canceled = eng.feed(
        trade(
            seq=100,
            time_ms=T0 + 4_500,
            price=2.0,
            size=50,
            exchange=302,
            conditions=(CANCEL,),
        ),
        100,
    )

    assert initial.clusters
    assert canceled.events[0].trade_treatment == "CANCELED"
    assert canceled.events[0].matched_profiles == (
        "ACCUMULATION",
        "SWEEP",
    )
    assert {row.profile for row in canceled.clusters} == {
        "ACCUMULATION",
        "SWEEP",
    }
    assert {
        row.profile
        for row in canceled.clusters
        if row.cumulative_contracts == 2450
    } == {"ACCUMULATION", "SWEEP"}


def test_prints_in_different_windows_do_not_cluster():
    messages = [quote(seq=1, time_ms=T0 - 100), _big(1, T0, 302), _big(2, T0 + 20_000, 303),
                _big(3, T0 + 40_000, 304)]
    assert run(_cluster_engine(), messages).clusters == []


def test_cluster_ids_are_deterministic_and_distinct_per_side_contract_and_window():
    first = run(_cluster_engine(), _burst()).clusters
    second = run(_cluster_engine(), _burst()).clusters
    assert first == second
    ids = {(row.contract_ticker, row.side_class, row.window_start): row.cluster_id for row in first}
    assert len(set(ids.values())) == len(ids)


def test_a_burst_straddling_a_window_boundary_is_still_caught_by_the_half_window_hop():
    burst_start = T0 + 2_400
    out = run(_cluster_engine(), _burst(start=burst_start, exchanges=(302, 303, 304, 305)))
    assert any(row.print_count == 4 for row in out.clusters)


def test_late_print_within_the_allowed_lateness_joins_before_finalization():
    messages = _burst() + [_big(4, T0 + 150, 305), trade(symbol=OTHER, seq=1, time_ms=T0 + 3_000, price=1.0, size=1)]
    messages.insert(4, trade(symbol=OTHER, seq=9, time_ms=T0 + 400, price=1.0, size=1))
    out = run(_cluster_engine(), messages, final=False)
    counts = {row.print_count for row in out.clusters}
    assert 3 in counts and all(row.status == "PROVISIONAL" for row in out.clusters)


def test_late_print_after_finalization_creates_a_superseding_final_revision():
    eng = _cluster_engine()
    out = run(eng, _burst() + [trade(symbol=OTHER, seq=1, time_ms=T0 + 30_000, price=1.0, size=1)], final=False)
    before = {(row.cluster_id, row.revision) for row in out.clusters}
    late = eng.feed(_big(9, T0 + 250, 306, price=5.0, size=100), 50)
    assert late.clusters, "a late print must revise finalized clusters"
    for row in late.clusters:
        check_cluster_rules(row)
        assert row.status == "FINAL" and (row.cluster_id, row.revision) not in before
    revised = next(row for row in late.clusters if row.side_class == "AT_OR_ABOVE_ASK")
    assert revised.print_count == 4 and revised.revision == 3
    assert eng._shards[eng.shard_of(CALL)].clusters.late_revisions > 0


def test_late_print_can_make_a_finalized_window_qualify_for_the_first_time():
    eng = _cluster_engine()
    messages = [quote(seq=1, time_ms=T0 - 100), _big(1, T0, 302), _big(2, T0 + 100, 303),
                trade(symbol=OTHER, seq=1, time_ms=T0 + 30_000, price=1.0, size=1)]
    out = run(eng, messages, final=False)
    assert out.clusters == []
    late = eng.feed(_big(3, T0 + 200, 304), 9)
    assert late.clusters and {row.revision for row in late.clusters} == {1}
    assert {row.status for row in late.clusters} == {"FINAL"}


def test_print_older_than_the_retention_is_counted_and_left_out_of_clusters_only():
    eng = _cluster_engine(finalized_retention_ms=1_000)
    run(eng, _burst() + [trade(symbol=OTHER, seq=1, time_ms=T0 + 60_000, price=1.0, size=1)], final=False)
    out = eng.feed(_big(9, T0 + 250, 306), 50)
    assert out.clusters == []
    assert eng._shards[eng.shard_of(CALL)].clusters.late_beyond_retention > 0
    assert len(out.events) == 1


def test_cancel_revises_an_open_cluster_and_a_cancel_below_threshold_is_visible():
    eng = _cluster_engine()
    messages = _burst() + [_big(7, T0 + 400, 302, conditions=(CANCEL,))]
    out = run(eng, messages, final=False)
    ask = [row for row in out.clusters if row.side_class == "AT_OR_ABOVE_ASK"]
    assert [row.revision for row in ask[:1]] == [1]
    assert ask[-1].print_count == 2 and ask[-1].revision == ask[0].revision + 1
    assert ask[-1].status == "PROVISIONAL"
    for row in out.clusters:
        check_cluster_rules(row)
    assert out.events[-1].supersedes_event_id == out.events[0].event_id


def test_cancel_after_finalization_creates_a_final_revision():
    eng = _cluster_engine()
    run(eng, _burst() + [trade(symbol=OTHER, seq=1, time_ms=T0 + 30_000, price=1.0, size=1)], final=False)
    out = eng.feed(_big(7, T0 + 500, 304, conditions=(CANCEL,)), 60)
    assert out.clusters and all(row.status == "FINAL" for row in out.clusters)
    assert any(row.print_count == 2 for row in out.clusters)


def test_a_cluster_that_loses_every_print_is_retracted_with_zero_counts():
    eng = _cluster_engine(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=1, minimum_distinct_exchanges=1))
    out = run(eng, [quote(seq=1, time_ms=T0 - 100), _big(1, T0, 302), _big(2, T0 + 5, 302, conditions=(CANCEL,))])
    retracted = [row for row in out.clusters if row.status == "RETRACTED"]
    assert retracted and {row.revision for row in retracted} == {2}
    for row in retracted:
        assert (row.print_count, row.total_size, row.total_premium, row.distinct_exchanges) == (0, 0, 0, 0)
    for row in out.clusters:
        check_cluster_rules(row)
    for row in retracted:
        history = [item for item in out.clusters if item.cluster_id == row.cluster_id]
        assert [item.status for item in history] == ["PROVISIONAL", "RETRACTED"]
        assert [item.revision for item in history] == [1, 2], "no FINAL row follows a retraction"
    assert eng.stats.cancel_revisions == 1


def test_retraction_rows_are_deterministic_on_replay():
    def stream():
        return [quote(seq=1, time_ms=T0 - 100), _big(1, T0, 302), _big(2, T0 + 5, 302, conditions=(CANCEL,))]

    options = dict(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=1, minimum_distinct_exchanges=1))
    first, second = run(_cluster_engine(**options), stream()), run(_cluster_engine(**options), stream())
    assert first.clusters == second.clusters and any(row.status == "RETRACTED" for row in first.clusters)


def test_a_retracted_cluster_that_gets_a_new_print_is_revised_again():
    eng = _cluster_engine(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=1, minimum_distinct_exchanges=1))
    out = run(eng, [quote(seq=1, time_ms=T0 - 100), _big(1, T0, 302), _big(2, T0 + 5, 302, conditions=(CANCEL,)),
                    _big(3, T0 + 10, 303)])
    one = next(row for row in out.clusters if row.status == "RETRACTED")
    history = [row for row in out.clusters if row.cluster_id == one.cluster_id]
    assert [(row.revision, row.status, row.print_count) for row in history] == [
        (1, "PROVISIONAL", 1), (2, "RETRACTED", 0), (3, "PROVISIONAL", 1), (4, "FINAL", 1)]
    for row in history:
        check_cluster_rules(row)


def test_retraction_after_finalization_follows_the_final_revision():
    eng = _cluster_engine(policy=policy(sweep_window_seconds=5, minimum_sweep_prints=1, minimum_distinct_exchanges=1))
    run(eng, [quote(seq=1, time_ms=T0 - 100), _big(1, T0, 302),
              trade(symbol=OTHER, seq=1, time_ms=T0 + 30_000, price=1.0, size=1)], final=False)
    out = eng.feed(_big(2, T0 + 5, 302, conditions=(CANCEL,)), 90)
    assert {row.status for row in out.clusters} == {"RETRACTED"} and {row.revision for row in out.clusters} == {3}
    for row in out.clusters:
        check_cluster_rules(row)


def test_multi_leg_prints_never_enter_a_cluster():
    eng = _cluster_engine()
    out = run(eng, [quote(seq=1, time_ms=T0 - 100)] + [
        _big(seq, T0 + seq * 50, 300 + seq, conditions=(MULTI_LEG,)) for seq in range(1, 6)])
    assert len(out.events) == 5 and out.clusters == []
    assert cluster_bucket("EXCLUDED_COMPLEX") is None


def test_mixed_side_aggregate_covers_what_no_single_side_reaches():
    eng = _cluster_engine()
    messages = [quote(seq=1, time_ms=T0 - 100, bid=4.0, ask=6.0)]
    for index in range(4):
        price = 6.0 if index % 2 == 0 else 5.0
        messages.append(_big(index + 1, T0 + index * 100, 302 + index, price=price, size=100 if index % 2 == 0 else 120))
    out = run(eng, messages)
    sides = {row.side_class for row in out.clusters}
    assert sides == {"MIXED"}
    assert out.clusters[-1].print_count == 4 and out.clusters[-1].status == "FINAL"
    assert run(_cluster_engine(emit_mixed_clusters=False), messages).clusters == []


def test_a_heartbeat_finalizes_the_clusters_of_a_shard_that_has_no_new_trades():
    eng = engine(policy=policy(sweep_window_seconds=5), cluster_windows_seconds=(5,), shard_count=4)
    quiet = run(eng, _burst(), final=False)
    assert quiet.clusters and all(row.status == "PROVISIONAL" for row in quiet.clusters)
    eng.submit_time(T0 + 30_000)
    heartbeat = eng.process()
    assert heartbeat.clusters and {row.status for row in heartbeat.clusters} == {"FINAL"}


def test_clusters_are_per_contract_and_side():
    messages = _burst(symbol=CALL) + _burst(first_seq=1, start=T0 + 50, symbol=OTHER)
    out = run(_cluster_engine(), messages)
    assert {row.contract_ticker for row in out.clusters} == {CALL, OTHER}
    assert {row.underlyer for row in out.clusters} == {"SPY", "QQQ"}


def test_unlisted_contracts_cluster_under_their_own_side():
    developer = _cluster_engine(provider_tier="MASSIVE_DEVELOPER")
    out = run(developer, _burst())
    assert {row.side_class for row in out.clusters} == {"AGGRESSION_UNAVAILABLE_NO_QUOTE"}
    assert {row.provider_tier for row in out.clusters} == {"MASSIVE_DEVELOPER"} and out.clusters
    for row in out.clusters:
        check_cluster_rules(row)
    assert cluster_bucket("AGGRESSION_UNAVAILABLE_NO_QUOTE") == "AGGRESSION_UNAVAILABLE_NO_QUOTE"
    assert cluster_bucket("NO_QUOTE") == "NO_QUOTE"
    assert cluster_bucket("QUOTE_STALE") == cluster_bucket("QUOTE_CROSSED_OR_LOCKED") == "NOT_CLASSIFIED"


def test_all_default_windows_produce_consistent_rows():
    eng = engine(policy=policy(minimum_sweep_prints=2))
    out = run(eng, _burst(exchanges=(302, 303)))
    assert {row.window_seconds for row in out.clusters} == {1, 5, 60, 180, 900}
    for row in out.clusters:
        check_cluster_rules(row)
        assert row.window_end - row.window_start == timedelta(seconds=row.window_seconds)


def test_cluster_replay_is_deterministic_including_revisions():
    def stream():
        messages = _burst() + [_big(7, T0 + 400, 302, conditions=(CANCEL,)), _big(9, T0 + 250, 306)]
        messages.append(trade(symbol=OTHER, seq=1, time_ms=T0 + 30_000, price=1.0, size=1))
        messages.append(_big(10, T0 + 260, 307))
        return messages

    first, second = run(_cluster_engine(), stream()), run(_cluster_engine(), stream())
    assert first.clusters == second.clusters
    assert len(first.clusters) > 4
