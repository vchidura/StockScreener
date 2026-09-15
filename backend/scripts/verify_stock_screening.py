"""Screening reader measurements with opt-in isolated publication transaction checks."""
import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import sys
import time

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / '.env')

from equity.screening_api import catalog, load_generation, query, publication_index
from research.screening import FIELDS, PATTERNS, GAP_VERSION, HOURLY_CATALOG, HOURLY_FIELDS, HOURLY_REFRESH_POLICY, supported_hourly_contract, GapPredicate, Query, Predicate, digest, evaluate_gaps, query_generation


def verify_momentum_horizons(generation):
    from database import get_db_cursor
    checks = []
    for row in generation['rows']:
        for field, lookback in (('momentum_12_1', 252), ('momentum_6_1', 126), ('momentum_3_1', 63)):
            value = row['values'].get(field)
            if value is not None:
                bars = generation['lineage'][row['security_id']]['bars']
                checks.append((row['security_id'], lookback, bars[-22], bars[-(lookback + 1)], value))
    identities = sorted({identity for _, _, recent, earlier, _ in checks for identity in (recent, earlier)})
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute('SELECT bar_revision_id,security_id,session_date,close_price,system_observed_at,created_at FROM equity_bar_revisions WHERE bar_revision_id=ANY(%s::uuid[])', (identities,))
        stored = {str(bar['bar_revision_id']): bar for bar in cursor.fetchall()}
    assert set(stored) == set(identities)
    import exchange_calendars
    calendar = exchange_calendars.get_calendar('XNYS')
    cutoff = datetime.fromisoformat(generation['source_cutoff'])
    for security_id, lookback, recent, earlier, actual in checks:
        numerator, denominator = stored[recent], stored[earlier]
        for bar, offset in ((numerator, 21), (denominator, lookback)):
            assert str(bar['security_id']) == security_id
            assert bar['session_date'] == calendar.session_offset(generation['session'], -offset).date()
            assert bar['system_observed_at'] <= cutoff and bar['created_at'] <= cutoff
        expected = float(numerator['close_price']) / float(denominator['close_price']) - 1
        assert abs(actual - expected) < 1e-12
    return dict(status='PASS', values_reconciled=len(checks), retained_closes_checked=len(identities),
        coverage={field: generation['field_coverage'].get(field) for field in ('momentum_12_1', 'momentum_6_1', 'momentum_3_1')})


def verify_hourly_context(generation):
    if not supported_hourly_contract(generation.get('hourly_contract')):
        return dict(status='NOT_PUBLISHED')
    from database import get_db_cursor
    import pandas as pd
    import exchange_calendars
    from equity.screening_hourly import hourly_slots
    source = generation['hourly_source']
    cutoff = datetime.fromisoformat(source['source_cutoff'])
    refreshed = generation.get('hourly_refresh_policy') == HOURLY_REFRESH_POLICY
    if not refreshed:
        assert cutoff <= datetime.fromisoformat(generation['source_cutoff'])
    assert source['snapshot_cutoff'] == generation['source_cutoff']
    watermark = datetime.fromisoformat(source['expected_market_time'])
    slots = hourly_slots(exchange_calendars.get_calendar('XNYS'), watermark)
    sources = set()
    for lineage in generation['hourly_lineage'].values():
        sources.update(identity for identity in lineage['bars'] if identity not in lineage['reconstructed_from_30m'])
        for identities in lineage['reconstructed_from_30m'].values():
            sources.update(identities)
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='15s'")
        if refreshed:
            cursor.execute("""SELECT payload FROM equity_portal_snapshots WHERE snapshot_type='SCREENING_DAILY_V1'
                AND source_manifest->>'generation'=%s LIMIT 1""", (generation['daily_generation'],))
            anchor = cursor.fetchone()['payload']
            assert not anchor.get('hourly_refresh_policy')
            assert datetime.fromisoformat(source['market_time']) >= datetime.fromisoformat(anchor['market_time'])
            changed = {'generation', 'hourly_contract', 'hourly_source', 'hourly_coverage', 'hourly_lineage', 'rows'}
            assert all(generation[key] == value for key, value in anchor.items() if key not in changed)
            assert [{key: value for key, value in row.items() if key != 'hourly'} for row in generation['rows']] == [
                {key: value for key, value in row.items() if key != 'hourly'} for row in anchor['rows']]
        cursor.execute('SELECT * FROM equity_bar_publications WHERE publication_id=%s', (source['source_publication_id'],))
        publication = cursor.fetchone()
        assert publication['market_time'].isoformat() == source['market_time'] and publication['published_at'] == cutoff
        cursor.execute('SELECT * FROM equity_bar_publication_members WHERE publication_id=%s', (source['source_publication_id'],))
        members = {str(member['security_id']): member for member in cursor.fetchall()}
        cursor.execute('SELECT * FROM equity_bar_revisions WHERE bar_revision_id=ANY(%s::uuid[])', (sorted(sources),))
        stored = {str(bar['bar_revision_id']): dict(bar) for bar in cursor.fetchall()}
    assert set(stored) == sources
    assert all(bar['created_at'] <= cutoff and bar['system_observed_at'] <= cutoff and bar['is_final']
               and not bar['adjusted'] and bar['session_scope'] == 'RTH' for bar in stored.values())
    for row in generation['rows']:
        lineage = generation['hourly_lineage'][row['security_id']]
        if lineage['selected_bar_id'] is None:
            assert all(value is None for value in row['hourly']['values'].values())
            continue
        assert str(members[row['security_id']]['selected_bar_revision_id']) == lineage['selected_bar_id']
        latest = stored[lineage['selected_bar_id']]
        assert latest['bar_end'] == watermark and latest['interval'] == '1h'
        closes = {}
        for identity in lineage['bars']:
            underlying = [stored[source_id] for source_id in lineage['reconstructed_from_30m'][identity]] if identity in lineage['reconstructed_from_30m'] else [stored[identity]]
            assert all(str(bar['security_id']) == row['security_id'] for bar in underlying)
            if identity in lineage['reconstructed_from_30m']:
                assert all(bar['interval'] == '30m' for bar in underlying)
                assert all(left['bar_end'] == right['bar_start'] for left, right in zip(underlying, underlying[1:]))
            assert (underlying[0]['bar_start'], underlying[-1]['bar_end']) in slots
            closes[underlying[-1]['bar_end']] = float(underlying[-1]['close_price'])
        for name, spec in HOURLY_FIELDS.items():
            actual = row['hourly']['values'][name]
            if actual is None:
                assert name in row['hourly']['missing']
                continue
            values = pd.Series([closes[end] for _, end in slots[-spec['warmup_sessions']:]])
            average = values.ewm(span=20, adjust=False).mean()
            expected = values.iloc[-1] if name == 'close' else values.iloc[-1] / values.iloc[-2] - 1 if name == 'change' else values.iloc[-1] / average.iloc[-1] - 1 if name == 'vs_ema20' else average.iloc[-1] / average.iloc[-4] - 1
            assert abs(actual - expected) < 1e-12
    expected_coverage = {name: sum(row['hourly']['values'][name] is not None for row in generation['rows']) for name in HOURLY_FIELDS}
    assert generation['hourly_coverage'] == expected_coverage
    scenarios = {}
    for name, predicate in dict(hourly_above_ema20=Predicate(hourly=dict(filters=[dict(field='vs_ema20', min=0.)])),
        daily_up_hourly_rising=Predicate(filters=[dict(field='discovery_trend', values=['UP'])], hourly=dict(filters=[dict(field='vs_ema20', min=0.), dict(field='ema20_change_3', min=0.)])),
        daily_pullback_hourly_positive=Predicate(filters=[dict(field='discovery_state', values=['PULLBACK'])], hourly=dict(filters=[dict(field='change', min=0.)]))).items():
        result = query_generation(generation, Query(predicate=predicate))
        assert result['comparison']['status'] == 'HOURLY_COMPARISON_NOT_ENABLED' and result['comparison']['new_count'] is None
        scenarios[name] = dict(matches=result['matched_count'], unknown=result['unknown_count'], examples=[row['ticker'] for row in result['rows'][:8]])
    return dict(status='PASS', source=source, coverage=expected_coverage, source_revisions_checked=len(sources), scenarios=scenarios,
                unavailable=[dict(ticker=row['ticker'], missing=row['hourly']['missing']) for row in generation['rows'] if row['hourly']['missing']])


def verify_gap_context(generation):
    if generation.get('gap_contract', {}).get('version') != GAP_VERSION:
        return dict(status='NOT_PUBLISHED')
    raw_rows = {row['security_id']: row for row in generation['rows']}
    episodes = [episode for row in raw_rows.values() for episode in row['gaps']['episodes']]
    assert len({episode['episode_id'] for episode in episodes}) == len(episodes)
    assert all(0 <= episode['values']['formation_age'] <= 20 and 0 <= episode['values']['fill_fraction'] <= 1
               and episode['zone_lower'] < episode['zone_upper'] for episode in episodes)
    assert all(len(row['gaps']['episodes']) <= 21 for row in raw_rows.values())
    recent_up = [dict(field='direction', values=['UP']), dict(field='formation_age', max=5.),
                 dict(field='state', values=['OPEN', 'PARTIALLY_FILLED'])]
    scenarios = dict(any_daily_gap=Predicate(gap=GapPredicate()),
        recent_unfilled_up=Predicate(gap=GapPredicate(filters=recent_up)),
        nearby_liquid_up=Predicate(filters=[dict(field='price', min=5.), dict(field='dollar_volume_20', min=20_000_000.)],
            gap=GapPredicate(filters=recent_up + [dict(field='distance_fraction', max=.02)])),
        uptrend_nearby_gap=Predicate(filters=[dict(field='discovery_trend', values=['UP'])],
            gap=GapPredicate(filters=recent_up + [dict(field='distance_fraction', max=.02)])))
    results = {}
    for name, predicate in scenarios.items():
        result = query(Query(predicate=predicate, limit=200))
        rows = result['rows']
        if result['matched_count'] > 200:
            rows += query(Query(predicate=predicate, offset=200, limit=200))['rows']
        assert len(rows) == result['matched_count']
        for row in rows:
            state, matching, _ = evaluate_gaps(raw_rows[row['security_id']]['gaps'], predicate.gap)
            assert state == 'MATCH' and matching
            assert row['gap_summary']['representative']['episode_id'] == matching[0]['episode_id']
            assert row['gap_summary']['matching_count'] == len(matching)
            assert row['explanations'][-1]['episode_ids'] == [episode['episode_id'] for episode in matching]
        results[name] = dict(matches=result['matched_count'], unknown=result['unknown_count'],
            new=result['comparison']['new_count'], examples=[row['ticker'] for row in rows[:8]])
    return dict(status='PASS', coverage=generation['gap_coverage'],
        unavailable=[dict(ticker=row['ticker'], reason=row['gaps']['reason']) for row in raw_rows.values() if row['gaps']['status'] != 'READY'],
        states=dict(Counter(episode['values']['state'] for episode in episodes)), scenarios=results)


def verify_history(start, end):
    import exchange_calendars
    from database import get_db_cursor
    from equity.polygon import sha256_json
    from scripts.prepare_stock_screening import preparation_sessions, preservation_state

    sessions = preparation_sessions(start=start, end=end)
    calendar = exchange_calendars.get_calendar('XNYS')
    before = preservation_state()
    reasons = defaultdict(set)
    gaps = defaultdict(set)
    gap_identities = defaultdict(set)
    publications = publication_index()
    for session in sessions:
        existing = [publication for publication in publications if publication['source_manifest']['session'] == str(session)]
        assert existing, f'Missing compatible screening session: {session}'
        with get_db_cursor() as cursor:
            cursor.execute('SET TRANSACTION READ ONLY')
            cursor.execute("SET LOCAL statement_timeout='10s'")
            cursor.execute('SELECT * FROM equity_portal_snapshots WHERE snapshot_id=%s', (existing[0]['snapshot_id'],))
            stored = cursor.fetchone()
            payload = stored['payload']
            assert sha256_json(payload) == stored['payload_sha256']
            assert sha256_json(stored['source_manifest']) == stored['source_manifest_sha256']
            assert digest({key: value for key, value in payload.items() if key != 'generation'}) == payload['generation']
            assert stored['source_manifest']['generation'] == payload['generation']
            assert payload['session'] == str(session)
            assert payload['previous_expected_session'] == str(calendar.previous_session(str(session)).date())
            assert payload['capture_mode'] == 'RECONSTRUCTED_FROM_RETAINED_PUBLICATION'
            cursor.execute('SELECT * FROM equity_bar_publications WHERE publication_id=%s', (payload['source_publication_id'],))
            source = cursor.fetchone()
            cutoff = datetime.fromisoformat(payload['source_cutoff'])
            assert source['published_at'] == cutoff <= stored['generated_at']
            assert source['market_time'].isoformat() == payload['market_time']
            cursor.execute('SELECT * FROM equity_bar_publication_members WHERE publication_id=%s', (payload['source_publication_id'],))
            members = {str(member['security_id']): member for member in cursor.fetchall()}
            rows = payload['rows']
            assert len(rows) == len(members) == payload['expected_members'] == source['expected_members']
            assert {row['security_id'] for row in rows} == set(members)
            assert set(payload['field_coverage']) <= set(FIELDS)
            assert payload['field_coverage'] == {name: sum(row['values'].get(name) is not None for row in rows) for name in payload['field_coverage']}
            assert payload['pattern_coverage'] == {name: sum(name in row['patterns'] for row in rows) for name in PATTERNS}
            missing_counts = Counter()
            unavailable = []
            session_gaps = {}
            for row in rows:
                member = members[row['security_id']]
                selected_id = member['selected_bar_revision_id']
                assert row['session'] == str(session) and row['ticker'] == member['ticker']
                assert row['source_bar_id'] == (str(selected_id) if selected_id else None)
                if member['status'] != 'SELECTED':
                    assert not row['eligible'] and row['values']['price'] is None and not row['patterns']
                    assert row['missing']['price'] == 'SOURCE_PUBLICATION_MEMBER_UNAVAILABLE'
                    unavailable.append(row['ticker'])
                for reason in set(row['missing'].values()):
                    reasons[reason].add(row['ticker'])
                    missing_counts[reason] += 1
                if not {'MISSING_EXPECTED_SESSION', 'INSUFFICIENT_HISTORY', 'SOURCE_PUBLICATION_MEMBER_UNAVAILABLE'} & set(row['missing'].values()):
                    continue
                bar_ids = payload['lineage'][row['security_id']]['bars']
                cursor.execute('SELECT session_date,system_observed_at,created_at FROM equity_bar_revisions WHERE bar_revision_id=ANY(%s::uuid[])', (bar_ids,))
                bars = cursor.fetchall()
                assert len(bars) == len(bar_ids)
                assert all(bar['system_observed_at'] <= cutoff and bar['created_at'] <= cutoff for bar in bars)
                present = {bar['session_date'] for bar in bars}
                if present:
                    expected = {stamp.date() for stamp in calendar.sessions_in_range(min(present), session)}
                    absent = expected - present
                    if absent:
                        gaps[row['ticker']].update(map(str, absent))
                        gap_identities[row['ticker']].add(row['security_id'])
                        session_gaps[row['ticker']] = dict(count=len(absent), first=str(min(absent)), last=str(max(absent)))
            result = query_generation(payload, Query())
            assert result['matched_count'] == sum(row['eligible'] for row in rows)
            assert result['unknown_count'] == len(unavailable)
            print(json.dumps(dict(status='HISTORY_SESSION_PASS', session=str(session),
                snapshot_id=str(stored['snapshot_id']), generation=payload['generation'],
                source_cutoff=payload['source_cutoff'], source_status=source['status'],
                expected=len(rows), eligible=result['matched_count'], source_unavailable=unavailable,
                field_coverage=payload['field_coverage'], missing_reason_member_counts=dict(missing_counts),
                internal_source_session_gaps=session_gaps), indent=2), flush=True)
    source_review = {}
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='10s'")
        for ticker, dates in gaps.items():
            cursor.execute("""SELECT ticker,security_id,session_date FROM equity_bar_revisions
                WHERE interval='1d' AND session_scope='RTH' AND adjusted=FALSE AND is_final
                AND session_date=ANY(%s::date[]) AND (ticker=%s OR security_id=ANY(%s::uuid[]))""",
                (sorted(dates), ticker, sorted(gap_identities[ticker])))
            retained = cursor.fetchall()
            same_ticker = {str(bar['session_date']) for bar in retained if bar['ticker'] == ticker}
            other_symbol_same_identity = {str(bar['session_date']) for bar in retained
                                          if bar['ticker'] != ticker and str(bar['security_id']) in gap_identities[ticker]}
            missing_all = dates - same_ticker - other_symbol_same_identity
            cursor.execute("""SELECT action_type,effective_date FROM equity_corporate_actions
                WHERE ticker=%s AND effective_date BETWEEN %s AND %s ORDER BY effective_date""",
                (ticker, min(dates), end))
            source_review[ticker] = dict(gap_count=len(dates), first=min(dates), last=max(dates),
                gap_date_sample=sorted(dates)[:10], sample_truncated=len(dates) > 10,
                latest_same_ticker_dates=sorted(same_ticker),
                latest_other_symbol_same_identity_count=len(other_symbol_same_identity),
                other_symbols=sorted({bar['ticker'] for bar in retained if bar['ticker'] != ticker}),
                absent_any_retained_daily_count=len(missing_all), absent_date_sample=sorted(missing_all)[:10],
                known_action_context=[dict(action_type=action['action_type'], effective_date=str(action['effective_date']))
                                      for action in cursor.fetchall()])
    assert preservation_state() == before, 'Read-only verification changed stored state'
    return dict(status='SIX_SESSION_PASS' if len(sessions) == 6 else 'BOUNDED_HISTORY_PASS',
                sessions=list(map(str, sessions)), storage_unchanged=True,
                missing_reason_tickers={reason: sorted(tickers) for reason, tickers in reasons.items()},
                source_gap_review=source_review)


def main():
    from scripts.prepare_stock_screening import preservation_state
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-publication', action='store_true')
    parser.add_argument('--start-session', type=date.fromisoformat)
    parser.add_argument('--end-session', type=date.fromisoformat)
    args = parser.parse_args()
    if bool(args.start_session) != bool(args.end_session):
        parser.error('Both historical session bounds are required')
    if args.start_session and args.check_publication:
        parser.error('Historical verification must remain read-only')
    preserved = preservation_state()
    history = verify_history(args.start_session, args.end_session) if args.start_session else None
    index = publication_index()
    assert index, 'No published screening generation'
    load_generation.cache_clear()
    started = time.perf_counter()
    result = query(Query())
    cold_ms = (time.perf_counter() - started) * 1000
    generation = load_generation(str(index[0]['snapshot_id']))
    identity_payload = {key: value for key, value in generation.items() if key not in {'generation', 'observed_at'}}
    assert digest(identity_payload) == generation['generation']
    assert generation['expected_members'] == len(generation['rows']) == len({row['security_id'] for row in generation['rows']})
    assert result['universe_count'] == result['matched_count'] + result['unknown_count'] + result['nonmatch_count']
    assert all(row['session'] == generation['session'] for row in generation['rows'])
    samples = []
    for _ in range(40):
        started = time.perf_counter()
        query(Query())
        samples.append((time.perf_counter() - started) * 1000)
    momentum = query(Query(predicate=Predicate(filters=[{'field': 'momentum_12_1', 'min': -100.}])))
    assert momentum['matched_count'] == generation['field_coverage']['momentum_12_1']
    assert momentum['unknown_count'] == len(generation['rows']) - momentum['matched_count']
    state_counts = {}
    if 'discovery_state' in generation['field_coverage']:
        for state in ('PULLBACK', 'BOUNCE', 'RESUMING_UP', 'RESUMING_DOWN'):
            predicate = Predicate(filters=[{'field': 'discovery_state', 'values': [state]}])
            selected = query(Query(predicate=predicate, limit=200))
            first = query(Query(predicate=predicate, new_only=True, limit=7))
            second = query(Query(predicate=predicate, new_only=True, offset=7, limit=200))
            expected_new = [row['security_id'] for row in selected['rows'] if row['new_status'] == 'NEW']
            assert len(selected['rows']) == selected['matched_count']
            assert first['matched_count'] == selected['matched_count'] == second['matched_count']
            assert first['result_count'] == len(expected_new) == selected['comparison']['new_count']
            assert [row['security_id'] for row in first['rows'] + second['rows']] == expected_new
            assert first['predicate_hash'] == selected['predicate_hash']
            state_counts[state] = dict(matches=selected['matched_count'], unknown=selected['unknown_count'],
                                      comparison=selected['comparison'], new_examples=[row['ticker'] for row in first['rows']])
        for older in index[1:]:
            prior = load_generation(str(older['snapshot_id']))
            assert digest({key: value for key, value in prior.items() if key not in {'generation', 'observed_at'}}) == prior['generation']
            if prior['source_publication_id'] != generation['source_publication_id']:
                continue
            current_rows = {row['security_id']: row for row in generation['rows']}
            assert set(current_rows) == {row['security_id'] for row in prior['rows']}
            for row in prior['rows']:
                current = current_rows[row['security_id']]
                assert all(current['values'][key] == value for key, value in row['values'].items())
                assert current['patterns'] == row['patterns'] and current['source_bar_id'] == row['source_bar_id']
            assert generation['lineage'] == prior['lineage']
    transaction_checks = 'NOT_REQUESTED'
    if args.check_publication:
        from database import get_db_cursor
        from equity import portal_snapshots
        from unittest.mock import patch

        def storage_state():
            with get_db_cursor() as cursor:
                cursor.execute('SET TRANSACTION READ ONLY')
                cursor.execute("""SELECT snapshot_id,payload_sha256 FROM equity_portal_snapshots ORDER BY snapshot_id""")
                snapshots = [(str(row['snapshot_id']), row['payload_sha256']) for row in cursor.fetchall()]
                cursor.execute("""SELECT snapshot_type,snapshot_id FROM equity_portal_current_projections ORDER BY snapshot_type""")
                pointers = [(row['snapshot_type'], str(row['snapshot_id'])) for row in cursor.fetchall()]
                return snapshots, pointers

        before = storage_state()
        raw_payload = {key: value for key, value in generation.items() if key != 'observed_at'}
        repeated = portal_snapshots.publish({'SCREENING_DAILY_V1': raw_payload}, index[0]['source_manifest'])
        assert repeated == [str(index[0]['snapshot_id'])] and storage_state() == before

        @contextmanager
        def interrupted_cursor():
            with get_db_cursor() as cursor:
                class Interrupted:
                    def execute(self, sql, parameters):
                        if 'equity_portal_current_projections' in sql:
                            raise RuntimeError('SIMULATED_INTERRUPTION_BEFORE_POINTER')
                        return cursor.execute(sql, parameters)
                yield Interrupted()

        with patch.object(portal_snapshots, 'get_db_cursor', interrupted_cursor):
            try:
                portal_snapshots.publish({'SCREENING_DAILY_V1': {'interrupted': True}}, {'source_generation': 0, 'test': 'rollback'})
            except RuntimeError as error:
                assert str(error) == 'SIMULATED_INTERRUPTION_BEFORE_POINTER'
            else:
                raise AssertionError('Interruption was not raised')
        assert storage_state() == before
        transaction_checks = 'IDEMPOTENT_RETRY_AND_INTERRUPTED_ROLLBACK_PASS'
    frozen = BACKEND_DIR / 'backups/stock-idea-independent-v1/replay-catalog/manifest.json'
    if frozen.exists():
        manifest = json.loads(frozen.read_text())
        assert all(hashlib.sha256((BACKEND_DIR / name).read_bytes()).hexdigest() == expected for name, expected in manifest['source_hashes'].items())
    gap_context = verify_gap_context(generation)
    hourly_context = verify_hourly_context(generation)
    momentum_horizons = verify_momentum_horizons(generation)
    if not args.check_publication:
        assert preservation_state() == preserved, 'Reader changed snapshot or pointer state'
    assert len(json.dumps(result).encode()) <= 250 * 1024
    print(json.dumps(dict(status='PASS', generation=generation['generation'], publications=len(index),
        session=generation['session'], cold_reader_ms=round(cold_ms, 3), warm_reader_p95_ms=round(sorted(samples)[37], 3),
        default_response_bytes=len(json.dumps(result).encode()), matched=result['matched_count'],
        momentum_matches=momentum['matched_count'], momentum_unknown=momentum['unknown_count'],
        cache=load_generation.cache_info()._asdict(), catalog_status=catalog()['status'],
        publication_transactions=transaction_checks, state_counts=state_counts,
        new_comparison=result['comparison'], read_only_preservation='PASS' if not args.check_publication else 'TRANSACTION_CHECK_REQUESTED',
        gap_context=gap_context,
        hourly_context=hourly_context,
        momentum_horizons=momentum_horizons,
        history=history,
        frozen_study_sources='UNCHANGED' if frozen.exists() else 'NOT_PRESENT'), indent=2))


if __name__ == '__main__':
    main()