"""Bounded, staged source repair for the audited ARKW/BKNG gaps and BNY identity."""
import argparse
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

import exchange_calendars
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / '.env')

from database import get_db_connection, get_db_cursor
from equity.domain import BarAvailabilityMode, BarSourceKind
from equity.polygon import PolygonEquityClient, normalize_grouped_daily_bars, normalize_security_reference, sha256_json
from equity.repositories import EquityBarRepository, EquityIngestionRepository
from scripts.prepare_stock_screening import preservation_state


VERSION = 'screening_source_gap_repair_v1'
REPORT = BACKEND_DIR / 'backups' / f'{VERSION}.json'
IDENTITIES = {
    'ARKW': ('d4fe03ca-988c-56e4-b9a6-6f2d605095d0', 'BBG0077Q7LF9'),
    'BKNG': ('00f384ef-1b75-57e7-9d08-c80c1bf2aa8f', 'BBG000BLBVN4'),
    'BK': ('7fcefd3c-becf-53c4-917a-33838f71d14f', 'BBG000BD8PN9'),
    'BNY': ('7fcefd3c-becf-53c4-917a-33838f71d14f', 'BBG000BD8PN9'),
}
GAPS = {
    'ARKW': ['2026-07-22', '2026-08-14', '2026-08-21', '2026-09-01', '2026-09-09'],
    'BKNG': ['2025-09-09', '2025-09-18', '2025-09-26', '2025-10-14'],
}


def targets():
    calendar = exchange_calendars.get_calendar('XNYS')
    first = calendar.session_offset('2026-09-03', -252)
    return dict(GAPS, BK=[str(session.date()) for session in calendar.sessions_in_range(first, '2026-05-20')])


def check_identity(ticker, payload, observed_at, session):
    reference = normalize_security_reference(payload, observed_at=observed_at, source_as_of_date=date.fromisoformat(session))
    identity, figi = IDENTITIES[ticker]
    if reference.ticker != ticker or str(reference.security_id) != identity or reference.composite_figi != figi:
        raise ValueError(f'Dated identity mismatch for {ticker} on {session}')
    if reference.security_type != ('ETF' if ticker == 'ARKW' else 'CS'):
        raise ValueError(f'Unexpected security type for {ticker}')
    return reference


def selected_daily(ticker, rows, sessions):
    selected = {}
    expected = set(sessions)
    for raw in rows:
        session = datetime.fromtimestamp(raw['t'] / 1000, timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
        if session not in expected:
            continue
        if session in selected:
            raise ValueError(f'Duplicate provider daily bar: {ticker} {session}')
        if any(key not in raw or raw[key] is None for key in ('o', 'h', 'l', 'c', 'v')):
            raise ValueError(f'Incomplete provider daily bar: {ticker} {session}')
        selected[session] = dict(raw)
    if set(selected) != expected:
        raise ValueError(f'Provider daily gaps remain for {ticker}: {sorted(expected - set(selected))}')
    return selected


def fetch_report(path):
    if path.exists():
        raise ValueError('Staged evidence already exists; do not overwrite the original observation')
    client = PolygonEquityClient()
    requested = targets()
    evidence = dict(version=VERSION, targets=requested, references=[], daily={})
    reference_requests = [(ticker, session) for ticker, sessions in requested.items() for session in (sessions[0], sessions[-1])]
    reference_requests += [('BNY', '2026-05-21'), ('BNY', '2025-09-05')]
    for ticker, session in reference_requests:
        payload = client.fetch_ticker_overview(ticker, as_of_date=date.fromisoformat(session))
        observed_at = datetime.now(timezone.utc)
        if ticker == 'BNY' and session == '2025-09-05':
            if not payload or payload.get('composite_figi') != 'BBG000BZF6G2' or payload.get('type') != 'FUND':
                raise ValueError('Old BNY fund identity evidence differs from the audited case')
        else:
            check_identity(ticker, payload, observed_at, session)
        evidence['references'].append(dict(ticker=ticker, session=session, observed_at=observed_at.isoformat(), payload=payload))
    for ticker, sessions in requested.items():
        path_request = f'/v2/aggs/ticker/{ticker}/range/1/day/{sessions[0]}/{sessions[-1]}'
        rows = client._get_results(path_request, dict(adjusted='false', sort='asc', limit=50000))
        observed_at = datetime.now(timezone.utc).isoformat()
        evidence['daily'][ticker] = dict(path=path_request, adjusted=False, observed_at=observed_at,
                                       rows=selected_daily(ticker, rows, sessions))
    envelope = dict(sha256=sha256_json(evidence), evidence=evidence)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as handle:
        json.dump(envelope, handle, indent=2, allow_nan=False)
    return envelope


def load_report(path):
    envelope = json.loads(path.read_text(encoding='utf-8'))
    evidence = envelope['evidence']
    if sha256_json(evidence) != envelope['sha256'] or evidence['version'] != VERSION or evidence['targets'] != targets():
        raise ValueError('Staged repair evidence or bounds do not match')
    for reference in evidence['references']:
        if reference['ticker'] == 'BNY' and reference['session'] == '2025-09-05':
            continue
        check_identity(reference['ticker'], reference['payload'], datetime.fromisoformat(reference['observed_at']), reference['session'])
    return envelope


def prepare_bars(envelope, existing):
    evidence = envelope['evidence']
    segment = uuid5(NAMESPACE_URL, f'{VERSION}:{envelope["sha256"]}')
    native, continuity = [], []
    for ticker, sessions in targets().items():
        source = evidence['daily'][ticker]
        if source['adjusted'] or set(source['rows']) != set(sessions):
            raise ValueError('Staged daily source bounds or price basis differ')
        observed_at = datetime.fromisoformat(source['observed_at'])
        for session in sessions:
            raw = source['rows'][session]
            selected_daily(ticker, [raw], [session])
            normalized = normalize_grouped_daily_bars([dict(raw, T=ticker)], session_date=date.fromisoformat(session),
                security_ids={ticker: UUID(IDENTITIES[ticker][0])}, observed_at=observed_at,
                ingestion_segment_id=segment, availability_mode=BarAvailabilityMode.LIVE_OBSERVED)
            if len(normalized) != 1:
                raise ValueError('Expected one validated daily source bar')
            content_hash = sha256_json(dict(version=VERSION, ticker=ticker, session=session, raw=raw))
            bar = replace(normalized[0], bar_revision_id=uuid5(NAMESPACE_URL, f'{VERSION}:native:{content_hash}'),
                          payload_sha256=content_hash, quality_codes=('TARGETED_DAILY_RANGE_REPAIR',))
            prior = existing.get((ticker, session), [])
            if ticker != 'BK' and any(str(row['bar_revision_id']) != str(bar.bar_revision_id) for row in prior):
                raise ValueError(f'A previously absent target now has other bars: {ticker} {session}')
            native.append(bar)
            if ticker == 'BK':
                old = [row for row in existing.get(('BNY', session), []) if row['source_kind'] != 'RECONCILED']
                continuity_hash = sha256_json(dict(version=VERSION, source_bar_id=str(bar.bar_revision_id),
                                                   reference_evidence=envelope['sha256'], current_ticker='BNY'))
                continuity.append(replace(bar, ticker='BNY', source_kind=BarSourceKind.RECONCILED,
                    bar_revision_id=uuid5(NAMESPACE_URL, f'{VERSION}:continuity:{continuity_hash}'),
                    payload_sha256=continuity_hash, source_bar_revision_ids=(bar.bar_revision_id,),
                    supersedes_bar_revision_id=old[0]['bar_revision_id'] if len(old) == 1 else None,
                    reconciliation_status='CORRECTED' if old else 'DERIVED_MISSING',
                    quality_codes=('DATED_SYMBOL_CONTINUITY_BK_TO_BNY', 'SOURCE_TICKER_BK', 'TARGETED_IDENTITY_REPAIR')))
    return segment, native, continuity


def stored_inputs():
    requested = targets()
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='20s'")
        cursor.execute("""SELECT * FROM equity_bar_revisions WHERE ticker=ANY(%s::text[])
            AND interval='1d' AND session_scope='RTH' AND adjusted=FALSE AND is_final
            AND session_date BETWEEN %s AND %s ORDER BY created_at,bar_revision_id""",
            (list(IDENTITIES), requested['BK'][0], GAPS['ARKW'][-1]))
        rows = [dict(row) for row in cursor.fetchall()]
        cursor.execute("""SELECT publication_id,market_time,published_at,status,expected_members,selected_members
            FROM equity_bar_publications WHERE interval='1d' AND market_time::date BETWEEN '2026-09-03' AND '2026-09-11'
            ORDER BY publication_id""")
        publications = [dict(row) for row in cursor.fetchall()]
    indexed = {}
    for row in rows:
        indexed.setdefault((row['ticker'], str(row['session_date'])), []).append(row)
    hashes = {str(row['bar_revision_id']): sha256_json(json.loads(json.dumps(row, default=str))) for row in rows}
    return indexed, hashes, publications


@contextmanager
def shared_transaction():
    with get_db_connection() as connection:
        class BorrowedConnection:
            closed = False

            def cursor(self, **kwargs):
                return connection.cursor(**kwargs)

            def commit(self):
                pass

            def rollback(self):
                connection.rollback()

        @contextmanager
        def factory():
            yield BorrowedConnection()

        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='30s'")
            cursor.execute("SET LOCAL lock_timeout='5s'")
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext('screening-source-gap-repair-v1'))")
        try:
            yield factory
            connection.commit()
        except Exception:
            connection.rollback()
            raise


def repair(path, apply=False):
    envelope = load_report(path)
    before = preservation_state()
    existing, old_hashes, publications = stored_inputs()
    segment, native, continuity = prepare_bars(envelope, existing)
    wrong_prices = []
    for bar in continuity:
        for old in existing.get(('BNY', str(bar.session_date)), []):
            if old['source_kind'] != 'RECONCILED' and old['close_price'] != bar.close_price:
                wrong_prices.append(dict(session=str(bar.session_date), old_close=str(old['close_price']), bank_close=str(bar.close_price)))
    inserted = 0
    if apply:
        with shared_transaction() as factory:
            ingestion = EquityIngestionRepository(factory)
            repository = EquityBarRepository(factory)
            observed_at = max(bar.system_observed_at for bar in native)
            ingestion.start_segment(ingestion_segment_id=segment, provider='POLYGON', provider_mode='REST',
                dataset=VERSION, interval='1d', requested_from=min(bar.bar_start for bar in native),
                requested_to=max(bar.bar_end for bar in native), observed_at=observed_at)
            inserted += repository.persist(native)
            inserted += repository.persist(continuity)
            ingestion.complete_segment(segment, status='COMPLETE', market_watermark=max(bar.bar_end for bar in native),
                record_count=len(native) + len(continuity), byte_count=path.stat().st_size,
                checksum_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), archive_uri=str(path),
                gap_details=dict(version=VERSION, evidence_sha256=envelope['sha256'],
                                 historical_ticker='BK', continuity_ticker='BNY', actual_observation_only=True),
                completed_at=datetime.now(timezone.utc))
    after, new_hashes, after_publications = stored_inputs()
    if preservation_state() != before or publications != after_publications or any(new_hashes.get(key) != value for key, value in old_hashes.items()):
        raise ValueError('Existing snapshot, source publication, pointer or bar was modified')
    if apply:
        for bar in native + continuity:
            actual = next((row for row in after[(bar.ticker, str(bar.session_date))] if row['bar_revision_id'] == bar.bar_revision_id), None)
            fields = ('security_id', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'payload_sha256')
            if not actual or any(actual[field] != getattr(bar, field) for field in fields):
                raise ValueError('Inserted source revision failed readback')
            if actual['system_observed_at'] != bar.system_observed_at or actual['replay_available_at'] is not None:
                raise ValueError('Repair observation time was backdated')
            if tuple(actual['source_bar_revision_ids']) != bar.source_bar_revision_ids:
                raise ValueError('Repair source lineage differs')
    result = dict(status='APPLIED' if apply else 'STAGED_NO_DATABASE_WRITES', evidence=str(path),
        segment=str(segment), native_bars=len(native), continuity_bars=len(continuity), inserted=inserted,
        direct_gap_counts={ticker: len(dates) for ticker, dates in GAPS.items()},
        bny_missing_dates=sum(not existing.get(('BNY', session)) for session in targets()['BK']),
        bny_existing_price_mismatches=len(wrong_prices), mismatch_examples=wrong_prices[:5],
        preservation='PASS', source_publications_and_snapshots_unchanged=True,
        availability='ACTUAL_RETRIEVAL_TIME_NO_REPLAY_BACKDATE')
    print(json.dumps(result, indent=2), flush=True)
    return result


def verify_sources(path):
    envelope = load_report(path)
    existing, _, _ = stored_inputs()
    _, native, continuity = prepare_bars(envelope, existing)
    calendar = exchange_calendars.get_calendar('XNYS')
    first, last = targets()['BK'][0], '2026-09-11'
    expected = {str(session.date()) for session in calendar.sessions_in_range(first, last)}
    before = preservation_state()
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='20s'")
        cursor.execute("""SELECT DISTINCT ON(ticker,session_date) * FROM equity_bar_revisions
            WHERE ticker=ANY(%s::text[]) AND interval='1d' AND session_scope='RTH' AND adjusted=FALSE
            AND is_final AND session_date BETWEEN %s AND %s AND system_observed_at<=now() AND created_at<=now()
            ORDER BY ticker,session_date,CASE source_kind WHEN 'RECONCILED' THEN 0 WHEN 'DERIVED' THEN 1 ELSE 2 END,
            system_observed_at DESC,created_at DESC,bar_revision_id""", (['ARKW', 'BKNG', 'BNY'], first, last))
        preferred = {(row['ticker'], str(row['session_date'])): dict(row) for row in cursor.fetchall()}
    for ticker in ('ARKW', 'BKNG', 'BNY'):
        actual_dates = {session for symbol, session in preferred if symbol == ticker}
        if actual_dates != expected:
            raise ValueError(f'Repaired preferred history has gaps: {ticker} {sorted(expected - actual_dates)}')
        if any(str(row['security_id']) != IDENTITIES[ticker][0] for (symbol, _), row in preferred.items() if symbol == ticker):
            raise ValueError(f'Repaired history crosses security identity: {ticker}')
    for bar in [source for source in native if source.ticker != 'BK'] + continuity:
        actual = preferred[(bar.ticker, str(bar.session_date))]
        if actual['bar_revision_id'] != bar.bar_revision_id or tuple(actual['source_bar_revision_ids']) != bar.source_bar_revision_ids:
            raise ValueError('Repaired source is not the preferred revision or its lineage changed')
    if preservation_state() != before:
        raise ValueError('Read-only source verification changed snapshot state')
    print(json.dumps(dict(status='SOURCE_REPAIR_VERIFIED', first_session=first, last_session=last,
        complete_sessions_per_ticker=len(expected), tickers=['ARKW', 'BKNG', 'BNY'], remaining_source_gaps=0,
        bny_pretransition_preferred_bank_revisions=len(continuity), native_bk_lineage_verified=True,
        snapshots_and_pointers_unchanged=True), indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--fetch', action='store_true')
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--verify', action='store_true')
    parser.add_argument('--report', type=Path, default=REPORT)
    args = parser.parse_args()
    if args.verify:
        verify_sources(args.report)
        return
    if args.fetch:
        fetch_report(args.report)
    repair(args.report, apply=args.apply)


if __name__ == '__main__':
    main()