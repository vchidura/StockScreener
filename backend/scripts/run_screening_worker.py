"""Publish completed hourly context alongside an immutable completed daily anchor."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
import threading

from dotenv import load_dotenv
import exchange_calendars

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / '.env')

from database import get_db_cursor
from equity.leadership import try_advisory_leadership
from equity.portal_snapshots import publish
from equity.screening_hourly import attach_hourly
from equity.screening_projection import build_latest, SNAPSHOT_TYPE
from research.screening import HOURLY_REFRESH_POLICY, REFRESHED_HOURLY_CATALOG, digest

LOCK_NAME = 'stock-screener:screening-publication-worker'


def manifest(payload):
    return dict({name: value for name, value in payload.items() if name not in {'rows', 'lineage', 'hourly_lineage'}}, source_generation=0)


def load_daily_anchor(now):
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='15s'")
        cursor.execute("""SELECT publication_id FROM equity_bar_publications
            WHERE interval='1d' AND session_scope='RTH' AND adjusted=FALSE AND status='COMPLETE'
            AND expected_members=selected_members AND market_time<=%s AND published_at<=%s AND created_at<=%s
            ORDER BY market_time DESC,published_at DESC,created_at DESC LIMIT 1""", (now, now, now))
        source = cursor.fetchone()
        cursor.execute("""SELECT payload FROM equity_portal_snapshots WHERE snapshot_type=%s
            AND NOT (source_manifest ? 'hourly_refresh_policy') AND generated_at<=%s
            ORDER BY source_manifest->>'session' DESC,generated_at DESC,snapshot_id LIMIT 1""", (SNAPSHOT_TYPE, now))
        anchor = cursor.fetchone()
    return str(source['publication_id']) if source else None, anchor['payload'] if anchor else None


def prepare_pair(anchor, now):
    code_hashes = {name: digest(BACKEND_DIR.joinpath('equity', name).read_text(encoding='utf-8'))
                   for name in ('screening_hourly.py', 'derivation.py')}
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute("""SELECT * FROM equity_bar_publications WHERE interval='1h' AND session_scope='RTH'
            AND adjusted=FALSE AND status IN ('COMPLETE','DEGRADED')
            AND market_time<=%s AND published_at<=%s AND created_at<=%s
            ORDER BY market_time DESC,published_at DESC,created_at DESC LIMIT 1""", (now, now, now))
        hourly = cursor.fetchone()
        daily = dict(market_time=datetime.fromisoformat(anchor['market_time']), published_at=datetime.fromisoformat(anchor['source_cutoff']))
        if not hourly or hourly['market_time'] < daily['market_time']:
            return None, dict(status='WAITING_FOR_HOURLY_PUBLICATION', daily_session=anchor['session'])
        pair_key = digest(dict(policy=HOURLY_REFRESH_POLICY, daily_generation=anchor['generation'],
                               hourly_publication_id=str(hourly['publication_id']), contract=REFRESHED_HOURLY_CATALOG, code_hashes=code_hashes))
        cursor.execute("""SELECT snapshot_id,source_manifest FROM equity_portal_snapshots
            WHERE snapshot_type=%s AND source_manifest->>'hourly_pair_key'=%s LIMIT 1""", (SNAPSHOT_TYPE, pair_key))
        existing = cursor.fetchone()
        if existing:
            return None, dict(status='ALREADY_PUBLISHED', snapshot_id=str(existing['snapshot_id']),
                generation=existing['source_manifest']['generation'], daily_session=anchor['session'], hourly_market_time=hourly['market_time'].isoformat())
        payload = deepcopy(anchor)
        payload.update(daily_generation=anchor['generation'], hourly_pair_key=pair_key)
        members = [dict(security_id=row['security_id'], ticker=row['ticker'], security_revision_id=row.get('reference_id')) for row in anchor['rows']]
        if not 0 < len(members) <= 1000 or len(members) != anchor['expected_members']:
            raise ValueError('Daily anchor membership exceeds bound or is incomplete')
        payload = attach_hourly(cursor, daily, members, payload, exchange_calendars.get_calendar('XNYS'), hourly_publication=hourly)
    return payload, dict(status='PREPARED', generation=payload['generation'], daily_generation=anchor['generation'],
        daily_session=anchor['session'], daily_source_cutoff=anchor['source_cutoff'], hourly_source=payload['hourly_source'],
        hourly_coverage=payload['hourly_coverage'])


def run_once(*, publish_result=True):
    now = datetime.now(timezone.utc)
    source_id, anchor = load_daily_anchor(now)
    if source_id is None:
        return dict(status='WAITING_FOR_DAILY_PUBLICATION')
    if anchor is None or anchor['source_publication_id'] != source_id:
        anchor = build_latest()
        if publish_result:
            publish({SNAPSHOT_TYPE: anchor}, manifest(anchor))
    payload, result = prepare_pair(anchor, datetime.now(timezone.utc))
    if payload is not None:
        if publish_result:
            result.update(status='PUBLISHED', snapshot_ids=publish({SNAPSHOT_TYPE: payload}, manifest(payload)))
        else:
            result['status'] = 'MEASURED_NO_WRITES'
    return result


def status():
    with get_db_cursor() as cursor:
        cursor.execute('SET TRANSACTION READ ONLY')
        cursor.execute("SET LOCAL statement_timeout='5s'")
        cursor.execute("""SELECT snapshot_id,source_manifest,generated_at FROM equity_portal_snapshots
            WHERE snapshot_type=%s ORDER BY source_manifest->>'session' DESC,generated_at DESC,snapshot_id LIMIT 1""", (SNAPSHOT_TYPE,))
        row = cursor.fetchone()
    if not row:
        return dict(status='AWAITING_PUBLICATION')
    source = row['source_manifest']
    return dict(status='RETAINED_PUBLICATION', snapshot_id=str(row['snapshot_id']), generated_at=row['generated_at'].isoformat(),
        daily_session=source['session'], daily_generation=source.get('daily_generation', source['generation']),
        generation=source['generation'], hourly_refresh_policy=source.get('hourly_refresh_policy'),
        hourly_source=source.get('hourly_source'), hourly_coverage=source.get('hourly_coverage'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--once', action='store_true')
    modes.add_argument('--status', action='store_true')
    modes.add_argument('--measure', action='store_true')
    parser.add_argument('--poll-seconds', type=int, default=int(os.getenv('SCREENING_WORKER_POLL_SECONDS', '60')))
    args = parser.parse_args()
    if args.poll_seconds < 15:
        parser.error('poll-seconds must be at least 15')
    if args.status or args.measure:
        print(json.dumps(status() if args.status else run_once(publish_result=False), indent=2), flush=True)
        return
    logging.basicConfig(level=logging.INFO)
    with try_advisory_leadership(LOCK_NAME) as leader:
        if not leader:
            raise RuntimeError('Another screening publisher holds leadership')
        stop = threading.Event()
        try:
            while True:
                try:
                    result = run_once()
                    print(json.dumps(dict(checked_at=datetime.now(timezone.utc).isoformat(), **result)), flush=True)
                except Exception:
                    if args.once:
                        raise
                    logging.exception('Screening refresh failed; previous immutable publication retained')
                if args.once:
                    return
                stop.wait(args.poll_seconds)
        except KeyboardInterrupt:
            print('Screening publisher stopped.', flush=True)


if __name__ == '__main__':
    main()