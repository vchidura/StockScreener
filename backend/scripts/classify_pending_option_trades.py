#!/usr/bin/env python3
"""Classify retained PENDING option trades under the versioned provider rules."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_connection  # noqa: E402
from options.data.trade_classification import (  # noqa: E402
    CANCELED_CONDITIONS,
    EXCLUDED_CONDITIONS,
    INCLUDED_CONDITIONS,
    TRADE_SEMANTICS_VERSION,
)

SQL_COUNTS = """
SELECT classification_status, COUNT(*) AS trades
FROM option_trade_events
WHERE provider = 'polygon'
  AND (sip_timestamp AT TIME ZONE 'America/New_York')::date = %s
GROUP BY classification_status
ORDER BY classification_status
"""

SQL_CLASSIFY = """
UPDATE option_trade_events
SET classification_status = CASE
        WHEN correction NOT IN (0) THEN 'UNKNOWN'
        WHEN cardinality(conditions) = 0 THEN 'UNKNOWN'
    WHEN NOT (conditions <@ %s::integer[]) THEN 'UNKNOWN'
        WHEN conditions && %s::integer[] THEN 'CANCELED'
        WHEN conditions && %s::integer[] THEN 'EXCLUDED'
        WHEN conditions <@ %s::integer[] THEN 'INCLUDED'
        ELSE 'UNKNOWN'
    END,
    classification_reasons = CASE
        WHEN correction NOT IN (0)
            THEN ARRAY['CORRECTION_SEMANTICS_UNAVAILABLE']::text[]
        WHEN cardinality(conditions) = 0
            THEN ARRAY['TRADE_CONDITION_MISSING']::text[]
        WHEN NOT (conditions <@ %s::integer[])
            THEN ARRAY['UNKNOWN_TRADE_CONDITION']::text[]
        WHEN conditions && %s::integer[]
            THEN ARRAY['PROVIDER_CANCELED_CONDITION']::text[]
        WHEN conditions && %s::integer[]
            THEN ARRAY['PROVIDER_NON_VOLUME_CONDITION']::text[]
        WHEN conditions <@ %s::integer[]
            THEN ARRAY[
                'PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE',
                'AGGRESSOR_SIDE_UNAVAILABLE'
            ]::text[]
        ELSE ARRAY['UNKNOWN_TRADE_CONDITION']::text[]
    END,
    semantics_version = %s,
    updated_at = NOW()
WHERE provider = 'polygon'
  AND classification_status = 'PENDING'
  AND (sip_timestamp AT TIME ZONE 'America/New_York')::date = %s
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", default=date.today().isoformat())
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def _counts(cursor, session: date) -> list[tuple[str, int]]:
    cursor.execute(SQL_COUNTS, (session,))
    return [
        (row[0], int(row[1]))
        for row in cursor.fetchall()
    ]


def main() -> int:
    args = _parse_args()
    session = date.fromisoformat(args.session)
    with get_db_connection() as connection:
        try:
            with connection.cursor() as cursor:
                before = _counts(cursor, session)
                print(f"session={session} semantics={TRADE_SEMANTICS_VERSION}")
                print(f"before={before}")
                if not args.apply:
                    print("Dry run. Re-run with --apply to classify PENDING trades.")
                    return 0
                canceled = sorted(CANCELED_CONDITIONS)
                excluded = sorted(EXCLUDED_CONDITIONS)
                included = sorted(INCLUDED_CONDITIONS)
                known = sorted(
                    CANCELED_CONDITIONS
                    | EXCLUDED_CONDITIONS
                    | INCLUDED_CONDITIONS
                )
                cursor.execute(
                    SQL_CLASSIFY,
                    (
                        known, canceled, excluded, included,
                        known, canceled, excluded, included,
                        TRADE_SEMANTICS_VERSION, session,
                    ),
                )
                updated = cursor.rowcount
                after = _counts(cursor, session)
            connection.commit()
            print(f"updated={updated}")
            print(f"after={after}")
            return 0
        except Exception:
            connection.rollback()
            raise


if __name__ == "__main__":
    raise SystemExit(main())
