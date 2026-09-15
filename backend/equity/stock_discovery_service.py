"""Read-only access to retained legacy discovery snapshots."""

from equity.stock_discovery import SNAPSHOT_SOURCE, VERSION

def load_snapshot(cursor, session=None):
    cursor.execute("""
        SELECT MAX(market_time) AS market_time FROM equity_evidence
        WHERE source_name=%s AND source_version=%s
          AND (%s::date IS NULL OR market_time::date=%s::date)
    """, (SNAPSHOT_SOURCE, VERSION, session, session))
    stamp = cursor.fetchone()["market_time"]
    if stamp is None:
        return None, []
    cursor.execute("""SELECT payload,observed_at FROM equity_evidence
        WHERE source_name=%s AND source_version=%s AND market_time=%s ORDER BY ticker""", (SNAPSHOT_SOURCE, VERSION, stamp))
    records = cursor.fetchall()
    return dict(market_time=stamp, observed_at=max(row["observed_at"] for row in records)), [row["payload"] for row in records]
