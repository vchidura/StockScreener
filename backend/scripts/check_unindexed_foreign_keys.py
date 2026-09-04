import sys
from pathlib import Path
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor

# Foreign keys whose referencing columns have no index prefix: every delete on the
# parent forces a sequential scan of the child table.
QUERY = """
SELECT
    child.relname  AS child_table,
    constraint_.conname AS constraint_name,
    parent.relname AS parent_table,
    (SELECT string_agg(attname, ',' ORDER BY ordinality)
       FROM unnest(constraint_.conkey) WITH ORDINALITY AS k(attnum, ordinality)
       JOIN pg_attribute a ON a.attrelid = child.oid AND a.attnum = k.attnum
    ) AS child_columns,
    pg_size_pretty(pg_relation_size(child.oid)) AS child_size
FROM pg_constraint constraint_
JOIN pg_class child  ON child.oid  = constraint_.conrelid
JOIN pg_class parent ON parent.oid = constraint_.confrelid
WHERE constraint_.contype = 'f'
  AND child.relnamespace = 'public'::regnamespace
  AND NOT EXISTS (
      SELECT 1 FROM pg_index index_
      WHERE index_.indrelid = constraint_.conrelid
        AND (index_.indkey::smallint[])[0:array_length(constraint_.conkey, 1) - 1]
            = constraint_.conkey
  )
ORDER BY pg_relation_size(child.oid) DESC
"""

with get_db_cursor() as cursor:
    cursor.execute(QUERY)
    rows = cursor.fetchall()

print(f"unindexed foreign keys: {len(rows)}\n")
for row in rows:
    print(f"  {row['child_table']}.({row['child_columns']}) -> {row['parent_table']}"
          f"  [{row['child_size']}]  {row['constraint_name']}")
