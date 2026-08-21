#!/bin/sh
set -eu

dump_file="/docker-entrypoint-initdb.d/stocks_db_backup.dump"

if [ ! -f "$dump_file" ]; then
    echo "Database backup not found: $dump_file" >&2
    exit 1
fi

echo "Restoring bundled stock database..."
pg_restore \
    --no-owner \
    --no-privileges \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    "$dump_file"
echo "Database restore complete."