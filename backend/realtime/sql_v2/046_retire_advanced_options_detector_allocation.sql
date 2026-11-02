SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Forward-only retirement of 045_advanced_options_detector_allocation, whose file
-- was withdrawn after it reached one ledger. The ledger row stays; see
-- RETIRED_REALTIME_MIGRATIONS. Only that database has this empty table, and no
-- code reads it. No CASCADE: an unexpected dependent must stop this migration.
DROP TABLE IF EXISTS rt.advanced_options_detector_allocations;
