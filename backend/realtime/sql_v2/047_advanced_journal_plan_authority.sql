SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Journal ownership is shared by Equity ingress plans and Options service plans.
-- The runtime validates that plan_id exists in one of those immutable authorities.
ALTER TABLE rt.advanced_journal_registry
    DROP CONSTRAINT advanced_journal_registry_plan_id_fkey;
