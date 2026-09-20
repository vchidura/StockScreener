SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.guard_equity_behavior_evidence_link() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE parent_kind text; parent_created_here boolean; source_contract_ok boolean;
BEGIN
    SELECT context_kind, xmin::text = pg_current_xact_id()::text
    INTO parent_kind, parent_created_here
    FROM public.equity_context_snapshots
    WHERE equity_context_snapshot_id = COALESCE(NEW.equity_context_snapshot_id, OLD.equity_context_snapshot_id);
    IF parent_kind = 'STOCK_BEHAVIOR' THEN
        IF TG_OP = 'INSERT' AND parent_created_here THEN
            SELECT EXISTS (
                SELECT 1 FROM public.equity_evidence
                WHERE evidence_id = NEW.evidence_id AND (
                    (source_name = 'STOCK_BEHAVIOR_ADJUSTED_DAILY'
                     AND source_version = 'provider_adjusted_daily_history_v1'
                     AND payload_schema_version = 'stock_behavior_adjusted_1d_v1')
                    OR
                    (source_name = 'STOCK_BEHAVIOR_RAW_SOURCE'
                     AND source_version = 'behavior_feature_source_v1'
                     AND payload_schema_version = 'stock_behavior_raw_source_v1')
                )
            ) INTO source_contract_ok;
            IF source_contract_ok THEN RETURN NEW; END IF;
            RAISE EXCEPTION 'stock behavior links require dedicated immutable source evidence';
        END IF;
        RAISE EXCEPTION 'stock behavior evidence links are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;