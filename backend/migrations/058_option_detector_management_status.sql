SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.option_signal_current_marks
    ADD COLUMN management_status varchar(24),
    ADD COLUMN management_status_evaluation_id uuid REFERENCES public.option_detector_evaluations(evaluation_id),
    ADD COLUMN management_status_plan_sha256 char(64),
    ADD COLUMN management_status_threshold_price numeric(20,8),
    ADD COLUMN management_status_mark_price numeric(20,8),
    ADD COLUMN management_status_market_time timestamptz,
    ADD COLUMN management_status_observed_time timestamptz,
    ADD COLUMN management_status_source_snapshot_ids uuid[],
    ADD COLUMN management_status_recorded_at timestamptz,
    ADD CONSTRAINT option_current_mark_management_status_contract CHECK (
        (management_status IS NULL AND management_status_evaluation_id IS NULL
            AND management_status_plan_sha256 IS NULL AND management_status_threshold_price IS NULL
            AND management_status_mark_price IS NULL AND management_status_market_time IS NULL
            AND management_status_observed_time IS NULL AND management_status_source_snapshot_ids IS NULL
            AND management_status_recorded_at IS NULL)
        OR (management_status IS NOT NULL AND management_status IN ('TARGET_MET','STOP_LOSS_HIT')
            AND management_status_evaluation_id IS NOT NULL
            AND management_status_plan_sha256 ~ '^[0-9a-f]{64}$'
            AND management_status_threshold_price >= 0 AND management_status_mark_price >= 0
            AND management_status_market_time IS NOT NULL
            AND management_status_observed_time >= management_status_market_time
            AND management_status_source_snapshot_ids IS NOT NULL
            AND cardinality(management_status_source_snapshot_ids) > 0
            AND management_status_recorded_at >= management_status_observed_time)
    );

CREATE OR REPLACE FUNCTION public.guard_option_detector_management_status() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.management_status IS NOT NULL AND ROW(
        NEW.management_status, NEW.management_status_evaluation_id, NEW.management_status_plan_sha256,
        NEW.management_status_threshold_price, NEW.management_status_mark_price,
        NEW.management_status_market_time, NEW.management_status_observed_time,
        NEW.management_status_source_snapshot_ids, NEW.management_status_recorded_at
    ) IS DISTINCT FROM ROW(
        OLD.management_status, OLD.management_status_evaluation_id, OLD.management_status_plan_sha256,
        OLD.management_status_threshold_price, OLD.management_status_mark_price,
        OLD.management_status_market_time, OLD.management_status_observed_time,
        OLD.management_status_source_snapshot_ids, OLD.management_status_recorded_at
    ) THEN
        RAISE EXCEPTION 'detector management terminal status is immutable';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS option_detector_management_status_immutable ON public.option_signal_current_marks;
CREATE TRIGGER option_detector_management_status_immutable BEFORE UPDATE ON public.option_signal_current_marks
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_detector_management_status();

INSERT INTO public.schema_migrations(version) VALUES ('058_option_detector_management_status') ON CONFLICT DO NOTHING;
