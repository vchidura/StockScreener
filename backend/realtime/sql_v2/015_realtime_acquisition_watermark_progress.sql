SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.acquisition_watermarks
    ADD COLUMN last_examined_bar_end timestamptz;

UPDATE rt.acquisition_watermarks
SET last_examined_bar_end = last_complete_bar_end;

ALTER TABLE rt.acquisition_watermarks
    ALTER COLUMN last_examined_bar_end SET NOT NULL,
    ALTER COLUMN last_complete_bar_end DROP NOT NULL,
    ADD CONSTRAINT acquisition_watermarks_progress_check CHECK (
        (last_complete_bar_end IS NULL
         OR last_complete_bar_end <= last_examined_bar_end)
        AND
        (terminal_state <> 'COMPLETE'
         OR last_complete_bar_end = last_examined_bar_end)
    );

CREATE INDEX idx_rt_acquisition_watermark_progress
    ON rt.acquisition_watermarks (
        cohort_id,
        interval,
        source_kind,
        last_examined_bar_end
    );
