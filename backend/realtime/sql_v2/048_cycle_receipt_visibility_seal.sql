ALTER TABLE rt.cycle_receipts
    ADD COLUMN known_by timestamptz;

ALTER TABLE rt.cycle_receipts
    ADD CONSTRAINT cycle_receipts_input_visibility_seal_check
    CHECK (
        (receipt_kind <> 'INPUT_READY')
        OR (known_by IS NOT NULL AND known_by >= cutoff)
    ) NOT VALID;
