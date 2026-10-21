CREATE TABLE rt.advanced_options_read_pins (
    cohort_id uuid PRIMARY KEY REFERENCES rt.cohorts(cohort_id),
    release_id uuid NOT NULL,
    release_cutoff timestamptz NOT NULL,
    package_sha256 text NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    enabled boolean NOT NULL,
    enabled_at timestamptz,
    disabled_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (enabled AND enabled_at IS NOT NULL AND disabled_at IS NULL)
        OR (NOT enabled AND disabled_at IS NOT NULL)
    ),
    FOREIGN KEY (release_id, release_cutoff)
        REFERENCES rt.portal_releases(release_id, cutoff)
);

CREATE INDEX advanced_options_read_pins_release_idx
    ON rt.advanced_options_read_pins(release_id, release_cutoff)
    WHERE enabled;
