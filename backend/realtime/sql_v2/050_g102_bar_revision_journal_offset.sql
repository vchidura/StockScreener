-- G102 design 5.4 (B24): the journal bridge checks, every tick, the highest canonical offset stored for its stream
-- session (`advanced_journal_replay`: MAX(source_offset) WHERE journal_generation AND stream_session_id). With no index
-- it scanned every rt.bar_revisions partition: 32% of database time on Nov 6, up to 2.8 s a call (634 ms idle on
-- Nov 7). This partial index makes it a short backward index read per partition. Built on the partitioned parent, so
-- every partition (and each one created later) gets it; about 750k rows on Nov 7, seconds to build.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '300s';

CREATE INDEX idx_rt_bar_revision_journal_offset
    ON rt.bar_revisions (journal_generation, stream_session_id, source_offset)
    WHERE stream_session_id IS NOT NULL;
