SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

INSERT INTO rt.stage_catalog (
    stage_id,
    version,
    domain,
    prerequisites,
    execution_lane,
    schedule_kind,
    gates_strategy,
    deadline_offset_seconds
) VALUES
    ('REFERENCE','stages_v3','shared',ARRAY[]::text[],'data','INTERVAL',false,NULL),
    ('EQUITY_INPUT_GATE','stages_v3','equity',ARRAY['REFERENCE'],'data','WINDOW',false,120),
    ('EQUITY_MATERIALIZE','stages_v3','equity',ARRAY['EQUITY_INPUT_GATE'],'data','WINDOW',false,120),
    ('STOCK_ALERTS','stages_v3','equity',ARRAY['EQUITY_MATERIALIZE'],'alert','WINDOW',false,120),
    ('EQUITY_PROJECTION','stages_v3','equity',ARRAY['EQUITY_MATERIALIZE'],'portal','COALESCED',false,300),
    ('PORTAL_PUBLISH','stages_v3','equity',ARRAY['EQUITY_PROJECTION'],'portal','COALESCED',false,300),
    ('STRATEGY_PUBLISH','stages_v3','equity',ARRAY['EQUITY_MATERIALIZE'],'portal','COALESCED',false,300);
