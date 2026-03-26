

-- PURPOSE : Called at the START of every load procedure Creates a STARTED log entry and returns log_id Reads run_id from session variable if set by master

CREATE OR REPLACE FUNCTION bl_cn.log_start(
    p_procedure_name VARCHAR(100),
    p_layer          VARCHAR(20)
)
RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_log_id  BIGINT;
    v_run_id  BIGINT;
BEGIN
    v_run_id := NULLIF(current_setting('bl_cn.current_run_id', TRUE), '')::BIGINT;
    INSERT INTO bl_cn.etl_log (run_id, procedure_name, layer, status, started_at, run_by) 
    VALUES ( v_run_id, p_procedure_name, p_layer, 'STARTED', NOW(), CURRENT_USER)
    RETURNING log_id INTO v_log_id;
    RAISE NOTICE '[%] STARTED (log_id=%)', p_procedure_name, v_log_id;
    RETURN v_log_id;
END;
$$;

