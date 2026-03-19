
-- Master procedure - runs full stg_cln layer

CREATE OR REPLACE PROCEDURE stg_cln.sp_load_stg_all()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '=== STG_CLN LAYER START ===';
    CALL stg_cln.sp_load_stg_offline();
    CALL stg_cln.sp_load_stg_online();
    RAISE NOTICE '=== STG_CLN LAYER COMPLETE ===';
END;
$$;