-- =============================================================================
-- 07_pipeline_tasks.sql — Orchestration with dry-run mode, run logging, notifications
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Master orchestrator with dry-run support and observability
CREATE OR REPLACE PROCEDURE SP_RUN_PIPELINE(P_DRY_RUN BOOLEAN DEFAULT FALSE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import time
from snowflake.snowpark.functions import col, lit, current_timestamp

def log_step(session, run_id, step, status, details, start_time, dry_run):
    duration = int((time.time() - start_time) * 1000)
    session.sql(
        f"INSERT INTO SELFHEALING_PROD.CONFIG.PIPELINE_RUN_LOG "
        f"(RUN_ID, STEP, STATUS, DETAILS, DURATION_MS, DRY_RUN) "
        f"VALUES ('{run_id}', '{step}', '{status}', '{details[:500].replace(chr(39), chr(39)+chr(39))}', {duration}, {dry_run})"
    ).collect()

def run(session, P_DRY_RUN):
    import uuid
    run_id = str(uuid.uuid4())
    results = []

    # Step 1: Detect
    t = time.time()
    try:
        detect_result = session.sql("CALL SELFHEALING_PROD.CONFIG.SP_SCHEMA_DRIFT_DETECTOR()").collect()
        detect_msg = detect_result[0][0]
        log_step(session, run_id, "DETECT", "OK", detect_msg, t, P_DRY_RUN)
        results.append(f"1.DETECT: {detect_msg}")
    except Exception as e:
        log_step(session, run_id, "DETECT", "ERROR", str(e)[:200], t, P_DRY_RUN)
        return f"Pipeline failed at DETECT: {str(e)[:200]}"

    # Step 2: Health checks
    t = time.time()
    try:
        health_result = session.sql("CALL SELFHEALING_PROD.CONFIG.SP_PIPELINE_HEALTH_CHECK()").collect()
        health_msg = health_result[0][0]
        log_step(session, run_id, "HEALTH_CHECK", "OK", health_msg, t, P_DRY_RUN)
        results.append(f"2.HEALTH: {health_msg}")
    except Exception as e:
        log_step(session, run_id, "HEALTH_CHECK", "ERROR", str(e)[:200], t, P_DRY_RUN)
        results.append(f"2.HEALTH: ERROR {str(e)[:100]}")

    # Step 3: Halt gate
    t = time.time()
    halt_result = session.sql("CALL SELFHEALING_PROD.CONFIG.SP_HALT_GATE()").collect()
    halt_msg = halt_result[0][0]
    log_step(session, run_id, "HALT_GATE", halt_msg[:6], halt_msg, t, P_DRY_RUN)
    results.append(f"3.HALT: {halt_msg}")

    if halt_msg.startswith("CLEAR"):
        return f"Pipeline clear (run_id={run_id}) | " + " | ".join(results)

    if P_DRY_RUN:
        log_step(session, run_id, "DRY_RUN_STOP", "OK", "Dry run — stopping before diagnosis/codegen", t, P_DRY_RUN)
        return f"DRY RUN complete (run_id={run_id}): would process events | " + " | ".join(results)

    # Step 4: Diagnose
    t = time.time()
    try:
        diag_result = session.sql("CALL SELFHEALING_PROD.CONFIG.SP_DIAGNOSE_PENDING()").collect()
        diag_msg = diag_result[0][0]
        log_step(session, run_id, "DIAGNOSE", "OK", diag_msg, t, P_DRY_RUN)
        results.append(f"4.DIAGNOSE: {diag_msg}")
    except Exception as e:
        log_step(session, run_id, "DIAGNOSE", "ERROR", str(e)[:200], t, P_DRY_RUN)
        results.append(f"4.DIAGNOSE: ERROR")

    # Step 5: Code generation for first pending event
    t = time.time()
    pending = session.table("SELFHEALING_PROD.CONFIG.SCHEMA_CHANGE_EVENTS").filter(
        col("PIPELINE_STATUS") == "PENDING"
    ).sort(col("DETECTED_AT")).limit(1).collect()

    if not pending:
        log_step(session, run_id, "CODEGEN", "SKIP", "No pending events", t, P_DRY_RUN)
        return f"Pipeline complete (run_id={run_id}) | " + " | ".join(results)

    event_id = pending[0]["EVENT_ID"]
    try:
        codegen_result = session.sql(f"CALL SELFHEALING_PROD.CONFIG.SP_GENERATE_ARTIFACT_CODE('{event_id}')").collect()
        codegen_msg = codegen_result[0][0]
        log_step(session, run_id, "CODEGEN", "OK", codegen_msg, t, P_DRY_RUN)
        results.append(f"5.CODEGEN: {codegen_msg}")
    except Exception as e:
        log_step(session, run_id, "CODEGEN", "ERROR", str(e)[:200], t, P_DRY_RUN)
        return f"Pipeline failed at CODEGEN (run_id={run_id}): {str(e)[:200]}"

    # Step 6: Validate
    t = time.time()
    try:
        val_result = session.sql(f"CALL SELFHEALING_PROD.CONFIG.SP_VALIDATE_GENERATED_CODE('{event_id}')").collect()
        val_msg = val_result[0][0]
        log_step(session, run_id, "VALIDATE", "OK", val_msg, t, P_DRY_RUN)
        results.append(f"6.VALIDATE: {val_msg}")
    except Exception as e:
        log_step(session, run_id, "VALIDATE", "ERROR", str(e)[:200], t, P_DRY_RUN)
        results.append(f"6.VALIDATE: ERROR")

    # Step 7: Mark complete
    log_step(session, run_id, "COMPLETE", "OK", "Pipeline run finished", t, P_DRY_RUN)
    return f"Pipeline complete (run_id={run_id}) | " + " | ".join(results)
$$;

-- Resolve event: advance baseline
CREATE OR REPLACE PROCEDURE SP_RESOLVE_EVENT(P_EVENT_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_table_name VARCHAR;
    v_change_type VARCHAR;
    v_column_name VARCHAR;
    v_new_type VARCHAR;
BEGIN
    SELECT TABLE_NAME, CHANGE_TYPE, COLUMN_NAME, NEW_DATA_TYPE
    INTO v_table_name, v_change_type, v_column_name, v_new_type
    FROM SCHEMA_CHANGE_EVENTS WHERE EVENT_ID = :P_EVENT_ID;

    IF (v_change_type = 'NEW_COLUMN') THEN
        INSERT INTO SCHEMA_REGISTRY (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE)
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE
        FROM SELFHEALING_PROD.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME = :v_table_name AND COLUMN_NAME = :v_column_name;
    ELSEIF (v_change_type = 'COLUMN_DROP') THEN
        DELETE FROM SCHEMA_REGISTRY WHERE TABLE_NAME = :v_table_name AND COLUMN_NAME = :v_column_name;
    ELSEIF (v_change_type = 'TYPE_CHANGE') THEN
        UPDATE SCHEMA_REGISTRY SET DATA_TYPE = :v_new_type WHERE TABLE_NAME = :v_table_name AND COLUMN_NAME = :v_column_name;
    END IF;

    UPDATE SCHEMA_CHANGE_EVENTS SET PIPELINE_STATUS = 'RESOLVED', RESOLVED_AT = CURRENT_TIMESTAMP() WHERE EVENT_ID = :P_EVENT_ID;
    UPDATE PIPELINE_HEALTH_AUDIT SET STATUS = 'RESOLVED', RESOLVED_AT = CURRENT_TIMESTAMP()
    WHERE TABLE_NAME = :v_table_name AND COLUMN_NAME = :v_column_name AND STATUS = 'OPEN';

    RETURN 'Event ' || :P_EVENT_ID || ' resolved. Baseline advanced.';
END;
$$;

-- Scheduled task (suspended by default)
CREATE OR REPLACE TASK TASK_SELF_HEALING_PIPELINE
    WAREHOUSE = SELFHEALING_WH
    SCHEDULE = '15 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
AS
    CALL SP_RUN_PIPELINE(FALSE);

ALTER TASK TASK_SELF_HEALING_PIPELINE SUSPEND;
