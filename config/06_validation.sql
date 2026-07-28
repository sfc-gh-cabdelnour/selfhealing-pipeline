-- =============================================================================
-- 06_validation.sql — Zero-copy clone + dbt run validation
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Procedure: Create/refresh DEV environment as zero-copy clone of PROD
CREATE OR REPLACE PROCEDURE SP_REFRESH_DEV_ENVIRONMENT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    CREATE OR REPLACE DATABASE SELFHEALING_DEV CLONE SELFHEALING_PROD;
    RETURN 'DEV environment refreshed (zero-copy clone of PROD)';
END;
$$;

-- Procedure: Run validation against DEV clone
-- In production this would use EXECUTE DBT PROJECT against the clone.
-- For the demo, we use direct SQL execution of generated code against DEV.
CREATE OR REPLACE PROCEDURE SP_VALIDATE_GENERATED_CODE(p_event_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_pass NUMBER := 0;
    v_fail NUMBER := 0;
    v_total NUMBER := 0;
BEGIN
    -- Refresh DEV clone first
    CALL SP_REFRESH_DEV_ENVIRONMENT();

    -- Execute each generated model against DEV
    FOR rec IN (
        SELECT ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, GENERATED_SQL
        FROM GENERATED_CODE
        WHERE EVENT_ID = :p_event_id
        ORDER BY ARTIFACT_SCHEMA  -- SILVER before GOLD (dependency order)
    ) DO
        v_total := v_total + 1;
        BEGIN
            -- Replace SELFHEALING_PROD with SELFHEALING_DEV for validation
            LET v_dev_sql VARCHAR := REPLACE(rec.GENERATED_SQL, 'SELFHEALING_PROD', 'SELFHEALING_DEV');
            -- Strip Jinja config blocks for direct execution
            v_dev_sql := REGEXP_REPLACE(v_dev_sql, '\\{\\{\\s*config\\([^)]*\\)\\s*\\}\\}', '');
            -- Replace source/ref with fully qualified names for DEV
            v_dev_sql := REGEXP_REPLACE(v_dev_sql, '\\{\\{\\s*source\\([^)]*\\)\\s*\\}\\}', 'SELFHEALING_DEV.BRONZE.' || rec.ARTIFACT_NAME);
            v_dev_sql := REGEXP_REPLACE(v_dev_sql, '\\{\\{\\s*ref\\(''([^'']+)''\\)\\s*\\}\\}', 'SELFHEALING_DEV.\\1');

            -- Wrap as CREATE OR REPLACE
            LET v_mat VARCHAR := CASE WHEN rec.ARTIFACT_SCHEMA = 'GOLD' THEN 'TABLE' ELSE 'VIEW' END;
            LET v_full_sql VARCHAR := 'CREATE OR REPLACE ' || v_mat || ' SELFHEALING_DEV.' || rec.ARTIFACT_SCHEMA || '.' || rec.ARTIFACT_NAME || ' AS ' || v_dev_sql;

            EXECUTE IMMEDIATE :v_full_sql;

            UPDATE GENERATED_CODE SET TEST_STATUS = 'PASS' WHERE EVENT_ID = :p_event_id AND ARTIFACT_NAME = rec.ARTIFACT_NAME;
            v_pass := v_pass + 1;
        EXCEPTION
            WHEN OTHER THEN
                UPDATE GENERATED_CODE SET TEST_STATUS = 'FAIL', TEST_ERROR = SQLERRM
                WHERE EVENT_ID = :p_event_id AND ARTIFACT_NAME = rec.ARTIFACT_NAME;
                v_fail := v_fail + 1;
        END;
    END FOR;

    -- Update event status
    IF (v_fail = 0 AND v_total > 0) THEN
        UPDATE SCHEMA_CHANGE_EVENTS SET PIPELINE_STATUS = 'CI_PASSED' WHERE EVENT_ID = :p_event_id;
    ELSEIF (v_fail > 0) THEN
        UPDATE SCHEMA_CHANGE_EVENTS SET PIPELINE_STATUS = 'CI_FAILED' WHERE EVENT_ID = :p_event_id;
    END IF;

    RETURN 'Validation complete — PASS: ' || v_pass || ', FAIL: ' || v_fail || ', TOTAL: ' || v_total;
END;
$$;
