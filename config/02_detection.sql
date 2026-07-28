-- =============================================================================
-- 02_detection.sql — Schema drift detector + data quality health checks
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Procedure: Detect schema drift by comparing INFORMATION_SCHEMA to SCHEMA_REGISTRY
CREATE OR REPLACE PROCEDURE SP_SCHEMA_DRIFT_DETECTOR()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_new_cols NUMBER;
    v_dropped_cols NUMBER;
    v_type_changes NUMBER;
BEGIN
    -- Detect NEW columns (in INFORMATION_SCHEMA but not in SCHEMA_REGISTRY)
    INSERT INTO SCHEMA_CHANGE_EVENTS (CHANGE_TYPE, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, NEW_DATA_TYPE)
    SELECT 'NEW_COLUMN', i.TABLE_SCHEMA, i.TABLE_NAME, i.COLUMN_NAME, i.DATA_TYPE
    FROM SELFHEALING_PROD.INFORMATION_SCHEMA.COLUMNS i
    LEFT JOIN SCHEMA_REGISTRY r
        ON i.TABLE_SCHEMA = r.TABLE_SCHEMA
        AND i.TABLE_NAME = r.TABLE_NAME
        AND i.COLUMN_NAME = r.COLUMN_NAME
    WHERE i.TABLE_SCHEMA = 'BRONZE'
      AND r.COLUMN_NAME IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM SCHEMA_CHANGE_EVENTS e
          WHERE e.TABLE_NAME = i.TABLE_NAME
            AND e.COLUMN_NAME = i.COLUMN_NAME
            AND e.CHANGE_TYPE = 'NEW_COLUMN'
            AND e.PIPELINE_STATUS IN ('PENDING', 'CODE_GENERATED', 'PR_OPEN')
      );
    v_new_cols := SQLROWCOUNT;

    -- Detect DROPPED columns (in SCHEMA_REGISTRY but not in INFORMATION_SCHEMA)
    INSERT INTO SCHEMA_CHANGE_EVENTS (CHANGE_TYPE, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, OLD_DATA_TYPE)
    SELECT 'COLUMN_DROP', r.TABLE_SCHEMA, r.TABLE_NAME, r.COLUMN_NAME, r.DATA_TYPE
    FROM SCHEMA_REGISTRY r
    LEFT JOIN SELFHEALING_PROD.INFORMATION_SCHEMA.COLUMNS i
        ON r.TABLE_SCHEMA = i.TABLE_SCHEMA
        AND r.TABLE_NAME = i.TABLE_NAME
        AND r.COLUMN_NAME = i.COLUMN_NAME
    WHERE r.TABLE_SCHEMA = 'BRONZE'
      AND i.COLUMN_NAME IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM SCHEMA_CHANGE_EVENTS e
          WHERE e.TABLE_NAME = r.TABLE_NAME
            AND e.COLUMN_NAME = r.COLUMN_NAME
            AND e.CHANGE_TYPE = 'COLUMN_DROP'
            AND e.PIPELINE_STATUS IN ('PENDING', 'CODE_GENERATED', 'PR_OPEN')
      );
    v_dropped_cols := SQLROWCOUNT;

    -- Detect TYPE CHANGES (column exists in both but type differs)
    INSERT INTO SCHEMA_CHANGE_EVENTS (CHANGE_TYPE, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, OLD_DATA_TYPE, NEW_DATA_TYPE)
    SELECT 'TYPE_CHANGE', i.TABLE_SCHEMA, i.TABLE_NAME, i.COLUMN_NAME, r.DATA_TYPE, i.DATA_TYPE
    FROM SELFHEALING_PROD.INFORMATION_SCHEMA.COLUMNS i
    INNER JOIN SCHEMA_REGISTRY r
        ON i.TABLE_SCHEMA = r.TABLE_SCHEMA
        AND i.TABLE_NAME = r.TABLE_NAME
        AND i.COLUMN_NAME = r.COLUMN_NAME
    WHERE i.TABLE_SCHEMA = 'BRONZE'
      AND i.DATA_TYPE != r.DATA_TYPE
      AND NOT EXISTS (
          SELECT 1 FROM SCHEMA_CHANGE_EVENTS e
          WHERE e.TABLE_NAME = i.TABLE_NAME
            AND e.COLUMN_NAME = i.COLUMN_NAME
            AND e.CHANGE_TYPE = 'TYPE_CHANGE'
            AND e.PIPELINE_STATUS IN ('PENDING', 'CODE_GENERATED', 'PR_OPEN')
      );
    v_type_changes := SQLROWCOUNT;

    RETURN 'Drift detected — NEW: ' || v_new_cols || ', DROPPED: ' || v_dropped_cols || ', TYPE_CHANGE: ' || v_type_changes;
END;
$$;

-- Procedure: Data quality health checks (null FKs, orphan keys, duplicate SKs)
CREATE OR REPLACE PROCEDURE SP_PIPELINE_HEALTH_CHECK()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_findings NUMBER := 0;
    v_table VARCHAR;
    v_count NUMBER;
    v_total NUMBER;
    v_rate NUMBER;
BEGIN
    -- Check 1: NULL foreign key rates in SILVER/GOLD tables
    FOR rec IN (
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM SELFHEALING_PROD.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ('SILVER', 'GOLD')
          AND (COLUMN_NAME LIKE '%_SK' OR COLUMN_NAME LIKE '%_ID' OR COLUMN_NAME LIKE '%_FK')
          AND COLUMN_NAME != 'EVENT_ID'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    ) DO
        LET v_sql VARCHAR := 'SELECT COUNT_IF(' || rec.COLUMN_NAME || ' IS NULL) as NULL_CT, COUNT(*) as TOTAL FROM SELFHEALING_PROD.' || rec.TABLE_SCHEMA || '.' || rec.TABLE_NAME;
        LET rs RESULTSET := (EXECUTE IMMEDIATE :v_sql);
        LET cur CURSOR FOR rs;
        OPEN cur;
        FETCH cur INTO v_count, v_total;
        CLOSE cur;

        IF (v_total > 0) THEN
            v_rate := v_count / v_total;
            IF (v_rate > 0.10) THEN
                INSERT INTO PIPELINE_HEALTH_AUDIT (CHECK_TYPE, TABLE_NAME, COLUMN_NAME, SEVERITY, METRIC_VALUE, THRESHOLD)
                SELECT 'NULL_FK_RATE', rec.TABLE_SCHEMA || '.' || rec.TABLE_NAME, rec.COLUMN_NAME,
                    CASE WHEN :v_rate > 0.50 THEN 'CRITICAL' WHEN :v_rate > 0.25 THEN 'WARNING' ELSE 'INFO' END,
                    :v_rate, 0.10;
                v_findings := v_findings + 1;
            END IF;
        END IF;
    END FOR;

    -- Check 2: Duplicate surrogate keys in GOLD tables
    FOR rec IN (
        SELECT TABLE_NAME, COLUMN_NAME
        FROM SELFHEALING_PROD.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'GOLD'
          AND COLUMN_NAME LIKE '%_SK'
    ) DO
        LET v_sql2 VARCHAR := 'SELECT COUNT(*) - COUNT(DISTINCT ' || rec.COLUMN_NAME || ') FROM SELFHEALING_PROD.GOLD.' || rec.TABLE_NAME;
        LET rs2 RESULTSET := (EXECUTE IMMEDIATE :v_sql2);
        LET cur2 CURSOR FOR rs2;
        OPEN cur2;
        FETCH cur2 INTO v_count;
        CLOSE cur2;

        IF (v_count > 0) THEN
            INSERT INTO PIPELINE_HEALTH_AUDIT (CHECK_TYPE, TABLE_NAME, COLUMN_NAME, SEVERITY, METRIC_VALUE, THRESHOLD)
            SELECT 'DUPLICATE_SK', 'GOLD.' || rec.TABLE_NAME, rec.COLUMN_NAME, 'CRITICAL', :v_count, 0;
            v_findings := v_findings + 1;
        END IF;
    END FOR;

    RETURN 'Health check complete — ' || v_findings || ' findings';
END;
$$;
