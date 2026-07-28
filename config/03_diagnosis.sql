-- =============================================================================
-- 03_diagnosis.sql — AI root cause analysis + halt gate
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Function: AI-powered root cause diagnosis using Cortex
CREATE OR REPLACE FUNCTION FN_DIAGNOSE_FAILURE(
    p_check_type VARCHAR,
    p_context VARCHAR,
    p_metrics VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE(
        'claude-4-sonnet',
        'You are a senior data engineer diagnosing a data pipeline issue in Snowflake.

ISSUE TYPE: ' || p_check_type || '
CONTEXT: ' || p_context || '
METRICS: ' || p_metrics::VARCHAR || '

Respond with a JSON object containing:
{
  "root_cause": "concise explanation of the most likely root cause",
  "confidence": 0.0-1.0,
  "severity": "CRITICAL|WARNING|INFO",
  "remediation_steps": ["step 1", "step 2", ...],
  "requires_human_review": true/false,
  "estimated_impact": "description of downstream impact if unresolved"
}

Be specific to the data engineering context. Reference Snowflake-specific causes (schema evolution, Dynamic Table refresh, staging view drift, source file format changes).'
    ))
$$;

-- Procedure: Halt gate — checks for unresolved CRITICAL findings before allowing pipeline to proceed
CREATE OR REPLACE PROCEDURE SP_HALT_GATE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_critical_count NUMBER;
    v_pending_events NUMBER;
BEGIN
    -- Check for unresolved CRITICAL health findings
    SELECT COUNT(*) INTO v_critical_count
    FROM PIPELINE_HEALTH_AUDIT
    WHERE SEVERITY = 'CRITICAL' AND STATUS = 'OPEN';

    -- Check for pending schema change events
    SELECT COUNT(*) INTO v_pending_events
    FROM SCHEMA_CHANGE_EVENTS
    WHERE PIPELINE_STATUS = 'PENDING';

    IF (v_critical_count > 0 OR v_pending_events > 0) THEN
        RETURN 'HALTED — ' || v_critical_count || ' critical findings, ' || v_pending_events || ' pending schema events. Pipeline blocked until resolved.';
    END IF;

    RETURN 'CLEAR — no blocking issues. Pipeline may proceed.';
END;
$$;

-- Procedure: Diagnose all pending events and health findings
CREATE OR REPLACE PROCEDURE SP_DIAGNOSE_PENDING()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_diagnosed NUMBER := 0;
BEGIN
    -- Diagnose pending schema change events
    FOR rec IN (
        SELECT EVENT_ID, CHANGE_TYPE, TABLE_NAME, COLUMN_NAME, OLD_DATA_TYPE, NEW_DATA_TYPE
        FROM SCHEMA_CHANGE_EVENTS
        WHERE PIPELINE_STATUS = 'PENDING'
    ) DO
        LET v_context VARCHAR := rec.CHANGE_TYPE || ' on ' || rec.TABLE_NAME || '.' || COALESCE(rec.COLUMN_NAME, '*');
        LET v_metrics VARIANT := OBJECT_CONSTRUCT(
            'change_type', rec.CHANGE_TYPE,
            'table', rec.TABLE_NAME,
            'column', rec.COLUMN_NAME,
            'old_type', rec.OLD_DATA_TYPE,
            'new_type', rec.NEW_DATA_TYPE
        );

        -- Store diagnosis in health audit
        INSERT INTO PIPELINE_HEALTH_AUDIT (CHECK_TYPE, TABLE_NAME, COLUMN_NAME, SEVERITY, METRIC_VALUE, THRESHOLD, DIAGNOSIS)
        SELECT 'SCHEMA_DRIFT', rec.TABLE_NAME, rec.COLUMN_NAME, 'CRITICAL', 1.0, 0.0,
               FN_DIAGNOSE_FAILURE('SCHEMA_DRIFT', :v_context, :v_metrics);

        v_diagnosed := v_diagnosed + 1;
    END FOR;

    -- Diagnose undiagnosed health audit findings
    FOR rec IN (
        SELECT AUDIT_ID, CHECK_TYPE, TABLE_NAME, COLUMN_NAME, METRIC_VALUE, THRESHOLD
        FROM PIPELINE_HEALTH_AUDIT
        WHERE DIAGNOSIS IS NULL AND STATUS = 'OPEN'
    ) DO
        LET v_context2 VARCHAR := rec.CHECK_TYPE || ' on ' || rec.TABLE_NAME || '.' || COALESCE(rec.COLUMN_NAME, '*') || ' (value=' || rec.METRIC_VALUE::VARCHAR || ', threshold=' || rec.THRESHOLD::VARCHAR || ')';
        LET v_metrics2 VARIANT := OBJECT_CONSTRUCT(
            'check_type', rec.CHECK_TYPE,
            'table', rec.TABLE_NAME,
            'column', rec.COLUMN_NAME,
            'metric_value', rec.METRIC_VALUE,
            'threshold', rec.THRESHOLD
        );

        UPDATE PIPELINE_HEALTH_AUDIT
        SET DIAGNOSIS = FN_DIAGNOSE_FAILURE(rec.CHECK_TYPE, :v_context2, :v_metrics2)
        WHERE AUDIT_ID = rec.AUDIT_ID;

        v_diagnosed := v_diagnosed + 1;
    END FOR;

    RETURN 'Diagnosed ' || v_diagnosed || ' issues';
END;
$$;
