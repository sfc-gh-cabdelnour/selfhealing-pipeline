-- =============================================================================
-- 03_diagnosis.sql — AI root cause analysis + halt gate (hardened)
-- TRY_PARSE_JSON fallback, error handling, run logging
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Function: AI-powered root cause diagnosis with safe JSON parsing
CREATE OR REPLACE FUNCTION FN_DIAGNOSE_FAILURE(p_check_type VARCHAR, p_context VARCHAR, p_metrics VARIANT)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    COALESCE(
        TRY_PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE(
            'claude-4-sonnet',
            'You are a senior data engineer diagnosing a data pipeline issue in Snowflake.

ISSUE TYPE: ' || p_check_type || '
CONTEXT: ' || p_context || '
METRICS: ' || p_metrics::VARCHAR || '

Respond with ONLY a JSON object (no markdown, no explanation):
{
  "root_cause": "concise explanation of the most likely root cause",
  "confidence": 0.85,
  "severity": "CRITICAL",
  "remediation_steps": ["step 1", "step 2"],
  "requires_human_review": true,
  "estimated_impact": "description of downstream impact if unresolved"
}'
        )),
        -- Fallback if LLM returns malformed JSON
        OBJECT_CONSTRUCT(
            'root_cause', 'AI diagnosis returned malformed response — manual investigation required',
            'confidence', 0.0,
            'severity', 'WARNING',
            'remediation_steps', ARRAY_CONSTRUCT('Check PIPELINE_HEALTH_AUDIT for raw context', 'Investigate manually'),
            'requires_human_review', TRUE,
            'estimated_impact', 'Unknown — diagnosis failed'
        )
    )
$$;

-- Procedure: Halt gate — checks for unresolved CRITICAL findings before allowing pipeline
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
    SELECT COUNT(*) INTO v_critical_count FROM PIPELINE_HEALTH_AUDIT WHERE SEVERITY = 'CRITICAL' AND STATUS = 'OPEN';
    SELECT COUNT(*) INTO v_pending_events FROM SCHEMA_CHANGE_EVENTS WHERE PIPELINE_STATUS = 'PENDING';
    IF (v_critical_count > 0 OR v_pending_events > 0) THEN
        RETURN 'HALTED — ' || v_critical_count || ' critical findings, ' || v_pending_events || ' pending schema events. Pipeline blocked.';
    END IF;
    RETURN 'CLEAR — no blocking issues. Pipeline may proceed.';
END;
$$;

-- Procedure: Diagnose all pending events (idempotent — skips already-diagnosed)
CREATE OR REPLACE PROCEDURE SP_DIAGNOSE_PENDING()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_diagnosed NUMBER := 0;
BEGIN
    -- Diagnose schema change events that have no corresponding health audit entry
    FOR rec IN (
        SELECT EVENT_ID, CHANGE_TYPE, TABLE_NAME, COLUMN_NAME, OLD_DATA_TYPE, NEW_DATA_TYPE
        FROM SCHEMA_CHANGE_EVENTS
        WHERE PIPELINE_STATUS = 'PENDING'
          AND NOT EXISTS (
              SELECT 1 FROM PIPELINE_HEALTH_AUDIT h
              WHERE h.TABLE_NAME = SCHEMA_CHANGE_EVENTS.TABLE_NAME
                AND h.COLUMN_NAME = SCHEMA_CHANGE_EVENTS.COLUMN_NAME
                AND h.CHECK_TYPE = 'SCHEMA_DRIFT'
                AND h.STATUS = 'OPEN'
          )
    ) DO
        LET v_event_id VARCHAR := rec.EVENT_ID;
        LET v_context VARCHAR := rec.CHANGE_TYPE || ' on ' || rec.TABLE_NAME || '.' || COALESCE(rec.COLUMN_NAME, '*');
        LET v_metrics VARIANT := OBJECT_CONSTRUCT(
            'change_type', rec.CHANGE_TYPE, 'table', rec.TABLE_NAME,
            'column', rec.COLUMN_NAME, 'old_type', rec.OLD_DATA_TYPE, 'new_type', rec.NEW_DATA_TYPE
        );

        INSERT INTO PIPELINE_HEALTH_AUDIT (CHECK_TYPE, TABLE_NAME, COLUMN_NAME, SEVERITY, METRIC_VALUE, THRESHOLD, DIAGNOSIS)
        SELECT 'SCHEMA_DRIFT', rec.TABLE_NAME, rec.COLUMN_NAME, 'CRITICAL', 1.0, 0.0,
               FN_DIAGNOSE_FAILURE('SCHEMA_DRIFT', :v_context, :v_metrics);

        v_diagnosed := v_diagnosed + 1;
    END FOR;

    RETURN 'Diagnosed ' || v_diagnosed || ' issues';
END;
$$;
