-- =============================================================================
-- 04_lineage_codegen.sql — Transitive lineage traversal + Cortex code regeneration
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Procedure: Get all impacted downstream artifacts for a given BRONZE table
CREATE OR REPLACE PROCEDURE SP_GET_IMPACTED_ARTIFACTS(p_table_name VARCHAR)
RETURNS TABLE (ARTIFACT_NAME VARCHAR, ARTIFACT_SCHEMA VARCHAR, FILE_PATH VARCHAR, DEPTH NUMBER)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (
        WITH RECURSIVE lineage AS (
            -- Seed: direct dependents of the changed BRONZE table
            SELECT ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, 1 AS DEPTH
            FROM ARTIFACT_REGISTRY
            WHERE DEPENDS_ON = :p_table_name

            UNION ALL

            -- Recurse: dependents of dependents
            SELECT ar.ARTIFACT_NAME, ar.ARTIFACT_SCHEMA, ar.FILE_PATH, l.DEPTH + 1
            FROM ARTIFACT_REGISTRY ar
            INNER JOIN lineage l ON ar.DEPENDS_ON = l.ARTIFACT_NAME
            WHERE l.DEPTH < 10  -- safety limit
        )
        SELECT DISTINCT ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, MIN(DEPTH) AS DEPTH
        FROM lineage
        GROUP BY ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH
        ORDER BY DEPTH, ARTIFACT_SCHEMA, ARTIFACT_NAME
    );
    RETURN TABLE(res);
END;
$$;

-- Procedure: Generate code for all impacted artifacts using Cortex LLM
CREATE OR REPLACE PROCEDURE SP_GENERATE_ARTIFACT_CODE(p_event_id VARCHAR)
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
    v_old_type VARCHAR;
    v_generated NUMBER := 0;
BEGIN
    -- Get event details
    SELECT TABLE_NAME, CHANGE_TYPE, COLUMN_NAME, NEW_DATA_TYPE, OLD_DATA_TYPE
    INTO v_table_name, v_change_type, v_column_name, v_new_type, v_old_type
    FROM SCHEMA_CHANGE_EVENTS
    WHERE EVENT_ID = :p_event_id;

    -- For each impacted artifact, regenerate code
    FOR rec IN (
        WITH RECURSIVE lineage AS (
            SELECT ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, 1 AS DEPTH
            FROM ARTIFACT_REGISTRY WHERE DEPENDS_ON = :v_table_name
            UNION ALL
            SELECT ar.ARTIFACT_NAME, ar.ARTIFACT_SCHEMA, ar.FILE_PATH, l.DEPTH + 1
            FROM ARTIFACT_REGISTRY ar INNER JOIN lineage l ON ar.DEPENDS_ON = l.ARTIFACT_NAME
            WHERE l.DEPTH < 10
        )
        SELECT DISTINCT ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH
        FROM lineage
    ) DO
        -- Get current model SQL from GENERATED_CODE or use a placeholder
        LET v_current_sql VARCHAR := '';
        SELECT COALESCE(MAX(GENERATED_SQL), MAX(ORIGINAL_SQL), '') INTO v_current_sql
        FROM GENERATED_CODE
        WHERE ARTIFACT_NAME = rec.ARTIFACT_NAME;

        -- If no prior code exists, read from the dbt model convention
        IF (v_current_sql = '') THEN
            v_current_sql := '-- Model: ' || rec.ARTIFACT_NAME || ' (no prior SQL found, generating from schema context)';
        END IF;

        -- Generate new SQL using Cortex
        LET v_prompt VARCHAR := 'You are a dbt SQL expert. Regenerate this Snowflake SQL model to accommodate a schema change.

SCHEMA CHANGE:
- Type: ' || :v_change_type || '
- Table: SELFHEALING_PROD.BRONZE.' || :v_table_name || '
- Column: ' || COALESCE(:v_column_name, 'N/A') || '
- Old data type: ' || COALESCE(:v_old_type, 'N/A') || '
- New data type: ' || COALESCE(:v_new_type, 'N/A') || '

TARGET MODEL: ' || rec.ARTIFACT_SCHEMA || '.' || rec.ARTIFACT_NAME || '
FILE: ' || rec.FILE_PATH || '

CURRENT SQL:
' || v_current_sql || '

INSTRUCTIONS:
1. If a NEW_COLUMN was added upstream, add it to this model SELECT list in the appropriate position.
2. If a COLUMN_DROP occurred, remove the column reference and handle any dependent calculations.
3. If a TYPE_CHANGE occurred, add appropriate CAST or TRY_CAST to maintain compatibility.
4. Preserve the existing model structure, joins, and aggregation logic.
5. Return ONLY the complete SQL statement — no explanation, no markdown fences.
6. Use {{ config(materialized=''table'') }} for GOLD models, {{ config(materialized=''view'') }} for SILVER models.
7. Use {{ source(''bronze'', ''TABLE_NAME'') }} for BRONZE references, {{ ref(''model_name'') }} for model-to-model refs.';

        LET v_new_sql VARCHAR := SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', v_prompt);

        -- Store generated code
        INSERT INTO GENERATED_CODE (EVENT_ID, ARTIFACT_NAME, FILE_PATH, ORIGINAL_SQL, GENERATED_SQL)
        SELECT :p_event_id, rec.ARTIFACT_NAME, rec.FILE_PATH, :v_current_sql, :v_new_sql;

        v_generated := v_generated + 1;
    END FOR;

    -- Update event status
    UPDATE SCHEMA_CHANGE_EVENTS SET PIPELINE_STATUS = 'CODE_GENERATED' WHERE EVENT_ID = :p_event_id;

    RETURN 'Generated code for ' || v_generated || ' impacted artifacts';
END;
$$;
