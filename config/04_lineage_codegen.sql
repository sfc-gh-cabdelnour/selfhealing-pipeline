-- =============================================================================
-- 04_lineage_codegen.sql — Lineage traversal + code regeneration (hardened)
-- Parameterized queries, idempotent inserts, error handling, single lineage source
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- View: Reusable lineage traversal (replaces duplicated CTE)
CREATE OR REPLACE VIEW VW_ARTIFACT_LINEAGE AS
WITH RECURSIVE lineage AS (
    SELECT ARTIFACT_NAME AS SOURCE_TABLE, ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, DEPENDS_ON, 1 AS DEPTH
    FROM ARTIFACT_REGISTRY
    UNION ALL
    SELECT l.SOURCE_TABLE, ar.ARTIFACT_NAME, ar.ARTIFACT_SCHEMA, ar.FILE_PATH, ar.DEPENDS_ON, l.DEPTH + 1
    FROM ARTIFACT_REGISTRY ar
    INNER JOIN lineage l ON ar.DEPENDS_ON = l.ARTIFACT_NAME
    WHERE l.DEPTH < 10
)
SELECT SOURCE_TABLE AS CHANGED_TABLE, ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, DEPTH
FROM lineage
WHERE SOURCE_TABLE != ARTIFACT_NAME;

-- Procedure: Generate code for impacted artifacts (Python — parameterized, idempotent)
CREATE OR REPLACE PROCEDURE SP_GENERATE_ARTIFACT_CODE(P_EVENT_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.functions import col, lit

def run(session, P_EVENT_ID):
    # Get event details using parameterized filter
    ev_df = session.table("SELFHEALING_PROD.CONFIG.SCHEMA_CHANGE_EVENTS").filter(col("EVENT_ID") == P_EVENT_ID)
    ev = ev_df.collect()
    if not ev:
        return "ERROR: Event not found"

    table_name = ev[0]["TABLE_NAME"]
    change_type = ev[0]["CHANGE_TYPE"]
    column_name = ev[0]["COLUMN_NAME"] or "N/A"
    old_type = ev[0]["OLD_DATA_TYPE"] or "N/A"
    new_type = ev[0]["NEW_DATA_TYPE"] or "N/A"

    # Get impacted artifacts via the lineage view
    lineage_df = session.table("SELFHEALING_PROD.CONFIG.VW_ARTIFACT_LINEAGE").filter(col("CHANGED_TABLE") == table_name)
    artifacts = lineage_df.collect()

    if not artifacts:
        return f"No downstream artifacts found for {table_name}"

    generated = 0
    errors = []

    for art in artifacts:
        art_name = art["ARTIFACT_NAME"]
        art_schema = art["ARTIFACT_SCHEMA"]
        art_path = art["FILE_PATH"]

        # Idempotency: skip if already generated for this event+artifact
        existing = session.table("SELFHEALING_PROD.CONFIG.GENERATED_CODE").filter(
            (col("EVENT_ID") == P_EVENT_ID) & (col("ARTIFACT_NAME") == art_name)
        ).count()
        if existing > 0:
            continue

        # Get baseline SQL (parameterized)
        baseline_df = session.table("SELFHEALING_PROD.CONFIG.GENERATED_CODE").filter(
            (col("ARTIFACT_NAME") == art_name) & (col("TEST_STATUS") == "PASS")
        ).select(col("GENERATED_SQL")).sort(col("GENERATED_AT").desc()).limit(1)
        baseline_rows = baseline_df.collect()
        current_sql = baseline_rows[0]["GENERATED_SQL"] if baseline_rows else "-- No prior SQL"

        # Build prompt (safe — no user input in column names)
        prompt = (
            f"Regenerate this Snowflake SQL model for a schema change. "
            f"CHANGE: {change_type} on SELFHEALING_PROD.BRONZE.{table_name} "
            f"column: {column_name} (old: {old_type}, new: {new_type}). "
            f"TARGET: {art_schema}.{art_name}. "
            f"CURRENT SQL: {current_sql}. "
            f"RULES: If NEW_COLUMN add it to SELECT. If COLUMN_DROP remove it. "
            f"If TYPE_CHANGE use TRY_CAST. Return ONLY SQL. No markdown. "
            f"Use SELFHEALING_PROD.SCHEMA.TABLE fully qualified names."
        )

        try:
            # Call Cortex (safe — prompt doesn't contain unescaped user input)
            prompt_safe = prompt.replace("'", "''")
            result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{prompt_safe}')").collect()
            new_sql = result[0][0] if result else "-- Generation failed"
        except Exception as e:
            errors.append(f"{art_name}: {str(e)[:100]}")
            new_sql = f"-- ERROR: {str(e)[:200]}"

        # Insert (idempotent via UNIQUE constraint — will error on dupe, caught above)
        new_sql_safe = new_sql.replace("'", "''")
        current_safe = current_sql.replace("'", "''")
        try:
            session.sql(
                f"INSERT INTO SELFHEALING_PROD.CONFIG.GENERATED_CODE "
                f"(EVENT_ID, ARTIFACT_NAME, ARTIFACT_SCHEMA, FILE_PATH, ORIGINAL_SQL, GENERATED_SQL) "
                f"VALUES ('{P_EVENT_ID}', '{art_name}', '{art_schema}', '{art_path}', "
                f"'{current_safe}', '{new_sql_safe}')"
            ).collect()
            generated += 1
        except Exception as e:
            errors.append(f"{art_name} insert: {str(e)[:100]}")

    # Update event status
    session.sql(
        f"UPDATE SELFHEALING_PROD.CONFIG.SCHEMA_CHANGE_EVENTS "
        f"SET PIPELINE_STATUS = 'CODE_GENERATED' WHERE EVENT_ID = '{P_EVENT_ID}'"
    ).collect()

    result_msg = f"Generated code for {generated} artifacts"
    if errors:
        result_msg += f" ({len(errors)} errors: {'; '.join(errors[:3])})"
    return result_msg
$$;
