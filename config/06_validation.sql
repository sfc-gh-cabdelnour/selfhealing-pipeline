-- =============================================================================
-- 06_validation.sql — Zero-copy clone + validation (hardened)
-- Parameterized, proper error capture, no Jinja regex hacks
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Procedure: Validate generated code against zero-copy clone of PROD
CREATE OR REPLACE PROCEDURE SP_VALIDATE_GENERATED_CODE(P_EVENT_ID VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.functions import col

def run(session, P_EVENT_ID):
    # Create zero-copy clone
    try:
        session.sql("CREATE OR REPLACE DATABASE SELFHEALING_DEV CLONE SELFHEALING_PROD").collect()
    except Exception as e:
        return f"ERROR cloning: {str(e)[:200]}"

    # Get generated code (parameterized)
    code_df = session.table("SELFHEALING_PROD.CONFIG.GENERATED_CODE").filter(
        col("EVENT_ID") == P_EVENT_ID
    ).select(
        col("ARTIFACT_NAME"), col("ARTIFACT_SCHEMA"), col("GENERATED_SQL")
    ).sort(col("ARTIFACT_SCHEMA"))  # SILVER before GOLD

    rows = code_df.collect()
    if not rows:
        return "No generated code found for this event"

    passed = 0
    failed = 0
    results = []

    for row in rows:
        art_name = row["ARTIFACT_NAME"]
        art_schema = row["ARTIFACT_SCHEMA"] or "SILVER"
        gen_sql = row["GENERATED_SQL"] or ""

        if not gen_sql or gen_sql.startswith("-- ERROR") or gen_sql.startswith("-- No prior"):
            results.append(f"SKIP {art_name}: no valid SQL")
            continue

        # Replace PROD references with DEV for safe testing
        dev_sql = gen_sql.replace("SELFHEALING_PROD", "SELFHEALING_DEV")
        mat = "TABLE" if art_schema == "GOLD" else "VIEW"
        full_sql = f"CREATE OR REPLACE {mat} SELFHEALING_DEV.{art_schema}.{art_name} AS {dev_sql}"

        try:
            session.sql(full_sql).collect()
            # Update status (parameterized)
            session.table("SELFHEALING_PROD.CONFIG.GENERATED_CODE").filter(
                (col("EVENT_ID") == P_EVENT_ID) & (col("ARTIFACT_NAME") == art_name)
            ).update({"TEST_STATUS": "PASS"})
            passed += 1
            results.append(f"PASS {art_name}")
        except Exception as e:
            err_msg = str(e)[:200]
            session.table("SELFHEALING_PROD.CONFIG.GENERATED_CODE").filter(
                (col("EVENT_ID") == P_EVENT_ID) & (col("ARTIFACT_NAME") == art_name)
            ).update({"TEST_STATUS": "FAIL", "TEST_ERROR": err_msg})
            failed += 1
            results.append(f"FAIL {art_name}: {err_msg[:80]}")

    # Update event status
    new_status = "CI_PASSED" if failed == 0 else "CI_FAILED"
    session.table("SELFHEALING_PROD.CONFIG.SCHEMA_CHANGE_EVENTS").filter(
        col("EVENT_ID") == P_EVENT_ID
    ).update({"PIPELINE_STATUS": new_status})

    # Cleanup DEV clone (save storage)
    try:
        session.sql("DROP DATABASE IF EXISTS SELFHEALING_DEV").collect()
    except:
        pass

    return f"Validation: PASS={passed} FAIL={failed} | " + " | ".join(results)
$$;
