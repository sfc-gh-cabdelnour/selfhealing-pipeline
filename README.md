# Self-Healing Data Pipeline with Snowflake Cortex AI

A Snowflake-native self-healing data pipeline that automatically detects schema drift and data quality issues, diagnoses root causes using Cortex AI, regenerates impacted dbt models, and opens GitHub pull requests for human approval.

## Architecture

```
Source Change (DDL or file upload)
    │
    ▼
┌─────────────────────────────────────┐
│  SCHEMA_DRIFT_DETECTOR              │  ← detects schema changes
│  + PIPELINE_HEALTH_CHECK            │  ← detects data quality issues
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│  HALT GATE                          │  ← blocks pipeline on CRITICAL
│  SCHEMA_CHANGE_EVENTS              │
│  PIPELINE_HEALTH_AUDIT             │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│  FN_DIAGNOSE_FAILURE                │  ← explains WHY (Cortex AI)
│  GET_IMPACTED_ARTIFACTS             │  ← shows WHAT (lineage graph)
│  GENERATE_ARTIFACT_CODE             │  ← proposes FIX (Cortex LLM)
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│  GitHub PR via EAI                  │  ← PR with:
│  - Root cause + confidence          │     - diagnosis
│  - Regenerated SQL                  │     - code fix
│  - dbt run pass/fail                │     - test proof
└─────────────────┬───────────────────┘
                  │
                  ▼
        HUMAN REVIEWS & MERGES
```

## Key Capabilities

- **Schema drift detection**: Column add/drop/type change via INFORMATION_SCHEMA vs baseline
- **Data quality health checks**: Null FK rates, orphan keys, duplicate surrogate keys, path coverage
- **Halt gate**: Blocks pipeline on unresolved CRITICAL findings
- **AI root cause analysis**: Structured diagnosis with confidence scoring (claude-4-sonnet)
- **Transitive lineage**: Recursive CTE finds all downstream models affected
- **Automated code regeneration**: LLM regenerates dbt SQL for impacted models
- **PR-first GitOps**: Every change opens a PR before testing, human gates merge
- **Zero-copy clone validation**: Tests generated code against cloned production data

## Prerequisites

- Snowflake account with External Access Integration support
- GitHub Personal Access Token
- Snowflake CLI (`snow`) installed
- dbt-snowflake (for local development)

## Quick Start

```bash
# 1. Set environment variables
export SNOWFLAKE_CONNECTION=<your-connection>
export GITHUB_PAT=<your-github-pat>
export GITHUB_REPO=sfc-gh-cabdelnour/selfhealing-pipeline

# 2. Deploy foundation
snow sql -c $SNOWFLAKE_CONNECTION -f config/01_foundation.sql
snow sql -c $SNOWFLAKE_CONNECTION -f config/02_detection.sql
snow sql -c $SNOWFLAKE_CONNECTION -f config/03_diagnosis.sql
snow sql -c $SNOWFLAKE_CONNECTION -f config/04_lineage_codegen.sql
snow sql -c $SNOWFLAKE_CONNECTION -f config/05_github_eai.sql
snow sql -c $SNOWFLAKE_CONNECTION -f config/06_validation.sql
snow sql -c $SNOWFLAKE_CONNECTION -f config/07_pipeline_tasks.sql

# 3. Seed demo data
snow sql -c $SNOWFLAKE_CONNECTION -f config/08_seed_demo.sql

# 4. Deploy dbt project
snow dbt deploy SELFHEALING_PROD.CONFIG.SELFHEALING --source ./dbt

# 5. Simulate a schema change and watch it heal
snow sql -c $SNOWFLAKE_CONNECTION -q "ALTER TABLE SELFHEALING_PROD.BRONZE.ORDERS ADD COLUMN discount_code VARCHAR;"
snow sql -c $SNOWFLAKE_CONNECTION -q "CALL SELFHEALING_PROD.CONFIG.RUN_PIPELINE();"
```

## Demo Scenario

The demo uses a simple e-commerce model:
- `BRONZE.customers` / `BRONZE.orders` / `BRONZE.order_items`
- `SILVER.customers` / `SILVER.orders` / `SILVER.order_items`
- `GOLD.orders_daily` / `GOLD.category_summary`

Simulated drift scenarios:
1. **Column added**: `discount_code` added to ORDERS
2. **Column dropped**: `currency` removed from ORDERS
3. **Type changed**: `user_id` changed from INT to VARCHAR

## Credits

- Detection + lineage + code gen pattern inspired by [Tim Buchhorn's self-healing demo](https://github.com/sfc-gh-tbuchhorn/selfhealing-demo)
- Health checks + AI diagnosis + halt gate design by Christian Abdel-Nour (Snowflake Solutions Delivery)
