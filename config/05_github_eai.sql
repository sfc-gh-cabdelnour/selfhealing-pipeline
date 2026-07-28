-- =============================================================================
-- 05_github_eai.sql — External Access Integration for GitHub PR automation (hardened)
-- Parameterized queries, error handling, no string interpolation of untrusted input
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Network rule for GitHub API
CREATE OR REPLACE NETWORK RULE GITHUB_API_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.github.com:443');

-- Secret for GitHub PAT (run separately with your actual PAT):
-- CREATE OR REPLACE SECRET GITHUB_PAT_SECRET TYPE = GENERIC_STRING SECRET_STRING = 'ghp_...';

-- External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_EAI
    ALLOWED_NETWORK_RULES = (GITHUB_API_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (GITHUB_PAT_SECRET)
    ENABLED = TRUE;

-- Procedure: Open GitHub PR (parameterized, error-safe)
CREATE OR REPLACE PROCEDURE SP_OPEN_GITHUB_PR(P_EVENT_ID VARCHAR, P_REPO VARCHAR, P_BRANCH VARCHAR, P_TITLE VARCHAR, P_BODY VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'open_pr'
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_EAI)
SECRETS = ('github_pat' = GITHUB_PAT_SECRET)
AS
$$
import _snowflake
import requests
import base64
from snowflake.snowpark.functions import col

def open_pr(session, P_EVENT_ID, P_REPO, P_BRANCH, P_TITLE, P_BODY):
    pat = _snowflake.get_generic_secret_string('github_pat')
    headers = {
        'Authorization': f'Bearer {pat}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    base_url = f'https://api.github.com/repos/{P_REPO}'

    # 1. Get default branch SHA
    resp = requests.get(f'{base_url}/git/ref/heads/main', headers=headers, timeout=30)
    if resp.status_code != 200:
        return f'ERROR getting main: {resp.status_code} {resp.text[:200]}'
    main_sha = resp.json()['object']['sha']

    # 2. Create branch (422 = already exists, acceptable)
    resp = requests.post(f'{base_url}/git/refs', headers=headers, timeout=30, json={
        'ref': f'refs/heads/{P_BRANCH}',
        'sha': main_sha
    })
    if resp.status_code not in (201, 422):
        return f'ERROR creating branch: {resp.status_code} {resp.text[:200]}'

    # 3. Get generated code (parameterized via Snowpark)
    code_df = session.table("SELFHEALING_PROD.CONFIG.GENERATED_CODE").filter(col("EVENT_ID") == P_EVENT_ID)
    code_rows = code_df.select(col("ARTIFACT_NAME"), col("FILE_PATH"), col("GENERATED_SQL")).collect()

    committed = 0
    for row in code_rows:
        file_path = row['FILE_PATH']
        content = row['GENERATED_SQL'] or ''
        if not content or content.startswith('-- ERROR'):
            continue

        # Check if file exists on branch (for SHA to update)
        file_resp = requests.get(f'{base_url}/contents/{file_path}?ref={P_BRANCH}', headers=headers, timeout=30)
        payload = {
            'message': f'self-heal: regenerate {row["ARTIFACT_NAME"]}',
            'content': base64.b64encode(content.encode()).decode(),
            'branch': P_BRANCH
        }
        if file_resp.status_code == 200:
            payload['sha'] = file_resp.json()['sha']

        resp = requests.put(f'{base_url}/contents/{file_path}', headers=headers, timeout=30, json=payload)
        if resp.status_code in (200, 201):
            committed += 1
        else:
            return f'ERROR committing {file_path}: {resp.status_code} {resp.text[:200]}'

    if committed == 0:
        return 'WARNING: No files committed (all generated code had errors)'

    # 4. Open PR
    pr_resp = requests.post(f'{base_url}/pulls', headers=headers, timeout=30, json={
        'title': P_TITLE,
        'body': P_BODY,
        'head': P_BRANCH,
        'base': 'main'
    })

    if pr_resp.status_code == 201:
        pr_url = pr_resp.json()['html_url']
        # Update event (parameterized via Snowpark)
        session.table("SELFHEALING_PROD.CONFIG.SCHEMA_CHANGE_EVENTS").filter(
            col("EVENT_ID") == P_EVENT_ID
        ).update({"PIPELINE_STATUS": "PR_OPEN", "PR_URL": pr_url, "BRANCH_NAME": P_BRANCH})
        return f'PR opened: {pr_url} ({committed} files committed)'
    elif pr_resp.status_code == 422:
        return f'PR already exists for branch {P_BRANCH}'
    else:
        return f'ERROR opening PR: {pr_resp.status_code} {pr_resp.text[:200]}'
$$;

-- Procedure: Post CI results as PR comment (parameterized)
CREATE OR REPLACE PROCEDURE SP_POST_PR_COMMENT(P_REPO VARCHAR, P_PR_URL VARCHAR, P_COMMENT VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'post_comment'
EXTERNAL_ACCESS_INTEGRATIONS = (GITHUB_EAI)
SECRETS = ('github_pat' = GITHUB_PAT_SECRET)
AS
$$
import _snowflake
import requests

def post_comment(session, P_REPO, P_PR_URL, P_COMMENT):
    pat = _snowflake.get_generic_secret_string('github_pat')
    headers = {'Authorization': f'Bearer {pat}', 'Accept': 'application/vnd.github.v3+json'}
    pr_number = P_PR_URL.rstrip('/').split('/')[-1]
    url = f'https://api.github.com/repos/{P_REPO}/issues/{pr_number}/comments'
    resp = requests.post(url, headers=headers, timeout=30, json={'body': P_COMMENT})
    return 'Comment posted' if resp.status_code == 201 else f'ERROR: {resp.status_code} {resp.text[:200]}'
$$;
