-- =============================================================================
-- 05_github_eai.sql — External Access Integration for GitHub PR automation
-- =============================================================================

USE SCHEMA SELFHEALING_PROD.CONFIG;

-- Step 1: Create network rule for GitHub API
CREATE OR REPLACE NETWORK RULE GITHUB_API_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.github.com:443');

-- Step 2: Create secret for GitHub PAT (replace with your PAT)
-- NOTE: Run this separately with your actual PAT:
-- CREATE OR REPLACE SECRET GITHUB_PAT_SECRET
--     TYPE = GENERIC_STRING
--     SECRET_STRING = 'ghp_YOUR_PAT_HERE';

-- Step 3: Create External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GITHUB_EAI
    ALLOWED_NETWORK_RULES = (GITHUB_API_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (GITHUB_PAT_SECRET)
    ENABLED = TRUE;

-- Step 4: Stored procedure to open a GitHub PR
CREATE OR REPLACE PROCEDURE SP_OPEN_GITHUB_PR(
    p_event_id VARCHAR,
    p_repo VARCHAR,
    p_branch_name VARCHAR,
    p_title VARCHAR,
    p_body VARCHAR
)
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
import json

def open_pr(session, p_event_id, p_repo, p_branch_name, p_title, p_body):
    pat = _snowflake.get_generic_secret_string('github_pat')
    headers = {
        'Authorization': f'Bearer {pat}',
        'Accept': 'application/vnd.github.v3+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }
    base_url = f'https://api.github.com/repos/{p_repo}'

    # 1. Get default branch SHA
    resp = requests.get(f'{base_url}/git/ref/heads/main', headers=headers)
    if resp.status_code != 200:
        return f'ERROR: Could not get main branch: {resp.text}'
    main_sha = resp.json()['object']['sha']

    # 2. Create branch
    resp = requests.post(f'{base_url}/git/refs', headers=headers, json={
        'ref': f'refs/heads/{p_branch_name}',
        'sha': main_sha
    })
    if resp.status_code not in (201, 422):  # 422 = branch exists
        return f'ERROR: Could not create branch: {resp.text}'

    # 3. Get generated code for this event and commit each file
    code_rows = session.sql(f"""
        SELECT ARTIFACT_NAME, FILE_PATH, GENERATED_SQL
        FROM SELFHEALING_PROD.CONFIG.GENERATED_CODE
        WHERE EVENT_ID = '{p_event_id}'
    """).collect()

    for row in code_rows:
        file_path = row['FILE_PATH']
        content = row['GENERATED_SQL']

        # Check if file exists (to get SHA for update)
        file_resp = requests.get(f'{base_url}/contents/{file_path}?ref={p_branch_name}', headers=headers)
        
        payload = {
            'message': f'self-heal: regenerate {row["ARTIFACT_NAME"]} for event {p_event_id}',
            'content': __import__('base64').b64encode(content.encode()).decode(),
            'branch': p_branch_name
        }
        if file_resp.status_code == 200:
            payload['sha'] = file_resp.json()['sha']

        resp = requests.put(f'{base_url}/contents/{file_path}', headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            return f'ERROR: Could not commit {file_path}: {resp.text}'

    # 4. Open PR
    pr_resp = requests.post(f'{base_url}/pulls', headers=headers, json={
        'title': p_title,
        'body': p_body,
        'head': p_branch_name,
        'base': 'main'
    })
    if pr_resp.status_code == 201:
        pr_url = pr_resp.json()['html_url']
        session.sql(f"""
            UPDATE SELFHEALING_PROD.CONFIG.SCHEMA_CHANGE_EVENTS
            SET PIPELINE_STATUS = 'PR_OPEN', PR_URL = '{pr_url}', BRANCH_NAME = '{p_branch_name}'
            WHERE EVENT_ID = '{p_event_id}'
        """).collect()
        return f'PR opened: {pr_url}'
    else:
        return f'ERROR: Could not open PR: {pr_resp.text}'
$$;

-- Step 5: Procedure to post CI results as PR comment
CREATE OR REPLACE PROCEDURE SP_POST_PR_COMMENT(
    p_repo VARCHAR,
    p_pr_url VARCHAR,
    p_comment VARCHAR
)
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

def post_comment(session, p_repo, p_pr_url, p_comment):
    pat = _snowflake.get_generic_secret_string('github_pat')
    headers = {
        'Authorization': f'Bearer {pat}',
        'Accept': 'application/vnd.github.v3+json'
    }
    # Extract PR number from URL
    pr_number = p_pr_url.rstrip('/').split('/')[-1]
    url = f'https://api.github.com/repos/{p_repo}/issues/{pr_number}/comments'
    
    resp = requests.post(url, headers=headers, json={'body': p_comment})
    if resp.status_code == 201:
        return 'Comment posted'
    return f'ERROR: {resp.text}'
$$;
