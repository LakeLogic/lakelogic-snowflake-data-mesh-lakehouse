-- ============================================================================
-- RideFlow on Snowflake — one-time bootstrap (run in a Snowsight worksheet)
-- ============================================================================
-- This is the ONLY step that needs your GitHub PAT: Snowflake must authenticate to
-- read the private repo. It creates the database, warehouse, and Git repository, then
-- runs sql/setup.sql straight from the repo to flesh out everything else (compute pool,
-- staged wheelhouse, Notebooks, Task DAG) — no laptop / Python required.
--
-- Steps:
--   1. Run as a role that can create databases/warehouses/integrations (e.g. ACCOUNTADMIN).
--   2. Paste your GitHub username + a fine-grained PAT (Contents: Read on the repo) below.
--   3. Run all. Then watch Monitoring -> Task History (or the T_RIDEFLOW_MARKETPLACE Graph tab).
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS RIDEFLOW_DEV_DEMO;
CREATE WAREHOUSE IF NOT EXISTS RIDEFLOW_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

USE DATABASE RIDEFLOW_DEV_DEMO;
CREATE SCHEMA IF NOT EXISTS PIPELINE;
USE SCHEMA PIPELINE;

-- >>> PASTE YOUR CREDENTIALS HERE <<< ----------------------------------------
CREATE OR REPLACE SECRET GIT_PAT
  TYPE = PASSWORD
  USERNAME = 'your-github-username'
  PASSWORD = 'ghp_or_github_pat_xxxxxxxxxxxxxxxx';
-- ----------------------------------------------------------------------------

CREATE OR REPLACE API INTEGRATION GIT_API_RIDEFLOW
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/LakeLogic')
  ALLOWED_AUTHENTICATION_SECRETS = (GIT_PAT)
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY RIDEFLOW_REPO
  API_INTEGRATION = GIT_API_RIDEFLOW
  GIT_CREDENTIALS = GIT_PAT
  ORIGIN = 'https://github.com/LakeLogic/lakelogic-snowflake-data-mesh-lakehouse';

ALTER GIT REPOSITORY RIDEFLOW_REPO FETCH;

-- Flesh out the whole project straight from the repo:
EXECUTE IMMEDIATE FROM @RIDEFLOW_REPO/branches/main/sql/setup.sql;
