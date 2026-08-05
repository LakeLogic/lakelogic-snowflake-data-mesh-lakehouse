-- ============================================================================
-- RideFlow on Snowflake — one-time bootstrap (run in a Snowsight worksheet)
-- ============================================================================
-- The public quickstart needs NO GitHub credentials — Snowflake clones this public
-- repository anonymously. This creates the database, warehouse and Git repository,
-- then runs sql/setup.sql straight from the repo to build everything else (compute
-- pool, staged wheelhouse, Notebooks, Task DAG) — no laptop / Python required.
--
-- Steps:
--   1. Run as a role that can create databases/warehouses/integrations (e.g. ACCOUNTADMIN).
--   2. Run all. Then watch Monitoring -> Task History (or the T_RIDEFLOW_MESH Graph tab).
--
-- Forked the repo PRIVATELY? See the "private fork" block at the bottom.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS RIDEFLOW_DEV_DEMO;
CREATE WAREHOUSE IF NOT EXISTS RIDEFLOW_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

USE DATABASE RIDEFLOW_DEV_DEMO;
CREATE SCHEMA IF NOT EXISTS PIPELINE;
USE SCHEMA PIPELINE;

-- Public repository → no authentication secret required.
CREATE OR REPLACE API INTEGRATION GIT_API_RIDEFLOW
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/LakeLogic')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY RIDEFLOW_REPO
  API_INTEGRATION = GIT_API_RIDEFLOW
  ORIGIN = 'https://github.com/LakeLogic/lakelogic-snowflake-data-mesh-lakehouse';

ALTER GIT REPOSITORY RIDEFLOW_REPO FETCH;

-- Flesh out the whole project straight from the repo:
EXECUTE IMMEDIATE FROM @RIDEFLOW_REPO/branches/main/sql/setup.sql;

-- ----------------------------------------------------------------------------
-- PRIVATE FORK ONLY — if you forked this repo privately, Snowflake needs a
-- credential. Add a secret and reference it from the two objects above:
--
--   CREATE OR REPLACE SECRET GIT_PAT
--     TYPE = PASSWORD
--     USERNAME = '<github-username>'
--     PASSWORD = '<fine-grained PAT, Contents: Read on the repo>';
--
--   -- add to the API INTEGRATION:  ALLOWED_AUTHENTICATION_SECRETS = (GIT_PAT)
--   -- add to the GIT REPOSITORY:   GIT_CREDENTIALS = GIT_PAT
-- ----------------------------------------------------------------------------
