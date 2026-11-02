-- One-time bootstrap (run in a Snowsight worksheet as a role that can create databases,
-- warehouses and integrations). Creates the database, warehouse and Git repository, then runs
-- sql/setup.sql from the repository. EDIT the ORIGIN below to this project's repository.
CREATE DATABASE IF NOT EXISTS RIDEFLOW_DEV;
CREATE WAREHOUSE IF NOT EXISTS RIDEFLOW_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
USE DATABASE RIDEFLOW_DEV;
CREATE SCHEMA IF NOT EXISTS "pipeline";
USE SCHEMA "pipeline";
CREATE OR REPLACE API INTEGRATION GIT_API_RIDEFLOW
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<owner>')
  ENABLED = TRUE;
CREATE OR REPLACE GIT REPOSITORY RIDEFLOW_REPO
  API_INTEGRATION = GIT_API_RIDEFLOW
  ORIGIN = 'https://github.com/<owner>/<repo>';
ALTER GIT REPOSITORY RIDEFLOW_REPO FETCH;
EXECUTE IMMEDIATE FROM @RIDEFLOW_REPO/branches/main/platform/snowflake/sql/setup.sql
  USING (DB => 'RIDEFLOW_DEV', WH => 'RIDEFLOW_WH', POOL => 'RIDEFLOW_POOL',
         REPO => 'RIDEFLOW_REPO', BRANCH => 'main');
