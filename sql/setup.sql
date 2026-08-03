-- ============================================================================
-- RideFlow on Snowflake — project setup (run FROM the Git repo)
-- ============================================================================
-- Fleshes out the whole native project: schemas, compute pool, staged LakeLogic
-- wheelhouse, Notebooks, and the Task DAG. Run it directly from the repo after
-- sql/bootstrap.sql has created the DATABASE, WAREHOUSE and GIT REPOSITORY:
--
--   EXECUTE IMMEDIATE FROM @RIDEFLOW_DEV_DEMO.PIPELINE.RIDEFLOW_REPO/branches/main/sql/setup.sql;
--
-- Demo object names are fixed (RIDEFLOW_DEV_DEMO / RIDEFLOW_WH / RIDEFLOW_POOL).
-- Edit them here + in bootstrap.sql if you want different names.
-- ============================================================================

USE DATABASE RIDEFLOW_DEV_DEMO;
CREATE SCHEMA IF NOT EXISTS LANDING;
CREATE SCHEMA IF NOT EXISTS QUARANTINE;
CREATE SCHEMA IF NOT EXISTS MARKETPLACE;
CREATE SCHEMA IF NOT EXISTS PIPELINE;
USE SCHEMA PIPELINE;

-- Compute pool for the container-runtime Notebooks -----------------------------
CREATE COMPUTE POOL IF NOT EXISTS RIDEFLOW_POOL
  MIN_NODES = 1 MAX_NODES = 2 INSTANCE_FAMILY = CPU_X64_XS AUTO_SUSPEND_SECS = 300;

-- Stage the LakeLogic wheelhouse from the repo. Headless task runs don't have the
-- internal PyPI mirror, so notebooks install lakelogic (official 1.41.0) + pure-python
-- deps from here with pip --no-index; the runtime already ships pandas/pydantic/pyyaml.
CREATE STAGE IF NOT EXISTS WHEELS;
COPY FILES INTO @WHEELS FROM @RIDEFLOW_REPO/branches/main/wheelhouse/;

-- One parameterized driver notebook — created FROM the repo ROOT (MAIN_FILE points into
-- notebooks/) so the whole repo (runtime/ + domains_rideflow/) is available at runtime.
CREATE OR REPLACE NOTEBOOK NB_PIPELINE FROM '@RIDEFLOW_REPO/branches/main/'
  MAIN_FILE = 'notebooks/pipeline_driver.ipynb'
  QUERY_WAREHOUSE = RIDEFLOW_WH COMPUTE_POOL = RIDEFLOW_POOL RUNTIME_NAME = 'SYSTEM$BASIC_RUNTIME';
ALTER NOTEBOOK NB_PIPELINE ADD LIVE VERSION FROM LAST;

-- Task DAG — one task per layer, each runs its notebook; root is nightly + on-demand.
-- Suspend then DROP any existing graph first (root first to disarm), so we always build
-- fresh — CREATE OR REPLACE can't modify a task whose root isn't seen suspended in a
-- git-run script. Order: suspend root->children, drop children->root, create root->children.
ALTER TASK IF EXISTS T_RIDEFLOW_MARKETPLACE SUSPEND;
ALTER TASK IF EXISTS T_00_PROVISION SUSPEND;
ALTER TASK IF EXISTS T_01_LANDING SUSPEND;
ALTER TASK IF EXISTS T_02_BRONZE SUSPEND;
ALTER TASK IF EXISTS T_03_SILVER SUSPEND;
ALTER TASK IF EXISTS T_04_GOLD SUSPEND;
DROP TASK IF EXISTS T_04_GOLD;
DROP TASK IF EXISTS T_03_SILVER;
DROP TASK IF EXISTS T_02_BRONZE;
DROP TASK IF EXISTS T_01_LANDING;
DROP TASK IF EXISTS T_00_PROVISION;
DROP TASK IF EXISTS T_RIDEFLOW_MARKETPLACE;

CREATE TASK T_RIDEFLOW_MARKETPLACE WAREHOUSE = RIDEFLOW_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
  COMMENT = 'RideFlow marketplace medallion root - nightly 0300 UTC or on demand'
  AS SELECT 1;
-- Every task calls the ONE driver, passing (domain, system, layer).
CREATE TASK T_00_PROVISION WAREHOUSE = RIDEFLOW_WH AFTER T_RIDEFLOW_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=provision');
CREATE TASK T_01_LANDING WAREHOUSE = RIDEFLOW_WH AFTER T_00_PROVISION
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=landing');
CREATE TASK T_02_BRONZE WAREHOUSE = RIDEFLOW_WH AFTER T_01_LANDING
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=bronze');
CREATE TASK T_03_SILVER WAREHOUSE = RIDEFLOW_WH AFTER T_02_BRONZE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=silver');
CREATE TASK T_04_GOLD WAREHOUSE = RIDEFLOW_WH AFTER T_03_SILVER
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=gold');

-- Arm the graph (children first, root last).
ALTER TASK T_04_GOLD RESUME;
ALTER TASK T_03_SILVER RESUME;
ALTER TASK T_02_BRONZE RESUME;
ALTER TASK T_01_LANDING RESUME;
ALTER TASK T_00_PROVISION RESUME;
ALTER TASK T_RIDEFLOW_MARKETPLACE RESUME;

-- Kick off one run now (comment out to only run on the nightly schedule):
EXECUTE TASK T_RIDEFLOW_MARKETPLACE;
