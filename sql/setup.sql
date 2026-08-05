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
-- MAX_NODES = 8 gives headroom for the natural FK DAG's peak width (~7 systems fan
-- out concurrently after marketplace). ALTER as well so an existing pool is widened
-- (CREATE IF NOT EXISTS is a no-op on an existing pool). For a tighter node
-- footprint, lower MAX_NODES and the graph still runs — Snowflake just schedules
-- fewer tasks at once.
CREATE COMPUTE POOL IF NOT EXISTS RIDEFLOW_POOL
  MIN_NODES = 1 MAX_NODES = 8 INSTANCE_FAMILY = CPU_X64_XS AUTO_SUSPEND_SECS = 300;
ALTER COMPUTE POOL RIDEFLOW_POOL SET MAX_NODES = 8;

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

-- Task DAG — natural FK dependency graph (one task per system) at MAX parallelism:
-- each system depends only on its real cross-domain parents, so after marketplace the
-- downstream systems fan out and run concurrently (mirrors deploy/deploy_project.py and
-- tasks/tasks.sql). Clean slate: suspend + drop the whole graph, then create fresh.
ALTER TASK IF EXISTS T_RIDEFLOW_MESH SUSPEND;
ALTER TASK IF EXISTS T_MARKETPLACE SUSPEND;
ALTER TASK IF EXISTS T_PAYMENTS SUSPEND;
ALTER TASK IF EXISTS T_MKT_GOOGLE_ADS SUSPEND;
ALTER TASK IF EXISTS T_MKT_GA SUSPEND;
ALTER TASK IF EXISTS T_MKT_HUBSPOT SUSPEND;
ALTER TASK IF EXISTS T_MKT_META SUSPEND;
ALTER TASK IF EXISTS T_OPS_TWILIO SUSPEND;
ALTER TASK IF EXISTS T_OPS_CHECKR SUSPEND;
ALTER TASK IF EXISTS T_OPS_ZENDESK SUSPEND;
ALTER TASK IF EXISTS T_REFERENCE SUSPEND;
ALTER TASK IF EXISTS T_SHARED SUSPEND;
DROP TASK IF EXISTS T_SHARED;
DROP TASK IF EXISTS T_REFERENCE;
DROP TASK IF EXISTS T_OPS_ZENDESK;
DROP TASK IF EXISTS T_OPS_CHECKR;
DROP TASK IF EXISTS T_OPS_TWILIO;
DROP TASK IF EXISTS T_MKT_META;
DROP TASK IF EXISTS T_MKT_HUBSPOT;
DROP TASK IF EXISTS T_MKT_GA;
DROP TASK IF EXISTS T_MKT_GOOGLE_ADS;
DROP TASK IF EXISTS T_PAYMENTS;
DROP TASK IF EXISTS T_MARKETPLACE;
DROP TASK IF EXISTS T_RIDEFLOW_MESH;

CREATE TASK T_RIDEFLOW_MESH WAREHOUSE = RIDEFLOW_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
  COMMENT = 'RideFlow data-mesh root - nightly 0300 UTC or on demand'
  AS SELECT 1;
-- Roots (after the mesh root):
CREATE TASK T_MARKETPLACE WAREHOUSE = RIDEFLOW_WH AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=all');
CREATE TASK T_REFERENCE WAREHOUSE = RIDEFLOW_WH AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=reference system=internal layer=all');
-- Read marketplace ids (fan out in parallel after marketplace):
CREATE TASK T_PAYMENTS WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=payments system=stripe layer=all');
CREATE TASK T_OPS_CHECKR WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=operations system=checkr layer=all');
CREATE TASK T_OPS_TWILIO WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=operations system=twilio layer=all');
CREATE TASK T_MKT_GOOGLE_ADS WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketing system=google_ads layer=all');
CREATE TASK T_MKT_GA WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketing system=google_analytics layer=all');
CREATE TASK T_MKT_HUBSPOT WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketing system=hubspot layer=all');
CREATE TASK T_MKT_META WAREHOUSE = RIDEFLOW_WH AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=marketing system=meta_ads layer=all');
-- zendesk enriches payments charges:
CREATE TASK T_OPS_ZENDESK WAREHOUSE = RIDEFLOW_WH AFTER T_PAYMENTS
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=operations system=zendesk layer=all');
-- shared marts join across every other system → run last:
CREATE TASK T_SHARED WAREHOUSE = RIDEFLOW_WH
  AFTER T_MARKETPLACE, T_REFERENCE, T_PAYMENTS, T_OPS_CHECKR, T_OPS_TWILIO, T_OPS_ZENDESK,
        T_MKT_GOOGLE_ADS, T_MKT_GA, T_MKT_HUBSPOT, T_MKT_META
  AS EXECUTE NOTEBOOK RIDEFLOW_DEV_DEMO.PIPELINE.NB_PIPELINE('domain=shared system=shared layer=all');

-- Arm the graph (children first, root last).
ALTER TASK T_SHARED RESUME;
ALTER TASK T_REFERENCE RESUME;
ALTER TASK T_OPS_ZENDESK RESUME;
ALTER TASK T_OPS_CHECKR RESUME;
ALTER TASK T_OPS_TWILIO RESUME;
ALTER TASK T_MKT_META RESUME;
ALTER TASK T_MKT_HUBSPOT RESUME;
ALTER TASK T_MKT_GA RESUME;
ALTER TASK T_MKT_GOOGLE_ADS RESUME;
ALTER TASK T_PAYMENTS RESUME;
ALTER TASK T_MARKETPLACE RESUME;
ALTER TASK T_RIDEFLOW_MESH RESUME;

-- Kick off one run now (comment out to only run on the nightly schedule):
EXECUTE TASK T_RIDEFLOW_MESH;
