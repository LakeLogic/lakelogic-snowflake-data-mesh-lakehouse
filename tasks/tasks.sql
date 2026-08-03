-- ============================================================================
-- RideFlow marketplace medallion — Snowflake Task DAG
-- ============================================================================
-- One Task per layer, each running its LakeLogic Notebook via EXECUTE NOTEBOOK.
-- The DAG (root -> landing -> bronze -> silver -> gold) gives native, observable
-- run history in Snowsight -> Monitoring -> Task History / Task Graph.
--
-- Notebooks are created from the Git-connected repo by deploy/deploy_project.py
-- into schema PIPELINE (NB_00_PROVISION ... NB_04_GOLD). Run this AFTER they exist.
--
-- Placeholders {{DB}} and {{WH}} are substituted by deploy_project.py (defaults:
-- RIDEFLOW_DEV_DEMO / RIDEFLOW_WH).
-- ============================================================================

USE DATABASE {{DB}};
USE SCHEMA PIPELINE;

-- Root task: entry point for the whole medallion (manually triggered, or give it a
-- SCHEDULE to run nightly). Resumes the graph; children fire via AFTER.
CREATE OR REPLACE TASK T_RIDEFLOW_MARKETPLACE
  WAREHOUSE = {{WH}}
  SCHEDULE  = 'USING CRON 0 3 * * * UTC'
  COMMENT   = 'RideFlow marketplace medallion root - nightly 0300 UTC or on demand'
  AS SELECT 1;

CREATE OR REPLACE TASK T_00_PROVISION
  WAREHOUSE = {{WH}}
  AFTER T_RIDEFLOW_MARKETPLACE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_00_PROVISION();

CREATE OR REPLACE TASK T_01_LANDING
  WAREHOUSE = {{WH}}
  AFTER T_00_PROVISION
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_01_LANDING();

CREATE OR REPLACE TASK T_02_BRONZE
  WAREHOUSE = {{WH}}
  AFTER T_01_LANDING
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_02_BRONZE();

CREATE OR REPLACE TASK T_03_SILVER
  WAREHOUSE = {{WH}}
  AFTER T_02_BRONZE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_03_SILVER();

CREATE OR REPLACE TASK T_04_GOLD
  WAREHOUSE = {{WH}}
  AFTER T_03_SILVER   -- gold waits on silver
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_04_GOLD();

-- Tasks are created SUSPENDED. Resume children first, then the root, so the graph is
-- armed. deploy_project.py does this; to do it by hand:
--   ALTER TASK T_04_GOLD RESUME;  ALTER TASK T_03_SILVER RESUME;
--   ALTER TASK T_02_BRONZE RESUME; ALTER TASK T_01_LANDING RESUME;
--   ALTER TASK T_00_PROVISION RESUME; ALTER TASK T_RIDEFLOW_MARKETPLACE RESUME;
-- Kick off a run on demand:
--   EXECUTE TASK T_RIDEFLOW_MARKETPLACE;
