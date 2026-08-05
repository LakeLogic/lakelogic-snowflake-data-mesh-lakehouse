-- ============================================================================
-- RideFlow data-mesh — Task DAG (MAX PARALLELISM — natural FK DAG)
-- ============================================================================
-- REFERENCE SNAPSHOT. The deploy GENERATES the DAG from MESH_SYSTEMS per
-- `--mesh-max-parallel` (default 0 = max). This file mirrors the default (0).
-- Peak concurrency ~7; the pool auto-sizes to MAX_NODES=8. Validated on the
-- Snowflake trial: whole 11-system mesh green in ~9 min. Each wave's task depends
-- on its real cross-domain FK parents only (marketplace before its consumers,
-- zendesk after payments, shared last). Placeholders {{DB}}/{{WH}}.
-- For a tighter node footprint use `--mesh-max-parallel 2` (waves of 2).
-- ============================================================================
USE DATABASE {{DB}};
USE SCHEMA PIPELINE;
CREATE OR REPLACE TASK T_RIDEFLOW_MESH WAREHOUSE = {{WH}} SCHEDULE = 'USING CRON 0 3 * * * UTC' COMMENT = 'RideFlow data-mesh root' AS SELECT 1;
CREATE OR REPLACE TASK T_MARKETPLACE WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=all');
CREATE OR REPLACE TASK T_REFERENCE WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=reference system=internal layer=all');
CREATE OR REPLACE TASK T_PAYMENTS WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=payments system=stripe layer=all');
CREATE OR REPLACE TASK T_OPS_CHECKR WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=operations system=checkr layer=all');
CREATE OR REPLACE TASK T_OPS_TWILIO WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=operations system=twilio layer=all');
CREATE OR REPLACE TASK T_OPS_ZENDESK WAREHOUSE = {{WH}} AFTER T_PAYMENTS AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=operations system=zendesk layer=all');
CREATE OR REPLACE TASK T_MKT_GOOGLE_ADS WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=google_ads layer=all');
CREATE OR REPLACE TASK T_MKT_GA WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=google_analytics layer=all');
CREATE OR REPLACE TASK T_MKT_HUBSPOT WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=hubspot layer=all');
CREATE OR REPLACE TASK T_MKT_META WAREHOUSE = {{WH}} AFTER T_MARKETPLACE AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=meta_ads layer=all');
CREATE OR REPLACE TASK T_SHARED WAREHOUSE = {{WH}} AFTER T_MARKETPLACE, T_REFERENCE, T_PAYMENTS, T_OPS_CHECKR, T_OPS_TWILIO, T_OPS_ZENDESK, T_MKT_GOOGLE_ADS, T_MKT_GA, T_MKT_HUBSPOT, T_MKT_META AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=shared system=shared layer=all');
