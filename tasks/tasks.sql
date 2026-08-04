-- ============================================================================
-- RideFlow data-mesh — full Task DAG (one task per system, FK dependency order)
-- ============================================================================
-- Every task calls the ONE parameterized driver NB_PIPELINE with (domain, system, layer=all).
-- Waves: reference/marketplace/independent -> payments+marketing+ops(need marketplace)
--        -> zendesk(needs payments) -> shared cross-domain marts (need everything).
-- Placeholders {{DB}}/{{WH}} substituted by deploy/deploy_project.py.
-- ============================================================================
USE DATABASE {{DB}};
USE SCHEMA PIPELINE;

CREATE OR REPLACE TASK T_RIDEFLOW_MESH WAREHOUSE = {{WH}}
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
  COMMENT = 'RideFlow data-mesh root - nightly 0300 UTC or on demand'
  AS SELECT 1;
CREATE OR REPLACE TASK T_REFERENCE WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=reference system=internal layer=all');
CREATE OR REPLACE TASK T_MARKETPLACE WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketplace system=rideflow layer=all');
CREATE OR REPLACE TASK T_MKT_GOOGLE_ADS WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=google_ads layer=all');
CREATE OR REPLACE TASK T_MKT_META WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=meta_ads layer=all');
CREATE OR REPLACE TASK T_OPS_TWILIO WAREHOUSE = {{WH}} AFTER T_RIDEFLOW_MESH
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=operations system=twilio layer=all');
CREATE OR REPLACE TASK T_PAYMENTS WAREHOUSE = {{WH}} AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=payments system=stripe layer=all');
CREATE OR REPLACE TASK T_MKT_GA WAREHOUSE = {{WH}} AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=google_analytics layer=all');
CREATE OR REPLACE TASK T_MKT_HUBSPOT WAREHOUSE = {{WH}} AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=marketing system=hubspot layer=all');
CREATE OR REPLACE TASK T_OPS_CHECKR WAREHOUSE = {{WH}} AFTER T_MARKETPLACE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=operations system=checkr layer=all');
CREATE OR REPLACE TASK T_OPS_ZENDESK WAREHOUSE = {{WH}} AFTER T_PAYMENTS
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=operations system=zendesk layer=all');
CREATE OR REPLACE TASK T_SHARED WAREHOUSE = {{WH}} AFTER T_OPS_ZENDESK, T_MKT_GA, T_MKT_HUBSPOT, T_OPS_CHECKR, T_MKT_GOOGLE_ADS, T_MKT_META, T_OPS_TWILIO, T_REFERENCE
  AS EXECUTE NOTEBOOK {{DB}}.PIPELINE.NB_PIPELINE('domain=shared system=shared layer=all');
