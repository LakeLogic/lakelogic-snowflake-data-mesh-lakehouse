# Iceberg roadmap — making the Snowflake mesh an open lakehouse

The current build is **fully Snowflake-native**: LakeLogic's `snowflake` engine runs
contracts as SQL and persists ordinary Snowflake tables (proprietary FDN format,
Snowflake-managed storage). No Spark, no open table format.

This note captures how to evolve it to **open Apache Iceberg** — deferred until the
native Project ships, but scoped here so nothing is lost.

## Delta vs Iceberg on Snowflake

- Snowflake **writes and manages Iceberg** natively; **Delta is read-only** (external
  tables / Delta→Iceberg conversion). So the write lakehouse format on Snowflake is
  **Iceberg**.
- LakeLogic's Iceberg support lives in the **`spark`, `duckdb`, and `pyiceberg`** paths
  (`core/materialization.py`, `core/quarantine.py`) — **not** the `snowflake` SQL engine.

## Horizon Iceberg REST Catalog (the key enabler)

Snowflake **Horizon Catalog** is a built-in Iceberg REST catalog, **available in every
account** (no separate service to provision). It supports external engines to **read AND
write** Snowflake-managed Iceberg v2/v3 tables (GA). Supported engines include
**Apache Spark, PyIceberg, DuckDB, Trino, Dremio, Flink, Doris, StarRocks**.

- **Endpoint:** `https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog`
- **Auth:** key-pair JWT→token, **PAT on a service user**, or external OAuth (~1h tokens).
- **External volume required** for `CREATE ICEBERG TABLE` (data files live in *your*
  S3/Azure/GCS bucket; Horizon holds only metadata).
- **Privileges:** read = `SELECT` + `USAGE`; write = `INSERT/UPDATE/DELETE/TRUNCATE` +
  `USAGE`; create = `CREATE ICEBERG TABLE` on the schema + `USAGE` on the external volume.
- **Billing:** 0.5 credit / million API calls (starts H2 2026) — negligible.

Spark catalog config (note `.warehouse` = the Snowflake **database** name):

```python
spark.sql.catalog.<cat>.type      = rest
spark.sql.catalog.<cat>.uri       = https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog
spark.sql.catalog.<cat>.warehouse = <DATABASE>
spark.sql.catalog.<cat>.token     = <ACCESS_TOKEN>
spark.sql.catalog.<cat>.scope     = session:role:<ROLE>
spark.sql.catalog.<cat>.header.X-Iceberg-Access-Delegation = vended-credentials
```

## The three paths

| | Writer | Catalog | Storage | In Snowflake? | Open / multi-engine | Spark? | Effort |
|---|---|---|---|---|---|---|---|
| **Native (today)** | Snowflake SQL engine | Snowflake (FDN) | Snowflake-managed | ✅ writer | ❌ | No | shipped |
| **A · Snowflake-managed Iceberg** | Snowflake | Horizon | your bucket (ext. volume) | ✅ writer | ✅ | No | moderate |
| **B · LakeLogic → Horizon REST** | **LakeLogic Spark** | Horizon (built-in) | your bucket (ext. volume) | ✅ read/query | ✅ | Yes | large |

- **A (light):** keep the `snowflake` engine; change the persist step from native
  `write_pandas` to `CREATE ICEBERG TABLE … EXTERNAL_VOLUME=…` (Snowflake owns writes).
  Tables become open + externally readable. No Spark, no compute pool.
- **B (full/open):** run LakeLogic's **`spark`** engine (e.g. via **Snowpark Connect** on
  a compute pool, or any Spark) configured against the **Horizon REST catalog** above, so
  **LakeLogic** writes Snowflake-managed Iceberg tables directly. Full Databricks-parity
  medallion (gates/quarantine/SCD2 via spark) in open format, governed by Snowflake's own
  catalog.

## The one unavoidable prerequisite

Either path needs **external cloud storage (S3/Azure/GCS) + a Snowflake external volume**
for the Iceberg data files. The current native build deliberately avoids this (Snowflake-
managed storage only). Horizon removes the need for a *third-party* catalog — it does not
remove the need for a storage bucket.

## Recommended sequence

1. Ship the native Project (notebooks + Task DAG) — in progress.
2. **A** — stand up one bucket + external volume, switch Gold (then all layers) to
   Snowflake-managed Iceberg. Smallest change, open format, no Spark.
3. **B** — when full engine-agnostic parity + LakeLogic-as-writer is wanted, wire the
   Spark engine to the Horizon REST catalog.
