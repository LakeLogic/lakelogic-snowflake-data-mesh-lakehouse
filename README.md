# Governed Data Mesh on Snowflake — Data Contracts, Quarantine & Quality Gates

A working reference implementation of a **domain-driven data mesh on Snowflake**,
powered by [LakeLogic](https://pypi.org/project/lakelogic/). Six domains own their
Bronze, Silver and Gold data products while sharing the same contract-driven quality,
quarantine, lineage and dependency controls.

The example is **RideFlow**, a fictional ride-hailing and food-delivery business. All
data is synthetic.

> A community reference from the LakeLogic team, not an official Snowflake product.

## What this proves

- **Domain ownership** — teams own their contracts and data products.
- **Shared controls** — schema checks, row rules, dataset gates, SLO checks and
  dependency-aware execution use common machinery.
- **Explained quarantine** — failing rows are retained with the rule and reason that
  rejected them, never silently dropped.
- **Quality gates** — a dataset-level `min_good_ratio` gate can stop an unsafe run.
- **Cross-domain products** — declared dependencies connect products across teams.

## How it maps to Snowflake

Snowflake is both the **compute and the storage** — there is no object-store file
medallion. LakeLogic's `snowflake` engine runs every contract **as SQL inside
Snowflake** via `snowflake-connector-python`, driven by a small Python client.

| Data-mesh need | Snowflake implementation |
| --- | --- |
| Catalog | A **database** (e.g. `RIDEFLOW_DEV_DEMO`) |
| Domain boundary | A **schema** per domain (`MARKETPLACE`, `MARKETING`, …) |
| Bronze / Silver / Gold | **Tables** in the domain schema |
| Landing zone | A `LANDING` schema of tables loaded from seeded synthetic data |
| Rejected rows | A shared `QUARANTINE` schema |
| Compute | A **warehouse** (`RIDEFLOW_WH`, XSMALL, auto-suspend) |

Bronze reads a **landing table** (Snowflake has no file source), so the deploy
generates synthetic data and loads it into `LANDING.<system>_<entity>` before the
medallion runs.

## Prerequisites

- Python 3.9+ and `pip install "snowflake-connector-python[pandas]" lakelogic pyyaml`.
- A Snowflake account and a role that can **create a database and a warehouse**.
  A free [Snowflake trial](https://signup.snowflake.com/) works and gives you an
  account plus `ACCOUNTADMIN`.

## Configure your connection

Copy the template and fill it in (the file is gitignored):

```bash
cp deploy/.env.example deploy/.env
```

```ini
SNOWFLAKE_ACCOUNT=ORGNAME-ACCOUNTNAME
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=RIDEFLOW_WH
SNOWFLAKE_DATABASE=RIDEFLOW_DEV_DEMO
```

### Where each value comes from

Run this once in a **Snowsight worksheet** — it prints most of what you need:

```sql
SELECT CURRENT_ORGANIZATION_NAME()  AS org,
       CURRENT_ACCOUNT_NAME()       AS account,
       CURRENT_USER()               AS "user",
       CURRENT_ROLE()               AS role;
```

- **`SNOWFLAKE_ACCOUNT`** — the **account identifier**, in the form
  `ORGNAME-ACCOUNTNAME` (join the two columns above with a hyphen, e.g.
  `MYORG-XY12345`).
  - Or in Snowsight: click your **account name (bottom-left)** → hover the account →
    **Copy account identifier**.
  - Or read it from your Snowsight URL `https://app.snowflake.com/<org>/<account>/…`
    → use `<org>-<account>`.
  - The legacy **account locator** (e.g. `xy12345.eu-west-1.aws`) also works if that's
    what you have. Do **not** include `.snowflakecomputing.com`.
- **`SNOWFLAKE_USER`** — your login name (the `CURRENT_USER()` value, or the username
  you sign in with).
- **`SNOWFLAKE_PASSWORD`** — your account password.
  - If your account **enforces MFA**, password auth will prompt/fail non-interactively.
    Use a **key-pair** instead (see below) or a dedicated service user without MFA.
- **`SNOWFLAKE_ROLE`** — a role allowed to `CREATE DATABASE` and `CREATE WAREHOUSE`.
  `ACCOUNTADMIN` or `SYSADMIN` work on a trial. List yours with:
  ```sql
  SHOW GRANTS TO USER CURRENT_USER();      -- roles you have
  ```
  To use a custom role, grant it once as an admin:
  ```sql
  GRANT CREATE DATABASE ON ACCOUNT TO ROLE MY_ROLE;
  GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE MY_ROLE;
  ```
- **`SNOWFLAKE_WAREHOUSE`** — an existing warehouse, or a **new name** the deploy will
  create (XSMALL, `AUTO_SUSPEND=60s`). List existing ones with `SHOW WAREHOUSES;`.
- **`SNOWFLAKE_DATABASE`** — the database the demo creates. Pick any name you like
  (default `RIDEFLOW_DEV_DEMO`). The deploy runs `CREATE DATABASE IF NOT EXISTS`.

#### Optional: key-pair auth (recommended for MFA accounts / automation)

Generate a key, register the public key on your user, and set
`SNOWFLAKE_PRIVATE_KEY_PATH` instead of `SNOWFLAKE_PASSWORD`:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```
```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without header/footer>';
```
Then in `.env`: `SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8` (and leave the
password blank).

## Run it

```bash
# 1. Prove one system end-to-end (creates DB, warehouse, schemas; loads landing; runs Bronze→Silver→Gold)
python deploy/deploy.py --domain marketplace/rideflow

# 2. Just provision (no data)
python deploy/deploy.py --setup-only

# 3. Full mesh (all systems that have a generator)
python deploy/deploy.py
```

### Verify

In Snowsight:

```sql
USE DATABASE RIDEFLOW_DEV_DEMO;
SHOW SCHEMAS;
SHOW TABLES IN SCHEMA MARKETPLACE;

SELECT * FROM MARKETPLACE.GOLD_RIDEFLOW_FACT_TRIP_DAILY_KPIS LIMIT 20;
SHOW TABLES IN SCHEMA QUARANTINE;   -- rejected rows + their contract errors
```

## Repository layout

```text
domains_rideflow/          Domain-owned contract trees (portable, engine-agnostic)
  <domain>/<system>/contracts/{bronze,silver,gold}/
runtime/                   Shared LakeLogic-on-Snowflake runtime (used by deploy AND notebooks)
  medallion.py             run_layer(): run + persist one medallion layer on a connection
  landing.py               generate + load LANDING tables
notebooks/                 In-Snowflake LakeLogic Notebooks (provision/landing/bronze/silver/gold)
tasks/tasks.sql            Snowflake Task DAG (EXECUTE NOTEBOOK) — run history in Task Graph
deploy/deploy.py           Local client: provision + landing + run medallion (fastest path)
deploy/deploy_project.py   Deploy the native Snowflake Project (Git + Notebooks + Tasks)
deploy/.env.example        Connection + Project template
docs/                      Architecture and notes
```

`domains_rideflow/` is shared verbatim with the LakeLogic Databricks and Fabric
reference repos — the contracts are portable. Only the deploy/runtime layer differs
per platform.

## Native Snowflake Project (Notebooks + Contracts + Tasks)

Beyond the local client, the whole medallion can run **inside Snowflake** as a
**Git-connected Workspace** so every component is visible in Snowsight:

| Component | Where it shows up |
| --- | --- |
| Contracts (`domains_rideflow/`) + notebooks + tasks | **Projects → Workspaces** (the Git repo) |
| LakeLogic Notebooks (per layer) | **Projects → Notebooks** — each runs `run_layer()` on the session |
| Orchestration + run history | **Monitoring → Task History / Task Graph** |
| Data products / quarantine | **Databases →** `RIDEFLOW_DEV_DEMO` schemas |

The Notebooks `pip install lakelogic` (container runtime) and run the **same
`runtime/` code the local deploy uses**, on the notebook's own Snowpark session
(`SnowflakeAdapter.set_shared_connection(session.connection)`) — no password in the
notebook. A Task per layer runs its notebook via `EXECUTE NOTEBOOK`, chained
`provision → landing → bronze → silver → gold`.

**Requires a LakeLogic build with the Snowflake-engine fixes** used here
(`set_shared_connection` + `TRY_CAST`→`TO_VARCHAR`). Point `LAKELOGIC_SPEC` (top of each
notebook) at that PyPI release or a git build.

```bash
# In deploy/.env, also set: GITHUB_REPO_URL, GITHUB_USER, GITHUB_PAT, SNOWFLAKE_COMPUTE_POOL
python deploy/deploy_project.py            # wire Git repo + compute pool + notebooks + task DAG
python deploy/deploy_project.py --run      # ...and kick off one live run
```

> Container-runtime Notebooks need Snowpark Container Services (compute pools) enabled
> on the account. The local `deploy/deploy.py` path has no such requirement.

### How LANDING would be fed in production

The demo synthesizes LANDING. In production each source system maps to a Snowflake
ingestion path — e.g. **Snowflake Connector for Google Analytics** →
`marketing/google_analytics`, **ServiceNow connector**, or `COPY INTO` from an
S3/Azure/GCS/OneLake stage / Snowpipe Streaming — feeding the same `LANDING.<system>_<entity>`
tables the Bronze contracts read.

## Cost & teardown

The warehouse consumes credits only while queries run (auto-suspends after 60s). To
remove everything:

```sql
DROP DATABASE IF EXISTS RIDEFLOW_DEV_DEMO;
DROP WAREHOUSE IF EXISTS RIDEFLOW_WH;
```

## Take it further

```bash
pip install lakelogic
```

- **Documentation:** <https://lakelogic.github.io/LakeLogic/>
- **Package:** <https://pypi.org/project/lakelogic/>
