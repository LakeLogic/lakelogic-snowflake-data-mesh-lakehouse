# Architecture and reference

[Back to the quickstart](../README.md)

Detailed examples and repository layout for the RideFlow reference lakehouse on Snowflake.

> Current inventory: 158 contracts across seven domains, including Business/Salesforce. There are 14 source-system trees plus the Shared registry. The contracts are identical to the [Databricks reference project](https://github.com/LakeLogic/lakelogic-rideflow-databricks-governed-lakehouse); only `_system.yaml` platform settings and `platform/snowflake/` differ.

## Reference estate

```text
RideFlow · one Snowflake account · 7 domains · 14 source systems plus Shared
Source-processing domains use the medallion pattern — Bronze (raw, all-string, schema-flexible) → Silver (typed, cleaned, transformed, validated) → Gold (products)
│
├── Marketplace   (rideflow)                          ← shown expanded as the example
│   ├── Bronze   raw · all-string · schema-flexible (evolves, keeps everything)
│   │     driver_profiles · rider_profiles · driver_telemetry · rider_app_events
│   │     trip_requests · trip_completed · trip_cancellations
│   ├── Silver   typed, cleaned, deduped, transformed, validated
│   │     driver_profiles · rider_profiles · trips (joined) · trip_requests
│   │     driver_telemetry · rider_app_events
│   └── Gold   business products
│         dim_driver · dim_rider (SCD2) · code dims (trip_type, vehicle_type, …)
│         fact_trip_completed · fact_trip_requests · fact_trip_cancellations
│         fact_driver_telemetry · fact_rider_app_events
│
├── Payments     (stripe · driver_expenses)                        →  Bronze → Silver → Gold
├── Operations   (checkr · onboarding · trust_safety · twilio · zendesk)  →  Bronze → Silver → Gold
├── Marketing    (google_ads · google_analytics · hubspot · meta_ads)     →  Bronze → Silver → Gold
├── Business     (salesforce)                                      →  Bronze → Silver → Gold
├── Reference    (internal)      →  Bronze → Silver → Gold   ·  cities, fx rates
└── Shared       (cross-domain marts)   →  Gold   ·  products that join every domain
```

## How the architecture works

Source-processing domains own Bronze, Silver, and Gold definitions. Shared products consume other domains' outputs directly. Shared
settings can be inherited from `_domain.yaml` and `_system.yaml`; individual
contracts keep dataset-specific rules beside the team that understands the data.

The repository contains 158 contracts across Marketplace, Marketing, Payments,
Operations, Business, Reference, and Shared. The examples include CSV, JSON, unstructured
PDF, image and Word input, external Python logic, cross-domain Gold products, row-level lineage
tags, freshness and volume SLOs, and an engine-agnostic pipeline DAG.

### Medallion plus quarantine

Contracts govern every layer. Row failures can enter quarantine while valid rows
continue; dataset gates can still stop an unsafe run. Quarantine is a failure path,
not another medallion layer. A rejected row retains the failed rule and source
context. Structured run logs record what passed, what failed, and how long each
contract took.

### Dependency-aware execution

Contracts declare `depends_on` edges. LakeLogic builds the directed acyclic graph
and processes products in dependency order, including dependencies within the
same medallion layer. The mesh orchestrator is a Snowflake task graph whose tasks
follow the same dependencies: Business and Reference first, then the systems that
read them, then Shared.

### Cross-domain products

Shared Gold products can consume products owned by other domains. Their declared
dependencies preserve lineage across team boundaries.

### SCD Type 2

Gold contracts can declare SCD Type 2 materialization, including tracked columns,
surrogate keys, effective dates, current-version flags, and an unknown member.

## What gets created

```text
Platform database: RIDEFLOW_DEV
└── "pipeline"            Git repository object, notebooks, tasks, the wheels stage and run logs

One database per domain: RIDEFLOW_DEV_<DOMAIN>
├── FILES.LANDING         Internal stage of inbound source files, one folder per system
├── BRONZE                Bronze tables
├── SILVER                Silver tables
├── GOLD                  Gold tables
├── QUARANTINE            Contract-failing rows
└── LAKELOGIC             Run log, SLO results, retention and erasure records

Domains: marketplace, marketing, payments, operations, business,
         reference (shared lookups and conformed dimensions),
         shared (cross-domain marts; gold only)

Account objects: RIDEFLOW_WH warehouse, RIDEFLOW_POOL compute pool
```

## Repository layout

The repository keeps reusable data-product definitions separate from the code
that deploys and runs them on Snowflake:

```text
contracts/                    Domain-owned contract trees
  marketing/                  Google Ads, Google Analytics, HubSpot, and Meta Ads
    _domain.yaml              Domain ownership, SLOs, compliance and retention
    <system>/
      _system.yaml            System defaults, Snowflake platform settings and contract registry
      contracts/
        bronze/               Source-aligned ingestion contracts
        silver/               Cleaned and conformed data products
        gold/                 Business-facing facts, dimensions, and aggregates
  business/                   Salesforce accounts, contacts, opportunities, cases and contracts
  marketplace/                Trips, riders, drivers, telemetry, and pricing
  operations/                 Screening, onboarding, trust and safety, support
  payments/                   Charges, refunds, payouts, and driver expenses
  reference/                  Shared lookups and conformed reference data
  shared/                     Cross-domain products and marts
platform/snowflake/
  deploy/deploy_project.py    Deploys the project from its Git repository
  deploy/project.json         Database, warehouse, compute pool and engine version per environment
  deploy/deployment_targets.json  The deployment target as Lakehouse Studio stored it
  deploy/.env.example         Connection settings to copy into .env
  notebooks/                  Snowflake Notebooks
    nb_00_setup               Domain databases, schemas and landing stages
    nb_pipeline_driver        Registry-driven Bronze, Silver, and Gold runner
    nb_test_data_driver       Synthetic landing-data generator
    nb_slo_checks             Service-level (freshness / row-count) checks
    nb_maintenance_optimize   Table maintenance
    nb_compliance_retention_check  Read-only retention check
    nb_compliance_erasure     Data-subject erasure tooling, dry run by default
  runtime/                    Python the notebooks import
  sql/                        bootstrap.sql (one-time) and setup.sql
  tasks/                      Tasks, grouped as LakeLogic Lakehouse Studio renders them
    admin/                    pl_admin_setup (a script the deploy runs), pl_admin_maintenance
    data/                     pl_data_00_mesh_orchestrator, one task graph per system
    monitor/                  pl_monitor_slo
    privacy/                  pl_privacy_retention, pl_privacy_erasure
    test/                     pl_test_synthetic_seed
  tests/                      Project structure tests
docs/                         Architecture reference
  images/                     README screenshots
README.md                     Public overview and quickstart
LICENSE                       Apache 2.0 licence
```

`contracts/` contains the portable business definitions. `platform/snowflake/`
contains the platform-specific deployment and execution code. This boundary is
intentional: a domain team can review its contracts without needing to understand
the Snowflake objects, while the platform team can change deployment code
without moving business rules into notebooks. The same `contracts/` tree runs on
Databricks and Fabric with a different `platform/` folder.
