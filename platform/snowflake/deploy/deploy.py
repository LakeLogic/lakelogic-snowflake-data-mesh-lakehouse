#!/usr/bin/env python
"""LakeLogic RideFlow data-mesh demo on Snowflake.

Snowflake is the compute AND the storage: LakeLogic's `snowflake` engine runs every
contract as SQL inside Snowflake via snowflake-connector-python. There is no object
store / file medallion — Bronze reads a landing TABLE that this deploy loads from
seeded synthetic data; Silver/Gold are tables built by the engine.

Flow:
  1. connect (env / .env)                    5. load landing tables
  2. create warehouse + database             6. run Bronze -> Silver -> Gold per system
  3. create a schema per domain + LANDING + QUARANTINE
  4. generate seeded synthetic landing data

Auth: env vars SNOWFLAKE_ACCOUNT/USER/PASSWORD/ROLE/WAREHOUSE/DATABASE
(copy platform/snowflake/deploy/.env.example -> .env).

Usage:  python snowflake/deploy/deploy.py [--domain marketplace/rideflow] [--setup-only]
"""
from __future__ import annotations

import argparse
import os
import sys
import tempfile
import glob
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]      # contracts/ live at the repository root
PLATFORM = Path(__file__).resolve().parents[1]  # platform/snowflake
sys.path.insert(0, str(PLATFORM))  # import the shared runtime (runtime.medallion)


def load_env():
    envf = Path(__file__).with_name(".env")
    if envf.exists():
        for line in envf.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.split("#")[0].strip())


def connect(database=None):
    import snowflake.connector
    p = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    }
    # Key-pair auth (recommended for MFA accounts) takes precedence over a password.
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        from cryptography.hazmat.primitives import serialization
        pk = serialization.load_pem_private_key(
            Path(key_path).read_bytes(),
            password=(os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or "").encode() or None)
        p["private_key"] = pk.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
    else:
        p["password"] = os.getenv("SNOWFLAKE_PASSWORD")
    if database:
        p["database"] = database
    need = ["account", "user"] + ([] if key_path else ["password"])
    missing = [k for k in need if not p.get(k)]
    if missing:
        sys.exit(f"Missing SNOWFLAKE_{{{','.join(m.upper() for m in missing)}}} — set them in platform/snowflake/deploy/.env")
    return snowflake.connector.connect(**{k: v for k, v in p.items() if v})


def sql(cur, stmt):
    print(f"  → {stmt}")
    cur.execute(stmt)


def discover_systems():
    out = []
    for sy in sorted((REPO / "contracts").rglob("_system.yaml")):
        import yaml
        d = yaml.safe_load(sy.read_text(encoding="utf-8"))
        if d.get("domain") and d.get("system"):
            out.append((d["domain"], d["system"], sy))
    # de-dupe (domain,system)
    seen, uniq = set(), []
    for d, s, p in out:
        if (d, s) not in seen:
            seen.add((d, s)); uniq.append((d, s, p))
    return uniq


def provision(cur, db, wh, domains):
    print("── Provision ────────────────────────────────────────────────")
    # Snowflake-native casing: everything is UPPERCASE (contract field names, table
    # columns), matching Snowflake's default folding of unquoted identifiers. Ensure
    # the ignore-case flag is OFF (a prior run may have set it TRUE, which breaks the
    # engine's case-sensitive column matching).
    sql(cur, f"ALTER USER {os.getenv('SNOWFLAKE_USER')} UNSET QUOTED_IDENTIFIERS_IGNORE_CASE")
    sql(cur, "ALTER SESSION UNSET QUOTED_IDENTIFIERS_IGNORE_CASE")
    sql(cur, f"CREATE WAREHOUSE IF NOT EXISTS {wh} WAREHOUSE_SIZE=XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE")
    sql(cur, f"USE WAREHOUSE {wh}")
    sql(cur, f"CREATE DATABASE IF NOT EXISTS {db}")
    sql(cur, f"USE DATABASE {db}")
    for s in ["LANDING", "QUARANTINE"] + list(domains):
        sql(cur, f"CREATE SCHEMA IF NOT EXISTS {db}.{s}")


def run_and_persist(conn, db, sys_yaml, layers=("bronze", "silver", "gold")):
    """Run the full medallion for one system via the shared runtime (the same code the
    in-Snowflake Notebooks use), so local deploy and Notebook execution are identical."""
    from runtime.medallion import run_layer
    for layer in layers:
        run_layer(conn, db, sys_yaml, layer)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", default="", help="only run <domain>/<system>")
    ap.add_argument("--setup-only", action="store_true")
    ap.add_argument("--via-stage", action="store_true",
                    help="land via the internal stage (PUT + COPY INTO) instead of direct load")
    args = ap.parse_args()
    load_env()
    db = os.getenv("SNOWFLAKE_DATABASE", "RIDEFLOW_DEV_DEMO")
    wh = os.getenv("SNOWFLAKE_WAREHOUSE", "RIDEFLOW_WH")
    os.environ["SNOWFLAKE_DATABASE"] = db  # contracts resolve {catalog} from this

    systems = discover_systems()
    if args.domain:
        want = tuple(args.domain.split("/"))
        systems = [x for x in systems if (x[0], x[1]) == want]
    domains = sorted({d for d, _, _ in systems})

    conn = connect(); cur = conn.cursor()
    provision(cur, db, wh, sorted({d for d, _, _ in discover_systems()}))
    conn.commit()
    if args.setup_only:
        print("✅ setup-only complete."); return

    from runtime.landing import load_system_landing, GENERATORS
    print("── Generate + load landing, then run medallion ──────────────")
    for d, s, sy in systems:
        print(f"  ▶ {d}/{s}")
        if (d, s) not in GENERATORS:
            print(f"    ⏩ no generator yet for {d}/{s} — skipping"); continue
        load_system_landing(conn, db, d, s, via_stage=args.via_stage)
        run_and_persist(conn, db, sy)
    print("✅ Snowflake RideFlow deploy complete.")


if __name__ == "__main__":
    main()
