#!/usr/bin/env python
"""Deploy this LakeLogic project as a native Snowflake project, from the GENERATED files.

  1. GIT REPOSITORY (+ API integration; a secret only for a private repo)
  2. sql/setup.sql equivalent: schemas, compute pool, wheel stage, notebooks
  3. every task file under tasks/<group>/ (admin, data, monitor, privacy, test), in that order
  4. (--run) EXECUTE TASK on the mesh root

Nothing here names a system: the task files are generated from the contracts, and the object
names come from deploy/project.json (generated from the project's deployment target in
LakeLogic Deliver, one entry per environment) unless overridden in deploy/.env. Credentials are
read from the environment / deploy/.env only — never stored.

Usage:  python deploy/deploy_project.py [--env dev] [--branch main] [--run]

Optional in deploy/.env: SNOWFLAKE_PYPI_INTEGRATION=<external access integration> so task runs
can install LakeLogic from PyPI.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
GROUP_ORDER = ["admin", "data", "monitor", "privacy", "test"]


def load_env() -> None:
    envf = Path(__file__).with_name(".env")
    if envf.exists():
        for line in envf.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.split("#")[0].strip())


def connect(database=None):
    import snowflake.connector

    p = {"account": os.getenv("SNOWFLAKE_ACCOUNT"), "user": os.getenv("SNOWFLAKE_USER"),
         "role": os.getenv("SNOWFLAKE_ROLE"), "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")}
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        from cryptography.hazmat.primitives import serialization

        pk = serialization.load_pem_private_key(
            Path(key_path).read_bytes(),
            password=(os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or "").encode() or None)
        p["private_key"] = pk.private_bytes(
            encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
    else:
        p["password"] = os.getenv("SNOWFLAKE_PASSWORD")
    if database:
        p["database"] = database
    missing = [k for k in ["account", "user"] + ([] if key_path else ["password"]) if not p.get(k)]
    if missing:
        sys.exit(f"Missing SNOWFLAKE_{{{','.join(m.upper() for m in missing)}}} - set them in deploy/.env")
    return snowflake.connector.connect(**{k: v for k, v in p.items() if v})


def run(cur, stmt):
    head = next((l for l in stmt.strip().splitlines() if l.strip()), "")
    print(f"  -> {head[:100]}")
    cur.execute(stmt)


def sql_statements(text):
    """Executable statements from a .sql file (-- comments stripped)."""
    lines = []
    for l in text.splitlines():
        i = l.find("--")
        lines.append(l[:i] if i != -1 else l)
    for stmt in "\n".join(lines).split(";"):
        if stmt.strip():
            yield stmt.strip()


def render(text, names):
    """Fill the task files' {{DB}} / {{WH}} / {{POOL}} / {{REPO}} / {{BRANCH}} placeholders."""
    return re.sub(r"\{\{\s*(\w+)\s*\}\}", lambda m: names.get(m.group(1), m.group(0)), text)


def task_files():
    """tasks/<group>/*.sql in the layout's group order; within a group, operational files first."""
    out = []
    for group in GROUP_ORDER:
        out += sorted((REPO / "tasks" / group).glob("*.sql"), key=lambda p: (not p.name.startswith("_"), p.name))
    # Setup first: it creates the notebooks every other task runs.
    return sorted(out, key=lambda p: p.name != "_setup.sql")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--branch", default="main")
    ap.add_argument("--run", action="store_true", help="EXECUTE TASK on the mesh root after deploy")
    ap.add_argument("--env", default=None,
                    help="Environment from deploy/project.json 'environments' (default: the first)")
    args = ap.parse_args()
    load_env()
    project = json.loads((REPO / "deploy" / "project.json").read_text(encoding="utf-8"))
    if args.env:
        envs = project.get("environments") or {}
        if args.env not in envs:
            sys.exit(f"No deployment target for environment {args.env!r} "
                     f"(set: {', '.join(envs) or 'none'}). Set it in LakeLogic Deliver.")
        project = envs[args.env]
    names = {
        "DB": (os.getenv("SNOWFLAKE_DATABASE") or "").strip() or project["database"],
        "WH": (os.getenv("SNOWFLAKE_WAREHOUSE") or "").strip() or project["warehouse"],
        "POOL": (os.getenv("SNOWFLAKE_COMPUTE_POOL") or "").strip() or project["compute_pool"],
        "REPO": project["git_repository"],
        # A branch with `/` in its name (`feat/x`) must be double-quoted in a repository path:
        # `/branches/"feat/x"/...`. Unquoted, Snowflake reads `feat` as the branch (2026-11-01).
        "BRANCH": f'"{args.branch}"' if "/" in args.branch else args.branch,
    }
    integration = project["git_api_integration"]
    repo_url = os.getenv("GITHUB_REPO_URL", "").strip()
    if not re.match(r"https://github\.com/[^/]+/", repo_url):
        sys.exit("Set GITHUB_REPO_URL=https://github.com/<owner>/<repo> in deploy/.env")
    owner = re.match(r"https://github\.com/([^/]+)/", repo_url).group(1)
    gh_user, pat = os.getenv("GITHUB_USER", "").strip(), os.getenv("GITHUB_PAT", "").strip()
    db = names["DB"]

    conn = connect()
    cur = conn.cursor()
    # The warehouse the notebooks and tasks run on. bootstrap.sql created it by hand once, so a
    # deploy to a fresh account failed creating the first notebook (2026-11-01).
    run(cur, f"CREATE WAREHOUSE IF NOT EXISTS {names['WH']} WAREHOUSE_SIZE = XSMALL "
             "AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE")
    run(cur, f"CREATE DATABASE IF NOT EXISTS {db}")
    run(cur, f"USE DATABASE {db}")
    run(cur, 'CREATE SCHEMA IF NOT EXISTS "pipeline"')
    run(cur, 'USE SCHEMA "pipeline"')
    if gh_user and pat:
        print("  -> CREATE OR REPLACE SECRET GIT_PAT (value not printed)")
        cur.execute(f"CREATE OR REPLACE SECRET GIT_PAT TYPE=PASSWORD USERNAME='{gh_user}' PASSWORD='{pat}'")
        run(cur, f"CREATE OR REPLACE API INTEGRATION {integration} API_PROVIDER = git_https_api "
                 f"API_ALLOWED_PREFIXES = ('https://github.com/{owner}') "
                 f'ALLOWED_AUTHENTICATION_SECRETS = ({db}."pipeline".GIT_PAT) ENABLED = TRUE')
        run(cur, f"CREATE OR REPLACE GIT REPOSITORY {names['REPO']} API_INTEGRATION = {integration} "
                 f'GIT_CREDENTIALS = {db}."pipeline".GIT_PAT ORIGIN = \'{repo_url}\'')
    else:
        run(cur, f"CREATE OR REPLACE API INTEGRATION {integration} API_PROVIDER = git_https_api "
                 f"API_ALLOWED_PREFIXES = ('https://github.com/{owner}') ENABLED = TRUE")
        run(cur, f"CREATE OR REPLACE GIT REPOSITORY {names['REPO']} API_INTEGRATION = {integration} "
                 f"ORIGIN = '{repo_url}'")
    run(cur, f"ALTER GIT REPOSITORY {names['REPO']} FETCH")

    # Clean slate for tasks: suspend + drop every task in PIPELINE and the per-group task schemas
    # (PIPELINE_DATA, PIPELINE_ADMIN, ... — Fabric's pipeline folders), leaves first.
    cur.execute(f"SHOW SCHEMAS LIKE 'PIPELINE%' IN DATABASE {db}")
    schemas = [r[1] for r in cur.fetchall()]
    for schema in schemas:
        cur.execute(f'SHOW TASKS IN SCHEMA {db}."{schema}"')
        for t in [r[1] for r in cur.fetchall()]:
            try:
                cur.execute(f'ALTER TASK IF EXISTS {db}."{schema}"."{t}" SUSPEND')
            except Exception:
                pass
        for _ in range(6):
            cur.execute(f'SHOW TASKS IN SCHEMA {db}."{schema}"')
            left = [r[1] for r in cur.fetchall()]
            if not left:
                break
            for t in left:
                try:
                    cur.execute(f'DROP TASK IF EXISTS {db}."{schema}"."{t}"')
                except Exception:
                    pass

    # Everything else comes from the generated files, in the layout's group order.
    for path in task_files():
        print(f"== {path.relative_to(REPO).as_posix()}")
        for stmt in sql_statements(render(path.read_text(encoding="utf-8"), names)):
            run(cur, stmt)

    # The notebooks install LakeLogic from PyPI. A task run (headless) reaches PyPI only through
    # an external access integration an account admin creates once (see README); name it in
    # SNOWFLAKE_PYPI_INTEGRATION. Without one, the install falls back to wheels staged in
    # PIPELINE.WHEELS.
    eai = os.getenv("SNOWFLAKE_PYPI_INTEGRATION", "").strip()
    if eai:
        for nb in ("nb_00_setup", "nb_pipeline_driver", "nb_test_data_driver", "nb_slo_checks",
                   "nb_compliance_erasure", "nb_compliance_retention_check", "nb_maintenance_optimize"):
            run(cur, f'ALTER NOTEBOOK {db}."pipeline"."{nb}" SET EXTERNAL_ACCESS_INTEGRATIONS = ({eai})')
    else:
        print("  note: no SNOWFLAKE_PYPI_INTEGRATION set, so task runs install LakeLogic from "
              "\"pipeline\".\"wheels\" (headless notebooks cannot reach PyPI without one).")

    if args.run:
        run(cur, f'EXECUTE TASK {db}."pipeline_data"."pl_data_00_mesh_orchestrator"')
    conn.commit()
    print("Deployed. Snowsight -> Monitoring -> Task History for runs.")


if __name__ == "__main__":
    main()
